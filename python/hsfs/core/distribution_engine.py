#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import numpy as np


if TYPE_CHECKING:
    from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


logger = logging.getLogger(__name__)

# Discriminator constants used as the second key in the per-run cache.
_WINDOW_REFERENCE = 0
_WINDOW_DETECTION = 1

# Fractional margin applied around a degenerate single-value range (min == max)
# when constructing EQUI_WIDTH edges. Expressed as a fraction of abs(value),
# with this absolute fallback when the value is zero.
_DEGENERATE_RANGE_MARGIN = 0.01


class DistributionEngine:
    """Resolves bin edges and builds probability distributions from feature statistics.

    One instance is created per monitoring run. The cache keyed by
    (fds_id, window_id) avoids re-parsing extended_statistics for the same
    FeatureDescriptiveStatistics object within a single run.
    """

    def __init__(self):
        # (fds_id, window_id) -> parsed extended_statistics dict
        self._cache: dict[tuple[int | None, int], dict] = {}

    def _clear_cache(self):
        """Clear the per-run cache. Call at the start of each monitoring run."""
        self._cache.clear()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _resolve_bin_edges(
        self,
        reference_fds: FeatureDescriptiveStatistics,
        detection_fds: FeatureDescriptiveStatistics,
        binning_strategy: str,
        bin_count: int,
        custom_edges: list[float] | None,
    ) -> list[float] | list[str]:
        """Resolve the bin edges that will be applied to both windows.

        Bin edges are always derived from the reference window (plus detection
        for CATEGORICAL to union unseen categories). The same edges are then
        passed to _build_distribution for both reference and detection.

        Parameters:
            reference_fds: Descriptive statistics for the reference window.
            detection_fds: Descriptive statistics for the detection window.
            binning_strategy: One of EQUI_WIDTH, EQUI_FREQUENCY, CUSTOM_EDGES, CATEGORICAL.
            bin_count: Number of bins (ignored for CATEGORICAL).
            custom_edges: User-supplied edges (required when binning_strategy=CUSTOM_EDGES).

        Returns:
            For numeric strategies: a sorted list of floats (bin boundaries, length = bin_count + 1).
            For CATEGORICAL: a sorted list of category-value strings.
        """
        strategy = binning_strategy.upper()

        if strategy == "CUSTOM_EDGES":
            return self._resolve_custom_edges(custom_edges)

        if strategy == "CATEGORICAL":
            return self._resolve_categorical_edges(reference_fds, detection_fds)

        if strategy == "EQUI_FREQUENCY":
            edges = self._resolve_equi_frequency_edges(reference_fds, bin_count)
            if edges is not None:
                return edges
            # fallback logged inside; drop through to EQUI_WIDTH
            strategy = "EQUI_WIDTH"

        if strategy == "EQUI_WIDTH":
            return self._resolve_equi_width_edges(reference_fds, bin_count)

        raise ValueError(
            f"Unknown binning_strategy '{binning_strategy}'. "
            "Expected one of: EQUI_WIDTH, EQUI_FREQUENCY, CUSTOM_EDGES, CATEGORICAL."
        )

    def _build_distribution(
        self,
        fds: FeatureDescriptiveStatistics,
        binning_strategy: str,
        bin_edges: list[float] | list[str],
        epsilon: float,
        window_id: int = _WINDOW_REFERENCE,
    ) -> np.ndarray:
        """Build a normalised probability vector for the given statistics object.

        Parameters:
            fds: Descriptive statistics for the window.
            binning_strategy: One of EQUI_WIDTH, EQUI_FREQUENCY, CUSTOM_EDGES, CATEGORICAL.
            bin_edges: Pre-resolved edges (from _resolve_bin_edges).
            epsilon: Small additive constant applied to every bin before renormalisation.
            window_id: Cache discriminator (_WINDOW_REFERENCE or _WINDOW_DETECTION).

        Returns:
            A 1-D numpy array of floats that sums to 1.0 ± 1e-9.
        """
        strategy = binning_strategy.upper()
        extended_stats = self._get_extended_stats(fds, window_id)

        if strategy == "CATEGORICAL":
            return self._build_categorical_distribution(
                extended_stats, bin_edges, epsilon
            )

        # Numeric strategies: build counts from the Deequ histogram aligned to bin_edges
        histogram = extended_stats.get("histogram") if extended_stats else None
        counts = self._align_numeric_histogram(histogram, bin_edges)
        return self._normalise_with_epsilon(counts, epsilon)

    def _resolve_merged_reference(
        self,
        reference_fds_list: list[FeatureDescriptiveStatistics],
        histogram_bins: int,
    ) -> FeatureDescriptiveStatistics | None:
        """Merge per-batch KLL sidecars into a synthetic reference FDS.

        Parameters:
            reference_fds_list: Per-batch descriptive statistics carrying native
                KLL sidecars to merge.
            histogram_bins: Number of bins for the CDF-approximated histogram.

        Returns:
            A FeatureDescriptiveStatistics object with merged percentiles and a
            CDF-approximated histogram, suitable for the unchanged
            _resolve_bin_edges / _build_distribution code paths. Returns None if
            the merge is not viable — the caller must fall back to the
            full-window re-profile path.
        """
        from hsfs.core.feature_descriptive_statistics import (
            FeatureDescriptiveStatistics,
        )

        if not reference_fds_list:
            return None

        total = len(reference_fds_list)

        # Validate that every FDS carries a native KLL sidecar.
        native_count = 0
        for fds in reference_fds_list:
            ext = fds.extended_statistics or {}
            kll_entry = ext.get("kll")
            if (
                isinstance(kll_entry, dict)
                and kll_entry.get("kllFormat") == "datasketches-native-v1"
            ):
                native_count += 1

        if native_count < total:
            missing = total - native_count
            feature_name = reference_fds_list[0].feature_name
            logger.info(
                "%d of %d constituent batches lack native KLL sidecars; "
                "falling back to full-window re-profile for feature '%s'",
                missing,
                total,
                feature_name,
            )
            return None

        # Collect base64 sketch bytes from each FDS.
        base64_sketches = [
            fds.extended_statistics["kll"]["bytes"] for fds in reference_fds_list
        ]

        # Invoke the JVM KllMerger via the Spark Py4J bridge.
        merged_json = self._call_kll_merger(base64_sketches, histogram_bins)
        if merged_json is None:
            return None

        parsed = json.loads(merged_json)

        # Empty merge (no rows in any batch).
        if not parsed.get("percentiles"):
            return None

        first_fds = reference_fds_list[0]

        kll_sidecar = None
        if "kll" in parsed and "kllFormat" in parsed:
            kll_sidecar = {"kllFormat": parsed["kllFormat"], "bytes": parsed["kll"]}

        extended = {"histogram": parsed["histogram"]}
        if kll_sidecar is not None:
            extended["kll"] = kll_sidecar

        return FeatureDescriptiveStatistics(
            feature_name=first_fds.feature_name,
            feature_type=first_fds.feature_type,
            percentiles=parsed["percentiles"],
            min=parsed.get("min"),
            max=parsed.get("max"),
            count=parsed.get("n"),
            extended_statistics=extended,
        )

    # ------------------------------------------------------------------
    # Edge resolution helpers
    # ------------------------------------------------------------------

    def _resolve_custom_edges(self, custom_edges: list[float] | None) -> list[float]:
        if not custom_edges or len(custom_edges) < 2:
            raise ValueError(
                "custom_bin_edges must contain at least 2 values when binning_strategy is CUSTOM_EDGES."
            )
        for i in range(1, len(custom_edges)):
            if custom_edges[i] <= custom_edges[i - 1]:
                raise ValueError(
                    "custom_bin_edges must be strictly increasing, "
                    f"but got {custom_edges[i - 1]} >= {custom_edges[i]} at index {i}."
                )
        return list(custom_edges)

    def _resolve_categorical_edges(
        self,
        reference_fds: FeatureDescriptiveStatistics,
        detection_fds: FeatureDescriptiveStatistics,
    ) -> list[str]:
        ref_ext = (
            reference_fds.extended_statistics if reference_fds is not None else None
        )
        det_ext = (
            detection_fds.extended_statistics if detection_fds is not None else None
        )

        ref_hist = ref_ext.get("histogram", []) if ref_ext else []
        det_hist = det_ext.get("histogram", []) if det_ext else []

        ref_categories = {entry["value"] for entry in ref_hist}
        det_categories = {entry["value"] for entry in det_hist}
        all_categories = ref_categories | det_categories

        if not all_categories:
            raise ValueError(
                "No histogram data available for CATEGORICAL binning. "
                "Ensure histograms were enabled during statistics computation."
            )

        return sorted(all_categories)

    def _resolve_equi_frequency_edges(
        self,
        reference_fds: FeatureDescriptiveStatistics,
        bin_count: int,
    ) -> list[float] | None:
        percentiles = reference_fds.percentiles
        if percentiles is None:
            logger.warning(
                "EQUI_FREQUENCY binning requested but percentiles are not available "
                "for feature '%s'. Falling back to EQUI_WIDTH.",
                reference_fds.feature_name,
            )
            return None

        # percentiles may be a list (from Deequ approxPercentiles — 100 elements)
        # or a dict (from REST round-trip — keys like "25%"). Normalise to list.
        if isinstance(percentiles, dict):
            pct_list = [float(v) for v in percentiles.values()]
        else:
            pct_list = [float(v) for v in percentiles]

        n = len(pct_list)
        if n < 2:
            logger.warning(
                "EQUI_FREQUENCY binning: insufficient percentile data (%d values) "
                "for feature '%s'. Falling back to EQUI_WIDTH.",
                n,
                reference_fds.feature_name,
            )
            return None

        # Quantile-style: pick bin_count+1 evenly spaced indices across pct_list
        # so the result always has exactly bin_count+1 edges (before dedup).
        idx = np.linspace(0, n - 1, bin_count + 1).round().astype(int)
        edges = [pct_list[i] for i in idx]

        # Deduplicate while preserving order and strict-increasing invariant
        edges = _deduplicate_edges(edges)

        if len(edges) < 2:
            logger.warning(
                "EQUI_FREQUENCY binning produced degenerate edges for feature '%s'. "
                "Falling back to EQUI_WIDTH.",
                reference_fds.feature_name,
            )
            return None

        return edges

    def _resolve_equi_width_edges(
        self,
        reference_fds: FeatureDescriptiveStatistics,
        bin_count: int,
    ) -> list[float]:
        # Try to read edges from the Deequ equi-width histogram first.
        histogram = None
        if reference_fds.extended_statistics:
            histogram = reference_fds.extended_statistics.get("histogram")

        if histogram is not None:
            edges = _parse_numeric_histogram_edges(histogram)
            # Only reuse if the histogram parses cleanly.
            if edges is not None and len(edges) == len(histogram) + 1:
                native_bin_count = len(histogram)
                if native_bin_count != bin_count:
                    # Honour the histogram's native bin count rather than re-binning
                    # via linspace. Re-binning to a larger bin_count creates empty bins
                    # that, after epsilon padding, flatten the reference distribution and
                    # inflate drift scores asymmetrically.
                    logger.info(
                        "EQUI_WIDTH: requested bin_count=%d but histogram has %d native bins; "
                        "using native bin count to avoid distribution distortion.",
                        bin_count,
                        native_bin_count,
                    )
                return edges

        # Fallback: use min/max from the FDS.
        fds_min = reference_fds.min
        fds_max = reference_fds.max
        if fds_min is None or fds_max is None:
            raise ValueError(
                f"Cannot resolve EQUI_WIDTH bin edges for feature '{reference_fds.feature_name}': "
                "no histogram in extended_statistics and min/max are not available."
            )
        if fds_min == fds_max:
            # Degenerate: single unique value. Create a single bin around it.
            margin = (
                abs(fds_min) * _DEGENERATE_RANGE_MARGIN
                if fds_min != 0
                else _DEGENERATE_RANGE_MARGIN
            )
            return [fds_min - margin, fds_max + margin]

        return np.linspace(fds_min, fds_max, bin_count + 1).tolist()

    # ------------------------------------------------------------------
    # Distribution building helpers
    # ------------------------------------------------------------------

    def _call_kll_merger(
        self,
        base64_sketches: list[str],
        histogram_bins: int,
    ) -> str | None:
        """Call the JVM KllMerger via Py4J and return the raw JSON string.

        Returns None if the Spark engine is not available (Python-only context).
        """
        try:
            from hsfs.engine import spark as spark_engine_module

            spark_instance = spark_engine_module.Engine.get_instance()
            jvm = spark_instance._spark_session._jvm

            java_list = jvm.java.util.ArrayList()
            for b64 in base64_sketches:
                java_list.add(b64)

            return jvm.com.logicalclocks.hsfs.spark.engine.profile.KllMerger.merge(
                java_list, histogram_bins
            )
        except Exception as exc:
            logger.warning(
                "KLL merge via JVM not available (%s). "
                "This path requires the Spark engine. "
                "Falling back to full-window re-profile.",
                exc,
            )
            return None

    def _get_extended_stats(
        self,
        fds: FeatureDescriptiveStatistics,
        window_id: int,
    ) -> dict | None:
        cache_key = (fds.id, window_id)
        if cache_key not in self._cache:
            self._cache[cache_key] = fds.extended_statistics or {}
        return self._cache[cache_key]

    def _build_categorical_distribution(
        self,
        extended_stats: dict | None,
        category_edges: list[str],
        epsilon: float,
    ) -> np.ndarray:
        histogram = extended_stats.get("histogram", []) if extended_stats else []
        freq_map = {entry["value"]: entry["count"] for entry in histogram}

        counts = np.array(
            [float(freq_map.get(cat, 0)) for cat in category_edges], dtype=np.float64
        )
        return self._normalise_with_epsilon(counts, epsilon)

    def _align_numeric_histogram(
        self,
        histogram: list[dict] | None,
        bin_edges: list[float],
    ) -> np.ndarray:
        """Map the Deequ histogram bins onto the resolved bin_edges.

        For each resolved bin [edges[i], edges[i+1]], sum the counts of any
        Deequ histogram bin whose midpoint falls within the range.
        """
        n_bins = len(bin_edges) - 1
        counts = np.zeros(n_bins, dtype=np.float64)

        if histogram is None or len(histogram) == 0:
            return counts

        for entry in histogram:
            bin_count = float(entry.get("count", 0))
            if bin_count <= 0:
                continue

            value_str = entry.get("value", "")
            lower, upper = _parse_bin_value(value_str)
            if lower is None or upper is None:
                continue

            midpoint = (lower + upper) / 2.0
            # Find the resolved bin index for this midpoint.
            idx = np.searchsorted(bin_edges, midpoint, side="right") - 1
            idx = int(np.clip(idx, 0, n_bins - 1))
            counts[idx] += bin_count

        return counts

    @staticmethod
    def _normalise_with_epsilon(counts: np.ndarray, epsilon: float) -> np.ndarray:
        """Add epsilon to each bin and normalise to a probability vector."""
        smoothed = counts + epsilon
        total = smoothed.sum()
        return smoothed / total


# ------------------------------------------------------------------
# Module-level pure helpers (no state)
# ------------------------------------------------------------------


def _parse_bin_value(value_str: str) -> tuple[float | None, float | None]:
    """Parse a Deequ histogram bin label like '0.42 to 0.84' into (lower, upper)."""
    parts = value_str.split(" to ")
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None, None


def _parse_numeric_histogram_edges(histogram: list[dict]) -> list[float] | None:
    """Extract all bin boundary values from a Deequ numeric histogram.

    Returns a list of length len(histogram) + 1, or None if parsing fails.
    """
    if not histogram:
        return None

    edges = []
    for entry in histogram:
        lower, upper = _parse_bin_value(entry.get("value", ""))
        if lower is None:
            return None
        if not edges:
            edges.append(lower)
        edges.append(upper)

    return edges


def _deduplicate_edges(edges: list[float]) -> list[float]:
    """Remove duplicates and keep the list strictly increasing."""
    if not edges:
        return edges
    result = [edges[0]]
    for v in edges[1:]:
        if v > result[-1]:
            result.append(v)
    return result

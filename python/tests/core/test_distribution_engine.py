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
import json
from unittest.mock import MagicMock

import pytest
from hsfs.core.distribution_engine import (
    _WINDOW_DETECTION,
    _WINDOW_REFERENCE,
    DistributionEngine,
)
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fds(
    fid: int,
    feature_name: str = "amount",
    min_val: float | None = None,
    max_val: float | None = None,
    percentiles=None,
    histogram: list[dict] | None = None,
) -> FeatureDescriptiveStatistics:
    """Build a minimal FDS with the given attributes, optionally injecting extended_statistics."""
    extended = {}
    if histogram is not None:
        extended["histogram"] = histogram
    return FeatureDescriptiveStatistics(
        feature_name=feature_name,
        feature_type="Fractional",
        id=fid,
        min=min_val,
        max=max_val,
        percentiles=percentiles,
        extended_statistics=extended if extended else None,
    )


def _make_histogram(bin_specs: list[tuple[float, float, int]]) -> list[dict]:
    """Build a Deequ-style histogram: [{"value": "lo to hi", "count": c, "ratio": r}]."""
    total = sum(c for _, _, c in bin_specs)
    return [
        {
            "value": f"{lo} to {hi}",
            "count": c,
            "ratio": c / total,
        }
        for lo, hi, c in bin_specs
    ]


# ---------------------------------------------------------------------------
# _resolve_bin_edges — EQUI_WIDTH
# ---------------------------------------------------------------------------


class TestResolveEquiWidth:
    def test_reads_edges_from_histogram(self):
        # 3 bins: [0 to 1], [1 to 2], [2 to 3] → 4 edges
        hist = _make_histogram([(0.0, 1.0, 10), (1.0, 2.0, 20), (2.0, 3.0, 30)])
        fds = _make_fds(1, histogram=hist, min_val=0.0, max_val=3.0)
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=fds,
            detection_fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_count=3,
            custom_edges=None,
        )

        assert len(edges) == 4
        assert edges[0] == pytest.approx(0.0)
        assert edges[-1] == pytest.approx(3.0)

    def test_uses_native_bin_count_when_histogram_bin_count_differs(self, caplog):
        import logging

        # Histogram has 3 bins but bin_count=5 requested.
        # The engine must honour the native 3 bins to avoid distribution distortion.
        hist = _make_histogram([(0.0, 1.0, 10), (1.0, 2.0, 20), (2.0, 3.0, 30)])
        fds = _make_fds(1, histogram=hist, min_val=0.0, max_val=3.0)
        engine = DistributionEngine()

        with caplog.at_level(logging.INFO, logger="hsfs.core.distribution_engine"):
            edges = engine._resolve_bin_edges(
                reference_fds=fds,
                detection_fds=fds,
                binning_strategy="EQUI_WIDTH",
                bin_count=5,
                custom_edges=None,
            )

        # Native 3 bins → 4 edges, not 6 (which would have been the re-binned result)
        assert len(edges) == 4  # 3 native bins = 4 edges
        assert edges[0] == pytest.approx(0.0)
        assert edges[-1] == pytest.approx(3.0)
        # A warning must be logged
        assert "native bin count" in caplog.text

    def test_fallback_to_min_max_when_no_histogram(self):
        fds = _make_fds(2, min_val=0.0, max_val=10.0)
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=fds,
            detection_fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_count=5,
            custom_edges=None,
        )

        assert len(edges) == 6
        assert edges[0] == pytest.approx(0.0)
        assert edges[-1] == pytest.approx(10.0)

    def test_raises_when_no_histogram_and_no_min_max(self):
        fds = _make_fds(3)  # no min/max, no histogram
        engine = DistributionEngine()

        with pytest.raises(ValueError, match="min/max are not available"):
            engine._resolve_bin_edges(
                reference_fds=fds,
                detection_fds=fds,
                binning_strategy="EQUI_WIDTH",
                bin_count=4,
                custom_edges=None,
            )

    def test_degenerate_single_value(self):
        # min == max — engine should return a 2-edge single-bin
        fds = _make_fds(4, min_val=5.0, max_val=5.0)
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=fds,
            detection_fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_count=5,
            custom_edges=None,
        )

        assert len(edges) == 2
        assert edges[0] < edges[1]


# ---------------------------------------------------------------------------
# _resolve_bin_edges — EQUI_FREQUENCY
# ---------------------------------------------------------------------------


class TestResolveEquiFrequency:
    def test_uses_percentiles_list(self):
        # 100-element list from Deequ approxPercentiles
        percentiles = [float(i) for i in range(100)]  # 0..99
        fds = _make_fds(10, percentiles=percentiles)
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=fds,
            detection_fds=fds,
            binning_strategy="EQUI_FREQUENCY",
            bin_count=10,
            custom_edges=None,
        )

        # At least 2 edges
        assert len(edges) >= 2
        # Strictly increasing
        for i in range(1, len(edges)):
            assert edges[i] > edges[i - 1]

    def test_fallback_to_equi_width_when_percentiles_none(self, caplog):
        import logging

        fds = _make_fds(11, min_val=0.0, max_val=20.0)
        assert fds.percentiles is None
        engine = DistributionEngine()

        with caplog.at_level(logging.WARNING, logger="hsfs.core.distribution_engine"):
            edges = engine._resolve_bin_edges(
                reference_fds=fds,
                detection_fds=fds,
                binning_strategy="EQUI_FREQUENCY",
                bin_count=5,
                custom_edges=None,
            )

        # Fell back to EQUI_WIDTH
        assert len(edges) == 6
        assert "Falling back to EQUI_WIDTH" in caplog.text

    def test_deduplicates_identical_percentiles(self):
        # All-same values → degenerate case → fallback to EQUI_WIDTH
        percentiles = [5.0] * 100
        fds = _make_fds(12, percentiles=percentiles, min_val=0.0, max_val=10.0)
        engine = DistributionEngine()

        # Should not raise — falls back
        edges = engine._resolve_bin_edges(
            reference_fds=fds,
            detection_fds=fds,
            binning_strategy="EQUI_FREQUENCY",
            bin_count=5,
            custom_edges=None,
        )
        assert len(edges) >= 2

    @pytest.mark.parametrize("bin_count", [3, 7, 9, 10, 15])
    def test_equi_frequency_produces_exactly_bin_count_plus_one_edges(self, bin_count):
        """Quantile-style indexing must yield exactly bin_count+1 edges (before dedup).

        Regression against the old step-based algorithm that produced bin_count+1
        bins at non-divisor bin counts (e.g. bin_count=9 on 100 percentiles → 10 bins).
        """
        # 100 strictly increasing percentiles so no dedup collisions occur.
        percentiles = [float(i) for i in range(100)]
        fds = _make_fds(13, percentiles=percentiles)
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=fds,
            detection_fds=fds,
            binning_strategy="EQUI_FREQUENCY",
            bin_count=bin_count,
            custom_edges=None,
        )

        assert len(edges) == bin_count + 1


# ---------------------------------------------------------------------------
# _resolve_bin_edges — CUSTOM_EDGES
# ---------------------------------------------------------------------------


class TestResolveCustomEdges:
    def test_returns_supplied_edges(self):
        custom = [0.0, 1.0, 2.5, 5.0]
        fds = _make_fds(20)
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=fds,
            detection_fds=fds,
            binning_strategy="CUSTOM_EDGES",
            bin_count=3,
            custom_edges=custom,
        )

        assert edges == custom

    def test_raises_when_fewer_than_2_edges(self):
        fds = _make_fds(21)
        engine = DistributionEngine()

        with pytest.raises(ValueError, match="at least 2 values"):
            engine._resolve_bin_edges(
                reference_fds=fds,
                detection_fds=fds,
                binning_strategy="CUSTOM_EDGES",
                bin_count=1,
                custom_edges=[1.0],
            )

    def test_raises_when_not_strictly_increasing(self):
        fds = _make_fds(22)
        engine = DistributionEngine()

        with pytest.raises(ValueError, match="strictly increasing"):
            engine._resolve_bin_edges(
                reference_fds=fds,
                detection_fds=fds,
                binning_strategy="CUSTOM_EDGES",
                bin_count=2,
                custom_edges=[3.0, 2.0, 5.0],
            )

    def test_raises_on_equal_adjacent_edges(self):
        fds = _make_fds(23)
        engine = DistributionEngine()

        with pytest.raises(ValueError, match="strictly increasing"):
            engine._resolve_bin_edges(
                reference_fds=fds,
                detection_fds=fds,
                binning_strategy="CUSTOM_EDGES",
                bin_count=2,
                custom_edges=[1.0, 2.0, 2.0, 3.0],
            )


# ---------------------------------------------------------------------------
# _resolve_bin_edges — CATEGORICAL
# ---------------------------------------------------------------------------


class TestResolveCategorical:
    def _cat_fds(self, fid: int, categories: list[str]) -> FeatureDescriptiveStatistics:
        hist = [{"value": c, "count": 10, "ratio": 0.5} for c in categories]
        return FeatureDescriptiveStatistics(
            feature_name="colour",
            feature_type="String",
            id=fid,
            extended_statistics={"histogram": hist},
        )

    def test_union_of_ref_and_det_categories(self):
        ref_fds = self._cat_fds(30, ["red", "green"])
        det_fds = self._cat_fds(31, ["green", "blue"])
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=ref_fds,
            detection_fds=det_fds,
            binning_strategy="CATEGORICAL",
            bin_count=2,  # ignored for CATEGORICAL
            custom_edges=None,
        )

        assert set(edges) == {"red", "green", "blue"}

    def test_deterministic_sort(self):
        ref_fds = self._cat_fds(32, ["banana", "apple", "cherry"])
        det_fds = self._cat_fds(33, [])
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=ref_fds,
            detection_fds=det_fds,
            binning_strategy="CATEGORICAL",
            bin_count=5,
            custom_edges=None,
        )

        assert edges == sorted(edges)

    def test_raises_when_no_histogram_data(self):
        ref_fds = FeatureDescriptiveStatistics(
            feature_name="colour", feature_type="String", id=34
        )
        det_fds = FeatureDescriptiveStatistics(
            feature_name="colour", feature_type="String", id=35
        )
        engine = DistributionEngine()

        with pytest.raises(ValueError, match="No histogram data"):
            engine._resolve_bin_edges(
                reference_fds=ref_fds,
                detection_fds=det_fds,
                binning_strategy="CATEGORICAL",
                bin_count=2,
                custom_edges=None,
            )


# ---------------------------------------------------------------------------
# Unknown strategy
# ---------------------------------------------------------------------------


class TestUnknownStrategy:
    def test_raises_on_unknown_strategy(self):
        fds = _make_fds(99)
        engine = DistributionEngine()

        with pytest.raises(ValueError, match="Unknown binning_strategy"):
            engine._resolve_bin_edges(
                reference_fds=fds,
                detection_fds=fds,
                binning_strategy="MAGIC",
                bin_count=5,
                custom_edges=None,
            )


# ---------------------------------------------------------------------------
# _build_distribution invariants
# ---------------------------------------------------------------------------


class TestBuildDistribution:
    def _numeric_fds_with_hist(
        self, fid: int, bin_specs: list[tuple]
    ) -> FeatureDescriptiveStatistics:
        hist = _make_histogram(bin_specs)
        return _make_fds(
            fid, histogram=hist, min_val=bin_specs[0][0], max_val=bin_specs[-1][1]
        )

    def test_ref_and_det_same_length(self):
        bin_specs = [(0.0, 1.0, 10), (1.0, 2.0, 20), (2.0, 3.0, 30)]
        ref = self._numeric_fds_with_hist(50, bin_specs)
        det = self._numeric_fds_with_hist(
            51, [(0.0, 1.0, 5), (1.0, 2.0, 10), (2.0, 3.0, 15)]
        )
        engine = DistributionEngine()

        edges = engine._resolve_bin_edges(
            reference_fds=ref,
            detection_fds=det,
            binning_strategy="EQUI_WIDTH",
            bin_count=3,
            custom_edges=None,
        )
        ref_probs = engine._build_distribution(
            fds=ref,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_REFERENCE,
        )
        det_probs = engine._build_distribution(
            fds=det,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_DETECTION,
        )

        assert len(ref_probs) == len(det_probs)

    def test_sums_to_one(self):
        bin_specs = [(0.0, 2.0, 10), (2.0, 4.0, 30), (4.0, 6.0, 20)]
        fds = self._numeric_fds_with_hist(52, bin_specs)
        edges = [0.0, 2.0, 4.0, 6.0]
        engine = DistributionEngine()

        probs = engine._build_distribution(
            fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_REFERENCE,
        )

        assert probs.sum() == pytest.approx(1.0, abs=1e-9)

    def test_unseen_categories_get_epsilon_mass(self):
        # ref has categories A, B, C; det only has A
        ref_hist = [
            {"value": "A", "count": 50, "ratio": 0.5},
            {"value": "B", "count": 30, "ratio": 0.3},
            {"value": "C", "count": 20, "ratio": 0.2},
        ]
        det_hist = [
            {"value": "A", "count": 100, "ratio": 1.0},
        ]
        ref_fds = FeatureDescriptiveStatistics(
            feature_name="cat",
            feature_type="String",
            id=60,
            extended_statistics={"histogram": ref_hist},
        )
        det_fds = FeatureDescriptiveStatistics(
            feature_name="cat",
            feature_type="String",
            id=61,
            extended_statistics={"histogram": det_hist},
        )
        engine = DistributionEngine()
        edges = sorted({"A", "B", "C"})  # ["A", "B", "C"]

        ref_probs = engine._build_distribution(
            fds=ref_fds,
            binning_strategy="CATEGORICAL",
            bin_edges=edges,
            epsilon=0.01,
            window_id=_WINDOW_REFERENCE,
        )
        det_probs = engine._build_distribution(
            fds=det_fds,
            binning_strategy="CATEGORICAL",
            bin_edges=edges,
            epsilon=0.01,
            window_id=_WINDOW_DETECTION,
        )

        # Reference has mass on all three categories
        assert ref_probs[edges.index("A")] > 0.0
        assert ref_probs[edges.index("B")] > 0.0
        assert ref_probs[edges.index("C")] > 0.0
        assert ref_probs.sum() == pytest.approx(1.0, abs=1e-9)

        # Detection: B and C are unseen but should still get > 0 mass via epsilon smoothing
        assert det_probs[edges.index("B")] > 0.0
        assert det_probs[edges.index("C")] > 0.0
        assert det_probs.sum() == pytest.approx(1.0, abs=1e-9)


# ---------------------------------------------------------------------------
# Cache behaviour
# ---------------------------------------------------------------------------


class TestCache:
    def test_same_key_returns_cached_result(self):
        hist = _make_histogram([(0.0, 1.0, 10), (1.0, 2.0, 20)])
        fds = _make_fds(70, histogram=hist)
        engine = DistributionEngine()
        edges = [0.0, 1.0, 2.0]

        # Call twice with the same window_id
        engine._build_distribution(
            fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_REFERENCE,
        )
        initial_cache_size = len(engine._cache)
        # Second call: cache must not grow
        engine._build_distribution(
            fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_REFERENCE,
        )
        assert len(engine._cache) == initial_cache_size

    def test_different_window_id_adds_cache_entry(self):
        hist = _make_histogram([(0.0, 1.0, 10), (1.0, 2.0, 20)])
        fds = _make_fds(71, histogram=hist)
        engine = DistributionEngine()
        edges = [0.0, 1.0, 2.0]

        engine._build_distribution(
            fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_REFERENCE,
        )
        first_size = len(engine._cache)
        engine._build_distribution(
            fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_DETECTION,
        )
        assert len(engine._cache) == first_size + 1

    def test_clear_cache_empties_dict(self):
        hist = _make_histogram([(0.0, 1.0, 10)])
        fds = _make_fds(72, histogram=hist)
        engine = DistributionEngine()
        edges = [0.0, 1.0]

        engine._build_distribution(
            fds=fds,
            binning_strategy="EQUI_WIDTH",
            bin_edges=edges,
            epsilon=1e-6,
            window_id=_WINDOW_REFERENCE,
        )
        assert len(engine._cache) > 0
        engine._clear_cache()
        assert len(engine._cache) == 0


# ---------------------------------------------------------------------------
# _resolve_merged_reference
# ---------------------------------------------------------------------------


def _make_native_kll_fds(
    fid: int,
    feature_name: str = "amount",
    feature_type: str = "Fractional",
    b64_bytes: str = "AAAA",
) -> FeatureDescriptiveStatistics:
    """Build an FDS with a native-format KLL sidecar."""
    return FeatureDescriptiveStatistics(
        feature_name=feature_name,
        feature_type=feature_type,
        id=fid,
        min=0.0,
        max=10.0,
        count=100,
        extended_statistics={
            "kll": {"kllFormat": "datasketches-native-v1", "bytes": b64_bytes}
        },
    )


def _make_legacy_kll_fds(fid: int) -> FeatureDescriptiveStatistics:
    """Build an FDS with a Deequ-style kll dict that lacks kllFormat."""
    return FeatureDescriptiveStatistics(
        feature_name="amount",
        feature_type="Fractional",
        id=fid,
        extended_statistics={"kll": {"someDeequKey": "someValue"}},
    )


def _make_no_kll_fds(fid: int) -> FeatureDescriptiveStatistics:
    """Build an FDS with no kll key at all in extended_statistics."""
    return FeatureDescriptiveStatistics(
        feature_name="amount",
        feature_type="Fractional",
        id=fid,
        extended_statistics={"histogram": []},
    )


# Fixed merged JSON payload returned by the mocked JVM KllMerger.
_MERGED_JSON = json.dumps(
    {
        "percentiles": [float(i) for i in range(1, 100)],  # 99 elements
        "histogram": [
            {"low_value": 0.0, "high_value": 5.0, "count": 50, "ratio": 0.5},
            {"low_value": 5.0, "high_value": 10.0, "count": 50, "ratio": 0.5},
        ],
        "kll": "bWVyZ2VkX2J5dGVz",
        "kllFormat": "datasketches-native-v1",
        "n": 300,
        "min": 0.0,
        "max": 10.0,
    }
)


class TestResolveMergedReference:
    def _make_engine_with_jvm_mock(self, jvm_merge_return: str):
        """Return a DistributionEngine whose _call_kll_merger is mocked."""
        engine = DistributionEngine()
        engine._call_kll_merger = MagicMock(return_value=jvm_merge_return)
        return engine

    def test_happy_path_with_native_sketches(self):
        """Merges 3 native FDS objects and returns a synthetic FDS."""
        fds_list = [
            _make_native_kll_fds(1, b64_bytes="Ynl0ZXMx"),
            _make_native_kll_fds(2, b64_bytes="Ynl0ZXMy"),
            _make_native_kll_fds(3, b64_bytes="Ynl0ZXMz"),
        ]
        engine = self._make_engine_with_jvm_mock(_MERGED_JSON)

        result = engine._resolve_merged_reference(fds_list, histogram_bins=20)

        assert result is not None
        assert result.feature_name == "amount"
        assert result.feature_type == "Fractional"
        assert result.percentiles is not None
        assert len(result.percentiles) == 99
        assert (
            result.extended_statistics["kll"]["kllFormat"] == "datasketches-native-v1"
        )
        assert len(result.extended_statistics["histogram"]) == 2
        # The merge was called with 3 base64 strings and 20 bins.
        engine._call_kll_merger.assert_called_once_with(
            ["Ynl0ZXMx", "Ynl0ZXMy", "Ynl0ZXMz"], 20
        )

    def test_falls_back_on_legacy_kll(self):
        """Mixed input (2 native + 1 legacy) returns None — no partial merge."""
        fds_list = [
            _make_native_kll_fds(10),
            _make_native_kll_fds(11),
            _make_legacy_kll_fds(12),
        ]
        engine = DistributionEngine()

        result = engine._resolve_merged_reference(fds_list, histogram_bins=20)

        assert result is None

    def test_falls_back_on_empty_input(self):
        """Empty list and None both return None."""
        engine = DistributionEngine()

        assert engine._resolve_merged_reference([], histogram_bins=20) is None
        assert engine._resolve_merged_reference(None, histogram_bins=20) is None

    def test_falls_back_when_kll_key_missing(self):
        """An FDS with no kll in extended_statistics triggers fallback."""
        fds_list = [
            _make_native_kll_fds(20),
            _make_no_kll_fds(21),
        ]
        engine = DistributionEngine()

        result = engine._resolve_merged_reference(fds_list, histogram_bins=20)

        assert result is None

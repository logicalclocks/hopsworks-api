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
"""Distribution distance metrics for feature monitoring.

All inputs are expected to be normalised probability vectors (sum to 1).
Smoothing (epsilon padding) is applied upstream by DistributionEngine.build_distribution
before calling compute(), so no zero-division guards are needed here beyond what
numpy handles naturally.
"""

from __future__ import annotations

import numpy as np


_SUPPORTED_METRICS = {
    "PSI",
    "KL_DIVERGENCE",
    "JS_DIVERGENCE",
    "HELLINGER",
    "WASSERSTEIN",
    "KOLMOGOROV_SMIRNOV",
}


def compute(
    det_probs: np.ndarray,
    ref_probs: np.ndarray,
    metric: str,
    *,
    bin_centres: np.ndarray | None = None,
) -> float:
    """Compute the distribution distance between detection and reference probability vectors.

    Parameters:
        det_probs: Probability vector for the detection window. Must sum to 1.
        ref_probs: Probability vector for the reference window. Must sum to 1.
        metric: Uppercase metric name. One of:
            PSI, KL_DIVERGENCE, JS_DIVERGENCE, HELLINGER, WASSERSTEIN, KOLMOGOROV_SMIRNOV.
        bin_centres: Required for WASSERSTEIN. Centre values for each bin.

    Returns:
        A finite non-negative float representing the distance.

    Raises:
        ValueError: If metric is not recognised or bin_centres is missing for WASSERSTEIN.
        ImportError: If scipy is not installed and WASSERSTEIN is requested.
    """
    metric_upper = metric.upper()
    if metric_upper not in _SUPPORTED_METRICS:
        raise ValueError(
            f"Unknown distribution metric '{metric}'. "
            f"Supported: {sorted(_SUPPORTED_METRICS)}."
        )

    det = np.asarray(det_probs, dtype=np.float64)
    ref = np.asarray(ref_probs, dtype=np.float64)

    if det.shape != ref.shape:
        raise ValueError(
            f"det_probs and ref_probs must have the same shape, "
            f"got {det.shape} vs {ref.shape}."
        )

    if np.isnan(det).any() or np.isnan(ref).any():
        raise ValueError(
            "det_probs and ref_probs must not contain NaN values. "
            "Check that statistics were computed on non-empty data and that "
            "smoothing_epsilon is positive."
        )

    _LOG_BASED_METRICS = {"PSI", "KL_DIVERGENCE", "JS_DIVERGENCE"}
    if metric_upper in _LOG_BASED_METRICS and ((det <= 0).any() or (ref <= 0).any()):
        raise ValueError(
            f"det_probs and ref_probs must contain only positive values for "
            f"log-based metric '{metric_upper}'. "
            "Ensure smoothing_epsilon > 0 is applied before calling compute()."
        )

    if metric_upper == "PSI":
        return _psi(det, ref)
    if metric_upper == "KL_DIVERGENCE":
        return _kl(det, ref)
    if metric_upper == "JS_DIVERGENCE":
        return _js(det, ref)
    if metric_upper == "HELLINGER":
        return _hellinger(det, ref)
    if metric_upper == "WASSERSTEIN":
        return _wasserstein(det, ref, bin_centres)
    if metric_upper == "KOLMOGOROV_SMIRNOV":
        return _ks(det, ref)

    # Unreachable — the guard above covers all cases.
    raise ValueError(f"Unhandled metric '{metric_upper}'.")  # pragma: no cover


# ------------------------------------------------------------------
# Metric implementations (all vectorised, no Python loops)
# ------------------------------------------------------------------


def _psi(det: np.ndarray, ref: np.ndarray) -> float:
    """Population Stability Index: sum((det - ref) * log(det / ref))."""
    log_ratio = np.log(det / ref)
    return float(np.sum((det - ref) * log_ratio))


def _kl(det: np.ndarray, ref: np.ndarray) -> float:
    """KL divergence KL(det || ref): sum(det * log(det / ref))."""
    return float(np.sum(det * np.log(det / ref)))


def _js(det: np.ndarray, ref: np.ndarray) -> float:
    """Jensen-Shannon divergence: 0.5 * KL(det || M) + 0.5 * KL(ref || M).

    M = 0.5 * (det + ref).
    JS is symmetric and bounded in [0, log(2)].
    """
    midpoint = 0.5 * (det + ref)
    kl_det_m = np.sum(det * np.log(det / midpoint))
    kl_ref_m = np.sum(ref * np.log(ref / midpoint))
    return float(0.5 * kl_det_m + 0.5 * kl_ref_m)


def _hellinger(det: np.ndarray, ref: np.ndarray) -> float:
    """Hellinger distance: sqrt(0.5 * sum((sqrt(det) - sqrt(ref))^2)).

    Bounded in [0, 1].
    """
    diff_sqrt = np.sqrt(det) - np.sqrt(ref)
    return float(np.sqrt(0.5 * np.sum(diff_sqrt**2)))


def _wasserstein(
    det: np.ndarray,
    ref: np.ndarray,
    bin_centres: np.ndarray | None,
) -> float:
    """Earth Mover's Distance (Wasserstein-1) using scipy.

    Requires scipy. If not installed, raises ImportError with an install hint.
    bin_centres must be provided — they serve as the support points for both distributions.
    """
    if bin_centres is None:
        raise ValueError(
            "bin_centres is required for WASSERSTEIN distance. "
            "Pass the midpoint of each bin as a numpy array."
        )

    try:
        from scipy.stats import wasserstein_distance
    except ImportError as exc:
        raise ImportError(
            "scipy is required for the WASSERSTEIN distance metric. "
            'Install it with: pip install "hopsworks[monitoring]"'
        ) from exc

    centres = np.asarray(bin_centres, dtype=np.float64)
    return float(
        wasserstein_distance(
            u_values=centres,
            v_values=centres,
            u_weights=det,
            v_weights=ref,
        )
    )


def _ks(det: np.ndarray, ref: np.ndarray) -> float:
    """Kolmogorov-Smirnov statistic: max(|CDF_det - CDF_ref|).

    Pure NumPy — no scipy required. Operates on the binned probability vectors
    (CDFs derived from them) rather than raw samples.
    """
    cdf_det = np.cumsum(det)
    cdf_ref = np.cumsum(ref)
    return float(np.max(np.abs(cdf_det - cdf_ref)))

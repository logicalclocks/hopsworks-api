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
import math

import numpy as np
import pytest
from hsfs.core.distribution_distance import compute


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _norm(v):
    """Normalise a list to a probability vector."""
    v = np.array(v, dtype=np.float64)
    return v / v.sum()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# 2-bin distributions: [0.8, 0.2] vs [0.4, 0.6]
REF_2 = _norm([4, 6])  # [0.4, 0.6]
DET_2 = _norm([8, 2])  # [0.8, 0.2]

# 5-bin distributions: gently shifted
REF_5 = _norm([1, 2, 4, 2, 1])
DET_5 = _norm([1, 1, 2, 3, 3])

# 20-bin: nearly identical (small distance expected)
REF_20 = _norm(list(range(1, 21)))
DET_20 = _norm(list(range(2, 22)))

# Symmetric pair
SYM_A = _norm([3, 1, 1, 5])
SYM_B = _norm([1, 4, 3, 2])


# ---------------------------------------------------------------------------
# PSI
# ---------------------------------------------------------------------------


class TestPSI:
    def test_2bin_hand_value(self):
        # PSI = sum((det - ref) * log(det/ref))
        # det=[0.8,0.2], ref=[0.4,0.6]
        # term1 = (0.8-0.4)*log(0.8/0.4) = 0.4*log(2) ≈ 0.2773
        # term2 = (0.2-0.6)*log(0.2/0.6) = -0.4*log(1/3) ≈ 0.4394
        # sum ≈ 0.7167
        expected = (0.8 - 0.4) * math.log(0.8 / 0.4) + (0.2 - 0.6) * math.log(0.2 / 0.6)
        result = compute(DET_2, REF_2, "PSI")
        assert result == pytest.approx(expected, abs=1e-9)

    def test_identical_distributions(self):
        # PSI of identical distributions is 0
        p = _norm([1, 2, 3, 4])
        assert compute(p, p, "PSI") == pytest.approx(0.0, abs=1e-9)

    def test_5bin(self):
        result = compute(DET_5, REF_5, "PSI")
        assert result > 0.0
        assert math.isfinite(result)

    def test_20bin(self):
        result = compute(DET_20, REF_20, "PSI")
        assert result > 0.0
        assert math.isfinite(result)

    def test_case_insensitive(self):
        r1 = compute(DET_2, REF_2, "PSI")
        r2 = compute(DET_2, REF_2, "psi")
        assert r1 == pytest.approx(r2)


# ---------------------------------------------------------------------------
# KL Divergence
# ---------------------------------------------------------------------------


class TestKLDivergence:
    def test_2bin_hand_value(self):
        # KL(det||ref) = sum(det * log(det/ref))
        # = 0.8*log(0.8/0.4) + 0.2*log(0.2/0.6) ≈ 0.5545 - 0.2197 = 0.3348
        expected = 0.8 * math.log(0.8 / 0.4) + 0.2 * math.log(0.2 / 0.6)
        result = compute(DET_2, REF_2, "KL_DIVERGENCE")
        assert result == pytest.approx(expected, abs=1e-9)

    def test_asymmetric(self):
        # KL(det||ref) != KL(ref||det) in general
        forward = compute(DET_2, REF_2, "KL_DIVERGENCE")
        backward = compute(REF_2, DET_2, "KL_DIVERGENCE")
        assert forward != pytest.approx(backward, abs=1e-6)

    def test_identical_distributions(self):
        p = _norm([2, 3, 5])
        assert compute(p, p, "KL_DIVERGENCE") == pytest.approx(0.0, abs=1e-9)

    def test_5bin(self):
        result = compute(DET_5, REF_5, "KL_DIVERGENCE")
        assert result > 0.0
        assert math.isfinite(result)


# ---------------------------------------------------------------------------
# JS Divergence
# ---------------------------------------------------------------------------


class TestJSDivergence:
    def test_2bin_hand_value(self):
        # JS = 0.5*KL(det||M) + 0.5*KL(ref||M) where M = 0.5*(det+ref)
        m = 0.5 * (DET_2 + REF_2)
        kl_dm = float(np.sum(DET_2 * np.log(DET_2 / m)))
        kl_rm = float(np.sum(REF_2 * np.log(REF_2 / m)))
        expected = 0.5 * kl_dm + 0.5 * kl_rm
        result = compute(DET_2, REF_2, "JS_DIVERGENCE")
        assert result == pytest.approx(expected, abs=1e-9)

    def test_symmetric(self):
        # JS is symmetric
        js_ab = compute(SYM_A, SYM_B, "JS_DIVERGENCE")
        js_ba = compute(SYM_B, SYM_A, "JS_DIVERGENCE")
        assert js_ab == pytest.approx(js_ba, abs=1e-9)

    def test_bounded_0_log2(self):
        # JS in [0, log(2)]
        result = compute(DET_5, REF_5, "JS_DIVERGENCE")
        assert 0.0 <= result <= math.log(2) + 1e-9

    def test_identical_distributions(self):
        p = _norm([1, 1, 1])
        assert compute(p, p, "JS_DIVERGENCE") == pytest.approx(0.0, abs=1e-9)


# ---------------------------------------------------------------------------
# Hellinger
# ---------------------------------------------------------------------------


class TestHellinger:
    def test_2bin_hand_value(self):
        # H = sqrt(0.5 * sum((sqrt(det) - sqrt(ref))^2))
        expected = math.sqrt(
            0.5
            * (
                (math.sqrt(DET_2[0]) - math.sqrt(REF_2[0])) ** 2
                + (math.sqrt(DET_2[1]) - math.sqrt(REF_2[1])) ** 2
            )
        )
        result = compute(DET_2, REF_2, "HELLINGER")
        assert result == pytest.approx(expected, abs=1e-9)

    def test_symmetric(self):
        h_ab = compute(SYM_A, SYM_B, "HELLINGER")
        h_ba = compute(SYM_B, SYM_A, "HELLINGER")
        assert h_ab == pytest.approx(h_ba, abs=1e-9)

    def test_bounded_0_1(self):
        result = compute(DET_5, REF_5, "HELLINGER")
        assert 0.0 <= result <= 1.0 + 1e-9

    def test_identical_distributions(self):
        p = _norm([3, 7])
        assert compute(p, p, "HELLINGER") == pytest.approx(0.0, abs=1e-9)


# ---------------------------------------------------------------------------
# KS (pure numpy, no scipy)
# ---------------------------------------------------------------------------


class TestKolmogorovSmirnov:
    def test_2bin_hand_value(self):
        # CDF_det = [0.8, 1.0], CDF_ref = [0.4, 1.0]
        # KS = max(|0.8-0.4|, |1.0-1.0|) = 0.4
        result = compute(DET_2, REF_2, "KOLMOGOROV_SMIRNOV")
        assert result == pytest.approx(0.4, abs=1e-9)

    def test_5bin(self):
        # cumulative sums differ, KS should be > 0
        result = compute(DET_5, REF_5, "KOLMOGOROV_SMIRNOV")
        assert result > 0.0
        assert result <= 1.0 + 1e-9

    def test_identical_distributions(self):
        p = _norm([1, 2, 3, 4])
        assert compute(p, p, "KOLMOGOROV_SMIRNOV") == pytest.approx(0.0, abs=1e-9)

    def test_3bin_hand_value(self):
        # det=[0.5, 0.3, 0.2], ref=[0.2, 0.5, 0.3]
        det = np.array([0.5, 0.3, 0.2])
        ref = np.array([0.2, 0.5, 0.3])
        # CDF_det=[0.5, 0.8, 1.0], CDF_ref=[0.2, 0.7, 1.0]
        # diffs = [0.3, 0.1, 0.0], KS=0.3
        result = compute(det, ref, "KOLMOGOROV_SMIRNOV")
        assert result == pytest.approx(0.3, abs=1e-9)


# ---------------------------------------------------------------------------
# Wasserstein (scipy)
# ---------------------------------------------------------------------------


class TestWasserstein:
    def test_requires_bin_centres(self):
        with pytest.raises(ValueError, match="bin_centres is required"):
            compute(DET_2, REF_2, "WASSERSTEIN")

    def test_identical_distributions_zero(self):
        pytest.importorskip("scipy")
        p = _norm([1, 3, 2])
        centres = np.array([0.5, 1.5, 2.5])
        result = compute(p, p, "WASSERSTEIN", bin_centres=centres)
        assert result == pytest.approx(0.0, abs=1e-9)

    def test_2bin_matches_scipy(self):
        pytest.importorskip("scipy")
        from scipy.stats import wasserstein_distance

        centres = np.array([0.25, 0.75])
        result = compute(DET_2, REF_2, "WASSERSTEIN", bin_centres=centres)
        expected = float(
            wasserstein_distance(
                u_values=centres,
                v_values=centres,
                u_weights=DET_2,
                v_weights=REF_2,
            )
        )
        assert result == pytest.approx(expected, abs=1e-9)

    def test_shifted_distributions(self):
        pytest.importorskip("scipy")
        # All mass in bin 0 vs all mass in bin 4 — maximum separation
        det = np.array([1.0, 0.0, 0.0, 0.0, 0.0])
        ref = np.array([0.0, 0.0, 0.0, 0.0, 1.0])
        centres = np.array([0.0, 1.0, 2.0, 3.0, 4.0])
        result = compute(det, ref, "WASSERSTEIN", bin_centres=centres)
        # Earth mover's distance = 4.0 (move all mass from bin 0 to bin 4)
        assert result == pytest.approx(4.0, abs=1e-9)


# ---------------------------------------------------------------------------
# Epsilon behaviour (zero-bin with epsilon mass)
# ---------------------------------------------------------------------------


class TestEpsilonBehaviour:
    def test_zero_bin_with_epsilon_does_not_raise(self):
        # Inputs with epsilon already applied (non-zero bins).
        # PSI and KL require all-positive entries; the engine adds epsilon before
        # calling compute(). Simulate that here.
        epsilon = 1e-6
        ref_raw = np.array([1.0, 0.0, 3.0]) + epsilon
        det_raw = np.array([2.0, 1.0, 0.0]) + epsilon
        ref = ref_raw / ref_raw.sum()
        det = det_raw / det_raw.sum()
        # Must not raise — all bins are positive after epsilon
        result = compute(det, ref, "PSI")
        assert math.isfinite(result)
        result_kl = compute(det, ref, "KL_DIVERGENCE")
        assert math.isfinite(result_kl)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrors:
    def test_unknown_metric_raises(self):
        p = _norm([1, 2])
        with pytest.raises(ValueError, match="Unknown distribution metric"):
            compute(p, p, "UNKNOWN_METRIC")

    def test_shape_mismatch_raises(self):
        p2 = _norm([1, 2])
        p3 = _norm([1, 2, 3])
        with pytest.raises(ValueError, match="same shape"):
            compute(p2, p3, "PSI")

    def test_nan_in_det_probs_raises(self):
        det = np.array([float("nan"), 0.5])
        ref = np.array([0.4, 0.6])
        with pytest.raises(ValueError, match="NaN"):
            compute(det, ref, "PSI")

    def test_nan_in_ref_probs_raises(self):
        det = np.array([0.4, 0.6])
        ref = np.array([0.5, float("nan")])
        with pytest.raises(ValueError, match="NaN"):
            compute(det, ref, "KL_DIVERGENCE")

    def test_zero_in_det_probs_raises_for_psi(self):
        det = np.array([0.0, 1.0])
        ref = np.array([0.4, 0.6])
        with pytest.raises(ValueError, match="positive values"):
            compute(det, ref, "PSI")

    def test_zero_in_ref_probs_raises_for_kl(self):
        det = np.array([0.4, 0.6])
        ref = np.array([0.0, 1.0])
        with pytest.raises(ValueError, match="positive values"):
            compute(det, ref, "KL_DIVERGENCE")

    def test_zero_in_ref_probs_raises_for_js(self):
        det = np.array([0.3, 0.7])
        ref = np.array([0.0, 1.0])
        with pytest.raises(ValueError, match="positive values"):
            compute(det, ref, "JS_DIVERGENCE")

    def test_zero_in_probs_allowed_for_hellinger(self):
        # Hellinger does not use log, so zero bins are fine
        det = np.array([0.0, 1.0])
        ref = np.array([0.5, 0.5])
        result = compute(det, ref, "HELLINGER")
        assert math.isfinite(result)
        assert result > 0.0

    def test_zero_in_probs_allowed_for_ks(self):
        # KS does not use log, so zero bins are fine
        det = np.array([0.0, 1.0])
        ref = np.array([0.5, 0.5])
        result = compute(det, ref, "KOLMOGOROV_SMIRNOV")
        assert math.isfinite(result)

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
import pytest
from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig


class TestStatisticsComparisonConfig:
    # ------------------------------------------------------------------
    # Construction — XOR invariant
    # ------------------------------------------------------------------

    def test_scalar_config_construction(self):
        sc = StatisticsComparisonConfig(
            metric="mean", threshold=1.0, relative=False, strict=True
        )
        assert sc.metric == "MEAN"
        assert sc.distribution_metric is None
        assert sc.threshold == 1.0
        assert sc.strict is True

    def test_distribution_config_construction(self):
        sc = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=0.2,
            binning_strategy="EQUI_WIDTH",
            bin_count=10,
            smoothing_epsilon=1e-6,
        )
        assert sc.distribution_metric == "PSI"
        assert sc.metric is None
        assert sc.binning_strategy == "EQUI_WIDTH"
        assert sc.bin_count == 10
        assert sc.smoothing_epsilon == pytest.approx(1e-6)

    def test_both_metric_and_distribution_metric_raises(self):
        with pytest.raises(ValueError, match="Exactly one of"):
            StatisticsComparisonConfig(
                metric="mean", distribution_metric="PSI", threshold=1.0
            )

    def test_neither_metric_nor_distribution_metric_raises(self):
        with pytest.raises(ValueError, match="Exactly one of"):
            StatisticsComparisonConfig(threshold=1.0)

    # ------------------------------------------------------------------
    # to_dict round-trip — new 5 fields
    # ------------------------------------------------------------------

    def test_distribution_config_round_trip_to_dict(self):
        custom_edges = [0.0, 1.5, 3.0]
        sc = StatisticsComparisonConfig(
            distribution_metric="KL_DIVERGENCE",
            threshold=0.5,
            strict=False,
            binning_strategy="CUSTOM_EDGES",
            bin_count=3,
            smoothing_epsilon=0.001,
            custom_bin_edges=custom_edges,
        )

        d = sc.to_dict()

        assert d["distributionMetric"] == "KL_DIVERGENCE"
        assert d["binningStrategy"] == "CUSTOM_EDGES"
        assert d["binCount"] == 3
        assert d["smoothingEpsilon"] == pytest.approx(0.001)
        assert d["customBinEdges"] == custom_edges
        # metric must not appear (or be null) for distribution configs
        assert d.get("metric") is None

    def test_scalar_config_to_dict_excludes_distribution_fields(self):
        sc = StatisticsComparisonConfig(metric="COMPLETENESS", threshold=0.05)
        d = sc.to_dict()

        assert "distributionMetric" not in d or d.get("distributionMetric") is None
        assert "binningStrategy" not in d
        assert "binCount" not in d
        assert "smoothingEpsilon" not in d
        assert "customBinEdges" not in d

    # ------------------------------------------------------------------
    # from_response_json round-trip — new 5 fields
    # ------------------------------------------------------------------

    def test_distribution_config_from_response_json_round_trip(self):
        payload = {
            "id": 5,
            "metric": None,
            "threshold": 0.2,
            "relative": False,
            "strict": True,
            "specificValue": None,
            "distributionMetric": "HELLINGER",
            "binningStrategy": "EQUI_FREQUENCY",
            "binCount": 20,
            "smoothingEpsilon": 1e-5,
            "customBinEdges": None,
        }

        sc = StatisticsComparisonConfig.from_response_json(payload)

        assert sc.distribution_metric == "HELLINGER"
        assert sc.binning_strategy == "EQUI_FREQUENCY"
        assert sc.bin_count == 20
        assert sc.smoothing_epsilon == pytest.approx(1e-5)
        assert sc.custom_bin_edges is None
        assert sc.metric is None

    def test_custom_bin_edges_round_trip(self):
        custom_edges = [0.0, 1.5, 3.0]
        sc = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=0.2,
            binning_strategy="CUSTOM_EDGES",
            custom_bin_edges=custom_edges,
        )
        d = sc.to_dict()
        assert d["customBinEdges"] == custom_edges

        # Reconstruct from camelCase payload (as returned by the backend)
        payload = {
            "metric": None,
            "threshold": 0.2,
            "relative": False,
            "strict": False,
            "specificValue": None,
            "distributionMetric": "PSI",
            "binningStrategy": "CUSTOM_EDGES",
            "customBinEdges": custom_edges,
        }
        sc2 = StatisticsComparisonConfig.from_response_json(payload)
        assert sc2.custom_bin_edges == custom_edges

    # ------------------------------------------------------------------
    # Back-compat: old scalar JSON payload must still deserialise
    # ------------------------------------------------------------------

    def test_old_scalar_json_deserialises(self):
        # An old backend response that has no distribution fields at all
        payload = {
            "id": 1,
            "metric": "MEAN",
            "threshold": 1.0,
            "relative": False,
            "strict": True,
            "specificValue": None,
        }

        sc = StatisticsComparisonConfig.from_response_json(payload)

        assert sc.metric == "MEAN"
        assert sc.distribution_metric is None
        assert sc.binning_strategy is None
        assert sc.bin_count is None
        assert sc.smoothing_epsilon is None
        assert sc.custom_bin_edges is None

    # ------------------------------------------------------------------
    # Equality: unset distribution fields don't break scalar equality
    # ------------------------------------------------------------------

    def test_scalar_configs_equal_when_new_fields_unset(self):
        sc_a = StatisticsComparisonConfig(
            metric="mean", threshold=1.0, relative=False, strict=True
        )
        sc_b = StatisticsComparisonConfig(
            metric="mean", threshold=1.0, relative=False, strict=True
        )
        assert sc_a == sc_b

    def test_distribution_configs_equal(self):
        sc_a = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=0.2,
            binning_strategy="EQUI_WIDTH",
            bin_count=10,
            smoothing_epsilon=1e-6,
        )
        sc_b = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=0.2,
            binning_strategy="EQUI_WIDTH",
            bin_count=10,
            smoothing_epsilon=1e-6,
        )
        assert sc_a == sc_b

    def test_scalar_and_distribution_not_equal(self):
        sc_scalar = StatisticsComparisonConfig(metric="mean", threshold=1.0)
        sc_dist = StatisticsComparisonConfig(
            distribution_metric="PSI", threshold=0.2, binning_strategy="EQUI_WIDTH"
        )
        assert sc_scalar != sc_dist

    # ------------------------------------------------------------------
    # Valid metric / strategy validation
    # ------------------------------------------------------------------

    def test_invalid_distribution_metric_raises(self):
        with pytest.raises(ValueError, match="Invalid distribution_metric"):
            StatisticsComparisonConfig(distribution_metric="UNKNOWN_METRIC")

    def test_invalid_binning_strategy_raises(self):
        with pytest.raises(ValueError, match="Invalid binning_strategy"):
            StatisticsComparisonConfig(
                distribution_metric="PSI", binning_strategy="HEXAGONAL"
            )

    def test_all_valid_distribution_metrics_accepted(self):
        for metric in [
            "PSI",
            "KL_DIVERGENCE",
            "JS_DIVERGENCE",
            "WASSERSTEIN",
            "HELLINGER",
            "KOLMOGOROV_SMIRNOV",
        ]:
            sc = StatisticsComparisonConfig(distribution_metric=metric)
            assert sc.distribution_metric == metric

    def test_all_valid_binning_strategies_accepted(self):
        for strategy in ["EQUI_WIDTH", "EQUI_FREQUENCY", "CUSTOM_EDGES", "CATEGORICAL"]:
            sc = StatisticsComparisonConfig(
                distribution_metric="PSI", binning_strategy=strategy
            )
            assert sc.binning_strategy == strategy

    # ------------------------------------------------------------------
    # smoothing_epsilon validation
    # ------------------------------------------------------------------

    def test_smoothing_epsilon_zero_raises(self):
        with pytest.raises(
            ValueError, match="smoothing_epsilon must be a positive number"
        ):
            StatisticsComparisonConfig(distribution_metric="PSI", smoothing_epsilon=0)

    def test_smoothing_epsilon_negative_raises(self):
        with pytest.raises(
            ValueError, match="smoothing_epsilon must be a positive number"
        ):
            StatisticsComparisonConfig(
                distribution_metric="PSI", smoothing_epsilon=-1e-6
            )

    def test_smoothing_epsilon_none_accepted(self):
        # None means "use default" — must not raise
        sc = StatisticsComparisonConfig(
            distribution_metric="PSI", smoothing_epsilon=None
        )
        assert sc.smoothing_epsilon is None

    def test_smoothing_epsilon_positive_accepted(self):
        sc = StatisticsComparisonConfig(
            distribution_metric="PSI", smoothing_epsilon=1e-6
        )
        assert sc.smoothing_epsilon == pytest.approx(1e-6)

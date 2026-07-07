#
#   Copyright 2024 Hopsworks AB
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

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import dateutil
import pytest
from hsfs import util
from hsfs.core import feature_monitoring_result_engine
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


DEFAULT_MONITORING_TIME_SORT_BY = "monitoring_time:desc"
DEFAULT_FEATURE_STORE_ID = 67
DEFAULT_FEATURE_GROUP_ID = 13
DEFAULT_FEATURE_VIEW_NAME = "test_feature_view"
DEFAULT_FEATURE_VIEW_VERSION = 2
DEFAULT_CONFIG_ID = 32
DEFAULT_FEATURE_NAME = "amount"
DEFAULT_JOB_NAME = "test_job"
DEFAULT_SPECIFIC_VALUE = 2.01
DEFAULT_DIFFERENCE = 6.5

FEATURE_MONITORING_RESULT_CREATE_API = (
    "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi._create"
)
GET_JOB_API = "hsfs.core.job_api.JobApi.get"
LAST_EXECUTION_API = "hsfs.core.job_api.JobApi.last_execution"
HSFS_CLIENT_GET_INSTANCE = "hopsworks_common.client._get_instance"


class TestFeatureMonitoringResultEngine:
    # Fetch results

    def test_get_by_config_id_via_fg(self, mocker):
        # Arrange
        start_time = "2022-01-01 10:10:10"
        end_time = "2022-02-02 20:20:20"

        mock_result_api = mocker.patch(
            "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi._get_by_config_id",
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        result_engine._fetch_all_feature_monitoring_results_by_config_id(
            config_id=DEFAULT_CONFIG_ID,
            start_time=start_time,
            end_time=end_time,
        )

        # Assert
        assert mock_result_api.call_args[1]["config_id"] == DEFAULT_CONFIG_ID
        assert isinstance(mock_result_api.call_args[1]["query_params"], dict)
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][0]
            == "monitoring_time_gte:1641031810000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][1]
            == "monitoring_time_lte:1643833220000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["sort_by"]
            == DEFAULT_MONITORING_TIME_SORT_BY
        )

    def test_get_by_config_id_via_fv(self, mocker):
        # Arrange
        start_time = "2022-01-01 01:01:01"
        end_time = "2022-02-02 02:02:02"

        mock_result_api = mocker.patch(
            "hsfs.core.feature_monitoring_result_api.FeatureMonitoringResultApi._get_by_config_id",
        )

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_view_name=DEFAULT_FEATURE_VIEW_NAME,
            feature_view_version=DEFAULT_FEATURE_VIEW_VERSION,
        )

        # Act
        result_engine._fetch_all_feature_monitoring_results_by_config_id(
            config_id=DEFAULT_CONFIG_ID,
            start_time=start_time,
            end_time=end_time,
        )

        # Assert
        assert mock_result_api.call_args[1]["config_id"] == DEFAULT_CONFIG_ID
        assert isinstance(mock_result_api.call_args[1]["query_params"], dict)
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][0]
            == "monitoring_time_gte:1640998861000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["filter_by"][1]
            == "monitoring_time_lte:1643767322000"
        )
        assert (
            mock_result_api.call_args[1]["query_params"]["sort_by"]
            == DEFAULT_MONITORING_TIME_SORT_BY
        )

    # Save results

    def test_save_with_exception(self, mocker):
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        mocker.patch(LAST_EXECUTION_API)
        mocker.patch(GET_JOB_API)
        result_engine_save_mock = mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine._save",
        )
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        before_time = util._convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        result_engine._save_with_exception(
            feature_monitoring_config_id=DEFAULT_CONFIG_ID,
            job_name=DEFAULT_JOB_NAME,
        )
        after_time = util._convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        result = result_engine_save_mock.call_args[1]["result"]
        assert result._feature_monitoring_config_id == DEFAULT_CONFIG_ID
        assert result._feature_statistics_results is None
        assert result._raised_exception is True
        assert after_time >= result._monitoring_time >= before_time

    def test_run_and_save_statistics_comparison_empty_detection_statistics(
        self, mocker
    ):
        """Zero detection FDS must still flag the detection window as empty.

        An all-features config combined with an empty window can yield an empty FDS
        list; the per-FDS loop cannot flip the empty flags in that case.
        """
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        mocker.patch(LAST_EXECUTION_API)
        mocker.patch(GET_JOB_API)
        result_engine_save_mock = mocker.patch(
            "hsfs.core.feature_monitoring_result_engine.FeatureMonitoringResultEngine._save",
        )
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        fm_config = MagicMock()
        fm_config.id = DEFAULT_CONFIG_ID
        fm_config.feature_statistics_configs = None

        # Act
        result_engine._run_and_save_statistics_comparison(
            fm_config=fm_config,
            detection_statistics=[],
            reference_statistics=[],
        )

        # Assert
        result = result_engine_save_mock.call_args[0][0]
        assert result._empty_detection_window is True
        assert result._empty_reference_window is True
        assert result._feature_statistics_results == []

    # Build result

    def test_build_feature_monitoring_result_no_statistics(self, mocker):
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        mocker.patch(LAST_EXECUTION_API)
        mocker.patch(GET_JOB_API)
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        before_time = util._convert_event_time_to_timestamp(
            datetime.now() - timedelta(seconds=1)
        )
        result = result_engine._build_feature_monitoring_result(
            feature_monitoring_config_id=DEFAULT_CONFIG_ID,
            feature_statistics_results=None,
            job_name=DEFAULT_JOB_NAME,
        )
        after_time = util._convert_event_time_to_timestamp(
            datetime.now() + timedelta(seconds=1)
        )

        # Assert
        assert result._feature_monitoring_config_id == DEFAULT_CONFIG_ID
        assert result._feature_store_id == DEFAULT_FEATURE_STORE_ID
        assert result._feature_statistics_results is None
        assert result._raised_exception is False
        assert result._empty_detection_window is False
        assert result._empty_reference_window is False
        assert after_time >= result._monitoring_time >= before_time

    def test_build_feature_monitoring_result_with_shifted_features(self, mocker):
        # Arrange
        mocker.patch(HSFS_CLIENT_GET_INSTANCE)
        from hsfs.core.feature_statistics_result import FeatureStatisticsResult

        fs_result = FeatureStatisticsResult(
            feature_name="amount",
            shifted_metric_names={"mean"},
        )
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        result = result_engine._build_feature_monitoring_result(
            feature_monitoring_config_id=DEFAULT_CONFIG_ID,
            feature_statistics_results=[fs_result],
        )

        # Assert
        assert result._feature_monitoring_config_id == DEFAULT_CONFIG_ID
        assert "amount" in result._shifted_feature_names
        assert result.shift_detected is True

    # Compute methods

    def test_compute_difference_between_specific_values(self):
        # Arrange
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        abs_diff = result_engine._compute_difference_between_specific_values(
            detection_value=25, reference_value=50, relative=False
        )
        rel_diff = result_engine._compute_difference_between_specific_values(
            detection_value=25, reference_value=50, relative=True
        )
        inf_diff = result_engine._compute_difference_between_specific_values(
            detection_value=5, reference_value=0, relative=True
        )

        # Assert
        assert abs_diff == 25
        assert rel_diff == 0.5
        assert inf_diff == float("inf")

    def test_compute_difference_between_stats(self, backend_fixtures):
        # Arrange
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        detection_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )
        reference_statistics = FeatureDescriptiveStatistics.from_response_json(
            backend_fixtures["feature_descriptive_statistics"][
                "get_fractional_feature_statistics"
            ]["response"]
        )
        reference_statistics._count += 6

        # Act
        mean_difference = result_engine._compute_difference_between_stats(
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            metric="mean",
            relative=False,
        )
        count_relative_difference = result_engine._compute_difference_between_stats(
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            metric="count",
            relative=True,
        )
        count_specific_difference = result_engine._compute_difference_between_stats(
            detection_statistics=detection_statistics,
            metric="count",
            relative=False,
            specific_value=2,
        )
        none_difference = result_engine._compute_difference_between_stats(
            detection_statistics=detection_statistics,
            metric="mean",
            relative=False,
        )

        # Assert
        assert mean_difference == 0
        assert count_relative_difference == 0.6
        assert count_specific_difference == 2
        assert none_difference is None

    def test_compute_difference_and_shift(self):
        # Arrange
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det_stats = FeatureDescriptiveStatistics(
            feature_name="amount", feature_type="Fractional", count=10, mean=5.0
        )
        ref_stats = FeatureDescriptiveStatistics(
            feature_name="amount", feature_type="Fractional", count=10, mean=10.0
        )
        sc_config_no_shift = StatisticsComparisonConfig(
            metric="mean", threshold=10.0, relative=False, strict=False, id=1
        )
        sc_config_with_shift = StatisticsComparisonConfig(
            metric="mean", threshold=3.0, relative=False, strict=False, id=2
        )

        # Act
        diff_no_shift, no_shift = result_engine._compute_difference_and_shift(
            sc_config=sc_config_no_shift,
            detection_statistics=det_stats,
            reference_statistics=ref_stats,
        )
        diff_with_shift, with_shift = result_engine._compute_difference_and_shift(
            sc_config=sc_config_with_shift,
            detection_statistics=det_stats,
            reference_statistics=ref_stats,
        )

        # Assert
        assert diff_no_shift == 5.0
        assert no_shift is False
        assert diff_with_shift == 5.0
        assert with_shift is True

    def test_default_binning_strategy_embedding(self):
        # Embeddings bin over their numeric norm histogram, so EQUI_FREQUENCY.
        fds = FeatureDescriptiveStatistics(
            feature_name="user_vector", feature_type="Embedding", count=10
        )
        assert (
            feature_monitoring_result_engine._default_binning_strategy(fds)
            == "EQUI_FREQUENCY"
        )

    def test_default_binning_strategy_non_numeric(self):
        fds = FeatureDescriptiveStatistics(
            feature_name="city", feature_type="String", count=10
        )
        assert (
            feature_monitoring_result_engine._default_binning_strategy(fds)
            == "CATEGORICAL"
        )

    def _embedding_fds(self, centroid, count=10):
        return FeatureDescriptiveStatistics(
            feature_name="user_vector",
            feature_type="Embedding",
            count=count,
            extended_statistics={"embedding": {"dimension": 2, "centroid": centroid}},
        )

    def test_compute_centroid_distance_l2(self):
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det = self._embedding_fds([0.0, 0.0])
        ref = self._embedding_fds([3.0, 4.0])

        distance = result_engine._compute_centroid_distance(
            detection_statistics=det, reference_statistics=ref
        )

        assert distance == 5.0

    def test_compute_centroid_distance_via_dispatch(self):
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det = self._embedding_fds([0.0, 0.0])
        ref = self._embedding_fds([3.0, 4.0])
        sc_config = StatisticsComparisonConfig(
            metric="CENTROID_DISTANCE", threshold=4.0, strict=False, id=1
        )

        difference, shift = result_engine._compute_difference_and_shift(
            sc_config=sc_config,
            detection_statistics=det,
            reference_statistics=ref,
        )

        assert difference == 5.0
        assert shift is True

    def test_compute_centroid_distance_length_mismatch(self):
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det = self._embedding_fds([1.0, 2.0, 3.0])
        ref = self._embedding_fds([3.0, 4.0])

        distance = result_engine._compute_centroid_distance(
            detection_statistics=det, reference_statistics=ref
        )

        assert distance is None

    def test_compute_centroid_distance_missing_centroid(self):
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det = self._embedding_fds([])  # all-invalid window
        ref = self._embedding_fds([3.0, 4.0])

        distance = result_engine._compute_centroid_distance(
            detection_statistics=det, reference_statistics=ref
        )

        assert distance is None

    def test_compute_centroid_distance_empty_window(self):
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det = self._embedding_fds([1.0, 2.0], count=0)
        ref = self._embedding_fds([3.0, 4.0])

        distance = result_engine._compute_centroid_distance(
            detection_statistics=det, reference_statistics=ref
        )

        assert distance is None

    # Helper methods

    def test_build_query_params_time_none(self):
        # Arrange
        start_time = None
        end_time = None

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert "filter_by" not in query_params
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_datetime(self):
        # Arrange
        start_time = dateutil.parser.parse("2022-01-01T01:01:01Z")
        end_time = dateutil.parser.parse("2022-02-02T02:02:02Z")
        start_timestamp = 1640998861000
        end_timestamp = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_date(self):
        # Arrange
        start_time = date(year=2022, month=1, day=1)
        end_time = date(year=2022, month=2, day=2)
        start_timestamp = 1640995200000
        end_timestamp = 1643760000000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_str(self):
        # Arrange
        start_time = "2022-01-01 01:01:01"
        end_time = "2022-02-02 02:02:02"
        start_timestamp = 1640998861000
        end_timestamp = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_timestamp}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_timestamp}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_int(self):
        # Arrange
        start_time = 1640998861000
        end_time = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=False
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_time}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_time}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY

    def test_build_query_params_time_int_with_statistics(self):
        # Arrange
        start_time = 1640998861000
        end_time = 1643767322000

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        query_params = result_engine._build_query_params(
            start_time=start_time, end_time=end_time, with_statistics=True
        )

        # Assert
        assert isinstance(query_params, dict)
        assert isinstance(query_params["filter_by"], list)
        assert len(query_params["filter_by"]) == 2
        assert query_params["filter_by"][0] == f"monitoring_time_gte:{start_time}"
        assert query_params["filter_by"][1] == f"monitoring_time_lte:{end_time}"
        assert query_params["sort_by"] == DEFAULT_MONITORING_TIME_SORT_BY
        assert query_params["expand"] == "statistics"

    # ------------------------------------------------------------------
    # H3 — Distribution dispatch in _compute_difference_and_shift
    # ------------------------------------------------------------------

    def _make_numeric_fds_with_histogram(
        self, fid: int, bins: list[tuple]
    ) -> FeatureDescriptiveStatistics:
        """Build a FDS with a Deequ-style histogram and matching min/max."""
        histogram = [
            {
                "value": f"{lo} to {hi}",
                "count": count,
                "ratio": count / sum(c for _, _, c in bins),
            }
            for lo, hi, count in bins
        ]
        lo_vals = [b[0] for b in bins]
        hi_vals = [b[1] for b in bins]
        return FeatureDescriptiveStatistics(
            feature_name="amount",
            feature_type="Fractional",
            id=fid,
            count=sum(b[2] for b in bins),
            min=min(lo_vals),
            max=max(hi_vals),
            extended_statistics={"histogram": histogram},
        )

    def test_distribution_dispatch_distance_in_difference(self):
        """When distribution_metric is set, distance lands in `difference`."""
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        bins_ref = [(0.0, 1.0, 40), (1.0, 2.0, 30), (2.0, 3.0, 30)]
        bins_det = [(0.0, 1.0, 20), (1.0, 2.0, 50), (2.0, 3.0, 30)]
        ref_fds = self._make_numeric_fds_with_histogram(101, bins_ref)
        det_fds = self._make_numeric_fds_with_histogram(102, bins_det)

        sc_config = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=10.0,  # very high — no shift
            strict=False,
            binning_strategy="EQUI_WIDTH",
            bin_count=3,
            smoothing_epsilon=1e-6,
        )

        difference, shift_detected = result_engine._compute_difference_and_shift(
            sc_config=sc_config,
            detection_statistics=det_fds,
            reference_statistics=ref_fds,
        )

        assert difference is not None
        assert difference >= 0.0
        assert shift_detected is False  # threshold=10.0 > any PSI on small distribution

    def test_strict_boundary_semantics(self):
        """strict=True uses > (excludes equality); strict=False uses >= (includes equality).

        When difference == threshold exactly:
          - strict=True  → no shift (difference is not strictly greater than threshold)
          - strict=False → shift detected (difference equals threshold, which satisfies >=)
        """
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        bins_ref = [(0.0, 1.0, 50), (1.0, 2.0, 50)]
        bins_det = [(0.0, 1.0, 80), (1.0, 2.0, 20)]
        ref_fds = self._make_numeric_fds_with_histogram(103, bins_ref)
        det_fds = self._make_numeric_fds_with_histogram(104, bins_det)

        # Compute the actual distance first so we can set threshold == distance exactly.
        sc_probe = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=100.0,
            strict=False,
            binning_strategy="EQUI_WIDTH",
            bin_count=2,
            smoothing_epsilon=1e-6,
        )
        distance, _ = result_engine._compute_difference_and_shift(
            sc_config=sc_probe,
            detection_statistics=det_fds,
            reference_statistics=ref_fds,
        )

        # strict=True with threshold == distance → no shift (difference is not > threshold)
        sc_strict = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=distance,
            strict=True,
            binning_strategy="EQUI_WIDTH",
            bin_count=2,
            smoothing_epsilon=1e-6,
        )
        _, shift_strict = result_engine._compute_difference_and_shift(
            sc_config=sc_strict,
            detection_statistics=det_fds,
            reference_statistics=ref_fds,
        )

        # strict=False with threshold == distance → shift detected (difference >= threshold)
        sc_nonstrict = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=distance,
            strict=False,
            binning_strategy="EQUI_WIDTH",
            bin_count=2,
            smoothing_epsilon=1e-6,
        )
        _, shift_nonstrict = result_engine._compute_difference_and_shift(
            sc_config=sc_nonstrict,
            detection_statistics=det_fds,
            reference_statistics=ref_fds,
        )

        assert shift_strict is False  # difference is NOT > threshold (they are equal)
        assert shift_nonstrict is True  # difference >= threshold (equal satisfies >=)

    def test_mixed_children_fixture(self):
        """A parent with one scalar child and one distribution child produces correct result rows."""
        from hsfs.core.feature_statistics_config import FeatureStatisticsConfig
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        bins_ref = [(0.0, 1.0, 40), (1.0, 2.0, 30), (2.0, 3.0, 30)]
        bins_det = [(0.0, 1.0, 20), (1.0, 2.0, 50), (2.0, 3.0, 30)]
        ref_fds = self._make_numeric_fds_with_histogram(110, bins_ref)
        ref_fds._mean = 1.0
        det_fds = self._make_numeric_fds_with_histogram(111, bins_det)
        det_fds._mean = 1.5

        # scalar child
        sc_scalar = StatisticsComparisonConfig(
            metric="mean",
            threshold=5.0,
            strict=False,
            id=201,
        )
        # distribution child
        sc_dist = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=10.0,
            strict=False,
            id=202,
            binning_strategy="EQUI_WIDTH",
            bin_count=3,
            smoothing_epsilon=1e-6,
        )

        # FeatureStatisticsConfig with both children
        fs_config = FeatureStatisticsConfig(
            feature_name="amount",
            statistics_comparison_configs=[sc_scalar, sc_dist],
        )

        fs_result = result_engine._run_feature_statistics_comparisons(
            fs_config=fs_config,
            detection_statistics=det_fds,
            reference_statistics=ref_fds,
        )

        # Both children should produce a StatisticsComparisonResult row
        sc_results = fs_result._statistics_comparison_results
        assert sc_results is not None
        assert len(sc_results) == 2
        sc_ids = {r._statistics_comparison_config_id for r in sc_results}
        assert 201 in sc_ids
        assert 202 in sc_ids

    def test_validate_passes_when_reference_has_extra_features_in_any_order(self):
        # Reproduces a real PSI/distribution-monitoring run where the ALL_TIME
        # reference returns the FG's full schema while the detection re-profile
        # only covers the configured feature. Order of reference features must
        # not affect the validator.
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det = [
            FeatureDescriptiveStatistics(
                feature_name="amount", feature_type="Fractional", count=10
            )
        ]
        ref = [
            FeatureDescriptiveStatistics(
                feature_name="gender", feature_type="String", count=10
            ),
            FeatureDescriptiveStatistics(
                feature_name="country", feature_type="String", count=10
            ),
            FeatureDescriptiveStatistics(
                feature_name="amount", feature_type="Fractional", count=10
            ),
        ]

        # Should not raise
        result_engine._validate_detection_and_reference_statistics(det, ref)

    def test_validate_raises_when_detection_feature_missing_from_reference(self):
        result_engine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        det = [
            FeatureDescriptiveStatistics(
                feature_name="foo", feature_type="Fractional", count=10
            )
        ]
        ref = [
            FeatureDescriptiveStatistics(
                feature_name="amount", feature_type="Fractional", count=10
            )
        ]

        with pytest.raises(AssertionError):
            result_engine._validate_detection_and_reference_statistics(det, ref)

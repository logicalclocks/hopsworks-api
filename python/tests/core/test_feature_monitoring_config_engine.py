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

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hsfs import util
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_config_engine
from hsfs.core import monitoring_window_config as mwc
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig


DEFAULT_DESCRIPTION = "A feature monitoring configuration for unit test."
DEFAULT_NAME = "test_monitoring_config"
DEFAULT_FEATURE_NAME = "monitored_feature"
DEFAULT_FEATURE_MONITORING_CONFIG_CREATE_API = (
    "hsfs.core.feature_monitoring_config_api.FeatureMonitoringConfigApi._create"
)
DEFAULT_FEATURE_MONITORING_CONFIG_SETUP_JOB_API = "hsfs.core.feature_monitoring_config_api.FeatureMonitoringConfigApi._setup_feature_monitoring_job"
DEFAULT_FEATURE_STORE_ID = 67
DEFAULT_FEATURE_GROUP_ID = 13
DEFAULT_FEATURE_VIEW_NAME = "feature_view_unittest"
DEFAULT_FEATURE_VIEW_VERSION = 2
DEFAULT_JOB_SCHEDULE = {
    "cron_expression": "0 0 * ? * * *",
    "start_date_time": 1676457000,
    "enabled": True,
}

VALID_CATEGORICAL_METRICS = [
    "completeness",
    "num_non_null_values",
    "num_null_values",
    "distinctness",
    "entropy",
    "uniqueness",
    "approx_num_distinct_values",
    "exact_num_distinct_values",
]
VALID_FRACTIONAL_METRICS = [
    "completeness",
    "num_non_null_values",
    "num_null_values",
    "distinctness",
    "entropy",
    "uniqueness",
    "approx_num_distinct_values",
    "exact_num_distinct_values",
    "mean",
    "max",
    "min",
    "sum",
    "std_dev",
    "count",
]


class TestFeatureMonitoringConfigEngine:
    def test_build_default_scheduled_statistics_config(self, backend_fixtures):
        # Arrange
        default_config = fmc.FeatureMonitoringConfig.from_response_json(
            backend_fixtures["feature_monitoring_config"][
                "default_scheduled_statistics_config"
            ]
        )
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        time_before = datetime.now()
        config = config_engine._build_default_scheduled_statistics_config(
            name=DEFAULT_NAME,
            feature_names=["monitored_feature"],
            valid_feature_names=["monitored_feature"],
        )
        time_after = datetime.now()

        # Assert
        assert config._feature_store_id == default_config._feature_store_id
        assert config._feature_group_id == default_config._feature_group_id
        assert config._feature_view_name == default_config._feature_view_name
        assert config._feature_view_version == default_config._feature_view_version
        assert config.enabled == default_config.enabled is True
        assert config.name == DEFAULT_NAME
        assert config.description == default_config._description is None
        assert (
            config._feature_monitoring_type
            == default_config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_COMPUTATION
        )
        assert (
            config.detection_window_config.window_config_type
            == default_config.detection_window_config.window_config_type
            == mwc.WindowConfigType.ALL_TIME
        )
        assert (
            config.detection_window_config.time_offset
            == default_config.detection_window_config.time_offset
            is None
        )
        assert (
            config.detection_window_config.window_length
            == default_config.detection_window_config.window_length
            is None
        )
        assert (
            config.detection_window_config.row_percentage
            == default_config.detection_window_config.row_percentage
            == 1.0
        )
        assert (
            config.job_schedule.cron_expression
            == default_config.job_schedule.cron_expression
            == "0 0 12 ? * * *"
        )

        assert (
            util._convert_event_time_to_timestamp(time_before)
            <= util._convert_event_time_to_timestamp(
                config.job_schedule.start_date_time
            )
            <= util._convert_event_time_to_timestamp(time_after)
        )

    def test_build_default_feature_monitoring_config(self, backend_fixtures):
        # Arrange
        default_config = fmc.FeatureMonitoringConfig.from_response_json(
            backend_fixtures["feature_monitoring_config"][
                "default_statistics_comparison_config"
            ]
        )
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        time_before = datetime.now()
        config = config_engine._build_default_feature_monitoring_config(
            name=DEFAULT_NAME,
            valid_feature_names=["monitored_feature"],
        )
        time_after = datetime.now()

        # Assert
        assert config._feature_store_id == default_config._feature_store_id
        assert config._feature_group_id == default_config._feature_group_id
        assert config._feature_view_name == default_config._feature_view_name
        assert config._feature_view_version == default_config._feature_view_version
        assert config.enabled == default_config.enabled is True
        assert config.name == DEFAULT_NAME
        assert config.description == default_config._description is None
        assert (
            config._feature_monitoring_type
            == default_config._feature_monitoring_type
            == fmc.FeatureMonitoringType.STATISTICS_COMPARISON
        )
        assert (
            config.detection_window_config.window_config_type
            == default_config.detection_window_config.window_config_type
            == mwc.WindowConfigType.ALL_TIME
        )
        assert (
            config.detection_window_config.time_offset
            == default_config.detection_window_config.time_offset
            is None
        )
        assert (
            config.detection_window_config.window_length
            == default_config.detection_window_config.window_length
            is None
        )
        assert (
            config.detection_window_config.row_percentage
            == default_config.detection_window_config.row_percentage
            == 1.0
        )
        assert (
            config.job_schedule.cron_expression
            == default_config.job_schedule.cron_expression
            == "0 0 12 ? * * *"
        )

        assert (
            util._convert_event_time_to_timestamp(time_before)
            <= util._convert_event_time_to_timestamp(config.job_schedule.start_date_time)
            <= util._convert_event_time_to_timestamp(time_after)
        )

    def test_valid_categorical_metrics(self):
        # Assert
        assert len(VALID_CATEGORICAL_METRICS) == len(
            feature_monitoring_config_engine.VALID_CATEGORICAL_METRICS
        )
        assert set(VALID_CATEGORICAL_METRICS) == set(
            feature_monitoring_config_engine.VALID_CATEGORICAL_METRICS
        )

    def test_valid_fractional_metrics(self):
        # Assert
        assert len(VALID_FRACTIONAL_METRICS) == len(
            feature_monitoring_config_engine.VALID_FRACTIONAL_METRICS
        ), (
            "One or more metrics have changed. Please, revert changes or update this test accordingly."
        )
        assert set(VALID_FRACTIONAL_METRICS) == set(
            feature_monitoring_config_engine.VALID_FRACTIONAL_METRICS
        ), (
            "One or more metrics have changed. Please, revert changes or update this test accordingly."
        )

    def test_validate_feature_statistics_configs_invalid(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        mock_fs_config = MagicMock(statistics_comparison_configs=[])

        # Act
        with pytest.raises(TypeError, match=r"must be a list of dicts or None"):
            config_engine._validate_feature_statistics_configs(
                feature_statistics_configs="asdf"
            )
        with pytest.raises(
            AttributeError,
            match=r"statistics_comparison_config is only available for feature monitoring",
        ):
            config_engine._validate_feature_statistics_configs(
                feature_statistics_configs=[mock_fs_config], without_stats_configs=True
            )

    def test_validate_statistics_comparison_config(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        sc_config = StatisticsComparisonConfig(
            metric="MEAN",
            threshold=1.0,
            relative=True,
            strict=True,
            specific_value=1.0,
        )
        config_engine.validate_statistics_comparison_config(sc_config)

        sc_config = StatisticsComparisonConfig(
            metric="MEAN",
            threshold=1.0,
            relative=True,
            strict=True,
            specific_value=None,
        )
        config_engine.validate_statistics_comparison_config(sc_config)

    def test_validate_statistics_comparison_config_invalid(self, mocker):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        mock_sc_config = MagicMock(
            metric="MEAN",
            threshold=1.0,
            relative="notvalid",
            strict=True,
            specific_value=1.0,
            distribution_metric=None,
        )
        with pytest.raises(TypeError, match=r"relative must be a boolean value"):
            config_engine.validate_statistics_comparison_config(mock_sc_config)

        mock_sc_config = MagicMock(
            metric="MEAN",
            threshold=1.0,
            relative=True,
            strict=2,
            specific_value=1.0,
            distribution_metric=None,
        )
        with pytest.raises(TypeError, match=r"strict must be a boolean value"):
            config_engine.validate_statistics_comparison_config(mock_sc_config)

        mock_sc_config = MagicMock(
            metric="MEAN",
            threshold=True,
            relative=True,
            strict=True,
            specific_value=1.0,
            distribution_metric=None,
        )
        with pytest.raises(TypeError, match=r"threshold must be a numeric value"):
            config_engine.validate_statistics_comparison_config(mock_sc_config)

        mock_sc_config = MagicMock(
            metric=True,
            threshold=1.0,
            relative=True,
            strict=True,
            specific_value=1.0,
            distribution_metric=None,
        )
        with pytest.raises(TypeError, match=r"metric must be a string value"):
            config_engine.validate_statistics_comparison_config(mock_sc_config)

        mock_sc_config = MagicMock(
            metric="MEAN",
            threshold=1.0,
            relative=True,
            strict=True,
            specific_value=True,
            distribution_metric=None,
        )
        with pytest.raises(TypeError, match=r"specific value must be a numeric value"):
            config_engine.validate_statistics_comparison_config(mock_sc_config)

    def test_validate_config_name(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        config_engine.validate_config_name("valid_name")
        config_engine.validate_config_name("v" * 64)

    def test_validate_config_name_invalid(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        with pytest.raises(
            TypeError, match=r"Invalid config name. Config name must be a string"
        ):
            config_engine.validate_config_name(1)
        with pytest.raises(
            ValueError,
            match=r"Invalid config name. Config name must be less than 64 characters",
        ):
            config_engine.validate_config_name("1" * 65)
        with pytest.raises(
            ValueError,
            match=r"Invalid config name. Config name must be alphanumeric or underscore",
        ):
            config_engine.validate_config_name("invalid%$")

    def test_validate_description(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        config_engine.validate_description("valid description")
        config_engine.validate_description("v" * 256)

    def test_validate_description_invalid(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Assert
        with pytest.raises(
            TypeError, match=r"Invalid description. Description must be a string"
        ):
            config_engine.validate_description(1)
        with pytest.raises(
            ValueError,
            match=r"Invalid description. Description must be less than 256 characters",
        ):
            config_engine.validate_description("1" * 257)

    def test_validate_feature_name(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        config_engine.validate_feature_name(
            feature_name="my_feature", valid_feature_names=["my_feature"]
        )
        config_engine.validate_feature_name(
            feature_name="my_feature", valid_feature_names=None
        )
        config_engine.validate_feature_name(
            feature_name=None, valid_feature_names=["my_feature"]
        )

    def test_validate_feature_name_invalid(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Assert
        with pytest.raises(
            TypeError, match=r"Invalid feature name. Feature name must be a string"
        ):
            config_engine.validate_feature_name(
                feature_name=1, valid_feature_names=["my_feature"]
            )
        with pytest.raises(
            ValueError, match=r"Invalid feature name. Feature name must be one of"
        ):
            config_engine.validate_feature_name(
                feature_name="invalid", valid_feature_names=["my_feature"]
            )

    def test_validate_statistics_metric(self):
        # Arrange
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        # Act
        config_engine._validate_statistics_metric("uniqueness")  # valid metric

        # Assert
        with pytest.raises(ValueError, match=r"Invalid metric"):
            config_engine._validate_statistics_metric("invalid_metric")

    def _build_mock_fm_config(self, feature_names, with_reference_window=True):
        """Build a minimal FeatureMonitoringConfig mock for run_feature_monitoring tests."""
        fs_configs = [
            MagicMock(
                feature_name=f,
                statistics_comparison_configs=None,
            )
            for f in feature_names
        ]
        config = MagicMock()
        config.id = 42
        config.get_feature_names.return_value = feature_names
        config.feature_statistics_configs = fs_configs
        config.detection_window_config = MagicMock()
        config.reference_window_config = MagicMock() if with_reference_window else None
        return config

    def test_run_feature_monitoring_empty_reference_window(self, mocker):
        # Arrange: reference window returns empty stats (count=0 per feature)
        feature_names = ["amount"]
        empty_fds = [FeatureDescriptiveStatistics(feature_name="amount", count=0)]
        non_empty_fds = [FeatureDescriptiveStatistics(feature_name="amount", count=100)]

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        mock_config = self._build_mock_fm_config(
            feature_names, with_reference_window=True
        )
        mocker.patch.object(
            config_engine._feature_monitoring_config_api,
            "get_by_name",
            return_value=mock_config,
        )

        mocker.patch.object(
            config_engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            side_effect=[non_empty_fds, empty_fds],
        )

        saved_result = MagicMock()
        saved_result.empty_reference_window = True
        saved_result.empty_detection_window = False
        saved_result.shift_detected = False
        mocker.patch.object(
            config_engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=saved_result,
        )

        entity = MagicMock()

        # Act
        result = config_engine.run_feature_monitoring(
            entity=entity,
            config_name="test_config",
        )

        # Assert
        assert result.empty_reference_window is True
        assert result.empty_detection_window is False
        assert result.shift_detected is False

    def test_run_feature_monitoring_swallows_270228(self, mocker):
        # Arrange: reference window raises RestAPIError with errorCode 270228
        feature_names = ["amount"]
        non_empty_fds = [FeatureDescriptiveStatistics(feature_name="amount", count=100)]

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        mock_config = self._build_mock_fm_config(
            feature_names, with_reference_window=True
        )
        mocker.patch.object(
            config_engine._feature_monitoring_config_api,
            "get_by_name",
            return_value=mock_config,
        )

        not_found_response = MagicMock()
        not_found_response.json.return_value = {
            "errorCode": RestAPIError.FeatureStoreErrorCode.STATISTICS_NOT_FOUND
        }
        not_found_response.status_code = 404

        def side_effect_raise_on_second_call(*args, **kwargs):
            if side_effect_raise_on_second_call.call_count == 0:
                side_effect_raise_on_second_call.call_count += 1
                return non_empty_fds
            raise RestAPIError("url", not_found_response)

        side_effect_raise_on_second_call.call_count = 0

        mocker.patch.object(
            config_engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            side_effect=side_effect_raise_on_second_call,
        )

        saved_result = MagicMock()
        saved_result.empty_reference_window = True
        saved_result.empty_detection_window = False
        saved_result.shift_detected = False
        mocker.patch.object(
            config_engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=saved_result,
        )

        entity = MagicMock()

        # Act — must not propagate the RestAPIError
        result = config_engine.run_feature_monitoring(
            entity=entity,
            config_name="test_config",
        )

        # Assert
        assert result.empty_reference_window is True
        assert result.empty_detection_window is False
        assert result.shift_detected is False

    # ------------------------------------------------------------------
    # H4 — profile_flags gate & XOR validator
    # ------------------------------------------------------------------

    def _build_mock_fm_config_with_distribution_child(self, feature_names):
        """Build a mock FeatureMonitoringConfig with one distribution child."""
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        sc_dist = StatisticsComparisonConfig(
            distribution_metric="PSI",
            threshold=0.2,
            strict=False,
            binning_strategy="EQUI_WIDTH",
            bin_count=10,
            smoothing_epsilon=1e-6,
        )
        fs_configs = [
            MagicMock(
                feature_name=f,
                statistics_comparison_configs=[sc_dist],
            )
            for f in feature_names
        ]
        config = MagicMock()
        config.id = 42
        config.get_feature_names.return_value = feature_names
        config.feature_statistics_configs = fs_configs
        config.detection_window_config = MagicMock()
        config.reference_window_config = MagicMock()
        return config

    def test_run_feature_monitoring_passes_profile_flags_for_distribution_config(
        self, mocker
    ):
        """When any child has distribution_metric set, profile_flags must include histograms and kll."""
        feature_names = ["amount"]
        non_empty_fds = [FeatureDescriptiveStatistics(feature_name="amount", count=100)]

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        mock_config = self._build_mock_fm_config_with_distribution_child(feature_names)
        mocker.patch.object(
            config_engine._feature_monitoring_config_api,
            "get_by_name",
            return_value=mock_config,
        )

        run_single_mock = mocker.patch.object(
            config_engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=non_empty_fds,
        )

        saved_result = MagicMock()
        saved_result.empty_reference_window = False
        saved_result.empty_detection_window = False
        saved_result.shift_detected = False
        mocker.patch.object(
            config_engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=saved_result,
        )

        entity = MagicMock()
        config_engine.run_feature_monitoring(entity=entity, config_name="pdf_config")

        # Both calls (detection + reference) must pass profile_flags with histograms=True, kll=True
        for call in run_single_mock.call_args_list:
            passed_flags = call.kwargs.get("profile_flags")
            assert passed_flags is not None, (
                "profile_flags must not be None for distribution config"
            )
            assert passed_flags.get("histograms") is True
            assert passed_flags.get("kll") is True

    def test_run_feature_monitoring_passes_none_profile_flags_for_scalar_config(
        self, mocker
    ):
        """When no distribution child exists, profile_flags must be None."""
        feature_names = ["amount"]
        non_empty_fds = [FeatureDescriptiveStatistics(feature_name="amount", count=100)]

        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )

        mock_config = self._build_mock_fm_config(
            feature_names, with_reference_window=True
        )
        mocker.patch.object(
            config_engine._feature_monitoring_config_api,
            "get_by_name",
            return_value=mock_config,
        )

        run_single_mock = mocker.patch.object(
            config_engine._monitoring_window_config_engine,
            "_run_single_window_monitoring",
            return_value=non_empty_fds,
        )

        saved_result = MagicMock()
        saved_result.empty_reference_window = False
        saved_result.empty_detection_window = False
        saved_result.shift_detected = False
        mocker.patch.object(
            config_engine._result_engine,
            "_run_and_save_statistics_comparison",
            return_value=saved_result,
        )

        entity = MagicMock()
        config_engine.run_feature_monitoring(entity=entity, config_name="scalar_config")

        for call in run_single_mock.call_args_list:
            passed_flags = call.kwargs.get("profile_flags")
            assert passed_flags is None, (
                "profile_flags must be None for scalar-only config"
            )

    def test_distribution_child_rejects_specific_value(self):
        """validate_statistics_comparison_config raises when distribution child has specific_value."""
        config_engine = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
            feature_store_id=DEFAULT_FEATURE_STORE_ID,
            feature_group_id=DEFAULT_FEATURE_GROUP_ID,
        )
        mock_sc = MagicMock(
            distribution_metric="PSI",
            specific_value=5.0,
            strict=True,
            threshold=0.2,
        )
        with pytest.raises(
            ValueError, match="specific_value is not allowed for distribution"
        ):
            config_engine.validate_statistics_comparison_config(mock_sc)

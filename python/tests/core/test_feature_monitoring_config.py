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

from datetime import datetime, timezone

from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.feature_monitoring_config import FeatureMonitoringType
from hsfs.core.job_schedule import JobSchedule
from hsfs.core.monitoring_window_config import WindowConfigType


class TestFeatureMonitoringConfig:
    # via feature group

    def test_from_response_json_scheduled_stats_det_rolling_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["scheduled_stats_detection_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_rolling(config, fg_id=13)

    def test_from_response_json_scheduled_stats_det_all_time_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["scheduled_stats_detection_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_all_time(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_and_spec_value_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_rolling_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling_and_spec_value(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_and_spec_value_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_all_time_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time_and_spec_value(config, fg_id=13)

    def test_from_response_json_stats_comp_det_rolling_ref_spec_value_via_fg(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_group"
        ]["stats_comparison_detection_rolling_reference_specific_value"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_spec_value(config, fg_id=13)

    # via feature view

    def test_from_response_json_scheduled_stats_det_rolling_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["scheduled_stats_detection_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_rolling(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_scheduled_stats_det_all_time_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["scheduled_stats_detection_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_scheduled_stats_det_all_time(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_rolling"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_all_time"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_td_version_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_training_dataset"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_td_version(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_all_time_ref_td_version_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_all_time_reference_training_dataset"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_all_time_ref_td_version(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_rolling_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_rolling_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_rolling_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_all_time_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_all_time_and_specific_value"][
            "response"
        ]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_all_time_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_td_version_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ][
            "stats_comparison_detection_rolling_reference_training_dataset_and_specific_value"
        ]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_td_version_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_rolling_ref_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_rolling_reference_specific_value"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_rolling_ref_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_all_time_ref_td_version_and_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ][
            "stats_comparison_detection_all_time_reference_training_dataset_and_specific_value"
        ]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_all_time_ref_td_version_and_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_stats_comp_det_all_time_ref_spec_value_via_fv(
        self, backend_fixtures
    ):
        # Arrange
        config_json = backend_fixtures["feature_monitoring_config"][
            "get_via_feature_view"
        ]["stats_comparison_detection_all_time_reference_specific_value"]["response"]

        # Act
        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        # Assert
        self.assert_stats_comp_det_all_time_ref_spec_value(
            config, fv_name="test_feature_view", fv_version=1
        )

    def test_from_response_json_with_null_job_schedule(self, backend_fixtures):
        # Regression: ingestion/legacy-FG-statistics configs have no schedule.
        # The backend emits "jobSchedule": null; deserialization must not raise.
        config_json = dict(
            backend_fixtures["feature_monitoring_config"]["get_via_feature_group"][
                "stats_comparison_detection_rolling_reference_rolling"
            ]["response"]
        )
        config_json["jobSchedule"] = None

        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        assert config.job_schedule is None

    # assert utils

    def assert_scheduled_stats_det_rolling(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPUTATION,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

    def assert_scheduled_stats_det_all_time(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPUTATION,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

    def assert_stats_comp_det_rolling_ref_rolling(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_rolling(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_rolling_ref_all_time(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_all_time(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_rolling_ref_td_version(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_all_time_ref_td_version(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config)

    def assert_stats_comp_det_rolling_ref_rolling_and_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_rolling(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_rolling_ref_all_time_and_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_all_time(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_rolling_ref_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_ref_stats=False, with_spec_value=True)

    def assert_stats_comp_det_rolling_ref_td_version_and_spec_value(
        self, config, fv_name, fv_version
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_rolling(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_all_time_ref_td_version_and_spec_value(
        self, config, fv_name, fv_version
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        self.assert_ref_td_version(config)

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_spec_value=True)

    def assert_stats_comp_det_all_time_ref_spec_value(
        self, config, fg_id=None, fv_name=None, fv_version=None
    ):
        self.assert_fm_config(
            config,
            FeatureMonitoringType.STATISTICS_COMPARISON,
            fg_id=fg_id,
            fv_name=fv_name,
            fv_version=fv_version,
        )

        self.assert_det_all_time(config)

        assert config._reference_window_config is None

        self.assert_feature_stats(config)

        self.assert_stats_configs(config, with_ref_stats=False, with_spec_value=True)

    # -- assert fm config

    def assert_fm_config(
        self, config, type=None, fg_id=None, fv_name=None, fv_version=None
    ):
        assert config._id == 32
        assert config._feature_store_id == 67
        assert config._feature_group_id == fg_id
        assert config._feature_view_name == fv_name
        assert config._feature_view_version == fv_version
        assert config._href[-2:] == "32"
        assert config._name == "unit_test_config"
        assert config.enabled is True
        assert config._feature_monitoring_type == type
        assert (
            config.job_name
            == "fg_or_fv_name_version_fm_config_name_run_feature_monitoring"
        )

        assert isinstance(config._job_schedule, JobSchedule)
        assert config._job_schedule.id == 222
        assert config._job_schedule.cron_expression == "0 0 * ? * * *"
        assert config._job_schedule.enabled is True
        assert config._job_schedule.start_date_time == datetime.fromtimestamp(
            1676457000000 / 1000, tz=timezone.utc
        )

    # -- assert windows

    def assert_det_rolling(self, config):
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._detection_window_config.time_offset == "1w"
        assert config._detection_window_config.window_length == "1d"
        assert config._detection_window_config.training_dataset_version is None

    def assert_det_all_time(self, config):
        assert (
            config._detection_window_config.window_config_type
            == WindowConfigType.ALL_TIME
        )
        assert config._detection_window_config.time_offset is None
        assert config._detection_window_config.window_length is None
        assert config._detection_window_config.training_dataset_version is None

    def assert_ref_rolling(self, config):
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.ROLLING_TIME
        )
        assert config._reference_window_config.time_offset == "1w"
        assert config._reference_window_config.window_length == "1d"
        assert config._detection_window_config.training_dataset_version is None

    def assert_ref_all_time(self, config):
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.ALL_TIME
        )
        assert config._reference_window_config.time_offset is None
        assert config._reference_window_config.window_length is None
        assert config._detection_window_config.training_dataset_version is None

    def assert_ref_td_version(self, config):
        assert (
            config._reference_window_config.window_config_type
            == WindowConfigType.TRAINING_DATASET
        )
        assert config._reference_window_config.time_offset is None
        assert config._reference_window_config.window_length is None
        assert config._reference_window_config.training_dataset_version == 33

    # -- assert feat stats and stats configs

    def assert_feature_stats(self, config):
        assert len(config._feature_statistics_configs) == 1

        fsc = config._feature_statistics_configs[0]
        assert fsc.id == 332
        assert fsc.feature_name == "monitored_feature"

        assert (
            len(config.get_feature_names()) == 1
            and "monitored_feature" in config.get_feature_names()
        )

    def assert_stats_configs(self, config, with_ref_stats=True, with_spec_value=False):
        feat_stats_configs = config._feature_statistics_configs

        stats_configs = feat_stats_configs[0].statistics_comparison_configs
        assert len(stats_configs) == 2 if with_ref_stats and with_spec_value else 1

        if with_ref_stats:
            self.assert_stats_config(stats_configs[0])
            if not with_spec_value:
                assert len(stats_configs) == 1
            else:
                assert len(stats_configs) == 2
                self.assert_stats_config(stats_configs[1], spec_value=6.6)
        elif with_spec_value:
            assert len(stats_configs) == 1
            self.assert_stats_config(stats_configs[0], spec_value=6.6)

    def assert_stats_config(self, stats_config, spec_value=None):
        assert stats_config.threshold == 1
        assert stats_config.strict is True
        assert stats_config.relative is False
        assert stats_config.metric == "MEAN"
        assert stats_config.specific_value == spec_value


# ---------------------------------------------------------------------------
# H5 — New tests for mixed-children, type promotion, fan-out, FV shortcut
# ---------------------------------------------------------------------------


class TestFeatureMonitoringConfigDistribution:
    """Tests for the distribution-related fluent builder methods (Track D/H5)."""

    DEFAULT_FS_ID = 67
    DEFAULT_FG_ID = 13

    def _build_config(self, valid_features=None):
        """Minimal FeatureMonitoringConfig with reference window already set."""
        from datetime import datetime

        cfg = fmc.FeatureMonitoringConfig(
            feature_store_id=self.DEFAULT_FS_ID,
            feature_group_id=self.DEFAULT_FG_ID,
            feature_monitoring_type=FeatureMonitoringType.STATISTICS_COMPARISON,
            name="test_config",
            feature_statistics_configs=[],
            valid_features=valid_features,
            job_schedule={
                "start_date_time": datetime.now(),
                "cron_expression": "0 0 12 ? * * *",
                "enabled": True,
            },
        )
        # inject a rolling reference window directly to bypass the setter validation chain
        from hsfs.core.monitoring_window_config import (
            MonitoringWindowConfig,
            WindowConfigType,
        )

        cfg._reference_window_config = MonitoringWindowConfig(
            window_config_type=WindowConfigType.ROLLING_TIME,
            time_offset="1w",
            window_length="1d",
        )
        cfg._detection_window_config = MonitoringWindowConfig(
            window_config_type=WindowConfigType.ROLLING_TIME,
            time_offset="0d",
            window_length="1d",
        )
        return cfg

    # Mixed-children: compare_on then compare_on_distribution
    def test_compare_on_then_compare_on_distribution_coexist(self):
        valid_features = {"amount": "double", "label": "string"}
        cfg = self._build_config(valid_features=valid_features)

        cfg.compare_on(metric="mean", threshold=1.0, feature_name="amount")
        cfg.compare_on_distribution(metric="PSI", threshold=0.2, feature_name="amount")

        # Both children on the same feature
        all_sc_configs = [
            sc
            for fs_config in cfg._feature_statistics_configs
            for sc in (fs_config.statistics_comparison_configs or [])
        ]
        scalar_children = [sc for sc in all_sc_configs if sc.metric is not None]
        dist_children = [
            sc for sc in all_sc_configs if sc.distribution_metric is not None
        ]
        assert len(scalar_children) >= 1
        assert len(dist_children) >= 1

    def test_type_promoted_to_pdf_after_compare_on_distribution(self):
        valid_features = {"amount": "double"}
        cfg = self._build_config(valid_features=valid_features)

        cfg.compare_on(metric="mean", threshold=1.0, feature_name="amount")
        cfg.compare_on_distribution(metric="PSI", threshold=0.2, feature_name="amount")

        assert (
            cfg._feature_monitoring_type
            == FeatureMonitoringType.DISTRIBUTION_COMPARISON
        )

    def test_scalar_does_not_downgrade_from_pdf(self):
        """compare_on called AFTER compare_on_distribution must not lower type to STATISTICS_COMPARISON."""
        valid_features = {"amount": "double"}
        cfg = self._build_config(valid_features=valid_features)

        cfg.compare_on_distribution(metric="PSI", threshold=0.2, feature_name="amount")
        cfg.compare_on(metric="mean", threshold=1.0, feature_name="amount")

        assert (
            cfg._feature_monitoring_type
            == FeatureMonitoringType.DISTRIBUTION_COMPARISON
        )

    def test_scalar_only_gives_statistics_comparison_type(self):
        valid_features = {"amount": "double"}
        cfg = self._build_config(valid_features=valid_features)

        cfg.compare_on(metric="mean", threshold=1.0, feature_name="amount")

        assert (
            cfg._feature_monitoring_type == FeatureMonitoringType.STATISTICS_COMPARISON
        )

    # Feature-name fallback: fan-out by metric compatibility
    def test_compare_on_mean_fans_out_to_numeric_only(self):
        valid_features = {
            "amount": "double",
            "age": "int",
            "label": "string",
            "active": "boolean",
        }
        cfg = self._build_config(valid_features=valid_features)
        cfg.compare_on(metric="mean", threshold=1.0)  # no feature_name → fan-out

        feature_names_monitored = {
            fs.feature_name for fs in cfg._feature_statistics_configs
        }
        assert "amount" in feature_names_monitored
        assert "age" in feature_names_monitored
        # Non-numeric must be absent
        assert "label" not in feature_names_monitored
        assert "active" not in feature_names_monitored

    def test_compare_on_completeness_fans_out_to_all_primitives(self):
        valid_features = {
            "amount": "double",
            "label": "string",
            "active": "boolean",
        }
        cfg = self._build_config(valid_features=valid_features)
        cfg.compare_on(metric="completeness", threshold=0.01)

        feature_names_monitored = {
            fs.feature_name for fs in cfg._feature_statistics_configs
        }
        assert "amount" in feature_names_monitored
        assert "label" in feature_names_monitored
        assert "active" in feature_names_monitored

    def test_compare_on_distribution_psi_fans_out_to_all_primitives(self):
        valid_features = {
            "amount": "double",
            "label": "string",
            "active": "boolean",
        }
        cfg = self._build_config(valid_features=valid_features)
        cfg.compare_on_distribution(metric="PSI", threshold=0.2)

        feature_names_monitored = {
            fs.feature_name for fs in cfg._feature_statistics_configs
        }
        assert "amount" in feature_names_monitored
        assert "label" in feature_names_monitored
        assert "active" in feature_names_monitored

    def test_compare_on_distribution_wasserstein_fans_out_to_numeric_only(self):
        valid_features = {
            "amount": "double",
            "label": "string",
            "active": "boolean",
        }
        cfg = self._build_config(valid_features=valid_features)
        cfg.compare_on_distribution(metric="WASSERSTEIN", threshold=0.5)

        feature_names_monitored = {
            fs.feature_name for fs in cfg._feature_statistics_configs
        }
        assert "amount" in feature_names_monitored
        assert "label" not in feature_names_monitored
        assert "active" not in feature_names_monitored

    def test_empty_compatible_set_raises_valueerror(self):
        # All features are complex types → no compatible features
        valid_features = {
            "tags": "array<string>",
            "metadata": "map<string,int>",
        }
        cfg = self._build_config(valid_features=valid_features)

        import pytest

        with pytest.raises(ValueError):
            cfg.compare_on(metric="mean", threshold=1.0)

    def test_complex_types_always_skipped_in_fan_out(self):
        valid_features = {
            "amount": "double",
            "tags": "array<string>",
            "info": "struct<x:int>",
        }
        cfg = self._build_config(valid_features=valid_features)
        cfg.compare_on(metric="completeness", threshold=0.01)

        feature_names_monitored = {
            fs.feature_name for fs in cfg._feature_statistics_configs
        }
        assert "tags" not in feature_names_monitored
        assert "info" not in feature_names_monitored

    def test_wasserstein_on_string_feature_raises(self):
        valid_features = {"label": "string"}
        cfg = self._build_config(valid_features=valid_features)

        import pytest

        with pytest.raises(ValueError, match="requires a numeric feature"):
            cfg.compare_on_distribution(
                metric="WASSERSTEIN", threshold=0.5, feature_name="label"
            )

    def test_compare_on_unknown_feature_name_raises(self):
        # A typo in feature_name must be rejected upfront when feature types are
        # known, rather than silently producing an invalid config.
        valid_features = {"amount": "double", "age": "int"}
        cfg = self._build_config(valid_features=valid_features)

        import pytest

        with pytest.raises(ValueError, match="not a valid feature"):
            cfg.compare_on(metric="mean", threshold=1.0, feature_name="amunt")

    # Row-level XOR
    def test_both_metric_and_distribution_metric_raises(self):
        import pytest
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        with pytest.raises(ValueError, match="Exactly one of"):
            StatisticsComparisonConfig(
                metric="mean", distribution_metric="PSI", threshold=1.0
            )

    def test_neither_metric_nor_distribution_metric_raises(self):
        import pytest
        from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig

        with pytest.raises(ValueError, match="Exactly one of"):
            StatisticsComparisonConfig(threshold=1.0)


# ---------------------------------------------------------------------------
# FSTORE-2050 — Model monitoring (model_name / model_version)
# ---------------------------------------------------------------------------


class TestFeatureMonitoringConfigModelFields:
    """Round-trip and serialization of model_name / model_version on FeatureMonitoringConfig."""

    def _build(self, **overrides):
        kwargs = {
            "feature_store_id": 67,
            "feature_group_id": 42,
            "feature_monitoring_type": FeatureMonitoringType.STATISTICS_COMPARISON,
            "name": "model_monitoring",
            "feature_statistics_configs": [],
        }
        kwargs.update(overrides)
        return fmc.FeatureMonitoringConfig(**kwargs)

    def test_default_model_fields_are_none(self):
        cfg = self._build()
        assert cfg.model_name is None
        assert cfg.model_version is None
        # Internal-only sentinel for with_reference_training_dataset validation.
        assert cfg._associated_model_td_version is None

    def test_constructor_accepts_model_fields(self):
        cfg = self._build(model_name="iris", model_version=3)
        assert cfg.model_name == "iris"
        assert cfg.model_version == 3

    def test_to_dict_emits_model_fields_when_set(self):
        from hsfs.core.monitoring_window_config import MonitoringWindowConfig

        cfg = self._build(model_name="iris", model_version=3)
        cfg._detection_window_config = MonitoringWindowConfig(
            window_config_type=WindowConfigType.ALL_TIME, row_percentage=1.0
        )
        d = cfg.to_dict()
        assert d["modelName"] == "iris"
        assert d["modelVersion"] == 3

    def test_to_dict_omits_model_fields_when_unset(self):
        from hsfs.core.monitoring_window_config import MonitoringWindowConfig

        cfg = self._build()
        cfg._detection_window_config = MonitoringWindowConfig(
            window_config_type=WindowConfigType.ALL_TIME, row_percentage=1.0
        )
        d = cfg.to_dict()
        assert "modelName" not in d
        assert "modelVersion" not in d


class TestWithReferenceTrainingDatasetModelValidation:
    """FSTORE-2050: with_reference_training_dataset validates against the model's TD version."""

    def _build_with_assoc(self, td_version):
        cfg = fmc.FeatureMonitoringConfig(
            feature_store_id=67,
            feature_group_id=42,
            feature_monitoring_type=FeatureMonitoringType.STATISTICS_COMPARISON,
            name="cfg",
            feature_statistics_configs=[],
            model_name="iris",
            model_version=3,
        )
        cfg._associated_model_td_version = td_version
        return cfg

    def test_default_uses_model_td_version_when_none_passed(self):
        cfg = self._build_with_assoc(td_version=7)
        cfg.with_reference_training_dataset()  # no arg
        assert cfg.reference_window_config.training_dataset_version == 7

    def test_explicit_matching_value_accepted(self):
        cfg = self._build_with_assoc(td_version=7)
        cfg.with_reference_training_dataset(training_dataset_version=7)
        assert cfg.reference_window_config.training_dataset_version == 7

    def test_explicit_mismatch_raises(self):
        import pytest
        from hopsworks_common.client.exceptions import FeatureStoreException

        cfg = self._build_with_assoc(td_version=7)
        with pytest.raises(FeatureStoreException, match="does not match"):
            cfg.with_reference_training_dataset(training_dataset_version=99)

    def test_no_association_preserves_legacy_behaviour(self):
        # Without _associated_model_td_version (regular create_feature_monitoring path),
        # the user-supplied value is taken verbatim and any (including None) is accepted.
        cfg = fmc.FeatureMonitoringConfig(
            feature_store_id=67,
            feature_group_id=42,
            feature_monitoring_type=FeatureMonitoringType.STATISTICS_COMPARISON,
            name="cfg",
            feature_statistics_configs=[],
        )
        cfg.with_reference_training_dataset(training_dataset_version=42)
        assert cfg.reference_window_config.training_dataset_version == 42


class TestFeatureViewCreateModelMonitoring:
    """FSTORE-2050: FeatureView.create_model_monitoring delegates and stamps model fields."""

    def test_raises_when_logging_not_enabled(self):
        from unittest.mock import MagicMock

        import pytest
        from hopsworks_common.client.exceptions import FeatureStoreException
        from hsfs.feature_view import FeatureView

        fv = MagicMock(spec=FeatureView)
        fv.logging_enabled = False

        with pytest.raises(FeatureStoreException, match="enable_logging"):
            FeatureView.create_model_monitoring(
                fv, name="cfg", model_name="iris", model_version=3
            )

    def test_raises_when_model_not_found(self, mocker):
        from unittest.mock import MagicMock

        import pytest
        from hopsworks_common.client.exceptions import FeatureStoreException
        from hsfs.feature_view import FeatureView

        fv = MagicMock(spec=FeatureView)
        fv.logging_enabled = True

        mocker.patch(
            "hopsworks_common.client.get_instance",
            return_value=MagicMock(_project_id=119),
        )
        mock_model_api = MagicMock()
        mock_model_api.get.return_value = None
        mocker.patch("hsml.core.model_api.ModelApi", return_value=mock_model_api)

        with pytest.raises(FeatureStoreException, match="was not found"):
            FeatureView.create_model_monitoring(
                fv, name="cfg", model_name="iris", model_version=3
            )

    def test_raises_when_model_has_no_training_dataset_version(self, mocker):
        from unittest.mock import MagicMock

        import pytest
        from hopsworks_common.client.exceptions import FeatureStoreException
        from hsfs.feature_view import FeatureView

        fv = MagicMock(spec=FeatureView)
        fv.logging_enabled = True

        mocker.patch(
            "hopsworks_common.client.get_instance",
            return_value=MagicMock(_project_id=119),
        )
        model_meta = MagicMock()
        model_meta.training_dataset_version = None
        mock_model_api = MagicMock()
        mock_model_api.get.return_value = model_meta
        mocker.patch("hsml.core.model_api.ModelApi", return_value=mock_model_api)

        with pytest.raises(
            FeatureStoreException, match="no recorded training dataset version"
        ):
            FeatureView.create_model_monitoring(
                fv, name="cfg", model_name="iris", model_version=3
            )

    def test_happy_path_stamps_model_fields_and_td_version(self, mocker):
        from unittest.mock import MagicMock, PropertyMock

        from hsfs.feature_view import FeatureView

        fv = MagicMock(spec=FeatureView)
        fv.logging_enabled = True

        mocker.patch(
            "hopsworks_common.client.get_instance",
            return_value=MagicMock(_project_id=119),
        )
        model_meta = MagicMock()
        model_meta.training_dataset_version = 9
        mock_model_api = MagicMock()
        mock_model_api.get.return_value = model_meta
        mocker.patch("hsml.core.model_api.ModelApi", return_value=mock_model_api)

        # Stub feature_logging chain and the inner create_feature_monitoring builder.
        mock_logging = MagicMock()
        mock_fg = MagicMock()
        # Use a real FeatureMonitoringConfig so attribute writes stick (MagicMock would
        # accept anything but the test would not check the write semantics).
        inner_config = fmc.FeatureMonitoringConfig(
            feature_store_id=67,
            feature_group_id=42,
            feature_monitoring_type=FeatureMonitoringType.STATISTICS_COMPARISON,
            name="cfg",
            feature_statistics_configs=[],
        )
        mock_fg.create_feature_monitoring.return_value = inner_config
        mock_logging.get_feature_group.return_value = mock_fg
        type(fv).feature_logging = PropertyMock(return_value=mock_logging)

        result = FeatureView.create_model_monitoring(
            fv,
            name="model_drift",
            model_name="iris",
            model_version=3,
            description="test",
        )

        # Assert ModelApi.get was called with name+version+project-id.
        mock_model_api.get.assert_called_once_with(
            name="iris", version=3, model_registry_id=119
        )
        # Assert delegation to the logging FG.
        mock_fg.create_feature_monitoring.assert_called_once_with(
            name="model_drift",
            description="test",
            start_date_time=None,
            end_date_time=None,
            cron_expression="0 0 12 ? * * *",
        )
        # Returned config carries the model fields and the resolved TD version.
        assert result is inner_config
        assert result.model_name == "iris"
        assert result.model_version == 3
        assert result._associated_model_td_version == 9

    # ------------------------------------------------------------------
    # Round-trip: training_dataset_id / feature_view_id / enabled
    # ------------------------------------------------------------------

    def test_from_response_json_propagates_td_id_fv_id_enabled(self, backend_fixtures):
        config_json = backend_fixtures["feature_monitoring_config"][
            "stats_comparison_with_td_id_fv_id_enabled"
        ]["response"]

        config = fmc.FeatureMonitoringConfig.from_response_json(config_json)

        assert config._training_dataset_id == 42
        assert config._feature_view_id == 7
        assert config._enabled is True
        # enabled property must return the top-level flag
        assert config.enabled is True

    def test_to_dict_emits_td_id_fv_id_enabled_when_set(self):
        config = fmc.FeatureMonitoringConfig(
            feature_store_id=67,
            feature_group_id=13,
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_COMPUTATION,
            name="test_new_fields",
            feature_statistics_configs=[],
            detection_window_config={
                "window_config_type": "ALL_TIME",
            },
            training_dataset_id=42,
            feature_view_id=7,
            enabled=True,
        )

        d = config.to_dict()

        assert d["trainingDatasetId"] == 42
        assert d["featureViewId"] == 7
        assert d["enabled"] is True

    def test_to_dict_omits_td_id_fv_id_enabled_when_none(self):
        config = fmc.FeatureMonitoringConfig(
            feature_store_id=67,
            feature_group_id=13,
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_COMPUTATION,
            name="test_no_new_fields",
            feature_statistics_configs=[],
            detection_window_config={
                "window_config_type": "ALL_TIME",
            },
        )

        d = config.to_dict()

        assert "trainingDatasetId" not in d
        assert "featureViewId" not in d
        assert "enabled" not in d

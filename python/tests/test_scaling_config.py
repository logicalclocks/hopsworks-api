#
#   Copyright 2025 Hopsworks AB
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
from hopsworks_common.constants import PREDICTOR, SCALING_CONFIG
from hsml.scaling_config import (
    PredictorScalingConfig,
    ScaleMetric,
    TransformerScalingConfig,
)


class TestScalingConfig:
    def test_scale_metric_has_value(self):
        assert ScaleMetric.has_value("CONCURRENCY")
        assert ScaleMetric.has_value("RPS")
        assert not ScaleMetric.has_value("BOGUS")

    def test_predictor_scaling_config_accepts_scale_metric_string(self):
        sc = PredictorScalingConfig(min_instances=1, scale_metric="rps")
        assert sc.scale_metric == ScaleMetric.RPS

    def test_predictor_scaling_config_invalid_scale_metric_string(self):
        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig(min_instances=1, scale_metric="bogus")

        assert "Invalid scale_metric" in str(exc_info.value)

    def test_predictor_scaling_config_invalid_scale_metric_type(self):
        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig(min_instances=1, scale_metric=123)

        assert "scale_metric must be a string or ScaleMetric" in str(exc_info.value)

    def test_get_default_scaling_configuration_kserve_scale_to_zero_required(
        self, mocker
    ):
        mocker.patch(
            "hopsworks_common.client.is_scale_to_zero_required", return_value=True
        )

        sc = PredictorScalingConfig.get_default_scaling_configuration(
            PREDICTOR.SERVING_TOOL_KSERVE, None
        )

        assert sc.min_instances == 0
        assert sc.scale_metric.value == SCALING_CONFIG.SCALE_METRIC_CONCURRENCY
        assert sc.target == SCALING_CONFIG.DEFAULT_CONCURRENCY_TARGET
        assert (
            sc.panic_window_percentage == SCALING_CONFIG.DEFAULT_PANIC_WINDOW_PERCENTAGE
        )
        assert (
            sc.panic_threshold_percentage
            == SCALING_CONFIG.DEFAULT_PANIC_THRESHOLD_PERCENTAGE
        )
        assert sc.stable_window_seconds == SCALING_CONFIG.DEFAULT_STABLE_WINDOW_SECONDS
        assert (
            sc.scale_to_zero_retention_seconds
            == SCALING_CONFIG.DEFAULT_SCALE_TO_ZERO_RETENTION_SECONDS
        )

    def test_get_default_scaling_configuration_transformer_type(self, mocker):
        mocker.patch(
            "hopsworks_common.client.is_scale_to_zero_required", return_value=False
        )

        sc = PredictorScalingConfig.get_default_scaling_configuration(
            PREDICTOR.SERVING_TOOL_DEFAULT, 1, component_type="transformer"
        )

        assert isinstance(sc, TransformerScalingConfig)
        assert sc.min_instances == 1

    def test_get_default_scaling_configuration_non_kserve_min_zero_raises(self, mocker):
        mocker.patch(
            "hopsworks_common.client.is_scale_to_zero_required", return_value=False
        )

        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig.get_default_scaling_configuration(
                PREDICTOR.SERVING_TOOL_DEFAULT, 0
            )

        assert "Minimum number of instances cannot be 0" in str(exc_info.value)

    def test_get_default_scaling_configuration_kserve_requires_zero(self, mocker):
        mocker.patch(
            "hopsworks_common.client.is_scale_to_zero_required", return_value=True
        )

        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig.get_default_scaling_configuration(
                PREDICTOR.SERVING_TOOL_KSERVE, 1
            )

        assert "Scale-to-zero is required" in str(exc_info.value)

    def test_from_json_to_json_roundtrip(self):
        json_payload = {
            "predictor_scaling_config": {
                "min_instances": 1,
                "max_instances": 2,
                "scale_metric": "CONCURRENCY",
                "target": 10,
                "panic_window_percentage": 5.0,
                "panic_threshold_percentage": 150.0,
                "stable_window_seconds": 30,
                "scale_to_zero_retention_seconds": 60,
            }
        }

        sc = PredictorScalingConfig.from_json(json_payload)

        assert sc.min_instances == 1
        assert sc.max_instances == 2
        assert sc.scale_metric.value == "CONCURRENCY"
        assert sc.target == 10
        assert sc.panic_window_percentage == 5.0
        assert sc.panic_threshold_percentage == 150.0
        assert sc.stable_window_seconds == 30
        assert sc.scale_to_zero_retention_seconds == 60

        assert sc.to_json() == {
            "min_instances": 1,
            "scale_metric": "CONCURRENCY",
            "target": 10,
            "max_instances": 2,
            "panic_window_percentage": 5.0,
            "panic_threshold_percentage": 150.0,
            "stable_window_seconds": 30,
            "scale_to_zero_retention_seconds": 60,
        }

    def test_to_dict_camelizes_scaling_config(self):
        sc = PredictorScalingConfig(
            min_instances=1,
            max_instances=2,
            scale_metric="CONCURRENCY",
            target=10,
            panic_window_percentage=5.0,
            panic_threshold_percentage=150.0,
            stable_window_seconds=30,
            scale_to_zero_retention_seconds=60,
        )

        assert sc.to_dict() == {
            "predictorScalingConfig": {
                "minInstances": 1,
                "scaleMetric": "CONCURRENCY",
                "target": 10,
                "maxInstances": 2,
                "panicWindowPercentage": 5.0,
                "panicThresholdPercentage": 150.0,
                "stableWindowSeconds": 30,
                "scaleToZeroRetentionSeconds": 60,
            }
        }

    def test_from_json_missing_min_instances_raises(self):
        json_payload = {"predictor_scaling_config": {"scale_metric": "CONCURRENCY"}}

        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig.from_json(json_payload)

        assert "missing 'min_instances'" in str(exc_info.value)

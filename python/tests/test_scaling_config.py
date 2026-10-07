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
from hopsworks_common.constants import PREDICTOR, SCALING_CONFIG
from hsml.scaling_config import (
    Autoscaler,
    LogPersistence,
    PredictorScalingConfig,
    ScaleMetric,
    TransformerScalingConfig,
)


class TestScalingConfig:
    def test_scale_metric_has_value(self):
        assert ScaleMetric._has_value("CONCURRENCY")
        assert ScaleMetric._has_value("RPS")
        assert ScaleMetric._has_value("CPU")
        assert ScaleMetric._has_value("MEMORY")
        assert ScaleMetric._has_value("QUEUE_DEPTH")
        assert ScaleMetric._has_value("KV_CACHE_USAGE")
        assert not ScaleMetric._has_value("BOGUS")

    def test_engine_metrics_and_autoscaler_round_trip(self):
        """KServe standard-mode LLM deployments scale on the vLLM engine metrics through KEDA (HWORKS-3106)."""
        sc = PredictorScalingConfig(
            min_instances=1,
            max_instances=4,
            scale_metric="queue_depth",
            target=8,
            autoscaler="keda",
        )
        assert sc.scale_metric is ScaleMetric.QUEUE_DEPTH
        assert sc.autoscaler is Autoscaler.KEDA
        assert sc.to_json()["scale_metric"] == "QUEUE_DEPTH"
        assert sc.to_json()["autoscaler"] == "KEDA"

        read_back = PredictorScalingConfig.from_response_json(
            {
                "predictor_scaling_config": {
                    "min_instances": 1,
                    "max_instances": 4,
                    "scale_metric": "KV_CACHE_USAGE",
                    "target": 80,
                    "autoscaler": "KEDA",
                }
            }
        )
        assert read_back.scale_metric is ScaleMetric.KV_CACHE_USAGE
        assert read_back.autoscaler is Autoscaler.KEDA

    def test_keda_windows_and_additional_metrics_round_trip(self):
        sc = PredictorScalingConfig(
            min_instances=1,
            max_instances=4,
            scale_metric="queue_depth",
            autoscaler="keda",
            scale_down_stabilization_window_seconds=600,
            scale_up_stabilization_window_seconds=0,
            additional_scale_metrics=[("kv_cache_usage", 80), {"scale_metric": "cpu"}],
        )
        json = sc.to_json()
        assert json["scale_down_stabilization_window_seconds"] == 600
        assert json["scale_up_stabilization_window_seconds"] == 0
        assert json["additional_scale_metrics"] == [
            {"scale_metric": "KV_CACHE_USAGE", "target": 80},
            {"scale_metric": "CPU", "target": None},
        ]
        # The REST payload camelizes nested keys too.
        payload = sc.to_dict()["predictorScalingConfig"]
        assert payload["additionalScaleMetrics"][0]["scaleMetric"] == "KV_CACHE_USAGE"

        read_back = PredictorScalingConfig.from_response_json(
            {
                "predictor_scaling_config": {
                    "min_instances": 1,
                    "max_instances": 4,
                    "scale_metric": "QUEUE_DEPTH",
                    "autoscaler": "KEDA",
                    "scale_down_stabilization_window_seconds": 600,
                    "additional_scale_metrics": [
                        {"scale_metric": "KV_CACHE_USAGE", "target": 80},
                    ],
                }
            }
        )
        assert read_back.scale_down_stabilization_window_seconds == 600
        assert read_back.scale_up_stabilization_window_seconds is None
        assert read_back.additional_scale_metrics == [
            {"scale_metric": ScaleMetric.KV_CACHE_USAGE, "target": 80}
        ]

    def test_further_engine_metrics_round_trip(self):
        for name in [
            "RUNNING_REQUESTS",
            "QUEUE_TIME",
            "TIME_TO_FIRST_TOKEN",
            "REQUEST_LATENCY",
        ]:
            sc = PredictorScalingConfig(
                min_instances=1,
                max_instances=3,
                scale_metric=name.lower(),
                autoscaler="keda",
            )
            assert sc.scale_metric == ScaleMetric(name)
            assert sc.to_json()["scale_metric"] == name
            read_back = PredictorScalingConfig.from_response_json(
                {
                    "predictor_scaling_config": {
                        "min_instances": 1,
                        "max_instances": 3,
                        "scale_metric": name,
                        "target": 7,
                    }
                }
            )
            assert read_back.scale_metric == ScaleMetric(name)
            assert read_back.target == 7

    def test_idle_scale_to_zero_round_trip(self):
        sc = PredictorScalingConfig(
            min_instances=1,
            max_instances=3,
            autoscaler="keda",
            idle_scale_to_zero=True,
            idle_cooldown_seconds=120,
            cold_start_timeout_seconds=900,
        )
        json = sc.to_json()
        assert json["idle_scale_to_zero"] is True
        assert json["idle_cooldown_seconds"] == 120
        assert json["cold_start_timeout_seconds"] == 900
        read_back = PredictorScalingConfig.from_response_json(
            {
                "predictor_scaling_config": {
                    "min_instances": 1,
                    "max_instances": 3,
                    "autoscaler": "KEDA",
                    "idle_scale_to_zero": True,
                    "idle_cooldown_seconds": 120,
                    "cold_start_timeout_seconds": 900,
                }
            }
        )
        assert read_back.idle_scale_to_zero is True
        assert read_back.idle_cooldown_seconds == 120
        assert read_back.cold_start_timeout_seconds == 900

    def test_idle_scale_to_zero_is_a_minimum_of_zero(self):
        # The flag asked for in the constructor or the setter lowers the minimum to 0.
        sc = PredictorScalingConfig(
            min_instances=1, max_instances=3, idle_scale_to_zero=True
        )
        assert sc.min_instances == 0
        sc = PredictorScalingConfig(min_instances=2, max_instances=3)
        sc.idle_scale_to_zero = True
        assert sc.min_instances == 0
        assert sc.to_json()["min_instances"] == 0

    def test_raising_the_minimum_clears_a_flag_read_back(self):
        # A configuration read back at 0 carries the flag; a minimum raised by hand must
        # win, not be pulled back to 0 by the echoed flag on the next save.
        read_back = PredictorScalingConfig.from_response_json(
            {
                "predictor_scaling_config": {
                    "min_instances": 0,
                    "max_instances": 3,
                    "autoscaler": "KEDA",
                    "idle_scale_to_zero": True,
                    "idle_cooldown_seconds": 120,
                }
            }
        )
        read_back.min_instances = 1
        assert read_back.idle_scale_to_zero is None
        json = read_back.to_json()
        assert json["min_instances"] == 1
        assert "idle_scale_to_zero" not in json
        # Lowering it to 0 again is the idle flag's meaning, no flag needed.
        read_back.min_instances = 0
        assert read_back.to_json()["min_instances"] == 0

    def test_idle_scale_to_zero_omitted_when_unset(self):
        sc = PredictorScalingConfig(min_instances=1, max_instances=3)
        json = sc.to_json()
        assert "idle_scale_to_zero" not in json
        assert "idle_cooldown_seconds" not in json
        assert "cold_start_timeout_seconds" not in json
        assert sc.idle_scale_to_zero is None
        sc.idle_scale_to_zero = True
        sc.idle_cooldown_seconds = 60
        assert sc.to_json()["idle_scale_to_zero"] is True
        assert sc.to_json()["idle_cooldown_seconds"] == 60

    def test_additional_scale_metric_without_metric_rejected(self):
        with pytest.raises(ValueError, match="must name a scale_metric"):
            PredictorScalingConfig(
                min_instances=1, additional_scale_metrics=[{"target": 5}]
            )

    def test_unknown_additional_scale_metric_from_backend_is_ignored_with_warning(self):
        with pytest.warns(
            UserWarning, match="Ignoring unknown additional scale metric"
        ):
            sc = PredictorScalingConfig.from_response_json(
                {
                    "predictor_scaling_config": {
                        "min_instances": 1,
                        "additional_scale_metrics": [
                            {"scale_metric": "FUTURE", "target": 1}
                        ],
                    }
                }
            )
        assert sc.additional_scale_metrics is None

    def test_autoscaler_unset_is_omitted_and_hpa_accepted(self):
        unset = PredictorScalingConfig(min_instances=1, scale_metric="cpu")
        assert unset.autoscaler is None
        assert "autoscaler" not in unset.to_json()

        hpa = PredictorScalingConfig(
            min_instances=1, scale_metric="cpu", autoscaler=Autoscaler.HPA
        )
        assert hpa.to_json()["autoscaler"] == "HPA"
        hpa.autoscaler = "keda"
        assert hpa.autoscaler is Autoscaler.KEDA

    def test_invalid_autoscaler_rejected(self):
        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig(min_instances=1, autoscaler="bogus")
        assert "Invalid autoscaler" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig(min_instances=1, autoscaler=123)
        assert "autoscaler must be a string or Autoscaler" in str(exc_info.value)

    def test_unknown_autoscaler_from_backend_is_ignored_with_warning(self):
        with pytest.warns(UserWarning, match="Ignoring unknown autoscaler"):
            sc = PredictorScalingConfig.from_response_json(
                {
                    "predictor_scaling_config": {
                        "min_instances": 1,
                        "autoscaler": "FUTURE",
                    }
                }
            )
        assert sc.autoscaler is None

    def test_the_horizontal_pod_autoscalers_metrics_read_back(self):
        """KServe standard mode scales on CPU or MEMORY, and defaults to CPU."""
        assert ScaleMetric("CPU") is ScaleMetric.CPU
        assert ScaleMetric("MEMORY") is ScaleMetric.MEMORY
        sc = PredictorScalingConfig.from_response_json(
            {"predictor_scaling_config": {"min_instances": 1, "scale_metric": "CPU"}}
        )
        assert sc.scale_metric is ScaleMetric.CPU

    def test_predictor_scaling_config_accepts_cpu_and_memory_metrics(self):
        cpu = PredictorScalingConfig(min_instances=1, scale_metric="cpu", target=80)
        memory = PredictorScalingConfig(
            min_instances=1, scale_metric="memory", target=80
        )

        assert cpu.scale_metric == ScaleMetric.CPU
        assert memory.scale_metric == ScaleMetric.MEMORY
        assert cpu.to_json()["scale_metric"] == "CPU"
        assert memory.to_json()["scale_metric"] == "MEMORY"

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
            "hopsworks_common.client._is_scale_to_zero_required", return_value=True
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
            "hopsworks_common.client._is_scale_to_zero_required", return_value=False
        )

        sc = PredictorScalingConfig.get_default_scaling_configuration(
            PREDICTOR.SERVING_TOOL_DEFAULT, 1, component_type="transformer"
        )

        assert isinstance(sc, TransformerScalingConfig)
        assert sc.min_instances == 1

    def test_get_default_scaling_configuration_non_kserve_min_zero_raises(self, mocker):
        mocker.patch(
            "hopsworks_common.client._is_scale_to_zero_required", return_value=False
        )

        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig.get_default_scaling_configuration(
                PREDICTOR.SERVING_TOOL_DEFAULT, 0
            )

        assert "Minimum number of instances cannot be 0" in str(exc_info.value)

    def test_get_default_scaling_configuration_kserve_requires_zero(self, mocker):
        mocker.patch(
            "hopsworks_common.client._is_scale_to_zero_required", return_value=True
        )

        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig.get_default_scaling_configuration(
                PREDICTOR.SERVING_TOOL_KSERVE, 1
            )

        assert "Scale-to-zero is required" in str(exc_info.value)

    def test_get_default_scaling_configuration_standard_mode_no_scale_to_zero_default(
        self, mocker
    ):
        # Standard mode on KServe: no scale-to-zero and no Knative-only KPA defaults, even when the cluster forces scale-to-zero.
        mocker.patch(
            "hopsworks_common.client._is_scale_to_zero_required", return_value=True
        )

        sc = PredictorScalingConfig.get_default_scaling_configuration(
            PREDICTOR.SERVING_TOOL_KSERVE, None, effective_knative_mode=False
        )

        assert sc.min_instances == SCALING_CONFIG.MIN_NUM_INSTANCES
        assert sc.scale_metric is None
        assert sc.target is None
        assert sc.panic_window_percentage is None
        assert sc.panic_threshold_percentage is None
        assert sc.stable_window_seconds is None
        assert sc.scale_to_zero_retention_seconds is None

    def test_get_default_scaling_configuration_standard_mode_does_not_raise_on_scale_to_zero(
        self, mocker
    ):
        # A caller-supplied min_instances=0 in standard mode is left to the
        # backend to validate; the client no longer raises on it.
        mocker.patch(
            "hopsworks_common.client._is_scale_to_zero_required", return_value=True
        )

        sc = PredictorScalingConfig.get_default_scaling_configuration(
            PREDICTOR.SERVING_TOOL_KSERVE, 0, effective_knative_mode=False
        )

        assert sc.min_instances == 0

    def test_get_default_scaling_configuration_knative_mode_unaffected(self, mocker):
        # effective_knative_mode=True (the default) preserves the pre-existing
        # KServe Knative behavior.
        mocker.patch(
            "hopsworks_common.client._is_scale_to_zero_required", return_value=True
        )

        sc = PredictorScalingConfig.get_default_scaling_configuration(
            PREDICTOR.SERVING_TOOL_KSERVE, None
        )

        assert sc.min_instances == 0
        assert sc.scale_metric.value == SCALING_CONFIG.SCALE_METRIC_CONCURRENCY

    def test_from_json_ignores_unknown_scale_metric(self):
        # An older client parsing a newer backend's config must not crash (runtime images bundle the client).
        with pytest.warns(UserWarning, match="unknown scale metric"):
            sc = PredictorScalingConfig.from_json(
                {
                    "predictor_scaling_config": {
                        "min_instances": 1,
                        "max_instances": 3,
                        "scale_metric": "GPU_UTIL",
                        "target": 80,
                    }
                }
            )
        assert sc.scale_metric is None
        assert sc.target == 80

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

    def test_log_persistence_survives_a_read_modify_write(self):
        # Reading a deployment and saving it back must not reset the stored choice: without
        # round-tripping the field, an SDK update would silently re-apply the backend default.
        json_payload = {
            "predictor_scaling_config": {
                "min_instances": 1,
                "log_persistence": "NONE",
            }
        }

        sc = PredictorScalingConfig.from_json(json_payload)

        assert sc.log_persistence == LogPersistence.NONE
        assert sc.to_json()["log_persistence"] == "NONE"
        assert sc.to_dict()["predictorScalingConfig"]["logPersistence"] == "NONE"

    def test_log_persistence_accepts_a_string_and_is_settable(self):
        sc = TransformerScalingConfig(min_instances=1, log_persistence="all_replicas")
        assert sc.log_persistence == LogPersistence.ALL_REPLICAS

        sc.log_persistence = "none"
        assert sc.log_persistence == LogPersistence.NONE

    def test_log_persistence_omitted_when_unset(self):
        # Unset must stay unset on the wire, so the backend applies its own default rather
        # than the client asserting one.
        sc = PredictorScalingConfig(min_instances=1)
        assert sc.log_persistence is None
        assert "log_persistence" not in sc.to_json()

    def test_log_persistence_invalid_value_raises(self):
        with pytest.raises(ValueError) as exc_info:
            PredictorScalingConfig(min_instances=1, log_persistence="ONE_REPLICA")

        assert "Invalid log_persistence" in str(exc_info.value)

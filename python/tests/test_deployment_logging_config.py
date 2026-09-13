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
from hsml.deployment.logging_config import DeploymentLoggingConfig


class TestDeploymentLoggingConfig:
    def test_empty_config_means_platform_defaults(self):
        config = DeploymentLoggingConfig()
        assert config.flush_interval_seconds is None
        assert config.sidecar_cpu is None
        # Nothing set, nothing on the wire: the backend applies its settings.
        assert config.to_dict() == {}

    def test_round_trip_only_set_fields(self):
        config = DeploymentLoggingConfig(
            flush_interval_seconds=600,
            flush_bytes=4 * 1024 * 1024,
            batch_rows=128,
            sidecar_cpu=0.5,
            sidecar_memory_mb=1024,
        )
        wire = config.to_dict()
        assert wire == {
            "flushIntervalSeconds": 600,
            "flushBytes": 4 * 1024 * 1024,
            "batchRows": 128,
            "sidecarCpu": 0.5,
            "sidecarMemoryMb": 1024,
        }
        restored = DeploymentLoggingConfig.from_response_json(wire)
        assert restored.flush_interval_seconds == 600
        assert restored.flush_bytes == 4 * 1024 * 1024
        assert restored.batch_rows == 128
        assert restored.sidecar_cpu == 0.5
        assert restored.sidecar_memory_mb == 1024
        assert restored.max_buffer_bytes is None

    def test_from_json_accepts_nested_keys(self):
        for wrapper in ("featureLogging", "featureLoggingConfig"):
            restored = DeploymentLoggingConfig.from_response_json(
                {wrapper: {"queueSize": 2000, "batchSeconds": 2}}
            )
            assert restored.queue_size == 2000
            assert restored.batch_seconds == 2

    def test_from_json_ignores_unknown_fields(self):
        restored = DeploymentLoggingConfig.from_response_json(
            {"flushIntervalSeconds": 60, "somethingNew": 1}
        )
        assert restored.flush_interval_seconds == 60

    def test_rejects_non_positive_and_non_integer_values(self):
        with pytest.raises(ValueError, match="flush_interval_seconds"):
            DeploymentLoggingConfig(flush_interval_seconds=0)
        with pytest.raises(ValueError, match="batch_rows"):
            DeploymentLoggingConfig(batch_rows=-1)
        with pytest.raises(ValueError, match="queue_size"):
            DeploymentLoggingConfig(queue_size=2.5)
        with pytest.raises(ValueError, match="shutdown_seconds"):
            DeploymentLoggingConfig(shutdown_seconds=True)
        with pytest.raises(ValueError, match="sidecar_cpu"):
            DeploymentLoggingConfig(sidecar_cpu=0)

    def test_rejects_inconsistent_bounds(self):
        with pytest.raises(ValueError, match="flush_bytes"):
            DeploymentLoggingConfig(flush_bytes=100, max_buffer_bytes=50)
        with pytest.raises(ValueError, match="max_event_bytes"):
            DeploymentLoggingConfig(max_event_bytes=100, max_buffer_bytes=50)
        with pytest.raises(ValueError, match="batch_rows"):
            DeploymentLoggingConfig(batch_rows=500, queue_size=100)
        # One side unset is fine: the platform default fills it in.
        DeploymentLoggingConfig(flush_bytes=100)
        DeploymentLoggingConfig(batch_rows=500)

    def test_setters_validate_and_keep_previous_value_on_error(self):
        config = DeploymentLoggingConfig(flush_bytes=1024, max_buffer_bytes=4096)
        config.flush_interval_seconds = 60
        assert config.flush_interval_seconds == 60
        with pytest.raises(ValueError):
            config.flush_bytes = 8192
        assert config.flush_bytes == 1024
        with pytest.raises(ValueError):
            config.sidecar_memory_mb = 0
        assert config.sidecar_memory_mb is None
        config.flush_bytes = None
        assert config.to_dict() == {"maxBufferBytes": 4096, "flushIntervalSeconds": 60}

    def test_update_from_response_json_replaces_all_fields(self):
        config = DeploymentLoggingConfig(flush_interval_seconds=60, batch_rows=10)
        config.update_from_response_json({"batchSeconds": 1})
        assert config.batch_seconds == 1
        assert config.flush_interval_seconds is None
        assert config.batch_rows is None

    def test_repr_lists_every_field(self):
        text = repr(DeploymentLoggingConfig(queue_size=5))
        assert text.startswith("DeploymentLoggingConfig(")
        assert "queue_size: 5" in text
        assert "sidecar_cpu: None" in text


class TestFeatureLoggingMarker:
    """The view owns the transport; the deployment only carries it."""

    def _view(self, transport):
        from types import SimpleNamespace

        return SimpleNamespace(
            name="fv",
            version=2,
            logging_enabled=True,
            feature_logging=SimpleNamespace(transport=transport),
        )

    def test_the_marker_takes_the_views_transport(self):
        from hsml.deployment.predictor import _mark_feature_logging

        kwargs = {"feature_logging": DeploymentLoggingConfig(batch_seconds=2)}
        _mark_feature_logging(kwargs, self._view("job"))
        assert kwargs["env_vars"] == {"SERVING_FEATURE_LOGGING": "job"}
        assert kwargs["feature_logging"].transport == "job"

        as_dict = {"feature_logging": {"batch_seconds": 2}, "env_vars": {"A": "1"}}
        _mark_feature_logging(as_dict, self._view("realtime"))
        assert as_dict["env_vars"] == {"A": "1", "SERVING_FEATURE_LOGGING": "realtime"}
        assert as_dict["feature_logging"]["transport"] == "realtime"

    def test_a_conflicting_deployment_transport_is_refused(self):
        from hsml.deployment.predictor import _mark_feature_logging

        kwargs = {"feature_logging": DeploymentLoggingConfig(transport="job")}
        with pytest.raises(ValueError, match="one transport"):
            _mark_feature_logging(kwargs, self._view("realtime"))
        with pytest.raises(ValueError, match="one transport"):
            _mark_feature_logging(
                {"feature_logging": {"transport": "realtime"}}, self._view("job")
            )

    def test_a_view_without_logging_gets_no_marker(self):
        from types import SimpleNamespace

        from hsml.deployment.predictor import _mark_feature_logging

        kwargs = {}
        _mark_feature_logging(kwargs, SimpleNamespace(logging_enabled=False))
        assert kwargs == {}
        _mark_feature_logging(kwargs, None)
        assert kwargs == {}

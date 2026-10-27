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
from hsml.deployment_tracing_config import DeploymentTracingConfig


class TestDeploymentTracingConfig:
    def test_defaults_to_online(self):
        config = DeploymentTracingConfig()

        assert config.enabled is None
        assert config.otel_tracing_storage == DeploymentTracingConfig.STORAGE_ONLINE
        assert config.to_dict() == {"otelTracingStorage": "online"}

    def test_round_trip(self):
        config = DeploymentTracingConfig(
            enabled=True,
            otel_tracing_storage=DeploymentTracingConfig.STORAGE_BOTH,
        )

        restored = DeploymentTracingConfig.from_response_json(config.to_dict())

        assert restored.enabled is True
        assert restored.otel_tracing_storage == DeploymentTracingConfig.STORAGE_BOTH

    def test_round_trip_with_cost_prices(self):
        config = DeploymentTracingConfig(
            enabled=True,
            otel_tracing_storage=DeploymentTracingConfig.STORAGE_OFFLINE,
            input_token_price_per_million_tokens=1.25,
            output_token_price_per_million_tokens=2.5,
        )

        restored = DeploymentTracingConfig.from_response_json(config.to_dict())

        assert restored.enabled is True
        assert restored.otel_tracing_storage == DeploymentTracingConfig.STORAGE_OFFLINE
        assert restored.input_token_price_per_million_tokens == 1.25
        assert restored.output_token_price_per_million_tokens == 2.5

    def test_rejects_invalid_storage(self):
        with pytest.raises(ValueError, match="Possible values"):
            DeploymentTracingConfig(otel_tracing_storage="invalid")

    def test_round_trip_with_sampling_percentage(self):
        config = DeploymentTracingConfig(enabled=True, sampling_percentage=25)

        assert config.to_dict()["samplingPercentage"] == 25
        restored = DeploymentTracingConfig.from_response_json(config.to_dict())

        assert restored.sampling_percentage == 25

    @pytest.mark.parametrize("percentage", [-1, 101])
    def test_rejects_sampling_percentage_out_of_range(self, percentage):
        with pytest.raises(ValueError, match="between 0 and 100"):
            DeploymentTracingConfig(sampling_percentage=percentage)

    def test_reads_a_stored_sampling_percentage_out_of_range(self):
        # Agent deployments stored any integer before the range check.
        config = DeploymentTracingConfig.from_response_json(
            {"enabled": True, "samplingPercentage": 150, "active": False}
        )

        assert config.sampling_percentage == 150
        assert "active: False" in repr(config)

    def test_a_pause_is_read_and_sent_back(self):
        config = DeploymentTracingConfig.from_response_json(
            {
                "enabled": True,
                "samplingPercentage": 10,
                "pausedReason": "DISK_NEARLY_FULL",
            }
        )

        assert config.paused_reason == "DISK_NEARLY_FULL"
        assert config.to_dict()["pausedReason"] == "DISK_NEARLY_FULL"
        assert "pausedReason" not in DeploymentTracingConfig(enabled=True).to_dict()

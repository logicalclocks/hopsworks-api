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

    def test_rejects_invalid_storage(self):
        with pytest.raises(ValueError, match="Possible values"):
            DeploymentTracingConfig(otel_tracing_storage="invalid")

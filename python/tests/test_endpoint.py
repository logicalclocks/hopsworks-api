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

import copy

import pytest
from hsml import resources
from hsml.constants import PREDICTOR
from hsml.python.endpoint import Endpoint
from hsml.scaling_config import PredictorScalingConfig, TransformerScalingConfig


SERVING_NUM_INSTANCES_NO_LIMIT = [-1]


class TestEndpoint:
    # constructor

    def test_constructor_valid(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]
        mock_validate_serving_tool = mocker.patch(
            "hsml.predictor.Predictor._validate_serving_tool",
            return_value=p_json["serving_tool"],
        )
        mock_resources = mocker.MagicMock(spec=resources.PredictorResources)
        mock_validate_resources = mocker.patch(
            "hsml.predictor.Predictor._validate_resources",
            return_value=mock_resources,
        )
        mock_validate_script_file = mocker.patch(
            "hsml.predictor.Predictor._validate_script_file",
            return_value=p_json["predictor"],
        )

        # Act
        e = Endpoint(
            name=p_json["name"],
            script_file=p_json["predictor"],
            description=p_json["description"],
            resources=p_json["predictor_resources"],
        )

        # Assert
        assert e.name == p_json["name"]
        assert e.script_file == p_json["predictor"]
        assert e.description == p_json["description"]
        assert e.model_server == PREDICTOR.MODEL_SERVER_PYTHON
        assert isinstance(e.resources, resources.PredictorResources)
        mock_validate_serving_tool.assert_called_once()
        assert mock_validate_resources.call_count == 1
        mock_validate_script_file.assert_called_once()

        assert e.scaling_configuration is not None
        assert isinstance(e.scaling_configuration, PredictorScalingConfig)
        assert (
            e.scaling_configuration.scale_metric.value == "CONCURRENCY"
        )  # the default

    def test_constructor_with_all_parameters(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]
        mock_validate_serving_tool = mocker.patch(
            "hsml.predictor.Predictor._validate_serving_tool",
            return_value=p_json["serving_tool"],
        )
        mock_resources = mocker.MagicMock(spec=resources.PredictorResources)
        mock_validate_resources = mocker.patch(
            "hsml.predictor.Predictor._validate_resources",
            return_value=mock_resources,
        )
        mock_validate_script_file = mocker.patch(
            "hsml.predictor.Predictor._validate_script_file",
            return_value=p_json["predictor"],
        )

        # Act
        e = Endpoint(
            name=p_json["name"],
            script_file=p_json["predictor"],
            description=p_json["description"],
            version=p_json["version"],
            serving_tool=p_json["serving_tool"],
            api_protocol=p_json["api_protocol"],
            environment=p_json["environment_dto"]["name"],
            config_file=p_json["config_file"],
            resources=p_json["predictor_resources"],
            transformer={
                "script_file": p_json["transformer"],
                "resources": copy.deepcopy(p_json["transformer_resources"]),
                "scaling_configuration": copy.deepcopy(
                    p_json["transformer_scaling_config"]
                ),
            },
            inference_logger={
                "mode": p_json["inference_logging"],
                "kafka_topic": copy.deepcopy(p_json["kafka_topic_dto"]),
            },
            inference_batcher=copy.deepcopy(p_json["batching_configuration"]),
            scaling_configuration=copy.deepcopy(p_json["predictor_scaling_config"]),
        )

        # Assert
        assert e.name == p_json["name"]
        assert e.script_file == p_json["predictor"]
        assert e.description == p_json["description"]
        assert e.version == p_json["version"]
        assert e.model_server == PREDICTOR.MODEL_SERVER_PYTHON
        assert e.serving_tool == p_json["serving_tool"]
        assert e.api_protocol == p_json["api_protocol"]
        assert e.environment == p_json["environment_dto"]["name"]
        assert e.config_file == p_json["config_file"]
        assert isinstance(e.resources, resources.PredictorResources)
        mock_validate_serving_tool.assert_called_once_with(p_json["serving_tool"])
        assert mock_validate_resources.call_count == 1
        mock_validate_script_file.assert_called_once()

        # predictor scaling config
        assert e.scaling_configuration is not None
        assert isinstance(e.scaling_configuration, PredictorScalingConfig)
        assert (
            e.scaling_configuration.scale_metric.value
            == p_json["predictor_scaling_config"]["scale_metric"]
        )
        # transformer scaling config
        assert e.transformer is not None
        assert isinstance(e.transformer.scaling_configuration, TransformerScalingConfig)
        assert (
            e.transformer.scaling_configuration.scale_metric.value
            == p_json["transformer_scaling_config"]["scale_metric"]
        )

    def test_constructor_name_none(self, mocker):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)

        # Act & Assert
        with pytest.raises(ValueError) as e_info:
            _ = Endpoint(name=None, script_file="script.py")

        assert "A deployment name must be provided" in str(e_info.value)

    def test_constructor_script_file_none(self, mocker):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)

        # Act & Assert
        with pytest.raises(ValueError) as e_info:
            _ = Endpoint(name="test_endpoint", script_file=None)

        assert "Python scripts are required in deployments without models" in str(
            e_info.value
        )

    def test_constructor_name_and_script_file_none(self, mocker):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)

        # Act & Assert
        with pytest.raises(ValueError) as e_info:
            _ = Endpoint(name=None, script_file=None)

        assert "A deployment name must be provided" in str(e_info.value)

    def test_constructor_minimal_parameters(self, mocker):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        mock_validate_serving_tool = mocker.patch(
            "hsml.predictor.Predictor._validate_serving_tool",
            return_value=None,
        )
        mock_validate_resources = mocker.patch(
            "hsml.predictor.Predictor._validate_resources",
            return_value=None,
        )
        mock_validate_script_file = mocker.patch(
            "hsml.predictor.Predictor._validate_script_file",
            return_value="script.py",
        )

        # Act
        e = Endpoint(name="test_endpoint", script_file="script.py")

        # Assert
        assert e.name == "test_endpoint"
        assert e.script_file == "script.py"
        assert e.model_server == PREDICTOR.MODEL_SERVER_PYTHON
        mock_validate_serving_tool.assert_called_once()
        assert mock_validate_resources.call_count == 1
        mock_validate_script_file.assert_called_once()

    def test_constructor_model_server_forced_to_python(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]
        mock_validate_serving_tool = mocker.patch(
            "hsml.predictor.Predictor._validate_serving_tool",
            return_value=p_json["serving_tool"],
        )
        mock_validate_resources = mocker.patch(
            "hsml.predictor.Predictor._validate_resources",
            return_value=None,
        )
        mock_validate_script_file = mocker.patch(
            "hsml.predictor.Predictor._validate_script_file",
            return_value=p_json["predictor"],
        )

        # Act - Try to pass a different model_server, should be overridden
        e = Endpoint(
            name=p_json["name"],
            script_file=p_json["predictor"],
            model_server="TENSORFLOW",  # This should be overridden
        )

        # Assert
        assert e.model_server == PREDICTOR.MODEL_SERVER_PYTHON
        mock_validate_serving_tool.assert_called_once()
        assert mock_validate_resources.call_count == 1
        mock_validate_script_file.assert_called_once()

    # auxiliary methods

    def _mock_serving_variables(
        self,
        mocker,
        num_instances,
        force_scale_to_zero=False,
        is_saas_connection=False,
        is_kserve_installed=True,
    ):
        mocker.patch(
            "hopsworks_common.client.get_serving_num_instances_limits",
            return_value=num_instances,
        )
        mocker.patch(
            "hopsworks_common.client.is_scale_to_zero_required",
            return_value=force_scale_to_zero,
        )
        mocker.patch(
            "hopsworks_common.client.is_saas_connection",
            return_value=is_saas_connection,
        )
        mocker.patch(
            "hopsworks_common.client.is_kserve_installed",
            return_value=is_kserve_installed,
        )

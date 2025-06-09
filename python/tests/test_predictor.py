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

import copy

import pytest
from hsml import (
    inference_batcher,
    inference_logger,
    predictor,
    resources,
    transformer,
    util,
)
from hsml.constants import MODEL, PREDICTOR, RESOURCES


SERVING_NUM_INSTANCES_NO_LIMIT = [-1]
SERVING_NUM_INSTANCES_SCALE_TO_ZERO = [0]
SERVING_NUM_INSTANCES_ONE = [0]


class TestPredictor:
    # from response json

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["predictor"]["get_deployments_empty"]["response"]

        # Act
        pred = predictor.Predictor.from_response_json(json)

        # Assert
        assert isinstance(pred, list)
        assert len(pred) == 0

    def test_from_response_json_singleton(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"]

        # Act
        pred = predictor.Predictor.from_response_json(json)

        # Assert
        assert isinstance(pred, list)
        assert len(pred) == 1

        p = pred[0]
        p_json = json["items"][0]

        assert isinstance(p, predictor.Predictor)
        assert p.id == p_json["id"]
        assert p.name == p_json["name"]
        assert p.description == p_json["description"]
        assert p.created_at == p_json["created"]
        assert p.creator == p_json["creator"]
        assert p.model_path == p_json["model_path"]
        assert p.model_name == p_json["model_name"]
        assert p.model_version == p_json["model_version"]
        assert p.model_framework == p_json["model_framework"]
        assert p.model_server == p_json["model_server"]
        assert p.serving_tool == p_json["serving_tool"]
        assert p.api_protocol == p_json["api_protocol"]
        assert p.artifact_version == p_json["artifact_version"]
        assert p.environment == p_json["environment_dto"]["name"]
        assert p.script_file == p_json["predictor"]
        assert p.config_file == p_json["config_file"]
        assert isinstance(p.resources, resources.PredictorResources)
        assert isinstance(p.transformer, transformer.Transformer)
        assert p.transformer.script_file == p_json["transformer"]
        assert isinstance(p.transformer.resources, resources.TransformerResources)
        assert isinstance(p.inference_logger, inference_logger.InferenceLogger)
        assert p.inference_logger.mode == p_json["inference_logging"]
        assert isinstance(p.inference_batcher, inference_batcher.InferenceBatcher)
        assert p.inference_batcher.enabled == bool(
            p_json["batching_configuration"]["batching_enabled"]
        )

    def test_from_response_json_list(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["predictor"]["get_deployments_list"]["response"]

        # Act
        pred = predictor.Predictor.from_response_json(json)

        # Assert
        assert isinstance(pred, list)
        assert len(pred) == 2

        for i in range(len(pred)):
            p = pred[i]
            p_json = json["items"][i]

            assert isinstance(p, predictor.Predictor)
            assert p.id == p_json["id"]
            assert p.name == p_json["name"]
            assert p.description == p_json["description"]
            assert p.created_at == p_json["created"]
            assert p.creator == p_json["creator"]
            assert p.model_path == p_json["model_path"]
            assert p.model_name == p_json["model_name"]
            assert p.model_version == p_json["model_version"]
            assert p.model_framework == p_json["model_framework"]
            assert p.model_server == p_json["model_server"]
            assert p.serving_tool == p_json["serving_tool"]
            assert p.api_protocol == p_json["api_protocol"]
            assert p.environment == p_json["environment_dto"]["name"]
            assert p.artifact_version == p_json["artifact_version"]
            assert p.script_file == p_json["predictor"]
            assert p.config_file == p_json["config_file"]
            assert isinstance(p.resources, resources.PredictorResources)
            assert isinstance(p.transformer, transformer.Transformer)
            assert p.transformer.script_file == p_json["transformer"]
            assert isinstance(p.transformer.resources, resources.TransformerResources)
            assert isinstance(p.inference_logger, inference_logger.InferenceLogger)
            assert p.inference_logger.mode == p_json["inference_logging"]
            assert isinstance(p.inference_batcher, inference_batcher.InferenceBatcher)
            assert p.inference_batcher.enabled == bool(
                p_json["batching_configuration"]["batching_enabled"]
            )

    def test_from_response_json_single(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]

        # Act
        p = predictor.Predictor.from_response_json(p_json)

        # Assert
        assert isinstance(p, predictor.Predictor)
        assert p.id == p_json["id"]
        assert p.name == p_json["name"]
        assert p.description == p_json["description"]
        assert p.created_at == p_json["created"]
        assert p.creator == p_json["creator"]
        assert p.model_path == p_json["model_path"]
        assert p.model_version == p_json["model_version"]
        assert p.model_name == p_json["model_name"]
        assert p.model_framework == p_json["model_framework"]
        assert p.model_server == p_json["model_server"]
        assert p.serving_tool == p_json["serving_tool"]
        assert p.api_protocol == p_json["api_protocol"]
        assert p.environment == p_json["environment_dto"]["name"]
        assert p.artifact_version == p_json["artifact_version"]
        assert p.script_file == p_json["predictor"]
        assert p.config_file == p_json["config_file"]
        assert isinstance(p.resources, resources.PredictorResources)
        assert isinstance(p.transformer, transformer.Transformer)
        assert p.transformer.script_file == p_json["transformer"]
        assert isinstance(p.transformer.resources, resources.TransformerResources)
        assert isinstance(p.inference_logger, inference_logger.InferenceLogger)
        assert p.inference_logger.mode == p_json["inference_logging"]
        assert isinstance(p.inference_batcher, inference_batcher.InferenceBatcher)
        assert p.inference_batcher.enabled == bool(
            p_json["batching_configuration"]["batching_enabled"]
        )

    # constructor

    def test_constructor(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]
        mock_validate_serving_tool = mocker.patch(
            "hsml.predictor.Predictor._validate_serving_tool",
            return_value=p_json["serving_tool"],
        )
        mock_resources = util.get_obj_from_json(
            copy.deepcopy(p_json["predictor_resources"]), resources.PredictorResources
        )
        mock_validate_resources = mocker.patch(
            "hsml.predictor.Predictor._validate_resources",
            return_value=mock_resources,
        )
        mock_validate_script_file = mocker.patch(
            "hsml.predictor.Predictor._validate_script_file",
            return_value=p_json["predictor"],
        )

        # Act
        p = predictor.Predictor(
            id=p_json["id"],
            name=p_json["name"],
            description=p_json["description"],
            created_at=p_json["created"],
            creator=p_json["creator"],
            model_path=p_json["model_path"],
            model_version=p_json["model_version"],
            model_name=p_json["model_name"],
            model_framework=p_json["model_framework"],
            model_server=p_json["model_server"],
            serving_tool=p_json["serving_tool"],
            api_protocol=p_json["api_protocol"],
            environment=p_json["environment_dto"]["name"],
            artifact_version=p_json["artifact_version"],
            script_file=p_json["predictor"],
            config_file=p_json["config_file"],
            resources=p_json["predictor_resources"],
            transformer={
                "script_file": p_json["transformer"],
                "resources": copy.deepcopy(p_json["transformer_resources"]),
            },
            inference_logger={
                "mode": p_json["inference_logging"],
                "kafka_topic": copy.deepcopy(p_json["kafka_topic_dto"]),
            },
            inference_batcher=copy.deepcopy(p_json["batching_configuration"]),
        )

        # Assert
        assert p.id == p_json["id"]
        assert p.name == p_json["name"]
        assert p.description == p_json["description"]
        assert p.created_at == p_json["created"]
        assert p.creator == p_json["creator"]
        assert p.model_path == p_json["model_path"]
        assert p.model_name == p_json["model_name"]
        assert p.model_version == p_json["model_version"]
        assert p.model_framework == p_json["model_framework"]
        assert p.model_server == p_json["model_server"]
        assert p.serving_tool == p_json["serving_tool"]
        assert p.api_protocol == p_json["api_protocol"]
        assert p.environment == p_json["environment_dto"]["name"]
        assert p.artifact_version == p_json["artifact_version"]
        assert p.script_file == p_json["predictor"]
        assert p.config_file == p_json["config_file"]
        assert isinstance(p.resources, resources.PredictorResources)
        assert isinstance(p.transformer, transformer.Transformer)
        assert p.transformer.script_file == p_json["transformer"]
        assert isinstance(p.transformer.resources, resources.TransformerResources)
        assert isinstance(p.inference_logger, inference_logger.InferenceLogger)
        assert p.inference_logger.mode == p_json["inference_logging"]
        assert isinstance(p.inference_batcher, inference_batcher.InferenceBatcher)
        assert p.inference_batcher.enabled == bool(
            p_json["batching_configuration"]["batching_enabled"]
        )
        mock_validate_serving_tool.assert_called_once_with(p_json["serving_tool"])
        assert mock_validate_resources.call_count == 1
        mock_validate_script_file.assert_called_once_with(
            p_json["model_framework"], p_json["predictor"]
        )

    # validate serving tool

    def test_validate_serving_tool_none(self):
        # Act
        st = predictor.Predictor._validate_serving_tool(None)

        # Assert
        assert st is None

    def test_validate_serving_tool_valid(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, is_saas_connection=False
        )
        mocker.patch("hopsworks_common.client.get_instance")

        # Act
        st = predictor.Predictor._validate_serving_tool(PREDICTOR.SERVING_TOOL_DEFAULT)

        # Assert
        assert st == PREDICTOR.SERVING_TOOL_DEFAULT

    def test_validate_serving_tool_invalid(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, is_saas_connection=False
        )
        mocker.patch("hopsworks_common.client.get_instance")

        # Act
        with pytest.raises(ValueError) as e_info:
            _ = predictor.Predictor._validate_serving_tool("INVALID_NAME")

        # Assert
        assert "is not valid" in str(e_info.value)

    def test_validate_serving_tool_valid_saas_connection(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, is_saas_connection=True
        )
        mocker.patch("hopsworks_common.client.get_instance")

        # Act
        st = predictor.Predictor._validate_serving_tool(PREDICTOR.SERVING_TOOL_KSERVE)

        # Assert
        assert st == PREDICTOR.SERVING_TOOL_KSERVE

    def test_validate_serving_tool_invalid_saas_connection(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, is_saas_connection=True
        )
        mocker.patch("hopsworks_common.client.get_instance")

        # Act
        with pytest.raises(ValueError) as e_info:
            _ = predictor.Predictor._validate_serving_tool(
                PREDICTOR.SERVING_TOOL_DEFAULT
            )

        # Assert
        assert "KServe deployments are the only supported" in str(e_info.value)

    # validate script file

    def test_validate_script_file_tf_none(self):
        # Act
        predictor.Predictor._validate_script_file(MODEL.FRAMEWORK_TENSORFLOW, None)

    def test_validate_script_file_sk_none(self):
        # Act
        predictor.Predictor._validate_script_file(MODEL.FRAMEWORK_SKLEARN, None)

    def test_validate_script_file_th_none(self):
        # Act
        predictor.Predictor._validate_script_file(MODEL.FRAMEWORK_TORCH, None)

    def test_validate_script_file_py_none(self):
        # Act
        with pytest.raises(ValueError) as e_info:
            _ = predictor.Predictor._validate_script_file(MODEL.FRAMEWORK_PYTHON, None)

        # Assert
        assert "Predictor scripts are required" in str(e_info.value)

    def test_validate_script_file_tf_script_file(self):
        # Act
        predictor.Predictor._validate_script_file(
            MODEL.FRAMEWORK_TENSORFLOW, "script_file"
        )

    def test_validate_script_file_sk_script_file(self):
        # Act
        predictor.Predictor._validate_script_file(
            MODEL.FRAMEWORK_SKLEARN, "script_file"
        )

    def test_validate_script_file_th_script_file(self):
        # Act
        predictor.Predictor._validate_script_file(MODEL.FRAMEWORK_TORCH, "script_file")

    def test_validate_script_file_py_script_file(self):
        # Act
        predictor.Predictor._validate_script_file(MODEL.FRAMEWORK_PYTHON, "script_file")

    def test_validate_script_file_llm_script_file(self):
        # Act
        predictor.Predictor._validate_script_file(MODEL.FRAMEWORK_LLM, "script_file")

    # infer model server

    def test_infer_model_server_tf(self):
        # Act
        ms = predictor.Predictor._infer_model_server(MODEL.FRAMEWORK_TENSORFLOW)

        # Assert
        assert ms == PREDICTOR.MODEL_SERVER_TF_SERVING

    def test_infer_model_server_sk(self):
        # Act
        ms = predictor.Predictor._infer_model_server(MODEL.FRAMEWORK_SKLEARN)

        # Assert
        assert ms == PREDICTOR.MODEL_SERVER_PYTHON

    def test_infer_model_server_th(self):
        # Act
        ms = predictor.Predictor._infer_model_server(MODEL.FRAMEWORK_TORCH)

        # Assert
        assert ms == PREDICTOR.MODEL_SERVER_PYTHON

    def test_infer_model_server_py(self):
        # Act
        ms = predictor.Predictor._infer_model_server(MODEL.FRAMEWORK_PYTHON)

        # Assert
        assert ms == PREDICTOR.MODEL_SERVER_PYTHON

    def test_infer_model_server_llm(self):
        # Act
        ms = predictor.Predictor._infer_model_server(MODEL.FRAMEWORK_LLM)

        # Assert
        assert ms == PREDICTOR.MODEL_SERVER_VLLM

    # default serving tool

    def test_get_default_serving_tool_kserve_installed(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, is_kserve_installed=True
        )

        # Act
        st = predictor.Predictor._get_default_serving_tool()

        # Assert
        assert st == PREDICTOR.SERVING_TOOL_KSERVE

    def test_get_default_serving_tool_kserve_not_installed(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, is_kserve_installed=False
        )

        # Act
        st = predictor.Predictor._get_default_serving_tool()

        # Assert
        assert st == PREDICTOR.SERVING_TOOL_DEFAULT

    # validate resources

    def test_validate_resources_none_non_kserve(self):
        # Act
        res = predictor.Predictor._validate_resources(
            None, PREDICTOR.SERVING_TOOL_DEFAULT
        )

        # Assert
        assert res is None

    def test_validate_resources_none_kserve(self):
        # Act
        res = predictor.Predictor._validate_resources(
            None, PREDICTOR.SERVING_TOOL_KSERVE
        )

        # Assert
        assert res is None

    def test_validate_resources_num_instances_zero_non_kserve(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        pr = resources.PredictorResources(num_instances=0)

        # Act
        res = predictor.Predictor._validate_resources(
            pr, PREDICTOR.SERVING_TOOL_DEFAULT
        )

        # Assert
        assert res == pr

    def test_validate_resources_num_instances_zero_kserve(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        pr = resources.PredictorResources(num_instances=0)

        # Act
        res = predictor.Predictor._validate_resources(pr, PREDICTOR.SERVING_TOOL_KSERVE)

        # Assert
        assert res == pr

    def test_validate_resources_num_instances_one_without_scale_to_zero_non_kserve(
        self, mocker
    ):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        pr = resources.PredictorResources(num_instances=1)

        # Act
        res = predictor.Predictor._validate_resources(
            pr, PREDICTOR.SERVING_TOOL_DEFAULT
        )

        # Assert
        assert res == pr

    def test_validate_resources_num_instances_one_without_scale_to_zero_kserve(
        self, mocker
    ):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        pr = resources.PredictorResources(num_instances=1)

        # Act
        res = predictor.Predictor._validate_resources(pr, PREDICTOR.SERVING_TOOL_KSERVE)

        # Assert
        assert res == pr

    def test_validate_resources_num_instances_one_with_scale_to_zero_non_kserve(
        self, mocker
    ):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        pr = resources.PredictorResources(num_instances=1)

        # Act
        res = predictor.Predictor._validate_resources(
            pr, PREDICTOR.SERVING_TOOL_DEFAULT
        )

        # Assert
        assert res == pr

    def test_validate_resources_num_instances_one_with_scale_to_zero_kserve(
        self, mocker
    ):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        pr = resources.PredictorResources(num_instances=1)

        # Act
        with pytest.raises(ValueError) as e_info:
            _ = predictor.Predictor._validate_resources(
                pr, PREDICTOR.SERVING_TOOL_KSERVE
            )

        # Assert
        assert "Scale-to-zero is required" in str(e_info.value)

    # default resources

    def test_get_default_resources_non_kserve_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )

        # Act
        res = predictor.Predictor._get_default_resources(PREDICTOR.SERVING_TOOL_DEFAULT)

        # Assert
        assert isinstance(res, resources.PredictorResources)
        assert res.num_instances == RESOURCES.MIN_NUM_INSTANCES

    def test_get_default_resources_non_kserve_with_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        # Act
        res = predictor.Predictor._get_default_resources(PREDICTOR.SERVING_TOOL_DEFAULT)

        # Assert
        assert isinstance(res, resources.PredictorResources)
        assert res.num_instances == RESOURCES.MIN_NUM_INSTANCES

    def test_get_default_resources_kserve_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )

        # Act
        res = predictor.Predictor._get_default_resources(PREDICTOR.SERVING_TOOL_KSERVE)

        # Assert
        assert isinstance(res, resources.PredictorResources)
        assert res.num_instances == RESOURCES.MIN_NUM_INSTANCES

    def test_get_default_resources_kserve_with_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        # Act
        res = predictor.Predictor._get_default_resources(PREDICTOR.SERVING_TOOL_KSERVE)

        # Assert
        assert isinstance(res, resources.PredictorResources)
        assert res.num_instances == 0

    # for model

    def test_for_model(self, mocker):
        # Arrange
        def spec(model, model_name, model_version, model_path):
            pass

        mock_get_predictor_for_model = mocker.patch(
            "hopsworks_common.util.get_predictor_for_model",
            return_value=True,
            spec=spec,
        )

        class MockModel:
            name = "model_name"
            version = "model_version"
            model_path = "model_path"

        mock_model = MockModel()

        # Act
        predictor.Predictor.for_model(mock_model)

        # Assert
        mock_get_predictor_for_model.assert_called_once_with(
            model=mock_model,
            model_name=mock_model.name,
            model_version=mock_model.version,
            model_path=mock_model.model_path,
        )

    # extract fields from json

    def extract_fields_from_json(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]

        # Act
        kwargs = predictor.Predictor.extract_fields_from_json(p_json)

        # Assert
        assert kwargs["id"] == p_json["id"]
        assert kwargs["name"] == p_json["name"]
        assert kwargs["description"] == p_json["description"]
        assert kwargs["created_at"] == p_json["created"]
        assert kwargs["creator"] == p_json["creator"]
        assert kwargs["model_name"] == p_json["model_name"]
        assert kwargs["model_path"] == p_json["model_path"]
        assert kwargs["model_version"] == p_json["model_version"]
        assert kwargs["model_framework"] == p_json["model_framework"]
        assert kwargs["artifact_version"] == p_json["artifact_version"]
        assert kwargs["model_server"] == p_json["model_server"]
        assert kwargs["serving_tool"] == p_json["serving_tool"]
        assert kwargs["script_file"] == p_json["predictor"]
        assert kwargs["config_file"] == p_json["config_file"]
        assert isinstance(kwargs["resources"], resources.PredictorResources)
        assert isinstance(kwargs["inference_logger"], inference_logger.InferenceLogger)
        assert kwargs["inference_logger"].mode == p_json["inference_logging"]
        assert isinstance(
            kwargs["inference_batcher"], inference_batcher.InferenceBatcher
        )
        assert kwargs["inference_batcher"].enabled == bool(
            p_json["batching_configuration"]["batching_enabled"]
        )
        assert kwargs["api_protocol"] == p_json["api_protocol"]
        assert kwargs["environment"] == p_json["environment_dto"]["name"]
        assert isinstance(kwargs["transformer"], transformer.Transformer)
        assert kwargs["transformer"].script_file == p_json["transformer"]
        assert isinstance(
            kwargs["transformer"].resources, resources.TransformerResources
        )

    # deploy

    def test_deploy(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]
        mock_deployment_init = mocker.patch(
            "hsml.deployment.Deployment.__init__", return_value=None
        )
        mock_deployment_save = mocker.patch("hsml.deployment.Deployment.save")

        # Act

        p = predictor.Predictor.from_response_json(p_json)
        p.deploy()

        # Assert
        mock_deployment_init.assert_called_once_with(
            predictor=p,
            name=p.name,
            description=p.description,
        )
        mock_deployment_save.assert_called_once()

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

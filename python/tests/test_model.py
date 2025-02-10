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
import os

import humps
from hsml import model
from hsml.constants import MODEL
from hsml.core import explicit_provenance


class TestModel:
    # from response json

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_empty"]["response"]

        # Act
        m_lst = model.Model.from_response_json(json)

        # Assert
        assert isinstance(m_lst, list)
        assert len(m_lst) == 0

    def test_from_response_json_singleton(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_python"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend

        # Act
        m = model.Model.from_response_json(copy.deepcopy(json_camelized))

        # Assert
        assert isinstance(m, list)
        assert len(m) == 1

        m = m[0]
        m_json = json["items"][0]

        self.assert_model(mocker, m, m_json, MODEL.FRAMEWORK_PYTHON)

    def test_from_response_json_list(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_list"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend

        # Act
        m_lst = model.Model.from_response_json(copy.deepcopy(json_camelized))

        # Assert
        assert isinstance(m_lst, list)
        assert len(m_lst) == 2

        for i in range(len(m_lst)):
            m = m_lst[i]
            m_json = json["items"][i]
            self.assert_model(mocker, m, m_json, MODEL.FRAMEWORK_PYTHON)

    # constructor

    def test_constructor_base(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_base"]["response"]["items"][0]
        m_json = copy.deepcopy(json)
        id = m_json.pop("id")
        name = m_json.pop("name")

        # Act
        m = model.Model(id=id, name=name, **m_json)

        # Assert
        self.assert_model(mocker, m, json, None)

    def test_constructor_python(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        m_json = copy.deepcopy(json)
        id = m_json.pop("id")
        name = m_json.pop("name")

        # Act
        m = model.Model(id=id, name=name, **m_json)

        # Assert
        self.assert_model(mocker, m, json, MODEL.FRAMEWORK_PYTHON)

    def test_constructor_sklearn(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_sklearn"]["response"]["items"][0]
        m_json = copy.deepcopy(json)
        id = m_json.pop("id")
        name = m_json.pop("name")

        # Act
        m = model.Model(id=id, name=name, **m_json)

        # Assert
        self.assert_model(mocker, m, json, MODEL.FRAMEWORK_SKLEARN)

    def test_constructor_tensorflow(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_tensorflow"]["response"]["items"][0]
        m_json = copy.deepcopy(json)
        id = m_json.pop("id")
        name = m_json.pop("name")

        # Act
        m = model.Model(id=id, name=name, **m_json)

        # Assert
        self.assert_model(mocker, m, json, MODEL.FRAMEWORK_TENSORFLOW)

    def test_constructor_torch(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_torch"]["response"]["items"][0]
        m_json = copy.deepcopy(json)
        id = m_json.pop("id")
        name = m_json.pop("name")

        # Act
        m = model.Model(id=id, name=name, **m_json)

        # Assert
        self.assert_model(mocker, m, json, MODEL.FRAMEWORK_TORCH)

    def test_constructor_llm(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["model"]["get_llm"]["response"]["items"][0]
        m_json = copy.deepcopy(json)
        id = m_json.pop("id")
        name = m_json.pop("name")

        # Act
        m = model.Model(id=id, name=name, **m_json)

        # Assert
        self.assert_model(mocker, m, json, MODEL.FRAMEWORK_LLM)

    # save

    def test_save(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_model_engine_save = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.save"
        )
        upload_configuration = {"config": "value"}

        # Act
        m = model.Model.from_response_json(m_json)
        m.save(
            model_path="model_path",
            await_registration=1234,
            keep_original_files=True,
            upload_configuration=upload_configuration,
        )

        # Assert
        mock_model_engine_save.assert_called_once_with(
            model_instance=m,
            model_path="model_path",
            await_registration=1234,
            keep_original_files=True,
            upload_configuration=upload_configuration,
        )

    # deploy

    def test_deploy(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        p_json = backend_fixtures["predictor"]["get_deployments_singleton"]["response"][
            "items"
        ][0]
        mock_predictor = mocker.Mock()
        mock_predictor_for_model = mocker.patch(
            "hsml.predictor.Predictor.for_model", return_value=mock_predictor
        )
        # params
        resources = copy.deepcopy(p_json["predictor_resources"])
        inference_logger = {
            "mode": p_json["inference_logging"],
            "kafka_topic": copy.deepcopy(p_json["kafka_topic_dto"]),
        }
        inference_batcher = copy.deepcopy(p_json["batching_configuration"])
        transformer = {
            "script_file": p_json["transformer"],
            "resources": copy.deepcopy(p_json["transformer_resources"]),
        }

        # Act
        m = model.Model.from_response_json(m_json)
        m.deploy(
            name=p_json["name"],
            description=p_json["description"],
            artifact_version=p_json["artifact_version"],
            serving_tool=p_json["serving_tool"],
            script_file=p_json["predictor"],
            config_file=p_json["config_file"],
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            transformer=transformer,
            api_protocol=p_json["api_protocol"],
            environment=p_json["environment_dto"]["name"],
        )

        # Assert
        mock_predictor_for_model.assert_called_once_with(
            m,
            name=p_json["name"],
            description=p_json["description"],
            artifact_version=p_json["artifact_version"],
            serving_tool=p_json["serving_tool"],
            script_file=p_json["predictor"],
            config_file=p_json["config_file"],
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            transformer=transformer,
            api_protocol=p_json["api_protocol"],
            environment=p_json["environment_dto"]["name"],
        )
        mock_predictor.deploy.assert_called_once()

    # delete

    def test_delete(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_model_engine_delete = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.delete"
        )

        # Act
        m = model.Model.from_response_json(m_json)
        m.delete()

        # Assert
        mock_model_engine_delete.assert_called_once_with(model_instance=m)

    # download

    def test_download(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_model_engine_download = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.download"
        )

        # Act
        m = model.Model.from_response_json(m_json)
        m.download()

        # Assert
        mock_model_engine_download.assert_called_once_with(
            model_instance=m, local_path=None
        )

    # tags

    def test_get_tag(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_model_engine_get_tag = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.get_tag"
        )

        # Act
        m = model.Model.from_response_json(m_json)
        m.get_tag("tag_name")

        # Assert
        mock_model_engine_get_tag.assert_called_once_with(
            model_instance=m, name="tag_name"
        )

    def test_get_tags(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_model_engine_get_tags = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.get_tags"
        )

        # Act
        m = model.Model.from_response_json(m_json)
        m.get_tags()

        # Assert
        mock_model_engine_get_tags.assert_called_once_with(model_instance=m)

    def test_set_tag(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_model_engine_set_tag = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.set_tag"
        )

        # Act
        m = model.Model.from_response_json(m_json)
        m.set_tag("tag_name", "tag_value")

        # Assert
        mock_model_engine_set_tag.assert_called_once_with(
            model_instance=m, name="tag_name", value="tag_value"
        )

    def test_delete_tag(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_model_engine_delete_tag = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.delete_tag"
        )

        # Act
        m = model.Model.from_response_json(m_json)
        m.delete_tag("tag_name")

        # Assert
        mock_model_engine_delete_tag.assert_called_once_with(
            model_instance=m, name="tag_name"
        )

    # get url

    def test_get_url(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]

        class ClientMock:
            _project_id = 1

        mock_client_get_instance = mocker.patch(
            "hopsworks_common.client.get_instance", return_value=ClientMock()
        )
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hopsworks_common.util.get_hostname_replaced_url", return_value="full_path"
        )
        path_arg = "/p/1/models/" + m_json["name"] + "/" + str(m_json["version"])

        # Act
        m = model.Model.from_response_json(m_json)
        url = m.get_url()

        # Assert
        assert url == "full_path"
        mock_client_get_instance.assert_called_once()
        mock_util_get_hostname_replaced_url.assert_called_once_with(sub_path=path_arg)

    # auxiliary methods
    def assert_model(self, mocker, m, m_json, model_framework):
        assert isinstance(m, model.Model)
        assert m.id == m_json["id"]
        assert m.name == m_json["name"]
        assert m.version == m_json["version"]
        assert m.created == m_json["created"]
        assert m.creator == m_json["creator"]
        assert m.description == m_json["description"]
        assert m.project_name == m_json["project_name"]
        assert m.training_metrics == m_json["metrics"]
        assert m._user_full_name == m_json["user_full_name"]
        assert m.model_registry_id == m_json["model_registry_id"]

        if model_framework is None:
            assert m.framework is None
        else:
            assert m.framework == model_framework

        mock_read_json = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.read_json",
            return_value="input_example_content",
        )
        assert m.input_example == "input_example_content"
        mock_read_json.assert_called_once_with(
            model_instance=m, resource=m_json["input_example"]
        )

        mock_read_json = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.read_json",
            return_value="model_schema_content",
        )
        assert m.model_schema == "model_schema_content"
        mock_read_json.assert_called_once_with(
            model_instance=m, resource=m_json["model_schema"]
        )

        mock_read_file = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.read_file",
            return_value="program_file_content",
        )
        assert m.program == "program_file_content"
        mock_read_file.assert_called_once_with(
            model_instance=m, resource=m_json["program"]
        )

        mock_read_file = mocker.patch(
            "hsml.engine.model_engine.ModelEngine.read_file",
            return_value="env_file_content",
        )
        assert m.environment == "env_file_content"
        mock_read_file.assert_called_once_with(
            model_instance=m, resource=m_json["environment"]
        )

    def test_get_feature_view(self, mocker):
        mock_fv = mocker.Mock()
        links = explicit_provenance.Links(accessible=[mock_fv])
        mock_fv_provenance = mocker.patch(
            "hsml.model.Model.get_feature_view_provenance", return_value=links
        )
        mock_td_provenance = mocker.patch(
            "hsml.model.Model.get_training_dataset_provenance", return_value=links
        )
        mocker.patch("os.environ", return_value={})
        m = model.Model(1, "test")
        m.get_feature_view()
        mock_fv_provenance.assert_called_once()
        mock_td_provenance.assert_called_once()
        assert not mock_fv.init_serving.called
        assert mock_fv.init_batch_scoring.called

    def test_get_feature_view_online(self, mocker):
        mock_fv = mocker.Mock()
        links = explicit_provenance.Links(accessible=[mock_fv])
        mock_fv_provenance = mocker.patch(
            "hsml.model.Model.get_feature_view_provenance", return_value=links
        )
        mock_td_provenance = mocker.patch(
            "hsml.model.Model.get_training_dataset_provenance", return_value=links
        )
        mocker.patch("os.environ", return_value={})
        m = model.Model(1, "test")
        m.get_feature_view(online=True)
        mock_fv_provenance.assert_called_once()
        mock_td_provenance.assert_called_once()
        assert mock_fv.init_serving.called
        assert not mock_fv.init_batch_scoring.called

    def test_get_feature_view_batch(self, mocker):
        mock_fv = mocker.Mock()
        links = explicit_provenance.Links(accessible=[mock_fv])
        mock_fv_provenance = mocker.patch(
            "hsml.model.Model.get_feature_view_provenance", return_value=links
        )
        mock_td_provenance = mocker.patch(
            "hsml.model.Model.get_training_dataset_provenance", return_value=links
        )
        mocker.patch("os.environ", return_value={})
        m = model.Model(1, "test")
        m.get_feature_view(online=False)
        mock_fv_provenance.assert_called_once()
        mock_td_provenance.assert_called_once()
        assert not mock_fv.init_serving.called
        assert mock_fv.init_batch_scoring.called

    def test_get_feature_view_deployment(self, mocker):
        mock_fv = mocker.Mock()
        links = explicit_provenance.Links(accessible=[mock_fv])
        mock_fv_provenance = mocker.patch(
            "hsml.model.Model.get_feature_view_provenance", return_value=links
        )
        mock_td_provenance = mocker.patch(
            "hsml.model.Model.get_training_dataset_provenance", return_value=links
        )
        mocker.patch.dict(os.environ, {"DEPLOYMENT_NAME": "test"})
        m = model.Model(1, "test")
        m.get_feature_view()
        mock_fv_provenance.assert_called_once()
        mock_td_provenance.assert_called_once()
        assert mock_fv.init_serving.called
        assert not mock_fv.init_batch_scoring.called

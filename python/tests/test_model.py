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
import pytest
from hopsworks_common import util
from hopsworks_common.client.exceptions import ModelRegistryException
from hsml import model
from hsml.constants import MODEL
from hsml.core import explicit_provenance
from hsml.engine import model_engine


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
        scaling_configuration = copy.deepcopy(p_json["predictor_scaling_config"])

        # Act
        m = model.Model.from_response_json(m_json)
        m.deploy(
            name=p_json["name"],
            description=p_json["description"],
            serving_tool=p_json["serving_tool"],
            script_file=p_json["predictor"],
            config_file=p_json["config_file"],
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            scaling_configuration=scaling_configuration,
            transformer=transformer,
            api_protocol=p_json["api_protocol"],
            environment=p_json["environment_dto"]["name"],
        )

        # Assert
        mock_predictor_for_model.assert_called_once_with(
            m,
            name=p_json["name"],
            description=p_json["description"],
            serving_tool=p_json["serving_tool"],
            script_file=p_json["predictor"],
            config_file=p_json["config_file"],
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            scaling_configuration=scaling_configuration,
            transformer=transformer,
            api_protocol=p_json["api_protocol"],
            environment=p_json["environment_dto"]["name"],
            env_vars=None,
            vllm_variant=None,
            vllm_image_tag=None,
        )
        mock_predictor.deploy.assert_called_once()

    def test_deploy_with_env_vars(self, mocker, backend_fixtures):
        # Arrange
        m_json = backend_fixtures["model"]["get_python"]["response"]["items"][0]
        mock_predictor = mocker.Mock()
        mock_predictor_for_model = mocker.patch(
            "hsml.predictor.Predictor.for_model", return_value=mock_predictor
        )
        env_vars = {"FOO": "bar", "BAZ": "qux"}

        # Act
        m = model.Model.from_response_json(m_json)
        m.deploy(name="test", env_vars=env_vars)

        # Assert
        assert mock_predictor_for_model.call_args.kwargs["env_vars"] == env_vars
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


class TestModelEngine:
    @pytest.mark.parametrize(
        "model_path,expected_hdfs_path",
        [
            # /hopsfs/ is the per-project mount: strip the prefix to get a
            # project-relative dataset path.
            (
                "/hopsfs/Models/model.pkl",
                "Models/model.pkl",
            ),
            # /mnt/hopsfs/ is the cluster-wide mount rooted at /Projects/, so
            # the path is /mnt/hopsfs/<projectName>/<rest>. Strip both segments.
            (
                "/mnt/hopsfs/demo/Models/model.pkl",
                "Models/model.pkl",
            ),
            # The actual failing case from the loadtest run on 2026-05-09.
            (
                "/mnt/hopsfs/demo/Resources/workflows/models/tensorflow",
                "Resources/workflows/models/tensorflow",
            ),
        ],
    )
    def test_normalize_hopsfs_mount_path(self, mocker, model_path, expected_hdfs_path):
        mocker.patch("hsml.engine.model_engine.model_api.ModelApi")
        mocker.patch("hsml.engine.model_engine.dataset_api.DatasetApi")
        mocker.patch("hsml.engine.model_engine.local_engine.LocalEngine")

        engine = model_engine.ModelEngine()

        assert engine._normalize_hopsfs_mount_path(model_path) == expected_hdfs_path

    def test_normalize_hopsfs_mount_path_returns_none_for_local_path(self, mocker):
        mocker.patch("hsml.engine.model_engine.model_api.ModelApi")
        mocker.patch("hsml.engine.model_engine.dataset_api.DatasetApi")
        mocker.patch("hsml.engine.model_engine.local_engine.LocalEngine")

        engine = model_engine.ModelEngine()

        assert engine._normalize_hopsfs_mount_path("local/model.pkl") is None

    @pytest.mark.parametrize(
        "model_path,expected_hdfs_path",
        [
            (
                "/hopsfs/Models/hopsfs/archive/model.pkl",
                "Models/hopsfs/archive/model.pkl",
            ),
            (
                "/mnt/hopsfs/demo/Models/mnt/hopsfs/archive/model.pkl",
                "Models/mnt/hopsfs/archive/model.pkl",
            ),
        ],
    )
    def test_normalize_hopsfs_mount_path_strips_only_leading_prefix(
        self, mocker, model_path, expected_hdfs_path
    ):
        mocker.patch("hsml.engine.model_engine.model_api.ModelApi")
        mocker.patch("hsml.engine.model_engine.dataset_api.DatasetApi")
        mocker.patch("hsml.engine.model_engine.local_engine.LocalEngine")

        engine = model_engine.ModelEngine()

        assert engine._normalize_hopsfs_mount_path(model_path) == expected_hdfs_path

    @pytest.mark.parametrize(
        "model_path,expected_hdfs_path",
        [
            (
                "/hopsfs/Models/model.pkl",
                "Models/model.pkl",
            ),
            (
                "/mnt/hopsfs/demo/Models/model.pkl",
                "Models/model.pkl",
            ),
        ],
    )
    def test_save_model_from_local_or_hopsfs_mount_uses_hopsfs_copy(
        self, mocker, model_path, expected_hdfs_path
    ):
        mocker.patch("hsml.engine.model_engine.model_api.ModelApi")
        mocker.patch("hsml.engine.model_engine.dataset_api.DatasetApi")
        mocker.patch("hsml.engine.model_engine.local_engine.LocalEngine")

        engine = model_engine.ModelEngine()
        copy_or_move = mocker.patch.object(engine, "_copy_or_move_hopsfs_model")
        upload_local = mocker.patch.object(engine, "_upload_local_model")
        model_instance = mocker.Mock(model_files_path="Models/test/1/Files")
        progress = mocker.Mock()

        engine._save_model_from_local_or_hopsfs_mount(
            model_instance=model_instance,
            model_path=model_path,
            keep_original_files=True,
            update_upload_progress=progress,
        )

        copy_or_move.assert_called_once_with(
            from_hdfs_model_path=expected_hdfs_path,
            to_model_files_path=model_instance.model_files_path,
            keep_original_files=True,
            update_upload_progress=progress,
        )
        upload_local.assert_not_called()

    def test_save_model_from_local_or_hopsfs_mount_uses_local_upload(self, mocker):
        mocker.patch("hsml.engine.model_engine.model_api.ModelApi")
        mocker.patch("hsml.engine.model_engine.dataset_api.DatasetApi")
        mocker.patch("hsml.engine.model_engine.local_engine.LocalEngine")

        engine = model_engine.ModelEngine()
        copy_or_move = mocker.patch.object(engine, "_copy_or_move_hopsfs_model")
        upload_local = mocker.patch.object(engine, "_upload_local_model")
        model_instance = mocker.Mock(model_files_path="Models/test/1/Files")
        progress = mocker.Mock()
        upload_configuration = {"config": "value"}

        engine._save_model_from_local_or_hopsfs_mount(
            model_instance=model_instance,
            model_path="local/model.pkl",
            keep_original_files=True,
            update_upload_progress=progress,
            upload_configuration=upload_configuration,
        )

        upload_local.assert_called_once_with(
            from_local_model_path="local/model.pkl",
            to_model_files_path=model_instance.model_files_path,
            update_upload_progress=progress,
            upload_configuration=upload_configuration,
        )
        copy_or_move.assert_not_called()


class TestModelEngineExportFastSlowPath:
    """Pin the slow/fast-path dispatch through to the leaf I/O calls.

    The other TestModelEngine cases mock _copy_or_move_hopsfs_model away,
    so they only assert on the *normalized* path passed to it. These tests
    leave _copy_or_move_hopsfs_model intact and mock only _dataset_api and
    _engine, so the path that actually reaches _dataset_api.get is asserted
    directly. That's where the HWORKS-2731 regression manifested — the
    normalizer produced a malformed path and the bug was only visible at
    the dataset_api call site.
    """

    @pytest.mark.parametrize(
        "model_path,expected_dataset_get_arg",
        [
            # /hopsfs/ — per-project mount.
            ("/hopsfs/Resources/foo/bar.pkl", "Resources/foo/bar.pkl"),
            # /mnt/hopsfs/ — cluster-wide mount, rooted at /Projects/.
            ("/mnt/hopsfs/demo/Resources/foo/bar.pkl", "Resources/foo/bar.pkl"),
            # The actual failing case from the 2026-05-09 loadtest run.
            (
                "/mnt/hopsfs/demo/Resources/workflows/models/tensorflow",
                "Resources/workflows/models/tensorflow",
            ),
        ],
    )
    def test_fast_path_passes_project_relative_path_to_dataset_api(
        self, mocker, model_path, expected_dataset_get_arg
    ):
        mocker.patch("hsml.engine.model_engine.model_api.ModelApi")
        mocker.patch("hsml.engine.model_engine.dataset_api.DatasetApi")
        mocker.patch("hsml.engine.model_engine.local_engine.LocalEngine")
        engine = model_engine.ModelEngine()
        engine._dataset_api.get.return_value = {
            "attributes": {
                "dir": False,
                "path": "/Projects/demo/" + expected_dataset_get_arg,
            },
        }

        engine._save_model_from_local_or_hopsfs_mount(
            model_instance=mocker.Mock(model_files_path="Models/test/1/Files"),
            model_path=model_path,
            keep_original_files=True,
            update_upload_progress=mocker.Mock(),
        )

        # Regression guard: must be project-relative — NOT "/<projectName>/<rest>".
        engine._dataset_api.get.assert_called_once_with(expected_dataset_get_arg)
        # Fast path: HopsFS-internal copy, no chunked HTTP upload.
        engine._engine.copy.assert_called_once()
        engine._engine.upload.assert_not_called()

    def test_slow_path_uses_engine_upload_for_non_mount_path(self, mocker):
        mocker.patch("hsml.engine.model_engine.model_api.ModelApi")
        mocker.patch("hsml.engine.model_engine.dataset_api.DatasetApi")
        mocker.patch("hsml.engine.model_engine.local_engine.LocalEngine")
        mocker.patch("os.path.isdir", return_value=False)
        engine = model_engine.ModelEngine()

        engine._save_model_from_local_or_hopsfs_mount(
            model_instance=mocker.Mock(model_files_path="Models/test/1/Files"),
            model_path="/some/local/model.pkl",
            keep_original_files=True,
            update_upload_progress=mocker.Mock(),
            upload_configuration={"chunk_size": 10},
        )

        # Slow path: chunked HTTP upload, no HopsFS-internal copy/move and no
        # dataset_api lookup.
        engine._engine.upload.assert_called_once()
        engine._engine.copy.assert_not_called()
        engine._engine.move.assert_not_called()
        engine._dataset_api.get.assert_not_called()


class TestModelNameValidation:
    """Tests for model name validation."""

    @pytest.mark.parametrize(
        "valid_name",
        [
            "my_model",
            "Model123",
            "test_model_v1",
            "a",
            "A",
            "model",
            "MODEL",
            "model_1_2_3",
            "_underscore_start",
            "end_underscore_",
        ],
    )
    def test_valid_model_names(self, valid_name):
        # Should not raise any exception
        util.validate_model_name(valid_name)

    @pytest.mark.parametrize(
        "invalid_name,description",
        [
            ("my-model", "hyphen"),
            ("model.v1", "dot"),
            ("model name", "space"),
            ("", "empty string"),
            ("model/path", "slash"),
            ("model@name", "at symbol"),
            ("model#1", "hash"),
            ("model$name", "dollar sign"),
        ],
    )
    def test_invalid_model_names(self, invalid_name, description):
        with pytest.raises(ModelRegistryException) as exc_info:
            util.validate_model_name(invalid_name)
        assert f"Invalid model name '{invalid_name}'" in str(exc_info.value)
        assert "[a-zA-Z0-9_]+" in str(exc_info.value)

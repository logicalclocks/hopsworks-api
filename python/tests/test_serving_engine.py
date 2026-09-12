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

import os

import pytest
from hsml.engine import serving_engine


class _FakePredictor:
    def __init__(
        self,
        script_file=None,
        config_file=None,
        transformer=None,
        default_predictor=False,
        schema=None,
        env_vars=None,
    ):
        self.script_file = script_file
        self.config_file = config_file
        self.transformer = transformer
        self.default_predictor = default_predictor
        self._schema = schema
        self.env_vars = env_vars

    @property
    def schema(self):
        return self._schema

    @property
    def schema_id(self):
        if self._schema is not None:
            return self._schema.schema_id
        return (self.env_vars or {}).get("SERVING_SCHEMA_ID")


class _FakeTransformer:
    def __init__(self, script_file=None):
        self.script_file = script_file


class _FakeDeployment:
    def __init__(self, predictor, name, id=None):
        self._predictor = predictor
        self.name = name
        self.id = id


class TestUploadLocalServingFiles:
    """Tests for ServingEngine._upload_local_serving_files.

    The actual upload + path-rewrite logic lives in
    `hsml.utils.local_paths._resolve_serving_file` (covered by
    test_local_paths.py). These tests focus on the engine wiring: that all
    three fields are processed, that the rewritten paths land back on the
    in-memory predictor/transformer, and that the transformer is skipped when
    absent.
    """

    def _engine(self, mocker):
        # Skip ServingApi/DatasetApi setup; we only need _engine attribute.
        eng = serving_engine.ServingEngine.__new__(serving_engine.ServingEngine)
        eng._engine = mocker.Mock()
        return eng

    def test_rewrites_all_three_fields(self, mocker):
        eng = self._engine(mocker)
        # Mock the resolver to echo back a sentinel so we can see which path
        # was passed in.
        mocker.patch.object(
            serving_engine,
            "_resolve_serving_file",
            side_effect=lambda engine, name, p, **kw: (
                None if p is None else f"hopsfs::{kw['field_name']}::{p}"
            ),
        )

        predictor = _FakePredictor(
            script_file="./predictor.py",
            config_file="./vllm.yaml",
            transformer=_FakeTransformer(script_file="./transformer.py"),
        )
        deployment = _FakeDeployment(predictor, name="my_dep")

        eng._upload_local_serving_files(deployment)

        assert predictor.script_file == "hopsfs::script_file::./predictor.py"
        assert predictor.config_file == "hopsfs::config_file::./vllm.yaml"
        assert (
            predictor.transformer.script_file
            == "hopsfs::transformer.script_file::./transformer.py"
        )

    def test_each_field_uploads_to_its_own_subdir(self, mocker):
        # Per-role subdirs prevent basename collisions when predictor and
        # transformer scripts share the same filename.
        eng = self._engine(mocker)
        mock_resolve = mocker.patch.object(
            serving_engine,
            "_resolve_serving_file",
            side_effect=lambda engine, name, p, **kw: p,
        )

        predictor = _FakePredictor(
            script_file="./p.py",
            config_file="./c.yaml",
            transformer=_FakeTransformer(script_file="./t.py"),
        )
        deployment = _FakeDeployment(predictor, name="my_dep")

        eng._upload_local_serving_files(deployment)

        subdirs = {
            call.kwargs["field_name"]: call.kwargs["subdir"]
            for call in mock_resolve.mock_calls
        }
        assert subdirs == {
            "script_file": "predictor",
            "config_file": "config",
            "transformer.script_file": "transformer",
        }

    def test_none_paths_remain_none(self, mocker):
        eng = self._engine(mocker)
        mocker.patch.object(
            serving_engine,
            "_resolve_serving_file",
            side_effect=lambda engine, name, p, **kw: p,
        )

        predictor = _FakePredictor(script_file=None, config_file=None, transformer=None)
        deployment = _FakeDeployment(predictor, name="my_dep")

        eng._upload_local_serving_files(deployment)

        assert predictor.script_file is None
        assert predictor.config_file is None
        assert predictor.transformer is None

    def test_no_transformer_skipped(self, mocker):
        eng = self._engine(mocker)
        mock_resolve = mocker.patch.object(
            serving_engine,
            "_resolve_serving_file",
            side_effect=lambda engine, name, p, **kw: p,
        )

        predictor = _FakePredictor(
            script_file="/Projects/x/y.py",
            config_file=None,
            transformer=None,
        )
        deployment = _FakeDeployment(predictor, name="my_dep")

        eng._upload_local_serving_files(deployment)

        # Only the predictor's two fields were processed, not the transformer.
        assert mock_resolve.call_count == 2
        fields = [call.kwargs["field_name"] for call in mock_resolve.mock_calls]
        assert "script_file" in fields
        assert "config_file" in fields
        assert "transformer.script_file" not in fields

    def test_passes_deployment_name_and_engine(self, mocker):
        eng = self._engine(mocker)
        mock_resolve = mocker.patch.object(
            serving_engine,
            "_resolve_serving_file",
            side_effect=lambda engine, name, p, **kw: p,
        )

        predictor = _FakePredictor(
            script_file="./p.py",
            config_file=None,
            transformer=_FakeTransformer(script_file="./t.py"),
        )
        deployment = _FakeDeployment(predictor, name="my_dep_123")

        eng._upload_local_serving_files(deployment)

        for call in mock_resolve.mock_calls:
            assert call.args[0] is eng._engine  # the LocalEngine
            assert call.args[1] == "my_dep_123"  # deployment_name

    def test_is_update_derived_from_deployment_id(self, mocker):
        # A deployment with an id is an update: the resolver is told so, so it
        # passes backend-managed references through instead of raising.
        eng = self._engine(mocker)
        mock_resolve = mocker.patch.object(
            serving_engine,
            "_resolve_serving_file",
            side_effect=lambda engine, name, p, **kw: p,
        )

        predictor = _FakePredictor(script_file="predictor.py", config_file=None)

        eng._upload_local_serving_files(_FakeDeployment(predictor, "d", id=None))
        assert all(c.kwargs["is_update"] is False for c in mock_resolve.mock_calls)

        mock_resolve.reset_mock()
        eng._upload_local_serving_files(_FakeDeployment(predictor, "d", id=42))
        assert all(c.kwargs["is_update"] is True for c in mock_resolve.mock_calls)


class TestSave:
    """Tests for ServingEngine._save() method.

    Tests for ServingEngine._save() - that save runs the upload pass and then dispatches to create / update based on the deployment id.
    """

    def _engine(self, mocker):
        eng = serving_engine.ServingEngine.__new__(serving_engine.ServingEngine)
        eng._engine = mocker.Mock()
        eng._serving_api = mocker.Mock()
        eng._update = mocker.Mock()
        eng._create = mocker.Mock()
        return eng

    def test_upload_runs_and_create_called_for_new_deployment(self, mocker):
        eng = self._engine(mocker)
        mock_upload = mocker.patch.object(eng, "_upload_local_serving_files")

        predictor = _FakePredictor(script_file="./p.py")
        deployment = _FakeDeployment(predictor, name="dep", id=None)

        eng._save(deployment, await_update=0)

        mock_upload.assert_called_once_with(deployment)
        eng._create.assert_called_once_with(deployment)
        eng._update.assert_not_called()

    def test_upload_runs_and_update_called_for_existing_deployment(self, mocker):
        eng = self._engine(mocker)
        mock_upload = mocker.patch.object(eng, "_upload_local_serving_files")

        predictor = _FakePredictor(script_file="./p.py")
        deployment = _FakeDeployment(predictor, name="dep", id=7)

        eng._save(deployment, await_update=0)

        mock_upload.assert_called_once_with(deployment)
        eng._update.assert_called_once_with(deployment, 0)
        eng._create.assert_not_called()


class TestSchemaPublishing:
    """The stub and the content-addressed schema file are in place before the PUT."""

    def _engine(self, mocker):
        eng = serving_engine.ServingEngine.__new__(serving_engine.ServingEngine)
        eng._engine = mocker.Mock()
        eng._engine._dataset_api.exists.return_value = False
        eng._serving_api = mocker.Mock()
        eng._dataset_api = mocker.Mock()
        eng._update = mocker.Mock()
        eng._create = mocker.Mock()
        return eng

    def test_stub_uploaded_for_default_predictor_without_script(self, mocker):
        eng = self._engine(mocker)
        mocker.patch.object(eng, "_upload_local_serving_files")
        resolve = mocker.patch(
            "hsml.engine.serving_engine._resolve_serving_file",
            return_value="/Projects/p/Deployments/dep/resources/predictor/default_predictor.py",
        )
        predictor = _FakePredictor(default_predictor=True)
        deployment = _FakeDeployment(predictor, name="dep")

        eng._save(deployment, await_update=0)

        assert predictor.script_file.endswith("default_predictor.py")
        assert resolve.call_args.kwargs["subdir"] == "predictor"
        assert resolve.call_args.args[2].endswith("default_predictor.py")
        eng._create.assert_called_once_with(deployment)

    def test_custom_script_is_kept(self, mocker):
        eng = self._engine(mocker)
        mocker.patch.object(eng, "_upload_local_serving_files")
        predictor = _FakePredictor(
            script_file="/Projects/p/mine.py", default_predictor=True
        )

        eng._save(_FakeDeployment(predictor, name="dep"), await_update=0)

        assert predictor.script_file == "/Projects/p/mine.py"

    def test_schema_written_once_and_env_var_set_before_put(self, mocker):
        from hsml.deployment_schema import DeploymentSchema

        eng = self._engine(mocker)
        mocker.patch.object(eng, "_upload_local_serving_files")
        mocker.patch("hsml.engine.serving_engine._ensure_dataset_dir")
        schema = DeploymentSchema(serving_keys=["k"])
        predictor = _FakePredictor(
            script_file="/Projects/p/s.py", schema=schema, env_vars={"A": "1"}
        )
        order = []
        eng._engine._upload.side_effect = lambda *a, **k: order.append("upload")
        eng._create.side_effect = lambda d: order.append("put")

        eng._save(_FakeDeployment(predictor, name="dep"), await_update=0)

        assert order == ["upload", "upload", "upload", "put"]
        uploaded = [c.args for c in eng._engine._upload.call_args_list]
        assert {remote for _, remote in uploaded} == {
            "Deployments/dep/resources/schema"
        }
        assert [os.path.basename(local) for local, _ in uploaded] == [
            f"{schema.schema_id}.json",
            f"{schema.schema_id}.jsonschema.json",
            f"{schema.schema_id}.openapi.json",
        ]
        assert predictor.env_vars == {
            "A": "1",
            "SERVING_SCHEMA_ID": schema.schema_id,
            "SERVING_SCHEMA_ENFORCER": "predictor",
        }

        eng._engine._dataset_api.exists.return_value = True
        eng._engine._upload.reset_mock()
        eng._save(_FakeDeployment(predictor, name="dep", id=3), await_update=0)
        eng._engine._upload.assert_not_called()

    def test_renderings_use_the_configured_batch_limit(self, mocker):
        import json

        from hsml.deployment_schema import DeploymentSchema

        eng = self._engine(mocker)
        mocker.patch.object(eng, "_upload_local_serving_files")
        mocker.patch("hsml.engine.serving_engine._ensure_dataset_dir")
        uploaded = {}

        def upload(local, remote, **k):
            with open(local) as f:
                uploaded[os.path.basename(local)] = f.read()

        eng._engine._upload.side_effect = upload
        schema = DeploymentSchema(serving_keys=["k"])
        predictor = _FakePredictor(
            script_file="/Projects/p/s.py",
            schema=schema,
            env_vars={"SERVING_MAX_BATCH_ROWS": "16"},
        )

        eng._save(_FakeDeployment(predictor, name="dep"), await_update=0)

        # the limit is contract content: a new id is published and enforced
        published = predictor._schema
        assert published.max_batch_rows == 16
        assert published.schema_id != schema.schema_id
        assert predictor.env_vars["SERVING_SCHEMA_ID"] == published.schema_id
        rendered = json.loads(uploaded[f"{published.schema_id}.jsonschema.json"])
        batches = rendered["request"]["properties"]["instances"]["oneOf"]
        assert {b["maxItems"] for b in batches} == {16}
        assert json.loads(uploaded[f"{published.schema_id}.json"])["maxBatchRows"] == 16

    def test_publish_propagates_an_unloaded_schema_to_a_new_transformer(self, mocker):
        """A fetched deployment carries only the id; attaching a transformer must hand it over."""
        eng = self._engine(mocker)
        mocker.patch.object(eng, "_upload_local_serving_files")
        transformer = _FakeTransformer(script_file="/Projects/p/t.py")
        transformer.env_vars = None
        predictor = _FakePredictor(
            script_file="/Projects/p/s.py",
            schema=None,
            env_vars={"SERVING_SCHEMA_ID": "0123456789abcdef"},
            transformer=transformer,
        )

        eng._save(_FakeDeployment(predictor, name="dep"), await_update=0)

        assert transformer.env_vars["SERVING_SCHEMA_ID"] == "0123456789abcdef"
        assert transformer.env_vars["SERVING_SCHEMA_ENFORCER"] == "transformer"
        assert predictor.env_vars["SERVING_SCHEMA_ENFORCER"] == "transformer"
        eng._engine._upload.assert_not_called()

    def test_publish_pins_the_enforcer_role_in_the_revision(self, mocker):
        from hsml.deployment_schema import DeploymentSchema

        eng = self._engine(mocker)
        mocker.patch.object(eng, "_upload_local_serving_files")
        mocker.patch("hsml.engine.serving_engine._ensure_dataset_dir")
        alone = _FakePredictor(
            script_file="/Projects/p/s.py", schema=DeploymentSchema(serving_keys=["k"])
        )

        eng._save(_FakeDeployment(alone, name="dep"), await_update=0)

        assert alone.env_vars["SERVING_SCHEMA_ENFORCER"] == "predictor"

    def test_transformer_gets_schema_id_too(self, mocker):
        from hsml.deployment_schema import DeploymentSchema

        eng = self._engine(mocker)
        mocker.patch.object(eng, "_upload_local_serving_files")
        mocker.patch("hsml.engine.serving_engine._ensure_dataset_dir")
        schema = DeploymentSchema(serving_keys=["k"])
        transformer = _FakeTransformer(script_file="/Projects/p/t.py")
        transformer.env_vars = {"T": "1"}
        predictor = _FakePredictor(
            script_file="/Projects/p/s.py", schema=schema, transformer=transformer
        )

        eng._save(_FakeDeployment(predictor, name="dep"), await_update=0)

        assert predictor.env_vars == {
            "SERVING_SCHEMA_ID": schema.schema_id,
            "SERVING_SCHEMA_ENFORCER": "transformer",
        }
        assert transformer.env_vars == {
            "T": "1",
            "SERVING_SCHEMA_ID": schema.schema_id,
            "SERVING_SCHEMA_ENFORCER": "transformer",
        }

    def test_schema_dir_removed_on_delete(self, mocker):
        eng = self._engine(mocker)
        eng._engine._dataset_api.exists.return_value = True
        mocker.patch(
            "hopsworks_common.client._get_instance",
            return_value=mocker.Mock(_project_name="p"),
        )
        deployment = mocker.Mock()
        deployment.name = "dep"
        deployment.get_state.return_value.status = "Stopped"

        eng._delete(deployment)

        eng._engine._dataset_api.remove.assert_called_once_with(
            "/Projects/p/Deployments/dep/resources/schema"
        )

    def test_read_schema_from_backend(self, mocker):
        from hsml.deployment_schema import DeploymentSchema

        eng = self._engine(mocker)
        schema = DeploymentSchema(serving_keys=["k"])
        eng._serving_api._get_schema.return_value = schema.to_dict()
        predictor = mocker.Mock(id=7)

        assert eng._read_schema(predictor, schema.schema_id) == schema
        eng._serving_api._get_schema.assert_called_once_with(7, schema.schema_id)
        eng._dataset_api.read_content.assert_not_called()

    def test_read_schema_backend_says_gone(self, mocker):
        from hopsworks_common.client.exceptions import RestAPIError

        eng = self._engine(mocker)
        response = mocker.Mock(status_code=404)
        response.json.return_value = {"errorCode": 240037, "errorMsg": "gone"}
        eng._serving_api._get_schema.side_effect = RestAPIError("url", response)

        assert eng._read_schema(mocker.Mock(id=7), "abc") is None
        eng._dataset_api.read_content.assert_not_called()

    def test_read_schema_other_404_codes_propagate(self, mocker):
        from hopsworks_common.client.exceptions import RestAPIError

        eng = self._engine(mocker)
        response = mocker.Mock(status_code=404)
        response.json.return_value = {
            "errorCode": 240000,
            "errorMsg": "no such deployment",
        }
        eng._serving_api._get_schema.side_effect = RestAPIError("url", response)

        with pytest.raises(RestAPIError):
            eng._read_schema(mocker.Mock(id=7), "abc")
        eng._dataset_api.read_content.assert_not_called()

    @pytest.mark.parametrize("predictor_id", [None, 7])
    def test_read_schema_falls_back_to_dataset(self, mocker, predictor_id):
        from hopsworks_common.client.exceptions import RestAPIError
        from hsml.deployment_schema import DeploymentSchema

        eng = self._engine(mocker)
        mocker.patch(
            "hopsworks_common.client._get_instance",
            return_value=mocker.Mock(_project_name="p"),
        )
        # a backend without the endpoint answers a bare 404 without an error code
        response = mocker.Mock(status_code=404)
        response.json.side_effect = ValueError("not json")
        eng._serving_api._get_schema.side_effect = RestAPIError("url", response)
        schema = DeploymentSchema(serving_keys=["k"])
        eng._dataset_api.read_content.return_value = mocker.Mock(
            content=schema.json().encode()
        )
        predictor = mocker.Mock(id=predictor_id)
        predictor.name = "dep"

        assert eng._read_schema(predictor, schema.schema_id) == schema
        eng._dataset_api.read_content.assert_called_once_with(
            f"/Projects/p/Deployments/dep/resources/schema/{schema.schema_id}.json"
        )
        eng._dataset_api.read_content.return_value = None
        assert eng._read_schema(predictor, "gone") is None

    def test_read_schema_other_backend_errors_propagate(self, mocker):
        from hopsworks_common.client.exceptions import RestAPIError

        eng = self._engine(mocker)
        response = mocker.Mock(status_code=500)
        response.json.return_value = {"errorCode": 240002}
        eng._serving_api._get_schema.side_effect = RestAPIError("url", response)

        with pytest.raises(RestAPIError):
            eng._read_schema(mocker.Mock(id=7), "abc")


class TestPredictValidation:
    def _engine(self, mocker):
        eng = serving_engine.ServingEngine.__new__(serving_engine.ServingEngine)
        eng._serving_api = mocker.Mock()
        eng._serving_api._send_inference_request.return_value = {"predictions": [1]}
        return eng

    def _deployment(self, mocker, schema):
        import datetime

        d = mocker.Mock()
        d.name = "dep"
        d.model_server = "PYTHON"
        d.api_protocol = "REST"
        d.predictor.serving_tool = "KSERVE"
        d.schema = schema
        self.now = datetime.datetime(2026, 9, 6, 10, 0, 0)
        return d

    def test_rows_encoded_and_validated(self, mocker):
        from hsml.deployment_schema import DeploymentSchema, DeploymentSchemaError

        eng = self._engine(mocker)
        schema = DeploymentSchema(
            serving_keys=[{"name": "k", "type": "bigint"}],
            passed_features=[{"name": "ts", "type": "timestamp"}],
        )
        d = self._deployment(mocker, schema)

        eng._predict(d, None, [{"k": 1, "ts": self.now}])

        payload = eng._serving_api._send_inference_request.call_args.args[1]
        assert payload == {"instances": [{"k": 1, "ts": "2026-09-06T10:00:00Z"}]}

        with pytest.raises(DeploymentSchemaError, match="'k' must not be null"):
            eng._predict(d, {"instances": [[None, 1]]}, None)
        eng._predict(d, {"instances": [[None, 1]]}, None, validate=False)

    def test_configured_batch_limit_applies_client_side(self, mocker):
        from hsml.deployment_schema import DeploymentSchema, DeploymentSchemaError

        eng = self._engine(mocker)
        schema = DeploymentSchema(
            serving_keys=[{"name": "k", "type": "bigint"}], max_batch_rows=1
        )
        d = self._deployment(mocker, schema)

        with pytest.raises(
            DeploymentSchemaError, match="batch has 2 rows, the limit is 1"
        ):
            eng._predict(d, None, [{"k": 1}, {"k": 2}])
        eng._predict(d, None, [{"k": 1}])

    def test_no_schema_or_grpc_skips_validation(self, mocker):
        eng = self._engine(mocker)
        d = self._deployment(mocker, None)
        eng._predict(d, None, [{"anything": 1}])
        d.api_protocol = "GRPC"
        d.schema = mocker.Mock()
        mocker.patch.object(eng, "_validate_inference_payload")
        mocker.patch.object(
            eng, "_build_inference_payload", return_value="grpc-payload"
        )
        eng._predict(d, "data", None)
        d.schema.validate_instances.assert_not_called()

    def test_single_dict_input_is_one_row(self, mocker):
        eng = self._engine(mocker)

        assert eng._parse_inference_inputs("REST", {"k": 1}) == {
            "instances": [{"k": 1}]
        }
        assert eng._parse_inference_inputs("REST", 1.5) == {"instances": [[1.5]]}
        assert eng._parse_inference_inputs("REST", [1, 2]) == {"instances": [[1, 2]]}


class TestPredictErrorMapping:
    def _engine_raising(self, mocker, status, body):
        from hopsworks_common.client.exceptions import RestAPIError

        response = mocker.Mock(status_code=status)
        response.json.return_value = body
        error = RestAPIError.__new__(RestAPIError)
        error.response = response
        error.error_code = None
        error.args = ("boom",)
        eng = serving_engine.ServingEngine.__new__(serving_engine.ServingEngine)
        eng._serving_api = mocker.Mock()
        eng._serving_api._send_inference_request.side_effect = error
        return eng, error

    def _deployment(self, mocker):
        d = mocker.Mock()
        d.model_server = "PYTHON"
        d.api_protocol = "REST"
        d.predictor.serving_tool = "KSERVE"
        d.schema = None
        return d

    def test_bare_404_means_not_running(self, mocker):
        from hopsworks_common.client.exceptions import ModelServingException

        eng, _ = self._engine_raising(mocker, 404, {"error": "model not found"})
        with pytest.raises(ModelServingException, match="not created or running"):
            eng._predict(self._deployment(mocker), None, [[1]])

    def test_structured_404_is_kept(self, mocker):
        from hopsworks_common.client.exceptions import RestAPIError

        eng, error = self._engine_raising(
            mocker,
            404,
            {"detail": {"code": "ENTITY_NOT_FOUND", "message": "x", "errors": []}},
        )
        with pytest.raises(RestAPIError) as info:
            eng._predict(self._deployment(mocker), None, [[1]])
        assert info.value is error
        assert "get_logs" in info.value.args[0]


class TestStartWithoutWaiting:
    def test_start_with_zero_wait_returns_before_the_state_is_known(self, mocker):
        """`start(await_running=0)` used to dereference the None the poller returns for a zero wait."""
        from hopsworks_common.constants import PREDICTOR_STATE
        from hsml.engine import serving_engine

        eng = serving_engine.ServingEngine()
        eng._serving_api = mocker.Mock()
        eng._check_status = mocker.Mock(
            return_value=(False, mocker.Mock(status=PREDICTOR_STATE.STATUS_STOPPED))
        )
        eng._get_available_instances = mocker.Mock(return_value=1)
        eng._get_starting_progress = mocker.Mock(return_value=(0, ""))
        deployment = mocker.Mock(
            name="dep", model_server="PYTHON", has_model=True, requested_instances=1
        )

        eng._start(deployment, await_status=0)

        eng._serving_api._post.assert_called_once()

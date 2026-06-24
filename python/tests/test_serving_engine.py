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

from hsml.engine import serving_engine


class _FakePredictor:
    def __init__(self, script_file=None, config_file=None, transformer=None):
        self.script_file = script_file
        self.config_file = config_file
        self.transformer = transformer


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

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
import zipfile

import build  # noqa: F401  # eagerly load so test patches resolve build.ProjectBuilder
import build.env  # noqa: F401  # eagerly load so test patches resolve build.env.DefaultIsolatedEnv
import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hsml import model_serving


@pytest.fixture
def ms():
    return model_serving.ModelServing(project_name="proj", project_id=99)


@pytest.fixture
def stub_apis(mocker):
    """Patch the DatasetApi and EnvironmentApi factories used by deploy_agent.

    Returns the (ds_api, env_api, env) MagicMocks so tests can assert call patterns.
    """
    ds_api = mocker.MagicMock(name="ds_api")
    # Default: dest dir already exists, uploads return their target path.
    ds_api.exists.return_value = True
    ds_api.upload.side_effect = lambda local, dest, overwrite=False: (
        f"{dest}/{os.path.basename(local)}"
    )

    env = mocker.MagicMock(name="env")
    env_api = mocker.MagicMock(name="env_api")
    env_api.get_environment.return_value = env

    mocker.patch("hsml.model_serving._dataset_api.DatasetApi", return_value=ds_api)
    mocker.patch(
        "hsml.model_serving._environment_api.EnvironmentApi", return_value=env_api
    )
    return ds_api, env_api, env


class TestDeployAgentEntryValidation:
    def test_rejects_nonexistent_path(self, ms):
        with pytest.raises(ValueError, match="must be a .py file"):
            ms.deploy_agent(entry="/nope/does/not/exist.py", name="agent")

    def test_rejects_dir_without_pyproject(self, ms, tmp_path):
        with pytest.raises(ValueError, match="must be a .py file"):
            ms.deploy_agent(entry=str(tmp_path), name="agent")

    def test_rejects_non_py_file(self, ms, tmp_path):
        bad = tmp_path / "agent.txt"
        bad.write_text("not python")
        with pytest.raises(ValueError, match="must be a .py file"):
            ms.deploy_agent(entry=str(bad), name="agent")


class TestDeployAgentIdentifierValidation:
    @pytest.fixture
    def script(self, tmp_path):
        s = tmp_path / "agent.py"
        s.write_text("")
        return s

    @pytest.mark.parametrize(
        "bad", ["../escape", "a/b", "..", ".", "name with space", ""]
    )
    def test_rejects_unsafe_name(self, ms, script, bad):
        with pytest.raises(ValueError, match="name must match"):
            ms.deploy_agent(entry=str(script), name=bad)

    @pytest.mark.parametrize("bad", ["../escape", "a/b", ".."])
    def test_rejects_unsafe_environment(self, ms, script, bad):
        with pytest.raises(ValueError, match="environment must match"):
            ms.deploy_agent(entry=str(script), name="ok", environment=bad)

    @pytest.mark.parametrize(
        "bad", ["", "/abs/path", "..", "../escape", "Resources/../.."]
    )
    def test_rejects_unsafe_upload_dir(self, ms, script, bad):
        with pytest.raises(ValueError, match="upload_dir"):
            ms.deploy_agent(entry=str(script), name="ok", upload_dir=bad)

    def test_normalizes_upload_dir(self, ms, mocker, script, stub_apis):
        # Trailing slash and intermediate '.'/'..' segments that resolve safely should be allowed
        # and the result reused as the upload base.
        ds_api, _, _ = stub_apis
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving.Predictor.for_server")

        ms.deploy_agent(
            entry=str(script), name="ok", upload_dir="Resources/foo/../agents/"
        )

        ds_api.upload.assert_called_once_with(
            str(script.resolve()), "Resources/agents/ok", overwrite=True
        )


class TestDeployAgentScript:
    def test_uploads_script_creates_env_and_predictor(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Arrange
        ds_api, env_api, env = stub_apis
        script = tmp_path / "my_agent.py"
        script.write_text("print('hi')")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")
        deployed = mocker.MagicMock(name="deployment")
        mock_for_server.return_value.deploy.return_value = deployed

        # Act
        result = ms.deploy_agent(entry=str(script), name="my_agent")

        # Assert
        assert result is deployed
        ds_api.upload.assert_called_once_with(
            str(script.resolve()), "Resources/agents/my_agent", overwrite=True
        )
        env_api.get_environment.assert_called_once_with("my_agent")
        env_api.create_environment.assert_not_called()
        env.install_wheel.assert_not_called()
        env.install_requirements.assert_not_called()
        kwargs = mock_for_server.call_args.kwargs
        assert kwargs["name"] == "my_agent"
        assert (
            kwargs["script_file"]
            == "/Projects/proj/Resources/agents/my_agent/my_agent.py"
        )
        assert kwargs["environment"] == "my_agent"

    def test_default_name_from_script_basename(self, ms, mocker, tmp_path, stub_apis):
        # Arrange: omit `name` — should be derived from the .py basename without extension.
        ds_api, env_api, _ = stub_apis
        script = tmp_path / "my_agent.py"
        script.write_text("")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(script))

        # Assert
        env_api.get_environment.assert_called_once_with("my_agent")
        ds_api.upload.assert_called_once_with(
            str(script.resolve()), "Resources/agents/my_agent", overwrite=True
        )
        assert mock_for_server.call_args.kwargs["name"] == "my_agent"

    def test_custom_upload_dir(self, ms, mocker, tmp_path, stub_apis):
        # Arrange
        ds_api, _, _ = stub_apis
        script = tmp_path / "agent.py"
        script.write_text("")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(script), name="agent", upload_dir="Jupyter/agents")

        # Assert: the file lands under the custom base directory.
        ds_api.upload.assert_called_once_with(
            str(script.resolve()), "Jupyter/agents/agent", overwrite=True
        )
        assert (
            mock_for_server.call_args.kwargs["script_file"]
            == "/Projects/proj/Jupyter/agents/agent/agent.py"
        )

    def test_creates_env_when_missing(self, ms, mocker, tmp_path, stub_apis):
        # Arrange
        ds_api, env_api, env = stub_apis
        env_api.get_environment.return_value = None
        env_api.create_environment.return_value = env
        script = tmp_path / "my_agent.py"
        script.write_text("")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(script), name="my_agent")

        # Assert
        env_api.create_environment.assert_called_once_with(
            "my_agent", base_environment_name="python-agent-pipeline"
        )

    def test_custom_environment_name_overrides_default(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Arrange
        ds_api, env_api, _ = stub_apis
        script = tmp_path / "agent.py"
        script.write_text("")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(script), name="my_agent", environment="shared_env")

        # Assert
        env_api.get_environment.assert_called_once_with("shared_env")
        assert mock_for_server.call_args.kwargs["environment"] == "shared_env"

    def test_existing_deployment_metadata_is_rewritten_and_saved(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Arrange
        ds_api, _, _ = stub_apis
        script = tmp_path / "agent.py"
        script.write_text("")
        existing = mocker.MagicMock(name="existing_deployment")
        existing.predictor.id = 42
        mocker.patch.object(ms, "get_deployment", return_value=existing)
        new_predictor = mocker.MagicMock(name="new_predictor")
        mock_for_server = mocker.patch(
            "hsml.model_serving.Predictor.for_server", return_value=new_predictor
        )

        # Act
        result = ms.deploy_agent(
            entry=str(script), name="my_agent", description="updated"
        )

        # Assert: the existing deployment is returned, its predictor is replaced
        # with one carrying the same id, and save() is called instead of deploy().
        assert result is existing
        ds_api.upload.assert_called_once()
        mock_for_server.assert_called_once()
        assert mock_for_server.call_args.kwargs["description"] == "updated"
        assert new_predictor._id == 42
        assert existing.predictor is new_predictor
        assert existing.description == "updated"
        existing.save.assert_called_once_with()
        new_predictor.deploy.assert_not_called()
        existing.start.assert_not_called()
        existing.stop.assert_not_called()
        existing.restart.assert_not_called()

    def test_uploads_requirements_and_installs(self, ms, mocker, tmp_path, stub_apis):
        # Arrange
        ds_api, _, env = stub_apis
        script = tmp_path / "agent.py"
        script.write_text("")
        reqs = tmp_path / "requirements.txt"
        reqs.write_text("requests\n")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(script), name="my_agent", requirements=str(reqs))

        # Assert
        upload_calls = [c.args for c in ds_api.upload.call_args_list]
        assert (str(reqs.resolve()), "Resources/agents/my_agent") in upload_calls
        env.install_requirements.assert_called_once_with(
            "Resources/agents/my_agent/requirements.txt"
        )


class TestDeployAgentPackage:
    def _make_package(self, tmp_path, pkg_name="my_agent"):
        pkg = tmp_path / pkg_name
        pkg.mkdir()
        (pkg / "pyproject.toml").write_text(
            f'[project]\nname = "{pkg_name}"\nversion = "0.1.0"\n'
        )
        return pkg

    def _write_wheel(self, wheel_path, top_level="my_agent"):
        """Write a minimal valid zip to `wheel_path` exposing `<top_level>/__main__.py`."""
        with zipfile.ZipFile(wheel_path, "w") as z:
            z.writestr(f"{top_level}/__init__.py", "")
            z.writestr(f"{top_level}/__main__.py", "")

    def _patch_builder(self, mocker, wheel_path, top_level="my_agent"):
        self._write_wheel(wheel_path, top_level=top_level)
        mocker.patch("build.env.DefaultIsolatedEnv")
        mock_builder = mocker.patch("build.ProjectBuilder")
        mock_builder.from_isolated_env.return_value.build.return_value = str(wheel_path)
        return mock_builder

    def _capture_runner(self, ds_api):
        """Override the default upload stub to snapshot runner.py before its tempdir is gone."""
        captured = {}

        def fake_upload(local, dest, overwrite=False):
            if local.endswith("runner.py"):
                with open(local) as f:
                    captured["content"] = f.read()
            return f"{dest}/{os.path.basename(local)}"

        ds_api.upload.side_effect = fake_upload
        return captured

    def test_builds_uninstalls_and_installs_wheel_and_writes_runner(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Arrange
        ds_api, _, env = stub_apis
        pkg = self._make_package(tmp_path)
        wheel_local = tmp_path / "my_agent-0.1.0-py3-none-any.whl"
        mock_builder = self._patch_builder(mocker, wheel_local)
        captured = self._capture_runner(ds_api)
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(pkg), name="my_agent")

        # Assert: wheel was built (in an isolated env), uninstalled, then installed.
        mock_builder.from_isolated_env.assert_called_once()
        assert mock_builder.from_isolated_env.call_args.args[1] == str(pkg.resolve())
        env.uninstall.assert_called_once_with("my_agent")
        env.install_wheel.assert_called_once_with(
            f"Resources/agents/my_agent/{wheel_local.name}"
        )

        # The runner script was uploaded as the predictor's script_file.
        assert (
            mock_for_server.call_args.kwargs["script_file"]
            == "/Projects/proj/Resources/agents/my_agent/runner.py"
        )
        # The runner contains the runpy invocation for the package.
        assert "runpy.run_module('my_agent'" in captured["content"]
        assert "run_name='__main__'" in captured["content"]

    def test_wheel_basename_return_is_resolved_against_build_dir(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Some versions of `build` return just the wheel filename, not an absolute path.
        # We must still hand a real path to the uploader, so deploy_agent should resolve
        # the result against the build directory it passed in.
        ds_api, _, _ = stub_apis
        pkg = self._make_package(tmp_path)
        wheel_filename = "my_agent-0.1.0-py3-none-any.whl"
        captured_build_dir = {}

        def fake_build(distribution, output_directory):
            captured_build_dir["dir"] = output_directory
            self._write_wheel(os.path.join(output_directory, wheel_filename))
            return wheel_filename  # basename only, as on older `build` versions

        mocker.patch("build.env.DefaultIsolatedEnv")
        mock_builder = mocker.patch("build.ProjectBuilder")
        mock_builder.from_isolated_env.return_value.build.side_effect = fake_build

        captured_uploads = []
        ds_api.upload.side_effect = lambda local, dest, overwrite=False: (
            captured_uploads.append(local) or f"{dest}/{os.path.basename(local)}"
        )

        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving.Predictor.for_server")

        ms.deploy_agent(entry=str(pkg), name="my_agent")

        wheel_uploads = [p for p in captured_uploads if p.endswith(wheel_filename)]
        assert len(wheel_uploads) == 1
        wheel_local_path = wheel_uploads[0]
        assert os.path.isabs(wheel_local_path)
        assert wheel_local_path == os.path.join(
            captured_build_dir["dir"], wheel_filename
        )

    def test_default_name_from_package_dir_basename(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Arrange: omit `name` — should be the package directory's basename.
        _, env_api, env = stub_apis
        pkg = self._make_package(tmp_path, pkg_name="my_pkg")
        wheel_local = tmp_path / "my_pkg-0.1.0-py3-none-any.whl"
        self._patch_builder(mocker, wheel_local, top_level="my_pkg")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(pkg))

        # Assert
        env_api.get_environment.assert_called_once_with("my_pkg")
        env.uninstall.assert_called_once_with("my_pkg")
        assert mock_for_server.call_args.kwargs["name"] == "my_pkg"

    def test_uninstall_404_swallowed_on_first_deploy(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Arrange
        _, _, env = stub_apis
        pkg = self._make_package(tmp_path)
        wheel_local = tmp_path / "my_agent-0.1.0-py3-none-any.whl"
        self._patch_builder(mocker, wheel_local)

        not_found = mocker.MagicMock()
        not_found.status_code = 404
        not_found.json.return_value = {}
        env.uninstall.side_effect = RestAPIError("", not_found)

        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act: must not raise on the 404 from the first-time uninstall.
        ms.deploy_agent(entry=str(pkg), name="my_agent")

        # Assert: install still happened.
        env.install_wheel.assert_called_once()

    def test_uninstall_non_404_propagates(self, ms, mocker, tmp_path, stub_apis):
        # Arrange
        _, _, env = stub_apis
        pkg = self._make_package(tmp_path)
        wheel_local = tmp_path / "my_agent-0.1.0-py3-none-any.whl"
        self._patch_builder(mocker, wheel_local)

        server_error = mocker.MagicMock()
        server_error.status_code = 500
        server_error.json.return_value = {}
        env.uninstall.side_effect = RestAPIError("", server_error)

        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act & Assert
        with pytest.raises(RestAPIError):
            ms.deploy_agent(entry=str(pkg), name="my_agent")
        env.install_wheel.assert_not_called()

    def test_runner_uses_importable_module_not_distribution_name(
        self, ms, mocker, tmp_path, stub_apis
    ):
        # Distribution name "my-agent" is not a valid Python identifier; the importable
        # package is "my_agent". The runner must use the importable name.
        ds_api, _, env = stub_apis
        pkg = self._make_package(tmp_path, pkg_name="my-agent")
        wheel_local = tmp_path / "my_agent-0.1.0-py3-none-any.whl"
        self._patch_builder(mocker, wheel_local, top_level="my_agent")
        captured = self._capture_runner(ds_api)
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(pkg), name="myagent")

        # Assert: pip-side uninstall uses the dist name; runpy uses the importable name.
        env.uninstall.assert_called_once_with("my-agent")
        assert "runpy.run_module('my_agent'" in captured["content"]

    def test_raises_when_wheel_has_no_runnable_module(
        self, ms, mocker, tmp_path, stub_apis
    ):
        ds_api, _, _ = stub_apis
        pkg = self._make_package(tmp_path)
        wheel_local = tmp_path / "my_agent-0.1.0-py3-none-any.whl"
        # Wheel exists but has no top-level package with __main__.py.
        with zipfile.ZipFile(wheel_local, "w") as z:
            z.writestr("my_agent/__init__.py", "")

        mocker.patch("build.env.DefaultIsolatedEnv")
        mock_builder = mocker.patch("build.ProjectBuilder")
        mock_builder.from_isolated_env.return_value.build.return_value = str(
            wheel_local
        )
        mocker.patch.object(ms, "get_deployment", return_value=None)

        with pytest.raises(ValueError, match="no top-level package with `__main__.py`"):
            ms.deploy_agent(entry=str(pkg), name="my_agent")


class TestEnsureDatasetDir:
    def test_noop_when_path_exists(self, mocker):
        ds_api = mocker.MagicMock()
        ds_api.exists.return_value = True

        model_serving._ensure_dataset_dir(ds_api, "Resources/agents/x")

        ds_api.mkdir.assert_not_called()

    def test_creates_missing_path_walking_up(self, mocker):
        ds_api = mocker.MagicMock()
        # "Resources" exists; everything below it does not.
        ds_api.exists.side_effect = lambda p: p == "Resources"

        model_serving._ensure_dataset_dir(ds_api, "Resources/agents/my_agent")

        # Parents are created before children.
        assert [c.args[0] for c in ds_api.mkdir.call_args_list] == [
            "Resources/agents",
            "Resources/agents/my_agent",
        ]


class TestReadPackageName:
    def test_reads_static_name(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text(
            '[project]\nname = "my_agent"\nversion = "0.1.0"\n'
        )

        assert model_serving._read_package_name(str(tmp_path)) == "my_agent"

    def test_raises_when_name_missing(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text('[project]\nversion = "0.1.0"\n')

        with pytest.raises(ValueError, match="\\[project\\].name"):
            model_serving._read_package_name(str(tmp_path))

    def test_raises_when_no_project_table(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text("[build-system]\nrequires = []\n")

        with pytest.raises(ValueError, match="\\[project\\].name"):
            model_serving._read_package_name(str(tmp_path))

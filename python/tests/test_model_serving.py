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

import os  # noqa: I001
import sys
import types
import zipfile

import build  # noqa: F401  # eagerly load so test patches resolve build.ProjectBuilder
import build.env  # noqa: F401  # eagerly load so test patches resolve build.env.DefaultIsolatedEnv
import pytest
from hopsworks_common.client.exceptions import ModelServingException, RestAPIError
from hsml import deployment_tracing_config as deployment_tracing_config, model_serving


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
        "hsml.model_serving._environment_api.EnvironmentApi",
        return_value=env_api,
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


class TestTracingForwarding:
    def test_create_predictor_forwards_tracing(self, ms, mocker):
        # Arrange
        tracing = deployment_tracing_config.DeploymentTracingConfig(
            enabled=True,
            otel_tracing_storage=deployment_tracing_config.DeploymentTracingConfig.STORAGE_OFFLINE,
        )
        model = mocker.Mock()
        model._get_default_serving_name.return_value = "my_model"
        mock_for_model = mocker.patch("hsml.model_serving.Predictor.for_model")

        # Act
        ms.create_predictor(model, tracing=tracing)

        # Assert
        assert mock_for_model.call_args.kwargs["tracing"] is tracing

    def test_create_predictor_forwards_tags(self, ms, mocker):
        # Arrange
        model = mocker.Mock()
        model._get_default_serving_name.return_value = "my_model"
        mock_for_model = mocker.patch("hsml.model_serving.Predictor.for_model")
        tags = {"owner": "team-a"}

        # Act
        ms.create_predictor(model, tags=tags)

        # Assert
        assert mock_for_model.call_args.kwargs["tags"] == tags

    def test_create_predictor_forwards_knative_mode(self, ms, mocker):
        # Arrange
        model = mocker.Mock()
        model._get_default_serving_name.return_value = "my_model"
        mock_for_model = mocker.patch("hsml.model_serving.Predictor.for_model")

        # Act
        ms.create_predictor(model, knative_mode=False)

        # Assert
        assert mock_for_model.call_args.kwargs["knative_mode"] is False

    def test_create_predictor_forwards_triton_image_tag(self, ms, mocker):
        # Arrange
        model = mocker.Mock()
        model._get_default_serving_name.return_value = "my_model"
        mock_for_model = mocker.patch("hsml.model_serving.Predictor.for_model")

        # Act
        ms.create_predictor(model, triton_image_tag="26.09-py3")

        # Assert
        assert mock_for_model.call_args.kwargs["triton_image_tag"] == "26.09-py3"

    def test_create_endpoint_forwards_knative_mode(self, ms, mocker):
        # Arrange
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.create_endpoint(
            name="endpoint",
            script_file="script.py",
            knative_mode=True,
        )

        # Assert
        assert mock_for_server.call_args.kwargs["knative_mode"] is True

    def test_deploy_agent_forwards_knative_mode(self, ms, mocker, stub_apis, tmp_path):
        # Arrange
        script = tmp_path / "agent.py"
        script.write_text("print('hi')")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(entry=str(script), name="my_agent", knative_mode=False)

        # Assert
        assert mock_for_server.call_args.kwargs["knative_mode"] is False

    def test_create_deployment_sets_tags_on_predictor(self, ms, mocker):
        # create_deployment normalizes the tags through the shared Tag helper and
        # stashes the resulting list[Tag] on the predictor so Predictor.to_dict
        # serializes them into the serving-create body (FSTORE-2049).
        # Arrange
        from hopsworks_common.tag import Tag

        predictor = mocker.Mock()
        mock_deployment = mocker.patch("hsml.model_serving.Deployment")
        tags = {"name": "owner", "value": "team-a"}

        # Act
        ms.create_deployment(predictor, tags=tags)

        # Assert
        assert Tag._tags_to_dict(predictor._tags) == {
            "count": 1,
            "items": [{"name": "owner", "value": "team-a"}],
        }
        mock_deployment.assert_called_once_with(predictor=predictor, name=None)

    def test_get_deployment_warns_on_missing_mandatory_tags(self, ms, mocker):
        # Arrange
        d = mocker.Mock()
        d.missing_mandatory_tags = [{"name": "owner"}]
        mocker.patch.object(ms._serving_api, "_get", return_value=d)

        # Act / Assert
        with pytest.warns(UserWarning, match="Missing mandatory tags"):
            ms.get_deployment("my_deployment")

    def test_get_deployment_no_warning_when_tags_present(self, ms, mocker):
        # Arrange
        import warnings

        d = mocker.Mock()
        d.missing_mandatory_tags = []
        mocker.patch.object(ms._serving_api, "_get", return_value=d)

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ms.get_deployment("my_deployment")

        # Assert
        assert not [w for w in caught if "Missing mandatory tags" in str(w.message)]

    def test_create_endpoint_forwards_tracing(self, ms, mocker):
        # Arrange
        tracing = deployment_tracing_config.DeploymentTracingConfig(
            enabled=True,
            otel_tracing_storage=deployment_tracing_config.DeploymentTracingConfig.STORAGE_BOTH,
        )
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.create_endpoint(
            name="endpoint",
            script_file="script.py",
            tracing=tracing,
        )

        # Assert
        assert mock_for_server.call_args.kwargs["tracing"] is tracing

    def test_create_endpoint_forwards_git_source(self, ms, mocker):
        # Arrange
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.create_endpoint(
            name="endpoint",
            script_file="src/endpoint.py",
            git_url="https://github.com/example/repo.git",
            git_provider="github",
            git_branch="main",
        )

        # Assert
        kwargs = mock_for_server.call_args.kwargs
        assert kwargs["script_file"] == "src/endpoint.py"
        assert kwargs["git_url"] == "https://github.com/example/repo.git"
        assert kwargs["git_provider"] == "GitHub"
        assert kwargs["git_branch"] == "main"

    def test_create_endpoint_forwards_git_auto_redeploy(self, ms, mocker):
        # Arrange
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.create_endpoint(
            name="endpoint",
            script_file="src/endpoint.py",
            git_url="https://github.com/example/repo.git",
            git_provider="github",
            git_auto_redeploy=True,
        )

        # Assert
        assert mock_for_server.call_args.kwargs["git_auto_redeploy"] is True

    def test_create_endpoint_rejects_git_auto_redeploy_without_git_url(
        self, ms, mocker
    ):
        # Mirrors ServingUtil: the backend rejects the flag without a git source, so fail
        # locally rather than after a round trip.
        mocker.patch("hsml.model_serving.Predictor.for_server")

        with pytest.raises(ValueError, match="git_auto_redeploy requires git_url"):
            ms.create_endpoint(
                name="endpoint",
                script_file="src/endpoint.py",
                git_auto_redeploy=True,
            )


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
            "hsml.model_serving.Predictor.for_server",
            return_value=new_predictor,
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

    def test_forwards_tracing(self, ms, mocker, tmp_path, stub_apis):
        # Arrange
        ds_api, _, _ = stub_apis
        script = tmp_path / "agent.py"
        script.write_text("")
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")
        tracing = deployment_tracing_config.DeploymentTracingConfig(
            enabled=True,
            otel_tracing_storage=deployment_tracing_config.DeploymentTracingConfig.STORAGE_OFFLINE,
        )

        # Act
        ms.deploy_agent(entry=str(script), name="my_agent", tracing=tracing)

        # Assert
        ds_api.upload.assert_called_once()
        assert mock_for_server.call_args.kwargs["tracing"] is tracing

    def test_git_source_uses_relative_entry_without_upload(self, ms, mocker, stub_apis):
        # Arrange
        ds_api, env_api, _ = stub_apis
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")
        deployed = mocker.MagicMock(name="deployment")
        mock_for_server.return_value.deploy.return_value = deployed

        # Act
        result = ms.deploy_agent(
            entry="src/agents/my_agent.py",
            git_url="https://github.com/example/repo.git",
            git_provider="github",
            git_branch="main",
        )

        # Assert
        assert result is deployed
        ds_api.upload.assert_not_called()
        ds_api.exists.assert_not_called()
        env_api.get_environment.assert_called_once_with("my_agent")
        kwargs = mock_for_server.call_args.kwargs
        assert kwargs["name"] == "my_agent"
        assert kwargs["script_file"] == "src/agents/my_agent.py"
        assert kwargs["git_url"] == "https://github.com/example/repo.git"
        assert kwargs["git_provider"] == "GitHub"
        assert kwargs["git_branch"] == "main"

    def test_git_source_forwards_git_auto_redeploy(self, ms, mocker, stub_apis):
        # Arrange
        mocker.patch.object(ms, "get_deployment", return_value=None)
        mock_for_server = mocker.patch("hsml.model_serving.Predictor.for_server")

        # Act
        ms.deploy_agent(
            entry="src/agents/my_agent.py",
            git_url="https://github.com/example/repo.git",
            git_provider="github",
            git_auto_redeploy=True,
        )

        # Assert
        assert mock_for_server.call_args.kwargs["git_auto_redeploy"] is True

    def test_rejects_git_auto_redeploy_without_git_source(
        self, ms, mocker, stub_apis, tmp_path
    ):
        # The flag is only meaningful for a git-backed agent; the backend rejects it otherwise.
        mocker.patch.object(ms, "get_deployment", return_value=None)
        script = tmp_path / "my_agent.py"
        script.write_text("")

        with pytest.raises(ValueError, match="git_auto_redeploy requires git_url"):
            ms.deploy_agent(entry=str(script), git_auto_redeploy=True)


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


class TestGetVllmImageTags:
    """The helper users call to find a valid vllm_image_tag for a new deployment."""

    RESPONSE = {
        "items": [
            {
                "variant": "VLLM",
                "tags": ["v0.21.0", "v0.15.0"],
                "defaultTag": "v0.21.0",
            },
            {"variant": "VLLM_OMNI", "tags": ["v0.21.0"], "defaultTag": "v0.21.0"},
        ]
    }

    def _stub(self, ms, mocker, response=None):
        return mocker.patch.object(
            ms._serving_api,
            "_get_vllm_image_tags",
            return_value=self.RESPONSE if response is None else response,
        )

    def test_defaults_to_standard_vllm(self, ms, mocker):
        self._stub(ms, mocker)
        assert ms.get_vllm_image_tags() == ["v0.21.0", "v0.15.0"]

    def test_selects_the_requested_variant(self, ms, mocker):
        self._stub(ms, mocker)
        assert ms.get_vllm_image_tags(variant="VLLM_OMNI") == ["v0.21.0"]

    def test_variant_advertising_nothing_returns_empty(self, ms, mocker):
        self._stub(
            ms,
            mocker,
            response={"items": [{"variant": "VLLM", "tags": [], "defaultTag": None}]},
        )
        assert ms.get_vllm_image_tags() == []

    def test_rejects_unknown_variant(self, ms, mocker):
        send = self._stub(ms, mocker)
        with pytest.raises(ValueError):
            ms.get_vllm_image_tags(variant="SGLANG")
        send.assert_not_called()


class TestGetTritonImageTags:
    """The helper users call to find a valid triton_image_tag, and to tell whether Triton is enabled."""

    def _stub(self, ms, mocker, response):
        return mocker.patch.object(
            ms._serving_api, "_get_triton_image_tags", return_value=response
        )

    def test_returns_the_advertised_tags(self, ms, mocker):
        self._stub(
            ms,
            mocker,
            {"tags": ["26.09-py3", "26.08-py3"], "defaultTag": "26.09-py3"},
        )
        assert ms.get_triton_image_tags() == ["26.09-py3", "26.08-py3"]

    def test_not_enabled_returns_empty(self, ms, mocker):
        self._stub(ms, mocker, {"tags": [], "defaultTag": None})
        assert ms.get_triton_image_tags() == []


class _StubOpenAI:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _StubHttpxClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


@pytest.fixture
def stub_openai(mocker):
    """Inject a stub `openai` module so the tests pass without the optional extra."""
    module = types.ModuleType("openai")
    module.OpenAI = _StubOpenAI
    module.DefaultHttpxClient = _StubHttpxClient
    mocker.patch.dict(sys.modules, {"openai": module})
    mocker.patch("hopsworks_common.decorators.HAS_OPENAI", True)
    return module


@pytest.fixture
def istio(mocker, main_client):
    istio_client = mocker.MagicMock()
    istio_client._base_url = "https://istio.example.com"
    istio_client._auth._token = "key.secret"
    istio_client._verify = True
    mocker.patch(
        "hopsworks_common.client.istio._get_instance", return_value=istio_client
    )
    return istio_client


@pytest.fixture
def main_client(mocker):
    main = mocker.MagicMock()
    main._verify = True
    mocker.patch("hopsworks_common.client._get_instance", return_value=main)
    return main


class TestOpenAI:
    @pytest.mark.parametrize(
        ("project_name", "project_namespace"),
        [
            ("myproject", "myproject"),
            ("my_project", "my-project"),
            ("model_serving_int_MhGJP", "loadtest-kserve-0-main-000"),
        ],
    )
    def test_get_openai_url_uses_the_namespace(
        self, istio, project_name, project_namespace
    ):
        ms = model_serving.ModelServing(
            project_name, 1, project_namespace=project_namespace
        )

        assert (
            ms.get_openai_url()
            == f"https://istio.example.com/v1/{project_namespace}/v1"
        )

    def test_get_openai_url_is_none_without_istio(self, mocker):
        mocker.patch("hopsworks_common.client.istio._get_instance", return_value=None)
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        assert ms.get_openai_url() is None

    def test_get_openai_url_fetches_the_namespace_when_not_given(
        self, mocker, istio, ms
    ):
        connection = mocker.patch("hsml.model_serving.client._get_connection")
        connection.return_value._get_project.return_value.project_namespace = "lazy"

        assert ms.get_openai_url() == "https://istio.example.com/v1/lazy/v1"
        assert ms.get_openai_url() == "https://istio.example.com/v1/lazy/v1"
        connection.return_value._get_project.assert_called_once()

    def test_get_openai_url_raises_when_the_namespace_is_unknown(
        self, mocker, istio, ms
    ):
        connection = mocker.patch("hsml.model_serving.client._get_connection")
        connection.return_value._get_project.return_value.project_namespace = None

        with pytest.raises(ModelServingException, match="namespace"):
            ms.get_openai_url()

    def test_project_namespace_property(self, ms):
        assert ms.project_namespace is None
        assert (
            model_serving.ModelServing("p", 1, project_namespace="ns").project_namespace
            == "ns"
        )

    def test_get_openai_client_builds_the_client(self, istio, stub_openai):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client()

        assert client.kwargs == {
            "base_url": "https://istio.example.com/v1/ns/v1",
            "api_key": "key.secret",
            "default_headers": {"Authorization": "ApiKey key.secret"},
        }

    def test_get_openai_client_merges_caller_headers(self, istio, stub_openai):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client(default_headers={"X-Trace": "1"})

        assert client.kwargs["default_headers"] == {
            "Authorization": "ApiKey key.secret",
            "X-Trace": "1",
        }

    def test_get_openai_client_lets_the_caller_override_authorization(
        self, istio, stub_openai
    ):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client(default_headers={"Authorization": "Bearer other"})

        assert client.kwargs["default_headers"] == {"Authorization": "Bearer other"}

    def test_get_openai_client_passes_other_kwargs_through(self, istio, stub_openai):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")
        http_client = object()

        client = ms.get_openai_client(
            timeout=5.0, max_retries=0, http_client=http_client
        )

        assert client.kwargs["timeout"] == 5.0
        assert client.kwargs["max_retries"] == 0
        assert client.kwargs["http_client"] is http_client

    def test_get_openai_client_verifies_by_default(
        self, istio, stub_openai, main_client
    ):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client()

        assert "http_client" not in client.kwargs

    def test_get_openai_client_ignores_the_istio_verify_flag(
        self, istio, stub_openai, main_client
    ):
        istio._verify = False
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client()

        assert "http_client" not in client.kwargs

    def test_get_openai_client_disables_verification_without_hostname_verification(
        self, istio, stub_openai, main_client
    ):
        main_client._verify = False
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client()

        assert client.kwargs["http_client"].kwargs == {"verify": False}

    def test_get_openai_client_uses_the_trust_store_as_ssl_context(
        self, mocker, istio, stub_openai, main_client
    ):
        main_client._verify = "/srv/hops/trust/ca.pem"
        context = object()
        create_context = mocker.patch(
            "hsml.core.openai_client.ssl.create_default_context",
            return_value=context,
        )
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client()

        create_context.assert_called_once_with(cafile="/srv/hops/trust/ca.pem")
        assert client.kwargs["http_client"].kwargs == {"verify": context}

    def test_get_openai_client_loads_a_trust_store_directory(
        self, mocker, tmp_path, istio, stub_openai, main_client
    ):
        main_client._verify = str(tmp_path)
        context = object()
        create_context = mocker.patch(
            "hsml.core.openai_client.ssl.create_default_context",
            return_value=context,
        )
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        client = ms.get_openai_client()

        create_context.assert_called_once_with(capath=str(tmp_path))
        assert client.kwargs["http_client"].kwargs == {"verify": context}

    def test_get_openai_client_keeps_the_callers_transport(
        self, istio, stub_openai, main_client
    ):
        main_client._verify = False
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")
        http_client = object()

        client = ms.get_openai_client(http_client=http_client)

        assert client.kwargs["http_client"] is http_client

    @pytest.mark.parametrize("reserved", ["base_url", "api_key"])
    def test_get_openai_client_rejects_reserved_kwargs(
        self, istio, stub_openai, reserved
    ):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        with pytest.raises(ValueError, match=reserved):
            ms.get_openai_client(**{reserved: "x"})

    def test_get_openai_client_raises_without_istio(self, mocker, stub_openai):
        mocker.patch("hopsworks_common.client.istio._get_instance", return_value=None)
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        with pytest.raises(ModelServingException, match="Istio"):
            ms.get_openai_client()

    def test_get_openai_client_requires_the_openai_package(self, istio, mocker):
        mocker.patch("hopsworks_common.decorators.HAS_OPENAI", False)
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")

        with pytest.raises(ModuleNotFoundError, match="hopsworks\\[openai\\]"):
            ms.get_openai_client()


class TestLlmGateway:
    def test_get_llm_gateway_adds_the_url(self, mocker, istio):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")
        mocker.patch.object(
            ms._serving_api,
            "_get_llm_gateway",
            return_value={"enabled": True, "available": False},
        )

        assert ms.get_llm_gateway() == {
            "enabled": True,
            "available": False,
            "url": "https://istio.example.com/v1/ns/v1",
        }

    def test_set_llm_gateway_puts_the_switch(self, mocker, istio):
        ms = model_serving.ModelServing("proj", 1, project_namespace="ns")
        put = mocker.patch.object(
            ms._serving_api,
            "_set_llm_gateway",
            return_value={"enabled": False, "available": True},
        )

        state = ms.set_llm_gateway(False)

        put.assert_called_once_with(False)
        assert state["enabled"] is False
        assert state["available"] is True


class TestDeployAgentFrameworkAndChatConfig:
    AGENT_APP = (
        "from hopsworks_agents.protocol import AgentApp\n"
        'agent_app = AgentApp(name="a", framework="{framework}")\n'
    )

    def _deploy(self, ms, mocker, entry, existing=None, **kwargs):
        mocker.patch.object(ms, "get_deployment", return_value=existing)
        mocker.patch("hsml.model_serving.Predictor.for_server")
        predictor = model_serving.Predictor.for_server.return_value
        ms.deploy_agent(entry=str(entry), name="my_agent", **kwargs)
        return predictor

    def test_reads_framework_and_contract_from_agent_app(
        self, ms, mocker, stub_apis, tmp_path
    ):
        # Arrange
        script = tmp_path / "agent.py"
        script.write_text(self.AGENT_APP.format(framework="LangGraph"))

        # Act
        predictor = self._deploy(ms, mocker, script)

        # Assert
        assert predictor._agent_framework == "langgraph"
        assert predictor._chat_config == {"protocol": "hopsworks"}

    def test_explicit_arguments_win(self, ms, mocker, stub_apis, tmp_path):
        # Arrange
        script = tmp_path / "agent.py"
        script.write_text(self.AGENT_APP.format(framework="langgraph"))

        # Act
        predictor = self._deploy(
            ms, mocker, script, framework="openai_agents", chat_config="openai"
        )

        # Assert
        assert predictor._agent_framework == "openai_agents"
        assert predictor._chat_config == {"protocol": "openai"}

    def test_reads_package_modules(self, ms, mocker, stub_apis, tmp_path):
        # Arrange
        (tmp_path / "pyproject.toml").write_text("[project]\nname='p'\n")
        pkg = tmp_path / "src" / "p"
        pkg.mkdir(parents=True)
        (pkg / "app.py").write_text(
            self.AGENT_APP.format(framework="claude_agents").replace(
                "AgentApp(", "protocol.AgentApp("
            )
        )
        mocker.patch(
            "hsml.model_serving._build_and_install_package", return_value="w.py"
        )

        # Act
        predictor = self._deploy(ms, mocker, tmp_path)

        # Assert
        assert predictor._agent_framework == "claude_agents"
        assert predictor._chat_config == {"protocol": "hopsworks"}

    def test_plain_script_gets_neither(self, ms, mocker, stub_apis, tmp_path):
        # Arrange
        script = tmp_path / "agent.py"
        script.write_text("class Predict:\n    pass\n")

        # Act
        predictor = self._deploy(ms, mocker, script)

        # Assert
        assert predictor._agent_framework is None
        assert predictor._chat_config is None

    def test_redeploy_keeps_stored_framework_and_contract(
        self, ms, mocker, stub_apis, tmp_path
    ):
        # The backend overwrites the framework with whatever arrives, and keeps the stored
        # chat contract only when none is sent.
        # Arrange
        script = tmp_path / "agent.py"
        script.write_text("from x import AgentApp\napp = AgentApp(name='a')\n")
        existing = mocker.MagicMock()
        existing.predictor.memory = None
        existing.predictor.agent_framework = "llamaindex"

        # Act
        predictor = self._deploy(ms, mocker, script, existing=existing)

        # Assert
        assert predictor._agent_framework == "llamaindex"
        assert predictor._chat_config is None
        existing.save.assert_called_once()

    def test_git_source_is_not_read(self, ms, mocker, stub_apis):
        # Act
        predictor = self._deploy(
            ms,
            mocker,
            "agent.py",
            git_url="https://github.com/acme/repo.git",
            git_provider="GitHub",
        )

        # Assert
        assert predictor._agent_framework is None
        assert predictor._chat_config is None

    @pytest.mark.parametrize(
        ("literal", "stored"),
        [
            ("claude-agent-sdk", "claude_agents"),
            ("OpenAI-Agents", "openai_agents"),
            # AgentApp runs an unknown name as custom, and the backend refuses it.
            ("crewai", "custom"),
        ],
    )
    def test_a_literal_is_recorded_as_the_backend_spells_it(
        self, ms, mocker, stub_apis, tmp_path, literal, stored
    ):
        # Arrange
        script = tmp_path / "agent.py"
        script.write_text(self.AGENT_APP.format(framework=literal))

        # Act
        predictor = self._deploy(ms, mocker, script)

        # Assert
        assert predictor._agent_framework == stored

    def test_an_explicit_alias_is_resolved(self, ms, mocker, stub_apis, tmp_path):
        # Arrange
        script = tmp_path / "agent.py"
        script.write_text("print('hi')")

        # Act
        predictor = self._deploy(ms, mocker, script, framework="Claude-Agents")

        # Assert
        assert predictor._agent_framework == "claude_agents"

    def _package(self, tmp_path, mocker, files):
        (tmp_path / "pyproject.toml").write_text("[project]\nname='p'\n")
        for relpath, framework in files.items():
            path = tmp_path / relpath
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self.AGENT_APP.format(framework=framework))
        mocker.patch(
            "hsml.model_serving._build_and_install_package", return_value="w.py"
        )

    def test_package_scan_skips_examples_tests_and_venvs(
        self, ms, mocker, stub_apis, tmp_path
    ):
        # Arrange: examples/ sorts before src/ and must not decide the framework.
        self._package(
            tmp_path,
            mocker,
            {
                "examples/demo.py": "langgraph",
                "tests/test_app.py": "llamaindex",
                "myenv/lib/site.py": "openai_agents",
                "src/p/app.py": "claude_agents",
            },
        )
        (tmp_path / "myenv" / "pyvenv.cfg").write_text("home = /usr\n")

        # Act
        predictor = self._deploy(ms, mocker, tmp_path)

        # Assert
        assert predictor._agent_framework == "claude_agents"

    def test_package_with_disagreeing_literals_records_none(
        self, ms, mocker, stub_apis, tmp_path
    ):
        # Arrange
        self._package(
            tmp_path,
            mocker,
            {"src/p/a.py": "langgraph", "src/p/b.py": "claude_agents"},
        )

        # Act
        predictor = self._deploy(ms, mocker, tmp_path)

        # Assert
        assert predictor._agent_framework is None
        assert predictor._chat_config == {"protocol": "hopsworks"}

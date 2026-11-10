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

from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
from hopsworks_common.app import App
from hopsworks_common.core.app_api import AppApi


class TestAppApiCreate:
    class _ProviderLike:
        def __init__(self, git_provider: str):
            self.git_provider = git_provider

    @pytest.fixture
    def mock_client(self, mocker):
        client_mock = Mock()
        client_mock._project_id = 119
        client_mock._project_name = "demo"
        client_mock._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.app_api.client._get_instance",
            return_value=client_mock,
        )
        return client_mock

    @pytest.fixture
    def api(self, mocker):
        api = AppApi()
        mocker.patch.object(api, "get_app", return_value=App(name="created"))
        return api

    def test_create_streamlit_app_defaults(self, mock_client, api, mocker):
        mocker.patch(
            "hopsworks_common.core.app_api.util._convert_to_abs",
            return_value="/Projects/demo/Resources/app.py",
        )

        created = api.create_app("my_app", app_path="Resources/app.py")

        assert created.name == "created"
        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["type"] == "pythonAppJobConfiguration"
        assert body["appName"] == "my_app"
        assert body["appKind"] == "STREAMLIT"
        assert body["appPath"] == "hdfs:///Projects/demo/Resources/app.py"
        assert body["environmentName"] == "python-app-pipeline"
        assert body["resourceConfig"] == {
            "memory": 2048,
            "cores": 1.0,
            "gpus": 0,
            "shmSize": 128,
        }
        assert "entrypointCommand" not in body
        assert "appPort" not in body
        assert "description" not in body

    def test_create_streamlit_app_requires_path(self, mock_client, api):
        with pytest.raises(ValueError, match="app_path is required"):
            api.create_app("my_app")

    def test_create_streamlit_git_app_payload(self, mock_client, api):
        api.create_app(
            "streamlit_git_app",
            app_kind="STREAMLIT",
            git_url="https://github.com/gibchikafa/appshopsworkstests.git",
            git_provider="github",
            git_branch="main",
            entrypoint_script="streamlitapp.py",
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["appKind"] == "STREAMLIT"
        assert body["gitUrl"] == "https://github.com/gibchikafa/appshopsworkstests.git"
        assert body["gitProvider"] == "GitHub"
        assert body["gitBranch"] == "main"
        assert body["entrypointScript"] == "streamlitapp.py"
        assert body["gitAutoRedeploy"] is False
        assert "appPath" not in body
        assert "entrypointCommand" not in body
        assert "appPort" not in body

    def test_create_git_app_with_auto_redeploy(self, mock_client, api):
        api.create_app(
            "streamlit_git_app",
            app_kind="STREAMLIT",
            git_url="https://github.com/gibchikafa/appshopsworkstests.git",
            git_provider="github",
            git_branch="main",
            git_auto_redeploy=True,
            entrypoint_script="streamlitapp.py",
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["gitAutoRedeploy"] is True

    def test_create_app_rejects_auto_redeploy_without_git_source(
        self, mock_client, api
    ):
        # Mirrors PythonAppJobValidator, so the caller gets the reason locally
        # rather than a REST error.
        with pytest.raises(ValueError, match="git_auto_redeploy is only supported"):
            api.create_app(
                "file_app",
                app_kind="STREAMLIT",
                app_path="Resources/app.py",
                git_auto_redeploy=True,
            )

    def test_create_streamlit_git_app_accepts_provider_like_object(
        self, mock_client, api
    ):
        api.create_app(
            "streamlit_git_app",
            app_kind="STREAMLIT",
            git_url="https://github.com/gibchikafa/appshopsworkstests.git",
            git_provider=self._ProviderLike("github"),
            entrypoint_script="streamlitapp.py",
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["gitProvider"] == "GitHub"

    def test_create_streamlit_git_app_requires_entrypoint_script(
        self, mock_client, api
    ):
        with pytest.raises(ValueError, match="entrypoint_script is required"):
            api.create_app(
                "streamlit_git_app",
                app_kind="STREAMLIT",
                git_url="https://github.com/gibchikafa/appshopsworkstests.git",
                git_provider="GitHub",
            )

    def test_create_custom_app_payload(self, mock_client, api, mocker):
        mocker.patch(
            "hopsworks_common.core.app_api.util._convert_to_abs",
            return_value="/Projects/demo/Resources/fastapi_custom_app.py",
        )

        api.create_app(
            "fastapi_app",
            app_path="Resources/fastapi_custom_app.py",
            app_kind="custom",
            entrypoint_command=(
                "python -m uvicorn fastapi_custom_app:app --host 0.0.0.0 "
                '--port "$APP_PORT"'
            ),
            app_port=8080,
            description="FastAPI demo",
            app_base_path="/myapp",
            readiness_probe_path="/health",
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["appKind"] == "CUSTOM"
        assert (
            body["appPath"] == "hdfs:///Projects/demo/Resources/fastapi_custom_app.py"
        )
        assert body["entrypointCommand"] == (
            'python -m uvicorn fastapi_custom_app:app --host 0.0.0.0 --port "$APP_PORT"'
        )
        assert body["appPort"] == 8080
        assert body["description"] == "FastAPI demo"
        assert body["appBasePath"] == "/myapp"
        assert body["readinessProbePath"] == "/health"
        assert body["proxyPathMode"] == "ROOT"
        assert body["dbAccess"] is True

    def test_create_app_without_db_access(self, mock_client, api):
        api.create_app("dash", app_path="Resources/dash.py", db_access=False)

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["dbAccess"] is False

    def test_create_app_leaves_idle_scale_to_zero_to_the_cluster_default(
        self, mock_client, api
    ):
        api.create_app("dash", app_path="Resources/dash.py")

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert "idleScaleToZero" not in body
        assert "idleCooldownSeconds" not in body
        assert "coldStartTimeoutSeconds" not in body

    def test_create_app_with_idle_scale_to_zero_off_sends_it_off(
        self, mock_client, api
    ):
        api.create_app("dash", app_path="Resources/dash.py", idle_scale_to_zero=False)

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["idleScaleToZero"] is False
        assert "idleCooldownSeconds" not in body

    def test_create_app_idle_timeouts_alone_turn_the_option_on(self, mock_client, api):
        api.create_app("dash", app_path="Resources/dash.py", idle_cooldown_seconds=120)

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["idleScaleToZero"] is True
        assert body["idleCooldownSeconds"] == 120

    def test_create_app_with_idle_scale_to_zero_sends_the_timeouts(
        self, mock_client, api
    ):
        api.create_app(
            "dash",
            app_path="Resources/dash.py",
            idle_scale_to_zero=True,
            idle_cooldown_seconds=120,
            cold_start_timeout_seconds=45,
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["idleScaleToZero"] is True
        assert body["idleCooldownSeconds"] == 120
        assert body["coldStartTimeoutSeconds"] == 45

    def test_create_app_with_idle_scale_to_zero_leaves_the_defaults_to_the_backend(
        self, mock_client, api
    ):
        api.create_app("dash", app_path="Resources/dash.py", idle_scale_to_zero=True)

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["idleScaleToZero"] is True
        assert "idleCooldownSeconds" not in body
        assert "coldStartTimeoutSeconds" not in body

    def test_create_app_rejects_idle_timeouts_with_the_option_off(
        self, mock_client, api
    ):
        with pytest.raises(ValueError, match="idle_scale_to_zero"):
            api.create_app(
                "dash",
                app_path="Resources/dash.py",
                idle_scale_to_zero=False,
                idle_cooldown_seconds=120,
            )

    def test_app_reads_idle_fields_from_the_response(self):
        a = App.from_response_json(
            {
                "name": "dash",
                "state": "IDLE",
                "idleScaleToZero": True,
                "idleCooldownSeconds": 120,
                "coldStartTimeoutSeconds": 45,
            }
        )
        assert a.state == "IDLE"
        assert a.idle_scale_to_zero is True
        assert a.idle_cooldown_seconds == 120
        assert a.cold_start_timeout_seconds == 45

    def test_idle_app_keeps_its_url(self, mock_client):
        mock_client._base_url = "https://hopsworks.example.com"
        a = App.from_response_json(
            {
                "name": "dash",
                "state": "IDLE",
                "serving": False,
                "appUrl": "pythonapp/demo/dash/",
                "idleScaleToZero": True,
            }
        )
        assert (
            a.app_url
            == "https://hopsworks.example.com/hopsworks-api/pythonapp/demo/dash/"
        )

    def test_running_but_not_serving_app_has_no_url(self, mock_client):
        mock_client._base_url = "https://hopsworks.example.com"
        a = App.from_response_json(
            {
                "name": "dash",
                "state": "RUNNING",
                "serving": False,
                "appUrl": "pythonapp/demo/dash/",
            }
        )
        assert a.app_url is None

    def test_app_from_an_older_server_does_not_scale_to_zero(self):
        a = App.from_response_json({"name": "dash", "state": "RUNNING"})
        assert a.idle_scale_to_zero is False
        assert a.idle_cooldown_seconds is None

    def test_create_custom_app_without_path_omits_app_path(self, mock_client, api):
        api.create_app(
            "flask_app",
            app_kind="CUSTOM",
            entrypoint_command=(
                "python -m pip install --no-cache-dir flask && "
                'exec python -m flask --app flask_custom_app run --host 0.0.0.0 --port "$APP_PORT"'
            ),
            app_port=8080,
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["appKind"] == "CUSTOM"
        assert "appPath" not in body
        assert body["appPort"] == 8080
        assert body["entrypointCommand"].startswith(
            "python -m pip install --no-cache-dir flask"
        )

    def test_create_custom_git_app_payload(self, mock_client, api):
        api.create_app(
            "fastapi_app",
            app_kind="CUSTOM",
            git_url="https://github.com/gibchikafa/appshopsworkstests.git",
            git_provider="GitHub",
            git_branch="main",
            entrypoint_command=(
                'python -m uvicorn fastapiapp:app --host 0.0.0.0 --port "$APP_PORT"'
            ),
            app_port=8080,
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["appKind"] == "CUSTOM"
        assert body["gitUrl"] == "https://github.com/gibchikafa/appshopsworkstests.git"
        assert body["gitProvider"] == "GitHub"
        assert body["gitBranch"] == "main"
        assert body["entrypointCommand"] == (
            'python -m uvicorn fastapiapp:app --host 0.0.0.0 --port "$APP_PORT"'
        )
        assert body["appPort"] == 8080
        assert "appPath" not in body
        assert "entrypointScript" not in body

    def test_create_custom_app_requires_entrypoint(self, mock_client, api):
        with pytest.raises(ValueError, match="entrypoint_command is required"):
            api.create_app("custom_app", app_kind="CUSTOM", app_path=None)

    def test_redeploy_posts_to_backend(self, mock_client):
        api = AppApi()
        api._redeploy("my_app")

        assert mock_client._send_request.call_args.args[0] == "POST"
        assert mock_client._send_request.call_args.args[1] == [
            "project",
            119,
            "apps",
            "my_app",
            "redeploy",
        ]

    def test_set_public_enable_posts_to_backend(self, mock_client):
        api = AppApi()
        api._set_public("my_app", True)

        assert mock_client._send_request.call_args.args[0] == "POST"
        assert mock_client._send_request.call_args.args[1] == [
            "project",
            119,
            "apps",
            "my_app",
            "public",
        ]
        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body == {"enabled": True}

    def test_set_public_disable_sends_enabled_false(self, mock_client):
        api = AppApi()
        api._set_public("my_app", False)

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body == {"enabled": False}

    def test_get_tables_gets_the_tables_path(self, mock_client):
        AppApi()._get_tables("my_app")

        assert mock_client._send_request.call_args.args == (
            "GET",
            ["project", 119, "apps", "my_app", "tables"],
        )

    def test_register_table_posts_the_name(self, mock_client):
        AppApi()._register_table("my_app", "app_my_app__notes")

        assert mock_client._send_request.call_args.args == (
            "POST",
            ["project", 119, "apps", "my_app", "tables"],
        )
        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body == {"name": "app_my_app__notes"}

    def test_get_table_gets_the_table_path(self, mock_client):
        AppApi()._get_table("my_app", "app_my_app__notes")

        assert mock_client._send_request.call_args.args == (
            "GET",
            ["project", 119, "apps", "my_app", "tables", "app_my_app__notes"],
        )

    def test_preview_table_passes_the_limit(self, mock_client):
        AppApi()._preview_table("my_app", "app_my_app__notes", 5)

        assert mock_client._send_request.call_args.args == (
            "GET",
            [
                "project",
                119,
                "apps",
                "my_app",
                "tables",
                "app_my_app__notes",
                "preview",
            ],
        )
        assert mock_client._send_request.call_args.kwargs["query_params"] == {
            "limit": 5
        }


class TestAppApiCreateKinds:
    @pytest.fixture
    def mock_client(self, mocker):
        client_mock = Mock()
        client_mock._project_id = 119
        client_mock._project_name = "demo"
        client_mock._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.app_api.client._get_instance",
            return_value=client_mock,
        )
        return client_mock

    @pytest.fixture
    def api(self, mocker):
        api = AppApi()
        mocker.patch.object(api, "get_app", return_value=App(name="created"))
        return api

    @staticmethod
    def _body(mock_client):
        return json.loads(mock_client._send_request.call_args.kwargs["data"])

    def test_flask_app_is_launched_from_its_file_on_a_port(self, mock_client, api):
        api.create_app(
            "web", app_path="Resources/app.py", app_kind="flask", app_port=5000
        )
        body = self._body(mock_client)
        assert body["appKind"] == "FLASK"
        assert body["appPath"] == "hdfs:///Projects/demo/Resources/app.py"
        assert body["appPort"] == 5000
        assert "entrypointCommand" not in body

    def test_node_app_takes_javascript_and_typescript_files(self, mock_client, api):
        for entry in (
            "server.js",
            "server.mjs",
            "server.cjs",
            "server.ts",
            "server.mts",
        ):
            api.create_app("api", app_path=f"Resources/{entry}", app_kind="NODEJS")
            assert self._body(mock_client)["appKind"] == "NODEJS"
        with pytest.raises(ValueError, match="app_path must be a .js"):
            api.create_app("api", app_path="Resources/app.py", app_kind="NODEJS")

    def test_gradio_git_app_needs_an_entrypoint_script(self, mock_client, api):
        with pytest.raises(
            ValueError, match="entrypoint_script is required for Gradio"
        ):
            api.create_app(
                "demo",
                app_kind="GRADIO",
                git_url="https://github.com/acme/demo",
                git_provider="GitHub",
            )
        api.create_app(
            "demo",
            app_kind="GRADIO",
            git_url="https://github.com/acme/demo",
            git_provider="GitHub",
            entrypoint_script="demo/app.py",
            app_port=7860,
        )
        body = self._body(mock_client)
        assert body["entrypointScript"] == "demo/app.py"
        assert body["appPort"] == 7860
        assert "appPath" not in body

    def test_file_launched_kinds_refuse_an_entrypoint_command(self, mock_client, api):
        with pytest.raises(
            ValueError, match="entrypoint_command is only used for custom apps"
        ):
            api.create_app(
                "web",
                app_path="Resources/app.py",
                app_kind="FLASK",
                entrypoint_command="flask run",
            )
        with pytest.raises(ValueError, match="app_path is required for Flask"):
            api.create_app("web", app_kind="FLASK")

    def test_unknown_kind_is_refused(self, mock_client, api):
        with pytest.raises(ValueError, match="Unknown app_kind"):
            api.create_app("web", app_path="Resources/app.py", app_kind="DJANGO")

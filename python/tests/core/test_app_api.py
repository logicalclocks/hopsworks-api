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
    @pytest.fixture
    def mock_client(self, mocker):
        client_mock = Mock()
        client_mock._project_id = 119
        client_mock._project_name = "demo"
        client_mock._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.app_api.client.get_instance",
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
            "hopsworks_common.core.app_api.util.convert_to_abs",
            return_value="hdfs:///Projects/demo/Resources/app.py",
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
        assert "appPath" not in body
        assert "entrypointCommand" not in body
        assert "appPort" not in body

    def test_create_streamlit_git_app_requires_entrypoint_script(self, mock_client, api):
        with pytest.raises(ValueError, match="entrypoint_script is required"):
            api.create_app(
                "streamlit_git_app",
                app_kind="STREAMLIT",
                git_url="https://github.com/gibchikafa/appshopsworkstests.git",
                git_provider="GitHub",
            )

    def test_create_custom_app_payload(self, mock_client, api, mocker):
        mocker.patch(
            "hopsworks_common.core.app_api.util.convert_to_abs",
            return_value="hdfs:///Projects/demo/Resources/fastapi_custom_app.py",
        )

        api.create_app(
            "fastapi_app",
            app_path="Resources/fastapi_custom_app.py",
            app_kind="custom",
            entrypoint_command=(
                'python -m uvicorn fastapi_custom_app:app --host 0.0.0.0 '
                '--port "$APP_PORT"'
            ),
            app_port=8080,
            description="FastAPI demo",
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["appKind"] == "CUSTOM"
        assert body["appPath"] == "hdfs:///Projects/demo/Resources/fastapi_custom_app.py"
        assert body["entrypointCommand"] == (
            'python -m uvicorn fastapi_custom_app:app --host 0.0.0.0 --port "$APP_PORT"'
        )
        assert body["appPort"] == 8080
        assert body["description"] == "FastAPI demo"

    def test_create_custom_app_without_path_omits_app_path(self, mock_client, api):
        api.create_app(
            "flask_app",
            app_kind="CUSTOM",
            entrypoint_command=(
                'python -m pip install --no-cache-dir flask && '
                'exec python -m flask --app flask_custom_app run --host 0.0.0.0 --port "$APP_PORT"'
            ),
            app_port=8080,
        )

        body = json.loads(mock_client._send_request.call_args.kwargs["data"])
        assert body["appKind"] == "CUSTOM"
        assert "appPath" not in body
        assert body["appPort"] == 8080
        assert body["entrypointCommand"].startswith("python -m pip install --no-cache-dir flask")

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

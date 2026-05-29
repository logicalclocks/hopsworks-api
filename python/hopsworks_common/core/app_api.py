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
import logging
from typing import TYPE_CHECKING

from hopsworks_apigen import public
from hopsworks_common import client, usage, util


if TYPE_CHECKING:
    from hopsworks_common import app


_GIT_PROVIDER_ALIASES = {
    "github": "GitHub",
    "gitlab": "GitLab",
    "bitbucket": "BitBucket",
}


@public("hopsworks.core.app_api.AppApi")
class AppApi:
    def __init__(self):
        self._log = logging.getLogger(__name__)

    @public
    @usage.method_logger
    def get_apps(self) -> list[app.App]:
        """Get all apps in the project.

        Returns:
            List of App objects.
        """
        from hopsworks_common import app

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "apps"]
        headers = {"content-type": "application/json"}
        response = _client._send_request("GET", path_params, headers=headers)
        return app.App.from_response_json_list(response)

    @public
    @usage.method_logger
    def get_app(self, name: str) -> app.App:
        """Get an app by name.

        Parameters:
            name: Name of the app.

        Returns:
            App object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the app does not exist or the backend encounters an error.
        """
        from hopsworks_common import app

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "apps", name]
        headers = {"content-type": "application/json"}
        response = _client._send_request("GET", path_params, headers=headers)
        return app.App.from_response_json(response)

    @public
    @usage.method_logger
    def create_app(
        self,
        name: str,
        app_path: str | None = None,
        environment: str = "python-app-pipeline",
        memory: int = 2048,
        cores: float = 1.0,
        env_vars: dict[str, str] | None = None,
        app_kind: str = "STREAMLIT",
        entrypoint_command: str | None = None,
        app_port: int | None = None,
        description: str | None = None,
        git_url: str | None = None,
        git_provider: str | None = None,
        git_branch: str | None = None,
        entrypoint_script: str | None = None,
    ) -> app.App:
        """Create a new Python app.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()
            apps = project.get_app_api()

            app = apps.create_app(
                "my_dashboard",
                app_path="Resources/app.py",
            )

            app.run()
            print(app.app_url)
            ```

        Parameters:
            name: Name of the app.
            app_path: Path to the app file in HopsFS.
            environment: Python environment name (default: "python-app-pipeline").
            memory: Memory in MB (default: 2048).
            cores: CPU cores (default: 1.0).
            env_vars: Per-runtime env vars applied when the app is started.
                These override account-level env vars for this app's executions.
            app_kind: App kind to create. Defaults to ``STREAMLIT``.
            entrypoint_command: Startup command for non-Streamlit apps.
            app_port: Port exposed by non-Streamlit apps.
            description: Optional app description.
            git_url: Optional Git repository URL. When set, the app is cloned on
                every start.
            git_provider: Git provider for git-backed apps (GitHub, GitLab or
                BitBucket).
            git_branch: Optional branch to clone for git-backed apps.
            entrypoint_script: Relative entrypoint script for Streamlit git apps.

        Returns:
            The created App object.
        """
        _client = client.get_instance()

        app_kind_name = str(getattr(app_kind, "name", app_kind) or "STREAMLIT").upper()
        app_path = self._trim_to_none(app_path)
        entrypoint_command = self._trim_to_none(entrypoint_command)
        git_url = self._trim_to_none(git_url)
        git_provider = self._normalize_git_provider(git_provider)
        git_branch = self._trim_to_none(git_branch)
        entrypoint_script = self._trim_to_none(entrypoint_script)
        git_repo_app = bool(git_url)
        streamlit_app = app_kind_name == "STREAMLIT"

        if streamlit_app:
            if entrypoint_command:
                raise ValueError("entrypoint_command is only used for custom apps.")
            if git_repo_app:
                if not git_provider:
                    raise ValueError(
                        "git_provider is required for Git repository apps."
                    )
                if not entrypoint_script:
                    raise ValueError(
                        "entrypoint_script is required for Streamlit Git repository apps."
                    )
            elif not app_path:
                raise ValueError("app_path is required for Streamlit apps.")
            elif entrypoint_script:
                raise ValueError(
                    "entrypoint_script is only used for Streamlit Git repository apps."
                )
        else:
            if not entrypoint_command:
                raise ValueError("entrypoint_command is required for custom apps.")
            if git_repo_app and not git_provider:
                raise ValueError("git_provider is required for Git repository apps.")
            if entrypoint_script:
                raise ValueError(
                    "entrypoint_script is only used for Streamlit Git repository apps."
                )

        if app_path and not git_repo_app:
            app_path = util.convert_to_abs(app_path, _client._project_name)
            if not app_path.startswith("hdfs://"):
                app_path = "hdfs://" + app_path

        config = {
            "type": "pythonAppJobConfiguration",
            "appName": name,
            "resourceConfig": {
                "memory": memory,
                "cores": cores,
                "gpus": 0,
                "shmSize": 128,
            },
        }
        if app_path and not git_repo_app:
            config["appPath"] = app_path
        config["appKind"] = app_kind_name
        config["environmentName"] = environment
        if git_repo_app:
            config["gitUrl"] = git_url
            config["gitProvider"] = git_provider
            if git_branch:
                config["gitBranch"] = git_branch
            if streamlit_app:
                config["entrypointScript"] = entrypoint_script
        if not streamlit_app and entrypoint_command:
            config["entrypointCommand"] = entrypoint_command
        if app_kind_name != "STREAMLIT" and app_port is not None:
            config["appPort"] = app_port
        if description is not None:
            config["description"] = description

        path_params = ["project", _client._project_id, "jobs", name]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps(config)
        )

        created = self.get_app(name)
        # env_vars is a runtime-only override applied at start time; the backend
        # has no app-config field for it, so attach it to the returned object.
        created._env_vars = dict(env_vars) if env_vars else None
        return created

    def _start(self, app_name: str, env_vars: dict[str, str] | None = None):
        """Start an app execution.

        When ``env_vars`` is provided, POSTs a JSON body with ``envVars`` so the
        backend applies the runtime override; otherwise falls back to the legacy
        text/plain POST that Jersey dispatches to the no-body start handler.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
        ]
        if env_vars:
            headers = {"content-type": "application/json"}
            body = {"envVars": dict(env_vars)}
            return _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(body)
            )
        headers = {"content-type": "text/plain"}
        return _client._send_request("POST", path_params, headers=headers)

    def _stop(self, app_name: str, execution_id: int):
        """Stop an app execution."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
            execution_id,
            "status",
        ]
        headers = {"content-type": "application/json"}
        _client._send_request(
            "PUT", path_params, headers=headers, data=json.dumps({"state": "stopped"})
        )

    def _redeploy(self, app_name: str):
        """Redeploy a running app."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "apps",
            app_name,
            "redeploy",
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request("POST", path_params, headers=headers)

    def _get_log(self, app_name: str, execution_id: int, log_type: str) -> dict:
        """Get stdout or stderr log metadata for an app execution."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
            "executions",
            execution_id,
            "log",
            log_type,
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers) or {}

    def _delete(self, app_name: str):
        """Delete an app."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            app_name,
        ]
        _client._send_request("DELETE", path_params)

    def _trim_to_none(self, value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        trimmed = value.strip()
        return trimmed or None

    def _normalize_git_provider(self, git_provider: str | None) -> str | None:
        if hasattr(git_provider, "git_provider"):
            git_provider = git_provider.git_provider
        provider = self._trim_to_none(git_provider)
        if not provider:
            return None
        normalized = _GIT_PROVIDER_ALIASES.get(provider.lower())
        return normalized or provider

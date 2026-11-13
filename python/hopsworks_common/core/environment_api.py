#
#   Copyright 2022 Hopsworks AB
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
import time

import humps
from hopsworks_apigen import public
from hopsworks_common import client, command, decorators, environment, usage, util
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.engine import environment_engine


_COMMAND_NOT_FOUND_ERROR_CODE = 300008


def _build_custom_commands_request(
    project_name: str,
    path: str,
    artifacts: list[str] | None,
    env_vars: dict[str, str] | None,
) -> dict:
    """Validate custom-command inputs and normalize paths without modifying them."""
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a nonempty Hopsworks file path.")
    if artifacts is not None and (
        not isinstance(artifacts, list)
        or any(
            not isinstance(item, str) or not item or "," in item for item in artifacts
        )
    ):
        raise ValueError("artifacts must be a list of nonempty paths without commas.")
    if env_vars is not None and (
        not isinstance(env_vars, dict)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in env_vars.items()
        )
    ):
        raise ValueError("env_vars must map variable names to string values.")
    if any(
        any(character in text for character in "\n\r\0")
        for pair in (env_vars or {}).items()
        for text in pair
    ):
        raise ValueError("env_vars cannot contain newlines or NUL characters.")

    def _absolute(item: str) -> str:
        return (
            item if item.startswith("/") else util._convert_to_abs(item, project_name)
        )

    return {
        "commandsFile": _absolute(path),
        "artifacts": ",".join(_absolute(item) for item in artifacts or []),
        "envVars": [
            {"name": key, "value": value} for key, value in (env_vars or {}).items()
        ],
    }


@public("hopsworks.core.environment_api.EnvironmentApi")
class EnvironmentApi:
    def __init__(self):
        self._environment_engine = environment_engine.EnvironmentEngine()

    @public
    @usage._method_logger
    def create_environment(
        self,
        name: str,
        description: str | None = None,
        base_environment_name: str | None = "python-feature-pipeline",
        await_creation: bool | None = True,
        timeout: float | None = None,
    ) -> environment.Environment:
        """Create Python environment for the project.

        ```python
        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        new_env = env_api.create_environment("my_custom_environment", base_environment_name="python-feature-pipeline")
        ```

        Parameters:
            name: Name of the environment.
            description: Description of the environment.
            base_environment_name: The name of the environment to clone from.
            await_creation: Whether the method returns only when the creation is finished.
            timeout: Seconds to wait for the creation to finish before raising.
                Falls back to the engine's default when unset.
                Ignored when `await_creation` is `False`, because the method then returns without waiting at all.

        Returns:
            The Environment object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()

        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            name,
        ]
        headers = {"content-type": "application/json"}
        data = {
            "name": name,
            "baseImage": {"name": base_environment_name, "description": description},
        }
        env = environment.Environment.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(data)
            )
        )

        if await_creation:
            self._environment_engine._await_environment_command(name, timeout)

        return env

    @public
    @usage._method_logger
    def get_environments(self) -> list[environment.Environment]:
        """Get all available environments in the project.

        ```python
        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        envs = env_api.get_environments()
        ```

        Returns:
            List of Environment objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()

        path_params = ["project", _client._project_id, "python", "environments"]
        query_params = {"expand": ["libraries", "commands"]}
        headers = {"content-type": "application/json"}
        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )

    @public
    @usage._method_logger
    @decorators._catch_not_found(
        "hopsworks_common.environment.Environment", fallback_return=None
    )
    def get_environment(self, name: str) -> environment.Environment | None:
        """Get handle for a Python environment in the project.

        ```python
        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        env = env_api.get_environment("my_custom_environment")
        ```

        Parameters:
            name: Name of the environment.

        Returns:
            The Environment object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()

        path_params = ["project", _client._project_id, "python", "environments", name]
        query_params = {"expand": ["libraries", "commands"]}
        headers = {"content-type": "application/json"}
        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )

    def _run_custom_commands(self, name: str, request: dict, deadline: float) -> int:
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            name,
            "commands",
            "custom",
        ]
        response = _client._send_request(
            "POST",
            path_params,
            headers={"content-type": "application/json"},
            data=json.dumps(request),
            timeout=max(0, deadline - time.monotonic()),
            deadline=deadline,
        )
        return response["id"]

    def _get_command(
        self, name: str, command_id: int, request_timeout: float | None = None
    ) -> command.Command | None:
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            name,
            "commands",
            command_id,
        ]
        try:
            response = _client._send_request(
                "GET",
                path_params,
                headers={"content-type": "application/json"},
                timeout=request_timeout,
                deadline=time.monotonic() + request_timeout
                if request_timeout is not None
                else None,
            )
        except RestAPIError as error:
            # Successful commands are deleted; queued/running commands cannot be deleted and failures remain.
            if (
                error.response.status_code == 400
                and getattr(error, "error_code", None) == _COMMAND_NOT_FOUND_ERROR_CODE
            ):
                return None
            raise
        return command.Command(**humps.decamelize(response))

    def _delete(self, name):
        """Delete the Python environment.

        Parameters:
            name: Name of environment to delete.
        """
        _client = client._get_instance()

        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            name,
        ]
        headers = {"content-type": "application/json"}
        (_client._send_request("DELETE", path_params, headers=headers),)

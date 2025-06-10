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

import json
from typing import List, Optional

from hopsworks_common import client, decorators, environment, usage
from hopsworks_common.engine import environment_engine


class EnvironmentApi:
    def __init__(self):
        self._environment_engine = environment_engine.EnvironmentEngine()

    @usage.method_logger
    def create_environment(
        self,
        name: str,
        description: Optional[str] = None,
        base_environment_name: Optional[str] = "python-feature-pipeline",
        await_creation: Optional[bool] = True,
    ) -> environment.Environment:
        """Create Python environment for the project

        ```python

        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        new_env = env_api.create_environment("my_custom_environment", base_environment_name="python-feature-pipeline")


        ```
        # Arguments
            name: name of the environment
            base_environment_name: the name of the environment to clone from
            await_creation: bool. If True the method returns only when the creation is finished. Default True
        # Returns
            `Environment`: The Environment object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()

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
            self._environment_engine.await_environment_command(name)

        return env

    @usage.method_logger
    def get_environments(self) -> List[environment.Environment]:
        """Get all available environments in the project.

        ```python

        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        envs = env_api.get_environments()

        ```
        # Returns
            `List[Environment]`: List of Environment objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()

        path_params = ["project", _client._project_id, "python", "environments"]
        query_params = {"expand": ["libraries", "commands"]}
        headers = {"content-type": "application/json"}
        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )

    @usage.method_logger
    @decorators.catch_not_found(
        "hopsworks_common.environment.Environment", fallback_return=None
    )
    def get_environment(self, name: str) -> Optional[environment.Environment]:
        """Get handle for a Python environment in the project

        ```python

        import hopsworks

        project = hopsworks.login()

        env_api = project.get_environment_api()

        env = env_api.get_environment("my_custom_environment")

        ```
        # Arguments
            name: name of the environment
        # Returns
            `Environment`: The Environment object or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()

        path_params = ["project", _client._project_id, "python", "environments", name]
        query_params = {"expand": ["libraries", "commands"]}
        headers = {"content-type": "application/json"}
        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            )
        )

    def _delete(self, name):
        """Delete the Python environment.
        :param name: name of environment to delete
        :type environment: Environment
        """
        _client = client.get_instance()

        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            name,
        ]
        headers = {"content-type": "application/json"}
        (_client._send_request("DELETE", path_params, headers=headers),)

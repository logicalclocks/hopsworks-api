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

from hopsworks import client, environment


class EnvironmentApi:

    PYTHON_VERSION = "3.8"

    def __init__(
        self,
        project_id,
    ):
        self._project_id = project_id

    def create(self):
        """Create Python environment for the project"""
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            EnvironmentApi.PYTHON_VERSION,
        ]
        headers = {"content-type": "application/json"}
        _client._send_request("POST", path_params, headers=headers),

    def get(self):
        """Get handle for the Python environment for the project"""
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            EnvironmentApi.PYTHON_VERSION,
        ]
        query_params = {"expand": ["libraries", "commands"]}
        headers = {"content-type": "application/json"}
        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=query_params, headers=headers
            ),
            self._project_id,
        )

    def delete(self):
        """Delete the project Python environment"""
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            EnvironmentApi.PYTHON_VERSION,
        ]
        headers = {"content-type": "application/json"}
        _client._send_request("DELETE", path_params, headers=headers),

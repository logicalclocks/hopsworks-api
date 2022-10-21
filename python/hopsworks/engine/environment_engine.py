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

import time

from hopsworks import client, library, command
from hopsworks.core import environment_api


class EnvironmentEngine:
    def __init__(self, project_id):
        self._project_id = project_id

    def await_installation(self, library_name=None):
        commands = [command.Command(status="ONGOING")]
        while len(commands) > 0 and not self._command_final_status(commands[0]):
            time.sleep(5)
            commands = self._poll_commands_library(library_name).commands

    def _command_final_status(self, command):
        return command.status == "FAILED" or command.status == "SUCCESS"

    def _poll_commands_library(self, library_name):
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            environment_api.EnvironmentApi.PYTHON_VERSION,
            "libraries",
            library_name,
        ]

        query_params = {"expand": "commands"}
        headers = {"content-type": "application/json"}

        return library.Library.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            ),
            None,
            None,
        )

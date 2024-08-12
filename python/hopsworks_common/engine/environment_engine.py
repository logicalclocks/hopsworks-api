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

from hopsworks_common import client, command, environment, library
from hopsworks_common.client.exceptions import EnvironmentException, RestAPIError


class EnvironmentEngine:
    def await_library_command(self, environment_name, library_name):
        commands = [command.Command(status="ONGOING")]
        while len(commands) > 0 and not self._is_final_status(commands[0]):
            time.sleep(5)
            library = self._poll_commands_library(environment_name, library_name)
            if library is None:
                commands = []
            else:
                commands = library._commands

    def await_environment_command(self, environment_name):
        commands = [command.Command(status="ONGOING")]
        while len(commands) > 0 and not self._is_final_status(commands[0]):
            time.sleep(5)
            environment = self._poll_commands_environment(environment_name)
            if environment is None:
                commands = []
            else:
                commands = environment._commands

    def _is_final_status(self, command):
        if command.status == "FAILED":
            raise EnvironmentException(
                "Command failed with stacktrace: \n{}".format(command.error_message)
            )
        elif command.status == "SUCCESS":
            return True
        else:
            return False

    def _poll_commands_library(self, environment_name, library_name):
        _client = client.get_instance()

        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            environment_name,
            "libraries",
            library_name,
        ]

        query_params = {"expand": "commands"}
        headers = {"content-type": "application/json"}

        try:
            return library.Library.from_response_json(
                _client._send_request(
                    "GET", path_params, headers=headers, query_params=query_params
                ),
            )
        except RestAPIError as e:
            if (
                e.response.json().get("errorCode", "") == 300003
                and e.response.status_code == 404
            ):
                return None

    def _poll_commands_environment(self, environment_name):
        _client = client.get_instance()

        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            environment_name,
        ]

        query_params = {"expand": "commands"}
        headers = {"content-type": "application/json"}

        return environment.Environment.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            ),
        )

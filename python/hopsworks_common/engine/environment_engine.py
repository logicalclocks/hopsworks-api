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

import time

from hopsworks_apigen import also_available_as
from hopsworks_common import client, command, environment, library
from hopsworks_common.client.exceptions import EnvironmentException, RestAPIError


@also_available_as("hopsworks.engine.environment_engine.EnvironmentEngine")
class EnvironmentEngine:
    # How long to keep waiting for a background command before giving up. Generous, because
    # resolving a large requirements.txt against a cold cache legitimately takes many minutes
    # and timing that out would be worse than waiting. What this bounds is the case that never
    # finishes at all: a command whose installer is gone stays ONGOING for good, and without a
    # deadline the caller polls it every POLL_INTERVAL seconds forever, silently.
    AWAIT_COMMAND_TIMEOUT = 30 * 60
    POLL_INTERVAL = 5

    def _await_library_command(
        self, environment_name, library_name, timeout: float | None = None
    ):
        self._await_command(
            lambda: self._poll_commands_library(environment_name, library_name),
            f"library '{library_name}' in environment '{environment_name}'",
            timeout,
        )

    def _await_environment_command(
        self, environment_name, timeout: float | None = None
    ):
        self._await_command(
            lambda: self._poll_commands_environment(environment_name),
            f"environment '{environment_name}'",
            timeout,
        )

    def _await_command(self, poll, description, timeout: float | None = None):
        """Poll until the command reaches a final status, the artifact is gone, or time runs out.

        A missing artifact ends the wait rather than failing it: an uninstall whose library has
        already been removed is done, not broken. That case was always handled; a command that
        stalls in ONGOING was not, which is what the deadline is for.
        """
        timeout = self.AWAIT_COMMAND_TIMEOUT if timeout is None else timeout
        deadline = time.monotonic() + timeout
        commands = [command.Command(status="ONGOING")]
        while len(commands) > 0 and not self._is_final_status(commands[0]):
            if time.monotonic() >= deadline:
                # Name the last status seen: "still ONGOING" and "no command at all" are very
                # different problems, and the caller cannot tell them apart from a bare timeout.
                status = commands[0].status if commands else "unknown"
                raise EnvironmentException(
                    f"Timed out after {timeout:.0f}s waiting for {description}. "
                    f"The last observed command status was {status}. The command may still be "
                    "running, or it may have stopped without reporting a final status: check "
                    "the environment page in the UI, where the command and its state are shown."
                )
            time.sleep(min(self.POLL_INTERVAL, max(0, deadline - time.monotonic())))
            artifact = poll()
            commands = [] if artifact is None else artifact._commands

    def _is_final_status(self, command):
        if command.status == "FAILED":
            raise EnvironmentException(
                f"Command failed with stacktrace: \n{command.error_message}"
            )
        return command.status == "SUCCESS"

    def _poll_commands_library(self, environment_name, library_name):
        _client = client._get_instance()

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
        _client = client._get_instance()

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

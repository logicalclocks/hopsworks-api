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

import logging
import time

import requests
from hopsworks_apigen import also_available_as
from hopsworks_common import client, command, environment, library
from hopsworks_common.client.exceptions import EnvironmentException, RestAPIError


_logger = logging.getLogger(__name__)


@also_available_as("hopsworks.engine.environment_engine.EnvironmentEngine")
class EnvironmentEngine:
    # How long to keep waiting for a background command before giving up.
    # Generous, because resolving a large requirements.txt against a cold cache legitimately takes many minutes, and timing that out would be worse than waiting.
    # What this bounds is the case that never finishes at all.
    # A command whose installer is gone stays ONGOING for good, and without a deadline the caller polls it every POLL_INTERVAL seconds forever, silently.
    AWAIT_COMMAND_TIMEOUT = 30 * 60
    POLL_INTERVAL = 5
    # Each poll request is bounded as well, because the loop can only honour its deadline if the HTTP call it makes comes back.
    # A GET that stalls blocks inside the socket rather than in the loop, so an unbounded request would defeat the deadline entirely.
    POLL_REQUEST_TIMEOUT = 60

    def _await_library_command(
        self, environment_name, library_name, timeout: float | None = None
    ):
        self._await_command(
            lambda request_timeout: self._poll_commands_library(
                environment_name, library_name, request_timeout
            ),
            f"library '{library_name}' in environment '{environment_name}'",
            timeout,
        )

    def _await_environment_command(
        self, environment_name, timeout: float | None = None
    ):
        self._await_command(
            lambda request_timeout: self._poll_commands_environment(
                environment_name, request_timeout
            ),
            f"environment '{environment_name}'",
            timeout,
        )

    def _await_command(self, poll, description, timeout: float | None = None):
        """Poll until the command reaches a final status, the artifact is gone, or time runs out.

        A missing artifact ends the wait rather than failing it: an uninstall whose library has already been removed is done, not broken.
        The same goes for an artifact that carries no commands at all, which is how the backend reports that nothing is in flight.
        Those cases were always handled; a command that stalls in ONGOING was not, which is what the deadline is for.
        """
        timeout = self.AWAIT_COMMAND_TIMEOUT if timeout is None else timeout
        deadline = time.monotonic() + timeout
        commands = [command.Command(status="ONGOING")]
        while len(commands) > 0 and not self._is_final_status(commands[0]):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Name the last status seen.
                # "Still ONGOING" and "no command at all" are very different problems, and the caller cannot tell them apart from a bare timeout.
                raise EnvironmentException(
                    f"Timed out after {timeout:g}s waiting for {description}. "
                    f"The last observed command status was {commands[0].status}. The command may still be "
                    "running, or it may have stopped without reporting a final status: check "
                    "the environment page in the UI, where the command and its state are shown."
                )
            time.sleep(min(self.POLL_INTERVAL, remaining))
            try:
                artifact = poll(self.POLL_REQUEST_TIMEOUT)
            except requests.exceptions.Timeout:
                # A poll that never came back is not an answer, but it is not a failure either.
                # Keep the last known status and let the deadline decide when to give up.
                _logger.debug(
                    "Polling %s timed out after %ss, retrying until the deadline.",
                    description,
                    self.POLL_REQUEST_TIMEOUT,
                )
                continue
            # Both Library and Environment leave _commands as None when the response carries none, which len() cannot take.
            commands = [] if artifact is None else artifact._commands or []

    def _is_final_status(self, command):
        if command.status == "FAILED":
            raise EnvironmentException(
                f"Command failed with stacktrace: \n{command.error_message}"
            )
        return command.status == "SUCCESS"

    def _poll_commands_library(
        self, environment_name, library_name, request_timeout: float | None = None
    ):
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
                    "GET",
                    path_params,
                    headers=headers,
                    query_params=query_params,
                    timeout=request_timeout,
                ),
            )
        except RestAPIError as e:
            if (
                e.response.json().get("errorCode", "") == 300003
                and e.response.status_code == 404
            ):
                return None

    def _poll_commands_environment(
        self, environment_name, request_timeout: float | None = None
    ):
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
                "GET",
                path_params,
                headers=headers,
                query_params=query_params,
                timeout=request_timeout,
            ),
        )

#
#   Copyright 2022 Logical Clocks AB
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
from typing import TYPE_CHECKING

import requests
from hopsworks_apigen import also_available_as
from hopsworks_common.client.exceptions import GitException
from hopsworks_common.core import git_op_execution_api


if TYPE_CHECKING:
    from hopsworks_common.git_op_execution import GitOpExecution


@also_available_as("hopsworks.engine.git_engine.GitEngine")
class GitEngine:
    def __init__(self):
        self._git_op_execution_api = git_op_execution_api.GitOpExecutionApi()
        self._log = logging.getLogger(__name__)

    def _execute_op_blocking(
        self, git_op: GitOpExecution, command: str, timeout: float | None = None
    ) -> GitOpExecution:
        """Poll a git execution status until it reaches a terminal state.

        Parameters:
            git_op: Git execution to monitor.
            command: Git operation running.
            timeout: Seconds to wait before giving up with a `GitException`, or `None` to wait as long as it runs.

        Returns:
            The final GitOpExecution object.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        def remaining() -> float | None:
            return None if deadline is None else deadline - time.monotonic()

        def expired() -> GitException:
            return GitException(
                f"Git command {command} has not finished after {timeout:.0f} seconds; "
                f"it is still running, in state {git_op.state}."
            )

        while git_op.success is None:
            left = remaining()
            if left is not None and left <= 0:
                raise expired()
            self._log.info(f"Running command {command}, current status {git_op.state}")
            try:
                # Each status request gets what is left of the budget, so a stalled poll cannot outlast it.
                git_op = self._git_op_execution_api._get_execution(
                    git_op.repository.id, git_op.id, timeout=left
                )
            except requests.exceptions.Timeout as e:
                raise expired() from e
            if git_op.success is not None:
                break
            left = remaining()
            if left is not None and left <= 0:
                raise expired()
            time.sleep(5 if left is None else min(5, left))

        if git_op.success is False:
            raise GitException(
                f"Git command failed with the message: {git_op.command_result_message}"
            )
        self._log.info(f"Git command {command} finished")

        return git_op

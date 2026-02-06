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

import logging
import time

from hopsworks_apigen import public
from hopsworks_common.client.exceptions import GitException
from hopsworks_common.core import git_op_execution_api


@public("hopsworks.engine.git_engine.GitEngine")
class GitEngine:
    def __init__(self):
        self._git_op_execution_api = git_op_execution_api.GitOpExecutionApi()
        self._log = logging.getLogger(__name__)

    def execute_op_blocking(self, git_op, command: str):
        """Poll a git execution status until it reaches a terminal state.

        Parameters:
            git_op (GitOpExecution): git execution to monitor.
            command: git operation running.

        Returns:
            The final GitOpExecution object.
        """
        while git_op.success is None:
            self._log.info(f"Running command {command}, current status {git_op.state}")
            git_op = self._git_op_execution_api._get_execution(
                git_op.repository.id, git_op.id
            )
            time.sleep(5)

        if git_op.success is False:
            raise GitException(
                f"Git command failed with the message: {git_op.command_result_message}"
            )
        self._log.info(f"Git command {command} finished")

        return git_op

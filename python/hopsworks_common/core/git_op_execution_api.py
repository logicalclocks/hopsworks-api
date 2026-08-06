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

from hopsworks_apigen import also_available_as
from hopsworks_common import client, git_op_execution


@also_available_as("hopsworks.core.git_op_execution_api.GitOpExecutionApi")
class GitOpExecutionApi:
    def __init__(self, project_id=None, project_name=None):
        """Git executions of one project.

        Parameters:
            project_id: The project whose executions this polls, and project_name its name.
                Both default to the connection's project. A GitEngine driving a repository in
                another project passes that project's, because an unbound handle polls an
                execution id in the login project, where it is either absent or someone
                else's.
        """
        self._project_id = project_id
        self._project_name = project_name

    def _pid(self):
        return (
            self._project_id
            if self._project_id is not None
            else client._get_instance()._project_id
        )

    def _get_execution(self, repo_id, execution_id):
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "git",
            "repository",
            str(repo_id),
            "execution",
            str(execution_id),
        ]
        query_params = {"expand": "repository"}

        return git_op_execution.GitOpExecution.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

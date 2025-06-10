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

from hopsworks_common import client, git_op_execution


class GitOpExecutionApi:
    def _get_execution(self, repo_id, execution_id):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
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

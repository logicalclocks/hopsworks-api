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

import json
import logging
from typing import List, Optional, Union

from hopsworks_common import (
    client,
    git_commit,
    git_file_status,
    git_op_execution,
    git_provider,
    git_repo,
    usage,
    util,
)
from hopsworks_common.client.exceptions import GitException
from hopsworks_common.core import git_provider_api
from hopsworks_common.engine import git_engine
from hopsworks_common.git_file_status import GitFileStatus


class GitApi:
    def __init__(self):
        self._git_engine = git_engine.GitEngine()
        self._git_provider_api = git_provider_api.GitProviderApi()
        self._log = logging.getLogger(__name__)

    @usage.method_logger
    def clone(
        self, url: str, path: str, provider: str = None, branch: str = None
    ) -> git_repo.GitRepo:
        """Clone a new Git Repo in to Hopsworks Filesystem.

        ```python

        import hopsworks

        project = hopsworks.login()

        git_api = project.get_git_api()

        git_repo = git_api.clone("https://github.com/logicalclocks/hops-examples.git", "Resources", "GitHub")

        ```
        # Arguments
            url: Url to the git repository
            path: Path in Hopsworks Filesystem to clone the repo to
            provider: The git provider where the repo is currently hosted. Valid values are "GitHub", "GitLab" and "BitBucket".
            branch: Optional branch to clone, defaults to configured main branch
        # Returns
            `GitRepo`: Git repository object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()

        # Support absolute and relative path to dataset
        path = util.convert_to_abs(path, _client._project_name)

        if provider is None:
            provider = self._git_provider_api._get_default_configured_provider()

        path_params = ["project", _client._project_id, "git", "clone"]

        clone_config = {
            "url": url,
            "path": path,
            "provider": provider,
            "branch": branch,
        }
        query_params = {"expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=json.dumps(clone_config),
                query_params=query_params,
            )
        )
        print(
            "Git clone operation running, explore it at "
            + util.get_hostname_replaced_url(
                "/p/" + str(_client._project_id) + "/settings/git"
            )
        )
        git_op = self._git_engine.execute_op_blocking(git_op, "CLONE")
        created_repo = self.get_repo(git_op.repository.name, git_op.repository.path)
        return created_repo

    @usage.method_logger
    def get_repos(self) -> List[git_repo.GitRepo]:
        """Get the existing Git repositories

        # Returns
            `List[GitRepo]`: List of git repository objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
        ]
        query_params = {"expand": "creator"}
        return git_repo.GitRepo.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

    @usage.method_logger
    def get_providers(self) -> List[git_provider.GitProvider]:
        """Get the configured Git providers

        # Returns
            `List[GitProvider]`: List of git provider objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._git_provider_api._get_providers()

    @usage.method_logger
    def get_provider(self, provider: str, host: str = None) -> Optional[git_provider.GitProvider]:
        """Get the configured Git provider

        # Arguments
            provider: Name of git provider. Valid values are "GitHub", "GitLab" and "BitBucket".
            host: Optional host for the git provider e.g. github.com for GitHub, gitlab.com for GitLab, bitbucket.org for BitBucket
        # Returns
            `GitProvider`: The git provider or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._git_provider_api._get_provider(provider, host)

    @usage.method_logger
    def set_provider(self, provider: str, username: str, token: str, host: str = None):
        """Configure a Git provider

        ```python

        import hopsworks

        project = hopsworks.login()

        git_api = project.get_git_api()

        git_api.set_provider("GitHub", "my_user", "my_token", host="github.com")

        ```
        # Arguments
            provider: Name of git provider. Valid values are "GitHub", "GitLab" and "BitBucket".
            username: Username for the git provider service
            token: Token to set for the git provider service
            host: host for the git provider e.g. github.com for GitHub, gitlab.com for GitLab, bitbucket.org for BitBucket
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if host is None:
            host = self._get_default_provider_host(provider)

        self._git_provider_api._set_provider(provider, username, token, host)

    @usage.method_logger
    def get_repo(self, name: str, path: str = None) -> Optional[git_repo.GitRepo]:
        """Get the cloned Git repository

        # Arguments
            name: Name of git repository
            path: Optional path to specify if multiple git repos with the same name exists in the project
        # Returns
            `GitRepo`: The git repository or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
        ]
        query_params = {"expand": "creator"}

        repos = git_repo.GitRepo.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

        if path is not None:
            path = util.convert_to_abs(path, _client._project_name)

        filtered_repos = []
        for repository in repos:
            if repository.name == name:
                if path is None:
                    filtered_repos.append(repository)
                elif repository.path == path:
                    filtered_repos.append(repository)

        if len(filtered_repos) == 1:
            return filtered_repos[0]
        elif len(filtered_repos) > 1:
            raise GitException(
                "Multiple repositories found matching name {}. Please specify the repository by setting the path keyword, for example path='Resources/{}'.".format(
                    name, name
                )
            )
        else:
            return None

    def _delete_repo(self, repo_id):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
        ]
        _client._send_request("DELETE", path_params)

    def _create(self, repo_id, branch: str, checkout=False):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {"branchName": branch, "expand": "repository"}

        if checkout:
            query_params["action"] = "CREATE_CHECKOUT"
        else:
            query_params["action"] = "CREATE"

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _delete(self, repo_id, branch: str):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {
            "action": "DELETE",
            "branchName": branch,
            "expand": "repository",
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _checkout(
        self, repo_id, branch: str = None, commit: str = None, force: bool = False
    ):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
        ]

        query_params = {"branchName": branch, "commit": commit, "expand": "repository"}

        if force:
            query_params["action"] = "CHECKOUT_FORCE"
        else:
            query_params["action"] = "CHECKOUT"

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, query_params=query_params
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _status(self, repo_id):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "STATUS", "expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
            )
        )
        git_op = self._git_engine.execute_op_blocking(git_op, query_params["action"])

        status_dict = json.loads(git_op.command_result_message)
        file_status = None
        if status_dict is not None and isinstance(status_dict["status"], list):
            file_status = []
            for status in status_dict["status"]:
                file_status.append(
                    git_file_status.GitFileStatus.from_response_json(status)
                )
        else:
            self._log.info("Nothing to commit, working tree clean")

        return file_status

    def _commit(self, repo_id, message: str, all=False, files=None):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "COMMIT", "expand": ["repository", "user"]}
        commit_config = {
            "type": "commitCommandConfiguration",
            "message": message,
            "all": all,
            "files": files,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(commit_config),
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _push(self, repo_id, branch: str, force: bool = False, remote: str = None):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "PUSH", "expand": ["repository", "user"]}
        push_config = {
            "type": "pushCommandConfiguration",
            "remoteName": remote,
            "force": force,
            "branchName": branch,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(push_config),
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _pull(self, repo_id, branch: str, force: bool = False, remote: str = None):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "PULL", "expand": ["repository", "user"]}
        push_config = {
            "type": "pullCommandConfiguration",
            "remoteName": remote,
            "force": force,
            "branchName": branch,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(push_config),
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _fetch(self, repo_id, remote: str, branch: str):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "FETCH", "expand": ["repository", "user"]}
        fetch_config = {
            "type": "fetchCommandConfiguration",
            "remoteName": remote,
            "branchName": branch,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(fetch_config),
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])

    def _reset(self, repo_id, remote: str, branch: str, commit: str):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
        ]

        query_params = {"action": "RESET", "expand": ["repository", "user"]}
        reset_config = {
            "type": "resetCommandConfiguration",
            "remoteName": remote,
            "branchName": branch,
            "commit": commit,
        }

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps(reset_config),
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, query_params["action"])


    def _checkout_files(self, repo_id, files: Union[List[str], List[GitFileStatus]]):
        files = util.convert_git_status_to_files(files)

        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
            "file",
        ]

        query_params = {"expand": ["repository", "user"]}

        headers = {"content-type": "application/json"}
        git_op = git_op_execution.GitOpExecution.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                query_params=query_params,
                data=json.dumps({"files": files}),
            )
        )
        _ = self._git_engine.execute_op_blocking(git_op, "CHECKOUT_FILES")

    def _get_commits(self, repo_id, branch: str):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "git",
            "repository",
            str(repo_id),
            "branch",
            branch,
            "commit",
        ]

        headers = {"content-type": "application/json"}
        return git_commit.GitCommit.from_response_json(
            _client._send_request("GET", path_params, headers=headers)
        )

    def _get_default_provider_host(self, provider: str) -> str:
        """Get the default host name for the given provider"""
        if provider == "GitHub":
            return "github.com"
        elif provider == "GitLab":
            return "gitlab.com"
        elif provider == "BitBucket":
            return "bitbucket.org"
        else:
            raise GitException(
                f"Unknown git provider {provider}. Supported providers are GitHub, GitLab and BitBucket."
            )

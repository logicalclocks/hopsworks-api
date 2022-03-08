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

import humps
import json
from hopsworks import user, git_commit, util
from hopsworks.core import git_repo_api, git_remote_api, dataset_api
from typing import List, Union
from hopsworks.git_file_status import GitFileStatus


class GitRepo:
    def __init__(
        self,
        id=None,
        name=None,
        path=None,
        creator=None,
        provider=None,
        current_branch=None,
        current_commit=None,
        ongoing_operation=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        project_id=None,
        project_name=None,
    ):
        self._id = id
        self._name = name
        self._path = path
        self._creator = user.User.from_response_json(creator)
        self._provider = provider
        self._current_branch = current_branch
        self._current_commit = git_commit.GitCommit.from_response_json(current_commit)
        self._type = type
        self._href = href
        self._expand = expand
        self._items = items
        self._count = count

        self._git_repo_api = git_repo_api.GitReposApi(project_id, project_name)
        self._git_remote_api = git_remote_api.GitRemoteApi(project_id, project_name)
        self._dataset_api = dataset_api.DatasetApi(project_id)

    @classmethod
    def from_response_json(cls, json_dict, project_id, project_name):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**repo, project_id=project_id) for repo in json_decamelized["items"]
            ]
        else:
            return cls(
                **json_decamelized, project_id=project_id, project_name=project_name
            )

    @property
    def id(self):
        """Id of the git repo"""
        return self._id

    @property
    def name(self):
        """Name of the git repo"""
        return self._name

    @property
    def path(self):
        """Path to the git repo in the Hopsworks Filesystem"""
        return self._path

    @property
    def creator(self):
        """Creator of the git repo"""
        return self._creator

    @property
    def provider(self):
        """Git provider for the repo, can be GitHub, GitLab or BitBucket"""
        return self._provider

    @property
    def current_branch(self):
        """The current branch for the git repo"""
        return self._current_branch

    @property
    def current_commit(self):
        """The current commit for the git repo"""
        return self._current_commit

    def status(self):
        """Get the status of the repo.
        # Returns
            `List[GitFileStatus]`
        # Raises
            `RestAPIError` in case the backend fails to retrieve the status.
        """
        return self._git_repo_api._status(self.id)

    def delete(self):
        """Delete the git repo from the filesystem.
        !!! danger "Potentially dangerous operation"
            This operation deletes the cloned git repository from the filesystem.
        # Raises
            `RestAPIError`.
        """
        return self._dataset_api.remove(self.path)

    def checkout_branch(self, branch: str, force: bool = False):
        """Checkout a branch
        # Arguments
            branch: name of the branch
            force: if true proceed even if the index or the working tree differs from HEAD. This will throw away local changes
        # Returns
            `List[GitCommit]`
        # Raises
            `RestAPIError` in case the backend fails to retrieve the commits.
        """
        self._git_repo_api._checkout(self.id, branch=branch, force=force)

    def checkout_files(self, files: Union[List[str], List[GitFileStatus]]):
        """Checkout a list of files
        # Arguments
            files: list of files or GitFileStatus objects to checkout
        # Raises
            `RestAPIError` in case the backend fails to checkout the files.
        """
        self._git_repo_api._checkout_files(self.id, files)

    def create_branch(self, branch: str, checkout: bool = False):
        """Create a new branch and optionally check it out
        # Arguments
            branch: name of the branch
            checkout: checkout the created branch
        # Raises
            `RestAPIError` in case the backend fails to delete the branch.
        """
        self._git_repo_api._create(self.id, branch, checkout=checkout)

    def delete_branch(self, branch: str):
        """Delete a branch from local repository
        # Arguments
            branch: name of the branch
        # Raises
            `RestAPIError` in case the backend fails to delete the branch.
        """
        self._git_repo_api._delete(self.id, branch)

    def checkout_commit(self, commit: str, force: bool = False):
        """Checkout specific commit
        # Arguments
            commit: hash of the commit
            force: 	If true when switching branches, proceed even if the index or the working tree differs from HEAD. This will throw away local changes
        # Raises
            `RestAPIError` in case the backend fails to checkout the commit.
        """
        self._git_repo_api._checkout(self.id, commit=commit, force=force)

    def commit(self, message: str, all: bool = True, files: List[str] = None):
        """Add changes and new files, and then commit them
        # Arguments
            message: name of the remote
            all: automatically stage files that have been modified and deleted, but new files are not affected
            files: list of new files to add and commit
        # Raises
            `RestAPIError` in case the backend fails to perform the commit.
        """
        self._git_repo_api._commit(self.id, message, all=all, files=files)

    def push(self, remote, branch: str, force: bool = False):
        """Push changes to the remote branch
        # Arguments
            remote: name of the remote
            branch: name of the branch
            force: update the remote branch even when the local branch does not descend from it
        # Raises
            `RestAPIError` in case the backend fails to retrieve the commits.
        """
        self._git_repo_api._push(self.id, remote, branch, force=force)

    def pull(self, remote, branch: str, force: bool = False):
        """Pull changes from remote branch
        # Arguments
            remote: name of the remote
            branch: name of the branch
            force: update the local branch even when the remote branch does not descend from it
        # Raises
            `RestAPIError` in case the backend fails to retrieve the commits.
        """
        self._git_repo_api._pull(self.id, remote, branch, force=force)

    def get_commits(self, branch: str):
        """Get the commits for the repo and branch.
        # Arguments
            branch: name of the branch
        # Returns
            `List[GitCommit]`
        # Raises
            `RestAPIError` in case the backend fails to retrieve the commits.
        """
        return self._git_repo_api._get_commits(self.id, branch)

    def add_remote(self, name: str, url: str):
        """Add a remote for the repo
        # Arguments
            name: name of the remote
            url: url of the remote
        # Returns
            `GitRemote`
        # Raises
            `RestAPIError` in case the backend fails to add the remote.
        """
        return self._git_remote_api._add(self.id, name, url)

    def get_remote(self, name: str):
        """Get a remote by name for the repo.
        # Arguments
            name: name of the remote
        # Returns
            `GitRemote`
        # Raises
            `RestAPIError` in case the backend fails to get the remote.
        """
        return self._git_remote_api._get(self.id, name)

    def get_remotes(self):
        """Get the configured remotes for the repo.
        # Returns
            `List[GitRemote]`
        # Raises
            `RestAPIError` in case the backend fails to retrieve the remotes.
        """
        return self._git_remote_api._get_remotes(self.id)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"GitRepo({self._name!r}, {self._creator.email!r}, {self._provider!r}, {self._path!r})"

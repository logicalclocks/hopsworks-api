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
from typing import List, Union

import humps
from hopsworks_common import git_commit, usage, user, util
from hopsworks_common.core import dataset_api, git_api, git_remote_api
from hopsworks_common.git_file_status import GitFileStatus


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
        read_only=None,
        ongoing_operation=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        **kwargs,
    ):
        self._id = id
        self._name = name
        self._path = path
        self._creator = user.User.from_response_json(creator)
        self._provider = provider
        self._current_branch = current_branch
        self._current_commit = git_commit.GitCommit.from_response_json(current_commit)
        self._read_only = read_only
        self._type = type
        self._href = href
        self._expand = expand
        self._items = items
        self._count = count

        self._git_api = git_api.GitApi()
        self._git_remote_api = git_remote_api.GitRemoteApi()
        self._dataset_api = dataset_api.DatasetApi()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**repo) for repo in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

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

    @property
    def read_only(self):
        """If True then the repository functions `GitRepo.commit`, `GitRepo.push` and `GitRepo.checkout_files` are forbidden."""
        return self._read_only

    def status(self):
        """Get the status of the repo.

        # Returns
            `List[GitFileStatus]`
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._git_api._status(self.id)

    @usage.method_logger
    def delete(self):
        """Delete the git repo from the filesystem.
        !!! danger "Potentially dangerous operation"
            This operation deletes the cloned git repository from the filesystem.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._git_api._delete_repo(self.id)

    @usage.method_logger
    def checkout_branch(self, branch: str, create: bool = False):
        """Checkout a branch

        # Arguments
            branch: name of the branch
            create: if true will create a new branch and check it out
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if create:
            self._git_api._create(self.id, branch=branch, checkout=True)
        else:
            self._git_api._checkout(self.id, branch=branch)

    def checkout_commit(self, commit: str):
        """Checkout a commit

        # Arguments
            commit: hash of the commit
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._git_api._checkout(self.id, commit=commit)

    @usage.method_logger
    def checkout_files(self, files: Union[List[str], List[GitFileStatus]]):
        """Checkout a list of files

        # Arguments
            files: list of files or GitFileStatus objects to checkout
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._git_api._checkout_files(self.id, files)

    @usage.method_logger
    def delete_branch(self, branch: str):
        """Delete a branch from local repository

        # Arguments
            branch: name of the branch
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._git_api._delete(self.id, branch)

    @usage.method_logger
    def commit(self, message: str, all: bool = True, files: List[str] = None):
        """Add changes and new files, and then commit them

        # Arguments
            message: name of the remote
            all: automatically stage files that have been modified and deleted, but new files are not affected
            files: list of new files to add and commit
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._git_api._commit(self.id, message, all=all, files=files)

    @usage.method_logger
    def push(self, branch: str, remote: str = "origin"):
        """Push changes to the remote branch

        # Arguments
            branch: name of the branch
            remote: name of the remote
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._git_api._push(self.id, branch, force=False, remote=remote)

    @usage.method_logger
    def pull(self, branch: str, remote: str = "origin"):
        """Pull changes from remote branch

        # Arguments
            branch: name of the branch
            remote: name of the remote
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._git_api._pull(self.id, branch, force=False, remote=remote)

    @usage.method_logger
    def fetch(self, remote: str = None, branch: str = None):
        """Fetch changes from remote

        # Arguments
            remote: name of the remote
            branch: name of the branch
        # Raises
            `RestAPIError` in case the backend fails perform the fetch operation.
        """
        self._git_api._fetch(self.id, remote, branch)

    @usage.method_logger
    def reset(self, remote: str = None, branch: str = None, commit: str = None):
        """Reset the branch to a specific commit or to a local branch or to a remote branch

        # Arguments
            remote: name of the remote
            branch: name of the branch
            commit: hash of the commit
        # Raises
            `RestAPIError` in case the backend fails to perform the reset.
        """
        self._git_api._reset(self.id, remote, branch, commit)

    def get_commits(self, branch: str):
        """Get the commits for the repo and branch.

        # Arguments
            branch: name of the branch
        # Returns
            `List[GitCommit]`: The list of commits for this repo
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._git_api._get_commits(self.id, branch)

    @usage.method_logger
    def add_remote(self, name: str, url: str):
        """Add a remote for the repo

        ```python

        import hopsworks

        project = hopsworks.login()

        git_api = project.get_git_api()

        repo = git_api.get_repo("my_repo")

        repo.add_remote("upstream", "https://github.com/organization/repo.git")

        ```
        # Arguments
            name: name of the remote
            url: url of the remote
        # Returns
            `GitRemote`
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._git_remote_api._add(self.id, name, url)

    def get_remote(self, name: str):
        """Get a remote by name for the repo.

        # Arguments
            name: name of the remote
        # Returns
            `GitRemote`: The git remote metadata object or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._git_remote_api._get(self.id, name)

    def get_remotes(self):
        """Get the configured remotes for the repo.

        # Returns
            `List[GitRemote]`
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._git_remote_api._get_remotes(self.id)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"GitRepo({self._name!r}, {self._creator.email!r}, {self._provider!r}, {self._path!r})"

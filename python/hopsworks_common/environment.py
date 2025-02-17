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

import os
from typing import Optional

import humps
from hopsworks_common import client, command, usage, util
from hopsworks_common.core import environment_api, library_api
from hopsworks_common.engine import environment_engine


class Environment:
    NOT_FOUND_ERROR_CODE = 300000

    def __init__(
        self,
        name=None,
        description=None,
        python_version=None,
        python_conflicts=None,
        pip_search_enabled=None,
        conflicts=None,
        conda_channel=None,
        libraries=None,
        commands=None,
        href=None,
        type=None,
        **kwargs,
    ):
        self._name = name
        self._description = description
        self._python_version = python_version
        self._python_conflicts = python_conflicts
        self._pip_search_enabled = pip_search_enabled
        self._conflicts = conflicts
        self._libraries = libraries
        self._commands = (
            command.Command.from_response_json(commands) if commands else None
        )

        self._environment_engine = environment_engine.EnvironmentEngine()
        self._library_api = library_api.LibraryApi()
        self._environment_api = environment_api.EnvironmentApi()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            return [cls(**env) for env in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    @property
    def python_version(self):
        """Python version of the environment"""
        return self._python_version

    @property
    def name(self):
        """Name of the environment"""
        return self._name

    @property
    def description(self):
        """Description of the environment"""
        return self._description

    @usage.method_logger
    def install_wheel(self, path: str, await_installation: Optional[bool] = True):
        """Install a python library packaged in a wheel file

        ```python

        import hopsworks

        project = hopsworks.login()

        # Upload to Hopsworks
        ds_api = project.get_dataset_api()
        whl_path = ds_api.upload("matplotlib-3.1.3-cp38-cp38-manylinux1_x86_64.whl", "Resources")

        # Install
        env_api = project.get_environment_api()
        env = env_api.get_environment("my_custom_environment")

        env.install_wheel(whl_path)

        ```

        # Arguments
            path: str. The path on Hopsworks where the wheel file is located
            await_installation: bool. If True the method returns only when the installation finishes. Default True
        # Returns
            `Library`: The library object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        # Wait for any ongoing environment operations
        self._environment_engine.await_environment_command(self.name)

        library_name = os.path.basename(path)

        _client = client.get_instance()
        path = util.convert_to_abs(path, _client._project_name)

        library_spec = {
            "dependencyUrl": path,
            "channelUrl": "wheel",
            "packageSource": "WHEEL",
        }

        library_rest = self._library_api._install(library_name, self.name, library_spec)

        if await_installation:
            return self._environment_engine.await_library_command(
                self.name, library_name
            )

        return library_rest

    @usage.method_logger
    def install_requirements(
        self, path: str, await_installation: Optional[bool] = True
    ):
        """Install libraries specified in a requirements.txt file

        ```python

        import hopsworks

        project = hopsworks.login()

        # Upload to Hopsworks
        ds_api = project.get_dataset_api()
        requirements_path = ds_api.upload("requirements.txt", "Resources")

        # Install
        env_api = project.get_environment_api()
        env = env_api.get_environment("my_custom_environment")


        env.install_requirements(requirements_path)

        ```

        # Arguments
            path: str. The path on Hopsworks where the requirements.txt file is located
            await_installation: bool. If True the method returns only when the installation is finished. Default True
        # Returns
            `Library`: The library object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        # Wait for any ongoing environment operations
        self._environment_engine.await_environment_command(self.name)

        library_name = os.path.basename(path)

        _client = client.get_instance()
        path = util.convert_to_abs(path, _client._project_name)

        library_spec = {
            "dependencyUrl": path,
            "channelUrl": "requirements_txt",
            "packageSource": "REQUIREMENTS_TXT",
        }

        library_rest = self._library_api._install(library_name, self.name, library_spec)

        if await_installation:
            return self._environment_engine.await_library_command(
                self.name, library_name
            )

        return library_rest

    @usage.method_logger
    def delete(self):
        """Delete the environment
        !!! danger "Potentially dangerous operation"
            This operation deletes the python environment.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._environment_api._delete(self.name)

    def __repr__(self):
        return f"Environment({self.name!r})"

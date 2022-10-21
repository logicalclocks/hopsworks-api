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

import humps
import json
import os

from hopsworks import client, library, command
from hopsworks.core import environment_api
from hopsworks.engine import environment_engine


class Environment:
    def __init__(
        self,
        python_version,
        python_conflicts,
        pip_search_enabled,
        conflicts=None,
        conda_channel=None,
        libraries=None,
        commands=None,
        href=None,
        type=None,
        project_id=None,
    ):
        self._python_version = python_version
        self._python_conflicts = python_conflicts
        self._pip_search_enabled = pip_search_enabled
        self._conflicts = conflicts
        self._libraries = libraries
        self._commands = (
            command.Command.from_response_json(commands) if commands else None
        )
        self._project_id = project_id

        self._environment_engine = environment_engine.EnvironmentEngine(project_id)

    @classmethod
    def from_response_json(cls, json_dict, project_id):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized, project_id=project_id)

    def install_wheel(self, path, await_installation=True):
        """Install Wheel library in the environment

        # Arguments
            path: str. The path on Hopsworks where the wheel file is located
            await_installation: bool. If True the method returns only when the installation finishes. Default True
        """
        _client = client.get_instance()

        library_name = os.path.basename(path)

        library_spec = {
            "library": library_name,
            "dependencyUrl": path,
            "channelUrl": "wheel",
            "packageSource": "WHEEL",
        }

        path_params = [
            "project",
            self._project_id,
            "python",
            "environments",
            environment_api.EnvironmentApi.PYTHON_VERSION,
            "libraries",
            library_name,
        ]

        headers = {"content-type": "application/json"}
        library_rest = library.Library.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(library_spec)
            ),
            environment=self,
            project_id=self._project_id,
        )

        if await_installation:
            return self._environment_engine.await_installation(library_name)

        return library_rest

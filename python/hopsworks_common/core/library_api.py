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

import json

from hopsworks_apigen import also_available_as
from hopsworks_common import client, library


@also_available_as("hopsworks.core.library_api.LibraryApi")
class LibraryApi:
    def __init__(self, project_id=None, project_name=None):
        """Libraries of one project's environments.

        Parameters:
            project_id: The project whose environments this installs into, and project_name
                its name. Both default to the connection's project. An Environment obtained
                from another project passes that project's, because an unbound handle
                installs into an identically named environment in the login project.
        """
        self._project_id = project_id
        self._project_name = project_name

    def _pid(self):
        return (
            self._project_id
            if self._project_id is not None
            else client._get_instance()._project_id
        )

    def _install(
        self, library_name: str, name: str, library_spec: dict
    ) -> library.Library:
        """Install a library in the environment.

        Parameters:
            library_name: Name of the library.
            name: Name of the environment.
            library_spec: Installation payload.

        Returns:
            The library object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()

        path_params = [
            "project",
            self._pid(),
            "python",
            "environments",
            name,
            "libraries",
            library_name,
        ]

        headers = {"content-type": "application/json"}
        return library.Library.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(library_spec)
            ),
            environment=self,
        )

    def _uninstall(self, library_name: str, name: str) -> None:
        """Uninstall a library from the environment.

        Parameters:
            library_name: Name of the library.
            name: Name of the environment.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()

        path_params = [
            "project",
            self._pid(),
            "python",
            "environments",
            name,
            "libraries",
            library_name,
        ]

        headers = {"content-type": "application/json"}
        _client._send_request("DELETE", path_params, headers=headers)

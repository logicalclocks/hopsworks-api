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
            _client._project_id,
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

    def _uninstall(
        self, library_name: str, name: str, package_source: str | None = None
    ) -> None:
        """Uninstall a library from the environment.

        Parameters:
            library_name: Name of the library.
            name: Name of the environment.
            package_source: The ecosystem the library belongs to, `PIP` or `NPM`, when the caller knows it.
                A name can exist on both PyPI and npm, and the backend refuses to guess.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()

        path_params = [
            "project",
            _client._project_id,
            "python",
            "environments",
            name,
            "libraries",
            # A scoped npm name (@scope/name) is two path segments on the backend's route.
            *library_name.split("/"),
        ]

        headers = {"content-type": "application/json"}
        if package_source:
            _client._send_request(
                "DELETE",
                path_params,
                headers=headers,
                query_params={"packageSource": package_source},
            )
        else:
            _client._send_request("DELETE", path_params, headers=headers)

    def _install_npm(self, name: str, request: dict) -> library.Library:
        """Install npm packages in the environment as one command and one image build.

        Parameters:
            name: Name of the environment.
            request: The install request: ``{"packages": [...], "flags": [...]}``.

        Returns:
            The library the operation is tracked under: the package itself for a request of one,
            a placeholder for a request of several, which the installed packages replace once the
            build has finished.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = self._npm_path(_client, name, "install")
        headers = {"content-type": "application/json"}
        return library.Library.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(request)
            ),
            environment=self,
        )

    def _resolve_npm(self, name: str, request: dict) -> dict:
        """Resolve a package.json's dependencies to exact versions.

        Parameters:
            name: Name of the environment.
            request: The resolve request (package.json content or project path, optional lockfile).

        Returns:
            The resolve result as the backend returns it.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = self._npm_path(_client, name, "resolve")
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "POST", path_params, headers=headers, data=json.dumps(request)
        )

    def _inspect_npm_git(self, name: str, request: dict) -> dict:
        """Read and resolve the package.json files of a git repository.

        Parameters:
            name: Name of the environment.
            request: ``{"url": ..., "ref": ..., "includeDevDependencies": ...}``.

        Returns:
            The inspection result as the backend returns it.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = self._npm_path(_client, name, "git", "inspect")
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "POST", path_params, headers=headers, data=json.dumps(request)
        )

    @staticmethod
    def _npm_path(_client, name: str, *tail: str) -> list:
        return [
            "project",
            _client._project_id,
            "python",
            "environments",
            name,
            "libraries",
            "npm",
            *tail,
        ]

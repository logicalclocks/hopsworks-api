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

from hopsworks_common import client, library


class LibraryApi:
    def _install(self, library_name: str, name: str, library_spec: dict):
        """Install a library in the environment

        # Arguments
            library_name: Name of the library.
            name: Name of the environment.
            library_spec: installation payload
        # Returns
            `Library`: The library object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()

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
        library_rest = library.Library.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(library_spec)
            ),
            environment=self,
        )
        return library_rest

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
from __future__ import annotations

import re
from typing import Optional, Tuple

from hopsworks import client
from hopsworks.client.exceptions import RestAPIError


class VariableApi:
    def __init__(self):
        pass

    def get_variable(self, variable: str):
        """Get the configured value of a variable.

        # Arguments
            vairable: Name of the variable.
        # Returns
            The vairable's value
        # Raises
            `RestAPIError`: If unable to get the variable
        """

        _client = client.get_instance()

        path_params = ["variables", variable]
        domain = _client._send_request("GET", path_params)

        return domain["successMessage"]

    def get_version(self, software: str) -> Optional[str]:
        """Get version of a software component.

        # Arguments
            software: Name of the software.
        # Returns
            The software's version, if the software is available, otherwise `None`.
        # Raises
            `RestAPIError`: If unable to get the version
        """

        _client = client.get_instance()

        path_params = ["variables", "versions"]
        resp = _client._send_request("GET", path_params)

        for entry in resp:
            if entry["software"] == software:
                return entry["version"]
        return None

    def parse_major_and_minor(
        self, backend_version: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Extract major and minor version from full version.

        # Arguments
            backend_version: The full version.
        # Returns
            (major, minor): The pair of major and minor parts of the version, or (None, None) if the version format is incorrect.
        """

        version_pattern = r"(\d+)\.(\d+)"
        matches = re.match(version_pattern, backend_version)

        if matches is None:
            return (None, None)
        return matches.group(1), matches.group(2)

    def get_flyingduck_enabled(self) -> bool:
        """Check if Flying Duck is enabled on the backend.

        # Returns
            `True`: If flying duck is availalbe, `False` otherwise.
        # Raises
            `RestAPIError`: If unable to obtain the flag's value.
        """
        return self.get_variable("enable_flyingduck") == "true"

    def get_loadbalancer_external_domain(self) -> str:
        """Get domain of external loadbalancer.

        # Returns
            `str`: The domain of external loadbalancer, if it is set up, otherwise empty string `""`.
        """
        try:
            return self.get_variable("loadbalancer_external_domain")
        except RestAPIError:
            return ""

    def get_service_discovery_domain(self) -> str:
        """Get domain of service discovery server.

        # Returns
            `str`: The domain of service discovery server, if it is set up, otherwise empty string `""`.
        """
        try:
            return self.get_variable("service_discovery_domain")
        except RestAPIError:
            return ""

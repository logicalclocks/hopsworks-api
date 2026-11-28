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

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import requests
from hopsworks_apigen import also_available_as
from hopsworks_common.client import auth
from hopsworks_common.client.istio import base as istio


if TYPE_CHECKING:
    from urllib.parse import ParseResult


@also_available_as("hsml.client.istio.external.Client")
class Client(istio.Client):
    def __init__(
        self,
        host,
        port,
        project,
        api_key_value,
        hostname_verification=None,
        trust_store_path=None,
        scheme="http",
    ):
        """Initializes a client in an external environment such as AWS Sagemaker."""
        self._host = host
        self._port = port
        self._base_url = f"{scheme}://{self._host}:{str(self._port)}"
        self._project_name = project

        self._auth = auth.ApiKeyAuth(api_key_value)

        self._session = requests.session()
        self._connected = True
        self._verify = self._get_verify(hostname_verification, trust_store_path)

        self._cert_key = None

    def _close(self):
        """Closes a client."""
        self._connected = False

    def _replace_public_host(self, url: ParseResult) -> ParseResult:
        """Replace the host with the public host set in HOPSWORKS_PUBLIC_HOST.

        Only used for UI URLs. API calls keep the client's base URL.

        Parameters:
            url: The URL to replace the host in.

        Returns:
            The URL with the host replaced when HOPSWORKS_PUBLIC_HOST is set and
            not empty, else the URL as is.
        """
        public_host = os.environ.get(self.HOPSWORKS_PUBLIC_HOST)
        if public_host:
            return url._replace(netloc=public_host)
        return url

    def _is_external(self):
        return True

    @property
    def host(self):
        return self._host

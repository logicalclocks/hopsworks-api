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

from abc import abstractmethod

from hopsworks_apigen import also_available_as
from hopsworks_common.client import base
from hopsworks_common.client.istio.grpc.inference_client import (
    GRPCInferenceServerClient,
)


@also_available_as("hsml.client.istio.base.Client")
class Client(base.Client):
    SERVING_API_KEY = "SERVING_API_KEY"
    HOPSWORKS_PUBLIC_HOST = "HOPSWORKS_PUBLIC_HOST"
    TOKEN_EXPIRED_MAX_RETRIES = 0

    BASE_PATH_PARAMS = []

    @abstractmethod
    def __init__(self):
        """To be implemented by clients."""

    def _get_verify(self, verify: bool, trust_store_path: str | None) -> str | bool:
        """Get verification method for sending inference requests to Istio.

        Credit to https://gist.github.com/gdamjan/55a8b9eec6cf7b771f92021d93b87b2c

        Parameters:
            verify: perform hostname verification, 'true' or 'false'
            trust_store_path: path of the truststore locally if it was uploaded manually to the external environment such as EKS or AKS

        Returns:
            if verify is true and the truststore is provided, then return the trust store location if verify is true but the truststore wasn't provided, then return true if verify is false, then return false
        """
        if verify == "true":
            if trust_store_path is not None:
                return trust_store_path
            return True

        return False

    def _get_host_port_pair(self) -> tuple[str, str]:
        """Removes "http or https" from the rest endpoint and returns a list [endpoint, port], where endpoint is on the format /path.. without http://.

        Returns:
            a tuple (endpoint, port)
        """
        endpoint = self._base_url
        if endpoint.startswith("http"):
            last_index = endpoint.rfind("/")
            endpoint = endpoint[last_index + 1 :]
        host, port = endpoint.split(":")
        return host, port

    def _close(self):
        """Closes a client. Can be implemented for clean up purposes, not mandatory."""
        self._connected = False

    def _create_grpc_channel(self, path_prefix: str) -> GRPCInferenceServerClient:
        return GRPCInferenceServerClient(
            url=self._host + ":" + str(self._port),
            path_prefix=path_prefix,
            serving_api_key=self._auth._token,
        )

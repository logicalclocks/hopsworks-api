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

import socket

from hsml import client
from hsml.client.exceptions import ModelRegistryException
from hsml.constants import INFERENCE_ENDPOINTS
from hsml.core import dataset_api, serving_api
from hsml.inference_endpoint import get_endpoint_by_type
from hsml.model_serving import ModelServing


class ModelServingApi:
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()
        self._serving_api = serving_api.ServingApi()

    def get(self):
        """Get model serving for specific project.
        :param project: project of the model registry
        :type project: str
        :return: the model serving metadata
        :rtype: ModelServing
        """

        _client = client.get_instance()

        # Validate that there is a Models dataset in the connected project
        if not self._dataset_api.path_exists("Models"):
            raise ModelRegistryException(
                "No Models dataset exists in project {}, Please enable the Serving service or create the dataset manually.".format(
                    _client._project_name
                )
            )

        return ModelServing(_client._project_name, _client._project_id)

    def load_default_configuration(self):
        """Load default configuration and set istio client for model serving"""

        # kserve installed
        is_kserve_installed = self._serving_api.is_kserve_installed()
        client.set_kserve_installed(is_kserve_installed)

        # istio client
        self._istio_init_if_available()

        # num instances limits
        num_instances_range = self._serving_api.get_num_instances_limits()
        client.set_serving_num_instances_limits(num_instances_range)

        # Knative domain
        knative_domain = self._serving_api.get_knative_domain()
        client.set_knative_domain(knative_domain)

    def _istio_init_if_available(self):
        """Initialize istio client if available"""

        if client.is_kserve_installed():
            # check existing istio client
            try:
                if client.istio.get_instance() is not None:
                    return  # istio client already set
            except Exception:
                pass

            # setup istio client
            inference_endpoints = self._serving_api.get_inference_endpoints()
            if not client._is_external():
                # if internal, get node port
                endpoint = get_endpoint_by_type(
                    inference_endpoints, INFERENCE_ENDPOINTS.ENDPOINT_TYPE_KUBE_CLUSTER
                )
                if endpoint is not None:
                    client.istio.init(
                        endpoint.get_any_host(),
                        endpoint.get_port(INFERENCE_ENDPOINTS.PORT_NAME_HTTP).number,
                    )
                else:
                    raise ValueError(
                        "Istio ingress endpoint of type '"
                        + INFERENCE_ENDPOINTS.ENDPOINT_TYPE_KUBE_CLUSTER
                        + "' not found"
                    )
            else:  # if external
                endpoint = get_endpoint_by_type(
                    inference_endpoints, INFERENCE_ENDPOINTS.ENDPOINT_TYPE_LOAD_BALANCER
                )

                port = (
                    endpoint.get_port(INFERENCE_ENDPOINTS.PORT_NAME_HTTPS)
                    if endpoint.get_port(INFERENCE_ENDPOINTS.PORT_NAME_HTTPS)
                    else endpoint.get_port(INFERENCE_ENDPOINTS.PORT_NAME_HTTP)
                )

                if endpoint is not None:
                    # if load balancer (external ip) available
                    _client = client.get_instance()
                    client.istio.init(
                        endpoint.get_any_host(),
                        port.number,
                        _client._project_name,
                        _client._auth._token,  # reuse hopsworks client token
                        scheme="https"
                        if endpoint.get_port(INFERENCE_ENDPOINTS.PORT_NAME_HTTPS)
                        else "http",
                    )
                    return

                # otherwise, fallback to hopsworks client
                print(
                    "External IP not configured for the Istio ingress gateway, the Hopsworks client will be used for model inference instead"
                )

    def _is_host_port_open(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            result = sock.connect_ex((host, port))
        finally:
            sock.settimeout(None)
            sock.close()
        return result == 0

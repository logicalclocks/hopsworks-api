#
#   Copyright 2026 Hopsworks AB
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

import pytest
from hsml.constants import INFERENCE_ENDPOINTS
from hsml.core.model_serving_api import ModelServingApi
from hsml.inference_endpoint import InferenceEndpoint, InferenceEndpointPort


def _endpoint(endpoint_type, ports):
    return InferenceEndpoint(
        type=endpoint_type,
        hosts=["host.example"],
        ports=[InferenceEndpointPort(name, number) for name, number in ports],
    )


class TestIstioInitIfAvailable:
    # Regression tests for hsml.core.model_serving_api.ModelServingApi._istio_init_if_available.
    # The external-LB branch used to dereference a None endpoint when the cluster had no
    # load balancer configured; the fallback print path must be reachable.

    @pytest.fixture
    def api(self, mocker):
        api = ModelServingApi()
        mocker.patch.object(api, "_serving_api")
        return api

    @pytest.fixture(autouse=True)
    def _istio_stubs(self, mocker):
        mocker.patch(
            "hsml.core.model_serving_api.client.is_kserve_installed", return_value=True
        )
        mocker.patch(
            "hsml.core.model_serving_api.client.istio.get_instance", return_value=None
        )
        self.istio_init = mocker.patch("hsml.core.model_serving_api.client.istio.init")

    def test_skips_when_kserve_not_installed(self, mocker, api):
        mocker.patch(
            "hsml.core.model_serving_api.client.is_kserve_installed", return_value=False
        )

        api._istio_init_if_available()

        api._serving_api.get_inference_endpoints.assert_not_called()
        self.istio_init.assert_not_called()

    def test_skips_when_istio_already_initialized(self, mocker, api):
        mocker.patch(
            "hsml.core.model_serving_api.client.istio.get_instance",
            return_value=object(),
        )

        api._istio_init_if_available()

        api._serving_api.get_inference_endpoints.assert_not_called()
        self.istio_init.assert_not_called()

    # internal (non-external) client

    def test_internal_uses_kube_cluster_http_port(self, mocker, api):
        mocker.patch(
            "hsml.core.model_serving_api.client._is_external", return_value=False
        )
        api._serving_api.get_inference_endpoints.return_value = [
            _endpoint(
                INFERENCE_ENDPOINTS.ENDPOINT_TYPE_KUBE_CLUSTER,
                [(INFERENCE_ENDPOINTS.PORT_NAME_HTTP, 80)],
            )
        ]

        api._istio_init_if_available()

        self.istio_init.assert_called_once_with("host.example", 80)

    def test_internal_raises_when_kube_cluster_missing(self, mocker, api):
        mocker.patch(
            "hsml.core.model_serving_api.client._is_external", return_value=False
        )
        api._serving_api.get_inference_endpoints.return_value = [
            _endpoint(
                INFERENCE_ENDPOINTS.ENDPOINT_TYPE_LOAD_BALANCER,
                [(INFERENCE_ENDPOINTS.PORT_NAME_HTTP, 80)],
            )
        ]

        with pytest.raises(ValueError, match="KUBE_CLUSTER"):
            api._istio_init_if_available()
        self.istio_init.assert_not_called()

    # external client

    def test_external_load_balancer_https_preferred(self, mocker, api):
        mocker.patch(
            "hsml.core.model_serving_api.client._is_external", return_value=True
        )
        fake_client = mocker.MagicMock()
        fake_client._project_name = "proj"
        fake_client._auth._token = "tok"
        mocker.patch(
            "hsml.core.model_serving_api.client.get_instance", return_value=fake_client
        )
        api._serving_api.get_inference_endpoints.return_value = [
            _endpoint(
                INFERENCE_ENDPOINTS.ENDPOINT_TYPE_LOAD_BALANCER,
                [
                    (INFERENCE_ENDPOINTS.PORT_NAME_HTTP, 80),
                    (INFERENCE_ENDPOINTS.PORT_NAME_HTTPS, 443),
                ],
            )
        ]

        api._istio_init_if_available()

        self.istio_init.assert_called_once_with(
            "host.example", 443, "proj", "tok", scheme="https"
        )

    def test_external_load_balancer_falls_back_to_http(self, mocker, api):
        mocker.patch(
            "hsml.core.model_serving_api.client._is_external", return_value=True
        )
        fake_client = mocker.MagicMock()
        fake_client._project_name = "proj"
        fake_client._auth._token = "tok"
        mocker.patch(
            "hsml.core.model_serving_api.client.get_instance", return_value=fake_client
        )
        api._serving_api.get_inference_endpoints.return_value = [
            _endpoint(
                INFERENCE_ENDPOINTS.ENDPOINT_TYPE_LOAD_BALANCER,
                [(INFERENCE_ENDPOINTS.PORT_NAME_HTTP, 80)],
            )
        ]

        api._istio_init_if_available()

        self.istio_init.assert_called_once_with(
            "host.example", 80, "proj", "tok", scheme="http"
        )

    def test_external_no_load_balancer_falls_back_without_crashing(
        self, mocker, capsys, api
    ):
        # Regression: previously raised AttributeError: 'NoneType' object has no attribute 'get_port'
        # when the cluster had no LOAD_BALANCER inference endpoint configured.
        mocker.patch(
            "hsml.core.model_serving_api.client._is_external", return_value=True
        )
        api._serving_api.get_inference_endpoints.return_value = [
            _endpoint(
                INFERENCE_ENDPOINTS.ENDPOINT_TYPE_KUBE_CLUSTER,
                [(INFERENCE_ENDPOINTS.PORT_NAME_HTTP, 80)],
            )
        ]

        api._istio_init_if_available()

        self.istio_init.assert_not_called()
        assert "External IP not configured" in capsys.readouterr().out

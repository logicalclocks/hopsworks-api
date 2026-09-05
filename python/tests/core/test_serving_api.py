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

import json
import re
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hsml.client.istio.utils.infer_type import InferInput
from hsml.constants import INFERENCE_ENDPOINTS as IE
from hsml.core.serving_api import ServingApi


def _tags_response(items: list[tuple[str, str]]) -> dict:
    return {
        "count": len(items),
        "items": [{"name": name, "value": value} for name, value in items],
    }


def _patch_client(mocker, send_request_return) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 1
    if isinstance(send_request_return, Exception):
        client_instance._send_request.side_effect = send_request_return
    else:
        client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hsml.core.serving_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


def _deployment() -> SimpleNamespace:
    return SimpleNamespace(id=12)


# The relation between a project's name and the Kubernetes namespace its
# deployments run in, over the cluster configurations that exist. Istio routes
# inference on the namespace, so every one of these has to be addressed by it.
#
#   identical   the backend created the namespace from a project name that was
#               already a legal namespace, so the two strings coincide
#   derived     the backend created the namespace from a project name that was
#               not, lowercasing it and replacing the characters it had to
#   unrelated   an administrator pre-created the namespace, which is how
#               Hopsworks is deployed on OpenShift, and it bears no relation to
#               the project name
#   unstamped   the deployment carries no project name at all, which is its
#               state between `from_response_json` and the client stamping one
NAMESPACE_CASES = [
    pytest.param("myproject", "myproject", id="identical"),
    pytest.param("my_project", "my-project", id="derived"),
    pytest.param(
        "model_serving_int_MhGJP", "loadtest-kserve-0-main-000", id="unrelated"
    ),
    pytest.param(None, "pre-created-ns", id="unstamped"),
]

# The two cases a cluster that names its own namespaces can produce.
BACKEND_DERIVED_NAMESPACE_CASES = NAMESPACE_CASES[:2]


def _inference_deployment(
    project_name: str | None,
    project_namespace: str,
    name: str = "skdepl",
    api_protocol: str = IE.API_PROTOCOL_REST,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        project_name=project_name,
        project_namespace=project_namespace,
        api_protocol=api_protocol,
        _grpc_channel=None,
    )


def _as_gateway_authority(segment: str) -> str:
    """Normalize a path segment the way the istio gateway does.

    The gateway turns `/v1/<segment>/<deployment>/...` into the authority it
    routes on, lowercasing the segment and replacing every character outside
    `[a-z0-9-]` with a hyphen. See `charts/kserve/files/envoyfilter.yaml` in
    hopsworks-helm.
    """
    return re.sub(r"[^a-z0-9-]", "-", segment.lower())


class TestServingApi:
    def test_get_tag_returns_value_for_name(self, mocker):
        # Arrange
        api = ServingApi()
        value = {"owner": "team-a"}
        _patch_client(mocker, _tags_response([("meta", json.dumps(value))]))

        # Act
        result = api._get_tag(_deployment(), "meta")

        # Assert
        assert result == value

    def test_get_tag_numeric_value(self, mocker):
        # Arrange
        api = ServingApi()
        _patch_client(mocker, _tags_response([("version", json.dumps(7))]))

        # Act
        result = api._get_tag(_deployment(), "version")

        # Assert
        assert result == 7

    def test_get_tag_absent_name_returns_none(self, mocker):
        # Arrange
        api = ServingApi()
        _patch_client(mocker, {"count": 0, "items": []})

        # Act
        result = api._get_tag(_deployment(), "missing")

        # Assert
        assert result is None

    def test_get_tag_not_found_error_returns_none(self, mocker):
        # Arrange
        api = ServingApi()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {
            "errorCode": 370002,
            "errorMsg": "not found",
            "usrMsg": "not found",
        }
        _patch_client(
            mocker,
            RestAPIError("/project/1/serving/12/tags/missing", mock_response),
        )

        # Act
        result = api._get_tag(_deployment(), "missing")

        # Assert
        assert result is None

    @pytest.mark.parametrize("bad_value", [{"a": 1}, 7, ["x"], True])
    def test_get_tags_does_not_double_decode(self, mocker, bad_value):
        # Arrange
        api = ServingApi()
        _patch_client(mocker, _tags_response([("t", json.dumps(bad_value))]))

        # Act
        result = api._get_tags(_deployment())

        # Assert
        assert result == {"t": bad_value}

    # inference addressing

    # Regression: Istio routes on the namespace a deployment runs in, and the
    # backend reports that namespace on every deployment the client parses.
    # Addressing by project name still reached the deployment on a cluster that
    # derives one from the other, so this was invisible everywhere except the
    # pre-created-namespace clusters where it broke every prediction.

    @pytest.mark.parametrize(("project_name", "project_namespace"), NAMESPACE_CASES)
    def test_istio_inference_path_addresses_the_namespace(
        self, project_name, project_namespace
    ):
        # Arrange
        api = ServingApi()
        deployment = _inference_deployment(project_name, project_namespace)

        # Act
        path = api._get_istio_inference_path(deployment)

        # Assert
        assert path == [
            "v1",
            project_namespace,
            "skdepl",
            "v1",
            "models",
            "skdepl:predict",
        ]

    @pytest.mark.parametrize(("project_name", "project_namespace"), NAMESPACE_CASES)
    def test_istio_inference_base_path_addresses_the_namespace(
        self, project_name, project_namespace
    ):
        # Arrange: vLLM deployments and Python deployments without a model
        # address the base path and append their own suffix.
        api = ServingApi()
        deployment = _inference_deployment(
            project_name, project_namespace, name="vllmdepl"
        )

        # Act
        path = api._get_istio_inference_path(deployment, base_only=True)

        # Assert
        assert path == ["v1", project_namespace, "vllmdepl"]

    @pytest.mark.parametrize("project_namespace", ["myproject", "pre-created-ns"])
    @pytest.mark.parametrize("base_only", [False, True])
    def test_istio_inference_path_ignores_the_project_name(
        self, base_only, project_namespace
    ):
        # Arrange
        api = ServingApi()

        # Act: every project name a deployment in this namespace could carry,
        # including one equal to the namespace, where asserting on the path
        # cannot tell which of the two it was built from.
        paths = {
            tuple(
                api._get_istio_inference_path(
                    _inference_deployment(project_name, project_namespace),
                    base_only=base_only,
                )
            )
            for project_name in (
                None,
                "myproject",
                "my_project",
                "model_serving_int_MhGJP",
            )
        }

        # Assert
        assert len(paths) == 1

    @pytest.mark.parametrize(
        ("project_name", "project_namespace"), BACKEND_DERIVED_NAMESPACE_CASES
    )
    def test_namespace_addressing_is_inert_on_backend_created_namespaces(
        self, project_name, project_namespace
    ):
        # Arrange
        api = ServingApi()

        # Act
        segment = api._get_istio_inference_path(
            _inference_deployment(project_name, project_namespace)
        )[1]

        # Assert: the gateway normalizes whichever of the two it is handed, and
        # the namespace is what that normalization produces from the project
        # name, so on a cluster that names its own namespaces the request this
        # builds is the one the gateway already routed.
        assert _as_gateway_authority(project_name) == project_namespace
        assert _as_gateway_authority(project_namespace) == project_namespace
        assert _as_gateway_authority(segment) == _as_gateway_authority(project_name)

    @pytest.mark.parametrize(("project_name", "project_namespace"), NAMESPACE_CASES)
    def test_rest_inference_request_posts_to_the_namespace_path(
        self, mocker, project_name, project_namespace
    ):
        # Arrange
        api = ServingApi()
        istio_client = MagicMock()
        istio_client._send_request.return_value = {"predictions": [1]}
        mocker.patch(
            "hsml.core.serving_api.client.istio._get_instance",
            return_value=istio_client,
        )
        deployment = _inference_deployment(project_name, project_namespace)

        # Act
        response = api._send_inference_request(deployment, {"inputs": [[1]]})

        # Assert
        assert response == {"predictions": [1]}
        args, kwargs = istio_client._send_request.call_args
        assert args[0] == "POST"
        assert args[1] == [
            "v1",
            project_namespace,
            "skdepl",
            "v1",
            "models",
            "skdepl:predict",
        ]
        assert kwargs["with_base_path_params"] is False
        # Inference moved from host-based to path-based routing in 4.8: the
        # gateway derives the authority from the path it is given, so a host
        # header here would be a return to the scheme this path replaced.
        assert "host" not in {header.lower() for header in kwargs["headers"]}

    @pytest.mark.parametrize(("project_name", "project_namespace"), NAMESPACE_CASES)
    def test_grpc_channel_prefixes_the_namespace(
        self, mocker, project_name, project_namespace
    ):
        # Arrange
        api = ServingApi()
        istio_client = MagicMock()
        mocker.patch(
            "hsml.core.serving_api.client.istio._get_instance",
            return_value=istio_client,
        )

        # Act
        api._create_grpc_channel(_inference_deployment(project_name, project_namespace))

        # Assert
        istio_client._create_grpc_channel.assert_called_once_with(
            f"/v1/{project_namespace}/skdepl"
        )

    @pytest.mark.parametrize(("project_name", "project_namespace"), NAMESPACE_CASES)
    def test_grpc_inference_request_addresses_the_namespace(
        self, mocker, project_name, project_namespace
    ):
        # Arrange
        api = ServingApi()
        istio_client = MagicMock()
        mocker.patch(
            "hsml.core.serving_api.client.istio._get_instance",
            return_value=istio_client,
        )
        deployment = _inference_deployment(
            project_name, project_namespace, api_protocol=IE.API_PROTOCOL_GRPC
        )
        infer_input = InferInput(name="input", shape=[1], datatype="FP32", data=[1.0])

        # Act
        api._send_inference_request(deployment, [infer_input])

        # Assert
        istio_client._create_grpc_channel.assert_called_once_with(
            f"/v1/{project_namespace}/skdepl"
        )
        # The channel is built once and kept on the deployment, so an address
        # built here outlives the call that built it.
        assert (
            deployment._grpc_channel is istio_client._create_grpc_channel.return_value
        )

    def test_rest_inference_request_through_hopsworks_addresses_neither(self, mocker):
        # Arrange
        api = ServingApi()
        hopsworks_client = _patch_client(mocker, {"predictions": [1]})
        deployment = _inference_deployment(
            "model_serving_int_MhGJP", "loadtest-kserve-0-main-000"
        )

        # Act
        api._send_inference_request(
            deployment, {"inputs": [[1]]}, through_hopsworks=True
        )

        # Assert: Hopsworks proxies by deployment name under the project id and
        # resolves the namespace itself, so neither name belongs in this path.
        args, kwargs = hopsworks_client._send_request.call_args
        assert args[1] == ["project", 1, "inference", "models", "skdepl:predict"]
        assert kwargs["with_base_path_params"] is True

    def test_rest_inference_request_without_istio_falls_back_to_hopsworks(self, mocker):
        # Arrange
        api = ServingApi()
        hopsworks_client = _patch_client(mocker, {"predictions": [1]})
        mocker.patch(
            "hsml.core.serving_api.client.istio._get_instance", return_value=None
        )
        deployment = _inference_deployment(
            "model_serving_int_MhGJP", "loadtest-kserve-0-main-000"
        )

        # Act
        api._send_inference_request(deployment, {"inputs": [[1]]})

        # Assert
        args, _ = hopsworks_client._send_request.call_args
        assert args[1] == ["project", 1, "inference", "models", "skdepl:predict"]

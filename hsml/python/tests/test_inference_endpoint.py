#
#   Copyright 2024 Hopsworks AB
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

import copy

import humps
from hsml import inference_endpoint


class TestInferenceEndpoint:
    # InferenceEndpointPort

    # from response json

    def test_from_response_json_port(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_port"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_ie_from_json = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpointPort.from_json"
        )

        # Act
        _ = inference_endpoint.InferenceEndpointPort.from_response_json(json_camelized)

        # Assert
        mock_ie_from_json.assert_called_once_with(json)

    # from json

    def test_from_json_port(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_port"]["response"]
        mock_ie_extract_fields = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpointPort.extract_fields_from_json",
            return_value=json,
        )
        mock_ie_init = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpointPort.__init__", return_value=None
        )

        # Act
        _ = inference_endpoint.InferenceEndpointPort.from_json(json)

        # Assert
        mock_ie_extract_fields.assert_called_once_with(json)
        mock_ie_init.assert_called_once_with(**json)

    # constructor

    def test_constructor_port(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_port"]["response"]

        # Act
        ie_port = inference_endpoint.InferenceEndpointPort(
            name=json["name"], number=json["number"]
        )

        # Assert
        assert ie_port.name == json["name"]
        assert ie_port.number == json["number"]

    # extract fields from json

    def test_extract_fields_from_json_port(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_port"]["response"]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = inference_endpoint.InferenceEndpointPort.extract_fields_from_json(
            json_copy
        )

        # Assert
        assert kwargs["name"] == json["name"]
        assert kwargs["number"] == json["number"]

    # InferenceEndpoint

    # from response json

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_empty"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_ie_from_json = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpoint.from_json"
        )

        # Act
        ie = inference_endpoint.InferenceEndpoint.from_response_json(json_camelized)

        # Assert
        assert isinstance(ie, list)
        assert len(ie) == 0
        mock_ie_from_json.assert_not_called()

    def test_from_response_json_singleton(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_singleton"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_ie_from_json = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpoint.from_json"
        )

        # Act
        ie = inference_endpoint.InferenceEndpoint.from_response_json(json_camelized)

        # Assert
        assert isinstance(ie, list)
        assert len(ie) == 1
        mock_ie_from_json.assert_called_once_with(json["items"][0])

    def test_from_response_json_list(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_list"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_ie_from_json = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpoint.from_json"
        )

        # Act
        ie = inference_endpoint.InferenceEndpoint.from_response_json(json_camelized)

        # Assert
        assert isinstance(ie, list)
        assert len(ie) == json["count"]
        assert mock_ie_from_json.call_count == json["count"]

    def test_from_response_json_single(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_singleton"]["response"][
            "items"
        ][0]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_ie_from_json = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpoint.from_json"
        )

        # Act
        _ = inference_endpoint.InferenceEndpoint.from_response_json(json_camelized)

        # Assert
        mock_ie_from_json.assert_called_once_with(json)

    # from json

    def test_from_json(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_singleton"]["response"][
            "items"
        ][0]
        mock_ie_extract_fields = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpoint.extract_fields_from_json",
            return_value=json,
        )
        mock_ie_init = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpoint.__init__", return_value=None
        )

        # Act
        _ = inference_endpoint.InferenceEndpoint.from_json(json)

        # Assert
        mock_ie_extract_fields.assert_called_once_with(json)
        mock_ie_init.assert_called_once_with(**json)

    # constructor

    def test_constructor(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_singleton"]["response"][
            "items"
        ][0]

        # Act
        ie = inference_endpoint.InferenceEndpoint(
            type=json["type"], hosts=json["hosts"], ports=json["ports"]
        )

        # Assert
        assert isinstance(ie, inference_endpoint.InferenceEndpoint)
        assert ie.type == json["type"]
        assert ie.hosts == json["hosts"]
        assert ie.ports == json["ports"]

    # extract fields from json

    def test_extract_fields_from_json(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_singleton"]["response"][
            "items"
        ][0]
        json_copy = copy.deepcopy(json)
        mock_ie_port_from_json = mocker.patch(
            "hsml.inference_endpoint.InferenceEndpointPort.from_json", return_value=None
        )

        # Act
        kwargs = inference_endpoint.InferenceEndpoint.extract_fields_from_json(
            json_copy
        )

        # Assert
        assert kwargs["type"] == json["type"]
        assert kwargs["hosts"] == json["hosts"]
        mock_ie_port_from_json.assert_called_once_with(json["ports"][0])

    # get any host

    def test_get_any_host(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_singleton"]["response"][
            "items"
        ][0]
        ie = inference_endpoint.InferenceEndpoint(
            type=None, hosts=json["hosts"], ports=None
        )
        mocker_random_choice = mocker.patch("random.choice", return_value=None)

        # Act
        _ = ie.get_any_host()

        # Assert
        mocker_random_choice.assert_called_once_with(ie.hosts)

    def test_get_any_host_none(self, mocker, backend_fixtures):
        # Arrange
        ie = inference_endpoint.InferenceEndpoint(type=None, hosts=None, ports=None)
        mocker_random_choice = mocker.patch("random.choice", return_value=None)

        # Act
        host = ie.get_any_host()

        # Assert
        assert host is None
        mocker_random_choice.assert_not_called()

    # get port

    def test_get_port_existing(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_list"]["response"]["items"][
            1
        ]
        ports = [
            inference_endpoint.InferenceEndpointPort(p["name"], p["number"])
            for p in json["ports"]
        ]
        ie = inference_endpoint.InferenceEndpoint(type=None, hosts=None, ports=ports)

        # Act
        port = ie.get_port(ports[0].name)

        # Assert
        assert port == ports[0]

    def test_get_port_not_found(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_endpoint"]["get_list"]["response"]["items"][
            1
        ]
        ports = [
            inference_endpoint.InferenceEndpointPort(p["name"], p["number"])
            for p in json["ports"]
        ]
        ie = inference_endpoint.InferenceEndpoint(type=None, hosts=None, ports=ports)

        # Act
        port = ie.get_port("not_found")

        # Assert
        assert port is None

    def test_get_port_none(self):
        # Arrange
        ie = inference_endpoint.InferenceEndpoint(type=None, hosts=None, ports=None)

        # Act
        port = ie.get_port("not_found")

        # Assert
        assert port is None

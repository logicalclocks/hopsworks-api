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
from hopsworks_common.client.exceptions import FeatureStoreException, RestAPIError
from hopsworks_common.core.variable_api import VariableApi


def _patch_send_request(mocker):
    client = mocker.Mock()
    mocker.patch("hopsworks_common.client._get_instance", return_value=client)
    return client._send_request


def _rest_api_error(mocker, status_code: int) -> RestAPIError:
    response = mocker.Mock(status_code=status_code)
    response.json.return_value = {"errorMsg": "error"}
    return RestAPIError("url", response)


class TestVariableApi:
    def test_get_loadbalancer_external_domain(self, mocker):
        # Arrange
        send_request = _patch_send_request(mocker)
        send_request.return_value = {"successMessage": "dn.example.com"}

        # Act
        domain = VariableApi()._get_loadbalancer_external_domain("datanode")

        # Assert
        assert domain == "dn.example.com"
        send_request.assert_called_once_with(
            "GET", ["variables", "loadbalancer_external_domain_datanode"]
        )

    def test_get_loadbalancer_external_domain_not_found(self, mocker):
        # Arrange
        error = _rest_api_error(mocker, RestAPIError.STATUS_CODE_NOT_FOUND)
        _patch_send_request(mocker).side_effect = error

        # Act & Assert
        with pytest.raises(FeatureStoreException) as e:
            VariableApi()._get_loadbalancer_external_domain("datanode")
        assert "loadbalancer_external_domain_datanode" in str(e.value)
        assert e.value.__cause__ is error

    @pytest.mark.parametrize(
        "status_code",
        [
            RestAPIError.STATUS_CODE_FORBIDDEN,
            RestAPIError.STATUS_CODE_INTERNAL_SERVER_ERROR,
        ],
    )
    def test_get_loadbalancer_external_domain_other_error_propagates(
        self, mocker, status_code
    ):
        # Arrange
        error = _rest_api_error(mocker, status_code)
        _patch_send_request(mocker).side_effect = error

        # Act & Assert
        with pytest.raises(RestAPIError) as e:
            VariableApi()._get_loadbalancer_external_domain("datanode")
        assert e.value is error

    def test_get_loadbalancer_external_port(self, mocker):
        # Arrange
        send_request = _patch_send_request(mocker)
        send_request.return_value = {"successMessage": "8020"}

        # Act
        port = VariableApi()._get_loadbalancer_external_port("namenode")

        # Assert
        assert port == 8020
        send_request.assert_called_once_with(
            "GET", ["variables", "loadbalancer_external_port_namenode"]
        )

    def test_get_loadbalancer_external_port_not_found(self, mocker):
        # Arrange
        error = _rest_api_error(mocker, RestAPIError.STATUS_CODE_NOT_FOUND)
        _patch_send_request(mocker).side_effect = error

        # Act & Assert
        assert VariableApi()._get_loadbalancer_external_port("namenode") is None

    def test_get_loadbalancer_external_port_not_a_number(self, mocker):
        # Arrange
        _patch_send_request(mocker).return_value = {"successMessage": ""}

        # Act & Assert
        with pytest.raises(FeatureStoreException) as e:
            VariableApi()._get_loadbalancer_external_port("namenode")
        assert "loadbalancer_external_port_namenode" in str(e.value)

    @pytest.mark.parametrize(
        "status_code",
        [
            RestAPIError.STATUS_CODE_UNAUTHORIZED,
            RestAPIError.STATUS_CODE_FORBIDDEN,
            RestAPIError.STATUS_CODE_INTERNAL_SERVER_ERROR,
        ],
    )
    def test_get_loadbalancer_external_port_other_error_propagates(
        self, mocker, status_code
    ):
        # Arrange
        error = _rest_api_error(mocker, status_code)
        _patch_send_request(mocker).side_effect = error

        # Act & Assert
        with pytest.raises(RestAPIError) as e:
            VariableApi()._get_loadbalancer_external_port("namenode")
        assert e.value is error

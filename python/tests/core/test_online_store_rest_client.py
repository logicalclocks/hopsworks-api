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
import pytest
from furl import furl
from hopsworks_common.client import exceptions, online_store_rest_client


class TestOnlineStoreRestClient:
    def test_setup_rest_client_external(self, mocker):
        # Arrange
        online_store_rest_client._online_store_rest_client = None

        mocker.patch("hopsworks_common.client._is_external", return_value=True)
        mocker.patch("hopsworks_common.client.get_instance")
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_loadbalancer_external_domain",
            return_value="app.hopsworks.ai",
        )
        ping_rdrs_mock = mocker.patch(
            "hopsworks_common.client.online_store_rest_client.OnlineStoreRestClientSingleton.is_connected",
        )

        # Act
        online_store_rest_client.init_or_reset_online_store_rest_client()
        online_store_rest_client_instance = online_store_rest_client.get_instance()

        # Assert
        variable_api_mock.assert_called_once()
        assert (
            online_store_rest_client_instance._current_config["host"]
            == "app.hopsworks.ai"
        )
        assert online_store_rest_client_instance._current_config["port"] == 4406
        assert online_store_rest_client_instance._current_config["verify_certs"] is True
        assert online_store_rest_client_instance._base_url == furl(
            "https://app.hopsworks.ai:4406/0.1.0"
        )
        assert ping_rdrs_mock.call_count == 1

    def test_setup_online_store_rest_client_internal(self, mocker):
        # Arrange
        online_store_rest_client._online_store_rest_client = None

        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mocker.patch("hopsworks_common.client.get_instance")
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_service_discovery_domain",
            return_value="consul",
        )
        optional_config = {"api_key": "provided_api_key"}
        ping_rdrs_mock = mocker.patch(
            "hopsworks_common.client.online_store_rest_client.OnlineStoreRestClientSingleton.is_connected",
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException):
            online_store_rest_client.init_or_reset_online_store_rest_client()
        online_store_rest_client.init_or_reset_online_store_rest_client(
            optional_config=optional_config
        )
        online_store_rest_client_instance = online_store_rest_client.get_instance()

        # Assert
        assert variable_api_mock.call_count == 2
        assert (
            online_store_rest_client_instance._current_config["host"]
            == "rdrs.service.consul"
        )
        assert online_store_rest_client_instance._current_config["port"] == 4406
        assert online_store_rest_client_instance._current_config["verify_certs"] is True
        assert online_store_rest_client_instance._base_url == furl(
            "https://rdrs.service.consul:4406/0.1.0"
        )
        assert online_store_rest_client_instance._auth._token == "provided_api_key"
        assert ping_rdrs_mock.call_count == 1

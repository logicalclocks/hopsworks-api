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

from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hsml.connection import (
    HOPSWORKS_PORT_DEFAULT,
    HOSTNAME_VERIFICATION_DEFAULT,
    Connection,
)
from hsml.constants import HOSTS


class TestConnection:
    # constants

    def test_constants(self):
        # The purpose of this test is to ensure that (1) we don't make undesired changes to contant values
        # that might break things somewhere else, and (2) we remember to update the pytests accordingly by
        # adding / removing / updating tests, if necessary.
        assert HOSTS.SAAS_HOST == "eu-west.cloud.hopsworks.ai"
        assert HOPSWORKS_PORT_DEFAULT == 443
        assert HOSTNAME_VERIFICATION_DEFAULT is False

    # constructor

    def test_constructor_default(self, mocker):
        # Arrange
        class MockConnection:
            pass

        mock_connection = MockConnection()
        mock_connection.connect = mocker.MagicMock()
        mock_connection.init = Connection.__init__

        # Act
        mock_connection.init(mock_connection)

        # Assert
        assert mock_connection._host is None
        assert mock_connection._port == HOPSWORKS_PORT_DEFAULT
        assert mock_connection._project is None
        assert mock_connection._hostname_verification == HOSTNAME_VERIFICATION_DEFAULT
        assert mock_connection._trust_store_path is None
        assert mock_connection._api_key_file is None
        assert mock_connection._api_key_value is None
        assert not mock_connection._connected
        mock_connection.connect.assert_called_once()

    def test_constructor(self, mocker):
        # Arrange
        class MockConnection:
            pass

        mock_connection = MockConnection()
        mock_connection.connect = mocker.MagicMock()
        mock_connection.init = Connection.__init__

        # Act
        mock_connection.init(
            mock_connection,
            host="host",
            port=1234,
            project="project",
            hostname_verification=False,
            trust_store_path="ts_path",
            api_key_file="ak_file",
            api_key_value="ak_value",
        )

        # Assert
        assert mock_connection._host == "host"
        assert mock_connection._port == 1234
        assert mock_connection._project == "project"
        assert not mock_connection._hostname_verification
        assert mock_connection._trust_store_path == "ts_path"
        assert mock_connection._api_key_file == "ak_file"
        assert mock_connection._api_key_value == "ak_value"
        assert not mock_connection._connected
        mock_connection.connect.assert_called_once()

    # handlers

    def test_get_model_registry(self, mocker):
        # Arrange
        mock_connection = mocker.MagicMock()
        mock_connection.get_model_registry = Connection.get_model_registry
        mock_connection._model_registry_api = mocker.MagicMock()
        mock_connection._model_registry_api.get = mocker.MagicMock(return_value="mr")

        # Act
        mr = mock_connection.get_model_registry(mock_connection)

        # Assert
        assert mr == "mr"
        mock_connection._model_registry_api.get.assert_called_once()

    def test_get_model_serving(self, mocker):
        # Arrange
        mock_connection = mocker.MagicMock()
        mock_connection.get_model_serving = Connection.get_model_serving
        mock_connection._model_serving_api = mocker.MagicMock()
        mock_connection._model_serving_api.get = mocker.MagicMock(return_value="ms")

        # Act
        ms = mock_connection.get_model_serving(mock_connection)

        # Assert
        assert ms == "ms"
        mock_connection._model_serving_api.get.assert_called_once()

    # connection

    # TODO: Add tests for connection-related methods

    def test_connect(self, mocker):
        pass

    def test_close(self, mocker):
        pass

    def test_connection(self, mocker):
        pass


class TestProvideProject:
    # Tests for hopsworks_common.connection.Connection._provide_project, which runs
    # during hopsworks.login() after the client is initialized and is responsible for
    # loading the default model serving configuration.

    @pytest.fixture
    def conn(self):
        conn = MagicMock(spec=Connection)
        conn._connected = True
        conn._project = None
        conn._variable_api = MagicMock()
        conn._model_serving_api = MagicMock()
        # Bind the real method so the decorator can read _connected from our mock.
        conn._provide_project = Connection._provide_project.__get__(conn, Connection)
        return conn

    @pytest.fixture
    def client_instance(self, mocker):
        hw_client = MagicMock()
        hw_client._project_name = None
        hw_client._is_external.return_value = True
        hw_client.provide_project = MagicMock()
        mocker.patch(
            "hopsworks_common.connection.client.get_instance", return_value=hw_client
        )
        return hw_client

    @pytest.fixture(autouse=True)
    def _stub_hsfs_engine(self, mocker):
        mocker.patch("hsfs.engine.get_instance")

    def _make_rest_error(self, status_error_code, error_code):
        err = RestAPIError.__new__(RestAPIError)
        err.response = MagicMock()
        err.response.error_code = status_error_code
        err.error_code = error_code
        return err

    def test_name_delegates_to_external_client(self, conn, client_instance):
        conn._variable_api.get_data_science_profile_enabled.return_value = False

        conn._provide_project(name="proj")

        assert conn._project == "proj"
        client_instance.provide_project.assert_called_once_with("proj")

    def test_name_is_not_forwarded_when_internal(self, conn, client_instance):
        client_instance._is_external.return_value = False
        conn._variable_api.get_data_science_profile_enabled.return_value = False

        conn._provide_project(name="proj")

        assert conn._project == "proj"
        client_instance.provide_project.assert_not_called()

    def test_uses_client_project_name_when_already_set(self, conn, client_instance):
        client_instance._project_name = "already-set"
        conn._variable_api.get_data_science_profile_enabled.return_value = False

        conn._provide_project()

        assert conn._project == "already-set"

    def test_short_circuits_when_no_project(self, conn, client_instance):
        conn._provide_project()

        conn._variable_api.get_data_science_profile_enabled.assert_not_called()
        conn._model_serving_api.load_default_configuration.assert_not_called()

    def test_loads_default_configuration_when_profile_enabled(
        self, conn, client_instance
    ):
        client_instance._project_name = "proj"
        conn._variable_api.get_data_science_profile_enabled.return_value = True

        conn._provide_project()

        conn._model_serving_api.load_default_configuration.assert_called_once()

    def test_skips_default_configuration_when_profile_disabled(
        self, conn, client_instance
    ):
        client_instance._project_name = "proj"
        conn._variable_api.get_data_science_profile_enabled.return_value = False

        conn._provide_project()

        conn._model_serving_api.load_default_configuration.assert_not_called()

    def test_missing_serving_scope_is_swallowed(self, conn, client_instance, capsys):
        # 403 + 320004 means the API key lacks the SERVING scope; model serving is disabled
        # but login must still succeed.
        client_instance._project_name = "proj"
        conn._variable_api.get_data_science_profile_enabled.return_value = True
        conn._model_serving_api.load_default_configuration.side_effect = (
            self._make_rest_error(status_error_code=403, error_code=320004)
        )

        conn._provide_project()

        assert "SERVING" in capsys.readouterr().out

    def test_other_rest_api_errors_are_reraised(self, conn, client_instance):
        client_instance._project_name = "proj"
        conn._variable_api.get_data_science_profile_enabled.return_value = True
        conn._model_serving_api.load_default_configuration.side_effect = (
            self._make_rest_error(status_error_code=500, error_code=999999)
        )

        with pytest.raises(RestAPIError):
            conn._provide_project()

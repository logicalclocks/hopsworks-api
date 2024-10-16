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
        assert HOSTS.APP_HOST == "c.app.hopsworks.ai"
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

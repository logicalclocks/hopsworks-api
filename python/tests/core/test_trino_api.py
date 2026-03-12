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
from unittest.mock import Mock

import pytest
from hopsworks_common.client.exceptions import TrinoException
from hopsworks_common.core import trino_api


class TestTrinoApi:
    def test_retrieve_host_external_client(self, mocker):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.core.trino_api.client")
        mock_client._is_external.return_value = True

        mock_variable_api = Mock()
        mock_variable_api.get_loadbalancer_external_domain.return_value = (
            "trino.example.com"
        )
        mock_variable_api.get_service_discovery_domain.return_value = "cluster.local"

        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch("hopsworks_common.core.trino_api.secret_api.SecretsApi")
        mocker.patch("hopsworks_common.core.trino_api.project_api.ProjectApi")
        mocker.patch("hopsworks_common.core.trino_api.hopsworks.get_current_project")

        api = trino_api._TrinoApi()

        # Act
        host = api._retrieve_host()

        # Assert
        assert host == "trino.example.com"
        mock_variable_api.get_loadbalancer_external_domain.assert_called_once_with(
            "trino"
        )

    def test_retrieve_host_internal_client(self, mocker):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.core.trino_api.client")
        mock_client._is_external.return_value = False

        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = "cluster.local"

        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch("hopsworks_common.core.trino_api.secret_api.SecretsApi")
        mocker.patch("hopsworks_common.core.trino_api.project_api.ProjectApi")
        mocker.patch("hopsworks_common.core.trino_api.hopsworks.get_current_project")

        api = trino_api._TrinoApi()

        # Act
        host = api._retrieve_host()

        # Assert
        expected_host = f"{trino_api.TRINO_SERVICE_NAME}.cluster.local"
        assert host == expected_host

    def test_retrieve_host_internal_client_missing_service_discovery_domain(
        self, mocker
    ):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.core.trino_api.client")
        mock_client._is_external.return_value = False

        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = ""

        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch("hopsworks_common.core.trino_api.secret_api.SecretsApi")
        mocker.patch("hopsworks_common.core.trino_api.project_api.ProjectApi")
        mocker.patch("hopsworks_common.core.trino_api.hopsworks.get_current_project")

        api = trino_api._TrinoApi()

        # Act & Assert
        with pytest.raises(TrinoException) as exc_info:
            api._retrieve_host()
        assert "service_discovery_domain" in str(exc_info.value)

    def test_trino_connect_success(self, mocker):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.core.trino_api.client")
        mock_client._is_external.return_value = False

        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = "cluster.local"

        mock_secret = Mock()
        mock_secret.value = "test_password"
        mock_secret_api = Mock()
        mock_secret_api.get_secret.return_value = mock_secret

        mock_project_api = Mock()
        mock_project_api.get_user_info.return_value = {"username": "test_user"}

        mock_project = Mock()
        mock_project.name = "test_project"

        mock_connection = Mock()
        mock_trino_connect = mocker.patch(
            "hopsworks_common.core.trino_api._trino_connect",
            return_value=mock_connection,
        )

        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.secret_api.SecretsApi",
            return_value=mock_secret_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.project_api.ProjectApi",
            return_value=mock_project_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.hopsworks.get_current_project",
            return_value=mock_project,
        )

        api = trino_api._TrinoApi(catalog="iceberg", schema="test_db")

        # Act
        connection = api.trino_connect()

        # Assert
        assert connection == mock_connection
        mock_trino_connect.assert_called_once()
        call_kwargs = mock_trino_connect.call_args.kwargs
        assert call_kwargs["catalog"] == "iceberg"
        assert call_kwargs["schema"] == "test_db"
        assert call_kwargs["user"] == "test_project__test_user"
        assert call_kwargs["verify"] is False

    def test_create_engine_success(self, mocker):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.core.trino_api.client")
        mock_client._is_external.return_value = False

        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = "cluster.local"

        mock_secret = Mock()
        mock_secret.value = "test_password"
        mock_secret_api = Mock()
        mock_secret_api.get_secret.return_value = mock_secret

        mock_project_api = Mock()
        mock_project_api.get_user_info.return_value = {"username": "test_user"}

        mock_project = Mock()
        mock_project.name = "test_project"

        mock_engine = Mock()
        mock_create_engine = mocker.patch(
            "sqlalchemy.create_engine", return_value=mock_engine
        )

        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.secret_api.SecretsApi",
            return_value=mock_secret_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.project_api.ProjectApi",
            return_value=mock_project_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.hopsworks.get_current_project",
            return_value=mock_project,
        )

        api = trino_api._TrinoApi(catalog="iceberg", schema="test_db")

        # Act
        engine = api.create_engine()

        # Assert
        assert engine == mock_engine
        mock_create_engine.assert_called_once()
        # Check connect_args
        call_kwargs = mock_create_engine.call_args.kwargs
        assert call_kwargs["connect_args"]["verify"] is False
        assert "auth" in call_kwargs["connect_args"]

    def test_connect_function_calls_trino_api(self, mocker):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.core.trino_api.client")
        mock_client._is_external.return_value = False

        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = "cluster.local"

        mock_secret = Mock()
        mock_secret.value = "test_password"
        mock_secret_api = Mock()
        mock_secret_api.get_secret.return_value = mock_secret

        mock_project_api = Mock()
        mock_project_api.get_user_info.return_value = {"username": "test_user"}

        mock_project = Mock()
        mock_project.name = "test_project"

        mock_connection = Mock()
        mocker.patch(
            "hopsworks_common.core.trino_api._trino_connect",
            return_value=mock_connection,
        )

        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.secret_api.SecretsApi",
            return_value=mock_secret_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.project_api.ProjectApi",
            return_value=mock_project_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.hopsworks.get_current_project",
            return_value=mock_project,
        )

        # Act
        connection = trino_api.connect(catalog="iceberg", schema="test_db")

        # Assert
        assert connection == mock_connection

    def test_create_engine_function_calls_trino_api(self, mocker):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.core.trino_api.client")
        mock_client._is_external.return_value = False

        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = "cluster.local"

        mock_secret = Mock()
        mock_secret.value = "test_password"
        mock_secret_api = Mock()
        mock_secret_api.get_secret.return_value = mock_secret

        mock_project_api = Mock()
        mock_project_api.get_user_info.return_value = {"username": "test_user"}

        mock_project = Mock()
        mock_project.name = "test_project"

        mock_engine = Mock()
        mocker.patch("sqlalchemy.create_engine", return_value=mock_engine)

        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.secret_api.SecretsApi",
            return_value=mock_secret_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.project_api.ProjectApi",
            return_value=mock_project_api,
        )
        mocker.patch(
            "hopsworks_common.core.trino_api.hopsworks.get_current_project",
            return_value=mock_project,
        )

        # Act
        engine = trino_api.create_engine(catalog="iceberg", schema="test_db")

        # Assert
        assert engine == mock_engine

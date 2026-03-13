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
from hopsworks_common.core.trino_api import (
    DEFAULT_SOURCE,
    DEFAULT_SQLALCHEMY_SOURCE,
    TRINO_PORT,
    TRINO_SERVICE_NAME,
    TrinoApi,
)
from trino import constants
from trino.auth import BasicAuthentication
from trino.transaction import IsolationLevel


class TestTrinoApi:
    """Test suite for TrinoApi class."""

    @pytest.fixture
    def mock_project(self):
        """Create a mock project."""
        project = Mock()
        project.name = "test_project"
        return project

    @pytest.fixture
    def mock_secret(self):
        """Create a mock secret."""
        secret = Mock()
        secret.value = "test_password"
        return secret

    @pytest.fixture
    def trino_api(self, mocker, mock_project, mock_secret):
        """Create a TrinoApi instance with mocked dependencies."""
        # Mock VariableApi
        mock_variable_api = Mock()
        mock_variable_api.get_service_discovery_domain.return_value = "example.local"
        mocker.patch(
            "hopsworks_common.core.trino_api.VariableApi",
            return_value=mock_variable_api,
        )

        # Mock SecretsApi
        mock_secret_api = Mock()
        mock_secret_api.get_secret.return_value = mock_secret
        mocker.patch(
            "hopsworks_common.core.trino_api.secret_api.SecretsApi",
            return_value=mock_secret_api,
        )

        # Mock ProjectApi
        mock_project_api = Mock()
        mock_project_api.get_user_info.return_value = {"username": "test_user"}
        mocker.patch(
            "hopsworks_common.core.trino_api.project_api.ProjectApi",
            return_value=mock_project_api,
        )

        # Mock hopsworks.get_current_project (since it's imported inside __init__)
        mocker.patch(
            "hopsworks.get_current_project",
            return_value=mock_project,
        )

        return TrinoApi(project=mock_project)

    def test_init_with_project(self, mocker, mock_project):
        """Test TrinoApi initialization with an explicit project."""
        # Arrange
        mocker.patch("hopsworks_common.core.trino_api.VariableApi")
        mocker.patch("hopsworks_common.core.trino_api.secret_api.SecretsApi")
        mocker.patch("hopsworks_common.core.trino_api.project_api.ProjectApi")

        # Act
        api = TrinoApi(project=mock_project)

        # Assert
        assert api.project == mock_project

    def test_init_without_project(self, mocker, mock_project):
        """Test TrinoApi initialization without an explicit project."""
        # Arrange
        mocker.patch("hopsworks_common.core.trino_api.VariableApi")
        mocker.patch("hopsworks_common.core.trino_api.secret_api.SecretsApi")
        mocker.patch("hopsworks_common.core.trino_api.project_api.ProjectApi")
        mocker.patch(
            "hopsworks.get_current_project",
            return_value=mock_project,
        )

        # Act
        api = TrinoApi()

        # Assert
        assert api.project == mock_project

    def test_download_ssl_cert_internal_client(self, mocker, trino_api):
        """Test SSL certificate download for internal client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=False
        )
        mock_client = Mock()
        mock_client.get_certs_folder.return_value = "/path/to/certs"
        mocker.patch(
            "hopsworks_common.core.trino_api.client.get_instance",
            return_value=mock_client,
        )

        # Act
        result = trino_api._download_ssl_cert(verify=True)

        # Assert
        assert result == "/path/to/certs/ca_chain.pem"
        mock_client.download_certs.assert_called_once()

    def test_download_ssl_cert_internal_client_no_verify(self, mocker, trino_api):
        """Test SSL certificate download when verify is False."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=False
        )

        # Act
        result = trino_api._download_ssl_cert(verify=False)

        # Assert
        assert result is False

    def test_download_ssl_cert_external_client(self, mocker, trino_api):
        """Test SSL certificate download for external client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=True
        )

        # Act
        result = trino_api._download_ssl_cert(verify=True)

        # Assert
        assert result is False

    def test_get_host_internal_client(self, mocker, trino_api):
        """Test getting host for internal client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=False
        )

        # Act
        host = trino_api.get_host()

        # Assert
        expected_host = f"{TRINO_SERVICE_NAME}.example.local"
        assert host == expected_host

    def test_get_host_external_client(self, mocker, trino_api):
        """Test getting host for external client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=True
        )
        trino_api._variable_api.get_loadbalancer_external_domain.return_value = (
            "trino.example.com"
        )

        # Act
        host = trino_api.get_host()

        # Assert
        assert host == "trino.example.com"
        trino_api._variable_api.get_loadbalancer_external_domain.assert_called_once_with(
            "trino"
        )

    def test_get_host_internal_no_service_discovery_domain(self, mocker, trino_api):
        """Test getting host for internal client when service discovery domain is not configured."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=False
        )
        trino_api._service_discovery_domain = ""

        # Act & Assert
        with pytest.raises(
            TrinoException,
            match="Client could not get Trino hostname from service_discovery_domain",
        ):
            trino_api.get_host()

    def test_get_port(self, trino_api):
        """Test getting port number."""
        # Act
        port = trino_api.get_port()

        # Assert
        assert port == TRINO_PORT

    def test_get_password(self, trino_api, mock_secret):
        """Test retrieving password from secrets storage."""
        # Arrange
        user = "test_project__test_user"

        # Act
        password = trino_api._get_password(user)

        # Assert
        assert password == "test_password"
        trino_api._secret_api.get_secret.assert_called_once_with(user)

    def test_get_password_secret_not_found(self, mocker, trino_api):
        """Test retrieving password when secret does not exist."""
        # Arrange
        user = "test_project__test_user"
        trino_api._secret_api.get_secret.return_value = None

        # Act & Assert
        with pytest.raises(
            TrinoException,
            match=f"Client could not retrieve credentials for user {user}",
        ):
            trino_api._get_password(user)

    def test_get_basic_auth(self, trino_api):
        """Test getting BasicAuthentication object."""
        # Act
        auth = trino_api.get_basic_auth()

        # Assert
        assert isinstance(auth, BasicAuthentication)
        assert auth._username == "test_project__test_user"
        assert auth._password == "test_password"

    def test_connect_internal_client(self, mocker, trino_api):
        """Test connecting to Trino using DBAPI from internal client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=False
        )
        mock_client = Mock()
        mock_client.get_certs_folder.return_value = "/path/to/certs"
        mocker.patch(
            "hopsworks_common.core.trino_api.client.get_instance",
            return_value=mock_client,
        )
        mock_connection = Mock()
        mock_trino_connect = mocker.patch(
            "hopsworks_common.core.trino_api._trino_connect",
            return_value=mock_connection,
        )

        # Act
        conn = trino_api.connect(catalog="test_catalog", schema="test_schema")

        # Assert
        assert conn == mock_connection
        expected_host = f"{TRINO_SERVICE_NAME}.example.local"
        mock_trino_connect.assert_called_once()
        call_kwargs = mock_trino_connect.call_args[1]
        assert call_kwargs["host"] == expected_host
        assert call_kwargs["port"] == TRINO_PORT
        assert call_kwargs["user"] == "test_project__test_user"
        assert call_kwargs["catalog"] == "test_catalog"
        assert call_kwargs["schema"] == "test_schema"
        assert call_kwargs["source"] == DEFAULT_SOURCE
        assert isinstance(call_kwargs["auth"], BasicAuthentication)
        assert call_kwargs["http_scheme"] == constants.HTTPS
        assert call_kwargs["verify"] == "/path/to/certs/ca_chain.pem"

    def test_connect_external_client(self, mocker, trino_api):
        """Test connecting to Trino using DBAPI from external client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=True
        )
        trino_api._variable_api.get_loadbalancer_external_domain.return_value = (
            "trino.example.com"
        )
        mock_connection = Mock()
        mock_trino_connect = mocker.patch(
            "hopsworks_common.core.trino_api._trino_connect",
            return_value=mock_connection,
        )

        # Act
        conn = trino_api.connect(
            catalog="iceberg",
            schema="my_db",
            isolation_level=IsolationLevel.READ_UNCOMMITTED,
        )

        # Assert
        assert conn == mock_connection
        call_kwargs = mock_trino_connect.call_args[1]
        assert call_kwargs["host"] == "trino.example.com"
        assert call_kwargs["port"] == TRINO_PORT
        assert call_kwargs["catalog"] == "iceberg"
        assert call_kwargs["schema"] == "my_db"
        assert call_kwargs["isolation_level"] == IsolationLevel.READ_UNCOMMITTED
        assert call_kwargs["verify"] is False

    def test_connect_with_custom_parameters(self, mocker, trino_api):
        """Test connecting to Trino with custom parameters."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=True
        )
        trino_api._variable_api.get_loadbalancer_external_domain.return_value = (
            "trino.example.com"
        )
        mock_connection = Mock()
        mock_trino_connect = mocker.patch(
            "hopsworks_common.core.trino_api._trino_connect",
            return_value=mock_connection,
        )
        custom_source = "my-custom-source"
        custom_headers = {"X-Custom": "Header"}
        session_props = {"query_max_memory": "10GB"}

        # Act
        conn = trino_api.connect(
            source=custom_source,
            http_headers=custom_headers,
            session_properties=session_props,
            max_attempts=5,
            request_timeout=120,
            client_tags=["tag1", "tag2"],
        )

        # Assert
        assert conn == mock_connection
        call_kwargs = mock_trino_connect.call_args[1]
        assert call_kwargs["source"] == custom_source
        assert call_kwargs["http_headers"] == custom_headers
        assert call_kwargs["session_properties"] == session_props
        assert call_kwargs["max_attempts"] == 5
        assert call_kwargs["request_timeout"] == 120
        assert call_kwargs["client_tags"] == ["tag1", "tag2"]

    def test_create_engine_internal_client(self, mocker, trino_api):
        """Test creating SQLAlchemy engine from internal client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=False
        )
        mock_client = Mock()
        mock_client.get_certs_folder.return_value = "/path/to/certs"
        mocker.patch(
            "hopsworks_common.core.trino_api.client.get_instance",
            return_value=mock_client,
        )
        mock_engine = Mock()
        mock_create_engine = mocker.patch(
            "sqlalchemy.create_engine",
            return_value=mock_engine,
        )

        # Act
        engine = trino_api.create_engine(catalog="test_catalog", schema="test_schema")

        # Assert
        assert engine == mock_engine
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args
        url_str = str(call_args[0][0])
        connect_args = call_args[1]["connect_args"]

        # Verify URL string contains expected parts
        assert f"{TRINO_SERVICE_NAME}.example.local" in url_str
        assert str(TRINO_PORT) in url_str
        assert "test_project__test_user" in url_str
        assert "test_catalog" in url_str
        assert "test_schema" in url_str

        # Verify connect_args
        assert isinstance(connect_args["auth"], BasicAuthentication)
        assert connect_args["http_scheme"] == constants.HTTPS
        assert connect_args["verify"] == "/path/to/certs/ca_chain.pem"
        assert connect_args["source"] == DEFAULT_SQLALCHEMY_SOURCE

    def test_create_engine_external_client(self, mocker, trino_api):
        """Test creating SQLAlchemy engine from external client."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=True
        )
        trino_api._variable_api.get_loadbalancer_external_domain.return_value = (
            "trino.example.com"
        )
        mock_engine = Mock()
        mock_create_engine = mocker.patch(
            "sqlalchemy.create_engine",
            return_value=mock_engine,
        )

        # Act
        engine = trino_api.create_engine(catalog="iceberg", schema="my_db")

        # Assert
        assert engine == mock_engine
        call_args = mock_create_engine.call_args
        url_str = str(call_args[0][0])
        connect_args = call_args[1]["connect_args"]

        # Verify URL string contains expected parts
        assert "trino.example.com" in url_str
        assert "iceberg" in url_str
        assert "my_db" in url_str
        assert connect_args["verify"] is False

    def test_create_engine_with_custom_parameters(self, mocker, trino_api):
        """Test creating SQLAlchemy engine with custom parameters."""
        # Arrange
        mocker.patch(
            "hopsworks_common.core.trino_api.client._is_external", return_value=True
        )
        trino_api._variable_api.get_loadbalancer_external_domain.return_value = (
            "trino.example.com"
        )
        mock_engine = Mock()
        mock_create_engine = mocker.patch(
            "sqlalchemy.create_engine",
            return_value=mock_engine,
        )
        custom_source = "my-sqlalchemy-source"
        session_props = {"optimize_hash_generation": "true"}

        # Act
        engine = trino_api.create_engine(
            source=custom_source,
            session_properties=session_props,
            max_attempts=10,
            isolation_level=IsolationLevel.READ_COMMITTED,
            timezone="UTC",
        )

        # Assert
        assert engine == mock_engine
        connect_args = mock_create_engine.call_args[1]["connect_args"]
        assert connect_args["source"] == custom_source
        assert connect_args["session_properties"] == session_props
        assert connect_args["max_attempts"] == 10
        assert connect_args["isolation_level"] == IsolationLevel.READ_COMMITTED
        assert connect_args["timezone"] == "UTC"

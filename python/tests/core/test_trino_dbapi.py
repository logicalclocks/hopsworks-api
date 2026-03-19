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
from hopsworks_common.core import trino_dbapi
from hopsworks_common.core.trino_api import (
    DEFAULT_SOURCE,
    DEFAULT_SQLALCHEMY_SOURCE,
)
from trino.transaction import IsolationLevel


class TestTrinoDbapi:
    """Test suite for trino_dbapi module-level convenience functions."""

    @pytest.fixture
    def mock_trino_api(self, mocker):
        """Create a mock TrinoApi instance."""
        mock_api = Mock()
        mock_api.connect.return_value = Mock()  # Mock connection
        mock_api.create_engine.return_value = Mock()  # Mock engine
        mocker.patch(
            "hopsworks_common.core.trino_dbapi.TrinoApi",
            return_value=mock_api,
        )
        return mock_api

    def test_connect_default_parameters(self, mock_trino_api):
        """Test connect function with default parameters."""
        # Arrange
        from trino import constants

        # Act
        conn = trino_dbapi.connect()

        # Assert
        assert conn is not None
        mock_trino_api.connect.assert_called_once_with(
            source=DEFAULT_SOURCE,
            catalog=constants.DEFAULT_CATALOG,
            schema=constants.DEFAULT_SCHEMA,
            session_properties=None,
            http_headers=None,
            max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
            request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
            isolation_level=IsolationLevel.AUTOCOMMIT,
            verify=True,
            http_session=None,
            client_tags=None,
            legacy_primitive_types=False,
            legacy_prepared_statements=None,
            roles=None,
            timezone=None,
            encoding=None,
        )

    def test_connect_with_custom_catalog_and_schema(self, mock_trino_api):
        """Test connect function with custom catalog and schema."""
        # Act
        conn = trino_dbapi.connect(catalog="iceberg", schema="my_db")

        # Assert
        assert conn is not None
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["catalog"] == "iceberg"
        assert call_kwargs["schema"] == "my_db"

    def test_connect_with_custom_source(self, mock_trino_api):
        """Test connect function with custom source identifier."""
        # Arrange
        custom_source = "my-custom-app"

        # Act
        trino_dbapi.connect(source=custom_source)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["source"] == custom_source

    def test_connect_with_session_properties(self, mock_trino_api):
        """Test connect function with session properties."""
        # Arrange
        session_props = {
            "query_max_memory": "10GB",
            "optimize_hash_generation": "true",
        }

        # Act
        trino_dbapi.connect(session_properties=session_props)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["session_properties"] == session_props

    def test_connect_with_http_headers(self, mock_trino_api):
        """Test connect function with custom HTTP headers."""
        # Arrange
        headers = {"X-Custom-Header": "value", "X-Application": "test"}

        # Act
        trino_dbapi.connect(http_headers=headers)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["http_headers"] == headers

    def test_connect_with_retry_configuration(self, mock_trino_api):
        """Test connect function with custom retry configuration."""
        # Act
        trino_dbapi.connect(max_attempts=10, request_timeout=120)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["max_attempts"] == 10
        assert call_kwargs["request_timeout"] == 120

    def test_connect_with_isolation_level(self, mock_trino_api):
        """Test connect function with custom isolation level."""
        # Act
        trino_dbapi.connect(isolation_level=IsolationLevel.READ_UNCOMMITTED)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["isolation_level"] == IsolationLevel.READ_UNCOMMITTED

    def test_connect_with_verify_disabled(self, mock_trino_api):
        """Test connect function with SSL verification disabled."""
        # Act
        trino_dbapi.connect(verify=False)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["verify"] is False

    def test_connect_with_custom_verify_path(self, mock_trino_api):
        """Test connect function with custom SSL certificate path."""
        # Arrange
        cert_path = "/path/to/ca.crt"

        # Act
        trino_dbapi.connect(verify=cert_path)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["verify"] == cert_path

    def test_connect_with_http_session(self, mock_trino_api):
        """Test connect function with custom HTTP session."""
        # Arrange
        mock_http_session = Mock()

        # Act
        trino_dbapi.connect(http_session=mock_http_session)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["http_session"] == mock_http_session

    def test_connect_with_client_tags(self, mock_trino_api):
        """Test connect function with client tags."""
        # Arrange
        tags = ["tag1", "tag2", "production"]

        # Act
        trino_dbapi.connect(client_tags=tags)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["client_tags"] == tags

    def test_connect_with_legacy_options(self, mock_trino_api):
        """Test connect function with legacy options."""
        # Act
        trino_dbapi.connect(
            legacy_primitive_types=True,
            legacy_prepared_statements=True,
        )

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["legacy_primitive_types"] is True
        assert call_kwargs["legacy_prepared_statements"] is True

    def test_connect_with_roles(self, mock_trino_api):
        """Test connect function with catalog roles."""
        # Arrange
        roles = {"system": "admin", "catalog": "data_engineer"}

        # Act
        trino_dbapi.connect(roles=roles)

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["roles"] == roles

    def test_connect_with_timezone(self, mock_trino_api):
        """Test connect function with timezone."""
        # Act
        trino_dbapi.connect(timezone="America/New_York")

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["timezone"] == "America/New_York"

    def test_connect_with_encoding(self, mock_trino_api):
        """Test connect function with encoding."""
        # Act
        trino_dbapi.connect(encoding="utf-8")

        # Assert
        call_kwargs = mock_trino_api.connect.call_args[1]
        assert call_kwargs["encoding"] == "utf-8"

    def test_connect_with_all_custom_parameters(self, mock_trino_api):
        """Test connect function with all custom parameters."""
        # Arrange
        params = {
            "source": "my-app",
            "catalog": "iceberg",
            "schema": "analytics",
            "session_properties": {"query_max_execution_time": "1h"},
            "http_headers": {"X-App": "test"},
            "max_attempts": 5,
            "request_timeout": 90,
            "isolation_level": IsolationLevel.READ_COMMITTED,
            "verify": False,
            "http_session": Mock(),
            "client_tags": ["prod"],
            "legacy_primitive_types": True,
            "legacy_prepared_statements": False,
            "roles": {"iceberg": "admin"},
            "timezone": "UTC",
            "encoding": ["utf-8", "latin1"],
        }

        # Act
        conn = trino_dbapi.connect(**params)

        # Assert
        assert conn is not None
        mock_trino_api.connect.assert_called_once_with(**params)

    def test_create_engine_default_parameters(self, mock_trino_api):
        """Test create_engine function with default parameters."""
        # Arrange
        from trino import constants

        # Act
        engine = trino_dbapi.create_engine()

        # Assert
        assert engine is not None
        mock_trino_api.create_engine.assert_called_once_with(
            source=DEFAULT_SQLALCHEMY_SOURCE,
            catalog=constants.DEFAULT_CATALOG,
            schema=constants.DEFAULT_SCHEMA,
            session_properties=None,
            http_headers=None,
            max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
            request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
            isolation_level=IsolationLevel.AUTOCOMMIT,
            verify=True,
            http_session=None,
            client_tags=None,
            legacy_primitive_types=False,
            legacy_prepared_statements=None,
            roles=None,
            timezone=None,
            encoding=None,
        )

    def test_create_engine_with_custom_catalog_and_schema(self, mock_trino_api):
        """Test create_engine function with custom catalog and schema."""
        # Act
        engine = trino_dbapi.create_engine(catalog="iceberg", schema="my_db")

        # Assert
        assert engine is not None
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["catalog"] == "iceberg"
        assert call_kwargs["schema"] == "my_db"

    def test_create_engine_with_custom_source(self, mock_trino_api):
        """Test create_engine function with custom source identifier."""
        # Arrange
        custom_source = "my-sqlalchemy-app"

        # Act
        trino_dbapi.create_engine(source=custom_source)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["source"] == custom_source

    def test_create_engine_with_session_properties(self, mock_trino_api):
        """Test create_engine function with session properties."""
        # Arrange
        session_props = {"optimize_metadata_queries": "true"}

        # Act
        trino_dbapi.create_engine(session_properties=session_props)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["session_properties"] == session_props

    def test_create_engine_with_http_headers(self, mock_trino_api):
        """Test create_engine function with custom HTTP headers."""
        # Arrange
        headers = {"Authorization": "Bearer token"}

        # Act
        trino_dbapi.create_engine(http_headers=headers)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["http_headers"] == headers

    def test_create_engine_with_retry_configuration(self, mock_trino_api):
        """Test create_engine function with custom retry configuration."""
        # Act
        trino_dbapi.create_engine(max_attempts=15, request_timeout=180)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["max_attempts"] == 15
        assert call_kwargs["request_timeout"] == 180

    def test_create_engine_with_isolation_level(self, mock_trino_api):
        """Test create_engine function with custom isolation level."""
        # Act
        trino_dbapi.create_engine(isolation_level=IsolationLevel.READ_COMMITTED)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["isolation_level"] == IsolationLevel.READ_COMMITTED

    def test_create_engine_with_verify_disabled(self, mock_trino_api):
        """Test create_engine function with SSL verification disabled."""
        # Act
        trino_dbapi.create_engine(verify=False)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["verify"] is False

    def test_create_engine_with_custom_verify_path(self, mock_trino_api):
        """Test create_engine function with custom SSL certificate path."""
        # Arrange
        cert_path = "/etc/ssl/certs/ca.pem"

        # Act
        trino_dbapi.create_engine(verify=cert_path)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["verify"] == cert_path

    def test_create_engine_with_http_session(self, mock_trino_api):
        """Test create_engine function with custom HTTP session."""
        # Arrange
        mock_http_session = Mock()

        # Act
        trino_dbapi.create_engine(http_session=mock_http_session)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["http_session"] == mock_http_session

    def test_create_engine_with_client_tags(self, mock_trino_api):
        """Test create_engine function with client tags."""
        # Arrange
        tags = ["sqlalchemy", "production", "analytics"]

        # Act
        trino_dbapi.create_engine(client_tags=tags)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["client_tags"] == tags

    def test_create_engine_with_legacy_options(self, mock_trino_api):
        """Test create_engine function with legacy options."""
        # Act
        trino_dbapi.create_engine(
            legacy_primitive_types=False,
            legacy_prepared_statements=False,
        )

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["legacy_primitive_types"] is False
        assert call_kwargs["legacy_prepared_statements"] is False

    def test_create_engine_with_roles(self, mock_trino_api):
        """Test create_engine function with catalog roles."""
        # Arrange
        roles = {"catalog1": "role1", "catalog2": "role2"}

        # Act
        trino_dbapi.create_engine(roles=roles)

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["roles"] == roles

    def test_create_engine_with_timezone(self, mock_trino_api):
        """Test create_engine function with timezone."""
        # Act
        trino_dbapi.create_engine(timezone="UTC")

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["timezone"] == "UTC"

    def test_create_engine_with_encoding(self, mock_trino_api):
        """Test create_engine function with encoding."""
        # Act
        trino_dbapi.create_engine(encoding=["utf-8"])

        # Assert
        call_kwargs = mock_trino_api.create_engine.call_args[1]
        assert call_kwargs["encoding"] == ["utf-8"]

    def test_create_engine_with_all_custom_parameters(self, mock_trino_api):
        """Test create_engine function with all custom parameters."""
        # Arrange
        params = {
            "source": "sqlalchemy-app",
            "catalog": "hive",
            "schema": "default",
            "session_properties": {"query_priority": "high"},
            "http_headers": {"X-Team": "data"},
            "max_attempts": 3,
            "request_timeout": 60,
            "isolation_level": IsolationLevel.REPEATABLE_READ,
            "verify": "/custom/ca.crt",
            "http_session": Mock(),
            "client_tags": ["critical"],
            "legacy_primitive_types": False,
            "legacy_prepared_statements": True,
            "roles": {"hive": "power_user"},
            "timezone": "Europe/London",
            "encoding": "utf-8",
        }

        # Act
        engine = trino_dbapi.create_engine(**params)

        # Assert
        assert engine is not None
        mock_trino_api.create_engine.assert_called_once_with(**params)

    def test_connect_creates_new_trino_api_instance(self, mocker):
        """Test that connect creates a new TrinoApi instance for each call."""
        # Arrange
        mock_trino_api_class = mocker.patch(
            "hopsworks_common.core.trino_dbapi.TrinoApi"
        )
        mock_api_instance = Mock()
        mock_api_instance.connect.return_value = Mock()
        mock_trino_api_class.return_value = mock_api_instance

        # Act
        trino_dbapi.connect()

        # Assert
        mock_trino_api_class.assert_called_once()
        mock_api_instance.connect.assert_called_once()

    def test_create_engine_creates_new_trino_api_instance(self, mocker):
        """Test that create_engine creates a new TrinoApi instance for each call."""
        # Arrange
        mock_trino_api_class = mocker.patch(
            "hopsworks_common.core.trino_dbapi.TrinoApi"
        )
        mock_api_instance = Mock()
        mock_api_instance.create_engine.return_value = Mock()
        mock_trino_api_class.return_value = mock_api_instance

        # Act
        trino_dbapi.create_engine()

        # Assert
        mock_trino_api_class.assert_called_once()
        mock_api_instance.create_engine.assert_called_once()

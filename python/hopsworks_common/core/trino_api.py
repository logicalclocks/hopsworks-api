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
"""Trino API for connecting to Trino from within Hopsworks.

This module provides convenience functions for connecting to Trino
using either the native DBAPI interface or SQLAlchemy.
Authentication and connection configuration are handled automatically.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import hopsworks
from hopsworks_apigen import public
from hopsworks_common import client, usage
from hopsworks_common.client.exceptions import TrinoException
from hopsworks_common.core import project_api, secret_api
from hopsworks_common.core.variable_api import VariableApi
from trino import constants
from trino.auth import BasicAuthentication
from trino.dbapi import Connection
from trino.dbapi import connect as _trino_connect
from trino.sqlalchemy import URL
from trino.transaction import IsolationLevel


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


_logger = logging.getLogger(__name__)

TRINO_SERVICE_NAME = "coordinator.trino.service"
TRINO_PORT = 8443


@public
@usage.method_logger
def connect(
    source: str = constants.DEFAULT_SOURCE,
    catalog: str = constants.DEFAULT_CATALOG,
    schema: str = constants.DEFAULT_SCHEMA,
    session_properties: dict | None = None,
    http_headers: dict | None = None,
    max_attempts: int = constants.DEFAULT_MAX_ATTEMPTS,
    request_timeout: int = constants.DEFAULT_REQUEST_TIMEOUT,
    isolation_level: IsolationLevel = IsolationLevel.AUTOCOMMIT,
    http_session: Any = None,
    client_tags: list[str] | None = None,
    legacy_primitive_types: bool = False,
    legacy_prepared_statements: bool | None = None,
    roles: dict | None = None,
    timezone: str | None = None,
    encoding: str | list[str] | None = None,
) -> Connection:
    """Connect to Trino using the native DBAPI interface.

    Use this when you want to work with cursors and the native Python DB API.
    For SQLAlchemy integration, use `create_engine()` instead.

    Hopsworks automatically handles authentication using project credentials from the secrets storage.
    The connection is configured to use HTTPS with self-signed certificates.

    Note: SSL Verification Disabled
        SSL certificate verification is disabled for Trino connections.
        This is required because Hopsworks uses self-signed certificates by default.

    Parameters:
        source: Source identifier for Trino queries.
        catalog: Trino catalog to connect to.
        schema: Database schema within the catalog.
        session_properties: Dictionary of Trino session properties.
        http_headers: Additional HTTP headers for the connection.
        max_attempts: Maximum number of retry attempts for failed requests.
        request_timeout: Timeout in seconds for each HTTP request.
        isolation_level: Transaction isolation level.
        http_session: Custom HTTP session for connection pooling.
        client_tags: Tags to identify the client in Trino query logs.
        legacy_primitive_types: Whether to use legacy primitive type handling.
        legacy_prepared_statements: Whether to use legacy prepared statement handling.
        roles: Dictionary mapping catalog names to role names.
        timezone: Timezone for the session.
        encoding: Character encoding for the connection.

    Returns:
        A connection object implementing the Python DB API 2.0 specification.

    Raises:
        hopsworks_common.client.exceptions.TrinoException: If the service discovery domain is not configured.
        hopsworks.client.exceptions.RestAPIError: If credentials cannot be retrieved.

    Example:
        ```python
        import hopsworks
        from hopsworks_common.core.trino_api import connect

        project = hopsworks.login()
        conn = connect(catalog="iceberg", schema="my_db")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_table")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        ```
    """
    trino_api = _TrinoApi(
        source=source,
        catalog=catalog,
        schema=schema,
        session_properties=session_properties,
        http_headers=http_headers,
        max_attempts=max_attempts,
        request_timeout=request_timeout,
        isolation_level=isolation_level,
        http_session=http_session,
        client_tags=client_tags,
        legacy_primitive_types=legacy_primitive_types,
        legacy_prepared_statements=legacy_prepared_statements,
        roles=roles,
        timezone=timezone,
        encoding=encoding,
    )
    return trino_api.trino_connect()


@public
@usage.method_logger
def create_engine(
    source: str = constants.DEFAULT_SOURCE,
    catalog: str = constants.DEFAULT_CATALOG,
    schema: str = constants.DEFAULT_SCHEMA,
    session_properties: dict | None = None,
    http_headers: dict | None = None,
    max_attempts: int = constants.DEFAULT_MAX_ATTEMPTS,
    request_timeout: int = constants.DEFAULT_REQUEST_TIMEOUT,
    isolation_level: IsolationLevel = IsolationLevel.AUTOCOMMIT,
    http_session: Any = None,
    client_tags: list[str] | None = None,
    legacy_primitive_types: bool = False,
    legacy_prepared_statements: bool | None = None,
    roles: dict | None = None,
    timezone: str | None = None,
    encoding: str | list[str] | None = None,
) -> Engine:
    """Create a SQLAlchemy engine for Trino.

    Use this when you want to work with SQLAlchemy for database operations.
    For the native Python DB API, use `connect()` instead.

    Hopsworks automatically handles authentication using project credentials from the secrets storage.
    The connection is configured to use HTTPS with self-signed certificates.

    Note: SSL Verification Disabled
        SSL certificate verification is disabled for Trino connections.
        This is required because Hopsworks uses self-signed certificates by default.

    Parameters:
        source: Source identifier for Trino queries.
        catalog: Trino catalog to connect to.
        schema: Database schema within the catalog.
        session_properties: Dictionary of Trino session properties.
        http_headers: Additional HTTP headers for the connection.
        max_attempts: Maximum number of retry attempts for failed requests.
        request_timeout: Timeout in seconds for each HTTP request.
        isolation_level: Transaction isolation level.
        http_session: Custom HTTP session for connection pooling.
        client_tags: Tags to identify the client in Trino query logs.
        legacy_primitive_types: Whether to use legacy primitive type handling.
        legacy_prepared_statements: Whether to use legacy prepared statement handling.
        roles: Dictionary mapping catalog names to role names.
        timezone: Timezone for the session.
        encoding: Character encoding for the connection.

    Returns:
        An Engine object implementing the SQLAlchemy interface.

    Raises:
        hopsworks_common.client.exceptions.TrinoException: If the service discovery domain is not configured.
        hopsworks.client.exceptions.RestAPIError: If credentials cannot be retrieved.

    Example:
        ```python
        import hopsworks
        from hopsworks_common.core.trino_api import create_engine
        from sqlalchemy.sql.expression import text

        project = hopsworks.login()
        engine = create_engine(catalog="iceberg", schema="my_db")
        with engine.connect() as connection:
            cursor = connection.execute(text("SELECT * FROM tiny.nation")).cursor
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        ```
    """
    trino_api = _TrinoApi(
        source=source,
        catalog=catalog,
        schema=schema,
        session_properties=session_properties,
        http_headers=http_headers,
        max_attempts=max_attempts,
        request_timeout=request_timeout,
        isolation_level=isolation_level,
        http_session=http_session,
        client_tags=client_tags,
        legacy_primitive_types=legacy_primitive_types,
        legacy_prepared_statements=legacy_prepared_statements,
        roles=roles,
        timezone=timezone,
        encoding=encoding,
    )
    return trino_api.create_engine()


class _TrinoApi:
    """Internal API for connecting to Trino from within Hopsworks.

    This class provides methods to establish connections to Trino using either
    the native Trino DBAPI or SQLAlchemy engine.
    Authentication is handled automatically using Hopsworks project credentials
    stored in the secrets storage.

    The connection configuration adapts based on whether you're connecting from
    within the Hopsworks cluster or externally through the load balancer.

    Note: Private API
        This class is internal and should not be used directly.
        Use the `connect()` or `create_engine()` functions instead.

    Note: SSL Verification Disabled
        SSL certificate verification is disabled for Trino connections.
        This is required because Hopsworks uses self-signed certificates by default.
    """

    def __init__(
        self,
        source=constants.DEFAULT_SOURCE,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        session_properties=None,
        http_headers=None,
        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level=IsolationLevel.AUTOCOMMIT,
        http_session=None,
        client_tags=None,
        legacy_primitive_types=False,
        legacy_prepared_statements=None,
        roles=None,
        timezone=None,
        encoding: str | list[str] | None = None,
    ):
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.session_properties = session_properties
        self.http_headers = http_headers
        self.max_attempts = max_attempts
        self.request_timeout = request_timeout
        self.isolation_level = isolation_level
        self.http_session = http_session
        self.client_tags = client_tags
        self.legacy_primitive_types = legacy_primitive_types
        self.legacy_prepared_statements = legacy_prepared_statements
        self.roles = roles
        self.timezone = timezone
        self.encoding = encoding

        self._variable_api: VariableApi = VariableApi()
        self._service_discovery_domain = (
            self._variable_api.get_service_discovery_domain()
        )
        self._secret_api: secret_api.SecretsApi = secret_api.SecretsApi()
        self._project_api: project_api.ProjectApi = project_api.ProjectApi()
        self.project: hopsworks.Project = hopsworks.get_current_project()

    def _retrieve_host(self) -> str:
        """Retrieve the Trino host based on client location.

        Returns the external load balancer domain if connecting from outside
        the cluster, otherwise returns the internal service discovery hostname.

        Returns:
            The Trino host URL.

        Raises:
            hopsworks_common.client.exceptions.TrinoException:
                If service discovery domain is not configured for internal clients.
        """
        _logger.debug("Retrieving Trino host.")
        if client._is_external():
            external_domain = self._variable_api.get_loadbalancer_external_domain(
                "trino"
            )
            host = f"{external_domain}"
        else:
            if self._service_discovery_domain == "":
                raise TrinoException(
                    "Client could not get Trino hostname from service_discovery_domain. "
                    "The variable is either not set or empty in Hopsworks cluster configuration."
                )
            host = f"{TRINO_SERVICE_NAME}.{self._service_discovery_domain}"
        _logger.debug(f"Connecting to Trino on host {host} and port {TRINO_PORT}.")
        return host

    def _get_password(self, user: str) -> str:
        """Retrieve the password for the given user from secrets storage.

        Parameters:
            user: The username for which to retrieve the password.

        Returns:
            The password for the given user.

        Raises:
            hopsworks_common.client.exceptions.TrinoException:
                If the password cannot be retrieved.
        """
        secret = self._secret_api.get_secret(user)
        if secret is None:
            raise TrinoException(
                f"Client could not retrieve credentials for user {user} from secrets storage. "
                "Ensure that the secret exists and is accessible."
            )
        return secret.value

    def trino_connect(self) -> Connection:
        """Connect to Trino using the native DBAPI interface.

        Returns:
            A connection object implementing the Python DB API 2.0 specification.

        Raises:
            hopsworks_common.client.exceptions.TrinoException:
                If service discovery domain is not configured.
            hopsworks.client.exceptions.RestAPIError:
                If credentials cannot be retrieved.
        """
        host = self._retrieve_host()
        port = TRINO_PORT

        username = self._project_api.get_user_info().get("username", None)
        user = f"{self.project.name}__{username}"
        password = self._get_password(user)

        return _trino_connect(
            host=host,
            port=port,
            user=user,
            catalog=self.catalog,
            schema=self.schema,
            source=self.source,
            auth=BasicAuthentication(user, password),
            http_scheme=constants.HTTPS,
            verify=False,
            session_properties=self.session_properties,
            http_headers=self.http_headers,
            max_attempts=self.max_attempts,
            request_timeout=self.request_timeout,
            isolation_level=self.isolation_level,
            http_session=self.http_session,
            client_tags=self.client_tags,
            legacy_primitive_types=self.legacy_primitive_types,
            legacy_prepared_statements=self.legacy_prepared_statements,
            roles=self.roles,
            timezone=self.timezone,
            encoding=self.encoding,
        )

    def create_engine(self) -> Engine:
        """Create a SQLAlchemy engine for Trino.

        Returns:
            A SQLAlchemy engine for executing queries against Trino.

        Raises:
            hopsworks_common.client.exceptions.TrinoException:
                If service discovery domain is not configured.
            hopsworks.client.exceptions.RestAPIError:
                If credentials cannot be retrieved.
        """
        from sqlalchemy import create_engine

        host = self._retrieve_host()
        port = TRINO_PORT

        username = self._project_api.get_user_info().get("username", None)
        user = f"{self.project.name}__{username}"
        password = self._get_password(user)

        connection_url = URL(
            host=host,
            port=port,
            user=user,
            catalog=self.catalog,
            schema=self.schema,
            source=self.source,
            session_properties=self.session_properties,
            http_headers=self.http_headers,
            client_tags=self.client_tags,
            legacy_primitive_types=self.legacy_primitive_types,
            legacy_prepared_statements=self.legacy_prepared_statements,
            roles=self.roles,
        )
        connect_args = {
            "auth": BasicAuthentication(user, password),
            "http_scheme": constants.HTTPS,
            "verify": False,
            "timezone": self.timezone,
            "encoding": self.encoding,
        }
        return create_engine(connection_url, connect_args=connect_args)

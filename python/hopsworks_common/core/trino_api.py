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

from hopsworks_apigen import public
from hopsworks_common import client, usage
from hopsworks_common.client.exceptions import TrinoException
from hopsworks_common.core import project_api, secret_api
from hopsworks_common.core.constants import HAS_TRINO
from hopsworks_common.core.variable_api import VariableApi
from hopsworks_common.decorators import uses_trino


if HAS_TRINO:
    from trino.auth import BasicAuthentication
    from trino.constants import (
        DEFAULT_CATALOG,
        DEFAULT_MAX_ATTEMPTS,
        DEFAULT_REQUEST_TIMEOUT,
        DEFAULT_SCHEMA,
    )
    from trino.dbapi import connect as _trino_connect
    from trino.sqlalchemy import URL
    from trino.transaction import IsolationLevel

    AUTOCOMMIT = IsolationLevel.AUTOCOMMIT
else:
    DEFAULT_CATALOG = None
    DEFAULT_SCHEMA = None
    DEFAULT_MAX_ATTEMPTS = None
    DEFAULT_REQUEST_TIMEOUT = None
    AUTOCOMMIT = None


if TYPE_CHECKING:
    from hopsworks_common import project
    from sqlalchemy.engine import Engine
    from trino.dbapi import Connection
    from trino.transaction import IsolationLevel  # noqa: TC004


_logger = logging.getLogger(__name__)

TRINO_SERVICE_NAME = "coordinator.trino.service"
TRINO_PORT = 8443
DEFAULT_SOURCE = "hopsworks-trino-python-client"
DEFAULT_SQLALCHEMY_SOURCE = "hopsworks-trino-sqlalchemy"
HTTPS = "https"


@public("hopsworks.core.trino_api")
class TrinoApi:
    """API for connecting to Trino from within Hopsworks.

    This class provides methods to establish connections to Trino using either
    the native Trino DBAPI or SQLAlchemy engine.
    Authentication is handled automatically using Hopsworks project credentials
    stored in the secrets storage.

    The connection configuration adapts based on whether you're connecting from
    within the Hopsworks cluster or externally through the load balancer.

    Example:
        ```python
        import hopsworks
        from hopsworks.core.trino_api import TrinoApi
        from sqlalchemy import text

        project = hopsworks.login()
        trino_api = project.get_trino_api()  # Get an instance of TrinoApi from the project context
        conn = trino_api.connect(catalog="iceberg", schema="my_db")  # Get a DBAPI connection
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_table")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

        # Or using SQLAlchemy
        engine = trino_api.create_engine(catalog="iceberg", schema="my_db")
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM my_table"))
            for row in result:
                print(row)
        ```
    """

    def __init__(self, project: project.Project | None = None):
        """Initialize the TrinoApi instance.

        Parameters:
            project: The Hopsworks project context. If None, the default project is used.

        """
        self._variable_api: VariableApi = VariableApi()
        self._service_discovery_domain = (
            self._variable_api.get_service_discovery_domain()
        )
        self._secret_api: secret_api.SecretsApi = secret_api.SecretsApi()
        self._project_api: project_api.ProjectApi = project_api.ProjectApi()
        if project is None:
            _client = client.get_instance()
            self.project_name = _client._project_name
            self.project_id = _client._project_id
        else:
            self.project_name = project.name
            self.project_id = project.id

    def _get_ca_chain_path(self, verify: bool | str) -> bool | str:
        """Get the SSL certificate or pass through a custom verify value.

        Parameters:
            verify: Whether to verify the SSL certificate, or a path to a custom CA bundle.

        Returns:
            The file path of the downloaded SSL certificate if it was downloaded.
            The original verify value if verification is handled by the caller or disabled.
        """
        if not client._is_external() and verify is True:
            _client = client.get_instance()
            return _client._get_ca_chain_path()
        return verify

    @public
    @usage.method_logger
    def get_host(self) -> str:
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
            host = external_domain
        else:
            if self._service_discovery_domain == "":
                raise TrinoException(
                    "Client could not get Trino hostname from service_discovery_domain. "
                    "The variable is either not set or empty in Hopsworks cluster configuration."
                )
            host = f"{TRINO_SERVICE_NAME}.{self._service_discovery_domain}"
        _logger.debug(f"Connecting to Trino on host {host} and port {TRINO_PORT}.")
        return host

    @public
    @usage.method_logger
    def get_port(self) -> int:
        """Get the Trino port number.

        Returns:
            The port number for connecting to Trino.
        """
        return TRINO_PORT

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

    @public
    @usage.method_logger
    def get_basic_auth(self) -> tuple[str, str]:
        """Get a tuple containing the username and password for the current project user.

        Returns:
            A tuple containing the username and the password for the current project user.

        Raises:
            hopsworks_common.client.exceptions.TrinoException:
                If credentials cannot be retrieved from secrets storage or if the client cannot determine the username for the current project user.
        """
        username = self._project_api.get_user_info().get("username", None)
        if username is None:
            raise TrinoException(
                "Client could not determine username for the current project user."
            )
        user = f"{self.project_name}__{username}"
        password = self._get_password(user)
        return user, password

    @uses_trino
    @public
    @usage.method_logger
    def connect(
        self,
        source: str = DEFAULT_SOURCE,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        session_properties: dict | None = None,
        http_headers: dict | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        isolation_level: IsolationLevel = AUTOCOMMIT,
        verify: bool | str = True,
        http_session: Any = None,
        client_tags: list[str] | None = None,
        legacy_primitive_types: bool = False,
        legacy_prepared_statements: bool | None = None,
        roles: dict | None = None,
        timezone: str | None = None,
        encoding: str | list[str] | None = None,
    ) -> Connection:
        """Connect to Trino using the native DBAPI interface.

        Parameters:
            source: Source identifier for Trino queries.
            catalog: Trino catalog to connect to.
            schema: Database schema within the catalog.
            session_properties: Dictionary of Trino session properties.
            http_headers: Additional HTTP headers for the connection.
            max_attempts: Maximum number of retry attempts for failed requests.
            request_timeout: Timeout in seconds for each HTTP request.
            isolation_level: Transaction isolation level.
            verify: Whether to verify SSL certificates.
                    Set verify="/path/to/cert.crt" if you want to verify the ssl cert (default: True).
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
            hopsworks_common.client.exceptions.TrinoException:
                If service discovery domain is not configured.
            hopsworks.client.exceptions.RestAPIError:
                If credentials cannot be retrieved.

        Example:
            ```python
            import hopsworks
            from hopsworks.core.trino_api import TrinoApi

            project = hopsworks.login()
            trino_api = TrinoApi()
            conn = trino_api.connect(catalog="iceberg", schema="my_db")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM my_table")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
            ```
        """
        host = self.get_host()
        port = self.get_port()

        user, password = self.get_basic_auth()
        basic_auth = BasicAuthentication(user, password)

        return _trino_connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
            source=source,
            auth=basic_auth,
            http_scheme=HTTPS,
            verify=self._get_ca_chain_path(verify),
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

    @uses_trino
    @public
    @usage.method_logger
    def create_engine(
        self,
        source: str = DEFAULT_SQLALCHEMY_SOURCE,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        session_properties: dict | None = None,
        http_headers: dict | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        isolation_level: IsolationLevel = AUTOCOMMIT,
        verify: bool | str = True,
        http_session: Any = None,
        client_tags: list[str] | None = None,
        legacy_primitive_types: bool = False,
        legacy_prepared_statements: bool | None = None,
        roles: dict | None = None,
        timezone: str | None = None,
        encoding: str | list[str] | None = None,
    ) -> Engine:
        """Create a SQLAlchemy engine for Trino.

        Parameters:
            source: Source identifier for Trino queries.
            catalog: Trino catalog to connect to.
            schema: Database schema within the catalog.
            session_properties: Dictionary of Trino session properties.
            http_headers: Additional HTTP headers for the connection.
            max_attempts: Maximum number of retry attempts for failed requests.
            request_timeout: Timeout in seconds for each HTTP request.
            isolation_level: Transaction isolation level.
            verify: Whether to verify SSL certificates.
                    Set verify="/path/to/cert.crt" if you want to verify the ssl cert (default: True).
            http_session: Custom HTTP session for connection pooling.
            client_tags: Tags to identify the client in Trino query logs.
            legacy_primitive_types: Whether to use legacy primitive type handling.
            legacy_prepared_statements: Whether to use legacy prepared statement handling.
            roles: Dictionary mapping catalog names to role names.
            timezone: Timezone for the session.
            encoding: Character encoding for the connection.

        Returns:
            A SQLAlchemy engine for executing queries against Trino.

        Raises:
            hopsworks_common.client.exceptions.TrinoException:
                If service discovery domain is not configured.
            hopsworks.client.exceptions.RestAPIError:
                If credentials cannot be retrieved.

        Example:
            ```python
            import hopsworks
            from hopsworks.core.trino_api import TrinoApi
            from sqlalchemy import text

            project = hopsworks.login()
            trino_api = TrinoApi()
            engine = trino_api.create_engine(catalog="iceberg", schema="my_db")
            with engine.connect() as connection:
                result = connection.execute(text("SELECT * FROM my_table"))
                for row in result:
                    print(row)
            ```
        """
        from sqlalchemy import create_engine

        host = self.get_host()
        port = self.get_port()

        user, password = self.get_basic_auth()
        basic_auth = BasicAuthentication(user, password)

        connection_url = URL(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
        )
        connect_args = {
            "auth": basic_auth,
            "http_scheme": HTTPS,
            "verify": self._get_ca_chain_path(verify),
            "source": source,
            "session_properties": session_properties,
            "http_headers": http_headers,
            "client_tags": client_tags,
            "legacy_primitive_types": legacy_primitive_types,
            "legacy_prepared_statements": legacy_prepared_statements,
            "roles": roles,
            "timezone": timezone,
            "encoding": encoding,
            "max_attempts": max_attempts,
            "request_timeout": request_timeout,
            "isolation_level": isolation_level,
            "http_session": http_session,
        }
        return create_engine(connection_url, connect_args=connect_args)

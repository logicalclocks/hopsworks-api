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
from typing import TYPE_CHECKING

import hopsworks
from hopsworks_apigen import public
from hopsworks_common import client, project, usage
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
DEFAULT_SOURCE = "hopsworks-trino-python-client"
DEFAULT_SQLALCHEMY_SOURCE = "hopsworks-trino-sqlalchemy"


@public("hopsworks.core.trino_api")
class TrinoApi:
    """API for connecting to Trino from within Hopsworks.

    This class provides methods to establish connections to Trino using either
    the native Trino DBAPI or SQLAlchemy engine.
    Authentication is handled automatically using Hopsworks project credentials
    stored in the secrets storage.

    The connection configuration adapts based on whether you're connecting from
    within the Hopsworks cluster or externally through the load balancer.

    Example usage:
        ```python
        import hopsworks
        from hopsworks.core.trino_api import TrinoApi

        project = hopsworks.login()
        trino_api = TrinoApi(project=project, catalog="my_catalog", schema="my_schema")
        conn = trino_api.connect()  # Get a DBAPI connection
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_table")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

        # Or using SQLAlchemy
        engine = trino_api.create_engine()
        with engine.connect() as connection:
            result = connection.execute("SELECT * FROM my_table")
            for row in result:
                print(row)
        ```
    """

    def __init__(
        self,
        project: project.Project | None = None,
        source=None,
        catalog=constants.DEFAULT_CATALOG,
        schema=constants.DEFAULT_SCHEMA,
        session_properties=None,
        http_headers=None,
        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
        isolation_level=IsolationLevel.AUTOCOMMIT,
        verify: bool | str = False,
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
        self.verify = verify
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
        self.project: project.Project = (
            project if project is not None else hopsworks.get_current_project()
        )

        _client = client.get_instance()
        _client.download_certs()

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

    @usage.method_logger
    def get_basic_auth(self) -> BasicAuthentication:
        """Get a BasicAuthentication object for the current project user.

        Returns:
            A BasicAuthentication object with the current project user's credentials.

        Raises:
            hopsworks_common.client.exceptions.TrinoException:
                If credentials cannot be retrieved from secrets storage.
        """
        username = self._project_api.get_user_info().get("username", None)
        user = f"{self.project.name}__{username}"
        password = self._get_password(user)
        return BasicAuthentication(user, password)

    @usage.method_logger
    def connect(self) -> Connection:
        """Connect to Trino using the native DBAPI interface.

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
            conn = trino_api.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM my_table")
            rows = cursor.fetchall()
            for row in rows:
                print(row)
            ```
        """
        host = self.get_host()
        port = self.get_port()

        basic_auth = self.get_basic_auth()

        source = DEFAULT_SOURCE if self.source is None else self.source

        return _trino_connect(
            host=host,
            port=port,
            user=basic_auth._username,
            catalog=self.catalog,
            schema=self.schema,
            source=source,
            auth=basic_auth,
            http_scheme=constants.HTTPS,
            verify=self.verify,
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

    @usage.method_logger
    def create_engine(self) -> Engine:
        """Create a SQLAlchemy engine for Trino.

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

            project = hopsworks.login()
            trino_api = TrinoApi()
            engine = trino_api.create_engine()
            with engine.connect() as connection:
                result = connection.execute("SELECT * FROM my_table")
                for row in result:
                    print(row)
            ```
        """
        from sqlalchemy import create_engine

        host = self.get_host()
        port = self.get_port()

        basic_auth = self.get_basic_auth()

        source = DEFAULT_SQLALCHEMY_SOURCE if self.source is None else self.source

        connection_url = URL(
            host=host,
            port=port,
            user=basic_auth._username,
            catalog=self.catalog,
            schema=self.schema,
        )
        connect_args = {
            "auth": basic_auth,
            "http_scheme": constants.HTTPS,
            "verify": self.verify,
            "source": source,
            "session_properties": self.session_properties,
            "http_headers": self.http_headers,
            "client_tags": self.client_tags,
            "legacy_primitive_types": self.legacy_primitive_types,
            "legacy_prepared_statements": self.legacy_prepared_statements,
            "roles": self.roles,
            "timezone": self.timezone,
            "encoding": self.encoding,
            "max_attempts": self.max_attempts,
            "request_timeout": self.request_timeout,
            "isolation_level": self.isolation_level,
            "http_session": self.http_session,
        }
        return create_engine(connection_url, connect_args=connect_args)

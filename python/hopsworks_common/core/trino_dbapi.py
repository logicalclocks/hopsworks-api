from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common import usage
from hopsworks_common.core.constants import (
    HAS_TRINO,
    trino_not_installed_message,
)
from hopsworks_common.core.trino_api import (
    DEFAULT_SOURCE,
    DEFAULT_SQLALCHEMY_SOURCE,
    TrinoApi,
)


if HAS_TRINO:
    from trino import constants
    from trino.transaction import IsolationLevel
else:
    raise ImportError(trino_not_installed_message)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from trino.dbapi import Connection


@public("hopsworks.core.trino_dbapi.connect")
@usage.method_logger
def connect(
    source: str = DEFAULT_SOURCE,
    catalog: str = constants.DEFAULT_CATALOG,
    schema: str = constants.DEFAULT_SCHEMA,
    session_properties: dict | None = None,
    http_headers: dict | None = None,
    max_attempts: int = constants.DEFAULT_MAX_ATTEMPTS,
    request_timeout: int = constants.DEFAULT_REQUEST_TIMEOUT,
    isolation_level: IsolationLevel = IsolationLevel.AUTOCOMMIT,
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

    Use this when you want to work with cursors and the native Python DB API.
    For SQLAlchemy integration, use `create_engine()` instead.

    Hopsworks automatically handles authentication using project credentials from the secrets storage.
    The connection is configured to use HTTPS with self-signed certificates.

    Parameters:
        source: Source identifier for Trino queries.
        catalog: Trino catalog to connect to.
        schema: Database schema within the catalog.
        session_properties: Dictionary of Trino session properties.
        http_headers: Additional HTTP headers for the connection.
        max_attempts: Maximum number of retry attempts for failed requests.
        request_timeout: Timeout in seconds for each HTTP request.
        isolation_level: Transaction isolation level.
        verify: Whether to verify SSL certificates. Set verify="/path/to/cert.crt" if you want to verify the ssl cert (default: True).
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
        from hopsworks.core.trino_dbapi import connect

        project = hopsworks.login()
        conn = connect(catalog="iceberg", schema="my_db")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_table")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        ```
    """
    trino_api = TrinoApi()
    return trino_api.connect(
        source=source,
        catalog=catalog,
        schema=schema,
        session_properties=session_properties,
        http_headers=http_headers,
        max_attempts=max_attempts,
        request_timeout=request_timeout,
        isolation_level=isolation_level,
        verify=verify,
        http_session=http_session,
        client_tags=client_tags,
        legacy_primitive_types=legacy_primitive_types,
        legacy_prepared_statements=legacy_prepared_statements,
        roles=roles,
        timezone=timezone,
        encoding=encoding,
    )


@public("hopsworks.core.trino_dbapi.create_engine")
@usage.method_logger
def create_engine(
    source: str = DEFAULT_SQLALCHEMY_SOURCE,
    catalog: str = constants.DEFAULT_CATALOG,
    schema: str = constants.DEFAULT_SCHEMA,
    session_properties: dict | None = None,
    http_headers: dict | None = None,
    max_attempts: int = constants.DEFAULT_MAX_ATTEMPTS,
    request_timeout: int = constants.DEFAULT_REQUEST_TIMEOUT,
    isolation_level: IsolationLevel = IsolationLevel.AUTOCOMMIT,
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

    Use this when you want to work with SQLAlchemy for database operations.
    For the native Python DB API, use `connect()` instead.

    Hopsworks automatically handles authentication using project credentials from the secrets storage.
    The connection is configured to use HTTPS with self-signed certificates.

    Parameters:
        source: Source identifier for Trino queries.
        catalog: Trino catalog to connect to.
        schema: Database schema within the catalog.
        session_properties: Dictionary of Trino session properties.
        http_headers: Additional HTTP headers for the connection.
        max_attempts: Maximum number of retry attempts for failed requests.
        request_timeout: Timeout in seconds for each HTTP request.
        isolation_level: Transaction isolation level.
        verify: Whether to verify SSL certificates. Set verify="/path/to/cert.crt" if you want to verify the ssl cert (default: True).
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
        from hopsworks.core.trino_dbapi import create_engine
        from sqlalchemy import text

        project = hopsworks.login()
        engine = create_engine(catalog="iceberg", schema="my_db")
        with engine.connect() as connection:
            cursor = connection.execute(text("SELECT * FROM tiny.nation")).cursor
            rows = cursor.fetchall()
            for row in rows:
                print(row)
        ```
    """
    trino_api = TrinoApi()
    return trino_api.create_engine(
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
        verify=verify,
        roles=roles,
        timezone=timezone,
        encoding=encoding,
    )

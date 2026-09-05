"""``hops datasource`` — storage connector read + write commands.

The SDK does not expose a ``create_storage_connector``; we POST straight to
``/featurestores/{id}/storageconnectors`` via the authenticated REST client.
The connector DTO is deserialized polymorphically (Jackson ``@JsonTypeInfo``
keyed on a ``type`` property), so every create body must carry the subtype
discriminator (e.g. ``"type": "featurestoreJdbcConnectorDTO"``). Omitting it
deserializes into the base DTO and the backend 500s with a class-cast error.
``databases``/``tables``/``preview`` delegate to the SDK's ``DataSource``
methods where available.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import click
from hopsworks.cli import output, session


@click.group("datasource")
def datasource_group() -> None:
    """Data source (storage connector) commands."""


@datasource_group.command("list")
@click.pass_context
def connector_list(ctx: click.Context) -> None:
    """List all storage connectors in the active project.

    Args:
        ctx: Click context.
    """
    fs = session.get_feature_store(ctx)
    items = _list_connectors(fs)
    rows = []
    for c in items:
        rows.append(
            [
                c.get("id", "?"),
                c.get("name", "?"),
                c.get("storageConnectorType", c.get("connectorType", "?")),
                output.first_line(c.get("description"), empty=""),
            ]
        )
    output.print_table(["ID", "NAME", "TYPE", "DESCRIPTION"], rows)


@datasource_group.command("info")
@click.argument("name")
@click.pass_context
def connector_info(ctx: click.Context, name: str) -> None:
    """Show details for a single storage connector.

    Args:
        ctx: Click context.
        name: Connector name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Connector '{name}' not found: {exc}") from exc

    sc = getattr(ds, "storage_connector", ds)
    if output.JSON_MODE:
        to_dict = getattr(sc, "to_dict", None)
        payload = to_dict() if callable(to_dict) else _connector_to_dict(sc)
        output.print_json(payload)
        return

    rows = [
        ["ID", getattr(sc, "id", "?")],
        ["Name", getattr(sc, "name", "?")],
        ["Type", type(sc).__name__],
        ["Description", output.first_line(getattr(sc, "description", ""))],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


def _list_connectors(fs: Any) -> list[dict[str, Any]]:
    from hopsworks_common.core import rest

    fs_id = getattr(fs, "id", None)
    if fs_id is None:
        return []
    try:
        payload = rest._send_request(
            "GET", rest._project_path("featurestores", fs_id, "storageconnectors")
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list connectors: {exc}") from exc
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("items") or []
    return []


def _connector_to_dict(sc: Any) -> dict[str, Any]:
    return {
        "id": getattr(sc, "id", None),
        "name": getattr(sc, "name", None),
        "type": type(sc).__name__,
        "description": getattr(sc, "description", None),
    }


# region Write commands


@datasource_group.group("create")
def connector_create() -> None:
    """Create a storage connector (subcommand per backend)."""


@connector_create.command("jdbc", help="Register a JDBC connector.")
@click.argument("name")
@click.option("--url", required=True, help="JDBC connection URL.")
@click.option("--user", help='Connection user, stored as "user".')
@click.option("--password", help='Connection password, stored as "password".')
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_jdbc(
    ctx: click.Context,
    name: str,
    url: str,
    user: str | None,
    password: str | None,
    description: str,
) -> None:
    """Register a JDBC connector.

    Args:
        ctx: Click context.
        name: Connector name.
        url: JDBC connection URL.
        user: Optional user.
        password: Optional password.
        description: Description.
    """
    args = []
    if user:
        args.append({"name": "user", "value": user})
    if password:
        args.append({"name": "password", "value": password})
    body = {
        "type": "featurestoreJdbcConnectorDTO",
        "name": name,
        "storageConnectorType": "JDBC",
        "connectionString": url,
        "description": description,
        "arguments": args,
    }
    _create_connector(ctx, body)


@connector_create.command("s3", help="Register an S3 connector.")
@click.argument("name")
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--access-key", help="AWS access key ID.")
@click.option("--secret-key", help="AWS secret access key.")
@click.option("--region", help="AWS region.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_s3(
    ctx: click.Context,
    name: str,
    bucket: str,
    access_key: str | None,
    secret_key: str | None,
    region: str | None,
    description: str,
) -> None:
    """Register an S3 connector.

    Args:
        ctx: Click context.
        name: Connector name.
        bucket: S3 bucket.
        access_key: AWS access key.
        secret_key: AWS secret key.
        region: AWS region.
        description: Description.
    """
    body = {
        "type": "featurestoreS3ConnectorDTO",
        "name": name,
        "storageConnectorType": "S3",
        "bucket": bucket,
        "description": description,
    }
    if access_key:
        body["accessKey"] = access_key
    if secret_key:
        body["secretKey"] = secret_key
    if region:
        body["region"] = region
    _create_connector(ctx, body)


@connector_create.command("snowflake", help="Register a Snowflake connector.")
@click.argument("name")
@click.option("--url", required=True, help="Snowflake account URL.")
@click.option("--user", required=True, help="User name.")
@click.option("--password", required=True, help="Password.")
@click.option("--database", required=True, help="Database name.")
@click.option("--schema", "db_schema", required=True, help="Schema name.")
@click.option("--warehouse", required=True, help="Warehouse name.")
@click.option("--role", help="Role to assume.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_snowflake(
    ctx: click.Context,
    name: str,
    url: str,
    user: str,
    password: str,
    database: str,
    db_schema: str,
    warehouse: str,
    role: str | None,
    description: str,
) -> None:
    """Register a Snowflake connector.

    Args:
        ctx: Click context.
        name: Connector name.
        url: Snowflake account URL.
        user: User.
        password: Password.
        database: Database.
        db_schema: Schema.
        warehouse: Warehouse.
        role: Optional role.
        description: Description.
    """
    body = {
        "type": "featurestoreSnowflakeConnectorDTO",
        "name": name,
        "storageConnectorType": "SNOWFLAKE",
        "url": url,
        "user": user,
        "password": password,
        "database": database,
        "schema": db_schema,
        "warehouse": warehouse,
        "description": description,
    }
    if role:
        body["role"] = role
    _create_connector(ctx, body)


@connector_create.command("bigquery", help="Register a BigQuery connector.")
@click.argument("name")
@click.option("--project-id", "project_id", required=True, help="GCP project ID.")
@click.option("--dataset", help="BigQuery dataset.")
@click.option("--key-path", "key_path", help="Path to the service-account JSON key.")
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_bigquery(
    ctx: click.Context,
    name: str,
    project_id: str,
    dataset: str | None,
    key_path: str | None,
    description: str,
) -> None:
    """Register a BigQuery connector.

    Args:
        ctx: Click context.
        name: Connector name.
        project_id: GCP project ID.
        dataset: Optional BigQuery dataset.
        key_path: Optional service-account key path.
        description: Description.
    """
    body = {
        "type": "featurestoreBigqueryConnectorDTO",
        "name": name,
        "storageConnectorType": "BIGQUERY",
        "queryProject": project_id,
        "description": description,
    }
    if dataset:
        body["dataset"] = dataset
    if key_path:
        body["keyPath"] = key_path
    _create_connector(ctx, body)


@connector_create.command("mongodb", help="Register a MongoDB connector.")
@click.argument("name")
@click.option(
    "--connection-string",
    "connection_string",
    required=True,
    help="MongoDB URI (mongodb:// or mongodb+srv://) without embedded credentials.",
)
@click.option("--database", required=True, help="Database name.")
@click.option("--collection", help="Default collection name.")
@click.option("--user", help="Database user.")
@click.option(
    "--password", help="Database password (stored in the Hopsworks secret store)."
)
@click.option("--auth-source", "auth_source", help="MongoDB authSource (e.g. admin).")
@click.option(
    "--auth-mechanism",
    "auth_mechanism",
    help="MongoDB authMechanism (e.g. SCRAM-SHA-256).",
)
@click.option("--description", default="", help="Free-form description.")
@click.pass_context
def connector_create_mongodb(
    ctx: click.Context,
    name: str,
    connection_string: str,
    database: str,
    collection: str | None,
    user: str | None,
    password: str | None,
    auth_source: str | None,
    auth_mechanism: str | None,
    description: str,
) -> None:
    """Register a MongoDB connector.

    The ``connection_string`` is a MongoDB URI without embedded credentials
    (``mongodb://`` or ``mongodb+srv://``); ``user`` and ``password`` are
    persisted into the Hopsworks secret store and spliced into the URI
    server-side at read time.

    Args:
        ctx: Click context.
        name: Connector name.
        connection_string: MongoDB URI (`mongodb://host[:port]` or
            `mongodb+srv://cluster.mongodb.net`) with no embedded
            ``user:password@`` userinfo.
        database: Default database the connector points at. The per-FG
            ``DataSource.database`` overrides this at read time.
        collection: Default collection (optional). Overridden per-FG by
            ``DataSource.table``.
        user: Database user. Persisted as a Hopsworks secret alongside
            ``password`` and spliced into the URI at read time.
        password: Database password. Persisted as a Hopsworks secret —
            never logged or returned in connector responses.
        auth_source: ``authSource`` URI parameter (typically ``admin``
            for Atlas users created outside the target database).
        auth_mechanism: ``authMechanism`` URI parameter (e.g.
            ``SCRAM-SHA-256``, ``MONGODB-X509``). Leave unset to let the
            server negotiate the default.
        description: Free-form description shown in the data-source list.
    """
    # No MongoDB storage-connector subtype was confirmed against the backend
    # (the FeaturestoreConnectorType enum has no MONGODB, and the live cluster
    # rejects it), so the ``type`` discriminator the other connectors carry is
    # left out here until a Mongo-capable backend confirms the right value.
    body = {
        "name": name,
        "storageConnectorType": "MONGODB",
        "connectionString": connection_string,
        "database": database,
        "description": description,
    }
    if collection:
        body["collection"] = collection
    if user:
        body["user"] = user
    if password:
        body["password"] = password
    if auth_source:
        body["authSource"] = auth_source
    if auth_mechanism:
        body["authMechanism"] = auth_mechanism
    _create_connector(ctx, body)


# region Connector specs
#
# The five commands above are written out by hand; the thirteen below differ only in
# their fields, so they are declared and built rather than copied. Each entry names the
# DTO Jackson expects on ``type``, the ``storageConnectorType`` enum value, and the
# options, whose ``json`` is the DTO property (dotted for the payloads that nest).


@dataclass(frozen=True)
class _Opt:
    """One CLI option and the connector property it fills."""

    flag: str
    json: str
    help: str
    required: bool = False
    kind: str = "str"
    choices: tuple[str, ...] = ()
    default: str | None = None


@dataclass(frozen=True)
class _Spec:
    """A connector type: what to send, and what to ask for."""

    dto: str
    connector_type: str
    summary: str
    opts: tuple[_Opt, ...]
    epilog: str = ""


_ARGS = _Opt(
    "--argument",
    "arguments",
    'Extra connector argument as "key=value". Repeat for several.',
    kind="args",
)

_SPECS: dict[str, _Spec] = {
    "hopsfs": _Spec(
        "featurestoreHopsfsConnectorDTO",
        "HOPSFS",
        "Register a HopsFS connector over a project dataset.",
        (
            _Opt(
                "--dataset",
                "datasetName",
                "Dataset the connector reads and writes.",
                required=True,
            ),
        ),
    ),
    "redshift": _Spec(
        "featurestoreRedshiftConnectorDTO",
        "REDSHIFT",
        "Register an Amazon Redshift connector.",
        (
            _Opt(
                "--cluster-identifier",
                "clusterIdentifier",
                "Redshift cluster identifier.",
                required=True,
            ),
            _Opt("--database", "databaseName", "Database name."),
            _Opt(
                "--endpoint", "databaseEndpoint", "Cluster endpoint, without the port."
            ),
            _Opt("--port", "databasePort", "Database port.", kind="int"),
            _Opt("--user", "databaseUserName", "Database user."),
            _Opt(
                "--password",
                "databasePassword",
                "Database password. Omit when using --iam-role.",
            ),
            _Opt("--iam-role", "iamRole", "IAM role to assume instead of a password."),
            _Opt("--group", "databaseGroup", "Database group."),
            _Opt("--driver", "databaseDriver", "JDBC driver class."),
            _Opt("--table", "tableName", "Default table."),
            _Opt(
                "--auto-create",
                "autoCreate",
                "Let Redshift create the user on connect.",
                kind="flag",
            ),
            _ARGS,
        ),
    ),
    "adls": _Spec(
        "featurestoreADLSConnectorDTO",
        "ADLS",
        "Register an Azure Data Lake Storage connector.",
        (
            _Opt(
                "--account-name", "accountName", "Storage account name.", required=True
            ),
            _Opt("--container-name", "containerName", "Container name.", required=True),
            _Opt("--generation", "generation", "ADLS generation, 1 or 2.", kind="int"),
            _Opt("--directory-id", "directoryId", "Azure AD directory (tenant) id."),
            _Opt("--application-id", "applicationId", "Application (client) id."),
            _Opt(
                "--service-credential",
                "serviceCredential",
                "Service principal credential.",
            ),
        ),
    ),
    "kafka": _Spec(
        "featureStoreKafkaConnectorDTO",
        "KAFKA",
        "Register an external Kafka connector.",
        (
            _Opt(
                "--bootstrap-servers",
                "bootstrapServers",
                "Comma-separated broker list.",
                required=True,
            ),
            _Opt(
                "--security-protocol",
                "securityProtocol",
                "Broker security protocol.",
                choices=("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"),
            ),
            _Opt(
                "--ssl-truststore-location",
                "sslTruststoreLocation",
                "Truststore, as a full HopsFS path.",
            ),
            _Opt(
                "--ssl-truststore-password",
                "sslTruststorePassword",
                "Truststore password.",
            ),
            _Opt(
                "--ssl-keystore-location",
                "sslKeystoreLocation",
                "Keystore, as a full HopsFS path.",
            ),
            _Opt(
                "--ssl-keystore-password", "sslKeystorePassword", "Keystore password."
            ),
            _Opt("--ssl-key-password", "sslKeyPassword", "Key password."),
            _Opt(
                "--ssl-endpoint-identification-algorithm",
                "sslEndpointIdentificationAlgorithm",
                'Endpoint identification algorithm. "" turns hostname verification off.',
                # The backend upper-cases this without a null check, so omitting it
                # answers 500 rather than a validation error.
                default="HTTPS",
                choices=("HTTPS", ""),
            ),
            _Opt(
                "--external",
                "externalKafka",
                "Cluster is outside Hopsworks.",
                kind="flag",
            ),
        ),
    ),
    "gcs": _Spec(
        "featureStoreGcsConnectorDTO",
        "GCS",
        "Register a Google Cloud Storage connector.",
        (
            _Opt("--bucket", "bucket", "GCS bucket.", required=True),
            _Opt(
                "--key-path",
                "keyPath",
                "Service-account key file, as a full HopsFS path "
                "(/Projects/<project>/Resources/key.json).",
                required=True,
            ),
            _Opt(
                "--algorithm",
                "algorithm",
                "Customer-supplied encryption algorithm.",
                choices=("AES256",),
            ),
            _Opt(
                "--encryption-key", "encryptionKey", "Customer-supplied encryption key."
            ),
            _Opt(
                "--encryption-key-hash",
                "encryptionKeyHash",
                "Hash of the encryption key.",
            ),
        ),
    ),
    "opensearch": _Spec(
        "featurestoreOpensearchConnectorDTO",
        "OPENSEARCH",
        "Register an OpenSearch connector.",
        (
            _Opt("--host", "host", "OpenSearch host.", required=True),
            _Opt("--port", "port", "OpenSearch port.", kind="int"),
            _Opt("--scheme", "scheme", "http or https.", choices=("http", "https")),
            _Opt("--user", "username", "User name."),
            _Opt("--password", "password", "Password."),
            _Opt(
                "--truststore-path",
                "trustStorePath",
                "Truststore, as a full HopsFS path.",
            ),
            _Opt("--truststore-password", "trustStorePassword", "Truststore password."),
            _Opt("--no-verify", "verify", "Skip TLS verification.", kind="flag_false"),
            _ARGS,
        ),
    ),
    "sql": _Spec(
        "featurestoreSqlConnectorDTO",
        "SQL",
        "Register a SQL database connector.",
        (
            _Opt(
                "--database-type",
                "databaseType",
                "Database engine.",
                required=True,
                choices=("MYSQL", "POSTGRESQL", "ORACLE"),
            ),
            _Opt("--host", "host", "Database host.", required=True),
            _Opt("--port", "port", "Database port.", kind="int"),
            _Opt("--database", "database", "Database name."),
            _Opt("--user", "user", "Database user."),
            _Opt("--password", "password", "Database password."),
            _Opt(
                "--wallet-path", "walletPath", "Oracle wallet, as a full HopsFS path."
            ),
            _Opt("--wallet-password", "walletPassword", "Oracle wallet password."),
            _ARGS,
        ),
    ),
    "sap-hana": _Spec(
        "featureStoreSapHanaConnectorDTO",
        "SAP_HANA",
        "Register an SAP HANA connector.",
        (
            _Opt("--host", "host", "HANA host.", required=True),
            _Opt("--port", "port", "HANA port.", kind="int"),
            _Opt("--database", "database", "Database name."),
            _Opt("--schema", "schema", "Schema name."),
            _Opt("--table", "table", "Default table."),
            _Opt("--user", "user", "User name."),
            _Opt("--password", "password", "Password."),
            _Opt("--application", "application", "Application name reported to HANA."),
            _ARGS,
        ),
    ),
    "unity-catalog": _Spec(
        "featurestoreUnityCatalogConnectorDTO",
        "UNITY_CATALOG",
        "Register a Databricks Unity Catalog connector.",
        (
            _Opt(
                "--workspace-url",
                "workspaceUrl",
                "Databricks workspace URL.",
                required=True,
            ),
            _Opt(
                "--auth-method",
                "authMethod",
                "Authentication method.",
                choices=("PAT", "OAUTH_M2M"),
            ),
            _Opt(
                "--access-token",
                "accessToken",
                "Personal access token, for --auth-method PAT.",
            ),
            _Opt(
                "--client-id",
                "clientId",
                "Service principal id, for --auth-method OAUTH_M2M.",
            ),
            _Opt(
                "--client-secret",
                "clientSecret",
                "Service principal secret, for --auth-method OAUTH_M2M.",
            ),
            _Opt(
                "--default-catalog",
                "defaultCatalog",
                "Catalog used when a query names none.",
            ),
            _Opt("--aws-region", "awsRegion", "AWS region of the workspace."),
            _Opt("--account-id", "accountId", "Databricks account id."),
            _Opt("--account-host", "accountHost", "Databricks account host."),
            _ARGS,
        ),
    ),
    "glue": _Spec(
        "featurestoreGlueConnectorDTO",
        "GLUE",
        "Register an AWS Glue Data Catalog connector.",
        (
            _Opt("--database", "database", "Glue database.", required=True),
            _Opt("--region", "region", "AWS region.", required=True),
            _Opt(
                "--catalog-id",
                "catalogId",
                "Glue catalog id; defaults to the account's.",
            ),
            _Opt("--iam-role", "iamRole", "IAM role to assume."),
            _Opt("--access-key", "accessKey", "AWS access key id."),
            _Opt("--secret-key", "secretKey", "AWS secret access key."),
            _Opt("--session-token", "sessionToken", "AWS session token."),
            _ARGS,
        ),
    ),
    "google-sheets": _Spec(
        "featurestoreGoogleSheetsConnectorDTO",
        "GOOGLE_SHEETS",
        "Register a Google Sheets connector.",
        (
            _Opt(
                "--spreadsheet-id",
                "spreadsheetId",
                "Spreadsheet id from its URL.",
                required=True,
            ),
            _Opt(
                "--key-path",
                "keyPath",
                "Service-account key file, as a full HopsFS path "
                "(/Projects/<project>/Resources/key.json).",
                required=True,
            ),
        ),
    ),
    "rest": _Spec(
        "featurestoreRESTConnectorDTO",
        "REST",
        "Register a REST API connector.",
        (
            _Opt(
                "--base-url",
                "clientConfig.baseUrl",
                "Base URL every request is built on.",
                required=True,
            ),
            _Opt(
                "--auth-type",
                "authConfig.authType",
                "How requests authenticate.",
                required=True,
                choices=(
                    "NONE",
                    "API_KEY",
                    "BEARER_TOKEN",
                    "HTTP_BASIC",
                    "OAUTH2_CLIENT",
                ),
            ),
            _Opt("--api-key", "authConfig.apiKey", "API key, for --auth-type API_KEY."),
            _Opt(
                "--bearer-token",
                "authConfig.bearerToken",
                "Token, for --auth-type BEARER_TOKEN.",
            ),
            _Opt(
                "--user",
                "authConfig.username",
                "User name, for --auth-type HTTP_BASIC.",
            ),
            _Opt(
                "--password",
                "authConfig.password",
                "Password, for --auth-type HTTP_BASIC.",
            ),
            _Opt(
                "--client-id",
                "authConfig.clientId",
                "Client id, for --auth-type OAUTH2_CLIENT.",
            ),
            _Opt(
                "--client-secret",
                "authConfig.clientSecret",
                "Client secret, for --auth-type OAUTH2_CLIENT.",
            ),
            _Opt(
                "--access-token",
                "authConfig.accessToken",
                "Access token, for --auth-type OAUTH2_CLIENT.",
            ),
            _Opt(
                "--access-token-url",
                "authConfig.accessTokenUrl",
                "Token endpoint, for --auth-type OAUTH2_CLIENT.",
            ),
            _Opt(
                "--token-timeout-minutes",
                "authConfig.defaultTokenTimeoutMinutes",
                "Minutes before a fetched token is refreshed.",
                kind="int",
            ),
        ),
        epilog="Example: hops datasource create rest weather --base-url https://api.example.com --auth-type API_KEY --api-key KEY",
    ),
    "crm": _Spec(
        "featurestoreCRMConnectorDTO",
        "CRM",
        "Register a CRM or analytics connector.",
        (
            _Opt(
                "--crm-type",
                "crmType",
                "Which service to connect to; it decides which other options apply.",
                required=True,
                choices=(
                    "HUBSPOT",
                    "SALESFORCE",
                    "PIPEDRIVE",
                    "FACEBOOK_ADS",
                    "FRESHDESK",
                    "GOOGLE_ADS",
                    "GOOGLE_ANALYTICS",
                    "SHOPIFY",
                ),
            ),
            _Opt(
                "--api-key",
                "apiKey",
                "API key or token. Required except for GOOGLE_ADS, GOOGLE_ANALYTICS and SHOPIFY.",
            ),
            _Opt("--user", "username", "User name, for SALESFORCE."),
            _Opt("--password", "password", "Password, for SALESFORCE."),
            _Opt("--account-id", "accountId", "Ad account id, for FACEBOOK_ADS."),
            _Opt("--domain", "domain", "Account domain, for FRESHDESK."),
            _Opt(
                "--key-path",
                "keyPath",
                "Service-account key file as a full HopsFS path, for GOOGLE_ADS and GOOGLE_ANALYTICS.",
            ),
            _Opt("--property-id", "propertyId", "Property id, for GOOGLE_ANALYTICS."),
            _Opt("--dev-token", "devToken", "Developer token, for GOOGLE_ADS."),
            _Opt("--customer-id", "customerId", "Customer id, for GOOGLE_ADS."),
            _Opt(
                "--impersonated-email",
                "impersonatedEmail",
                "Impersonated user, for GOOGLE_ADS.",
            ),
            _Opt("--refresh-token", "refreshToken", "Refresh token, for GOOGLE_ADS."),
            _Opt("--shop-url", "shopUrl", "Shop URL, for SHOPIFY."),
            _Opt(
                "--private-app-password",
                "privateAppPassword",
                "Private app password, for SHOPIFY.",
            ),
        ),
    ),
}


def _set_path(body: dict[str, Any], path: str, value: Any) -> None:
    """Assign ``value`` at a dotted ``path``, creating the objects it walks through."""
    keys = path.split(".")
    for key in keys[:-1]:
        body = body.setdefault(key, {})
    body[keys[-1]] = value


def _parse_arguments(values: tuple[str, ...]) -> list[dict[str, str]]:
    """Turn repeated ``key=value`` options into the arguments list connectors take."""
    parsed = []
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise click.BadParameter(
                f'expected "key=value", got {item!r}', param_hint="--argument"
            )
        parsed.append({"name": key, "value": value})
    return parsed


def _build_create_command(name: str, spec: _Spec) -> click.Command:
    """Build the ``create <name>`` command for one connector type."""

    def run(
        ctx: click.Context, connector_name: str, description: str, **values: Any
    ) -> None:
        body: dict[str, Any] = {
            "type": spec.dto,
            "name": connector_name,
            "storageConnectorType": spec.connector_type,
            "description": description,
        }
        for opt in spec.opts:
            value = values.get(opt.flag.lstrip("-").replace("-", "_"))
            if value is None or value == () or (opt.kind == "flag" and not value):
                continue
            if opt.kind == "args":
                _set_path(body, opt.json, _parse_arguments(value))
            elif opt.kind == "flag_false":
                _set_path(body, opt.json, False)
            else:
                _set_path(body, opt.json, value)
        _create_connector(ctx, body)

    # Click renders a command's docstring as its help, so the docstring is the
    # summary alone; the options carry their own help and the parameters are
    # generated, which leaves nothing for an Args section to say.
    run.__doc__ = spec.summary
    cmd = click.pass_context(run)
    cmd = click.option("--description", default="", help="Free-form description.")(cmd)
    for opt in reversed(spec.opts):
        if opt.kind in ("flag", "flag_false"):
            cmd = click.option(opt.flag, is_flag=True, help=opt.help)(cmd)
        elif opt.kind == "args":
            cmd = click.option(opt.flag, multiple=True, help=opt.help)(cmd)
        else:
            cmd = click.option(
                opt.flag,
                required=opt.required,
                default=opt.default,
                show_default=opt.default is not None,
                type=click.Choice(opt.choices)
                if opt.choices
                else (int if opt.kind == "int" else str),
                help=opt.help,
            )(cmd)
    cmd = click.argument("connector_name", metavar="NAME")(cmd)
    return click.command(name, short_help=spec.summary, epilog=spec.epilog or None)(cmd)


for _name, _spec in _SPECS.items():
    connector_create.add_command(_build_create_command(_name, _spec))

# endregion


@datasource_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def connector_delete(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete a storage connector.

    Args:
        ctx: Click context.
        name: Connector name.
        yes: Skip confirmation when True.
    """
    fs = session.get_feature_store(ctx)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete connector '{name}'?", abort=True)

    from hopsworks_common.core import rest

    try:
        rest._send_request(
            "DELETE",
            rest._project_path(
                "featurestores", getattr(fs, "id", None), "storageconnectors", name
            ),
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted connector %s", name)


@datasource_group.command("databases")
@click.argument("name")
@click.pass_context
def connector_databases(ctx: click.Context, name: str) -> None:
    """List databases visible through a connector.

    Args:
        ctx: Click context.
        name: Connector name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        databases = ds.get_databases()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list databases: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(databases)
        return
    output.print_table(["DATABASE"], [[db] for db in databases or []])


@datasource_group.command("tables")
@click.argument("name")
@click.option("--database", help="Database to list tables from.")
@click.pass_context
def connector_tables(ctx: click.Context, name: str, database: str | None) -> None:
    """List tables in a database reachable through a connector.

    Args:
        ctx: Click context.
        name: Connector name.
        database: Database name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        tables = ds.get_tables(database=database)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list tables: {exc}") from exc

    rows = [
        [
            getattr(t, "table", None) or "?",
            getattr(t, "database", None) or database or "-",
        ]
        for t in tables or []
    ]
    output.print_table(["TABLE", "DATABASE"], rows)


@datasource_group.command("preview")
@click.argument("name")
@click.pass_context
def connector_preview(ctx: click.Context, name: str) -> None:
    """Fetch a small data preview through a connector.

    Args:
        ctx: Click context.
        name: Connector name.
    """
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        data = ds.get_data()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Preview failed: {exc}") from exc
    if output.JSON_MODE:
        to_dict = getattr(data, "to_dict", None)
        output.print_json(to_dict() if callable(to_dict) else str(data))
        return
    click.echo(str(data))


@datasource_group.command("infer-metadata")
@click.argument("name")
@click.argument("table")
@click.option("--database", help="Database the table lives in (connector-dependent).")
@click.pass_context
def connector_infer_metadata(
    ctx: click.Context, name: str, table: str, database: str | None
) -> None:
    """Use platform intelligence to infer feature metadata for a table.

    Calls the same LLM-backed endpoint as the "Infer metadata" button in the
    UI: suggests a renamed feature name, Hopsworks type, and description per
    column, plus a primary key and event time. Use this before mounting an
    external table as an external feature group, or before creating a new
    feature group from it.

    Args:
        ctx: Click context.
        name: Connector name.
        table: Table name to infer metadata for.
        database: Database that contains the table.
    """
    from hopsworks_common.client.exceptions import PlatformIntelligenceException

    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
        tables = ds.get_tables(database=database) or []
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list tables: {exc}") from exc

    match = next((t for t in tables if getattr(t, "table", None) == table), None)
    if match is None:
        raise click.ClickException(
            f"Table '{table}' not found in connector '{name}'"
            + (f" / database '{database}'" if database else "")
        )

    try:
        inferred = match.infer_metadata()
    except PlatformIntelligenceException as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Infer metadata failed: {exc}") from exc

    if output.JSON_MODE:
        output.print_json(inferred.to_dict())
        return

    rows = [
        [f.original_name, f.new_name, f.type, output.first_line(f.description)]
        for f in inferred.features
    ]
    output.print_table(["ORIGINAL", "NEW", "TYPE", "DESCRIPTION"], rows)
    if inferred.suggested_primary_key:
        click.echo(
            f"Suggested primary key: {', '.join(inferred.suggested_primary_key)}"
        )
    if inferred.suggested_event_time:
        click.echo(f"Suggested event time: {inferred.suggested_event_time}")


def _create_connector(ctx: click.Context, body: dict[str, Any]) -> None:
    fs = session.get_feature_store(ctx)
    from hopsworks_common.core import rest

    try:
        rest._send_request(
            "POST",
            rest._project_path(
                "featurestores", getattr(fs, "id", None), "storageconnectors"
            ),
            json_body=body,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Create failed: {exc}") from exc
    output.success("✓ Created connector %s", body["name"])

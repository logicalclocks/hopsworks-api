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

import re
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import click
from click.core import ParameterSource
from hopsworks.cli import output, session


if TYPE_CHECKING:
    from collections.abc import Callable


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


# region Secret options
#
# A secret never has to travel on the command line. Every secret option also
# reads ``HOPSWORKS_DS_<CONNECTOR>_<OPTION>`` from the environment, scoped to
# the connector type so a variable exported for one connector cannot ride along
# on another; accepts ``-`` to read one line from stdin; and, when required and
# stdin is a terminal, asks for the value without echo.

_SECRET_ENV_PREFIX = "HOPSWORKS_DS_"
_STDIN_SECRET_KEY = "hops.datasource.secret_from_stdin"


def _dest(flag: str) -> str:
    """The keyword Click passes a ``--some-flag`` or ``--on/--off`` option under."""
    return flag.split("/")[0].lstrip("-").replace("-", "_")


def _env(connector: str, flag: str) -> str:
    return f"{_SECRET_ENV_PREFIX}{connector.upper().replace('-', '_')}_{_dest(flag).upper()}"


def _read_secret(
    ctx: click.Context, param: click.Parameter, value: str | None, required: bool
) -> str | None:
    """Resolve a secret option at parse time.

    ``-`` reads one line from stdin, once per command since stdin is consumed.
    A required secret that was not given is prompted for without echo when
    stdin is a terminal, and is a missing option otherwise, so a script or an
    agent gets an error instead of a hang.
    """
    flag = param.opts[0]
    if value == "-":
        taken = ctx.meta.get(_STDIN_SECRET_KEY)
        if taken:
            raise click.UsageError(
                f"Only one secret can be read from stdin; {taken} already was.", ctx
            )
        ctx.meta[_STDIN_SECRET_KEY] = flag
        value = sys.stdin.readline().rstrip("\r\n")
        if not value:
            raise click.BadParameter("stdin was empty", ctx=ctx, param=param)
    if value is None and required:
        if _interactive():
            value = click.prompt(flag.lstrip("-").replace("-", " "), hide_input=True)
        else:
            raise click.MissingParameter(ctx=ctx, param=param)
    return value


def _interactive() -> bool:
    return sys.stdin.isatty()


def _secret_option(
    connector: str, flag: str, help: str, required: bool = False
) -> Callable[[Any], Any]:
    """``click.option`` for a secret-bearing option of ``connector``."""

    def callback(ctx: click.Context, param: click.Parameter, value: str | None):
        return _read_secret(ctx, param, value, required)

    return click.option(
        flag,
        envvar=_env(connector, flag),
        show_envvar=True,
        callback=callback,
        help=f"{help} Pass - to read it from stdin.",
    )


# endregion


# region Write commands


@datasource_group.group("create")
def connector_create() -> None:
    """Create a storage connector (subcommand per backend)."""


@connector_create.command("jdbc", help="Register a JDBC connector.")
@click.argument("name")
@click.option("--url", required=True, help="JDBC connection URL.")
@click.option("--user", help='Connection user, stored as "user".')
@_secret_option("jdbc", "--password", 'Connection password, stored as "password".')
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
@_secret_option("s3", "--secret-key", "AWS secret access key.")
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
@_secret_option("snowflake", "--password", "Password.", required=True)
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


# region Connector specs
#
# The four commands above are written out by hand; the fourteen below differ only in
# their fields, so they are declared and built rather than copied. Each entry names the
# DTO Jackson expects on ``type``, the ``storageConnectorType`` enum value, the options,
# whose ``json`` is the DTO property (dotted for the payloads that nest), and a check
# for the rules that hold between options, mirroring the backend's create validation
# so a bad combination is refused before anything is sent.

_SECRET_ENV_PREFIX = "HOPSWORKS_DS_"


@dataclass(frozen=True)
class _Opt:
    """One CLI option and the connector property it fills.

    ``kind`` is ``str``, ``int``, ``flag`` (sent only when set), ``bool`` (an
    on/off pair, sent only when either is given) or ``args`` (repeatable
    ``key=value``, sent as the connector's argument list).
    A ``secret`` option also reads ``HOPSWORKS_DS_<CONNECTOR>_<OPTION>`` and
    accepts ``-`` for stdin, so the value never has to be an argument.
    """

    flag: str
    json: str
    help: str
    required: bool = False
    kind: str = "str"
    choices: tuple[str, ...] = ()
    default: str | None = None
    secret: bool = False


@dataclass(frozen=True)
class _Spec:
    """A connector type: what to send, what to ask for, and what must go together."""

    dto: str
    connector_type: str
    summary: str
    opts: tuple[_Opt, ...]
    check: Callable[[dict[str, Any]], str | None] | None = None
    epilog: str = ""


def _given(values: dict[str, Any], flag: str) -> bool:
    value = values.get(_dest(flag))
    return value is not None and value is not False and value != "" and value != ()


def _needs(values: dict[str, Any], context: str, *flags: str) -> str | None:
    missing = [f for f in flags if not _given(values, f)]
    return f"{context} needs {', '.join(missing)}." if missing else None


def _refuses(values: dict[str, Any], context: str, *flags: str) -> str | None:
    extra = [f for f in flags if _given(values, f)]
    return f"{context} does not take {', '.join(extra)}." if extra else None


def _one_of(values: dict[str, Any], context: str, *flags: str) -> str | None:
    if sum(_given(values, f) for f in flags) == 1:
        return None
    return f"{context} takes exactly one of {', '.join(flags)}."


def _check_redshift(v: dict[str, Any]) -> str | None:
    if _given(v, "--iam-role") and _given(v, "--password"):
        return "--iam-role and --password are alternatives; pass one."
    return (
        None
        if v.get("auto_create")
        else _needs(v, "redshift without --auto-create", "--user")
    )


def _check_adls(v: dict[str, Any]) -> str | None:
    if v["generation"] not in (1, 2):
        return "--generation must be 1 or 2."
    return (
        _needs(v, "--generation 2", "--container-name")
        if v["generation"] == 2
        else None
    )


_KAFKA_SSL_FLAGS = (
    "--ssl-truststore-location",
    "--ssl-truststore-password",
    "--ssl-keystore-location",
    "--ssl-keystore-password",
    "--ssl-key-password",
)


def _check_kafka(v: dict[str, Any]) -> str | None:
    protocol = v["security_protocol"]
    if protocol == "SSL":
        return _needs(
            v,
            "--security-protocol SSL",
            "--ssl-truststore-location",
            "--ssl-keystore-location",
            "--ssl-key-password",
        )
    if protocol.endswith("PLAINTEXT"):
        return _refuses(v, f"--security-protocol {protocol}", *_KAFKA_SSL_FLAGS)
    return None


def _check_gcs(v: dict[str, Any]) -> str | None:
    if not _given(v, "--algorithm"):
        return None
    return _needs(v, "--algorithm", "--encryption-key", "--encryption-key-hash")


def _check_sql(v: dict[str, Any]) -> str | None:
    if v["database_type"] == "ORACLE":
        return _one_of(v, "--database-type ORACLE", "--host", "--wallet-path")
    context = f"--database-type {v['database_type']}"
    return _needs(v, context, "--host") or _refuses(
        v, context, "--wallet-path", "--wallet-password"
    )


_UC_OAUTH_FLAGS = (
    "--client-id",
    "--client-secret",
    "--oauth-endpoint",
    "--account-id",
    "--account-host",
)


def _check_unity_catalog(v: dict[str, Any]) -> str | None:
    if v["auth_method"] == "PAT":
        return _needs(v, "--auth-method PAT", "--access-token") or _refuses(
            v, "--auth-method PAT", *_UC_OAUTH_FLAGS
        )
    problem = _needs(
        v,
        "--auth-method OAUTH_M2M",
        "--oauth-endpoint",
        "--client-id",
        "--client-secret",
    ) or _refuses(v, "--auth-method OAUTH_M2M", "--access-token")
    if problem:
        return problem
    if v["oauth_endpoint"] == "WORKSPACE":
        return _refuses(
            v, "--oauth-endpoint WORKSPACE", "--account-id", "--account-host"
        )
    return _needs(v, "--oauth-endpoint ACCOUNT", "--account-id", "--account-host")


def _check_glue(v: dict[str, Any]) -> str | None:
    if _given(v, "--iam-role"):
        return _refuses(
            v, "--iam-role", "--access-key", "--secret-key", "--session-token"
        )
    if _given(v, "--access-key") or _given(v, "--secret-key"):
        return _needs(v, "AWS access keys", "--access-key", "--secret-key")
    if _given(v, "--session-token"):
        return _needs(v, "--session-token", "--access-key", "--secret-key")
    return None


# Required, then optional, per auth type; every other auth option is refused,
# so a credential meant for another mode (or another shell) is never stored.
_REST_AUTH_NEEDS = {
    "NONE": (),
    "API_KEY": ("--api-key",),
    "BEARER_TOKEN": ("--bearer-token",),
    "HTTP_BASIC": ("--user", "--password"),
    "OAUTH2_CLIENT": ("--access-token-url", "--client-id", "--client-secret"),
}
_REST_AUTH_OPTIONAL = {"OAUTH2_CLIENT": ("--access-token", "--token-timeout-minutes")}
_REST_AUTH_FLAGS = frozenset(
    f
    for flags in (*_REST_AUTH_NEEDS.values(), *_REST_AUTH_OPTIONAL.values())
    for f in flags
)


def _check_rest(v: dict[str, Any]) -> str | None:
    auth = v["auth_type"]
    allowed = {*_REST_AUTH_NEEDS[auth], *_REST_AUTH_OPTIONAL.get(auth, ())}
    return _needs(v, f"--auth-type {auth}", *_REST_AUTH_NEEDS[auth]) or _refuses(
        v, f"--auth-type {auth}", *sorted(_REST_AUTH_FLAGS - allowed)
    )


_CRM_NEEDS = {
    "HUBSPOT": ("--api-key",),
    "PIPEDRIVE": ("--api-key",),
    "SALESFORCE": ("--api-key", "--user", "--password"),
    "FRESHDESK": ("--api-key", "--domain"),
    "FACEBOOK_ADS": ("--api-key", "--account-id"),
    "GOOGLE_ANALYTICS": ("--key-path", "--property-id"),
    "GOOGLE_ADS": (
        "--key-path",
        "--dev-token",
        "--customer-id",
        "--impersonated-email",
        "--refresh-token",
    ),
    "SHOPIFY": ("--shop-url", "--private-app-password"),
}


_CRM_FLAGS = frozenset(f for flags in _CRM_NEEDS.values() for f in flags)


def _check_crm(v: dict[str, Any]) -> str | None:
    crm = v["crm_type"]
    return _needs(v, f"--crm-type {crm}", *_CRM_NEEDS[crm]) or _refuses(
        v, f"--crm-type {crm}", *sorted(_CRM_FLAGS - set(_CRM_NEEDS[crm]))
    )


def _check_mongodb(v: dict[str, Any]) -> str | None:
    return _needs(v, "--password", "--user") if _given(v, "--password") else None


_ARGS = _Opt(
    "--argument",
    "arguments",
    'Extra connector argument as "key=value". Repeat for several.',
    kind="args",
)
_OPTIONS = _Opt(
    "--option",
    "options",
    'Extra client property as "key=value". Repeat for several.',
    kind="args",
)
_KEY_PATH_HELP = (
    "Service-account key file, as a full HopsFS path "
    "(/Projects/<project>/Resources/key.json)."
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
            _Opt(
                "--endpoint",
                "databaseEndpoint",
                "Cluster endpoint, without the port.",
                required=True,
            ),
            _Opt("--database", "databaseName", "Database name.", required=True),
            _Opt("--port", "databasePort", "Database port.", required=True, kind="int"),
            _Opt(
                "--user",
                "databaseUserName",
                "Database user. Required unless --auto-create.",
            ),
            _Opt(
                "--password",
                "databasePassword",
                "Database password. Not with --iam-role.",
                secret=True,
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
        check=_check_redshift,
    ),
    "adls": _Spec(
        "featurestoreADLSConnectorDTO",
        "ADLS",
        "Register an Azure Data Lake Storage connector.",
        (
            _Opt(
                "--account-name", "accountName", "Storage account name.", required=True
            ),
            _Opt(
                "--generation",
                "generation",
                "ADLS generation, 1 or 2.",
                required=True,
                kind="int",
            ),
            _Opt(
                "--container-name",
                "containerName",
                "Container name. Required for --generation 2.",
            ),
            _Opt(
                "--directory-id",
                "directoryId",
                "Azure AD directory (tenant) id.",
                required=True,
            ),
            _Opt(
                "--application-id",
                "applicationId",
                "Application (client) id.",
                required=True,
            ),
            _Opt(
                "--service-credential",
                "serviceCredential",
                "Service principal credential.",
                required=True,
                secret=True,
            ),
        ),
        check=_check_adls,
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
                required=True,
                choices=("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"),
            ),
            _Opt(
                "--ssl-truststore-location",
                "sslTruststoreLocation",
                "Truststore, as a full HopsFS path. Required for SSL.",
            ),
            _Opt(
                "--ssl-truststore-password",
                "sslTruststorePassword",
                "Truststore password.",
                secret=True,
            ),
            _Opt(
                "--ssl-keystore-location",
                "sslKeystoreLocation",
                "Keystore, as a full HopsFS path. Required for SSL.",
            ),
            _Opt(
                "--ssl-keystore-password",
                "sslKeystorePassword",
                "Keystore password.",
                secret=True,
            ),
            _Opt(
                "--ssl-key-password",
                "sslKeyPassword",
                "Key password. Required for SSL.",
                secret=True,
            ),
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
            _OPTIONS,
        ),
        check=_check_kafka,
    ),
    "gcs": _Spec(
        "featureStoreGcsConnectorDTO",
        "GCS",
        "Register a Google Cloud Storage connector.",
        (
            _Opt("--bucket", "bucket", "GCS bucket.", required=True),
            _Opt("--key-path", "keyPath", _KEY_PATH_HELP, required=True),
            _Opt(
                "--algorithm",
                "algorithm",
                "Customer-supplied encryption algorithm.",
                choices=("AES256",),
            ),
            _Opt(
                "--encryption-key",
                "encryptionKey",
                "Customer-supplied encryption key. Required with --algorithm.",
                secret=True,
            ),
            _Opt(
                "--encryption-key-hash",
                "encryptionKeyHash",
                "Hash of the encryption key. Required with --algorithm.",
            ),
        ),
        check=_check_gcs,
    ),
    "opensearch": _Spec(
        "featurestoreOpensearchConnectorDTO",
        "OPENSEARCH",
        "Register an OpenSearch connector.",
        (
            _Opt("--host", "host", "OpenSearch host.", required=True),
            _Opt("--port", "port", "OpenSearch port.", required=True, kind="int"),
            _Opt("--scheme", "scheme", "http or https.", choices=("http", "https")),
            _Opt("--user", "username", "User name."),
            _Opt("--password", "password", "Password.", secret=True),
            _Opt(
                "--truststore-path",
                "trustStorePath",
                "Truststore, as a full HopsFS path.",
            ),
            _Opt(
                "--truststore-password",
                "trustStorePassword",
                "Truststore password.",
                secret=True,
            ),
            _Opt(
                "--tls-verify/--no-tls-verify",
                "verify",
                "Verify the OpenSearch certificate; the server verifies when unset.",
                kind="bool",
            ),
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
            _Opt(
                "--host",
                "host",
                "Database host. Required, except for ORACLE with --wallet-path.",
            ),
            _Opt("--port", "port", "Database port.", required=True, kind="int"),
            _Opt(
                "--database",
                "database",
                "Database name; the service name, SID or TNS alias for ORACLE.",
                required=True,
            ),
            _Opt("--user", "user", "Database user.", required=True),
            _Opt("--password", "password", "Database password.", secret=True),
            _Opt(
                "--wallet-path",
                "walletPath",
                "Oracle wallet, as a full HopsFS path. Replaces --host.",
            ),
            _Opt(
                "--wallet-password",
                "walletPassword",
                "Oracle wallet password.",
                secret=True,
            ),
            _ARGS,
        ),
        check=_check_sql,
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
            _Opt("--user", "user", "User name.", required=True),
            _Opt("--password", "password", "Password.", required=True, secret=True),
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
                default="PAT",
                choices=("PAT", "OAUTH_M2M"),
            ),
            _Opt(
                "--access-token",
                "accessToken",
                "Personal access token, for --auth-method PAT.",
                secret=True,
            ),
            _Opt(
                "--oauth-endpoint",
                "oauthEndpoint",
                "Where the service principal is registered, for --auth-method OAUTH_M2M.",
                choices=("WORKSPACE", "ACCOUNT"),
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
                secret=True,
            ),
            _Opt(
                "--account-id",
                "accountId",
                "Databricks account id (a UUID), for --oauth-endpoint ACCOUNT.",
            ),
            _Opt(
                "--account-host",
                "accountHost",
                "Databricks account host, for --oauth-endpoint ACCOUNT.",
            ),
            _Opt(
                "--default-catalog",
                "defaultCatalog",
                "Catalog used when a query names none.",
            ),
            _Opt("--aws-region", "awsRegion", "AWS region of the workspace."),
            _ARGS,
        ),
        check=_check_unity_catalog,
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
            _Opt("--iam-role", "iamRole", "IAM role to assume. Not with access keys."),
            _Opt("--access-key", "accessKey", "AWS access key id."),
            _Opt("--secret-key", "secretKey", "AWS secret access key.", secret=True),
            _Opt("--session-token", "sessionToken", "AWS session token.", secret=True),
            _ARGS,
        ),
        check=_check_glue,
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
            _Opt("--key-path", "keyPath", _KEY_PATH_HELP, required=True),
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
                choices=tuple(_REST_AUTH_NEEDS),
            ),
            _Opt(
                "--api-key",
                "authConfig.apiKey",
                "API key, for --auth-type API_KEY.",
                secret=True,
            ),
            _Opt(
                "--bearer-token",
                "authConfig.bearerToken",
                "Token, for --auth-type BEARER_TOKEN.",
                secret=True,
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
                secret=True,
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
                secret=True,
            ),
            _Opt(
                "--access-token",
                "authConfig.accessToken",
                "Access token, for --auth-type OAUTH2_CLIENT.",
                secret=True,
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
        check=_check_rest,
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
                choices=tuple(_CRM_NEEDS),
            ),
            _Opt(
                "--api-key",
                "apiKey",
                "API key or token. Required except for GOOGLE_ADS, GOOGLE_ANALYTICS and SHOPIFY.",
                secret=True,
            ),
            _Opt("--user", "username", "User name, for SALESFORCE."),
            _Opt("--password", "password", "Password, for SALESFORCE.", secret=True),
            _Opt("--account-id", "accountId", "Ad account id, for FACEBOOK_ADS."),
            _Opt("--domain", "domain", "Account domain, for FRESHDESK."),
            _Opt(
                "--key-path",
                "keyPath",
                "Service-account key file as a full HopsFS path, for GOOGLE_ADS and GOOGLE_ANALYTICS.",
            ),
            _Opt("--property-id", "propertyId", "Property id, for GOOGLE_ANALYTICS."),
            _Opt(
                "--dev-token",
                "devToken",
                "Developer token, for GOOGLE_ADS.",
                secret=True,
            ),
            _Opt("--customer-id", "customerId", "Customer id, for GOOGLE_ADS."),
            _Opt(
                "--impersonated-email",
                "impersonatedEmail",
                "Impersonated user, for GOOGLE_ADS.",
            ),
            _Opt(
                "--refresh-token",
                "refreshToken",
                "Refresh token, for GOOGLE_ADS.",
                secret=True,
            ),
            _Opt("--shop-url", "shopUrl", "Shop URL, for SHOPIFY."),
            _Opt(
                "--private-app-password",
                "privateAppPassword",
                "Private app password, for SHOPIFY.",
                secret=True,
            ),
        ),
        check=_check_crm,
    ),
    "mongodb": _Spec(
        "featurestoreMongoConnectorDTO",
        "MONGODB",
        "Register a MongoDB connector.",
        (
            _Opt(
                "--connection-string",
                "connectionString",
                "mongodb:// or mongodb+srv:// URI without credentials; "
                "pass those as --user and --password.",
                required=True,
            ),
            _Opt("--database", "database", "Database name.", required=True),
            _Opt("--collection", "collection", "Default collection."),
            _Opt("--user", "user", "Database user."),
            _Opt(
                "--password",
                "password",
                "Database password, kept in the Hopsworks secret store.",
                secret=True,
            ),
            _Opt(
                "--auth-source", "authSource", "authSource URI parameter, e.g. admin."
            ),
            _Opt(
                "--auth-mechanism",
                "authMechanism",
                "authMechanism URI parameter, e.g. SCRAM-SHA-256.",
            ),
            _OPTIONS,
        ),
        check=_check_mongodb,
    ),
}


def _set_path(body: dict[str, Any], path: str, value: Any) -> None:
    """Assign ``value`` at a dotted ``path``, creating the objects it walks through."""
    keys = path.split(".")
    for key in keys[:-1]:
        body = body.setdefault(key, {})
    body[keys[-1]] = value


# Connector arguments are stored in a plain-text column and returned by the API,
# so a credential must not travel in one (Kafka's sasl.jaas.config is the usual
# offender); the connector's own secret options go to the secret store instead.
_CREDENTIAL_KEY = re.compile(
    r"(?i)(password|passwd|secret|token|credential|jaas|api[._-]?key|private[._-]?key)"
)


def _parse_arguments(flag: str, values: tuple[str, ...]) -> list[dict[str, str]]:
    """Turn repeated ``key=value`` options into the list of name/value pairs connectors take."""
    parsed = []
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise click.BadParameter(
                f'expected "key=value", got {item!r}', param_hint=flag
            )
        if _CREDENTIAL_KEY.search(key):
            raise click.BadParameter(
                f"{key!r} looks like a credential, and connector arguments are stored "
                f"and returned in plain text; use the connector's own secret options.",
                param_hint=flag,
            )
        parsed.append({"name": key, "value": value})
    return parsed


def _option(opt: _Opt, connector: str) -> Callable[[Any], Any]:
    """The ``click.option`` decorator for one spec option of ``connector``."""
    if opt.secret:
        return _secret_option(connector, opt.flag, opt.help, opt.required)
    kwargs: dict[str, Any] = {"help": opt.help}
    if opt.kind == "flag":
        return click.option(opt.flag, is_flag=True, **kwargs)
    if opt.kind == "bool":
        return click.option(opt.flag, default=None, **kwargs)
    if opt.kind == "args":
        return click.option(opt.flag, multiple=True, **kwargs)
    if opt.choices:
        kwargs["type"] = click.Choice(opt.choices)
    elif opt.kind == "int":
        kwargs["type"] = int
    # Click 8.3 and later read an explicit ``default=None`` as "None is a valid
    # value" and stop enforcing ``required``, so the default is only passed when
    # there is one.
    if opt.default is not None:
        kwargs.update(default=opt.default, show_default=True)
    return click.option(opt.flag, required=opt.required, **kwargs)


def _build_create_command(name: str, spec: _Spec) -> click.Command:
    """Build the ``create <name>`` command for one connector type."""

    def run(
        ctx: click.Context, connector_name: str, description: str, **values: Any
    ) -> None:
        problem = spec.check(values) if spec.check else None
        if problem:
            # A refused credential that arrived from the environment is invisible
            # on the command line, so say where it came from.
            from_env = [
                f"{opt.flag} came from ${_env(name, opt.flag)}"
                for opt in spec.opts
                if opt.secret
                and opt.flag in problem
                and ctx.get_parameter_source(_dest(opt.flag))
                is ParameterSource.ENVIRONMENT
            ]
            if from_env:
                problem += " (" + "; ".join(from_env) + ")"
            raise click.UsageError(problem, ctx)
        body: dict[str, Any] = {
            "type": spec.dto,
            "name": connector_name,
            "storageConnectorType": spec.connector_type,
            "description": description,
        }
        for opt in spec.opts:
            value = values.get(_dest(opt.flag))
            if value is None or value == () or (opt.kind == "flag" and not value):
                continue
            if opt.kind == "args":
                value = _parse_arguments(opt.flag, value)
            _set_path(body, opt.json, value)
        _create_connector(ctx, body)

    # Click renders a command's docstring as its help, so the docstring is the
    # summary alone; the options carry their own help and the parameters are
    # generated, which leaves nothing for an Args section to say.
    run.__doc__ = spec.summary
    cmd = click.pass_context(run)
    cmd = click.option("--description", default="", help="Free-form description.")(cmd)
    for opt in reversed(spec.opts):
        cmd = _option(opt, name)(cmd)
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

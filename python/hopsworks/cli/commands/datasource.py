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

import os
import re
import shutil
import sys
import tempfile
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import click
from click.core import ParameterSource
from hopsworks.cli import output, session
from hopsworks.cli.commands import job as job_cmd


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
                _credentials_label(c),
                output.first_line(c.get("description"), empty=""),
            ]
        )
    output.print_table(["ID", "NAME", "TYPE", "CREDENTIALS", "DESCRIPTION"], rows)


def _credentials_label(connector: dict[str, Any]) -> str:
    if connector.get("credentialsMode", "SHARED") != "PROVIDED":
        return "shared"
    status = (connector.get("userCredentials") or {}).get("status")
    return {
        "VALID": "provided: yours set",
        "INCOMPLETE": "provided: incomplete",
    }.get(status, "provided: missing")


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
# A secret never has to travel on the command line.
# Every secret option also reads ``HOPSWORKS_DS_<CONNECTOR>_<OPTION>`` from the
# environment, scoped to the connector type so a variable exported for one
# connector cannot ride along on another.
# It also accepts ``-`` to read one line from stdin, and ``account:NAME`` to
# read the user's account variable NAME, so a credential saved there (as the
# Factory's forms do) reaches the data source without passing through a shell.
# When required and stdin is a terminal, it asks for the value without echo.

_SECRET_ENV_PREFIX = "HOPSWORKS_DS_"
_STDIN_SECRET_KEY = "hops.datasource.secret_from_stdin"
_ACCOUNT_SECRET_PREFIX = "account:"


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
    if value and value.startswith(_ACCOUNT_SECRET_PREFIX):
        return _account_secret(ctx, param, value[len(_ACCOUNT_SECRET_PREFIX) :])
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
    # An empty value (``--password ""`` or an empty env var) is missing, not a secret.
    if not value and required:
        if _interactive():
            value = click.prompt(flag.lstrip("-").replace("-", " "), hide_input=True)
        else:
            raise click.MissingParameter(ctx=ctx, param=param)
    return value


def _account_secret(ctx: click.Context, param: click.Parameter, name: str) -> str:
    """The value of the user's account variable `name`, for a secret option."""
    from hopsworks_common.core import env_var_api

    session.get_project(ctx)
    var = env_var_api.EnvVarsApi().get_env_var(name)
    if var is None or not var.value:
        raise click.BadParameter(
            f"your account has no variable {name}", ctx=ctx, param=param
        )
    return var.value


def _interactive() -> bool:
    return sys.stdin.isatty()


def _secret_option(
    connector: str, flag: str, help: str, required: bool = False
) -> Callable[[Any], Any]:
    """``click.option`` for a secret-bearing option of ``connector``."""

    def callback(ctx: click.Context, param: click.Parameter, value: str | None):
        return _read_secret(ctx, param, value, required)

    text = f"{help} Pass - to read it from stdin, or account:NAME to read your account variable NAME."
    if required:
        # Click cannot flag a callback-enforced secret as required, so say it here.
        text += "  [required]"
    return click.option(
        flag,
        envvar=_env(connector, flag),
        show_envvar=True,
        callback=callback,
        help=text,
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


# region Connector specs
#
# The four commands above are written out by hand; the fourteen below differ only in
# their fields, so they are declared and built rather than copied. Each entry names the
# DTO Jackson expects on ``type``, the ``storageConnectorType`` enum value, the options,
# whose ``json`` is the DTO property (dotted for the payloads that nest), and a check
# for the rules that hold between options, mirroring the backend's create validation
# so a bad combination is refused before anything is sent.


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


_SQL_CREDENTIAL_FLAGS = ("--user", "--password", "--wallet-path", "--wallet-password")


def _check_sql(v: dict[str, Any]) -> str | None:
    if v["credentials_mode"] == "PROVIDED":
        if v["database_type"] != "ORACLE":
            return "--credentials-mode PROVIDED is supported for --database-type ORACLE only."
        return _refuses(v, "--credentials-mode PROVIDED", *_SQL_CREDENTIAL_FLAGS)
    problem = _needs(v, "--credentials-mode SHARED", "--user")
    if problem:
        return problem
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


def _check_elasticsearch(v: dict[str, Any]) -> str | None:
    auth = (v.get(_dest("--auth-type")) or "").upper()
    if not auth:
        if _given(v, "--user"):
            auth = "BASIC"
        elif _given(v, "--api-key"):
            auth = "API_KEY"
        else:
            auth = "NONE"
    if auth == "BASIC":
        return _needs(v, "basic authentication", "--user", "--password") or _refuses(
            v, "basic authentication", "--api-key"
        )
    if auth == "API_KEY":
        return _needs(v, "--auth-type API_KEY", "--api-key") or _refuses(
            v, "--auth-type API_KEY", "--user", "--password"
        )
    return _refuses(v, "--auth-type NONE", "--user", "--password", "--api-key")


def _check_hopsworks(v: dict[str, Any]) -> str | None:
    host = v.get(_dest("--host")) or ""
    if "://" in host or "/" in host:
        return "--host takes the bare host name of the remote cluster, without scheme or path; pass the port as --port."
    return None


def _check_bigquery(v: dict[str, Any]) -> str | None:
    if (
        _given(v, "--project-id")
        or _given(v, "--dataset")
        or _given(v, "--query-table")
    ):
        return None
    return _needs(
        v,
        "bigquery without a query project, dataset or table",
        "--materialization-dataset",
    )


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
    "elasticsearch": _Spec(
        "featurestoreElasticsearchConnectorDTO",
        "ELASTICSEARCH",
        "Register an Elasticsearch connector.",
        (
            _Opt(
                "--host",
                "host",
                "Elasticsearch host, without scheme or port.",
                required=True,
            ),
            _Opt("--port", "port", "HTTP port; 9200 when unset.", kind="int"),
            _Opt(
                "--scheme",
                "scheme",
                "http or https; https when unset.",
                choices=("http", "https"),
            ),
            _Opt(
                "--auth-type",
                "authType",
                "NONE, BASIC or API_KEY; when unset, BASIC with --user, API_KEY with only --api-key, else NONE.",
                choices=("NONE", "BASIC", "API_KEY"),
            ),
            _Opt("--user", "username", "User name for BASIC authentication."),
            _Opt(
                "--password",
                "password",
                "Password for BASIC authentication.",
                secret=True,
            ),
            _Opt(
                "--api-key",
                "apiKey",
                "Encoded API key for API_KEY authentication.",
                secret=True,
            ),
            _Opt(
                "--default-index",
                "defaultIndex",
                "Index, alias, data stream or pattern read when none is given.",
            ),
            _Opt(
                "--truststore-path",
                "trustStorePath",
                "JKS truststore with the cluster's CA, as a full HopsFS path.",
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
                "Verify the server certificate and hostname; verified when unset.",
                kind="bool",
            ),
            _ARGS,
        ),
        check=_check_elasticsearch,
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
                choices=(
                    "MYSQL",
                    "POSTGRESQL",
                    "ORACLE",
                    "CLICKHOUSE",
                    "TERADATA",
                    "SQLSERVER",
                ),
            ),
            _Opt(
                "--credentials-mode",
                "credentialsMode",
                "SHARED stores one set of credentials on the data source for every member; "
                "PROVIDED (ORACLE only) stores none, and each member adds their own with "
                "`hops datasource credentials set`.",
                choices=("SHARED", "PROVIDED"),
                default="SHARED",
            ),
            _Opt(
                "--host",
                "host",
                "Database host. Required, except for ORACLE with --wallet-path or "
                "--credentials-mode PROVIDED.",
            ),
            _Opt(
                "--port",
                "port",
                "Database port; the HTTP interface port (8123) for CLICKHOUSE, 1025 for TERADATA, 1433 for SQLSERVER.",
                required=True,
                kind="int",
            ),
            _Opt(
                "--database",
                "database",
                "Database name; the service name, SID or TNS alias for ORACLE.",
                required=True,
            ),
            _Opt("--user", "user", "Database user. Required with SHARED credentials."),
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
                "Spreadsheet id from its URL. Optional; a feature group can set its own.",
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
    "hopsworks": _Spec(
        "featurestoreHopsworksConnectorDTO",
        "HOPSWORKS",
        "Register a connector to a project on another Hopsworks cluster.",
        (
            _Opt(
                "--host",
                "host",
                "Host name or IP address of the remote cluster, without scheme or port.",
                required=True,
            ),
            _Opt("--port", "port", "HTTPS port; 443 when unset.", kind="int"),
            _Opt(
                "--project",
                "project",
                "Project on the remote cluster that the API key belongs to.",
                required=True,
            ),
            _Opt(
                "--api-key",
                "apiKey",
                "API key on the remote cluster with the PROJECT and FEATURESTORE scopes.",
                required=True,
                secret=True,
            ),
            _Opt(
                "--tls-verify/--no-tls-verify",
                "verify",
                "Verify the remote cluster's certificate and host name; verified when unset.",
                kind="bool",
            ),
        ),
        check=_check_hopsworks,
    ),
    "bigquery": _Spec(
        "featurestoreBigqueryConnectorDTO",
        "BIGQUERY",
        "Register a Google BigQuery connector.",
        (
            _Opt(
                "--parent-project",
                "parentProject",
                "GCP project billed for the query.",
                required=True,
            ),
            _Opt("--key-path", "keyPath", _KEY_PATH_HELP, required=True),
            _Opt("--project-id", "queryProject", "GCP project of the table to read."),
            _Opt("--dataset", "dataset", "Dataset that contains the table."),
            _Opt("--query-table", "queryTable", "Default table to read."),
            _Opt(
                "--materialization-dataset",
                "materializationDataset",
                "Dataset that materialized query results are written to. "
                "Required when no project, dataset or table is given.",
            ),
            _ARGS,
        ),
        check=_check_bigquery,
    ),
    "sharepoint": _Spec(
        "featurestoreSharePointConnectorDTO",
        "SHAREPOINT",
        "Register a SharePoint Online site, to copy or sync its library folders into datasets.",
        (
            _Opt(
                "--tenant-id",
                "tenantId",
                "Directory (tenant) id of the Microsoft Entra ID tenant.",
                required=True,
            ),
            _Opt(
                "--client-id",
                "clientId",
                "Application (client) id of the Entra ID app registration.",
                required=True,
            ),
            _Opt(
                "--client-secret",
                "clientSecret",
                "Client secret of the app registration.",
                required=True,
                secret=True,
            ),
            _Opt(
                "--site-url",
                "siteUrl",
                "Site URL, e.g. https://contoso.sharepoint.com/sites/Finance.",
                required=True,
            ),
            _Opt(
                "--site-id",
                "siteId",
                "Graph site id, for an app granted Sites.Selected that cannot look the site up by URL.",
            ),
            _Opt(
                "--cloud",
                "cloud",
                "Microsoft cloud of the tenant; GLOBAL when unset.",
                choices=("GLOBAL", "US_GOV", "CHINA"),
            ),
        ),
        epilog="Copy or sync folders with `hops datasource sharepoint`.",
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
        kwargs["type"] = click.Choice(opt.choices, case_sensitive=False)
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
        ctx: click.Context,
        connector_name: str,
        description: str,
        test: bool,
        **values: Any,
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
        if test:
            _test_connector(ctx, body)
        else:
            _create_connector(ctx, body)

    # Click renders a command's docstring as its help, so the docstring is the
    # summary alone; the options carry their own help and the parameters are
    # generated, which leaves nothing for an Args section to say.
    run.__doc__ = spec.summary
    cmd = click.pass_context(run)
    cmd = click.option("--description", default="", help="Free-form description.")(cmd)
    cmd = click.option(
        "--test",
        is_flag=True,
        help="Only test the connection; create nothing. Exits 1 when it fails.",
    )(cmd)
    for opt in reversed(spec.opts):
        cmd = _option(opt, name)(cmd)
    cmd = click.argument("connector_name", metavar="NAME")(cmd)
    return click.command(name, short_help=spec.summary, epilog=spec.epilog or None)(cmd)


for _name, _spec in _SPECS.items():
    connector_create.add_command(_build_create_command(_name, _spec))

# endregion


# region Member credentials
#
# A data source created with ``--credentials-mode PROVIDED`` stores no credentials.
# Each member stores their own in their account and binds them to the data source,
# so every command here acts on the caller's binding only.


@datasource_group.group("credentials")
def connector_credentials() -> None:
    """Your own credentials for a data source with provided credentials."""


_CREDENTIAL_PAIRS = (
    ("--user", "--user-env-var"),
    ("--password", "--password-secret"),
    ("--wallet-password", "--wallet-password-secret"),
)


def _credential_options(command: Callable[..., Any]) -> Callable[..., Any]:
    """The options ``set`` and ``validate`` share: each credential by value or by account entry name."""
    options = (
        click.option(
            "--user", help="Database username; saved as an env var in your account."
        ),
        click.option(
            "--user-env-var",
            help="Name of an env var in your account holding the username, or to save it under. "
            "Default: the one already bound when --user is not given, else "
            "DS_<NAME>_<feature store id>_USER.",
        ),
        _secret_option(
            "credentials",
            "--password",
            "Database password; saved as a secret in your account.",
        ),
        click.option(
            "--password-secret",
            help="Name of a secret in your account holding the password, or to save it under. "
            "Default: the one already bound when --password is not given, else "
            "ds_<name>_<feature store id>_password.",
        ),
        click.option(
            "--wallet",
            type=click.Path(exists=True, dir_okay=False),
            help="Local Oracle wallet zip; uploaded as a new .datasources/<name>/wallet-<id>.zip "
            "in your home directory in the project, so the wallet you already use stays "
            "untouched until the new one is saved. Without it, the wallet already bound is "
            "kept.",
        ),
        _secret_option(
            "credentials",
            "--wallet-password",
            "Wallet password; saved as a secret in your account.",
        ),
        click.option(
            "--wallet-password-secret",
            help="Name of a secret in your account holding the wallet password, or to save it "
            "under. Default: the one already bound when --wallet-password is not given, else "
            "ds_<name>_<feature store id>_wallet_password.",
        ),
    )
    for option in reversed(options):
        command = option(command)
    return command


def _credential_arguments(
    ctx: click.Context, sc: Any, values: dict[str, Any]
) -> dict[str, Any]:
    """Turn the options into ``set_credentials`` keyword arguments, prompting for a missing password.

    An option not given keeps what is already bound, which ``set_credentials`` fills in.
    """
    for value_flag, name_flag in _CREDENTIAL_PAIRS:
        if _given(values, value_flag) and _given(values, name_flag):
            raise click.UsageError(
                f"{value_flag} and {name_flag} are alternatives; pass one.", ctx
            )
    bound = getattr(sc, "user_credentials", None) or {}
    if not _given(values, "--password") and not _given(values, "--password-secret"):
        if _interactive():
            values["password"] = click.prompt("password", hide_input=True)
        elif not bound.get("password_secret_name"):
            raise click.UsageError(
                "Pass --password (- reads it from stdin), --password-secret, or export "
                f"${_env('credentials', '--password')}.",
                ctx,
            )
    if (
        not _given(values, "--wallet")
        and not bound.get("wallet_path")
        and (
            _given(values, "--wallet-password")
            or _given(values, "--wallet-password-secret")
        )
    ):
        raise click.UsageError(
            "A wallet password needs --wallet, or a wallet already bound.", ctx
        )
    return {
        key: values.get(key) or None
        for key in (
            "user",
            "password",
            "wallet_password",
            "user_env_var",
            "password_secret",
            "wallet_password_secret",
        )
    }


def _provided_connector(ctx: click.Context, name: str) -> Any:
    fs = session.get_feature_store(ctx)
    try:
        ds = fs.get_data_source(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Data source '{name}' not found: {exc}") from exc
    sc = getattr(ds, "storage_connector", ds)
    if not hasattr(sc, "set_credentials"):
        raise click.ClickException(
            f"Data source '{name}' does not take member credentials."
        )
    if getattr(sc, "credentials_mode", None) == "SHARED":
        raise click.ClickException(
            f"Data source '{name}' has shared credentials; its owner sets them on the "
            "data source itself."
        )
    return sc


def _upload_wallet(ctx: click.Context, sc: Any, local_zip: str) -> tuple[str, str]:
    """Stage the wallet under a new name in the caller's own home, where the backend accepts it.

    Returns:
        The HopsFS path to bind and the project-relative path to remove it by.
    """
    import hopsworks

    project = session.get_project(ctx)
    try:
        username = hopsworks.get_users_api()._get_current_user().username
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not resolve your username: {exc}") from exc
    target_dir = f"Users/{username}/.datasources/{sc._env_name().lower()}"
    file_name = f"wallet-{uuid.uuid4().hex}.zip"
    dataset_api = project.get_dataset_api()
    try:
        for directory in (os.path.dirname(target_dir), target_dir):
            if not dataset_api.exists(directory):
                dataset_api.mkdir(directory)
        with tempfile.TemporaryDirectory() as tmp:
            staged = os.path.join(tmp, file_name)
            shutil.copyfile(local_zip, staged)
            dataset_api.upload(staged, target_dir)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Wallet upload failed: {exc}") from exc
    relative = f"{target_dir}/{file_name}"
    output.info("Uploaded wallet to %s", relative)
    return f"/Projects/{project.name}/{relative}", relative


def _discard_wallet(ctx: click.Context, relative: str | None) -> None:
    if not relative:
        return
    try:
        session.get_project(ctx).get_dataset_api().remove(relative)
    except Exception as exc:  # noqa: BLE001
        output.warn("Could not remove the uploaded wallet %s: %s", relative, exc)


def _validate(sc: Any, name: str, args: dict[str, Any]) -> dict[str, Any]:
    """The backend's verdict on the credentials, as a click error when they are rejected."""
    try:
        result = sc._validate_credentials(**args)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Validation failed: {exc}") from exc
    if not result.get("valid"):
        code = result.get("errorCode")
        detail = result.get("message") or "the data source rejected the credentials"
        raise click.ClickException(
            f"Data source '{name}' rejected the credentials"
            + (f" ({code})" if code else "")
            + f": {detail}"
        )
    return result


def _binding_rows(binding: dict[str, Any]) -> list[list[Any]]:
    return [
        ["Status", binding.get("status", "?")],
        ["Username env var", binding.get("username_env_var") or "-"],
        ["Password secret", binding.get("password_secret_name") or "-"],
        ["Wallet path", binding.get("wallet_path") or "-"],
        ["Wallet password secret", binding.get("wallet_password_secret_name") or "-"],
        ["Validated at", output.format_ts(binding.get("validated_at"))],
    ]


# D417 asks for an Args entry for **values, which docsig (ignore_kwargs) refuses.
@connector_credentials.command("set")
@click.argument("name")
@_credential_options
@click.pass_context
def credentials_set(ctx: click.Context, name: str, **values: Any) -> None:  # noqa: D417
    """Save your credentials for a data source to your account, once the data source accepts them.

    Nothing is saved when the data source rejects them.
    An option not given keeps what is already bound, so `--password` alone rotates the password and keeps the username and wallet.
    Each credential option is given by value or by account entry name.

    Args:
        ctx: Click context.
        name: Data source name.
    """
    sc = _provided_connector(ctx, name)
    args = _credential_arguments(ctx, sc, values)
    staged = None
    if values.get("wallet"):
        args["wallet_path"], staged = _upload_wallet(ctx, sc, values["wallet"])
    # Once saved, the backend owns the staged wallet and removes the one it replaced.
    try:
        binding = sc.set_credentials(**args)
    except Exception as exc:  # noqa: BLE001
        _discard_wallet(ctx, staged)
        raise click.ClickException(f"Could not save the credentials: {exc}") from exc
    except BaseException:
        _discard_wallet(ctx, staged)
        raise
    output.success("✓ Saved your credentials for %s", name)
    if output.JSON_MODE:
        output.print_json(binding)
    else:
        output.print_table(["FIELD", "VALUE"], _binding_rows(binding))


@connector_credentials.command("validate")
@click.argument("name")
@_credential_options
@click.pass_context
def credentials_validate(ctx: click.Context, name: str, **values: Any) -> None:  # noqa: D417
    """Test your credentials against a data source without saving them.

    A wallet is uploaded under a new name for the test, which reads it from HopsFS,
    and removed again afterwards.
    Each credential option is given by value or by account entry name.

    Args:
        ctx: Click context.
        name: Data source name.
    """
    sc = _provided_connector(ctx, name)
    args = _credential_arguments(ctx, sc, values)
    staged = None
    if values.get("wallet"):
        args["wallet_path"], staged = _upload_wallet(ctx, sc, values["wallet"])
    try:
        result = _validate(sc, name, args)
    finally:
        _discard_wallet(ctx, staged)
    if output.JSON_MODE:
        output.print_json(result)
        return
    output.success("✓ Data source %s accepted the credentials", name)


@connector_credentials.command("show")
@click.argument("name")
@click.pass_context
def credentials_show(ctx: click.Context, name: str) -> None:
    """Show your binding for a data source: status and account entry names, never values.

    Args:
        ctx: Click context.
        name: Data source name.
    """
    sc = _provided_connector(ctx, name)
    try:
        binding = sc.get_credentials()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not read your credentials: {exc}") from exc
    if output.JSON_MODE:
        output.print_json(binding)
        return
    output.print_table(["FIELD", "VALUE"], _binding_rows(binding))


@connector_credentials.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def credentials_delete(ctx: click.Context, name: str, yes: bool) -> None:
    """Remove your binding for a data source, its wallet file and its default-named account entries.

    The default env var and secrets (DS_<NAME>_<feature store id>_USER, ds_<name>_<feature store id>_password and ds_<name>_<feature store id>_wallet_password) are deleted from your account; ones you named yourself stay.

    Args:
        ctx: Click context.
        name: Data source name.
        yes: Skip confirmation when True.
    """
    sc = _provided_connector(ctx, name)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Remove your credentials for '{name}'?", abort=True)
    try:
        sc.delete_credentials()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not remove the credentials: {exc}") from exc
    output.success("✓ Removed your credentials for %s", name)


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
    if inferred.suggested_description:
        click.echo(f"Suggested description: {inferred.suggested_description}")


# region SharePoint
#
# A SharePoint data source is not read as tables: its syncs copy library folders
# into datasets, so it gets its own command group.


def _sharepoint(ctx: click.Context, name: str) -> Any:
    fs = session.get_feature_store(ctx)
    try:
        sc = fs.get_data_source(name).storage_connector
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Data source '{name}' not found: {exc}") from exc
    if sc is None:
        raise click.ClickException(f"Data source '{name}' not found.")
    if getattr(sc, "type", None) != "SHAREPOINT":
        raise click.ClickException(
            f"Data source '{name}' is {getattr(sc, 'type', None)}, not SHAREPOINT."
        )
    return sc


def _sync(ctx: click.Context, name: str, sync_id: int) -> Any:
    sc = _sharepoint(ctx, name)
    try:
        return sc.get_sync(sync_id)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Sync {sync_id} of '{name}': {exc}") from exc


def _sync_rows(syncs: list[Any]) -> list[list[Any]]:
    return [
        [
            s.id,
            s.dataset_name,
            f"{s.drive_name or s.drive_id}/{s.folder_path or ''}",
            (s.cron_expression or "once") + (" (paused)" if s.paused else ""),
            s.state,
            output.format_ts(s.last_run_finished),
            s.files_total,
            s.bytes_total,
            s.index_feature_group or "-",
        ]
        for s in syncs
    ]


_SYNC_HEADERS = [
    "ID",
    "DATASET",
    "SOURCE",
    "SCHEDULE",
    "STATE",
    "LAST RUN",
    "FILES",
    "BYTES",
    "INDEX",
]


def _print_sync(sync: Any) -> None:
    if output.JSON_MODE:
        output.print_json(sync.to_dict())
        return
    output.print_table(_SYNC_HEADERS, _sync_rows([sync]))


def _call(what: str, fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except click.ClickException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"{what} failed: {exc}") from exc


@datasource_group.group("sharepoint")
def sharepoint_group() -> None:
    """Browse a SharePoint data source and copy or sync its folders into datasets."""


@sharepoint_group.command("drives")
@click.argument("name")
@click.pass_context
def sharepoint_drives(ctx: click.Context, name: str) -> None:
    """List the document libraries of the site.

    Args:
        ctx: Click context.
        name: Data source name.
    """
    sc = _sharepoint(ctx, name)
    drives = _call("Listing libraries", sc.list_drives)
    output.print_table(
        ["ID", "NAME", "WEB URL"],
        [[d.get("id"), d.get("name"), d.get("web_url", "-")] for d in drives],
    )


@sharepoint_group.command("folders")
@click.argument("name")
@click.option("--drive", "drive_id", required=True, help="Library id, from `drives`.")
@click.option(
    "--path", default="", help="Folder inside the library; the root when unset."
)
@click.pass_context
def sharepoint_folders(ctx: click.Context, name: str, drive_id: str, path: str) -> None:
    """List the child folders of a folder in a document library.

    Args:
        ctx: Click context.
        name: Data source name.
        drive_id: Library id.
        path: Folder path inside the library.
    """
    sc = _sharepoint(ctx, name)
    folders = _call("Listing folders", lambda: sc.list_folders(drive_id, path))
    output.print_table(["ID", "NAME"], [[f.get("id"), f.get("name")] for f in folders])


@sharepoint_group.command("copy")
@click.argument("name")
@click.option("--drive", "drive_id", required=True, help="Library id, from `drives`.")
@click.option(
    "--folder", default="", help="Folder inside the library; the root when unset."
)
@click.option(
    "--dataset", default="sharepoint", show_default=True, help="Dataset to create."
)
@click.option("--alert-receiver", help="Alert receiver notified when the copy fails.")
@click.option(
    "--index-files",
    is_flag=True,
    help="Index the files in a Delta feature group, one row per file, partitioned by first ingestion date.",
)
@click.pass_context
def sharepoint_copy(
    ctx: click.Context,
    name: str,
    drive_id: str,
    folder: str,
    dataset: str,
    alert_receiver: str | None,
    index_files: bool,
) -> None:
    """Copy a library folder into a new dataset once, now, without a schedule.

    Args:
        ctx: Click context.
        name: Data source name.
        drive_id: Library id.
        folder: Folder path inside the library.
        dataset: Dataset to create.
        alert_receiver: Alert receiver for failed runs.
        index_files: Whether to index the files in a feature group.
    """
    sc = _sharepoint(ctx, name)
    sync = _call(
        "Copy",
        lambda: sc.copy_folder(
            drive_id=drive_id,
            folder_path=folder,
            dataset_name=dataset,
            alert_receiver=alert_receiver,
            index_files=index_files,
        ),
    )
    _print_sync(sync)
    output.success("✓ Copying into dataset %s (sync %s)", sync.dataset_name, sync.id)


@sharepoint_group.command("reset-script")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def sharepoint_reset_script(ctx: click.Context, name: str, yes: bool) -> None:
    """Overwrite the data source's sync script with the platform template.

    Args:
        ctx: Click context.
        name: Data source name.
        yes: Skip confirmation when True.
    """
    sc = _sharepoint(ctx, name)
    if not yes and not output.JSON_MODE:
        click.confirm(
            f"Overwrite {sc.script_path or 'the sync script'}, discarding its edits?",
            abort=True,
        )
    _call("Reset", sc.reset_script)
    output.success("✓ Reset %s", sc.script_path or "the sync script")


@sharepoint_group.group("sync")
def sharepoint_sync_group() -> None:
    """Syncs that copy a library folder into a dataset, once or on a schedule."""


@sharepoint_sync_group.command("create")
@click.argument("name")
@click.option("--drive", "drive_id", required=True, help="Library id, from `drives`.")
@click.option(
    "--folder", default="", help="Folder inside the library; the root when unset."
)
@click.option(
    "--dataset", default="sharepoint", show_default=True, help="Dataset to create."
)
@click.option(
    "--cron",
    default="0 0 * ? * *",
    show_default=True,
    help="Schedule, a Quartz cron expression in UTC, seconds first, e.g. '0 0 2 ? * *' for daily at 02:00.",
)
@click.option(
    "--include",
    multiple=True,
    help="rclone pattern of files to copy. Repeat for several.",
)
@click.option(
    "--exclude",
    multiple=True,
    help="rclone pattern of files to skip. Repeat for several.",
)
@click.option(
    "--max-file-size-mb",
    type=int,
    default=1024,
    show_default=True,
    help="Skip larger files; 0 for no limit.",
)
@click.option(
    "--mirror-deletions/--no-mirror-deletions",
    default=True,
    show_default=True,
    help="Delete files from the dataset that are not in the folder.",
)
@click.option("--alert-receiver", help="Alert receiver notified when a run fails.")
@click.option(
    "--run-now/--no-run-now",
    default=True,
    show_default=True,
    help="Start the first run now.",
)
@click.option(
    "--index-files",
    is_flag=True,
    help="Index the files in a Delta feature group, one row per file, partitioned by first ingestion date.",
)
@click.option(
    "--index-feature-group",
    help="Name of the index feature group; sharepoint_<dataset> when unset. Implies --index-files.",
)
@click.pass_context
def sharepoint_sync_create(
    ctx: click.Context,
    name: str,
    drive_id: str,
    folder: str,
    dataset: str,
    cron: str,
    include: tuple[str, ...],
    exclude: tuple[str, ...],
    max_file_size_mb: int,
    mirror_deletions: bool,
    alert_receiver: str | None,
    run_now: bool,
    index_files: bool,
    index_feature_group: str | None,
) -> None:
    """Create a scheduled sync of a library folder into a new dataset.

    Args:
        ctx: Click context.
        name: Data source name.
        drive_id: Library id.
        folder: Folder path inside the library.
        dataset: Dataset to create.
        cron: Cron expression of the schedule.
        include: Include patterns.
        exclude: Exclude patterns.
        max_file_size_mb: File size cap in MB.
        mirror_deletions: Whether deletions are mirrored.
        alert_receiver: Alert receiver for failed runs.
        run_now: Whether to start the first run now.
        index_files: Whether to index the files in a feature group.
        index_feature_group: Name of the index feature group.
    """
    sc = _sharepoint(ctx, name)
    sync = _call(
        "Create",
        lambda: sc.create_sync(
            drive_id=drive_id,
            folder_path=folder,
            dataset_name=dataset,
            schedule=cron,
            include=list(include),
            exclude=list(exclude),
            max_file_size_mb=max_file_size_mb,
            mirror_deletions=mirror_deletions,
            alert_receiver=alert_receiver,
            run_now=run_now,
            index_files=index_files,
            index_feature_group=index_feature_group,
        ),
    )
    _print_sync(sync)
    output.success("✓ Created sync %s into dataset %s", sync.id, sync.dataset_name)


@sharepoint_sync_group.command("list")
@click.argument("name")
@click.pass_context
def sharepoint_sync_list(ctx: click.Context, name: str) -> None:
    """List the syncs of a SharePoint data source.

    Args:
        ctx: Click context.
        name: Data source name.
    """
    sc = _sharepoint(ctx, name)
    syncs = _call("Listing syncs", sc.get_syncs)
    if output.JSON_MODE:
        output.print_json([s.to_dict() for s in syncs])
        return
    output.print_table(_SYNC_HEADERS, _sync_rows(syncs))


@sharepoint_sync_group.command("status")
@click.argument("name")
@click.argument("sync_id", type=int)
@click.pass_context
def sharepoint_sync_status(ctx: click.Context, name: str, sync_id: int) -> None:
    """Show the run state and counters of a sync.

    Args:
        ctx: Click context.
        name: Data source name.
        sync_id: Sync id.
    """
    sync = _sync(ctx, name, sync_id)
    status = _call("Status", sync.status)
    if output.JSON_MODE:
        output.print_json(status)
        return
    output.print_table(
        ["FIELD", "VALUE"],
        [[k, "-" if v is None else v] for k, v in status.items()]
        + [["audit_path", sync.audit_path or "-"]]
        + [["index_feature_group", sync.index_feature_group or "-"]],
    )


def _sync_action(command: str, help: str, act: Callable[[Any], Any], done: str) -> None:
    @sharepoint_sync_group.command(command, help=help)
    @click.argument("name")
    @click.argument("sync_id", type=int)
    @click.pass_context
    def run(ctx: click.Context, name: str, sync_id: int) -> None:
        sync = _sync(ctx, name, sync_id)
        result = _call(command.capitalize(), lambda: act(sync))
        if output.JSON_MODE:
            output.print_json(_action_json(result))
            return
        output.success(done, sync_id)


def _action_json(result: Any) -> Any:
    to_dict = getattr(result, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    if result is None or isinstance(result, dict):
        return result
    return job_cmd._execution_to_dict(result)


_sync_action(
    "run",
    "Start a run of the sync now, also while paused or unscheduled.",
    lambda s: s.run(),
    "✓ Started sync %s",
)
_sync_action(
    "pause", "Disable the sync's schedule.", lambda s: s.pause(), "✓ Paused sync %s"
)
_sync_action(
    "resume",
    "Re-enable the sync's schedule.",
    lambda s: s.resume(),
    "✓ Resumed sync %s",
)
_sync_action(
    "resync",
    "Run the sync now comparing file contents by checksum.",
    lambda s: s.force_full_resync(),
    "✓ Started a full resync of sync %s",
)


@sharepoint_sync_group.command("schedule")
@click.argument("name")
@click.argument("sync_id", type=int)
@click.option(
    "--cron",
    help="New schedule, a Quartz cron expression in UTC, seconds first, e.g. '0 0 2 ? * *' for daily at 02:00.",
)
@click.option(
    "--none",
    "remove",
    is_flag=True,
    help="Remove the schedule; the sync then runs only when started.",
)
@click.pass_context
def sharepoint_sync_schedule(
    ctx: click.Context, name: str, sync_id: int, cron: str | None, remove: bool
) -> None:
    """Set or remove the schedule of a sync.

    Args:
        ctx: Click context.
        name: Data source name.
        sync_id: Sync id.
        cron: New cron expression.
        remove: Remove the schedule.
    """
    if bool(cron) == remove:
        raise click.UsageError("Pass exactly one of --cron and --none.", ctx)
    sync = _sync(ctx, name, sync_id)
    _call("Schedule", lambda: sync.set_schedule(None if remove else cron))
    _print_sync(sync)


@sharepoint_sync_group.command("set-folder")
@click.argument("name")
@click.argument("sync_id", type=int)
@click.option(
    "--folder", required=True, help='Folder inside the same library; "" for the root.'
)
@click.pass_context
def sharepoint_sync_set_folder(
    ctx: click.Context, name: str, sync_id: int, folder: str
) -> None:
    """Point a sync at another folder of its library; the next run reconciles the dataset.

    Args:
        ctx: Click context.
        name: Data source name.
        sync_id: Sync id.
        folder: New folder path.
    """
    sync = _sync(ctx, name, sync_id)
    _call("Set folder", lambda: sync.set_folder(folder))
    _print_sync(sync)


@sharepoint_sync_group.command("index")
@click.argument("name")
@click.argument("sync_id", type=int)
@click.option(
    "--on/--off",
    "enabled",
    required=True,
    help="Start or stop indexing; --off keeps the feature group.",
)
@click.option(
    "--feature-group",
    help="Name of the index feature group; sharepoint_<dataset> when unset.",
)
@click.pass_context
def sharepoint_sync_index(
    ctx: click.Context,
    name: str,
    sync_id: int,
    enabled: bool,
    feature_group: str | None,
) -> None:
    """Turn indexing of a sync's files in a feature group on or off; the next run brings it up to date.

    Args:
        ctx: Click context.
        name: Data source name.
        sync_id: Sync id.
        enabled: Whether to index.
        feature_group: Name of the index feature group.
    """
    sync = _sync(ctx, name, sync_id)
    fields: dict[str, Any] = {"index_files": enabled}
    if enabled and feature_group:
        fields["index_feature_group"] = feature_group
    _call("Index", lambda: sync.update(**fields))
    _print_sync(sync)


@sharepoint_sync_group.command("delete")
@click.argument("name")
@click.argument("sync_id", type=int)
@click.option(
    "--keep-files",
    is_flag=True,
    help="Keep the dataset, audit log and index feature group.",
)
@click.option(
    "--force", is_flag=True, help="Stop a run in progress instead of refusing."
)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def sharepoint_sync_delete(
    ctx: click.Context,
    name: str,
    sync_id: int,
    keep_files: bool,
    force: bool,
    yes: bool,
) -> None:
    """Delete a sync and its job, and its dataset unless --keep-files.

    Args:
        ctx: Click context.
        name: Data source name.
        sync_id: Sync id.
        keep_files: Keep the dataset.
        force: Stop a run in progress.
        yes: Skip confirmation when True.
    """
    sync = _sync(ctx, name, sync_id)
    if not yes and not output.JSON_MODE:
        what = "keeping" if keep_files else "and DELETE"
        index = (
            f" and index feature group '{sync.index_feature_group}'"
            if sync.index_feature_group
            else ""
        )
        click.confirm(
            f"Delete sync {sync_id} {what} dataset '{sync.dataset_name}'{index}?",
            abort=True,
        )
    deleted = _call("Delete", lambda: sync.delete(keep_files=keep_files, force=force))
    if output.JSON_MODE:
        output.print_json({"id": sync_id, "deleted": deleted})
        return
    if deleted:
        output.success("✓ Deleted sync %s", sync_id)
        return
    output.warn(
        "Sync %s is still DELETING: its run did not stop in time.\n"
        "Run the delete again to finish it: hops datasource sharepoint sync delete %s %s%s --force --yes",
        sync_id,
        name,
        sync_id,
        " --keep-files" if keep_files else "",
    )


@sharepoint_sync_group.command("audit")
@click.argument("name")
@click.argument("sync_id", type=int)
@click.option("--day", help="Day in UTC as YYYY-MM-DD; today when unset.")
@click.pass_context
def sharepoint_sync_audit(
    ctx: click.Context, name: str, sync_id: int, day: str | None
) -> None:
    """Show the paths a sync's runs changed on one day.

    Args:
        ctx: Click context.
        name: Data source name.
        sync_id: Sync id.
        day: Day in UTC.
    """
    sync = _sync(ctx, name, sync_id)
    rows = _call("Audit", lambda: sync.get_audit(day=day))
    output.print_table(
        ["DATE", "EXECUTION", "OP", "KIND", "PATH", "SRC_PATH"],
        [
            [
                r.get("date"),
                r.get("execution_id"),
                r.get("op"),
                r.get("kind"),
                r.get("path"),
                r.get("src_path") or "",
            ]
            for r in rows
        ],
    )


# endregion


def _test_connector(ctx: click.Context, body: dict[str, Any]) -> None:
    fs = session.get_feature_store(ctx)
    from hopsworks_common.core import rest

    try:
        result = rest._send_request(
            "POST",
            rest._project_path(
                "featurestores",
                getattr(fs, "id", None),
                "storageconnectors",
                "testConnection",
            ),
            json_body=body,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Connection test failed: {exc}") from exc
    result = result if isinstance(result, dict) else {}
    message = (result.get("connectionOutput") or "").strip()
    # The checkers report success as exit code 0, or 200 for HTTP sources.
    if result.get("statusCode") not in (0, 200):
        raise click.ClickException(f"Connection test failed: {message or 'no output'}")
    output.success(
        "✓ Connection to %s works%s", body["name"], f": {message}" if message else ""
    )


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

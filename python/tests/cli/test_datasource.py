"""``hops datasource create`` builds the payload each connector type expects.

The bodies are asserted rather than sent: the generated commands differ only in
their fields, so what is worth pinning is the discriminator, the property names,
the shapes that are not flat (REST nests, CRM is discriminated a second time by
``crmType``), and the rules between options that the backend would otherwise
reject after the request.
"""

from __future__ import annotations

import json
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import output
from hopsworks.cli.commands import datasource as ds
from hopsworks.cli.main import cli


# The smallest argv the backend accepts for each type, after the connector name.
_MINIMAL: dict[str, list[str]] = {
    "hopsfs": ["--dataset", "Resources"],
    "jdbc": ["--url", "jdbc:mysql://h/db"],
    "redshift": [
        "--cluster-identifier",
        "c",
        "--endpoint",
        "e",
        "--database",
        "d",
        "--port",
        "5439",
        "--user",
        "u",
    ],
    "s3": ["--bucket", "b"],
    "adls": [
        "--account-name",
        "a",
        "--generation",
        "2",
        "--container-name",
        "c",
        "--directory-id",
        "d",
        "--application-id",
        "app",
        "--service-credential",
        "s",
    ],
    "snowflake": [
        "--url",
        "u",
        "--user",
        "u",
        "--password",
        "p",
        "--database",
        "d",
        "--schema",
        "s",
        "--warehouse",
        "w",
    ],
    "kafka": ["--bootstrap-servers", "b:9092", "--security-protocol", "PLAINTEXT"],
    "gcs": ["--bucket", "b", "--key-path", "/Projects/p/Resources/k.json"],
    "bigquery": [
        "--parent-project",
        "p",
        "--key-path",
        "/Projects/p/k.json",
        "--materialization-dataset",
        "m",
    ],
    "opensearch": ["--host", "h", "--port", "9200"],
    "elasticsearch": ["--host", "h"],
    "sql": [
        "--database-type",
        "MYSQL",
        "--host",
        "h",
        "--port",
        "3306",
        "--database",
        "d",
        "--user",
        "u",
    ],
    "crm": ["--crm-type", "HUBSPOT", "--api-key", "k"],
    "rest": ["--base-url", "https://api.example.com", "--auth-type", "NONE"],
    "sap-hana": ["--host", "h", "--user", "u", "--password", "p"],
    "unity-catalog": ["--workspace-url", "https://w", "--access-token", "t"],
    "mongodb": ["--connection-string", "mongodb://h", "--database", "d"],
    "hopsworks": ["--host", "h", "--project", "p", "--api-key", "k"],
    "glue": ["--database", "d", "--region", "eu-north-1"],
    "google-sheets": ["--key-path", "/Projects/p/k.json"],
    "sharepoint": [
        "--tenant-id",
        "t",
        "--client-id",
        "c",
        "--client-secret",
        "s",
        "--site-url",
        "https://contoso.sharepoint.com/sites/Finance",
    ],
}

# The Jackson subtype name and the FeaturestoreConnectorType each command sends.
_DTOS: dict[str, tuple[str, str]] = {
    "hopsfs": ("featurestoreHopsfsConnectorDTO", "HOPSFS"),
    "jdbc": ("featurestoreJdbcConnectorDTO", "JDBC"),
    "redshift": ("featurestoreRedshiftConnectorDTO", "REDSHIFT"),
    "s3": ("featurestoreS3ConnectorDTO", "S3"),
    "adls": ("featurestoreADLSConnectorDTO", "ADLS"),
    "snowflake": ("featurestoreSnowflakeConnectorDTO", "SNOWFLAKE"),
    "kafka": ("featureStoreKafkaConnectorDTO", "KAFKA"),
    "gcs": ("featureStoreGcsConnectorDTO", "GCS"),
    "bigquery": ("featurestoreBigqueryConnectorDTO", "BIGQUERY"),
    "opensearch": ("featurestoreOpensearchConnectorDTO", "OPENSEARCH"),
    "elasticsearch": ("featurestoreElasticsearchConnectorDTO", "ELASTICSEARCH"),
    "sql": ("featurestoreSqlConnectorDTO", "SQL"),
    "crm": ("featurestoreCRMConnectorDTO", "CRM"),
    "rest": ("featurestoreRESTConnectorDTO", "REST"),
    "sap-hana": ("featureStoreSapHanaConnectorDTO", "SAP_HANA"),
    "unity-catalog": ("featurestoreUnityCatalogConnectorDTO", "UNITY_CATALOG"),
    "mongodb": ("featurestoreMongoConnectorDTO", "MONGODB"),
    "hopsworks": ("featurestoreHopsworksConnectorDTO", "HOPSWORKS"),
    "glue": ("featurestoreGlueConnectorDTO", "GLUE"),
    "google-sheets": ("featurestoreGoogleSheetsConnectorDTO", "GOOGLE_SHEETS"),
    "sharepoint": ("featurestoreSharePointConnectorDTO", "SHAREPOINT"),
}


def _invoke(argv: list[str], env: dict[str, str] | None = None):
    """Run ``datasource create ...`` with the request mocked; return (result, mock)."""
    with mock.patch.object(ds, "_create_connector") as create:
        result = CliRunner().invoke(cli, ["datasource", "create", *argv], env=env)
    return result, create


def _create(argv: list[str], env: dict[str, str] | None = None) -> dict:
    """Run ``datasource create ...`` and return the body it would have posted."""
    result, create = _invoke(argv, env)
    assert result.exit_code == 0, result.output
    return create.call_args.args[1]


def _refused(argv: list[str]) -> str:
    """Run ``datasource create ...`` expecting a usage error; return its output."""
    result, create = _invoke(argv)
    assert result.exit_code == 2, result.output
    create.assert_not_called()
    return result.output


def _without(argv: list[str], flag: str) -> list[str]:
    at = argv.index(flag)
    return argv[:at] + argv[at + 2 :]


def test_every_backend_connector_type_has_a_subcommand():
    """FeaturestoreConnectorType in hopsworks-ee, as the UI offers them."""
    assert set(ds.connector_create.commands) == set(_MINIMAL) == set(_DTOS)


@pytest.mark.parametrize("kind", sorted(_MINIMAL))
def test_the_payload_names_the_dto_and_the_connector_type(kind):
    body = _create([kind, "n", *_MINIMAL[kind]])

    assert (body["type"], body["storageConnectorType"]) == _DTOS[kind]
    assert body["name"] == "n"


@pytest.mark.parametrize(
    ("kind", "flag"),
    [(kind, flag) for kind, argv in _MINIMAL.items() for flag in argv[::2]],
)
def test_each_option_of_the_minimal_command_is_needed(kind, flag):
    """Every required value is refused locally, under this Click, before any request."""
    output = _refused([kind, "n", *_without(_MINIMAL[kind], flag)])

    assert flag in output


def test_rest_nests_the_auth_and_client_config():
    body = _create(
        [
            "rest",
            "weather",
            "--base-url",
            "https://api.example.com",
            "--auth-type",
            "API_KEY",
            "--api-key",
            "secret",
        ]
    )

    assert body["clientConfig"] == {"baseUrl": "https://api.example.com"}
    assert body["authConfig"] == {"authType": "API_KEY", "apiKey": "secret"}


@pytest.mark.parametrize(
    ("auth_type", "missing"),
    [
        ("API_KEY", "--api-key"),
        ("BEARER_TOKEN", "--bearer-token"),
        ("HTTP_BASIC", "--password"),
        ("OAUTH2_CLIENT", "--client-secret"),
    ],
)
def test_rest_requires_the_credentials_of_its_auth_type(auth_type, missing):
    given = {
        "HTTP_BASIC": ["--user", "u"],
        "OAUTH2_CLIENT": ["--access-token-url", "https://t", "--client-id", "c"],
    }.get(auth_type, [])
    output = _refused(
        ["rest", "n", "--base-url", "https://a", "--auth-type", auth_type, *given]
    )

    assert missing in output


def test_crm_carries_the_second_discriminator_and_its_own_fields():
    body = _create(
        [
            "crm",
            "sf",
            "--crm-type",
            "SALESFORCE",
            "--api-key",
            "k",
            "--user",
            "u",
            "--password",
            "p",
        ]
    )

    assert body["crmType"] == "SALESFORCE"
    assert (body["username"], body["password"], body["apiKey"]) == ("u", "p", "k")


def test_crm_google_analytics_needs_a_key_file_and_a_property_but_no_api_key():
    ok = ["crm", "n", "--crm-type", "GOOGLE_ANALYTICS", "--key-path", "/P/k.json"]

    assert "propertyId" in _create([*ok, "--property-id", "42"])
    assert "--property-id" in _refused(ok)


def test_crm_shopify_needs_its_shop_and_app_password():
    output = _refused(["crm", "n", "--crm-type", "SHOPIFY", "--shop-url", "s"])

    assert "--private-app-password" in output


def test_options_that_were_not_given_are_left_out():
    body = _create(["opensearch", "os", *_MINIMAL["opensearch"]])

    assert body["host"] == "h"
    assert "username" not in body and "scheme" not in body


def test_kafka_sends_the_endpoint_algorithm_the_backend_requires():
    """The backend upper-cases this field unguarded, so an absent value is a 500."""
    body = _create(["kafka", "k", *_MINIMAL["kafka"]])

    assert body["sslEndpointIdentificationAlgorithm"] == "HTTPS"


def test_kafka_ssl_needs_the_stores_and_the_key_password():
    output = _refused(
        ["kafka", "k", "--bootstrap-servers", "b:9092", "--security-protocol", "SSL"]
    )

    assert "--ssl-truststore-location" in output and "--ssl-key-password" in output


def test_flags_are_sent_only_when_set():
    without = _create(["kafka", "k", *_MINIMAL["kafka"]])
    with_flag = _create(["kafka", "k", *_MINIMAL["kafka"], "--external"])

    assert "externalKafka" not in without
    assert with_flag["externalKafka"] is True


def test_opensearch_verification_is_sent_only_when_chosen():
    """Left out, the server keeps its default of verifying."""
    argv = ["opensearch", "os", *_MINIMAL["opensearch"]]

    assert "verify" not in _create(argv)
    assert _create([*argv, "--tls-verify"])["verify"] is True
    assert _create([*argv, "--no-tls-verify"])["verify"] is False


def test_opensearch_tls_flag_is_not_the_cli_wide_no_verify():
    """``--no-verify`` on every command disables control-plane TLS verification.

    The connector flag has its own name so the two cannot be confused, and the
    CLI-wide one still parses on this command without reaching the payload.
    """
    argv = ["opensearch", "os", *_MINIMAL["opensearch"]]
    _create(argv)
    cli_wide = next(
        p
        for p in ds.connector_create.commands["opensearch"].params
        if "--verify" in p.opts
    )

    with mock.patch.object(cli_wide, "callback", side_effect=lambda c, p, v: v) as cb:
        assert "verify" not in _create([*argv, "--no-verify"])
        _create([*argv, "--no-tls-verify"])

    assert [call.args[2] for call in cb.call_args_list] == [False, None]


def test_secrets_are_read_from_the_connector_scoped_environment_variable():
    body = _create(
        ["sap-hana", "n", "--host", "h", "--user", "u"],
        env={"HOPSWORKS_DS_SAP_HANA_PASSWORD": "from-env"},
    )

    assert body["password"] == "from-env"


def test_a_secret_is_read_from_an_account_variable_without_a_shell():
    var = mock.Mock(value="s3cret")
    with (
        mock.patch.object(ds.session, "get_project"),
        mock.patch(
            "hopsworks_common.core.env_var_api.EnvVarsApi.get_env_var",
            return_value=var,
        ) as get,
    ):
        body = _create(
            ["sap-hana", "n", "--host", "h", "--user", "u"],
            env={"HOPSWORKS_DS_SAP_HANA_PASSWORD": "account:CRM_PASSWORD"},
        )

    get.assert_called_once_with("CRM_PASSWORD")
    assert body["password"] == "s3cret"


def test_a_missing_account_variable_is_an_error():
    with (
        mock.patch.object(ds.session, "get_project"),
        mock.patch(
            "hopsworks_common.core.env_var_api.EnvVarsApi.get_env_var",
            return_value=None,
        ),
    ):
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "create",
                "sap-hana",
                "n",
                "--host",
                "h",
                "--user",
                "u",
                "--password",
                "account:NOPE",
            ],
        )

    assert result.exit_code != 0
    assert "no variable NOPE" in result.output


def test_secret_options_name_their_environment_variable_and_stdin_in_help():
    result = CliRunner().invoke(cli, ["datasource", "create", "sql", "--help"])

    text = " ".join(result.output.split())
    assert "HOPSWORKS_DS_SQL_PASSWORD" in text
    assert "HOPSWORKS_DS_SQL_WALLET_PASSWORD" in text
    assert "Pass - to read it from stdin" in text


# Variables exported for other connectors, plus the unscoped names an earlier
# revision used: none of them may reach a connector they were not meant for.
_POLLUTED = {
    "HOPSWORKS_DS_PASSWORD": "old-unscoped-password",
    "HOPSWORKS_DS_API_KEY": "old-unscoped-key",
    "HOPSWORKS_DS_SQL_PASSWORD": "sql-prod-secret",
    "HOPSWORKS_DS_CRM_API_KEY": "hubspot-prod-key",
    "HOPSWORKS_DS_S3_SECRET_KEY": "aws-secret",
}


def test_a_polluted_environment_leaves_other_connectors_alone():
    body = _create(
        ["rest", "public", "--base-url", "https://e.com", "--auth-type", "NONE"],
        env=_POLLUTED,
    )
    assert body["authConfig"] == {"authType": "NONE"}

    body = _create(["jdbc", "j", "--url", "jdbc:mysql://h/db"], env=_POLLUTED)
    assert body["arguments"] == []

    # A required secret is not satisfied by another connector's variable.
    result, create = _invoke(
        ["snowflake", "s", *_without(_MINIMAL["snowflake"], "--password")],
        env=_POLLUTED,
    )
    assert result.exit_code == 2 and "--password" in result.output
    create.assert_not_called()


def test_a_credential_from_the_environment_that_the_mode_rejects_names_its_variable():
    result, create = _invoke(
        ["rest", "public", "--base-url", "https://e.com", "--auth-type", "NONE"],
        env={"HOPSWORKS_DS_REST_API_KEY": "k"},
    )

    assert result.exit_code == 2
    assert "--auth-type NONE does not take --api-key" in result.output
    assert "HOPSWORKS_DS_REST_API_KEY" in result.output
    create.assert_not_called()


@pytest.mark.parametrize(
    ("argv", "refused"),
    [
        (
            [
                "rest",
                "n",
                "--base-url",
                "https://a",
                "--auth-type",
                "NONE",
                "--api-key",
                "k",
            ],
            "--api-key",
        ),
        (
            [
                "rest",
                "n",
                "--base-url",
                "https://a",
                "--auth-type",
                "API_KEY",
                "--api-key",
                "k",
                "--password",
                "p",
            ],
            "--password",
        ),
        (
            ["crm", "n", "--crm-type", "HUBSPOT", "--api-key", "k", "--password", "p"],
            "--password",
        ),
        (
            [*["sql", "n"], *_MINIMAL["sql"], "--wallet-password", "w"],
            "--wallet-password",
        ),
        (
            [
                "kafka",
                "k",
                "--bootstrap-servers",
                "b",
                "--security-protocol",
                "PLAINTEXT",
                "--ssl-key-password",
                "p",
            ],
            "--ssl-key-password",
        ),
        ([*["glue", "n"], *_MINIMAL["glue"], "--session-token", "t"], "--access-key"),
    ],
)
def test_credentials_outside_the_selected_mode_are_refused(argv, refused):
    assert refused in _refused(argv)


def test_a_secret_can_be_read_from_stdin():
    with mock.patch.object(ds, "_create_connector") as create:
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "create",
                "sap-hana",
                "n",
                "--host",
                "h",
                "--user",
                "u",
                "--password",
                "-",
            ],
            input="from-stdin\n",
        )

    assert result.exit_code == 0, result.output
    assert create.call_args.args[1]["password"] == "from-stdin"
    assert "from-stdin" not in result.output


def test_only_one_secret_can_come_from_stdin():
    with mock.patch.object(ds, "_create_connector") as create:
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "create",
                "sql",
                "n",
                *_MINIMAL["sql"],
                "--password",
                "-",
                "--wallet-password",
                "-",
            ],
            input="one\n",
        )

    assert result.exit_code == 2
    assert "Only one secret can be read from stdin" in result.output
    create.assert_not_called()


def test_a_required_secret_is_prompted_for_without_echo_on_a_terminal(monkeypatch):
    monkeypatch.setattr(ds, "_interactive", lambda: True)
    with mock.patch.object(ds, "_create_connector") as create:
        result = CliRunner().invoke(
            cli,
            ["datasource", "create", "sap-hana", "n", "--host", "h", "--user", "u"],
            input="typed\n",
        )

    assert result.exit_code == 0, result.output
    assert create.call_args.args[1]["password"] == "typed"
    assert "typed" not in result.output


def test_an_empty_required_secret_is_treated_as_missing(monkeypatch):
    """An empty value must not satisfy a required secret and carry an empty credential."""
    monkeypatch.setattr(ds, "_interactive", lambda: False)

    result, create = _invoke(
        ["sap-hana", "n", "--host", "h", "--user", "u", "--password", ""]
    )
    assert result.exit_code == 2 and "--password" in result.output
    create.assert_not_called()

    result, create = _invoke(
        ["sap-hana", "n", "--host", "h", "--user", "u"],
        env={"HOPSWORKS_DS_SAP_HANA_PASSWORD": ""},
    )
    assert result.exit_code == 2 and "--password" in result.output
    create.assert_not_called()


def test_required_secret_options_are_marked_required_in_help():
    """The callback enforces them, so --help must still say so."""
    result = CliRunner().invoke(cli, ["datasource", "create", "sap-hana", "--help"])
    text = " ".join(result.output.split())

    # --password is required for sap-hana; --application is not.
    password = text.split("--password", 1)[1].split("--")[0]
    assert "[required]" in password
    application = text.split("--application", 1)[1].split("--")[0]
    assert "[required]" not in application


@pytest.mark.parametrize(
    "argument",
    [
        "sasl.jaas.config=PlainLoginModule required username=u password=p;",
        "ssl.keystore.password=x",
        "apiKey=k",
    ],
)
def test_credential_bearing_connector_arguments_are_refused(argument):
    output = _refused(["kafka", "k", *_MINIMAL["kafka"], "--option", argument])

    assert "looks like a credential" in output


def test_sql_oracle_takes_a_wallet_instead_of_a_host():
    base = [
        "sql",
        "n",
        "--database-type",
        "ORACLE",
        "--port",
        "1522",
        "--database",
        "svc",
        "--user",
        "u",
    ]

    body = _create([*base, "--wallet-path", "/Projects/p/Resources/wallet"])
    assert body["walletPath"] == "/Projects/p/Resources/wallet" and "host" not in body

    assert "exactly one of --host, --wallet-path" in _refused(
        [*base, "--host", "h", "--wallet-path", "/w"]
    )
    assert "exactly one of --host, --wallet-path" in _refused(base)


def test_sql_clickhouse_is_a_database_type_of_the_sql_connector():
    body = _create(
        [
            "sql",
            "n",
            "--database-type",
            "CLICKHOUSE",
            "--host",
            "ch",
            "--port",
            "8123",
            "--database",
            "loadtest",
            "--user",
            "u",
            "--argument",
            "ssl=true",
        ]
    )

    assert body["type"] == "featurestoreSqlConnectorDTO"
    assert body["databaseType"] == "CLICKHOUSE"
    assert body["port"] == 8123
    assert body["arguments"] == [{"name": "ssl", "value": "true"}]


def test_sql_teradata_is_a_database_type_of_the_sql_connector():
    body = _create(
        [
            "sql",
            "n",
            "--database-type",
            "TERADATA",
            "--host",
            "td",
            "--port",
            "1025",
            "--database",
            "demo_user",
            "--user",
            "u",
        ]
    )

    assert body["type"] == "featurestoreSqlConnectorDTO"
    assert body["databaseType"] == "TERADATA"
    assert body["port"] == 1025


def test_sql_sqlserver_is_a_database_type_of_the_sql_connector():
    body = _create(
        [
            "sql",
            "n",
            "--database-type",
            "SQLSERVER",
            "--host",
            "mssql",
            "--port",
            "1433",
            "--database",
            "sales",
            "--user",
            "u",
        ]
    )

    assert body["type"] == "featurestoreSqlConnectorDTO"
    assert body["databaseType"] == "SQLSERVER"
    assert body["port"] == 1433


def test_unity_catalog_defaults_to_a_personal_access_token():
    body = _create(["unity-catalog", "n", *_MINIMAL["unity-catalog"]])

    assert body["authMethod"] == "PAT"
    assert body["accessToken"] == "t"
    assert "--client-id" in _refused(
        ["unity-catalog", "n", *_MINIMAL["unity-catalog"], "--client-id", "c"]
    )


def test_unity_catalog_oauth_m2m_needs_an_endpoint_and_a_service_principal():
    base = [
        "unity-catalog",
        "n",
        "--workspace-url",
        "https://w",
        "--auth-method",
        "OAUTH_M2M",
        "--client-id",
        "c",
        "--client-secret",
        "s",
    ]

    assert "--oauth-endpoint" in _refused(base)

    workspace = _create([*base, "--oauth-endpoint", "WORKSPACE"])
    assert (workspace["oauthEndpoint"], workspace["clientId"]) == ("WORKSPACE", "c")
    assert "--account-id" in _refused(
        [*base, "--oauth-endpoint", "WORKSPACE", "--account-id", "a"]
    )

    assert "--account-host" in _refused([*base, "--oauth-endpoint", "ACCOUNT"])
    account = _create(
        [
            *base,
            "--oauth-endpoint",
            "ACCOUNT",
            "--account-id",
            "a",
            "--account-host",
            "h",
        ]
    )
    assert account["accountHost"] == "h"

    assert "--access-token" in _refused(
        [*base, "--oauth-endpoint", "WORKSPACE", "--access-token", "t"]
    )


def test_redshift_user_is_optional_only_with_auto_create():
    base = _without(_MINIMAL["redshift"], "--user")

    assert _create(["redshift", "n", *base, "--auto-create"])["autoCreate"] is True
    assert "--user" in _refused(["redshift", "n", *base])
    assert "alternatives" in _refused(
        ["redshift", "n", *_MINIMAL["redshift"], "--iam-role", "r", "--password", "p"]
    )


def test_glue_takes_a_role_or_a_complete_key_pair():
    base = ["glue", "n", *_MINIMAL["glue"]]

    assert "--secret-key" in _refused([*base, "--access-key", "a"])
    assert "--access-key" in _refused([*base, "--iam-role", "r", "--access-key", "a"])
    assert _create([*base, "--iam-role", "r"])["iamRole"] == "r"


def test_adls_needs_a_container_for_generation_2_only():
    base = _without(_without(_MINIMAL["adls"], "--container-name"), "--generation")

    assert "containerName" not in _create(["adls", "n", *base, "--generation", "1"])
    assert "--container-name" in _refused(["adls", "n", *base, "--generation", "2"])
    assert "1 or 2" in _refused(["adls", "n", *base, "--generation", "3"])


def test_gcs_encryption_needs_the_key_and_its_hash():
    base = ["gcs", "n", *_MINIMAL["gcs"], "--algorithm", "AES256"]

    assert "--encryption-key-hash" in _refused([*base, "--encryption-key", "k"])


def test_bigquery_needs_a_parent_project_and_a_query_target():
    """Parent project is billed; a query target or materialization is required.

    --project-id is the queried project, not the parent.
    """
    assert "--parent-project" in _refused(
        ["bigquery", "n", "--key-path", "/P/k.json", "--dataset", "d"]
    )
    assert "--materialization-dataset" in _refused(
        ["bigquery", "n", "--parent-project", "p", "--key-path", "/P/k.json"]
    )

    body = _create(
        [
            "bigquery",
            "n",
            "--parent-project",
            "bill",
            "--project-id",
            "data",
            "--dataset",
            "d",
            "--key-path",
            "/P/k.json",
        ]
    )
    assert (body["parentProject"], body["queryProject"]) == ("bill", "data")
    assert "materializationDataset" not in body


def test_google_sheets_spreadsheet_id_is_optional():
    """The connector can be created without it; a feature group sets its own."""
    argv = ["google-sheets", "n", "--key-path", "/P/k.json"]

    assert "spreadsheetId" not in _create(argv)
    assert _create([*argv, "--spreadsheet-id", "s"])["spreadsheetId"] == "s"


def test_mongodb_password_needs_a_user():
    base = ["mongodb", "n", *_MINIMAL["mongodb"]]

    assert "--user" in _refused([*base, "--password", "p"])
    body = _create([*base, "--user", "u", "--password", "p", "--option", "tls=true"])
    assert body["options"] == [{"name": "tls", "value": "true"}]


def test_repeated_arguments_become_the_connector_argument_list():
    body = _create(
        ["sql", "n", *_MINIMAL["sql"], "--argument", "ssl=true", "--argument", "tz=UTC"]
    )

    assert body["arguments"] == [
        {"name": "ssl", "value": "true"},
        {"name": "tz", "value": "UTC"},
    ]


def test_a_malformed_argument_is_a_usage_error():
    output = _refused(["sql", "n", *_MINIMAL["sql"], "--argument", "nope"])

    assert "key=value" in output


def test_an_unknown_choice_is_refused_before_any_request():
    _refused(
        [
            "sql",
            "n",
            *_without(_MINIMAL["sql"], "--database-type"),
            "--database-type",
            "SQLITE",
        ]
    )


def test_sql_provided_credentials_refuse_the_owner_credentials():
    base = [
        "sql",
        "n",
        "--database-type",
        "ORACLE",
        "--host",
        "h",
        "--port",
        "1522",
        "--database",
        "svc",
        "--credentials-mode",
        "provided",
    ]

    body = _create(base)
    assert body["credentialsMode"] == "PROVIDED"
    assert "user" not in body and "password" not in body

    assert "does not take --user" in _refused([*base, "--user", "u"])
    assert "does not take --password" in _refused([*base, "--password", "p"])
    assert "does not take --wallet-path" in _refused([*base, "--wallet-path", "/w"])


def test_sql_provided_oracle_takes_a_host_only_or_nothing():
    base = [
        "sql",
        "n",
        "--database-type",
        "ORACLE",
        "--port",
        "1522",
        "--database",
        "svc",
        "--credentials-mode",
        "PROVIDED",
    ]

    assert "host" not in _create(base)
    assert _create([*base, "--host", "h"])["host"] == "h"


def test_sql_provided_credentials_are_oracle_only():
    output = _refused(
        [
            "sql",
            "n",
            *_without(_MINIMAL["sql"], "--user"),
            "--credentials-mode",
            "provided",
        ]
    )
    assert "ORACLE only" in output


def test_sql_shared_credentials_still_need_a_user():
    body = _create(["sql", "n", *_MINIMAL["sql"]])
    assert body["credentialsMode"] == "SHARED"
    assert body["user"] == "u"

    assert "--credentials-mode SHARED needs --user" in _refused(
        ["sql", "n", *_without(_MINIMAL["sql"], "--user")]
    )


@pytest.mark.parametrize(
    "argv,message",
    [
        (
            ["--auth-type", "NONE", "--api-key", "k"],
            "--auth-type NONE does not take --api-key",
        ),
        (
            [
                "--auth-type",
                "API_KEY",
                "--api-key",
                "k",
                "--user",
                "u",
                "--password",
                "p",
            ],
            "--auth-type API_KEY does not take --user, --password",
        ),
        (
            ["--user", "u", "--password", "p", "--api-key", "k"],
            "basic authentication does not take --api-key",
        ),
        (["--user", "u"], "basic authentication needs --password"),
    ],
)
def test_elasticsearch_auth_modes_are_exclusive(argv, message):
    output = _refused(["elasticsearch", "n", "--host", "h", *argv])
    assert message in output


def test_hopsworks_sends_the_remote_project_and_api_key():
    body = _create(
        [
            "hopsworks",
            "n",
            "--host",
            "hopsworks.example.com",
            "--port",
            "8181",
            "--project",
            "sales",
            "--no-tls-verify",
        ],
        env={"HOPSWORKS_DS_HOPSWORKS_API_KEY": "from-env"},
    )

    assert (body["host"], body["port"], body["project"], body["apiKey"]) == (
        "hopsworks.example.com",
        8181,
        "sales",
        "from-env",
    )
    assert body["verify"] is False


@pytest.mark.parametrize(
    "host", ["https://hopsworks.example.com", "hopsworks.example.com/hopsworks-api"]
)
def test_hopsworks_host_is_a_bare_host_name(host):
    output = _refused(
        ["hopsworks", "n", "--host", host, "--project", "p", "--api-key", "k"]
    )
    assert "--host takes the bare host name" in output


def _tested(response: dict) -> tuple:
    """Run ``datasource create sql ... --test`` against a mocked REST call."""
    from hopsworks_common.core import rest

    with (
        mock.patch.object(ds.session, "get_feature_store") as fs,
        mock.patch.object(rest, "_project_path", side_effect=lambda *p: list(p)),
        mock.patch.object(rest, "_send_request", return_value=response) as send,
        mock.patch.object(ds, "_create_connector") as create,
    ):
        fs.return_value.id = 7
        result = CliRunner().invoke(
            cli, ["datasource", "create", "sql", "n", *_MINIMAL["sql"], "--test"]
        )
    return result, send, create


def test_test_only_checks_the_connection_and_creates_nothing():
    result, send, create = _tested({"statusCode": 0, "connectionOutput": "OK"})

    assert result.exit_code == 0, result.output
    create.assert_not_called()
    assert send.call_args.args[1][-1] == "testConnection"
    assert send.call_args.kwargs["json_body"]["storageConnectorType"] == "SQL"


def test_a_failed_test_exits_with_the_checkers_output():
    result, _, _ = _tested({"statusCode": 1, "connectionOutput": "refused"})

    assert result.exit_code == 1
    assert "refused" in result.output


def test_sharepoint_sends_the_site_and_the_secret():
    body = _create(
        [
            "sharepoint",
            "n",
            *_MINIMAL["sharepoint"],
            "--cloud",
            "US_GOV",
            "--site-id",
            "sid",
        ]
    )

    assert body["tenantId"] == "t"
    assert body["clientId"] == "c"
    assert body["clientSecret"] == "s"
    assert body["siteUrl"] == "https://contoso.sharepoint.com/sites/Finance"
    assert body["siteId"] == "sid"
    assert body["cloud"] == "US_GOV"


def test_sharepoint_secret_is_read_from_its_scoped_environment_variable():
    argv = _without(_MINIMAL["sharepoint"], "--client-secret")
    body = _create(
        ["sharepoint", "n", *argv],
        env={"HOPSWORKS_DS_SHAREPOINT_CLIENT_SECRET": "from-env"},
    )

    assert body["clientSecret"] == "from-env"


# region hops datasource sharepoint


def _sharepoint(argv: list[str], connector_type: str = "SHAREPOINT"):
    """Run ``datasource sharepoint ...`` against a mocked connector; return (result, connector)."""
    from hsfs.storage_connector import SharePointConnector

    sc = mock.create_autospec(SharePointConnector, instance=True)
    sc.type = connector_type
    sc.script_path = "/Projects/p/DataSources/n/sharepoint_sync.py"
    with mock.patch.object(ds.session, "get_feature_store") as fs:
        fs.return_value.get_data_source.return_value.storage_connector = sc
        result = CliRunner().invoke(cli, ["datasource", "sharepoint", *argv])
    return result, sc


def _a_sync(**fields):
    sync = mock.Mock()
    sync.configure_mock(
        id=5,
        dataset_name="sharepoint",
        drive_id="b!1",
        drive_name="Documents",
        folder_path="Reports",
        cron_expression="0 0 * ? * *",
        paused=False,
        state="IDLE",
        last_run_finished=None,
        files_total=3,
        bytes_total=30,
        audit_path="/Projects/p/DataSources/n/audit/sharepoint",
        **fields,
    )
    return sync


def test_sharepoint_commands_refuse_another_type_of_data_source():
    result, sc = _sharepoint(["drives", "n"], connector_type="S3")

    assert result.exit_code == 1
    assert "not SHAREPOINT" in result.output
    sc.list_drives.assert_not_called()


def test_sharepoint_drives_and_folders():
    result, sc = _sharepoint(["drives", "n"])
    assert result.exit_code == 0, result.output
    sc.list_drives.assert_called_once_with()

    result, sc = _sharepoint(["folders", "n", "--drive", "b!1", "--path", "Reports"])
    assert result.exit_code == 0, result.output
    sc.list_folders.assert_called_once_with("b!1", "Reports")


def test_sharepoint_copy_is_a_copy_folder():
    result, sc = _sharepoint(["copy", "n", "--drive", "b!1", "--folder", "Reports"])

    assert result.exit_code == 0, result.output
    sc.copy_folder.assert_called_once_with(
        drive_id="b!1",
        folder_path="Reports",
        dataset_name="sharepoint",
        alert_receiver=None,
        index_files=False,
    )


def test_sharepoint_sync_create_defaults_to_an_hourly_mirror_run_now():
    result, sc = _sharepoint(["sync", "create", "n", "--drive", "b!1"])

    assert result.exit_code == 0, result.output
    sc.create_sync.assert_called_once_with(
        drive_id="b!1",
        folder_path="",
        dataset_name="sharepoint",
        schedule="0 0 * ? * *",
        include=[],
        exclude=[],
        max_file_size_mb=1024,
        mirror_deletions=True,
        alert_receiver=None,
        run_now=True,
        index_files=False,
        index_feature_group=None,
    )


def test_sharepoint_sync_create_passes_every_option():
    result, sc = _sharepoint(
        [
            "sync",
            "create",
            "n",
            "--drive",
            "b!1",
            "--folder",
            "Reports",
            "--dataset",
            "reports",
            "--cron",
            "0 0 2 ? * *",
            "--include",
            "*.pdf",
            "--include",
            "*.xlsx",
            "--exclude",
            "drafts/**",
            "--max-file-size-mb",
            "0",
            "--no-mirror-deletions",
            "--alert-receiver",
            "ops",
            "--no-run-now",
            "--index-feature-group",
            "reports_index",
        ]
    )

    assert result.exit_code == 0, result.output
    kwargs = sc.create_sync.call_args.kwargs
    assert kwargs["schedule"] == "0 0 2 ? * *"
    assert kwargs["include"] == ["*.pdf", "*.xlsx"]
    assert kwargs["exclude"] == ["drafts/**"]
    assert kwargs["max_file_size_mb"] == 0
    assert kwargs["mirror_deletions"] is False
    assert kwargs["alert_receiver"] == "ops"
    assert kwargs["run_now"] is False
    assert kwargs["index_feature_group"] == "reports_index"


def test_sharepoint_sync_list():
    result, sc = _sharepoint(["sync", "list", "n"])
    assert result.exit_code == 0, result.output
    sc.get_syncs.assert_called_once_with()


@pytest.mark.parametrize(
    ("command", "method"),
    [
        ("run", "run"),
        ("pause", "pause"),
        ("resume", "resume"),
        ("resync", "force_full_resync"),
        ("status", "status"),
    ],
)
def test_sharepoint_sync_actions_call_the_sync(command, method):
    sync = _a_sync()
    sync.status.return_value = {"state": "IDLE"}
    with mock.patch.object(ds, "_sync", return_value=sync) as get:
        result = CliRunner().invoke(
            cli, ["datasource", "sharepoint", "sync", command, "n", "5"]
        )

    assert result.exit_code == 0, result.output
    assert get.call_args.args[1:] == ("n", 5)
    getattr(sync, method).assert_called_once_with()


_EXECUTION = mock.Mock(
    spec=["id", "state", "final_status", "submission_time"],
    id=901,
    state="INITIALIZING",
    final_status="UNDEFINED",
    submission_time=None,
)


@pytest.mark.parametrize(
    ("returned", "expected"),
    [(_EXECUTION, {"id": 901, "state": "INITIALIZING"}), (None, None)],
)
def test_sharepoint_sync_resync_prints_the_execution_as_json(
    monkeypatch, returned, expected
):
    monkeypatch.setattr(output, "JSON_MODE", False)
    sync = _a_sync()
    sync.force_full_resync.return_value = returned
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli, ["--json", "datasource", "sharepoint", "sync", "resync", "n", "5"]
        )

    assert result.exit_code == 0, result.output
    printed = json.loads(result.output)
    if expected is None:
        assert printed is None
    else:
        assert printed.items() >= expected.items()


@pytest.mark.parametrize(
    ("argv", "expected"),
    [(["--cron", "0 0 2 ? * *"], "0 0 2 ? * *"), (["--none"], None)],
)
def test_sharepoint_sync_schedule(argv, expected):
    sync = _a_sync()
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli, ["datasource", "sharepoint", "sync", "schedule", "n", "5", *argv]
        )

    assert result.exit_code == 0, result.output
    sync.set_schedule.assert_called_once_with(expected)


@pytest.mark.parametrize("argv", [[], ["--cron", "0 0 2 ? * *", "--none"]])
def test_sharepoint_sync_schedule_takes_exactly_one_of_cron_and_none(argv):
    sync = _a_sync()
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli, ["datasource", "sharepoint", "sync", "schedule", "n", "5", *argv]
        )

    assert result.exit_code == 2
    sync.set_schedule.assert_not_called()


def test_sharepoint_sync_set_folder():
    sync = _a_sync()
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "sharepoint",
                "sync",
                "set-folder",
                "n",
                "5",
                "--folder",
                "R",
            ],
        )

    assert result.exit_code == 0, result.output
    sync.set_folder.assert_called_once_with("R")


@pytest.mark.parametrize(
    "argv,fields",
    [
        (["--on"], {"index_files": True}),
        (
            ["--on", "--feature-group", "docs"],
            {"index_files": True, "index_feature_group": "docs"},
        ),
        (["--off"], {"index_files": False}),
    ],
)
def test_sharepoint_sync_index(argv, fields):
    sync = _a_sync()
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli, ["datasource", "sharepoint", "sync", "index", "n", "5", *argv]
        )

    assert result.exit_code == 0, result.output
    sync.update.assert_called_once_with(**fields)


def test_sharepoint_sync_delete_confirms_and_passes_its_flags():
    sync = _a_sync()
    with mock.patch.object(ds, "_sync", return_value=sync):
        aborted = CliRunner().invoke(
            cli, ["datasource", "sharepoint", "sync", "delete", "n", "5"], input="n\n"
        )
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "sharepoint",
                "sync",
                "delete",
                "n",
                "5",
                "--keep-files",
                "--force",
                "--yes",
            ],
        )

    assert aborted.exit_code == 1
    assert result.exit_code == 0, result.output
    sync.delete.assert_called_once_with(keep_files=True, force=True)
    assert "Deleted sync 5" in result.output


def test_sharepoint_sync_delete_reports_a_pending_delete():
    sync = _a_sync()
    sync.delete.return_value = False
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "sharepoint",
                "sync",
                "delete",
                "n",
                "5",
                "--force",
                "--yes",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "Deleted sync" not in result.output
    assert "still DELETING" in result.output
    assert "hops datasource sharepoint sync delete n 5 --force --yes" in result.output


@pytest.mark.parametrize("deleted", [True, False])
def test_sharepoint_sync_delete_json_says_whether_it_finished(monkeypatch, deleted):
    monkeypatch.setattr(output, "JSON_MODE", False)
    sync = _a_sync()
    sync.delete.return_value = deleted
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli,
            [
                "--json",
                "datasource",
                "sharepoint",
                "sync",
                "delete",
                "n",
                "5",
                "--force",
            ],
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"id": 5, "deleted": deleted}


def test_sharepoint_sync_audit_prints_the_rows():
    sync = _a_sync()
    sync.get_audit.return_value = [
        {
            "execution_id": 812,
            "op": "renamed",
            "kind": "file",
            "path": "new.csv",
            "src_path": "old.csv",
        }
    ]
    with mock.patch.object(ds, "_sync", return_value=sync):
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "sharepoint",
                "sync",
                "audit",
                "n",
                "5",
                "--day",
                "2026-10-20",
            ],
        )

    assert result.exit_code == 0, result.output
    sync.get_audit.assert_called_once_with(day="2026-10-20")
    assert "old.csv" in result.output


def test_sharepoint_reset_script():
    result, sc = _sharepoint(["reset-script", "n", "--yes"])

    assert result.exit_code == 0, result.output
    sc.reset_script.assert_called_once_with()


# endregion

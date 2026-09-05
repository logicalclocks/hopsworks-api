"""``hops datasource create`` builds the payload each connector type expects.

The bodies are asserted rather than sent: the generated commands differ only in
their fields, so what is worth pinning is the discriminator, the property names,
the shapes that are not flat (REST nests, CRM is discriminated a second time by
``crmType``), and the rules between options that the backend would otherwise
reject after the request.
"""

from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner
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
    "bigquery": ["--project-id", "p"],
    "opensearch": ["--host", "h", "--port", "9200"],
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
    "glue": ["--database", "d", "--region", "eu-north-1"],
    "google-sheets": ["--spreadsheet-id", "s", "--key-path", "/Projects/p/k.json"],
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
    "sql": ("featurestoreSqlConnectorDTO", "SQL"),
    "crm": ("featurestoreCRMConnectorDTO", "CRM"),
    "rest": ("featurestoreRESTConnectorDTO", "REST"),
    "sap-hana": ("featureStoreSapHanaConnectorDTO", "SAP_HANA"),
    "unity-catalog": ("featurestoreUnityCatalogConnectorDTO", "UNITY_CATALOG"),
    "mongodb": ("featurestoreMongoConnectorDTO", "MONGODB"),
    "glue": ("featurestoreGlueConnectorDTO", "GLUE"),
    "google-sheets": ("featurestoreGoogleSheetsConnectorDTO", "GOOGLE_SHEETS"),
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

"""``hops datasource create`` builds the payload each connector type expects.

The bodies are asserted rather than sent: the thirteen generated commands differ
only in their fields, so what is worth pinning is the discriminator, the property
names, and the shapes that are not flat (REST nests, CRM is discriminated a second
time by ``crmType``).
"""

from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.commands import datasource as ds
from hopsworks.cli.main import cli


def _create(argv: list[str]) -> dict:
    """Run ``datasource create ...`` and return the body it would have posted."""
    with mock.patch.object(ds, "_create_connector") as create:
        result = CliRunner().invoke(cli, ["datasource", "create", *argv])
    assert result.exit_code == 0, result.output
    return create.call_args.args[1]


def test_every_backend_connector_type_has_a_subcommand():
    """FeaturestoreConnectorType in hopsworks-ee, as the UI offers them."""
    expected = {
        "hopsfs",
        "jdbc",
        "redshift",
        "s3",
        "adls",
        "snowflake",
        "kafka",
        "gcs",
        "bigquery",
        "opensearch",
        "sql",
        "crm",
        "rest",
        "sap-hana",
        "unity-catalog",
        "mongodb",
        "glue",
        "google-sheets",
    }

    assert set(ds.connector_create.commands) == expected


@pytest.mark.parametrize(
    ("argv", "dto", "connector_type"),
    [
        (
            ["hopsfs", "n", "--dataset", "Resources"],
            "featurestoreHopsfsConnectorDTO",
            "HOPSFS",
        ),
        (
            ["glue", "n", "--database", "db", "--region", "eu-north-1"],
            "featurestoreGlueConnectorDTO",
            "GLUE",
        ),
        (
            ["sql", "n", "--database-type", "MYSQL", "--host", "h"],
            "featurestoreSqlConnectorDTO",
            "SQL",
        ),
        (
            ["unity-catalog", "n", "--workspace-url", "https://w"],
            "featurestoreUnityCatalogConnectorDTO",
            "UNITY_CATALOG",
        ),
    ],
)
def test_the_payload_names_the_dto_and_the_connector_type(argv, dto, connector_type):
    body = _create(argv)

    assert body["type"] == dto
    assert body["storageConnectorType"] == connector_type
    assert body["name"] == "n"


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


def test_options_that_were_not_given_are_left_out():
    body = _create(["opensearch", "os", "--host", "h"])

    assert body["host"] == "h"
    assert "username" not in body and "port" not in body


def test_kafka_sends_the_endpoint_algorithm_the_backend_requires():
    """The backend upper-cases this field unguarded, so an absent value is a 500."""
    body = _create(["kafka", "k", "--bootstrap-servers", "b:9092"])

    assert body["sslEndpointIdentificationAlgorithm"] == "HTTPS"


def test_flags_are_sent_only_when_set():
    without = _create(["kafka", "k", "--bootstrap-servers", "b:9092"])
    with_flag = _create(["kafka", "k", "--bootstrap-servers", "b:9092", "--external"])

    assert "externalKafka" not in without
    assert with_flag["externalKafka"] is True


def test_no_verify_sends_false_rather_than_true():
    body = _create(["opensearch", "os", "--host", "h", "--no-verify"])

    assert body["verify"] is False


def test_repeated_arguments_become_the_connector_argument_list():
    body = _create(
        [
            "sql",
            "n",
            "--database-type",
            "MYSQL",
            "--host",
            "h",
            "--argument",
            "ssl=true",
            "--argument",
            "tz=UTC",
        ]
    )

    assert body["arguments"] == [
        {"name": "ssl", "value": "true"},
        {"name": "tz", "value": "UTC"},
    ]


def test_a_malformed_argument_is_a_usage_error():
    with mock.patch.object(ds, "_create_connector"):
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "create",
                "sql",
                "n",
                "--database-type",
                "MYSQL",
                "--host",
                "h",
                "--argument",
                "nope",
            ],
        )

    assert result.exit_code != 0
    assert "key=value" in result.output


def test_an_unknown_choice_is_refused_before_any_request():
    with mock.patch.object(ds, "_create_connector") as create:
        result = CliRunner().invoke(
            cli,
            [
                "datasource",
                "create",
                "sql",
                "n",
                "--database-type",
                "SQLITE",
                "--host",
                "h",
            ],
        )

    assert result.exit_code != 0
    create.assert_not_called()

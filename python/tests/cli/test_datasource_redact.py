"""The CLI prints a data source without its credentials.

Every connector class is built with the real SDK constructor and serialized with
its real ``to_dict``; a marker stands in for each secret, and no marker may
survive the redaction ``hops datasource info --json`` prints through. The secret
slots are listed by hand, so a secret the redaction misses fails here. The
markers are compared in-process, never printed.
"""

from __future__ import annotations

import inspect
import json

import pytest
from hopsworks.cli.redact import HIDDEN, redact
from hsfs import storage_connector as sc


_OPTION_LISTS = {
    "arguments",
    "options",
    "sf_options",
    "spark_options",
    "headers",
    "parameters",
}
_NO_SECRET = (sc.GoogleSheetsConnector, sc.HopsFSConnector)
_CONNECTION_STRINGS = {"connection_string"}
# The constructor slots that hold a secret, per connector class.
_SECRETS = {
    "AdlsConnector": ["service_credential"],
    "CRMAndAnalyticsConnector": ["api_key", "password", "dev_token", "refresh_token"],
    "ElasticsearchConnector": ["password", "api_key", "trust_store_password"],
    "GcsConnector": ["encryption_key"],
    "GlueConnector": ["access_key", "secret_key", "session_token"],
    "HopsworksConnector": ["api_key"],
    "KafkaConnector": [
        "ssl_truststore_password",
        "ssl_keystore_password",
        "ssl_key_password",
    ],
    "MongoDBConnector": ["password"],
    "OpenSearchConnector": ["password", "trust_store_password"],
    "RedshiftConnector": ["database_password"],
    "S3Connector": [
        "access_key",
        "secret_key",
        "server_encryption_key",
        "session_token",
    ],
    "SapHanaConnector": ["password"],
    "SharePointConnector": ["client_secret"],
    "SnowflakeConnector": ["password", "token", "private_key", "passphrase"],
    "SqlConnector": ["password", "wallet_password"],
    "UnityCatalogConnector": ["access_token", "client_secret"],
}


def _marker(param: str) -> str:
    return f"MARKER-{param}-7f3a"


def _build(cls: type) -> tuple[sc.StorageConnector, list[str]]:
    """Build ``cls`` from its own constructor with a marker in every secret slot."""
    markers: list[str] = []
    kwargs: dict = {}
    for name, param in inspect.signature(cls.__init__).parameters.items():
        if name in ("self", "kwargs", "args") or param.kind in (
            param.VAR_KEYWORD,
            param.VAR_POSITIONAL,
        ):
            continue
        if name == "id":
            kwargs[name] = 11
        elif name == "featurestore_id":
            kwargs[name] = 67
        elif name == "crm_type":
            kwargs[name] = sc.CRMSource.HUBSPOT
        elif name == "name":
            kwargs[name] = "src"
        elif name in _OPTION_LISTS:
            kwargs[name] = [
                {"name": "password", "value": _marker(name + "-pw")},
                {"name": "Authorization", "value": _marker(name + "-auth")},
                {"name": "sslmode", "value": "require"},
            ]
            markers += [_marker(name + "-pw"), _marker(name + "-auth")]
        elif name in _CONNECTION_STRINGS:
            kwargs[name] = (
                f"jdbc:postgresql://u:{_marker('uri')}@h:5432/db"
                f"?user=u&password={_marker('query')}&sslmode=require"
            )
            markers += [_marker("uri"), _marker("query")]
        elif name in _SECRETS.get(cls.__name__, []):
            kwargs[name] = _marker(name)
            markers.append(_marker(name))
    if cls is sc.RestConnector:
        kwargs["client_config"] = {
            "base_url": "https://api.example.com",
            "headers": [
                {"name": "X-Api-Key", "value": _marker("rest-header")},
                {"name": "Accept", "value": "application/json"},
            ],
        }
        kwargs["auth_config"] = {
            "auth_type": sc.RestConnectorAuthType.BEARER,
            "bearer_token": _marker("bearer"),
        }
        markers += [_marker("rest-header"), _marker("bearer")]
    return cls(**kwargs), markers


_CLASSES = [
    c
    for _, c in inspect.getmembers(sc, inspect.isclass)
    if issubclass(c, sc.StorageConnector) and c is not sc.StorageConnector
]


@pytest.mark.parametrize("cls", _CLASSES, ids=lambda c: c.__name__)
def test_no_connector_type_prints_a_credential(cls):
    connector, markers = _build(cls)
    raw = json.dumps(connector.to_dict(), default=str)
    clean = json.dumps(redact(connector.to_dict()), default=str)
    assert [m for m in markers if m in clean] == []
    # The control: the SDK serialization carries them, so the check can fail.
    assert any(m in raw for m in markers) or cls in _NO_SECRET


def test_non_secret_options_and_hosts_stay():
    connector, _ = _build(sc.SqlConnector)
    connector._host, connector._port = "db.example.com", 5432
    payload = redact(connector.to_dict())
    assert payload["host"] == "db.example.com" and payload["port"] == 5432
    assert payload["credentialsMode"] == connector.to_dict()["credentialsMode"]
    assert {"name": "sslmode", "value": "require"} in payload["arguments"]
    assert "password" not in payload and "walletPassword" not in payload


@pytest.mark.parametrize(
    "text",
    [
        "jdbc:postgresql://u:MARKER-a@h:5432/db?sslmode=require",
        "jdbc:oracle:thin:scott/MARKER-a@db.example:1521/service",
        "jdbc:postgresql://db.example/db?%70assword=MARKER-a",
        "jdbc:sqlserver://db.example;password={MARKER-a;MARKER-b};databaseName=db",
        "jdbc:db2://h:50000/db:user=u;password=MARKER-a;",
        "jdbc:snowflake://acct.snowflakecomputing.com/?private_key_file_pwd=MARKER-a",
        "mongodb+srv://u:MARKER-a@cluster0.example.net/db",
        "the source is jdbc:mysql://h/db?password=MARKER-a",
        "user=u;password={MARKER-a;MARKER-b};ssl=true",
        "a=1,%70wd=MARKER-a",
    ],
)
def test_no_credential_survives_in_a_connection_string(text):
    assert "MARKER" not in json.dumps(redact({"c": text}))


def test_a_connection_string_keeps_its_scheme_host_port_and_database():
    def clean(text):
        return redact({"c": text})["c"]

    assert (
        clean("jdbc:postgresql://h:5432/db?sslmode=require")
        == "jdbc:postgresql://h:5432/db"
    )
    assert clean("jdbc:sqlserver://h;databaseName=d;password=p") == "jdbc:sqlserver://h"
    assert clean("jdbc:oracle:thin:@//h:1521/svc") == "jdbc:oracle:thin:@//h:1521/svc"
    assert clean("https://api.example.com/v1") == "https://api.example.com/v1"
    assert clean("jdbc:postgresql://u:p@h/db") == HIDDEN
    assert clean("user=u;password=p;ssl=true") == "user=u;ssl=true"
    assert redact([{"name": "%70assword", "value": "p"}]) == []


def test_the_account_entry_names_of_a_binding_stay():
    binding = {
        "status": "VALID",
        "password_secret_name": "ds_oracle_sales_67_password",
        "wallet_password_secret_name": "ds_oracle_sales_67_wallet_password",
    }
    assert redact(binding) == binding


def test_info_json_of_a_shopify_source_prints_neither_credential():
    for secret_field in ("clientSecret", "privateAppPassword"):
        connector = sc.StorageConnector.from_response_json(
            {
                "type": "featurestoreCRMConnectorDTO",
                "storageConnectorType": "CRM",
                "id": 11,
                "name": "shop",
                "featurestoreId": 67,
                "crmType": "shopify",
                "shopUrl": "https://s.myshopify.com",
                "clientId": "id",
                secret_field: "MARKER-shopify-7f3a",
            }
        )
        payload = connector.to_dict()
        # The control: the SDK keeps the credential, so the check below can fail.
        assert "MARKER" in json.dumps(payload, default=str)
        # What `hops --json datasource info` prints (datasource.py, info).
        printed = json.dumps(redact(payload), default=str)
        assert "MARKER" not in printed
        assert json.loads(printed)["shopUrl"] == "https://s.myshopify.com"

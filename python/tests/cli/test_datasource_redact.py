"""The CLI prints a data source without its credentials.

Every connector class is built with the real SDK constructor and serialized with
its real ``to_dict``; a marker stands in for each secret, and no marker may
survive ``hops datasource info --json``. The markers are compared in-process,
never printed.
"""

from __future__ import annotations

import inspect
import json
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli
from hopsworks.cli.redact import is_secret_name, redact
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
        elif is_secret_name(name):
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


@pytest.mark.parametrize("cls", _CLASSES, ids=lambda c: c.__name__)
def test_info_json_prints_no_credential_and_keeps_the_metadata(mock_project, cls):
    connector, markers = _build(cls)
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = mock.MagicMock(storage_connector=connector)
    result = CliRunner().invoke(cli, ["--json", "datasource", "info", "src"])
    assert result.exit_code == 0, "info failed"
    assert [m for m in markers if m in result.output] == []
    payload = json.loads(result.output)
    assert payload["id"] == 11 and payload["name"] == "src"


def test_non_secret_options_and_hosts_stay():
    connector, _ = _build(sc.SqlConnector)
    connector._host, connector._port = "db.example.com", 5432
    payload = redact(connector.to_dict())
    assert payload["host"] == "db.example.com" and payload["port"] == 5432
    assert payload["credentialsMode"] == connector.to_dict()["credentialsMode"]
    assert {"name": "sslmode", "value": "require"} in payload["arguments"]
    assert "password" not in payload and "walletPassword" not in payload


def test_connection_string_keeps_its_host_and_plain_properties():
    text = "jdbc:postgresql://u:s3@h:5432/db?user=u&password=s3&sslmode=require"
    assert redact({"connectionString": text}) == {
        "connectionString": "jdbc:postgresql://h:5432/db?user=u&sslmode=require"
    }
    assert redact({"c": "mongodb://u:p@h/db"}) == {"c": "mongodb://h/db"}
    assert redact({"c": "jdbc:sqlserver://h;databaseName=d;password=p"}) == {
        "c": "jdbc:sqlserver://h;databaseName=d"
    }


def test_the_account_entry_names_of_a_binding_stay():
    binding = {
        "status": "VALID",
        "password_secret_name": "ds_oracle_sales_67_password",
        "wallet_password_secret_name": "ds_oracle_sales_67_wallet_password",
    }
    assert redact(binding) == binding

"""Tests for the ``hops trino`` / ``hops sql`` connection defaults."""

from __future__ import annotations

from unittest import mock

import click
from click.testing import CliRunner
from hopsworks.cli import config, session
from hopsworks.cli.commands import trino
from hopsworks.cli.main import cli
from trino.exceptions import TrinoUserError


def _connect_kwargs(catalog=None, schema=None):
    """Return the kwargs ``trino._connect`` passes to ``TrinoApi.connect``."""
    ctx = click.Context(cli)
    ctx.obj = {
        "config": config.HopsConfig(host="https://h", api_key="k", project="Sales")
    }
    fake_api = mock.MagicMock(name="TrinoApi")
    fake_project = mock.MagicMock(name="Project")
    fake_project.name = "Sales"
    fake_project.get_trino_api.return_value = fake_api
    fake_fs = mock.MagicMock(name="FeatureStore")
    fake_fs.name = "sales_featurestore"
    with (
        mock.patch.object(session, "get_project", return_value=fake_project),
        mock.patch.object(session, "get_feature_store", return_value=fake_fs),
    ):
        trino._connect(ctx, catalog=catalog, schema=schema)
    return fake_api.connect.call_args.kwargs


def test_defaults_to_the_feature_stores_delta_tables():
    # Feature groups are created as DELTA tables in <project>_featurestore, so a
    # bare ``hops sql "SELECT * FROM <fg>_1"`` must land there.
    kwargs = _connect_kwargs()
    assert kwargs["catalog"] == "delta"
    assert kwargs["schema"] == "sales_featurestore"


def test_explicit_catalog_and_schema_win():
    kwargs = _connect_kwargs(catalog="iceberg", schema="other")
    assert kwargs["catalog"] == "iceberg"
    assert kwargs["schema"] == "other"


def test_a_rejected_query_is_one_error_line(mock_project):
    cursor = mock.MagicMock()
    cursor.execute.side_effect = RuntimeError(
        "error 401: b'Access Denied: Invalid credentials'"
    )
    connection = mock.MagicMock()
    connection.cursor.return_value = cursor
    with mock.patch.object(trino, "_connect", return_value=connection):
        result = CliRunner().invoke(cli, ["sql", "SELECT 1"])
    assert result.exit_code == 1
    assert result.output.strip() == (
        "Error: Could not run the query: error 401: "
        "b'Access Denied: Invalid credentials'"
    )
    connection.close.assert_called_once()


def _rejected(exc):
    cursor = mock.MagicMock()
    cursor.execute.side_effect = exc
    connection = mock.MagicMock()
    connection.cursor.return_value = cursor
    with mock.patch.object(trino, "_connect", return_value=connection):
        return CliRunner().invoke(cli, ["sql", "SELECT count(*) FROM orders_1"])


def _store(name, group=None):
    fs = mock.MagicMock()
    fs.name = name
    fs.get_feature_group.return_value = group
    return fs


def _not_found(table):
    return TrinoUserError(
        {
            "errorName": "TABLE_NOT_FOUND",
            "errorType": "USER_ERROR",
            "message": f"line 1:22: Table 'delta.p_featurestore.{table}' does not exist",
        },
        "q1",
    )


def _with_stores(stores):
    return (
        mock.patch.object(
            session, "get_accessible_feature_stores", return_value=stores
        ),
        mock.patch.object(trino, "_featurestore_schema", return_value="p_featurestore"),
    )


def test_a_table_in_a_shared_store_names_that_store(mock_project):
    stores = [_store("p_featurestore"), _store("9abc_featurestore", mock.MagicMock())]
    a, b = _with_stores(stores)
    with a, b:
        result = _rejected(_not_found("orders_1"))
    assert result.exit_code == 1
    assert result.output.strip().endswith(
        "orders_1 is in the shared feature store 9abc_featurestore: write "
        '"9abc_featurestore".orders_1, or pass --schema 9abc_featurestore.'
    )
    stores[0].get_feature_group.assert_not_called()
    stores[1].get_feature_group.assert_called_once_with("orders", version=1)


def test_a_table_in_no_shared_store_gets_no_hint(mock_project):
    a, b = _with_stores([_store("p_featurestore"), _store("9abc_featurestore")])
    with a, b:
        result = _rejected(_not_found("orders_1"))
    assert result.exit_code == 1
    assert result.output.strip().endswith("query_id=q1)")


def test_a_store_name_starting_with_a_digit_gets_the_quoting_hint(mock_project):
    result = _rejected(
        TrinoUserError(
            {
                "errorName": "SYNTAX_ERROR",
                "errorType": "USER_ERROR",
                "message": "line 1:22: identifiers must not start with a digit; "
                "surround the identifier with double quotes",
            },
            "q1",
        )
    )
    assert result.exit_code == 1
    assert result.output.strip().endswith('"<store>".<name>_<version>.')

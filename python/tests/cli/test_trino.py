"""Tests for the ``hops trino`` / ``hops sql`` connection defaults."""

from __future__ import annotations

from unittest import mock

import click
from hopsworks.cli import config, session
from hopsworks.cli.commands import trino
from hopsworks.cli.main import cli


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

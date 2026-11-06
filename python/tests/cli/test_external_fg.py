"""CliRunner tests for external feature groups and other non-cached types."""

from __future__ import annotations

import inspect
import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli
from hsfs.core.data_source import DataSource
from hsfs.feature_group import ExternalFeatureGroup, SpineGroup
from hsfs.feature_store import FeatureStore


def _external_fg(**source):
    fg = mock.MagicMock(spec=ExternalFeatureGroup)
    fg.id = 7
    fg.name = "sales"
    fg.version = 1
    fg.online_enabled = False
    fg.primary_key = ["id"]
    fg.event_time = None
    fg.description = ""
    fg.features = []
    fg.data_format = source.pop("data_format", None)
    fg.data_source_access = {"status": "OK"}
    connector = mock.MagicMock()
    connector.name = "snowflake_prod"
    fg.data_source = DataSource(storage_connector=connector, **source)
    return fg


def _create(mock_project, args):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = DataSource(storage_connector=mock.MagicMock())
    fs.create_external_feature_group.return_value = _external_fg()
    result = CliRunner().invoke(
        cli, ["fg", "create-external", "sales", "--connector", "src", *args]
    )
    return result, fs.create_external_feature_group


def test_create_external_query_lands_on_the_data_source(mock_project):
    result, create = _create(mock_project, ["--query", "SELECT * FROM sales"])
    assert result.exit_code == 0, result.output
    kwargs = create.call_args.kwargs
    assert kwargs["data_source"].query == "SELECT * FROM sales"
    # The SDK ignores query/path/storage_connector next to a data source.
    assert not {"query", "path", "storage_connector"} & set(kwargs)
    declared = inspect.signature(FeatureStore.create_external_feature_group).parameters
    assert set(kwargs) <= set(declared)


def test_create_external_path_and_format(mock_project):
    result, create = _create(
        mock_project,
        ["--path", "sales/", "--data-format", "ICEBERG", "--online"]
        + ["--features", "id:bigint,city:string"],
    )
    assert result.exit_code == 0, result.output
    kwargs = create.call_args.kwargs
    assert [(f.name, f.type) for f in kwargs["features"]] == [
        ("id", "bigint"),
        ("city", "string"),
    ]
    assert kwargs["data_source"].path == "sales/"
    assert kwargs["data_format"] == "iceberg"
    assert kwargs["online_enabled"] is True


def test_create_external_refuses_query_and_path(mock_project):
    result, create = _create(
        mock_project, ["--query", "SELECT 1", "--path", "x/", "--data-format", "csv"]
    )
    assert result.exit_code == 2
    assert "not both" in result.output
    create.assert_not_called()


def test_create_external_json_prints_the_feature_group(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = DataSource(storage_connector=mock.MagicMock())
    fs.create_external_feature_group.return_value = _external_fg(query="SELECT 1")
    result = CliRunner().invoke(
        cli,
        ["--json", "fg", "create-external", "sales", "--connector", "src"]
        + ["--query", "SELECT 1"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["type"] == "external"
    assert payload["source"]["query"] == "SELECT 1"


def test_info_shows_where_an_external_fg_reads_from(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = _external_fg(
        query="SELECT * FROM sales", data_format="parquet"
    )
    result = CliRunner().invoke(cli, ["fg", "info", "sales", "--version", "1"])
    assert result.exit_code == 0, result.output
    for expected in ("snowflake_prod", "SELECT * FROM sales", "parquet", "ok"):
        assert expected in result.output


def test_insert_refuses_a_spine_group(mock_project):
    fs = mock_project.get_feature_store.return_value
    spine = mock.MagicMock(spec=SpineGroup)
    spine.name = "spine"
    fs.get_feature_group.return_value = spine
    result = CliRunner().invoke(
        cli, ["fg", "insert", "spine", "--version", "1"], input="[{}]"
    )
    assert result.exit_code == 1
    assert "spine group" in result.output


def test_delete_prompt_says_the_external_table_stays(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = _external_fg()
    result = CliRunner().invoke(
        cli, ["fg", "delete", "sales", "--version", "1"], input="n\n"
    )
    assert "external table is left untouched" in result.output


def test_list_shows_every_type_and_filters_by_type(mock_project):
    fs = mock_project.get_feature_store.return_value
    cached = mock.MagicMock()
    cached.name = "orders"
    cached.stream = False
    fs.get_feature_groups.return_value = [cached, _external_fg()]
    result = CliRunner().invoke(cli, ["fg", "list"])
    assert result.exit_code == 0, result.output
    assert "orders" in result.output and "sales" in result.output
    assert "managed" in result.output and "external" in result.output
    result = CliRunner().invoke(cli, ["fg", "list", "--type", "external"])
    assert result.exit_code == 0, result.output
    assert "sales" in result.output and "orders" not in result.output


def test_create_external_warns_when_only_statistics_fail(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = DataSource(storage_connector=mock.MagicMock())
    fg = _external_fg()
    fg.save.side_effect = RuntimeError("statistics job refused")
    fs.create_external_feature_group.return_value = fg
    result = CliRunner().invoke(
        cli, ["fg", "create-external", "sales", "--connector", "src", "--query", "q"]
    )
    assert result.exit_code == 0, result.output
    assert "Created external feature group sales" in result.output
    assert "Statistics were not computed: statistics job refused" in result.output


def test_create_external_fails_when_nothing_was_saved(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = DataSource(storage_connector=mock.MagicMock())
    fg = _external_fg()
    fg.id = None
    fg.save.side_effect = RuntimeError("bad schema")
    fs.create_external_feature_group.return_value = fg
    result = CliRunner().invoke(
        cli, ["fg", "create-external", "sales", "--connector", "src", "--query", "q"]
    )
    assert result.exit_code == 1
    assert "Could not create external FG: bad schema" in result.output


def test_create_external_refuses_empty_path(mock_project):
    result, create = _create(mock_project, ["--path", "", "--data-format", "csv"])
    assert result.exit_code == 2
    assert "--path is empty" in result.output
    assert "hops datasource tables src" in result.output
    create.assert_not_called()

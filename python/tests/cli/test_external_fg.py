"""CliRunner tests for external feature groups and other non-cached types."""

from __future__ import annotations

import inspect
import json
from unittest import mock

import pytest
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


def test_create_external_no_statistics_turns_the_job_off(mock_project):
    result, create = _create(mock_project, ["--query", "SELECT 1", "--no-statistics"])
    assert result.exit_code == 0, result.output
    assert create.call_args.kwargs["statistics_config"] is False


def test_create_external_keeps_the_sdk_statistics_default(mock_project):
    result, create = _create(mock_project, ["--query", "SELECT 1"])
    assert result.exit_code == 0, result.output
    assert create.call_args.kwargs["statistics_config"] is None


def test_create_external_features_help_does_not_claim_it_is_required():
    result = CliRunner().invoke(cli, ["fg", "create-external", "--help"])
    flat = " ".join(result.output.split())
    assert "Optional" in flat and "required outside Spark" not in flat


def _preview(mock_project, args, source=None):
    fs = mock_project.get_feature_store.return_value
    data_source = DataSource(storage_connector=mock.MagicMock(), **(source or {}))
    fs.get_data_source.return_value = data_source
    result = CliRunner().invoke(cli, ["datasource", "preview", "pg", *args])
    return result, data_source


def test_datasource_preview_reads_the_given_table(mock_project):
    result, source = _preview(
        mock_project, ["--table", "rna", "--database", "pfmegrnargs"]
    )
    assert result.exit_code == 0, result.output
    assert (source.table, source.database) == ("rna", "pfmegrnargs")
    source.storage_connector.get_data.assert_called_once()
    assert source.storage_connector.get_data.call_args.args[0] is source


def test_datasource_preview_without_a_table_is_a_usage_error(mock_project):
    result, source = _preview(mock_project, [])
    assert result.exit_code == 2
    assert "--table" in result.output and "hops datasource tables pg" in result.output
    source.storage_connector.get_data.assert_not_called()


def test_datasource_preview_accepts_a_source_that_names_its_own_table(mock_project):
    result, source = _preview(mock_project, [], source={"table": "sheet1"})
    assert result.exit_code == 0, result.output
    source.storage_connector.get_data.assert_called_once()


def test_datasource_preview_reads_the_table_as_the_source_lists_it(mock_project):
    listed = DataSource(
        storage_connector=mock.MagicMock(), database="db", group="rnacen", table="rna"
    )
    fs = mock_project.get_feature_store.return_value
    data_source = DataSource(storage_connector=mock.MagicMock())
    data_source.storage_connector.get_tables.return_value = [listed]
    fs.get_data_source.return_value = data_source
    result = CliRunner().invoke(
        cli, ["datasource", "preview", "pg", "--table", "rna", "--database", "db"]
    )
    assert result.exit_code == 0, result.output
    data_source.storage_connector.get_tables.assert_called_once_with("db")
    sent = listed.storage_connector.get_data.call_args.args[0]
    assert sent is listed and sent.group == "rnacen"


def test_datasource_preview_prints_the_rows_the_backend_nests(mock_project):
    data = mock.MagicMock()
    data.features = [mock.MagicMock(), mock.MagicMock()]
    data.features[0].name, data.features[0].type = "id", "bigint"
    data.features[1].name, data.features[1].type = "city", "string"
    data.preview = {
        "preview": [
            {
                "values": [
                    {"value0": "id", "value1": "387"},
                    {"value0": "city", "value1": "Paris"},
                ]
            }
        ]
    }
    fs = mock_project.get_feature_store.return_value
    data_source = DataSource(storage_connector=mock.MagicMock(), table="t")
    data_source.storage_connector.get_data.return_value = data
    fs.get_data_source.return_value = data_source
    result = CliRunner().invoke(cli, ["datasource", "preview", "pg"])
    assert result.exit_code == 0, result.output
    assert "387" in result.output and "Paris" in result.output
    as_json = CliRunner().invoke(cli, ["--json", "datasource", "preview", "pg"])
    assert json.loads(as_json.stdout)["preview"] == [{"id": "387", "city": "Paris"}]


def test_datasource_tables_shows_the_schema_of_each_table(mock_project):
    fs = mock_project.get_feature_store.return_value
    listed = [
        DataSource(
            storage_connector=mock.MagicMock(),
            database="db",
            group="rnacen",
            table="rna",
        )
    ]
    fs.get_data_source.return_value.get_tables.return_value = listed
    result = CliRunner().invoke(cli, ["datasource", "tables", "pg", "--database", "db"])
    assert result.exit_code == 0, result.output
    assert "SCHEMA" in result.output and "rnacen" in result.output
    as_json = CliRunner().invoke(
        cli, ["--json", "datasource", "tables", "pg", "--database", "db"]
    )
    assert json.loads(as_json.stdout) == [
        {"TABLE": "rna", "DATABASE": "db", "SCHEMA": "rnacen"}
    ]


@pytest.mark.parametrize(
    ("flags", "shown"),
    [([], 10), (["--limit", "3"], 3), (["--n", "2"], 2), (["-n", "1"], 1)],
)
def test_datasource_preview_bounds_the_rows_it_prints(mock_project, flags, shown):
    data = mock.MagicMock()
    data.features = [mock.MagicMock()]
    data.features[0].name, data.features[0].type = "id", "bigint"
    data.preview = {
        "preview": [{"values": [{"value0": "id", "value1": str(i)}]} for i in range(50)]
    }
    fs = mock_project.get_feature_store.return_value
    data_source = DataSource(storage_connector=mock.MagicMock(), table="t")
    data_source.storage_connector.get_data.return_value = data
    fs.get_data_source.return_value = data_source
    result = CliRunner().invoke(cli, ["--json", "datasource", "preview", "pg", *flags])
    assert result.exit_code == 0, result.output
    assert len(json.loads(result.stdout)["preview"]) == shown
    plain = CliRunner().invoke(cli, ["datasource", "preview", "pg", *flags])
    assert len(plain.output.strip().splitlines()) == shown + 2

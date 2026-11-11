"""CliRunner tests for external feature groups and other non-cached types."""

from __future__ import annotations

import inspect
import json
import types
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.commands.datasource import _preview_rows, _read_target
from hopsworks.cli.commands.fg import _truncate_cell
from hopsworks.cli.main import cli
from hsfs.core.data_source import DataSource
from hsfs.feature_group import ExternalFeatureGroup, SpineGroup
from hsfs.feature_store import FeatureStore
from hsfs.storage_connector import CRMSource


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


def test_create_external_features_help_does_not_claim_it_is_required():
    result = CliRunner().invoke(cli, ["fg", "create-external", "--help"])
    flat = " ".join(result.output.split())
    assert "Optional" in flat and "required outside Spark" not in flat


def test_datasource_preview_help_names_schema_table():
    result = CliRunner().invoke(cli, ["datasource", "preview", "--help"])
    assert "schema.table" in " ".join(result.output.split())


@pytest.mark.parametrize("json_mode", [False, True])
def test_datasource_preview_relays_the_rows_it_printed(
    mock_project, tmp_path, monkeypatch, json_mode
):
    events_file = tmp_path / "events.jsonl"
    monkeypatch.setenv("HOPS_EVENTS_FILE", str(events_file))
    ds = mock.MagicMock()
    ds.get_tables.return_value = []
    ds.get_data.return_value = types.SimpleNamespace(
        features=[types.SimpleNamespace(name=n, type="string") for n in ("id", "note")],
        preview={
            "preview": [
                {"values": [{"value0": "id", "value1": "1"}, {"value0": "note"}]},
                {
                    "values": [
                        {"value0": "id", "value1": "2"},
                        {"value0": "note", "value1": "line one\nline two " + "x" * 80},
                    ]
                },
            ]
        },
    )
    mock_project.get_feature_store.return_value.get_data_source.return_value = ds

    result = CliRunner().invoke(
        cli,
        [
            *(["--json"] if json_mode else []),
            "datasource",
            "preview",
            "neon",
            "--table",
            "orders",
        ],
    )

    assert result.exit_code == 0, result.output
    (line,) = [json.loads(x) for x in events_file.read_text().splitlines()]
    assert (line["kind"], line["op"], line["phase"]) == ("datasource", "read", "done")
    assert line["key"] == {"name": "neon"}
    assert line["detail"]["table"] == "orders"
    preview = events_file.parent / "previews" / f"{line['detail']['preview']}.json"
    note = _truncate_cell("line one\nline two " + "x" * 80)
    assert json.loads(preview.read_text()) == {
        "head": ["id", "note"],
        "rows": [["1", None], ["2", note]],
    }
    if not json_mode:
        assert note in result.output


def test_a_preview_line_names_the_source_when_nothing_else_names_the_read():
    ds = types.SimpleNamespace(table=None, path=None, query=None)
    assert _read_target(None, ds, "neon") == "neon"
    ds.path = "a.csv"
    assert _read_target(None, ds, "neon") == "a.csv"
    assert _read_target("orders", ds, "neon") == "orders"


def test_preview_rows_flattens_the_rows_the_backend_nests():
    nested = {
        "preview": [
            {
                "values": [
                    {"value0": "id", "value1": "387"},
                    {"value0": "city", "value1": "Paris"},
                ]
            },
            "not a row",
        ]
    }
    assert _preview_rows(nested) == [{"id": "387", "city": "Paris"}]
    assert _preview_rows(None) == []


def test_preview_rows_keeps_a_null_cell():
    # The backend leaves value1 out of a null cell (seen on Shopify products: image, published_at).
    nested = {
        "preview": [{"values": [{"value0": "id", "value1": "1"}, {"value0": "image"}]}]
    }
    assert _preview_rows(nested) == [{"id": "1", "image": None}]


def test_preview_rows_key_columns_by_their_feature_names():
    # Salesforce answers its own column case; the features beside it are lower case.
    nested = {
        "preview": [
            {
                "values": [
                    {"value0": "Id", "value1": "001"},
                    {"value0": "BillingPostalCode"},
                    {"value0": "Account Name", "value1": "Edge"},
                ]
            }
        ]
    }
    assert _preview_rows(nested) == [
        {"id": "001", "billingpostalcode": None, "account_name": "Edge"}
    ]


def test_info_shows_the_schema_of_an_external_fg(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = _external_fg(
        database="neondb", group="public", table="customers"
    )
    result = CliRunner().invoke(cli, ["fg", "info", "sales", "--version", "1"])
    assert result.exit_code == 0, result.output
    assert any(
        line.split()[:2] == ["Schema", "public"] for line in result.output.splitlines()
    ), result.output


def _crm_preview(table):
    return types.SimpleNamespace(
        features=[types.SimpleNamespace(name="id", type="string")],
        preview={"preview": [{"values": [{"value0": "id", "value1": f"{table}-1"}]}]},
    )


def _crm_source(mock_project, crm_type):
    connector = mock.MagicMock()
    connector.type = "CRM"
    connector.crm_type = crm_type
    connector.get_data.side_effect = lambda d, *a, **k: _crm_preview(d.table)
    connector.get_data_batch.side_effect = lambda reads, *a, **k: {
        d.table: _crm_preview(d.table) for d in reads
    }
    source = DataSource(storage_connector=connector)
    source.get_tables = mock.MagicMock(return_value=[])
    mock_project.get_feature_store.return_value.get_data_source.return_value = source
    return connector


def test_datasource_preview_reads_several_crm_tables_with_one_batch_call(
    mock_project, tmp_path, monkeypatch
):
    events_file = tmp_path / "events.jsonl"
    monkeypatch.setenv("HOPS_EVENTS_FILE", str(events_file))
    connector = _crm_source(mock_project, CRMSource.SALESFORCE)

    result = CliRunner().invoke(
        cli,
        [
            "--json",
            "datasource",
            "preview",
            "sf",
            "--table",
            "account",
            "--table",
            "lead",
        ],
    )

    assert result.exit_code == 0, result.output
    connector.get_data_batch.assert_called_once()
    assert [d.table for d in connector.get_data_batch.call_args.args[0]] == [
        "account",
        "lead",
    ]
    connector.get_data.assert_not_called()
    shown = json.loads(result.output)
    assert shown["account"]["preview"] == [{"id": "account-1"}]
    assert shown["lead"]["preview"] == [{"id": "lead-1"}]
    lines = [json.loads(x) for x in events_file.read_text().splitlines()]
    assert [line["detail"]["table"] for line in lines] == ["account", "lead"]


def test_datasource_preview_reads_google_analytics_tables_one_by_one(mock_project):
    connector = _crm_source(mock_project, CRMSource.GOOGLE_ANALYTICS)

    result = CliRunner().invoke(
        cli, ["datasource", "preview", "ga", "--table", "sessions", "--table", "pages"]
    )

    assert result.exit_code == 0, result.output
    connector.get_data_batch.assert_not_called()
    assert [c.args[0].table for c in connector.get_data.call_args_list] == [
        "sessions",
        "pages",
    ]
    assert "sessions-1" in result.output and "pages-1" in result.output


def test_datasource_preview_of_one_crm_table_keeps_the_single_read(mock_project):
    connector = _crm_source(mock_project, CRMSource.SALESFORCE)

    result = CliRunner().invoke(
        cli, ["--json", "datasource", "preview", "sf", "--table", "account"]
    )

    assert result.exit_code == 0, result.output
    connector.get_data_batch.assert_not_called()
    assert json.loads(result.output)["preview"] == [{"id": "account-1"}]

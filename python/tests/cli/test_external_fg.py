"""CliRunner tests for external feature groups and other non-cached types."""

from __future__ import annotations

import inspect
import json
import types
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.commands.datasource import _preview_rows
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


def test_ingest_makes_a_sink_group_on_the_table_and_runs_its_job(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = DataSource(storage_connector=mock.MagicMock())
    fg = fs.create_feature_group.return_value
    fg.name, fg.version = "sf_accounts", 1
    fg.sink_job.name = "my_salesforce_to_sf_accounts_1"
    result = CliRunner().invoke(
        cli,
        [
            "fg",
            "ingest",
            "sf_accounts",
            "--connector",
            "my_salesforce",
            "--table",
            "account",
            "--primary-key",
            "id",
            "--no-wait",
        ],
    )
    assert result.exit_code == 0, result.output
    kwargs = fs.create_feature_group.call_args.kwargs
    assert kwargs["sink_enabled"] is True
    assert kwargs["data_source"].table == "account"
    assert kwargs["primary_key"] == ["id"]
    assert "features" not in kwargs
    fg.save.assert_called_once_with()
    fg.sink_job.run.assert_called_once()
    assert "my_salesforce_to_sf_accounts_1" in result.output


def _ingest_args(*extra):
    return [
        "fg",
        "ingest",
        "events",
        "--connector",
        "pg",
        "--table",
        "events",
        "--primary-key",
        "id",
        "--no-wait",
        *extra,
    ]


def _ingest_project(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = DataSource(storage_connector=mock.MagicMock())
    fg = fs.create_feature_group.return_value
    fg.name, fg.version = "events", 1
    fg.sink_job.name = "pg_to_events_1"
    return fs


def test_ingest_incremental_sends_strategy_cursor_and_start(mock_project):
    fs = _ingest_project(mock_project)
    result = CliRunner().invoke(
        cli,
        _ingest_args(
            "--strategy",
            "incremental-timestamp",
            "--cursor",
            "updated_at",
            "--start",
            "2026-11-01T00:00:00Z",
        ),
    )
    assert result.exit_code == 0, result.output
    conf = fs.create_feature_group.call_args.kwargs["sink_job_conf"]
    assert conf.loading_config.to_dict()["loadingStrategy"] == "INCREMENTAL_TIMESTAMP"
    assert conf.loading_config.to_dict()["incrementalLoadingConfig"] == {
        "sourceCursorField": "updated_at",
        "initialValue": 1793491200000,
    }


def test_ingest_full_keeps_the_backend_defaults(mock_project):
    fs = _ingest_project(mock_project)
    result = CliRunner().invoke(cli, _ingest_args())
    assert result.exit_code == 0, result.output
    assert fs.create_feature_group.call_args.kwargs["sink_job_conf"] is None


@pytest.mark.parametrize(
    "extra, expected", [((), None), (("--no-statistics",), False)]
)
def test_ingest_statistics_off_only_when_asked(mock_project, extra, expected):
    fs = _ingest_project(mock_project)
    result = CliRunner().invoke(cli, [*_ingest_args(), *extra])
    assert result.exit_code == 0, result.output
    assert fs.create_feature_group.call_args.kwargs["statistics_config"] is expected


@pytest.mark.parametrize(
    "extra, message",
    [
        (("--strategy", "incremental-id"), "--strategy incremental-id needs --cursor"),
        (("--cursor", "id"), "--cursor and --start go with an incremental --strategy"),
        (("--start", "5"), "--cursor and --start go with an incremental --strategy"),
    ],
)
def test_ingest_refuses_a_cursor_mismatch_before_any_call(mock_project, extra, message):
    fs = _ingest_project(mock_project)
    result = CliRunner().invoke(cli, _ingest_args(*extra))
    assert result.exit_code == 2, result.output
    assert message in result.output
    fs.create_feature_group.assert_not_called()


def test_a_cell_keeps_its_row_on_one_line():
    from hopsworks.cli.commands.fg import _truncate_cell

    assert _truncate_cell("312 Constitution Place\nAustin, TX 78767\nUSA") == (
        "312 Constitution Place Austin, TX 78767 USA"
    )


def _listing(*schemas):
    source = DataSource(storage_connector=mock.MagicMock())
    source.get_tables = lambda database=None: [
        DataSource(storage_connector=mock.MagicMock(), group=g, table="orders")
        for g in schemas
    ]
    return source


def test_ingest_keeps_the_schema_of_a_listed_table(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = _listing("sales", "archive")
    result = CliRunner().invoke(
        cli,
        [
            "fg",
            "ingest",
            "orders",
            "--connector",
            "pg",
            "--table",
            "sales.orders",
            "--primary-key",
            "id",
            "--no-wait",
        ],
    )
    assert result.exit_code == 0, result.output
    picked = fs.create_feature_group.call_args.kwargs["data_source"]
    assert (picked.group, picked.table) == ("sales", "orders")


def test_ingest_refuses_a_bare_name_listed_in_several_schemas(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = _listing("sales", "archive")
    result = CliRunner().invoke(
        cli,
        ["fg", "ingest", "orders", "--connector", "pg", "--table", "orders"]
        + ["--primary-key", "id"],
    )
    assert result.exit_code != 0
    assert "several schemas" in result.output
    fs.create_feature_group.assert_not_called()


@pytest.mark.parametrize("wait", ["--no-wait", "--wait"])
def test_ingest_json_stdout_is_the_payload_only(mock_project, wait):
    # save() and run() print banners; --json stdout must stay one JSON object.
    execution = types.SimpleNamespace(
        id=5, state="RUNNING", final_status=None, submission_time=None
    )

    def run(await_termination=False, **_):
        print("Launching job: sf_accounts_1")
        return execution

    fg = types.SimpleNamespace(
        id=3,
        name="sf_accounts",
        version=1,
        sink_job=types.SimpleNamespace(name="my_salesforce_to_sf_accounts_1", run=run),
        save=lambda: print(
            "Feature Group created successfully, explore it at https://x"
        ),
    )
    fs = mock_project.get_feature_store.return_value
    fs.get_data_source.return_value = DataSource(storage_connector=mock.MagicMock())
    fs.create_feature_group.return_value = fg
    result = CliRunner().invoke(
        cli,
        [
            "--json",
            "fg",
            "ingest",
            "sf_accounts",
            "--connector",
            "sf",
            "--table",
            "account",
            "--primary-key",
            "id",
            wait,
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["job"] == "my_salesforce_to_sf_accounts_1"
    assert payload["execution"]["id"] == 5


def test_ingest_without_a_primary_key_is_refused_before_any_call(mock_project):
    result = CliRunner().invoke(
        cli, ["fg", "ingest", "sf_accounts", "--connector", "sf", "--table", "account"]
    )
    assert result.exit_code == 2
    assert "--primary-key" in result.output
    mock_project.get_feature_store.assert_not_called()


def test_create_external_refuses_a_crm_source_and_names_ingest(mock_project):
    fs = mock_project.get_feature_store.return_value
    connector = mock.MagicMock()
    connector.type = "CRM"
    connector.crm_type = CRMSource.HUBSPOT
    fs.get_data_source.return_value = DataSource(storage_connector=connector)
    result = CliRunner().invoke(
        cli,
        ["fg", "create-external", "deals", "--connector", "my_hubspot", "--table", "deals"],
    )
    assert result.exit_code == 2, result.output
    assert "hops fg ingest deals --connector my_hubspot" in result.output
    fs.create_external_feature_group.assert_not_called()


def test_create_external_refuses_google_analytics_without_an_ingest_hint(mock_project):
    fs = mock_project.get_feature_store.return_value
    connector = mock.MagicMock()
    connector.type = "CRM"
    connector.crm_type = CRMSource.GOOGLE_ANALYTICS
    fs.get_data_source.return_value = DataSource(storage_connector=connector)
    result = CliRunner().invoke(
        cli, ["fg", "create-external", "ga", "--connector", "my_ga", "--table", "traffic"]
    )
    assert result.exit_code == 2, result.output
    assert "cannot be read in place" in result.output
    assert "fg ingest" not in result.output
    fs.create_external_feature_group.assert_not_called()

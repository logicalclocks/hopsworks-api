"""CliRunner tests for the Superset MCP commands: chart generate, chart types, dashboard update."""

from __future__ import annotations

import json
from unittest import mock

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli


@pytest.fixture
def api(mock_project):
    api = mock.MagicMock()
    mock_project.get_superset_api.return_value = api
    with mock.patch(
        "hopsworks.cli.commands.superset.util._get_hostname_replaced_url",
        side_effect=lambda path: f"https://hops.example/{path}",
    ):
        yield api


def test_generate_builds_an_xy_config_from_flags(api):
    api.generate_chart.return_value = {
        "chart": {"id": 41, "slice_name": "Spend per day"}
    }

    result = CliRunner().invoke(
        cli,
        [
            "superset",
            "chart",
            "generate",
            "--dataset",
            "12",
            "--type",
            "xy",
            "--kind",
            "bar",
            "--x",
            "event_time",
            "--y",
            "SUM(amount),revenue",
            "--y",
            "COUNT(*)",
            "--group-by",
            "category",
            "--time-grain",
            "P1D",
            "--filter",
            "country IN SE,NO",
            "--filter",
            "amount > 100",
            "--filter",
            "email IS NULL",
            "--name",
            "Spend per day",
        ],
    )

    assert result.exit_code == 0, result.output
    dataset, config = api.generate_chart.call_args.args
    assert dataset == 12
    assert config == {
        "chart_type": "xy",
        "kind": "bar",
        "x": {"name": "event_time"},
        "y": [
            {"name": "amount", "aggregate": "SUM"},
            {"name": "revenue", "saved_metric": True},
            {"sql_expression": "COUNT(*)", "label": "COUNT(*)"},
        ],
        "group_by": [{"name": "category"}],
        "time_grain": "P1D",
        "filters": [
            {"column": "country", "op": "IN", "value": ["SE", "NO"]},
            {"column": "amount", "op": ">", "value": 100},
            {"column": "email", "op": "IS NULL"},
        ],
    }
    assert api.generate_chart.call_args.kwargs == {
        "name": "Spend per day",
        "save": True,
    }
    assert "41" in result.output
    assert (
        "https://hops.example/hopsworks-api/superset/explore/?slice_id=41"
        in result.output
    )


def test_generate_preview_leaves_the_chart_unsaved(api):
    api.generate_chart.return_value = {"chart": None, "form_data_key": "k1"}

    result = CliRunner().invoke(
        cli,
        [
            "superset",
            "chart",
            "generate",
            "--dataset",
            "12",
            "--type",
            "big_number",
            "--metric",
            "COUNT(*)",
            "--preview",
        ],
    )

    assert result.exit_code == 0, result.output
    assert api.generate_chart.call_args.kwargs["save"] is False
    assert api.generate_chart.call_args.args[1]["metric"] == {
        "sql_expression": "COUNT(*)",
        "label": "COUNT(*)",
    }
    assert "explore/?form_data_key=k1" in result.output


def test_generate_flags_override_a_config_file(api, tmp_path):
    config_file = tmp_path / "chart.json"
    config_file.write_text(
        json.dumps({"chart_type": "table", "columns": [{"name": "a"}], "row_limit": 50})
    )
    api.generate_chart.return_value = {"chart": {"id": 1}}

    result = CliRunner().invoke(
        cli,
        [
            "superset",
            "chart",
            "generate",
            "--dataset",
            "uuid-1",
            "--config",
            f"@{config_file}",
            "--columns",
            "b,SUM(c)",
        ],
    )

    assert result.exit_code == 0, result.output
    dataset, config = api.generate_chart.call_args.args
    assert dataset == "uuid-1"
    assert config == {
        "chart_type": "table",
        "columns": [{"name": "b"}, {"name": "c", "aggregate": "SUM"}],
        "row_limit": 50,
    }


def test_generate_without_a_type_is_a_usage_error(api):
    result = CliRunner().invoke(
        cli, ["superset", "chart", "generate", "--dataset", "1", "--y", "x"]
    )

    assert result.exit_code == 2
    assert "--type" in result.output
    api.generate_chart.assert_not_called()


def test_generate_reports_supersets_refusal(api):
    api.generate_chart.side_effect = Exception(
        "Superset generate_chart failed: column amt not found"
    )

    result = CliRunner().invoke(
        cli,
        [
            "superset",
            "chart",
            "generate",
            "--dataset",
            "1",
            "--type",
            "xy",
            "--y",
            "SUM(amt)",
        ],
    )

    assert result.exit_code == 1
    assert "column amt not found" in result.output


def test_a_malformed_filter_is_rejected_before_calling(api):
    result = CliRunner().invoke(
        cli,
        [
            "superset",
            "chart",
            "generate",
            "--dataset",
            "1",
            "--type",
            "xy",
            "--y",
            "n",
            "--filter",
            "amount>100",
        ],
    )

    assert result.exit_code == 2
    assert "column op value" in result.output
    api.generate_chart.assert_not_called()


def test_types_lists_the_core_types_without_calling_superset(api):
    result = CliRunner().invoke(cli, ["superset", "chart", "types"])

    assert result.exit_code == 0, result.output
    assert "big_number" in result.output
    api.chart_type_schema.assert_not_called()


def test_types_prints_one_schema(api):
    api.chart_type_schema.return_value = {
        "chart_type": "pie",
        "schema": {"type": "object"},
    }

    result = CliRunner().invoke(cli, ["superset", "chart", "types", "pie"])

    assert result.exit_code == 0, result.output
    api.chart_type_schema.assert_called_once_with("pie")
    assert json.loads(result.output)["chart_type"] == "pie"


def test_dashboard_update_sends_only_the_options_given(api, tmp_path):
    css = tmp_path / "print.css"
    css.write_text(".header-controls {display: none;}")
    api.patch_dashboard.return_value = {"dashboard": {"id": 7}}

    result = CliRunner().invoke(
        cli,
        [
            "superset",
            "dashboard",
            "update",
            "7",
            "--title",
            "Fraud",
            "--published",
            "--refresh",
            "300",
            "--filter-bar",
            "horizontal",
            "--css",
            f"@{css}",
            "--metadata",
            '{"color_scheme": "supersetColors"}',
            "--tags",
            "3,7",
        ],
    )

    assert result.exit_code == 0, result.output
    api.patch_dashboard.assert_called_once_with(
        7,
        dashboard_title="Fraud",
        published=True,
        css=".header-controls {display: none;}",
        refresh_frequency=300,
        filter_bar_orientation="HORIZONTAL",
        tags=[3, 7],
        json_metadata_overrides={"color_scheme": "supersetColors"},
    )
    assert "hopsworks-api/superset/dashboard/7/" in result.output


def test_dashboard_update_with_nothing_to_change_is_a_usage_error(api):
    result = CliRunner().invoke(cli, ["superset", "dashboard", "update", "7"])

    assert result.exit_code == 2
    api.patch_dashboard.assert_not_called()


def test_dashboard_update_rejects_bad_json(api):
    result = CliRunner().invoke(
        cli, ["superset", "dashboard", "update", "7", "--metadata", "{nope"]
    )

    assert result.exit_code == 2
    assert "not valid JSON" in result.output
    api.patch_dashboard.assert_not_called()


def test_commas_inside_parentheses_and_quotes_do_not_split():
    from hopsworks.cli.commands import superset

    assert superset._split(("SUM(COALESCE(amount, 0)),revenue", "'a,b'")) == [
        "SUM(COALESCE(amount, 0))",
        "revenue",
        "'a,b'",
    ]


def test_a_quoted_filter_value_stays_a_string():
    from hopsworks.cli.commands import superset

    assert superset._filter("postal_code = '00123'") == {
        "column": "postal_code",
        "op": "=",
        "value": "00123",
    }
    assert superset._filter("amount > 100")["value"] == 100
    assert superset._filter("region IN 'EU,North', 7")["value"] == ["EU,North", 7]


@pytest.mark.parametrize(
    ("text", "value"),
    [
        ("country IN (SE,NO)", ["SE", "NO"]),
        ("country NOT IN ( SE , 'N,O' )", ["SE", "N,O"]),
        ("country IN '(SE)', NO", ["(SE)", "NO"]),
    ],
)
def test_an_in_list_may_be_parenthesized(text, value):
    from hopsworks.cli.commands import superset

    assert superset._filter(text)["value"] == value


@pytest.mark.parametrize(
    "text",
    [
        "country IN ()",
        "country IN",
        "country IN (SE),(NO)",
        "country IN (SE)(NO)",
        "email IS NULL x",
        "email IS NOT NULL = 1",
    ],
)
def test_a_malformed_list_or_a_value_after_is_null_is_refused(text):
    from hopsworks.cli.commands import superset

    with pytest.raises(click.BadParameter):
        superset._filter(text)

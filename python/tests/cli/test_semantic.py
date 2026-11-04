"""Tests for ``hops semantic``: argument parsing into SDK calls, output, and exit codes."""

from __future__ import annotations

import json
from unittest import mock

import pandas as pd
import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli
from hopsworks_common.client.exceptions import (
    SemanticBuildException,
    SemanticLayerException,
)
from hopsworks_common.semantic_layer import (
    SemanticBuild,
    SemanticIssue,
    SemanticMetric,
)


@pytest.fixture
def sl(mock_project):
    api = mock.MagicMock(name="SemanticLayerApi")
    mock_project.get_semantic_layer_api.return_value = api
    return api


@pytest.fixture
def sales(sl):
    sp = mock.MagicMock(name="SemanticProject")
    sp.name = "sales"
    sp.current_build_id = 12
    sl.get_project.return_value = sp
    return sp


def _run(*args):
    return CliRunner().invoke(cli, ["semantic", *args])


def _invalid_build():
    return SemanticBuild(
        id=13,
        status="INVALID",
        error_count=1,
        warning_count=0,
        issues=[
            SemanticIssue(
                level="ERROR",
                rule="UNIQUE_NAMES",
                message="metric revenue is defined twice",
                file="models/semantic.yml",
                line=12,
                column=5,
            )
        ],
    )


class TestQueryFlags:
    def test_sql_parses_lists_repeats_and_descending_order(self, sales):
        sales.sql.return_value = "SELECT 1"

        result = _run(
            "sql",
            "sales",
            "--metrics",
            "revenue,order_count",
            "--group-by",
            "metric_time__month,customer__country",
            "--group-by",
            "order__status",
            "--where",
            "{{ Dimension('customer__country') }} IN ('SE', 'US')",
            "--where",
            "{{ Dimension('order__status') }} = 'paid'",
            "--order-by",
            "-revenue,metric_time__month",
            "--limit",
            "5",
        )

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "SELECT 1"
        sales.sql.assert_called_once_with(
            metrics=["revenue", "order_count"],
            group_by=["metric_time__month", "customer__country", "order__status"],
            # A comma inside a filter does not split it.
            where=[
                "{{ Dimension('customer__country') }} IN ('SE', 'US')",
                "{{ Dimension('order__status') }} = 'paid'",
            ],
            order_by=["-revenue", "metric_time__month"],
            limit=5,
            time_range=None,
        )

    def test_order_is_an_alias_and_times_become_a_range(self, sales):
        sales.sql.return_value = "SELECT 1"

        result = _run(
            "sql",
            "sales",
            "--metrics",
            "revenue",
            "--order",
            "-metric_time",
            "--start-time",
            "2026-01-01",
        )

        assert result.exit_code == 0, result.output
        kwargs = sales.sql.call_args.kwargs
        assert kwargs["order_by"] == ["-metric_time"]
        assert kwargs["time_range"] == ("2026-01-01", None)
        assert kwargs["group_by"] is None and kwargs["where"] is None

    def test_metrics_are_required(self, sales):
        result = _run("sql", "sales")

        assert result.exit_code == 2
        assert "--metrics" in result.output

    def test_unknown_project_names_the_list_command(self, sl):
        sl.get_project.return_value = None

        result = _run("sql", "nope", "--metrics", "revenue")

        assert result.exit_code == 1
        assert "hops semantic list" in result.output


class TestQueryOutput:
    @pytest.fixture
    def frame(self, sales):
        sales.query.return_value = pd.DataFrame(
            {
                "metric_time__month": ["2026-01-01", "2026-02-01"],
                "revenue": [10.5, None],
            }
        )
        return sales

    def test_table(self, frame):
        result = _run("query", "sales", "--metrics", "revenue")

        assert result.exit_code == 0, result.output
        assert frame.query.call_args.kwargs["dataframe_type"] == "pandas"
        lines = result.output.splitlines()
        assert lines[0].split() == ["metric_time__month", "revenue"]
        assert lines[2].split() == ["2026-01-01", "10.5"]

    def test_csv(self, frame):
        result = _run("query", "sales", "--metrics", "revenue", "--output", "csv")

        assert result.output.splitlines() == [
            "metric_time__month,revenue",
            "2026-01-01,10.5",
            "2026-02-01,",
        ]

    @pytest.mark.parametrize(
        "flags", [["--output", "json"], ["--json"], ["-o", "json"]]
    )
    def test_json_prints_missing_values_as_null(self, frame, flags):
        result = _run("query", "sales", "--metrics", "revenue", *flags)

        assert json.loads(result.output) == [
            {"metric_time__month": "2026-01-01", "revenue": 10.5},
            {"metric_time__month": "2026-02-01", "revenue": None},
        ]

    def test_gateway_errors_print_as_one_line(self, sales):
        sales.query.side_effect = SemanticLayerException(
            "Semantic layer request on 'sales' failed (HTTP 400, "
            "SEMANTIC_LAYER_INVALID_QUERY): unknown metric 'revnue'",
            status_code=400,
            error_code="SEMANTIC_LAYER_INVALID_QUERY",
        )

        result = _run("query", "sales", "--metrics", "revnue")

        assert result.exit_code == 1
        assert "unknown metric 'revnue'" in result.output
        assert "Traceback" not in result.output


class TestCompile:
    def test_without_wait_does_not_block(self, sales):
        sales.compile.return_value = SemanticBuild(id=13, status="COMPILING")

        result = _run("compile", "sales")

        assert result.exit_code == 0, result.output
        sales.compile.assert_called_once_with(wait=False)
        assert "Build 13: COMPILING" in result.output

    def test_an_invalid_build_exits_zero_without_fail_on_error(self, sales):
        sales.compile.side_effect = SemanticBuildException("sales", _invalid_build())

        result = _run("compile", "sales", "--wait")

        assert result.exit_code == 0
        sales.compile.assert_called_once_with(wait=True)
        assert "UNIQUE_NAMES" in result.output
        assert "models/semantic.yml:12:5" in result.output

    def test_fail_on_error_implies_wait_and_exits_one(self, sales):
        sales.compile.side_effect = SemanticBuildException("sales", _invalid_build())

        result = _run("compile", "sales", "--fail-on-error")

        assert result.exit_code == 1
        sales.compile.assert_called_once_with(wait=True)
        assert "Build 13: INVALID (1 errors, 0 warnings)" in result.output

    def test_fail_on_error_passes_on_a_valid_build_and_shows_warnings(self, sales):
        sales.compile.return_value = SemanticBuild(
            id=14, status="VALID", warning_count=1
        )
        sales.get_build.return_value = SemanticBuild(
            id=14,
            status="VALID",
            warning_count=1,
            issues=[SemanticIssue(level="WARNING", rule="PRIMARY_KEY", message="m")],
        )

        result = _run("compile", "sales", "--fail-on-error", "--json")

        assert result.exit_code == 0, result.output
        sales.get_build.assert_called_once_with(14)
        assert json.loads(result.output)["issues"][0]["rule"] == "PRIMARY_KEY"


class TestProjects:
    def test_create_passes_path_format_and_mode(self, sl):
        sl.create_project.return_value = mock.MagicMock(name="sp", id=7)

        result = _run(
            "create",
            "sales",
            "--path",
            "Jupyter/repo/dbt",
            "--format",
            "dbt",
            "--mode",
            "pinned",
        )

        assert result.exit_code == 0, result.output
        sl.create_project.assert_called_once_with(
            "sales", "Jupyter/repo/dbt", format="DBT", deployment_mode="PINNED"
        )

    def test_create_needs_a_path(self, sl):
        result = _run("create", "sales")

        assert result.exit_code == 2
        assert "--path" in result.output

    def test_delete_with_yes(self, sl):
        result = _run("delete", "sales", "--yes")

        assert result.exit_code == 0, result.output
        sl.delete_project.assert_called_once_with("sales")

    def test_promote_takes_an_integer_build(self, sales):
        sales.promote.return_value = sales

        result = _run("promote", "sales", "12")

        assert result.exit_code == 0, result.output
        sales.promote.assert_called_once_with(12)
        assert _run("promote", "sales", "latest").exit_code == 2

    def test_builds_marks_the_current_one(self, sales):
        sales.get_builds.return_value = [
            SemanticBuild(id=13, status="INVALID"),
            SemanticBuild(id=12, status="VALID"),
        ]

        result = _run("builds", "sales", "--json")

        assert [(b["id"], b["current"]) for b in json.loads(result.output)] == [
            (13, False),
            (12, True),
        ]

    def test_dimensions_filters_by_metrics(self, sales):
        sales.dimensions.return_value = []

        result = _run("dimensions", "sales", "--metrics", "revenue,order_count")

        assert result.exit_code == 0, result.output
        sales.dimensions.assert_called_once_with(metrics=["revenue", "order_count"])

    def test_metrics_table(self, sales):
        sales.list_metrics.return_value = [
            SemanticMetric(
                name="revenue", type="SIMPLE", agg="SUM", description="Net\nmore"
            )
        ]

        result = _run("metrics", "sales")

        assert result.exit_code == 0, result.output
        assert result.output.splitlines()[0].split() == [
            "NAME",
            "TYPE",
            "AGG",
            "LABEL",
            "TAGS",
            "DESCRIPTION",
        ]
        assert result.output.splitlines()[2].split() == [
            "revenue",
            "SIMPLE",
            "SUM",
            "-",
            "-",
            "Net",
        ]
        sales.list_metrics.assert_called_once_with(tag=None)

    def test_metrics_tag_filter_repeats_and_the_table_names_each_schema(self, sales):
        sales.list_metrics.return_value = [
            SemanticMetric(
                name="revenue",
                type="SIMPLE",
                tags={"ownership": {"owner": "finance"}, "domain": "sales"},
            )
        ]

        result = _run("metrics", "sales", "--tag", "ownership", "--tag", "domain")

        assert result.exit_code == 0, result.output
        sales.list_metrics.assert_called_once_with(tag=["ownership", "domain"])
        assert result.output.splitlines()[2].split()[4] == "domain,ownership"

    def test_create_passes_tags_parsed_as_json(self, sl):
        sl.create_project.return_value = mock.MagicMock(name="sp", id=7)

        result = _run(
            "create",
            "sales",
            "--path",
            "dbt",
            "--tag",
            'ownership={"owner": "finance"}',
            "--tag",
            "domain=sales",
        )

        assert result.exit_code == 0, result.output
        assert sl.create_project.call_args.kwargs["tags"] == {
            "ownership": {"owner": "finance"},
            "domain": "sales",
        }

    def test_create_refuses_a_tag_without_a_value(self, sl):
        result = _run("create", "sales", "--path", "dbt", "--tag", "ownership")

        assert result.exit_code == 2
        assert "NAME=VALUE" in result.output
        sl.create_project.assert_not_called()


class TestTags:
    def test_list_prints_each_value_as_json(self, sales):
        sales.get_tags.return_value = {"ownership": {"owner": "finance"}, "tier": 1}

        result = _run("tag", "list", "sales")

        assert result.exit_code == 0, result.output
        lines = result.output.splitlines()
        assert lines[0].split() == ["NAME", "VALUE"]
        assert lines[2].split(maxsplit=1) == ["ownership", '{"owner": "finance"}']
        assert lines[3].split() == ["tier", "1"]

    def test_list_in_json_is_the_mapping(self, sales):
        sales.get_tags.return_value = {"tier": 1}

        result = _run("--json", "tag", "list", "sales")

        assert json.loads(result.output) == {"tier": 1}

    def test_get_prints_the_value(self, sales):
        sales.get_tag.return_value = {"owner": "finance"}

        result = _run("tag", "get", "sales", "ownership")

        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {"owner": "finance"}
        sales.get_tag.assert_called_once_with("ownership")

    def test_get_of_a_missing_tag_exits_one(self, sales):
        sales.get_tag.return_value = None

        result = _run("tag", "get", "sales", "ownership")

        assert result.exit_code == 1
        assert "has no tag 'ownership'" in result.output

    @pytest.mark.parametrize(
        ("raw", "value"),
        [('{"owner": "finance"}', {"owner": "finance"}), ("sales", "sales"), ("3", 3)],
    )
    def test_set_parses_json_and_falls_back_to_a_string(self, sales, raw, value):
        result = _run("tag", "set", "sales", "domain", raw)

        assert result.exit_code == 0, result.output
        sales.add_tag.assert_called_once_with("domain", value)

    def test_delete(self, sales):
        result = _run("tag", "delete", "sales", "domain")

        assert result.exit_code == 0, result.output
        sales.delete_tag.assert_called_once_with("domain")

    def test_an_unknown_project_names_the_list_command(self, sl):
        sl.get_project.return_value = None

        result = _run("tag", "list", "nope")

        assert result.exit_code == 1
        assert "hops semantic list" in result.output


class TestDiscovery:
    def test_metrics_narrowed_to_those_compatible_with_group_bys(self, sales):
        sales.list_metrics.return_value = [
            SemanticMetric(name="revenue", type="SIMPLE"),
            SemanticMetric(name="bookings", type="SIMPLE"),
        ]
        sales.compatible_metrics.return_value = ["revenue"]

        result = _run("--json", "metrics", "sales", "--group-by", "customer__country")

        assert result.exit_code == 0, result.output
        sales.compatible_metrics.assert_called_once_with(["customer__country"])
        assert [m["name"] for m in json.loads(result.output)] == ["revenue"]

    def test_dimension_values_pass_the_group_by_metrics_and_limit(self, sales):
        sales.dimension_values.return_value = ["SE", None]

        result = _run(
            "dimension-values", "sales", "customer__country", "--metrics", "revenue"
        )

        assert result.exit_code == 0, result.output
        sales.dimension_values.assert_called_once_with(
            "customer__country", metrics=["revenue"], limit=100
        )
        assert result.output.splitlines() == ["SE", ""]

    def test_build_shows_its_issues(self, sales):
        sales.get_build.return_value = _invalid_build()

        result = _run("build", "sales", "13")

        assert result.exit_code == 0, result.output
        sales.get_build.assert_called_once_with(13)
        assert "models/semantic.yml:12:5" in result.output


class TestProjectSettings:
    def test_update_needs_a_setting(self, sales):
        result = _run("update", "sales")

        assert result.exit_code == 2
        sales.update.assert_not_called()

    def test_update_passes_only_the_given_settings(self, sales):
        result = _run("update", "sales", "--mode", "pinned")

        assert result.exit_code == 0, result.output
        sales.update.assert_called_once_with(
            root_path=None, format=None, deployment_mode="PINNED"
        )

    def test_show_reports_why_nothing_is_served(self, sales):
        sales.serving.side_effect = SemanticLayerException(
            "no valid build", error_msg="semantic project 7 has no valid build"
        )

        result = _run("show", "sales")

        assert result.exit_code == 0, result.output
        assert "has no valid build" in result.output


class TestExplainAndTest:
    def test_explain_prints_the_sql_without_running_the_query(self, sales):
        sales.sql.return_value = "SELECT 1"

        result = _run("query", "sales", "--metrics", "revenue", "--explain")

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "SELECT 1"
        sales.query.assert_not_called()

    def test_test_compiles_waits_for_the_new_build_and_runs_every_metric(
        self, sales, mocker
    ):
        mocker.patch("hopsworks.cli.commands.semantic.time.sleep")
        sales.compile.return_value = SemanticBuild(id=12, status="VALID")
        sales.serving.return_value = mock.MagicMock(build_id=12)
        sales.list_metrics.return_value = [
            SemanticMetric(name="revenue", type="SIMPLE"),
            SemanticMetric(name="bookings", type="SIMPLE"),
        ]
        sales.query.return_value = [{"revenue": 1.0}]

        result = _run("test", "sales")

        assert result.exit_code == 0, result.output
        sales.compile.assert_called_once_with(wait=True)
        assert [c.args[0] for c in sales.query.call_args_list] == [
            ["revenue"],
            ["bookings"],
        ]
        assert "2 of 2 metrics ran" in result.output

    def test_test_exits_one_when_a_metric_fails_on_the_engine(self, sales):
        sales.list_metrics.return_value = [
            SemanticMetric(name="revenue", type="SIMPLE"),
            SemanticMetric(name="bookings", type="SIMPLE"),
        ]
        sales.query.side_effect = [
            [{"revenue": 1.0}],
            SemanticLayerException("failed", error_msg="the query failed on Trino"),
        ]

        result = _run("--json", "test", "sales", "--no-compile")

        assert result.exit_code == 1
        sales.compile.assert_not_called()
        statuses = {r["metric"]: r["status"] for r in json.loads(result.output)}
        assert statuses == {"revenue": "PASS", "bookings": "FAIL"}

    def test_test_refuses_a_compiled_build_that_is_not_promoted(self, sales):
        sales.compile.return_value = SemanticBuild(id=13, status="VALID")

        result = _run("--json", "test", "sales")

        assert result.exit_code == 1
        sales.query.assert_not_called()
        assert "hops semantic promote sales 13" in result.output

    def test_test_refuses_while_the_gateway_serves_another_build(self, sales, mocker):
        mocker.patch("hopsworks.cli.commands.semantic.time.sleep")
        mocker.patch("hopsworks.cli.commands.semantic._SERVING_WAIT_SECONDS", 0)
        sales.compile.return_value = SemanticBuild(id=12, status="VALID")
        sales.serving.return_value = mock.MagicMock(build_id=11)

        result = _run("test", "sales")

        assert result.exit_code == 1
        sales.query.assert_not_called()
        assert "still serves build 11, not build 12" in result.output

    def test_test_stops_at_an_invalid_build(self, sales):
        sales.compile.side_effect = SemanticBuildException("sales", _invalid_build())

        result = _run("test", "sales")

        assert result.exit_code == 1
        sales.query.assert_not_called()
        assert "UNIQUE_NAMES" in result.output

"""Tests for ``hops semantic``: argument parsing into SDK calls, output, and exit codes."""

from __future__ import annotations

import json
from unittest import mock

import pandas as pd
import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli
from hopsworks_common.client.exceptions import (
    JobExecutionException,
    SemanticBuildException,
    SemanticLayerException,
)
from hopsworks_common.semantic_layer import (
    SemanticBuild,
    SemanticExport,
    SemanticIssue,
    SemanticMetric,
    SemanticProject,
    SemanticSavedQuery,
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

    def test_no_cache_skips_the_result_cache(self, frame):
        result = _run("query", "sales", "--metrics", "revenue", "--no-cache")

        assert result.exit_code == 0, result.output
        assert frame.query.call_args.kwargs["read_cache"] is False
        assert _run("query", "sales", "--metrics", "revenue").exit_code == 0
        assert frame.query.call_args.kwargs["read_cache"] is True

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


class TestDashboard:
    DASHBOARD = {
        "id": 12,
        "title": "Sales overview",
        "url": "https://h/hopsworks-api/superset/dashboard/12/?projectId=119",
    }

    def test_dashboard_flattens_metrics_and_prints_the_url(self, sales):
        sales.create_dashboard.return_value = self.DASHBOARD

        result = _run(
            "dashboard",
            "sales",
            "--metrics",
            "revenue,orders",
            "--metrics",
            "average_order_value",
            "--group-by",
            "customer__country",
            "--title",
            "Sales overview",
        )

        assert result.exit_code == 0, result.output
        sales.create_dashboard.assert_called_once_with(
            ["revenue", "orders", "average_order_value"],
            group_by=["customer__country"],
            title="Sales overview",
        )
        assert result.output.strip() == (
            'Created dashboard 12 "Sales overview": '
            "https://h/hopsworks-api/superset/dashboard/12/?projectId=119"
        )

    def test_dashboard_in_json_is_id_title_and_url(self, sales):
        sales.create_dashboard.return_value = self.DASHBOARD

        result = _run("dashboard", "sales", "--metrics", "revenue", "--json")

        assert result.exit_code == 0, result.output
        sales.create_dashboard.assert_called_once_with(
            ["revenue"], group_by=None, title=None
        )
        assert json.loads(result.output) == self.DASHBOARD

    def test_dashboard_needs_metrics(self, sales):
        result = _run("dashboard", "sales")

        assert result.exit_code == 2
        assert "--metrics" in result.output
        sales.create_dashboard.assert_not_called()

    def test_a_refused_dashboard_prints_one_line(self, sales):
        sales.create_dashboard.side_effect = SemanticLayerException(
            "Semantic layer request on 'sales' failed (HTTP 503, 600014): "
            "Superset dashboards from the semantic layer are not enabled",
            status_code=503,
            error_code=600014,
        )

        result = _run("dashboard", "sales", "--metrics", "revenue")

        assert result.exit_code == 1
        assert (
            "Superset dashboards from the semantic layer are not enabled"
            in result.output
        )
        assert "Traceback" not in result.output


class TestCompile:
    def test_without_wait_does_not_block(self, sales):
        sales.compile.return_value = SemanticBuild(id=13, status="COMPILING")

        result = _run("compile", "sales")

        assert result.exit_code == 0, result.output
        sales.compile.assert_called_once_with(wait=False)
        assert "Build 13: COMPILING" in result.output

    @pytest.mark.parametrize("flag", ["--wait", "--fail-on-error"])
    def test_a_waited_compile_exits_one_on_an_invalid_build(self, sales, flag):
        sales.compile.side_effect = SemanticBuildException("sales", _invalid_build())

        result = _run("compile", "sales", flag)

        assert result.exit_code == 1
        sales.compile.assert_called_once_with(wait=True)
        assert "Build 13: INVALID (1 errors, 0 warnings)" in result.output
        assert "UNIQUE_NAMES" in result.output
        assert "models/semantic.yml:12:5" in result.output

    def test_a_waited_compile_exits_one_on_a_failed_build(self, sales):
        sales.compile.side_effect = SemanticBuildException(
            "sales", SemanticBuild(id=13, status="FAILED")
        )

        result = _run("compile", "sales", "--wait")

        assert result.exit_code == 1
        assert "Build 13: FAILED" in result.output

    def test_an_invalid_build_in_json_mode_prints_the_build_and_exits_one(self, sales):
        sales.compile.side_effect = SemanticBuildException("sales", _invalid_build())

        result = _run("--json", "compile", "sales", "--wait")

        assert result.exit_code == 1
        assert json.loads(result.output)["status"] == "INVALID"

    def test_a_waited_compile_passes_on_a_valid_build_and_shows_warnings(self, sales):
        sales.compile.return_value = SemanticBuild(
            id=14, status="VALID", warning_count=1
        )
        sales.get_build.return_value = SemanticBuild(
            id=14,
            status="VALID",
            warning_count=1,
            issues=[SemanticIssue(level="WARNING", rule="PRIMARY_KEY", message="m")],
        )

        result = _run("compile", "sales", "--wait", "--json")

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

    def test_transfer_ownership_with_yes_names_the_new_owner(self, sales):
        sales.owner = "meb10000"
        sales.transfer_ownership.return_value = mock.MagicMock(
            name="sp", owner="meb10001"
        )
        sales.transfer_ownership.return_value.name = "sales"

        result = _run("transfer-ownership", "sales", "carol@example.com", "--yes")

        assert result.exit_code == 0, result.output
        sales.transfer_ownership.assert_called_once_with("carol@example.com")
        assert "sales is now owned by meb10001" in result.output

    def test_transfer_ownership_asks_first_and_a_no_changes_nothing(self, sales):
        sales.owner = "meb10000"

        result = CliRunner().invoke(
            cli,
            ["semantic", "transfer-ownership", "sales", "carol@example.com"],
            input="n\n",
        )

        assert result.exit_code == 1
        assert "from meb10000 to carol@example.com" in result.output
        sales.transfer_ownership.assert_not_called()

    def test_transfer_ownership_in_json_mode_prints_the_project(self, sales):
        sales.transfer_ownership.return_value.to_dict.return_value = {
            "name": "sales",
            "owner": "meb10001",
        }

        result = _run("--json", "transfer-ownership", "sales", "carol@example.com")

        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["owner"] == "meb10001"

    def test_show_names_the_owner(self, sales):
        sales.owner = "meb10001"

        result = _run("show", "sales")

        assert result.exit_code == 0, result.output
        assert ["owner", "meb10001"] in [
            line.split() for line in result.output.splitlines()
        ]

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

    def test_show_lists_tags_and_missing_mandatory_tags(self, sales):
        sales.get_tags.return_value = {"owner": "finance", "domain": "sales"}
        sales.missing_mandatory_tags = ["certified"]

        result = _run("show", "sales")

        assert result.exit_code == 0, result.output
        assert "domain, owner" in result.output
        assert "certified" in result.output

    def test_list_warns_about_missing_mandatory_tags(self, sl):
        sl.get_projects.return_value = [
            SemanticProject(name="sales", missing_mandatory_tags=[{"name": "owner"}]),
            SemanticProject(name="tagged"),
        ]

        result = _run("list")

        assert result.exit_code == 0, result.output
        assert "sales lacks mandatory tags: owner" in result.output
        assert "tagged lacks" not in result.output


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


class TestSavedQueries:
    def test_saved_queries_table(self, sales):
        sales.list_saved_queries.return_value = [
            SemanticSavedQuery(
                name="revenue_by_country",
                label="Revenue by country",
                metrics=["revenue", "orders"],
                group_by=["metric_time__month", "customer__country"],
            ),
            SemanticSavedQuery(name="all_revenue", metrics=["revenue"]),
        ]

        result = _run("saved-queries", "sales")

        assert result.exit_code == 0, result.output
        lines = result.output.splitlines()
        assert lines[0].split() == ["NAME", "LABEL", "METRICS", "GROUP-BYS"]
        assert lines[2].split() == [
            "revenue_by_country",
            "Revenue",
            "by",
            "country",
            "revenue,orders",
            "metric_time__month,customer__country",
        ]
        assert lines[3].split() == ["all_revenue", "-", "revenue", "-"]

    def test_saved_queries_in_json(self, sales):
        sales.list_saved_queries.return_value = [
            SemanticSavedQuery(name="all_revenue", metrics=["revenue"], limit=10)
        ]

        result = _run("saved-queries", "sales", "--json")

        assert result.exit_code == 0, result.output
        assert json.loads(result.output)[0] == {
            "name": "all_revenue",
            "label": None,
            "description": None,
            "metrics": ["revenue"],
            "group_by": [],
            "where": [],
            "order_by": [],
            "limit": 10,
            "tags": {},
        }

    def test_query_runs_a_saved_query_with_its_refinements(self, sales):
        sales.query.return_value = pd.DataFrame({"revenue": [1.0]})

        result = _run(
            "query",
            "sales",
            "--saved-query",
            "revenue_by_country",
            "--group-by",
            "metric_time__month",
            "--where",
            "{{ Dimension('customer__country') }} = 'SE'",
            "--limit",
            "5",
            "-o",
            "csv",
        )

        assert result.exit_code == 0, result.output
        sales.query.assert_called_once_with(
            saved_query="revenue_by_country",
            group_by=["metric_time__month"],
            where=["{{ Dimension('customer__country') }} = 'SE'"],
            order_by=None,
            limit=5,
            time_range=None,
            dataframe_type="pandas",
            read_cache=True,
        )
        assert result.output.splitlines() == ["revenue", "1.0"]

    def test_an_empty_group_by_asks_for_the_totals_and_an_empty_order_by_drops_the_sort(
        self, sales
    ):
        sales.sql.return_value = "SELECT 1"

        result = _run(
            "sql",
            "sales",
            "--saved-query",
            "revenue_by_country",
            "--group-by",
            "",
            "--order-by",
            "",
        )

        assert result.exit_code == 0, result.output
        sales.sql.assert_called_once_with(
            saved_query="revenue_by_country",
            group_by=[],
            where=None,
            order_by=[],
            limit=None,
            time_range=None,
        )

    @pytest.mark.parametrize("command", [["sql"], ["query", "--explain"]])
    def test_sql_of_a_saved_query(self, sales, command):
        sales.sql.return_value = "SELECT 1"

        result = _run(*command, "sales", "--saved-query", "revenue_by_country")

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "SELECT 1"
        sales.sql.assert_called_once_with(
            saved_query="revenue_by_country",
            group_by=None,
            where=None,
            order_by=None,
            limit=None,
            time_range=None,
        )

    @pytest.mark.parametrize("command", ["sql", "query", "dashboard"])
    def test_metrics_and_a_saved_query_do_not_combine(self, sales, command):
        result = _run(command, "sales", "--metrics", "revenue", "--saved-query", "q")

        assert result.exit_code == 2
        assert "not both" in result.output
        sales.sql.assert_not_called()
        sales.query.assert_not_called()
        sales.create_dashboard.assert_not_called()

    def test_dashboard_of_a_saved_query(self, sales):
        sales.create_dashboard.return_value = TestDashboard.DASHBOARD

        result = _run(
            "dashboard", "sales", "--saved-query", "revenue_by_country", "--title", "R"
        )

        assert result.exit_code == 0, result.output
        sales.create_dashboard.assert_called_once_with(
            saved_query="revenue_by_country", title="R"
        )

    def test_dashboard_of_a_saved_query_takes_no_group_by(self, sales):
        result = _run(
            "dashboard",
            "sales",
            "--saved-query",
            "revenue_by_country",
            "--group-by",
            "customer__country",
        )

        assert result.exit_code == 2
        assert "names its own group-bys" in result.output
        sales.create_dashboard.assert_not_called()


class TestTimeSpine:
    @pytest.fixture
    def spine(self, sl):
        fg = mock.MagicMock(name="FeatureGroup")
        fg.name = "time_spine_day"
        fg.version = 1
        features = []
        for column in ("date_day", "fiscal_quarter"):
            feature = mock.MagicMock()
            feature.name = column
            features.append(feature)
        fg.features = features
        sl.create_time_spine.return_value = fg
        return fg

    def test_create_passes_the_range_grain_and_custom_granularities(self, sl, spine):
        result = _run(
            "time-spine",
            "create",
            "--start",
            "2020-01-01",
            "--end",
            "2030-12-31",
            "--custom",
            "fiscal_quarter=Q-JAN",
            "--custom",
            "fiscal_year = Y-JAN",
        )

        assert result.exit_code == 0, result.output
        sl.create_time_spine.assert_called_once_with(
            "2020-01-01",
            "2030-12-31",
            granularity="day",
            custom_granularities={"fiscal_quarter": "Q-JAN", "fiscal_year": "Y-JAN"},
        )
        assert "feature group time_spine_day version 1" in result.output
        assert "date_day, fiscal_quarter" in result.output

    def test_create_in_json(self, sl, spine):
        result = _run(
            "--json",
            "time-spine",
            "create",
            "--start",
            "2020-01-01",
            "--end",
            "2020-12-31",
            "--grain",
            "week",
        )

        assert result.exit_code == 0, result.output
        assert sl.create_time_spine.call_args.kwargs == {
            "granularity": "week",
            "custom_granularities": None,
        }
        assert json.loads(result.output) == {
            "feature_group": "time_spine_day",
            "version": 1,
            "granularity": "week",
            "start": "2020-01-01",
            "end": "2020-12-31",
            "columns": ["date_day", "fiscal_quarter"],
        }

    @pytest.mark.parametrize(
        ("flags", "message"),
        [
            (["--custom", "fiscal_quarter"], "expected NAME=FREQ"),
            (["--grain", "fortnight"], "'fortnight' is not one of"),
        ],
    )
    def test_bad_flags_exit_two(self, sl, flags, message):
        result = _run(
            "time-spine",
            "create",
            "--start",
            "2020-01-01",
            "--end",
            "2020-12-31",
            *flags,
        )

        assert result.exit_code == 2
        assert message in result.output
        sl.create_time_spine.assert_not_called()

    def test_a_refused_range_prints_one_line(self, sl):
        sl.create_time_spine.side_effect = ValueError(
            "The end of the time spine, 2020-01-01, is before its start, 2020-12-31."
        )

        result = _run(
            "time-spine", "create", "--start", "2020-12-31", "--end", "2020-01-01"
        )

        assert result.exit_code == 1
        assert "is before its start" in result.output
        assert "Traceback" not in result.output


class TestTestGroupsByMetricTimeWhenRequired:
    def test_a_metric_that_requires_metric_time_runs_grouped_by_it(self, sales):
        sales.list_metrics.return_value = [
            SemanticMetric(name="revenue", type="SIMPLE"),
            SemanticMetric(
                name="revenue_mtd", type="CUMULATIVE", requires_metric_time=True
            ),
        ]
        sales.query.return_value = [{"x": 1}]

        result = _run("test", "sales", "--no-compile")

        assert result.exit_code == 0, result.output
        assert [
            (c.args[0], c.kwargs["group_by"]) for c in sales.query.call_args_list
        ] == [(["revenue"], None), (["revenue_mtd"], ["metric_time"])]
        assert all(c.kwargs["limit"] == 1 for c in sales.query.call_args_list)

    def test_the_named_metrics_are_looked_up_too(self, sales):
        sales.list_metrics.return_value = [
            SemanticMetric(
                name="revenue_mtd", type="CUMULATIVE", requires_metric_time=True
            ),
        ]
        sales.query.return_value = []

        result = _run("test", "sales", "--no-compile", "--metrics", "revenue_mtd,nope")

        assert result.exit_code == 0, result.output
        assert [
            (c.args[0], c.kwargs["group_by"]) for c in sales.query.call_args_list
        ] == [(["revenue_mtd"], ["metric_time"]), (["nope"], None)]


class TestHistory:
    @staticmethod
    def _frame():
        frame = pd.DataFrame(
            [
                {
                    "execution_id": "3f8c2a52-0000-4000-8000-000000000001",
                    "user": "meb10000",
                    "surface": "FLIGHT",
                    "engine": "TRINO",
                    "build_id": 12,
                    "status": "FAILED",
                    "error": "unknown metric 'nope'\nmore",
                    "rows": None,
                    "started_at": "2026-11-08T10:00:00.123Z",
                    "finished_at": "2026-11-08T10:00:00.200Z",
                    "duration_ms": 77,
                    "cache": "bypass",
                    "served_from": None,
                    "trino_query_id": None,
                    "request": {
                        "metrics": ["nope"],
                        "groupBy": [{"name": "metric_time", "grain": "month"}],
                    },
                    "sql": None,
                }
            ]
        )
        for column in ("started_at", "finished_at"):
            frame[column] = pd.to_datetime(frame[column], utc=True)
        for column in ("build_id", "rows", "duration_ms"):
            frame[column] = frame[column].astype("Int64")
        return frame

    def test_history_passes_its_filters_and_prints_a_line_per_query(self, sales):
        sales.query_history.return_value = self._frame()

        result = _run(
            "history",
            "sales",
            "--status",
            "failed",
            "--user",
            "meb10000",
            "--limit",
            "5",
        )

        assert result.exit_code == 0, result.output
        sales.query_history.assert_called_once_with(
            limit=5, status=["FAILED"], user="meb10000"
        )
        lines = result.output.splitlines()
        assert lines[0].split()[:4] == ["STARTED", "USER", "SURFACE", "STATUS"]
        assert lines[2].split() == [
            "2026-11-08T10:00:00.123000+00:00",
            "meb10000",
            "FLIGHT",
            "FAILED",
            "77",
            "-",
            "bypass",
            "nope",
            "by",
            "metric_time__month",
            "unknown",
            "metric",
            "'nope'",
        ]

    def test_history_in_json(self, sales):
        sales.query_history.return_value = self._frame()

        result = _run("--json", "history", "sales")

        assert result.exit_code == 0, result.output
        sales.query_history.assert_called_once_with(limit=50, status=None, user=None)
        record = json.loads(result.output)[0]
        assert record["started_at"] == "2026-11-08T10:00:00.123000+00:00"
        assert record["rows"] is None
        assert record["build_id"] == 12
        assert record["request"]["metrics"] == ["nope"]


class TestExports:
    EXPORT = SemanticExport(
        name="revenue_daily",
        saved_query="revenue_by_country",
        export_as="TABLE",
        feature_group="revenue_daily",
        feature_group_id=501,
        version=1,
        schedule=None,
        lookback="7 days",
        enabled=True,
        job_name="semantic_export_sales_revenue_daily",
        last_status="SUCCEEDED",
        last_run="2026-11-07T02:00:05Z",
        last_rows=123,
        last_message="Replaced the rows from 2026-10-31T00:00:00Z on.\nmore",
    )

    def test_list_is_a_table_of_each_export_and_its_last_run(self, sales):
        sales.exports.return_value = [self.EXPORT]

        result = _run("export", "list", "sales")

        assert result.exit_code == 0, result.output
        lines = result.output.splitlines()
        assert lines[0].split()[:3] == ["NAME", "SAVED", "QUERY"]
        assert lines[2].split() == [
            "revenue_daily",
            "revenue_by_country",
            "revenue_daily",
            "v1",
            "on",
            "demand",
            "7",
            "days",
            "SUCCEEDED",
            "2026-11-07T02:00:05Z",
            "123",
            "Replaced",
            "the",
            "rows",
            "from",
            "2026-10-31T00:00:00Z",
            "on.",
        ]

    def test_list_in_json(self, sales):
        sales.exports.return_value = [self.EXPORT]

        result = _run("--json", "export", "list", "sales")

        assert json.loads(result.output) == [self.EXPORT.to_dict()]

    def test_run_starts_the_job_without_waiting(self, sales):
        sales.run_export.return_value = mock.Mock(id=77, state="INITIALIZING")

        result = _run("export", "run", "sales", "revenue_daily")

        assert result.exit_code == 0, result.output
        sales.run_export.assert_called_once_with("revenue_daily", wait=False)
        sales.exports.assert_not_called()
        assert "Started export revenue_daily (execution #77)" in result.output

    def test_run_with_wait_prints_the_outcome(self, sales):
        sales.run_export.return_value = mock.Mock(id=77, state="FINISHED")
        sales.exports.return_value = [self.EXPORT]

        result = _run("--json", "export", "run", "sales", "revenue_daily", "--wait")

        assert result.exit_code == 0, result.output
        sales.run_export.assert_called_once_with("revenue_daily", wait=True)
        assert json.loads(result.output) == {
            "execution_id": 77,
            "state": "FINISHED",
            "export": self.EXPORT.to_dict(),
        }

    def test_a_failed_run_names_the_list_command(self, sales):
        sales.run_export.side_effect = JobExecutionException("The Hopsworks Job failed")

        result = _run("export", "run", "sales", "revenue_daily", "--wait")

        assert result.exit_code == 1
        assert "hops semantic export list sales" in result.output
        assert "Traceback" not in result.output


class TestConnect:
    INFO = {
        "environment_id": 7,
        "graphql_url": "https://hopsworks.example.com/semantic-layer/api/graphql",
        "adbc_url": "grpc+tls://flight.example.com:18446",
        "jdbc_url": "jdbc:arrow-flight-sql://flight.example.com:18446?environmentId=7",
        "flight_host": "flight.example.com",
        "flight_port": 18446,
        "env": {
            "DBT_SL_GRAPHQL_URL_FORMAT": "https://{server_host}/semantic-layer/api/graphql",
            "SSL_CERT_FILE": "/tmp/ca dir/ca.pem",
        },
        "ca_path": "/tmp/ca dir/ca.pem",
    }

    def test_stdout_is_only_export_lines(self, sales):
        sales.connection_info.return_value = self.INFO

        result = _run("connect", "sales", "--ca-path", "/tmp/ca dir/ca.pem")

        assert result.exit_code == 0, result.output
        sales.connection_info.assert_called_once_with(ca_path="/tmp/ca dir/ca.pem")
        assert result.stdout.splitlines() == [
            "export DBT_SL_GRAPHQL_URL_FORMAT='https://{server_host}/semantic-layer/api/graphql'",
            "export SSL_CERT_FILE='/tmp/ca dir/ca.pem'",
        ]
        assert "Environment id: 7" in result.stderr
        assert "GraphQL (metadata, compileSql): https://hopsworks" in result.stderr
        assert "ADBC (queries): grpc+tls://flight.example.com:18446" in result.stderr
        assert "CA certificate: /tmp/ca dir/ca.pem" in result.stderr

    def test_json_is_the_connection_info(self, sales):
        sales.connection_info.return_value = self.INFO

        result = _run("--json", "connect", "sales")

        assert json.loads(result.output) == self.INFO

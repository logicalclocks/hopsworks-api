#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""The semantic layer tools of the MCP server, called directly on a mocked project."""

from __future__ import annotations

import asyncio
import csv
import io
import threading
from unittest import mock

import pandas as pd
import pytest
from hopsworks_common.client.exceptions import (
    SemanticBuildException,
    SemanticLayerException,
)
from hopsworks_common.semantic_layer import (
    SemanticBuild,
    SemanticGroupBy,
    SemanticIssue,
    SemanticMetric,
    SemanticServing,
)


pytest.importorskip("fastmcp")

from hopsworks.mcp.tools import semantic_layer  # noqa: E402
from hopsworks.mcp.tools.semantic_layer import SemanticLayerTools  # noqa: E402


def _rows(text):
    return list(csv.reader(io.StringIO(text)))


@pytest.fixture
def sales():
    sp = mock.MagicMock(name="SemanticProject")
    sp.name = "sales"
    sp.root_path = "/Projects/demo/Resources/semantic/sales"
    sp.deployment_mode = "FOLLOW"
    return sp


@pytest.fixture
def sl(mocker, sales):
    project = mock.MagicMock(name="Project")
    project.name = "demo"
    api = project.get_semantic_layer_api.return_value
    api.get_project.side_effect = lambda name: sales if name == "sales" else None
    mocker.patch("hopsworks.get_current_project", return_value=project)
    return api


@pytest.fixture
def tools():
    return SemanticLayerTools(mock.MagicMock(name="FastMCP"))


def _call(coroutine):
    return asyncio.run(coroutine)


def test_every_tool_registers_with_the_semantic_layer_tag():
    mcp = mock.MagicMock(name="FastMCP")

    SemanticLayerTools(mcp)

    tags = [call.kwargs["tags"] for call in mcp.tool.call_args_list]
    assert len(tags) == 11
    assert all("semantic_layer" in t for t in tags)
    assert sum("write" in t for t in tags) == 3
    tools = [call.args[0].__name__ for call in mcp.tool.return_value.call_args_list]
    registered = dict(zip(tools, tags, strict=True))
    assert {"write", "stateful"} <= set(registered["create_dashboard_from_metrics"])


def test_list_semantic_projects_reports_what_each_serves(tools, sl, sales):
    sales.serving.return_value = SemanticServing(build_id=12, stale=True)
    empty = mock.MagicMock(name="empty")
    empty.name = "empty"
    empty.root_path = "/Projects/demo/Resources/semantic/empty"
    empty.deployment_mode = "PINNED"
    empty.serving.side_effect = SemanticLayerException(
        "x", error_msg="no valid build", error_code="SEMANTIC_LAYER_NO_VALID_BUILD"
    )
    sl.get_projects.return_value = [sales, empty]

    projects = _call(tools.list_semantic_projects())

    assert projects[0].model_dump() == {
        "name": "sales",
        "root_path": "/Projects/demo/Resources/semantic/sales",
        "deployment_mode": "FOLLOW",
        "serving_build_id": 12,
        "stale": True,
        "not_serving": None,
    }
    assert projects[1].serving_build_id is None
    assert projects[1].not_serving == "no valid build"


def test_list_metrics_is_csv_with_tags_as_json(tools, sl, sales):
    sales.list_metrics.return_value = [
        SemanticMetric(
            name="revenue",
            type="SIMPLE",
            label="Revenue",
            description="Gross, in EUR",
            tags={"ownership": {"owner": "finance"}},
        ),
        SemanticMetric(name="orders", type="SIMPLE"),
    ]
    ctx = mock.AsyncMock()

    rows = _rows(_call(tools.list_metrics("sales", tag=["ownership"], ctx=ctx)))

    sales.list_metrics.assert_called_once_with(tag=["ownership"])
    assert rows == [
        ["name", "type", "label", "description", "tags"],
        [
            "revenue",
            "SIMPLE",
            "Revenue",
            "Gross, in EUR",
            '{"ownership": {"owner": "finance"}}',
        ],
        ["orders", "SIMPLE", "", "", ""],
    ]
    ctx.info.assert_awaited()


def test_get_dimensions_lists_granularities_and_entity_paths(tools, sl, sales):
    sales.dimensions.return_value = [
        SemanticGroupBy(
            name="metric_time",
            type="TIME",
            entity_path=[],
            queryable_granularities=["day", "month"],
        ),
        SemanticGroupBy(
            name="customer__country",
            type="CATEGORICAL",
            entity_path=["customer"],
            description="ISO code",
        ),
    ]

    rows = _rows(_call(tools.get_dimensions("sales", ["revenue"])))

    sales.dimensions.assert_called_once_with(metrics=["revenue"])
    assert rows[1:] == [
        ["metric_time", "TIME", "day month", "", ""],
        ["customer__country", "CATEGORICAL", "", "customer", "ISO code"],
    ]


def test_get_entities_and_dimension_values(tools, sl, sales):
    sales.entities.return_value = [
        SemanticGroupBy(name="customer", type="ENTITY", entity_path=["order"])
    ]
    sales.dimension_values.return_value = ["SE", None]

    entities = _rows(_call(tools.get_entities("sales")))
    values = _rows(
        _call(tools.get_dimension_values("sales", "customer__country", limit=5))
    )

    sales.entities.assert_called_once_with(metrics=None)
    assert entities == [
        ["name", "entity_path", "description"],
        ["customer", "order", ""],
    ]
    sales.dimension_values.assert_called_once_with(
        "customer__country", metrics=None, limit=5
    )
    assert values == [["customer__country"], ["SE"], [""]]


def test_query_metrics_returns_the_rows_as_csv(tools, sl, sales):
    sales.query.return_value = pd.DataFrame(
        {"metric_time__month": ["2026-01-01"], "revenue": [100.5]}
    )

    text = _call(
        tools.query_metrics(
            "sales",
            ["revenue"],
            group_by=["metric_time__month"],
            where=["{{ Dimension('customer__country') }} = 'SE'"],
            order_by=["-revenue"],
        )
    )

    assert _rows(text) == [["metric_time__month", "revenue"], ["2026-01-01", "100.5"]]
    sales.query.assert_called_once_with(
        ["revenue"],
        group_by=["metric_time__month"],
        where=["{{ Dimension('customer__country') }} = 'SE'"],
        order_by=["-revenue"],
        limit=100,
        dataframe_type="pandas",
    )


def test_a_long_answer_is_cut_at_a_line_and_says_so(tools, sl, sales, monkeypatch):
    monkeypatch.setattr(semantic_layer, "MAX_RESPONSE_CHARS", 30)
    sales.query.return_value = pd.DataFrame(
        {"country": [f"c{i:03}" for i in range(50)]}
    )

    text = _call(tools.query_metrics("sales", ["orders"], limit=None))

    kept, note = text.rsplit("\n", 2)[0], text.splitlines()[-1]
    assert len(kept) <= 30
    assert kept.splitlines()[0] == "country"
    assert "cut at 30 characters" in note


def test_a_cut_answer_with_multiline_cells_is_still_csv(tools, sl, sales, monkeypatch):
    monkeypatch.setattr(semantic_layer, "MAX_RESPONSE_CHARS", 60)
    sales.query.return_value = pd.DataFrame(
        {"note": [f"line one {i}\nline two" for i in range(20)], "n": range(20)}
    )

    text = _call(tools.query_metrics("sales", ["orders"], limit=None))

    data, note = text[: text.rindex("... cut")], text[text.rindex("... cut") :]
    rows = list(csv.reader(io.StringIO(data), strict=True))
    assert rows[0] == ["note", "n"]
    assert all(len(row) == 2 for row in rows)
    assert len(data) <= 60
    assert note.startswith("... cut at 60 characters")


def test_missing_values_are_empty_cells(tools, sl, sales):
    sales.query.return_value = pd.DataFrame({"country": ["SE", None], "n": [1.0, None]})

    text = _call(tools.query_metrics("sales", ["orders"], limit=None))

    assert text.splitlines() == ["country,n", "SE,1.0", ","]


def test_get_metrics_compiled_sql_passes_the_query(tools, sl, sales):
    sales.sql.return_value = "SELECT 1"

    sql = _call(
        tools.get_metrics_compiled_sql(
            "sales", ["revenue"], group_by=["metric_time__day"]
        )
    )

    assert sql == "SELECT 1"
    sales.sql.assert_called_once_with(
        ["revenue"], group_by=["metric_time__day"], where=None, order_by=None, limit=100
    )


def test_get_semantic_project_tags(tools, sl, sales):
    sales.get_tags.return_value = {"ownership": {"owner": "finance"}, "tier": 1}

    tags = _call(tools.get_semantic_project_tags("sales"))

    assert [t.model_dump() for t in tags] == [
        {"name": "ownership", "value": {"owner": "finance"}},
        {"name": "tier", "value": 1},
    ]


def test_validate_reports_the_issues_of_an_invalid_build(tools, sl, sales):
    build = SemanticBuild(
        id=13,
        status="INVALID",
        error_count=1,
        warning_count=0,
        issues=[
            SemanticIssue(
                level="ERROR",
                rule="METRIC_TAG_UNKNOWN_SCHEMA",
                message="no tag schema 'owner'",
                file="models/semantic.yml",
                line=40,
                element="metric:revenue",
            )
        ],
    )
    sales.compile.side_effect = SemanticBuildException("sales", build)

    result = _call(tools.validate_semantic_project("sales"))

    sales.compile.assert_called_once_with(wait=True)
    assert result.build_id == 13
    assert result.status == "INVALID"
    assert result.issues[0].rule == "METRIC_TAG_UNKNOWN_SCHEMA"
    assert result.issues[0].line == 40


def test_compile_fetches_the_warnings_of_a_valid_build(tools, sl, sales):
    sales.compile.return_value = SemanticBuild(id=14, status="VALID", warning_count=1)
    sales.get_build.return_value = SemanticBuild(
        id=14,
        status="VALID",
        warning_count=1,
        issues=[SemanticIssue(level="WARNING", rule="PRIMARY_KEY", message="m")],
    )

    result = _call(tools.compile_semantic_project("sales"))

    sales.get_build.assert_called_once_with(14)
    assert result.status == "VALID"
    assert [i.level for i in result.issues] == ["WARNING"]


def test_create_dashboard_from_metrics_returns_id_title_and_url_off_the_event_loop(
    tools, sl, sales
):
    callers = []

    def create_dashboard(*args, **kwargs):
        callers.append(threading.current_thread())
        return {"id": 12, "title": "Sales overview", "url": "https://h/d/12/"}

    sales.create_dashboard.side_effect = create_dashboard

    dashboard = _call(
        tools.create_dashboard_from_metrics(
            "sales", ["revenue", "orders"], group_by=["customer__country"]
        )
    )

    sales.create_dashboard.assert_called_once_with(
        ["revenue", "orders"], group_by=["customer__country"], title=None
    )
    assert dashboard.model_dump() == {
        "id": 12,
        "title": "Sales overview",
        "url": "https://h/d/12/",
    }
    assert callers[0] is not threading.main_thread()


def test_an_unknown_semantic_project_names_the_list_tool(tools, sl):
    with pytest.raises(ValueError, match="list_semantic_projects"):
        _call(tools.list_metrics("nope"))


def test_without_a_login_the_tools_ask_for_one(tools, mocker):
    import hopsworks

    mocker.patch(
        "hopsworks.get_current_project",
        side_effect=hopsworks.ProjectException("No project is set for this session"),
    )

    with pytest.raises(RuntimeError, match="use login tool"):
        _call(tools.list_metrics("sales"))

"""The chatbot's translator, without a cluster or an LLM: python -m pytest tests in the system's directory."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import pytest


_spec = importlib.util.spec_from_file_location(
    "translator", Path(__file__).resolve().parents[1] / "app" / "translator.py"
)
tr = importlib.util.module_from_spec(_spec)
# Its dataclass resolves annotations through sys.modules.
sys.modules["translator"] = tr
_spec.loader.exec_module(tr)

TODAY = date(2026, 11, 14)
LINE_DIMS = {
    "metric_time",
    "customer__region",
    "customer__segment",
    "product__category",
    "order__channel",
}
ORDER_DIMS = {"metric_time", "customer__region", "customer__segment", "order__channel"}


def _metric(label, description="", requires=False):
    return {
        "label": label,
        "description": description,
        "type": "SIMPLE",
        "requires_metric_time": requires,
    }


def _dim(label, kind="CATEGORICAL", values=None):
    dim = {
        "label": label,
        "description": "",
        "type": kind,
        "grains": list(tr.GRAINS) if kind == "TIME" else [],
    }
    if values is not None:
        dim["values"] = values
    return dim


@pytest.fixture
def catalog():
    return tr.Catalog(
        metrics={
            "revenue": _metric("Net revenue", "In USD."),
            "return_rate": _metric("Return rate", "A share from 0 to 1."),
            "revenue_growth_yoy": _metric("Growth", "", requires=True),
        },
        dimensions={
            "metric_time": _dim("Time", "TIME"),
            "customer__region": _dim(
                "Region",
                values=["Asia Pacific", "Europe", "Latin America", "North America"],
            ),
            "customer__segment": _dim(
                "Segment", values=["Consumer", "Enterprise", "Small Business"]
            ),
            "product__category": _dim("Category", values=["Apparel", "Electronics"]),
            "order__channel": _dim(
                "Channel", values=["Marketplace", "Mobile app", "Web"]
            ),
        },
        reachable={
            "revenue": LINE_DIMS,
            "return_rate": ORDER_DIMS,
            "revenue_growth_yoy": LINE_DIMS,
        },
    )


def test_a_valid_query_is_normalized(catalog):
    query, problems = tr.check(
        {
            "metrics": ["revenue"],
            "group_by": ["metric_time__month", "customer__region"],
            "filters": [
                {"dimension": "customer__segment", "op": "in", "value": ["enterprise"]}
            ],
            "time_range": {"start": "2026-01-01", "end": "2026-06-30T00:00:00"},
            "order_by": ["-revenue"],
            "limit": 5000,
        },
        catalog,
    )
    assert problems == []
    assert query["filters"] == [
        {"dimension": "customer__segment", "op": "IN", "value": ["Enterprise"]}
    ]
    assert query["time_range"] == {"start": "2026-01-01", "end": "2026-06-30"}
    assert query["limit"] == tr.MAX_LIMIT
    assert query["chart"] == "line"


@pytest.mark.parametrize(
    ("query", "problem"),
    [
        ({"metrics": []}, "non-empty"),
        ({"metrics": ["profit"]}, "unknown metric 'profit'"),
        (
            {"metrics": ["revenue"], "group_by": ["customer__planet"]},
            "unknown group-by",
        ),
        (
            {"metrics": ["return_rate"], "group_by": ["product__category"]},
            "cannot be reached from return_rate",
        ),
        ({"metrics": ["revenue"], "group_by": ["metric_time"]}, "add a grain"),
        (
            {"metrics": ["revenue"], "group_by": ["customer__region__month"]},
            "takes no grain",
        ),
        ({"metrics": ["revenue_growth_yoy"]}, "must be grouped by metric_time"),
        (
            {
                "metrics": ["revenue"],
                "filters": [
                    {"dimension": "customer__region", "op": "=", "value": "Mars"}
                ],
            },
            "no value 'Mars'",
        ),
        (
            {
                "metrics": ["revenue"],
                "filters": [{"dimension": "customer__region", "op": "~", "value": "x"}],
            },
            "is not one of",
        ),
        (
            {
                "metrics": ["revenue"],
                "filters": [
                    {"dimension": "customer__region", "op": "=", "value": ["Europe"]}
                ],
            },
            "takes one value",
        ),
        (
            {
                "metrics": ["revenue"],
                "time_range": {"start": "2026-06-01", "end": "2026-01-01"},
            },
            "after its end",
        ),
        (
            {"metrics": ["revenue"], "time_range": {"start": "last year"}},
            "not a YYYY-MM-DD date",
        ),
        (
            {"metrics": ["revenue"], "order_by": ["-orders"]},
            "is not a metric or group-by",
        ),
    ],
)
def test_an_invalid_query_is_refused(catalog, query, problem):
    _, problems = tr.check(query, catalog)
    assert any(problem in p for p in problems), problems


def test_the_sdk_gets_literal_filters_and_a_time_range(catalog):
    query, _ = tr.check(
        {
            "metrics": ["revenue"],
            "group_by": ["customer__region"],
            "filters": [{"dimension": "order__channel", "op": "=", "value": "web"}],
            "time_range": {"start": "2026-07-01", "end": None},
        },
        catalog,
    )
    assert tr.sdk_arguments(query) == {
        "metrics": ["revenue"],
        "group_by": ["customer__region"],
        "filters": [("order__channel", "=", "Web")],
        "order_by": None,
        "limit": tr.DEFAULT_LIMIT,
        "time_range": ("2026-07-01", None),
    }


def test_the_prompt_holds_the_catalog_and_today(catalog):
    prompt = tr.system_prompt(catalog, TODAY)
    assert "Today is 2026-11-14" in prompt
    for name in [*catalog.metrics, *catalog.dimensions]:
        assert name in prompt
    assert "Values: Asia Pacific, Europe, Latin America, North America." in prompt
    assert "revenue, revenue_growth_yoy: customer__region" in prompt
    assert "revenue_growth_yoy: Growth. Must be grouped by metric_time." in prompt


def test_the_reply_may_wrap_its_json():
    assert tr.parse('Here it is:\n```json\n{"metrics": ["revenue"]}\n```') == {
        "metrics": ["revenue"]
    }
    with pytest.raises(ValueError):
        tr.parse("I cannot help with that.")


class Script:
    """An LLM that answers with the given replies in turn and records what it was sent."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.sent = []

    def __call__(self, messages):
        self.sent.append([dict(m) for m in messages])
        return self.replies.pop(0)


def test_a_refused_query_goes_back_to_the_model_once(catalog):
    llm = Script(
        json.dumps({"metrics": ["revenue"], "group_by": ["customer__country"]}),
        json.dumps(
            {
                "metrics": ["revenue"],
                "group_by": ["customer__region"],
                "explanation": "By region.",
            }
        ),
    )
    ran = []
    outcome = tr.translate(
        llm, catalog, "revenue by country", [], lambda q: ran.append(q) or "rows", TODAY
    )
    assert outcome["attempts"] == 2
    assert outcome["result"] == "rows"
    assert ran == [outcome["query"]]
    assert "unknown group-by 'customer__country'" in llm.sent[1][-1]["content"]


def test_a_query_the_gateway_refuses_goes_back_with_its_reason(catalog):
    llm = Script(
        json.dumps({"metrics": ["revenue"]}),
        json.dumps({"metrics": ["revenue"], "limit": 10}),
    )
    calls = []

    def run(query):
        calls.append(query)
        if len(calls) == 1:
            raise RuntimeError("SEMANTIC_LAYER_INVALID_QUERY: boom")
        return "rows"

    outcome = tr.translate(llm, catalog, "revenue", [], run, TODAY)
    assert outcome["result"] == "rows"
    assert "boom" in llm.sent[1][-1]["content"]


def test_two_refusals_end_in_an_error(catalog):
    llm = Script("no json here", json.dumps({"metrics": ["profit"]}))
    outcome = tr.translate(
        llm, catalog, "profit?", [], lambda q: pytest.fail("ran"), TODAY
    )
    assert outcome == {
        "error": "The query is invalid: unknown metric 'profit'.",
        "attempts": tr.ATTEMPTS,
    }


def test_a_question_without_a_metric_is_answered_with_a_clarification(catalog):
    llm = Script(json.dumps({"clarify": "I can answer about revenue and returns."}))
    outcome = tr.translate(
        llm, catalog, "what is the weather", [], lambda q: pytest.fail("ran"), TODAY
    )
    assert outcome == {
        "clarify": "I can answer about revenue and returns.",
        "attempts": 1,
    }


def test_a_follow_up_sees_the_earlier_queries(catalog):
    earlier = {"metrics": ["revenue"], "group_by": ["customer__region"]}
    llm = Script(
        json.dumps({"metrics": ["revenue"], "group_by": ["customer__segment"]})
    )
    tr.translate(
        llm,
        catalog,
        "by segment instead",
        [{"question": "revenue by region", "query": earlier}],
        lambda q: "rows",
        TODAY,
    )
    roles = [m["role"] for m in llm.sent[0]]
    assert roles == ["system", "user", "assistant", "user"]
    assert json.loads(llm.sent[0][2]["content"]) == earlier

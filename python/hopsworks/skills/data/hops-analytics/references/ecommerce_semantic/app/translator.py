"""Questions in plain language to semantic layer queries, through an LLM, checked before they run.

The LLM sees the semantic project's catalog (metrics, dimensions, the values of the small ones) and the
question, never a data row, and answers with a JSON query: metrics, group-bys, filters, a time range,
an order and a limit. `check` refuses every name the catalog does not have, a filter value a dimension
does not take and a group-by a metric cannot reach, so nothing the model invents reaches the gateway;
the filters are bound as literals by the SDK, never parsed as SQL. A refused query, or one the gateway
refuses, goes back to the model once with the reason.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable


OPS = ("=", "!=", "<", "<=", ">", ">=", "IN", "NOT IN", "LIKE")
GRAINS = ("day", "week", "month", "quarter", "year")
CHARTS = ("line", "bar", "table", "number")
MAX_METRICS = 6
MAX_LIMIT = 500
DEFAULT_LIMIT = 200
# A dimension with at most this many values has them listed in the prompt and checked in filters.
LISTED_VALUES = 60
ATTEMPTS = 2


@dataclass
class Catalog:
    """What a query may name: the served metrics, the dimensions, and which dimensions each metric reaches."""

    metrics: dict[str, dict[str, Any]]
    dimensions: dict[str, dict[str, Any]]
    reachable: dict[str, set[str]]
    examples: list[str] = field(default_factory=list)


def catalog_from(project, examples: list[str] | None = None) -> Catalog:
    """Read a semantic project's catalog through the SDK: one call per metric, made once at startup."""
    metrics = {
        m.name: {
            "label": m.label or m.name,
            "description": m.description or "",
            "type": m.type,
            "requires_metric_time": bool(m.requires_metric_time),
        }
        for m in project.list_metrics()
    }
    dimensions: dict[str, dict[str, Any]] = {}
    for g in project.dimensions():
        dimensions[g.name] = {
            "label": g.label or g.name,
            "description": g.description or "",
            "type": g.type,
            "grains": [x.lower() for x in (g.queryable_granularities or [])],
        }
    dimensions.setdefault(
        "metric_time",
        {"label": "Time", "description": "", "type": "TIME", "grains": list(GRAINS)},
    )
    for name, dim in dimensions.items():
        if dim["type"] != "CATEGORICAL":
            continue
        values = project.dimension_values(name, limit=LISTED_VALUES + 1)
        if len(values) <= LISTED_VALUES:
            dim["values"] = sorted(str(v) for v in values if v is not None)
    reachable = {name: {g.name for g in project.dimensions([name])} for name in metrics}
    return Catalog(metrics, dimensions, reachable, examples or [])


def system_prompt(catalog: Catalog, today: date) -> str:
    """The instructions and the catalog, the whole of what the model knows about the data."""
    metrics = "\n".join(
        f"- {name}: "
        + " ".join(
            part
            for part in (
                f"{m['label']}.",
                m["description"],
                "Must be grouped by metric_time." if m["requires_metric_time"] else "",
            )
            if part
        )
        for name, m in sorted(catalog.metrics.items())
    )
    dims = []
    for name, d in sorted(catalog.dimensions.items()):
        if name == "metric_time":
            continue
        line = f"- {name}: {d['label']}. {d['description']}".rstrip()
        if d["type"] == "TIME":
            line += " A date; group by it with a grain suffix, such as __month."
        if d.get("values"):
            line += f" Values: {', '.join(d['values'])}."
        dims.append(line)
    # Metrics of one semantic model reach the same dimensions: one line per set keeps the prompt short.
    groups: dict[frozenset, list[str]] = {}
    for name in sorted(catalog.metrics):
        reached = frozenset(n for n in catalog.reachable[name] if n != "metric_time")
        groups.setdefault(reached, []).append(name)
    reach = "\n".join(
        f"- {', '.join(names)}: {', '.join(sorted(reached)) or 'metric_time only'}"
        for reached, names in groups.items()
    )
    return f"""You translate a question about an online shop's sales into one query of its semantic layer.
Today is {today.isoformat()} (UTC). Reply with one JSON object and nothing else.

Metrics:
{metrics}

Dimensions, as group-bys and filters:
- metric_time: the time axis of every metric (order date for sales, signup date for new_customers). Group by metric_time__day, metric_time__week, metric_time__month, metric_time__quarter or metric_time__year.
{chr(10).join(dims)}

The dimensions each metric can be grouped and filtered by (metric_time always):
{reach}

The JSON query:
{{"metrics": ["revenue"],
  "group_by": ["metric_time__month", "customer__region"],
  "filters": [{{"dimension": "customer__segment", "op": "IN", "value": ["Enterprise"]}}],
  "time_range": {{"start": "2026-01-01", "end": "2026-06-30"}},
  "order_by": ["-revenue"],
  "limit": 100,
  "chart": "line",
  "explanation": "Net revenue per region and month for enterprise customers in the first half of 2026."}}

Rules:
- Use only the metric and dimension names above, exactly as written. Every group-by and filter must be reachable by every metric in the query.
- For a trend over time group by metric_time with the grain the question implies; for a total, leave metric_time out.
- time_range bounds metric_time, both ends inclusive, as YYYY-MM-DD; null for all time. Resolve "last month", "this year", "last 90 days" or "Q3 2025" from today. A month or quarter that has not ended is partial.
- op is one of {", ".join(OPS)}. IN and NOT IN take a list; LIKE takes a pattern with %. Filter values of listed dimensions must be one of the listed values.
- "Top 5 countries by revenue" is order_by ["-revenue"] and limit 5. order_by names a metric or a group-by of the query.
- chart: line for a trend over metric_time, bar for a comparison of categories, number for a single total, table otherwise.
- Ratios and rates (gross_margin, return_rate, discount_rate, cancellation_rate, revenue_growth_yoy) are shares from 0 to 1.
- explanation: one sentence on what the query shows, in the question's terms, naming the time range.
- When no metric fits the question, reply {{"clarify": "<what you can answer instead, naming metrics>"}}.
- A follow-up refines the previous query: keep what it does not change."""


def parse(text: str) -> dict[str, Any]:
    """The JSON object in a model's reply, which may wrap it in prose or a code fence."""
    match = re.search(r"\{.*\}", text or "", re.DOTALL)
    if not match:
        raise ValueError("the reply holds no JSON object")
    reply = json.loads(match.group(0))
    if not isinstance(reply, dict):
        raise ValueError("the reply is not a JSON object")
    return reply


def _base(group_by: str) -> tuple[str, str | None]:
    """A group-by split into the dimension and its grain suffix, if any."""
    for grain in GRAINS:
        if group_by.endswith(f"__{grain}"):
            return group_by[: -len(grain) - 2], grain
    return group_by, None


def _iso_date(value: Any, what: str, problems: list[str]) -> str | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        problems.append(f"{what} {value!r} is not a YYYY-MM-DD date")
        return None


def _canonical(value: Any, values: list[str]) -> Any:
    """A filter value in the dimension's own spelling, when it matches one ignoring case."""
    by_lower = {v.lower(): v for v in values}
    return by_lower.get(str(value).lower(), value)


def check(query: dict[str, Any], catalog: Catalog) -> tuple[dict[str, Any], list[str]]:
    """The query normalized, and what is wrong with it: an empty list means it may run."""
    problems: list[str] = []
    metrics = query.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        return query, ["metrics must be a non-empty list of metric names"]
    metrics = [str(m) for m in metrics][:MAX_METRICS]
    for m in metrics:
        if m not in catalog.metrics:
            problems.append(f"unknown metric {m!r}")
    known = [m for m in metrics if m in catalog.metrics]
    reachable = (
        set.intersection(*(catalog.reachable[m] for m in known)) if known else set()
    )

    group_by = [str(g) for g in query.get("group_by") or []]
    for g in group_by:
        base, grain = _base(g)
        dim = catalog.dimensions.get(base)
        if dim is None:
            problems.append(f"unknown group-by {g!r}")
            continue
        if dim["type"] == "TIME" and grain is None:
            problems.append(f"{g} is a time dimension: add a grain, such as {g}__month")
        elif grain is not None and dim["type"] != "TIME":
            problems.append(f"{base} is not a time dimension and takes no grain")
        elif grain is not None and dim["grains"] and grain not in dim["grains"]:
            problems.append(f"{base} cannot be grouped by {grain}")
        if base != "metric_time" and base not in reachable:
            problems.append(f"{base} cannot be reached from {', '.join(known)}")
    if not any(_base(g)[0] == "metric_time" for g in group_by):
        needing = [m for m in known if catalog.metrics[m]["requires_metric_time"]]
        if needing:
            problems.append(f"{', '.join(needing)} must be grouped by metric_time")

    filters = []
    for f in query.get("filters") or []:
        if not isinstance(f, dict):
            problems.append(f"filter {f!r} is not an object")
            continue
        name, op, value = (
            str(f.get("dimension", "")),
            str(f.get("op", "")).upper(),
            f.get("value"),
        )
        dim = catalog.dimensions.get(_base(name)[0])
        if dim is None:
            problems.append(f"unknown filter dimension {name!r}")
            continue
        if _base(name)[0] != "metric_time" and _base(name)[0] not in reachable:
            problems.append(f"{name} cannot be reached from {', '.join(known)}")
        if op not in OPS:
            problems.append(f"filter op {op!r} is not one of {', '.join(OPS)}")
            continue
        if op in ("IN", "NOT IN"):
            value = value if isinstance(value, list) else [value]
            if not value:
                problems.append(f"{op} on {name} needs at least one value")
        elif isinstance(value, list):
            problems.append(f"{op} on {name} takes one value, not a list")
            continue
        listed = dim.get("values")
        if listed and op in ("=", "!=", "IN", "NOT IN"):
            value = (
                [_canonical(v, listed) for v in value]
                if isinstance(value, list)
                else _canonical(value, listed)
            )
            unknown = [
                v
                for v in (value if isinstance(value, list) else [value])
                if v not in listed
            ]
            if unknown:
                problems.append(
                    f"{name} has no value {', '.join(map(repr, unknown))}; it takes {', '.join(listed)}"
                )
        filters.append({"dimension": name, "op": op, "value": value})

    time_range = None
    if isinstance(query.get("time_range"), dict):
        start = _iso_date(
            query["time_range"].get("start"), "time_range start", problems
        )
        end = _iso_date(query["time_range"].get("end"), "time_range end", problems)
        if start and end and start > end:
            problems.append(f"time_range starts {start}, after its end {end}")
        if start or end:
            time_range = {"start": start, "end": end}

    columns = set(metrics) | set(group_by)
    order_by = [str(o) for o in query.get("order_by") or []]
    for o in order_by:
        if o.lstrip("-") not in columns:
            problems.append(f"order_by {o!r} is not a metric or group-by of the query")
    try:
        limit = int(query.get("limit") or DEFAULT_LIMIT)
    except (TypeError, ValueError):
        limit = DEFAULT_LIMIT
    chart = (
        query.get("chart") if query.get("chart") in CHARTS else _default_chart(group_by)
    )

    return {
        "metrics": metrics,
        "group_by": group_by,
        "filters": filters,
        "time_range": time_range,
        "order_by": order_by,
        "limit": max(1, min(limit, MAX_LIMIT)),
        "chart": chart,
        "explanation": str(query.get("explanation") or ""),
    }, problems


def _default_chart(group_by: list[str]) -> str:
    if any(_base(g)[0] == "metric_time" for g in group_by):
        return "line"
    return "bar" if group_by else "number"


def sdk_arguments(query: dict[str, Any]) -> dict[str, Any]:
    """The checked query as keyword arguments of SemanticProject.query and .sql."""
    args: dict[str, Any] = {
        "metrics": query["metrics"],
        "group_by": query["group_by"],
        "filters": [(f["dimension"], f["op"], f["value"]) for f in query["filters"]],
        "order_by": query["order_by"] or None,
        "limit": query["limit"],
    }
    if query["time_range"]:
        args["time_range"] = (query["time_range"]["start"], query["time_range"]["end"])
    return args


def messages(
    catalog: Catalog, today: date, question: str, history: list[dict[str, Any]]
) -> list[dict[str, str]]:
    """The conversation sent to the model: the instructions, earlier turns and their queries, the question."""
    turns = [{"role": "system", "content": system_prompt(catalog, today)}]
    for turn in history[-4:]:
        if turn.get("question") and turn.get("query"):
            turns.append({"role": "user", "content": str(turn["question"])})
            turns.append({"role": "assistant", "content": json.dumps(turn["query"])})
    turns.append({"role": "user", "content": question})
    return turns


def translate(
    llm: Callable[[list[dict[str, str]]], str],
    catalog: Catalog,
    question: str,
    history: list[dict[str, Any]],
    run: Callable[[dict[str, Any]], Any],
    today: date,
) -> dict[str, Any]:
    """Ask the model for a query, check it and run it; a refused query goes back to the model once.

    `llm` takes chat messages and returns the reply's text; `run` takes a checked query and returns its
    result, raising when the gateway refuses it. Returns `clarify` with the model's question, or
    `query` and `result`, or `error` with the last reason, and `attempts`.
    """
    turns = messages(catalog, today, question, history)
    reason = ""
    for attempt in range(1, ATTEMPTS + 1):
        reply = llm(turns)
        turns.append({"role": "assistant", "content": reply})
        try:
            raw = parse(reply)
        except (ValueError, json.JSONDecodeError) as err:
            reason = f"Your reply was not one JSON object ({err})."
        else:
            if raw.get("clarify"):
                return {"clarify": str(raw["clarify"]), "attempts": attempt}
            query, problems = check(raw, catalog)
            if not problems:
                try:
                    return {"query": query, "result": run(query), "attempts": attempt}
                except Exception as err:  # noqa: BLE001 - the gateway's reason goes back to the model
                    reason = f"The semantic layer refused the query: {err}"
            else:
                reason = "The query is invalid: " + "; ".join(problems) + "."
        turns.append(
            {"role": "user", "content": reason + " Reply with a corrected JSON query."}
        )
    return {"error": reason, "attempts": ATTEMPTS}

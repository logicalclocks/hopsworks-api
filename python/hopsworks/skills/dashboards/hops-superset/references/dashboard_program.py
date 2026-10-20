# ruff: noqa: INP001
"""Customers overview.

Feature groups: customers v1.
Charts: total customers (big number), customers by plan (bar), tenure distribution (histogram).

The program behind one Superset dashboard, written and kept by `/hops dashboard`
under `Users/<user>/dashboards/<slug>.py`. The docstring above is the record of
what the dashboard is: the title line, the feature groups and the charts.

    python dashboards/customers-overview.py            # create, or update in place
    python dashboards/customers-overview.py --delete   # remove what this program created
    python dashboards/customers-overview.py --list     # every dashboard in the project, as JSON

It runs where you run, a Hopsworks terminal or an external client (a Hopsable
sandbox, a laptop): the SDK reaches Superset through the Hopsworks proxy.

Every step is idempotent, so a re-run updates the charts and the layout instead
of duplicating them. Each dashboard has its own datasets, named after SLUG, so two
dashboards over one feature group never share or delete each other's. After a build,
every chart's query runs once; a chart that errors or returns no rows fails the run.
`--delete` removes the dashboard, its charts, and its datasets that no other chart
still uses, and nothing else.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from urllib.parse import urlparse

SLUG = "customers-overview"
TITLE = "Customers Overview"
FEATURE_GROUPS = [{"name": "customers", "version": 1, "format": "delta"}]

COUNT = {
    "expressionType": "SQL",
    "sqlExpression": "COUNT(*)",
    "label": "count",
    "optionName": "metric_count",
    "hasCustomLabel": True,
}

# One entry per chart: the dataset it reads (a feature group above) and the
# viz-type specific params from hops-superset/references/chart_params.md.
CHARTS = [
    {
        "slice_name": "Total Customers",
        "dataset": "customers",
        "viz_type": "big_number_total",
        "params": {
            "metric": COUNT,
            "adhoc_filters": [],
            "y_axis_format": "SMART_NUMBER",
        },
        "width": 4,
    },
    {
        "slice_name": "Customers by Plan",
        "dataset": "customers",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "plan",
            "x_axis_force_categorical": True,
            "metrics": [COUNT],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 20,
            "orientation": "vertical",
            "x_axis_sort": "count",
            "x_axis_sort_asc": False,
        },
        "width": 8,
    },
    {
        "slice_name": "Tenure Distribution",
        "dataset": "customers",
        "viz_type": "histogram_v2",
        "params": {
            "column": "tenure_months",
            "groupby": [],
            "adhoc_filters": [],
            "bins": 20,
        },
        "width": 12,
    },
]


def list_all(api, resource: str) -> list[dict]:
    """Every object of a kind; the public list_* methods return only the first page."""
    items, page = [], 0
    while True:
        batch = api._request("GET", f"/api/v1/{resource}/?q=(page:{page},page_size:100)").get(
            "result", []
        )
        items.extend(batch)
        if len(batch) < 100:
            return items
        page += 1


def find_trino_db_id(api) -> int:
    """The Superset database backed by Trino; its id differs per installation."""
    for db in api.list_databases()["result"]:
        if "trino" in db.get("database_name", "").lower():
            return db["id"]
    raise RuntimeError("No Trino database connection found in Superset")


def table_name(fg: dict) -> str:
    """The Trino table of a feature group version."""
    return f"{fg['name']}_{fg['version']}"


def dataset_name(fg: dict) -> str:
    """This dashboard's Superset dataset over a feature group version."""
    return f"{SLUG}__{table_name(fg)}"


def ensure_dataset(api, database_id: int, schema: str, fg: dict) -> int:
    """Reuse this dashboard's dataset for the feature group version, or create it as a virtual dataset."""
    name = dataset_name(fg)
    for ds in list_all(api, "dataset"):
        if ds.get("table_name") == name and ds.get("schema") == schema:
            return ds["id"]
    sql = f"SELECT * FROM {fg.get('format', 'delta')}.{schema}.{table_name(fg)}"
    return api.create_dataset(database_id=database_id, table_name=name, schema=schema, sql=sql)[
        "id"
    ]


def find_dashboard(api) -> int | None:
    """This program's dashboard, found by its title."""
    return next(
        (d["id"] for d in list_all(api, "dashboard") if d.get("dashboard_title") == TITLE),
        None,
    )


def owned_charts(api, dashboard_id: int | None) -> list[int]:
    """Charts this program made: linked only to its dashboard, or unlinked and named as in CHARTS.

    A chart linked to another dashboard as well is detached from this one and kept, and a
    chart of the same name on another dashboard belongs to that dashboard and is left alone.
    """
    names = {chart["slice_name"] for chart in CHARTS}
    owned = []
    for chart in list_all(api, "chart"):
        linked = {d.get("id") for d in api.get_chart(chart["id"])["result"].get("dashboards") or []}
        if dashboard_id is not None and dashboard_id in linked:
            others = sorted(linked - {dashboard_id})
            if others:
                api.update_chart(chart["id"], dashboards=others)
            else:
                owned.append(chart["id"])
        elif not linked and chart.get("slice_name") in names:
            owned.append(chart["id"])
    return owned


def create_chart(api, chart: dict, datasource_id: int) -> int:
    """Create one chart from its entry in CHARTS."""
    params = {"viz_type": chart["viz_type"], **chart["params"]}
    return api.create_chart(
        slice_name=chart["slice_name"],
        viz_type=chart["viz_type"],
        datasource_id=datasource_id,
        params=json.dumps(params),
    )["id"]


def position_json(chart_ids: list[int]) -> str:
    """A grid of rows whose widths sum to 12, one chart cell per chart, in CHARTS order."""
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": TITLE}},
    }
    row, used = [], 0

    def close_row() -> None:
        row_id = f"ROW-{len(layout['GRID_ID']['children']) + 1}"
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": list(row),
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for child in row:
            layout[child]["parents"] = ["ROOT_ID", "GRID_ID", row_id]
        layout["GRID_ID"]["children"].append(row_id)
        row.clear()

    for chart, chart_id in zip(CHARTS, chart_ids, strict=True):
        width = chart.get("width", 6)
        if used + width > 12:
            close_row()
            used = 0
        node = f"CHART-{chart_id}"
        layout[node] = {
            "type": "CHART",
            "id": node,
            "children": [],
            "parents": [],
            "meta": {
                "width": width,
                "height": 50,
                "chartId": chart_id,
                "sliceName": chart["slice_name"],
            },
        }
        row.append(node)
        used += width
    if row:
        close_row()
    return json.dumps(layout)


def ensure_dashboard(api, existing: int | None, chart_ids: list[int]) -> int:
    """Create the dashboard or update it in place, and link every chart to it."""
    layout = position_json(chart_ids)
    if existing is None:
        dashboard_id = api.create_dashboard(
            dashboard_title=TITLE, published=True, position_json=layout
        )["id"]
    else:
        dashboard_id = existing
        api.update_dashboard(
            dashboard_id, dashboard_title=TITLE, published=True, position_json=layout
        )
    for chart_id in chart_ids:
        api.update_chart(chart_id, dashboards=[dashboard_id])
    return dashboard_id


def chart_query(params: dict) -> dict:
    """The query a chart's params ask Superset for, in the shape of /api/v1/chart/data."""
    metrics, columns, filters, where = [], [], [], []

    def add(target: list, value) -> None:
        for item in value if isinstance(value, list) else [value]:
            if item and item not in target:
                target.append(item)

    for key in ("metric", "metrics", "size"):
        add(metrics, params.get(key))
    for key in ("x_axis", "y_axis"):
        value = params.get(key)
        add(metrics if isinstance(value, dict) else columns, value)
    for key in ("column", "groupby", "groupbyRows", "groupbyColumns", "entity", "all_columns"):
        add(columns, params.get(key))
    for f in params.get("adhoc_filters") or []:
        if f.get("expressionType") == "SQL":
            where.append(f"({f['sqlExpression']})")
        else:
            filters.append({"col": f["subject"], "op": f["operator"], "val": f.get("comparator")})
    query = {"metrics": metrics, "columns": columns, "filters": filters, "row_limit": 1}
    if where:
        query["extras"] = {"where": " AND ".join(where)}
    return query


def check_charts(api, chart_ids: list[int], datasets: dict[str, int]) -> list[str]:
    """Run every chart's query once; return a line per chart that errors or returns no rows."""
    problems = []
    for chart, chart_id in zip(CHARTS, chart_ids, strict=True):
        payload = {
            "datasource": {"id": datasets[chart["dataset"]], "type": "table"},
            "queries": [chart_query(chart["params"])],
            "result_format": "json",
            "result_type": "full",
        }
        try:
            result = api._request("POST", "/api/v1/chart/data", json_data=payload)["result"][0]
        except Exception as exc:  # noqa: BLE001 - every failing chart is reported, then the run fails
            problems.append(f"{chart['slice_name']} (chart {chart_id}): {exc}")
            continue
        if result.get("error"):
            problems.append(f"{chart['slice_name']} (chart {chart_id}): {result['error']}")
        elif not result.get("rowcount") and not result.get("data"):
            problems.append(f"{chart['slice_name']} (chart {chart_id}): the query returns no rows")
    return problems


def build(api, project_name: str) -> int:
    """Create or update the datasets, charts and dashboard; return the dashboard id."""
    schema = f"{project_name.lower()}_featurestore"
    database_id = find_trino_db_id(api)
    datasets = {fg["name"]: ensure_dataset(api, database_id, schema, fg) for fg in FEATURE_GROUPS}
    existing = find_dashboard(api)
    # Replace every chart this dashboard had, so a chart dropped from CHARTS goes too.
    for chart_id in owned_charts(api, existing):
        api.delete_chart(chart_id)
    chart_ids = [create_chart(api, chart, datasets[chart["dataset"]]) for chart in CHARTS]
    dashboard_id = ensure_dashboard(api, existing, chart_ids)
    started = time.monotonic()
    problems = check_charts(api, chart_ids, datasets)
    print(f"[time] chart check {time.monotonic() - started:.1f}s", flush=True)
    if problems:
        raise SystemExit("charts that do not render:\n  " + "\n  ".join(problems))
    return dashboard_id


def delete(api, project_name: str) -> dict[str, list]:
    """Remove this program's dashboard and charts, and its datasets no other chart uses."""
    removed: dict[str, list] = {"dashboards": [], "charts": [], "datasets": []}
    dashboard_id = find_dashboard(api)
    charts = owned_charts(api, dashboard_id)
    if dashboard_id is not None:
        api.delete_dashboard(dashboard_id)
        removed["dashboards"].append(dashboard_id)
    for chart_id in charts:
        api.delete_chart(chart_id)
        removed["charts"].append(chart_id)
    schema = f"{project_name.lower()}_featurestore"
    ours = {dataset_name(fg) for fg in FEATURE_GROUPS}
    in_use = {c.get("datasource_id") for c in list_all(api, "chart")}
    for ds in list_all(api, "dataset"):
        if ds.get("table_name") in ours and ds.get("schema") == schema and ds["id"] not in in_use:
            api.delete_dataset(ds["id"])
            removed["datasets"].append(ds["id"])
    return removed


def main(argv: list[str] | None = None) -> int:
    """Build the dashboard and print its URL, delete it, or list the project's dashboards."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--delete", action="store_true", help="remove what this program created")
    mode.add_argument("--list", action="store_true", help="print every dashboard as JSON")
    # A scheduled run appends -start_time <fire instant> (and may append -end_time):
    # the instant the schedule fired, which ends the window rather than starting it.
    # They are accepted and ignored; a scheduled run's window is HOPS_START_TIME/HOPS_END_TIME.
    parser.add_argument("-start_time", dest="_fired_at", help=argparse.SUPPRESS)
    parser.add_argument("-end_time", dest="_fired_end", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    import hopsworks

    project = hopsworks.login()
    api = project.get_superset_api()
    if args.list:
        dashboards = list_all(api, "dashboard")
        print(json.dumps([{"id": d["id"], "title": d.get("dashboard_title")} for d in dashboards]))
        return 0
    if args.delete:
        print(json.dumps(delete(api, project.name)))
        return 0
    started = time.monotonic()
    dashboard_id = build(api, project.name)
    print(f"[time] build {time.monotonic() - started:.1f}s", flush=True)
    host = urlparse(project.get_url())
    print(
        f"{host.scheme}://{host.netloc}/hopsworks-api/superset/superset/dashboard/{dashboard_id}/"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

# ruff: noqa: INP001
"""TPC-H Q8: Brazil's share of the American market.

Feature groups: tpch_brazil_market_share v1, tpch_america_volume_by_nation v1.
Charts: Brazil's market share in the last year (big number), market share by year (bar),
the American market by supplier nation and year (stacked bar), the yearly figures (table).

The program behind the TPC-H pipeline's Superset dashboard, run after tpch_pipeline.py
has written the feature groups. It follows hops-superset's dashboard program: the
docstring above is the record of what the dashboard is.

    python tpch_dashboard.py            # create, or update in place
    python tpch_dashboard.py --delete   # remove what this program created
    python tpch_dashboard.py --list     # every dashboard in the project, as JSON

Superset is reachable only inside the cluster, so the program runs in a Hopsworks
terminal or as a Hopsworks job:

    hops job deploy <slug>-dashboard <HopsFS path>/tpch_dashboard.py --env python-feature-pipeline --run --wait

Every step is idempotent, so a re-run updates the charts and the layout instead of
duplicating them. `--delete` removes the dashboard, its charts, and the datasets it
created that no other chart still uses, and nothing else.
"""

from __future__ import annotations

import argparse
import json
import sys
from urllib.parse import urlparse


TITLE = "TPC-H Q8: Brazil's share of the American market"
FEATURE_GROUPS = [
    {"name": "tpch_brazil_market_share", "version": 1, "format": "delta"},
    {"name": "tpch_america_volume_by_nation", "version": 1, "format": "delta"},
]


def metric(sql: str, label: str) -> dict:
    """An adhoc SQL metric."""
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "label": label,
        "optionName": f"metric_{label}",
        "hasCustomLabel": True,
    }


LAST_SHARE = metric("MAX_BY(mkt_share, o_year)", "share_last_year")
SHARE = metric("SUM(mkt_share)", "mkt_share")
VOLUME = metric("SUM(volume)", "volume")
BRAZIL = metric("SUM(brazil_volume)", "brazil_volume")
AMERICA = metric("SUM(america_volume)", "america_volume")

# One entry per chart: the dataset it reads (a feature group above) and the
# viz-type specific params from hops-superset/references/chart_params.md.
CHARTS = [
    {
        "slice_name": "Brazil's market share, last year",
        "dataset": "tpch_brazil_market_share",
        "viz_type": "big_number_total",
        "params": {
            "metric": LAST_SHARE,
            "adhoc_filters": [],
            "y_axis_format": ".2%",
            "subheader": "of the American market for the part type",
        },
        "width": 4,
    },
    {
        "slice_name": "Brazil's market share by year",
        "dataset": "tpch_brazil_market_share",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "o_year",
            "x_axis_force_categorical": True,
            "metrics": [SHARE],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 100,
            "orientation": "vertical",
            "x_axis_sort": "o_year",
            "x_axis_sort_asc": True,
            "show_legend": False,
            "show_value": True,
            "y_axis_format": ".2%",
        },
        "width": 8,
    },
    {
        "slice_name": "American market by supplier nation and year",
        "dataset": "tpch_america_volume_by_nation",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "x_axis": "supplier_nation",
            "x_axis_force_categorical": True,
            "metrics": [VOLUME],
            "groupby": ["o_year"],
            "adhoc_filters": [],
            "row_limit": 1000,
            "orientation": "vertical",
            "stack": "Stack",
            "timeseries_limit_metric": VOLUME,
            "x_axis_sort": "volume",
            "x_axis_sort_asc": False,
            "show_legend": True,
            "y_axis_format": "SMART_NUMBER",
        },
        "width": 12,
    },
    {
        "slice_name": "Market share by year",
        "dataset": "tpch_brazil_market_share",
        "viz_type": "table",
        "params": {
            "query_mode": "aggregate",
            "groupby": ["o_year"],
            "metrics": [SHARE, BRAZIL, AMERICA],
            "adhoc_filters": [],
            "row_limit": 100,
            "order_by_cols": [json.dumps(["o_year", True])],
            "show_cell_bars": True,
            "column_config": {"mkt_share": {"d3NumberFormat": ".2%"}},
        },
        "width": 12,
    },
]


def list_all(api, resource: str) -> list[dict]:
    """Every object of a kind; the public list_* methods return only the first page."""
    items, page = [], 0
    while True:
        batch = api._request(
            "GET", f"/api/v1/{resource}/?q=(page:{page},page_size:100)"
        ).get("result", [])
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


def dataset_name(fg: dict) -> str:
    """The Superset dataset name for a feature group version."""
    return f"{fg['name']}_{fg['version']}"


def ensure_dataset(api, database_id: int, schema: str, fg: dict) -> int:
    """Reuse the dataset for this feature group version, or create it as a virtual dataset."""
    name = dataset_name(fg)
    for ds in list_all(api, "dataset"):
        if ds.get("table_name") == name and ds.get("schema") == schema:
            return ds["id"]
    sql = f"SELECT * FROM {fg.get('format', 'delta')}.{schema}.{name}"
    return api.create_dataset(
        database_id=database_id, table_name=name, schema=schema, sql=sql
    )["id"]


def find_dashboard(api) -> int | None:
    """This program's dashboard, found by its title."""
    return next(
        (
            d["id"]
            for d in list_all(api, "dashboard")
            if d.get("dashboard_title") == TITLE
        ),
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
        linked = {
            d.get("id")
            for d in api.get_chart(chart["id"])["result"].get("dashboards") or []
        }
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


def build(api, project_name: str) -> int:
    """Create or update the datasets, charts and dashboard; return the dashboard id."""
    schema = f"{project_name.lower()}_featurestore"
    database_id = find_trino_db_id(api)
    datasets = {
        fg["name"]: ensure_dataset(api, database_id, schema, fg)
        for fg in FEATURE_GROUPS
    }
    existing = find_dashboard(api)
    # Replace every chart this dashboard had, so a chart dropped from CHARTS goes too.
    for chart_id in owned_charts(api, existing):
        api.delete_chart(chart_id)
    chart_ids = [
        create_chart(api, chart, datasets[chart["dataset"]]) for chart in CHARTS
    ]
    return ensure_dashboard(api, existing, chart_ids)


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
        if (
            ds.get("table_name") in ours
            and ds.get("schema") == schema
            and ds["id"] not in in_use
        ):
            api.delete_dataset(ds["id"])
            removed["datasets"].append(ds["id"])
    return removed


def main(argv: list[str] | None = None) -> int:
    """Build the dashboard and print its URL, delete it, or list the project's dashboards."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--delete", action="store_true", help="remove what this program created"
    )
    mode.add_argument(
        "--list", action="store_true", help="print every dashboard as JSON"
    )
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
        print(
            json.dumps(
                [{"id": d["id"], "title": d.get("dashboard_title")} for d in dashboards]
            )
        )
        return 0
    if args.delete:
        print(json.dumps(delete(api, project.name)))
        return 0
    dashboard_id = build(api, project.name)
    host = urlparse(project.get_url())
    print(
        f"{host.scheme}://{host.netloc}/hopsworks-api/superset/superset/dashboard/{dashboard_id}/"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

---
name: hops-superset
description: Use when building Superset charts or dashboards inside Hopsworks via the Python SDK. Auto-invoke when the user wants to create Superset charts/dashboards/datasets, visualize a feature group in Superset, or interact with `project.get_superset_api()`.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Superset — Charts, Datasets, and Dashboards

Hopsworks exposes Apache Superset as a managed service. This skill covers the
Python SDK wrapper for the Superset REST API (`project.get_superset_api()`),
how to surface Hopsworks feature groups as Superset datasets via Trino, and
the modern viz_type keys + param schemas this Superset version expects.

The Python client lives in the Hopsworks venv:
`hopsworks_common/core/superset_api.py` (inside
`/srv/hops/venv/lib/python3.13/site-packages/` on a Hopsworks host).

---

## When to use this skill

Use this skill whenever the user wants to:
- Render a feature group (or any Trino table) in Superset
- Create / update / delete Superset charts, datasets, or dashboards programmatically
- Wire a Hopsworks feature group into an existing Superset dashboard
- Debug Superset chart errors like `Item with key "X" is not registered` or `Empty query?`

---

## 1. Connect to the Superset API

The Hopsworks SDK returns a pre-authenticated Superset REST client. Always go
through it — it handles session cookies and CSRF.

```python
import hopsworks

project = hopsworks.login()
api = project.get_superset_api()
```

Methods available on `api`:

| Area | Methods |
|---|---|
| Databases | `list_databases()` |
| Datasets | `create_dataset`, `get_dataset`, `list_datasets`, `update_dataset`, `delete_dataset` |
| Charts | `create_chart`, `get_chart`, `list_charts`, `update_chart`, `delete_chart` |
| Dashboards | `create_dashboard`, `get_dashboard`, `list_dashboards`, `update_dashboard`, `delete_dashboard` |

Signatures (from the Hopsworks SDK):

```python
api.create_dataset(database_id: int, table_name: str, schema: str | None = None,
                   sql: str | None = None, description: str | None = None,
                   owners: list[int] | None = None) -> dict
api.update_dataset(dataset_id: int, **kwargs) -> dict
api.delete_dataset(dataset_id: int) -> dict

api.create_chart(slice_name: str, viz_type: str, datasource_id: int, params: str,
                 datasource_type: str = "table", description: str | None = None,
                 dashboards: list[int] | None = None,
                 owners: list[int] | None = None) -> dict
api.update_chart(chart_id: int, **kwargs) -> dict     # e.g. dashboards=[id], params=...
api.delete_chart(chart_id: int) -> dict

api.create_dashboard(dashboard_title: str, published: bool = False,
                     slug: str | None = None, position_json: str | None = None,
                     json_metadata: str | None = None, css: str | None = None,
                     owners: list[int] | None = None) -> dict
api.update_dashboard(dashboard_id: int, **kwargs) -> dict
api.delete_dashboard(dashboard_id: int) -> dict
```

Raw requests (for paging, or endpoints the SDK doesn't wrap) go through
`api._request("GET", "/api/v1/...")` — the same client, auth headers included.

### Paging helper

`list_*()` returns only the first page (~25 rows). Paginate manually:

```python
def list_all(api, resource):
    items, page = [], 0
    while True:
        j = api._request("GET", f"/api/v1/{resource}/?q=(page:{page},page_size:100)")
        batch = j.get("result", [])
        items.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return items
```

---

## 2. Reading feature groups in Superset (via Trino)

Feature groups are **not** exposed as Superset-native tables. They are queried
through the Trino database connection with this naming pattern:

```
delta.<project>_featurestore.<feature_group>_<version>
```

Example (project `af`, feature group `customers` v1):

```sql
SELECT * FROM delta.af_featurestore.customers_1
```

Rules:
- Catalog is `delta` for all offline feature groups (Delta format).
- Schema is `<project>_featurestore` (project name is lowercase).
- Table name is `<fg_name>_<version>` — the version suffix is required.
- The Trino DB connection in Superset defaults its catalog to `hive`, so you
  **must** fully qualify with `delta.` or the query resolves to the wrong catalog.
- Trino database id varies per Hopsworks install. Use `api.list_databases()`
  to look it up, or cache it (e.g. Trino DB id = 2 on project `af`).

### Look up the Trino database id

```python
def find_trino_db_id(api):
    for db in api.list_databases()["result"]:
        # Names vary: "trino", "Trino", "trino-<project>"...
        if "trino" in db.get("database_name", "").lower():
            return db["id"]
    raise RuntimeError("No Trino database connection found in Superset")
```

### Create a Superset dataset for a feature group (virtual dataset)

Use a **virtual dataset** (a SELECT expression). A physical table reference
will not resolve the `delta.` catalog correctly.

```python
import hopsworks

project = hopsworks.login()
api = project.get_superset_api()

project_name = project.name.lower()                # e.g. "af"
fg_name, fg_version = "customers", 1

trino_db_id = find_trino_db_id(api)                # or a cached int
schema = f"{project_name}_featurestore"
sql = f"SELECT * FROM delta.{schema}.{fg_name}_{fg_version}"

created = api.create_dataset(
    database_id=trino_db_id,
    table_name=fg_name,                            # display name in Superset
    schema=schema,
    sql=sql,
)
dataset_id = created["id"]

# NOTE: Do NOT pass `description=...` to create_dataset. The SDK accepts
# the kwarg but the Superset REST API in this deployment rejects it with
# `400 {"message":{"description":["Unknown field."]}}`. Same for
# create_chart / create_dashboard — keep to the core fields only.
```

### Idempotent create-or-reuse

`create_dataset` fails if the (schema, table_name) pair already exists.
List-then-create:

```python
def ensure_dataset(api, database_id, schema, name, sql):
    for ds in list_all(api, "dataset"):
        if ds.get("table_name") == name and ds.get("schema") == schema:
            return ds["id"]
    return api.create_dataset(
        database_id=database_id, table_name=name, schema=schema, sql=sql,
    )["id"]
```

Use the same pattern for charts (key on `slice_name`) and dashboards
(key on `dashboard_title`).

---

## 3. Viz types — use the MODERN keys

Legacy viz_type keys are unregistered in current Superset and fail with
`Item with key "X" is not registered`. Hand-rolled chart scripts often copy
stale examples — always use the keys below.

### Complete registered viz_type enum

From Superset's `VizType` enum — these are the only keys the server accepts.

| Category | Viz name | viz_type key |
|---|---|---|
| Big number | Big Number | `big_number` |
| Big number | Big Number Total | `big_number_total` |
| Big number | Period-over-period KPI | `pop_kpi` |
| Categorical | Pie | `pie` |
| Categorical | Funnel | `funnel` |
| Categorical | Radar | `radar` |
| Categorical | Rose | `rose` |
| Categorical | Chord | `chord` |
| Categorical | Sankey | `sankey_v2` |
| Categorical | Sunburst | `sunburst_v2` |
| Categorical | Treemap | `treemap_v2` |
| Categorical | Partition | `partition` |
| Distribution | Histogram | `histogram_v2` |
| Distribution | Box Plot | `box_plot` |
| Time series | Bar (categorical or time) | `echarts_timeseries_bar` |
| Time series | Line | `echarts_timeseries_line` |
| Time series | Smooth Line | `echarts_timeseries_smooth` |
| Time series | Step | `echarts_timeseries_step` |
| Time series | Area | `echarts_area` |
| Time series | Scatter | `echarts_timeseries_scatter` |
| Time series | Generic time series | `echarts_timeseries` |
| Time series | Mixed time series | `mixed_timeseries` |
| Time series | Waterfall | `waterfall` |
| Time series | Compare | `compare` |
| Time series | Time Pivot | `time_pivot` |
| Time series | Time Table | `time_table` |
| Correlation | Heatmap | `heatmap_v2` |
| Correlation | Bubble | `bubble_v2` |
| Correlation | Parallel Coords | `para` |
| Correlation | Paired t-test | `paired_ttest` |
| Tabular | Table | `table` |
| Tabular | Table (AG Grid) | `ag-grid-table` |
| Tabular | Pivot Table | `pivot_table_v2` |
| Gauge/other | Gauge | `gauge_chart` |
| Gauge/other | Bullet | `bullet` |
| Gauge/other | Calendar heatmap | `cal_heatmap` |
| Maps | Country Map | `country_map` |
| Maps | World Map | `world_map` |
| Maps | MapBox | `mapbox` |
| Maps | Point cluster | `point_cluster_map` |
| Maps | Cartodiagram | `cartodiagram` |
| Other | Graph | `graph_chart` |
| Other | Tree | `tree_chart` |
| Other | Gantt | `gantt_chart` |
| Other | Horizon | `horizon` |
| Other | Word cloud | `word_cloud` |
| Other | Handlebars | `handlebars` |

### Legacy keys that will FAIL

| Legacy (don't use) | Modern replacement |
|---|---|
| `dist_bar` | `echarts_timeseries_bar` (with `x_axis_force_categorical: true`) |
| `bar` | `echarts_timeseries_bar` |
| `line` | `echarts_timeseries_line` |
| `area` | `echarts_area` |
| `histogram` | `histogram_v2` |
| `heatmap` | `heatmap_v2` |
| `bubble` | `bubble_v2` |
| `pivot_table` | `pivot_table_v2` |
| `sankey` | `sankey_v2` |
| `sunburst` | `sunburst_v2` |
| `treemap` | `treemap_v2` |

---

## 4. Creating charts — param schemas

`create_chart` takes a JSON-string `params` blob whose shape is **viz-type
specific**. The shared `COUNT_METRIC` below is a valid adhoc SQL metric
reusable across every viz:

```python
import json

COUNT_METRIC = {
    "expressionType": "SQL",
    "sqlExpression": "COUNT(*)",
    "label": "count",
    "optionName": "metric_count",
    "hasCustomLabel": True,
}

# Or an aggregate on a column:
SUM_AMOUNT = {
    "expressionType": "SIMPLE",
    "column": {"column_name": "amount", "type": "DOUBLE"},
    "aggregate": "SUM",
    "label": "total_amount",
    "optionName": "metric_sum_amount",
    "hasCustomLabel": True,
}

# adhoc_filters (reusable building block):
# Simple column filter:
#   {"expressionType": "SIMPLE", "subject": "country", "operator": "==",
#    "comparator": "US", "clause": "WHERE"}
# Raw SQL filter:
#   {"expressionType": "SQL", "sqlExpression": "amount > 100", "clause": "WHERE"}
```

### big_number_total — single KPI

```python
params = {
    "viz_type": "big_number_total",
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "y_axis_format": "SMART_NUMBER",
    "subheader": "Total customers",
}
```

### big_number — KPI with trend line

```python
params = {
    "viz_type": "big_number",
    "metric": COUNT_METRIC,
    "granularity_sqla": "created_at",     # temporal column
    "time_range": "No filter",
    "adhoc_filters": [],
    "compare_lag": "1",
    "compare_suffix": "vs prev",
    "y_axis_format": "SMART_NUMBER",
}
```

### pie

```python
params = {
    "viz_type": "pie",
    "groupby": ["category_col"],
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "row_limit": 100,
    "sort_by_metric": True,
    "show_legend": True,
    "label_type": "key_percent",          # or "key", "value", "percent"
    "donut": False,
    "innerRadius": 30,
    "outerRadius": 70,
}
```

### echarts_timeseries_bar — bar chart (use for categorical bars too)

This replaces the legacy `dist_bar`. For a **categorical** (non-temporal)
x-axis, set `x_axis_force_categorical: true`.

```python
params = {
    "viz_type": "echarts_timeseries_bar",
    "x_axis": "some_category_col",        # this is the grouping column, NOT `groupby`
    "x_axis_force_categorical": True,     # REQUIRED for string x_axis
    "metrics": [COUNT_METRIC],
    "groupby": [],                        # only for series breakdown (stacked)
    "adhoc_filters": [],
    "row_limit": 100,
    "orientation": "vertical",            # or "horizontal"
    "order_desc": True,
    # Sort bars by metric:
    "timeseries_limit_metric": COUNT_METRIC,
    "x_axis_sort": "count",               # must match the metric's `label`
    "x_axis_sort_asc": False,
    "show_legend": False,
    "y_axis_format": "SMART_NUMBER",
}
```

For a **time-series** bar chart, use a datetime column as `x_axis` and drop
`x_axis_force_categorical`. Add `time_grain_sqla` (e.g. `"P1D"`, `"P1M"`).

### echarts_timeseries_line

```python
params = {
    "viz_type": "echarts_timeseries_line",
    "x_axis": "event_ts",
    "time_grain_sqla": "P1D",             # P1D=day, PT1H=hour, P1M=month, P1Y=year
    "metrics": [COUNT_METRIC],
    "groupby": [],                        # series breakdown columns
    "adhoc_filters": [],
    "row_limit": 10000,
    "show_legend": True,
    "markerEnabled": False,
    "y_axis_format": "SMART_NUMBER",
}
```

### histogram_v2

```python
params = {
    "viz_type": "histogram_v2",
    "column": "numeric_col",              # SINGLE string (not `all_columns_x` array)
    "groupby": [],                        # optional series breakdown
    "adhoc_filters": [],
    "row_limit": 50000,
    "bins": 20,
    "normalize": False,
    "cumulative": False,
    "x_axis_title": "Value",              # NOT `x_axis_label`
    "y_axis_title": "Count",              # NOT `y_axis_label`
    "x_axis_format": "SMART_NUMBER",
    "y_axis_format": "SMART_NUMBER",
}
```

### table — tabular view

```python
params = {
    "viz_type": "table",
    "query_mode": "aggregate",            # or "raw" for row-level
    "groupby": ["col1", "col2"],
    "metrics": [COUNT_METRIC],
    "adhoc_filters": [],
    "row_limit": 100,
    "order_by_cols": [json.dumps(["count", False])],  # list of JSON-encoded [col, asc]
    "show_cell_bars": True,
    "table_timestamp_format": "smart_date",
}
```

### pivot_table_v2

```python
params = {
    "viz_type": "pivot_table_v2",
    "groupbyRows": ["row_col"],
    "groupbyColumns": ["pivot_col"],
    "metrics": [COUNT_METRIC],
    "adhoc_filters": [],
    "row_limit": 10000,
    "aggregateFunction": "Sum",
    "rowTotals": True,
    "colTotals": True,
}
```

### heatmap_v2

```python
params = {
    "viz_type": "heatmap_v2",
    "x_axis": "x_col",
    "groupby": "y_col",
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "row_limit": 10000,
    "normalize_across": "heatmap",        # or "x", "y"
    "linear_color_scheme": "schemeBlues",
}
```

### bubble_v2 (scatter with size)

```python
params = {
    "viz_type": "bubble_v2",
    "x_axis": {"expressionType": "SIMPLE",
               "column": {"column_name": "feature_x", "type": "DOUBLE"},
               "aggregate": "AVG", "label": "x"},
    "y_axis": {"expressionType": "SIMPLE",
               "column": {"column_name": "feature_y", "type": "DOUBLE"},
               "aggregate": "AVG", "label": "y"},
    "size": COUNT_METRIC,
    "entity": "group_col",
    "adhoc_filters": [],
    "row_limit": 1000,
}
```

### box_plot

```python
params = {
    "viz_type": "box_plot",
    "x_axis": "category_col",
    "x_axis_force_categorical": True,
    "metrics": [SUM_AMOUNT],              # numeric metric(s) to summarize
    "groupby": [],
    "adhoc_filters": [],
    "row_limit": 10000,
    "whiskerOptions": "Tukey",            # or "Min/max", "2/98 percentiles", etc.
}
```

### funnel

```python
params = {
    "viz_type": "funnel",
    "groupby": ["stage_col"],
    "metric": COUNT_METRIC,
    "adhoc_filters": [],
    "row_limit": 10,
    "sort_by_metric": True,
}
```

### Replace-by-name helper for idempotent chart scripts

```python
def replace_chart(api, slice_name, **kwargs):
    for c in list_all(api, "chart"):
        if c.get("slice_name") == slice_name:
            api.delete_chart(c["id"])
    return api.create_chart(slice_name=slice_name, **kwargs)["id"]
```

---

## 5. Creating dashboards

A dashboard needs (a) a `position_json` layout **and** (b) an explicit
chart→dashboard link via `update_chart(dashboards=[id])`. The layout alone
does not populate the chart's "Dashboards" tab in the UI.

### Layout primer

Superset uses a 12-column grid. Row heights are in ~25px units. Every node
has `id`, `type`, `children`, `parents`, and `meta`. Widths must sum to 12
within each row.

Node types used in `position_json`:

| Type | Purpose | `meta` fields |
|---|---|---|
| `ROOT` | Always present, single child `GRID_ID` | — |
| `GRID` | The 12-col grid, children are rows | — |
| `HEADER` | Dashboard title header | `text` |
| `ROW` | Horizontal row, children sum to 12 cols | `background` |
| `CHART` | A chart cell | `width`, `height`, `chartId`, `sliceName` |
| `MARKDOWN` | Static markdown cell | `width`, `height`, `code` |
| `COLUMN` | Vertical column container | `width`, `background` |
| `TABS` / `TAB` | Tabbed sections | `text` (on TAB) |

### Layout builder

```python
import json

def build_position_json(chart_ids, chart_slices, title):
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": [],                # filled below
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }

    def chart(key, width, height):
        nid = f"CHART-{key}"
        layout[nid] = {
            "type": "CHART", "id": nid, "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"width": width, "height": height,
                     "chartId": chart_ids[key], "sliceName": chart_slices[key]},
        }
        return nid

    def row(row_id, children):
        layout[row_id] = {
            "type": "ROW", "id": row_id, "children": children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for c in children:
            layout[c]["parents"] = ["ROOT_ID", "GRID_ID", row_id]
        layout["GRID_ID"]["children"].append(row_id)

    row("ROW-1", [chart("a", 6, 50), chart("b", 6, 50)])    # two half-width charts
    row("ROW-2", [chart("c", 12, 50)])                      # one full-width chart
    return json.dumps(layout)
```

### Create / update idempotently

```python
def ensure_dashboard(api, title, chart_ids, chart_slices):
    position_json = build_position_json(chart_ids, chart_slices, title)

    dashboard_id = next(
        (d["id"] for d in list_all(api, "dashboard")
         if d.get("dashboard_title") == title),
        None,
    )

    if dashboard_id is None:
        dashboard_id = api.create_dashboard(
            dashboard_title=title, published=True, position_json=position_json,
        )["id"]
    else:
        api.update_dashboard(
            dashboard_id, dashboard_title=title, published=True,
            position_json=position_json,
        )

    # Persist the chart -> dashboard relation explicitly.
    for cid in chart_ids.values():
        api.update_chart(cid, dashboards=[dashboard_id])

    return dashboard_id
```

Dashboard URL:
```
https://<hopsworks-host>/hopsworks-api/superset/superset/dashboard/<id>/
```

---

## 6. End-to-end pattern

```python
import json
import hopsworks

PROJECT_NAME = "af"
FG_NAME, FG_VERSION = "customers", 1

COUNT_METRIC = {
    "expressionType": "SQL", "sqlExpression": "COUNT(*)",
    "label": "count", "optionName": "metric_count", "hasCustomLabel": True,
}


def main():
    project = hopsworks.login()
    api = project.get_superset_api()

    trino_db_id = find_trino_db_id(api)
    schema = f"{PROJECT_NAME}_featurestore"
    sql = f"SELECT * FROM delta.{schema}.{FG_NAME}_{FG_VERSION}"

    dataset_id = ensure_dataset(
        api, database_id=trino_db_id, schema=schema, name=FG_NAME, sql=sql,
    )

    chart_ids = {
        "total": replace_chart(
            api, slice_name="Total Customers",
            viz_type="big_number_total", datasource_id=dataset_id,
            params=json.dumps({
                "viz_type": "big_number_total", "metric": COUNT_METRIC,
                "adhoc_filters": [], "y_axis_format": "SMART_NUMBER",
            }),
        ),
        "by_state": replace_chart(
            api, slice_name="Top States",
            viz_type="echarts_timeseries_bar", datasource_id=dataset_id,
            params=json.dumps({
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "state", "x_axis_force_categorical": True,
                "metrics": [COUNT_METRIC], "groupby": [], "adhoc_filters": [],
                "row_limit": 20, "orientation": "vertical", "order_desc": True,
                "timeseries_limit_metric": COUNT_METRIC,
                "x_axis_sort": "count", "x_axis_sort_asc": False,
                "y_axis_format": "SMART_NUMBER",
            }),
        ),
        "age_hist": replace_chart(
            api, slice_name="Age Distribution",
            viz_type="histogram_v2", datasource_id=dataset_id,
            params=json.dumps({
                "viz_type": "histogram_v2", "column": "age",
                "groupby": [], "adhoc_filters": [], "row_limit": 50000,
                "bins": 20, "x_axis_title": "Age", "y_axis_title": "Customers",
            }),
        ),
    }

    chart_slices = {"total": "Total Customers", "by_state": "Top States",
                    "age_hist": "Age Distribution"}
    dashboard_id = ensure_dashboard(
        api, "Customers Overview", chart_ids, chart_slices,
    )
    print(f"Dashboard id: {dashboard_id}")


if __name__ == "__main__":
    main()
```

---

## 7. Debugging cheatsheet

| Symptom | Likely cause | Fix |
|---|---|---|
| `Item with key "X" is not registered` | Legacy viz_type removed | Swap to modern key from §3 |
| `Empty query?` (histogram) | Used `all_columns_x` | Use `column` (single string) |
| `Empty query?` (bar) | Used `groupby` instead of `x_axis` | Set `x_axis`, optionally add `x_axis_force_categorical: true` |
| `Dataset ... already exists` | Non-idempotent create | Use `ensure_dataset` (list-then-create) |
| `400 {"message":{"description":["Unknown field."]}}` | SDK signature includes `description` but Superset REST rejects it | Drop `description` from `create_dataset`/`create_chart`/`create_dashboard` calls |
| Chart renders but absent from dashboard | Only `position_json` was set | Also call `update_chart(cid, dashboards=[dashboard_id])` |
| Trino query "Schema not found" | Missing `delta.` prefix | Use `SELECT * FROM delta.<project>_featurestore.<fg>_<version>` |
| Trino query "Table not found" | Missing `_<version>` suffix | FG tables are always suffixed with version (`customers_1`, not `customers`) |
| Bars come out unsorted | `x_axis_sort` doesn't match a metric label | Make `x_axis_sort` equal to the metric's `label` field |
| Time-series bar shows one bar | X axis is a string but `x_axis_force_categorical` missing | Add `x_axis_force_categorical: true` |
| Auth error from `api._request` | Session expired across long runs | Re-fetch `project.get_superset_api()` |

---

## Quick Reference

| Task | Code |
|---|---|
| Get Superset API | `api = project.get_superset_api()` |
| List Trino DBs | `api.list_databases()` |
| FG as Trino table | `delta.<project>_featurestore.<fg>_<version>` |
| Create virtual dataset | `api.create_dataset(database_id=..., table_name=..., schema=..., sql=...)` |
| Create chart | `api.create_chart(slice_name=..., viz_type=..., datasource_id=..., params=json.dumps({...}))` |
| Link chart to dashboard | `api.update_chart(chart_id, dashboards=[dashboard_id])` |
| Create dashboard | `api.create_dashboard(dashboard_title=..., published=True, position_json=...)` |
| Delete | `api.delete_chart(id)` / `api.delete_dataset(id)` / `api.delete_dashboard(id)` |
| Paginate any list | `api._request("GET", f"/api/v1/{resource}/?q=(page:{p},page_size:100)")` |

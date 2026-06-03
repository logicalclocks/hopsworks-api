---
name: hops-superset
description: Use when building Superset charts or dashboards inside Hopsworks via the Python SDK. Auto-invoke when the user wants to create Superset charts/dashboards/datasets, visualize a feature group in Superset, or interact with `project.get_superset_api()`. Input an offline-materialized FG; output a published Superset dashboard + URL.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Superset — Charts, Datasets, and Dashboards

Render Hopsworks feature groups as Superset charts and dashboards via the Python
SDK (`project.get_superset_api()`).

## Contract

- **Input:** an offline-materialized feature group (queryable in Trino).
- **Output:** a published Superset dashboard (charts + datasets) and its URL.
- **Pre-condition:** Superset enabled on the cluster, and the FG materialized to
  the offline (Trino) store.

## Smoke-test (cheap pre/post-flight)

Confirm auth + Superset reachability before building anything — cheapest from the
CLI, no Python:

```bash
hops superset dataset list      # auth + Superset reachable in one shot
# or in Python: api.list_databases()  -> find the Trino DB id (see §2)
```

The `hops superset {dataset,chart,dashboard} {list,info,create,delete}` CLI is the
quickest path to list/inspect/clean up (delete takes `--yes`). Re-run
`hops superset dashboard list` after building to verify the result.

## Ask the user (only when state is ambiguous)

- Which feature group (and version) to chart.
- Which columns / metrics to visualize, and which chart types (see the viz_type
  enum in §3).

---

Feature groups (FGs) are referenced in superset as either:
delta.<project_name>_featurestore.<featuregroup_name>_<version>
or
hudi.<project_name>_featurestore.<featuregroup_name>_<version>
depending on whether they are a delta offline feature group or a hudi offline feature group.
For example, the delta FG, transactions, in the jim project is referenced as:

SELECT * FROM delta.jim_featurestore.transactions_1;

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

**Pre-condition / smoke-test.** Superset must be enabled on the cluster and the FG
you chart must be materialized to the offline (Trino) store. See the **Smoke-test**
section above to confirm reachability (CLI: `hops superset dataset list`; Python:
`api.list_databases()` → find the Trino DB id, see §2).

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
- Trino database id varies per Hopsworks install — never hardcode it. Always
  resolve it with `api.list_databases()` / `find_trino_db_id(api)` below.

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

The full registered `viz_type` enum (the keys the server accepts) and the legacy keys that fail are tabulated in [references/viz_types.md](references/viz_types.md).

---

## 4. Creating charts — param schemas

`create_chart` takes a JSON-string `params` blob whose shape is **viz-type specific**. The reusable metric/filter building blocks (`COUNT_METRIC`, `SUM_AMOUNT`, `adhoc_filters`), a copy-paste `params` block for each supported viz type, and a replace-by-name idempotency helper are in [references/chart_params.md](references/chart_params.md).

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

# Columns below (state, age, …) are illustrative — swap for real ones from
# `hops fg features <FG_NAME> --version <FG_VERSION>`.
PROJECT_NAME = "<your_project>"        # don't hardcode; set to project.name after login
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

---

## Next Steps

- Get a feature group materialized offline to chart: **hops-fg**.
- Inspect / query the underlying Trino table: **hops-trino-sql**, **hops-data-discovery**.
- A custom interactive app instead of BI dashboards: **hops-app**.

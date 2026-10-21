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
- **Before deleting** — `api.delete_chart/dataset/dashboard(id)` / `hops superset ... delete --yes` is irreversible; confirm with the user which object to remove, and never delete one you created as a side effect (temp or test ones included) unless they asked.

---

Hopsworks exposes Apache Superset as a managed service. This skill covers the
Python SDK wrapper for the Superset REST API (`project.get_superset_api()`),
how to surface Hopsworks feature groups as Superset datasets via Trino, and
the modern viz_type keys + param schemas this Superset version expects. It
also builds monitoring dashboards over a model's **logging feature group**
(the logs an inference pipeline writes are an offline FG, so they chart the
same way) and debugs chart errors like `Item with key "X" is not registered`
or `Empty query?`.

The Python client lives in the Hopsworks venv:
`hopsworks_common/core/superset_api.py` (inside
`/srv/hops/venv/lib/python3.13/site-packages/` on a Hopsworks host).

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

`_request(...)` is **internal** (underscore-prefixed, not `@public`): it carries no cross-release stability guarantee. Prefer the `@public` methods above (`list_*`, `create_*`, `get_*`, `update_*`, `delete_*`) as the supported surface. Reach for `api._request("GET", "/api/v1/...")` only for what the public API does not cover yet — paging, or endpoints the SDK doesn't wrap — knowing it may break on upgrade.

### Paging helper

`list_*()` returns only the first page (~25 rows). There is no public paged-list method yet, so paginate via the internal `_request` (escape hatch — see the note above):

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
<catalog>.<project>_featurestore.<feature_group>_<version>   # catalog = delta | hudi | iceberg
```

Example (project `af`, feature group `customers` v1):

```sql
SELECT * FROM delta.af_featurestore.customers_1
```

Rules:
- Catalog is the FG's table format: `delta` (the default), `hudi`, or `iceberg` (`hops fg info` shows it).
- Schema is `<project>_featurestore` (project name is lowercase).
- Table name is `<fg_name>_<version>` — the version suffix is required.
- The Trino DB connection in Superset defaults its catalog to `hive`, so you
  **must** fully qualify with the catalog or the query resolves to the wrong one.
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

Layout primer, the look, a layout builder and an idempotent create or update: `hops skills info hops-superset references/layout.md`.

---

## 6. End-to-end pattern

A whole pattern, from a feature group to a dataset, charts and a dashboard: `hops skills info hops-superset references/end-to-end.md`.

---

## 7. Dashboard programs

Every dashboard `/hops dashboard` builds has one program that builds it, filled
in from [references/dashboard_program.py](references/dashboard_program.py): a
module docstring naming the title, the feature groups and the charts; `TITLE`,
`FEATURE_GROUPS` and `CHARTS`; the idempotent steps above; `--delete`; and a
last line printing the dashboard URL.

- **Where they live:** `Users/<user>/dashboards/<slug>.py` in the project, which
  is `~/dashboards/<slug>.py` in a Hopsworks terminal (the FUSE mount) and
  `./dashboards/<slug>.py` from an external client, mirrored with
  `hops files upload` and `hops files download` so both places hold the same programs.
- **Where they run:** where you run, a Hopsworks terminal or an external client:
  `get_superset_api()` and `hops superset` reach Superset through the Hopsworks
  proxy. Go through the SDK or the CLI, never raw requests: Superset's writes need
  the headers the SDK sets.
- **Each dashboard has its own datasets**, named `<SLUG>__<feature group>_<version>`,
  so two dashboards over one feature group never share or delete each other's.
- **Every chart is checked.** After a build the program runs each chart's query once
  (`/api/v1/chart/data`) and exits non-zero naming the charts that error or return no rows.
- **A re-run is an update.** The dashboard's datasets are reused by name, the charts the
  dashboard had are replaced (so a chart dropped from `CHARTS` goes too), and the
  dashboard keeps its id, URL and title.
- **`--delete` removes only what the program made:** the dashboard, its
  charts, and its datasets that no other chart still uses. A chart of the same
  name on another dashboard is left alone.
- An ML system's dashboard (the app phase of `/hops-build`) keeps its program in
  the system's repository, under `<slug>/dashboards/`, instead of the user's home.

```bash
python ~/dashboards/customers-overview.py            # create or update, prints the URL
python ~/dashboards/customers-overview.py --delete   # after confirming the exact title
hops superset dashboard list                         # confirm
```

A chart Superset rejects (a legacy viz type, `Empty query?`, an unknown field)
is fixed in the program and the program re-run, at most three times per chart;
a chart that still fails is dropped from `CHARTS` with a line saying why.

## 8. Debugging cheatsheet

Symptoms and fixes: `hops skills info hops-superset references/debugging.md`.

## References

Read the one a step needs, with `hops skills info hops-superset references/<file>`:

- [references/dashboard_program.py](references/dashboard_program.py): the skeleton every dashboard program starts from.
- [references/chart_params.md](references/chart_params.md): the `params` of each viz type, and the metric and filter blocks.
- [references/viz_types.md](references/viz_types.md): the `viz_type` keys the server accepts.
- [references/layout.md](references/layout.md): `position_json`, the look, a layout builder, create or update in place.
- [references/end-to-end.md](references/end-to-end.md): a feature group to a dataset, charts and a dashboard.
- [references/debugging.md](references/debugging.md): symptoms and fixes.

---

## Next Steps

- Get a feature group materialized offline to chart: **hops-fg**.
- Inspect / query the underlying Trino table: **hops-trino-sql**, **hops-data-discovery**.
- A custom interactive app instead of BI dashboards: **hops-app**.

### Monitoring dashboards

Inference pipelines log untransformed features, transformed features, and
predictions to a per-model logging feature group. That FG is offline-queryable
in Trino like any other, so the same dataset/chart/dashboard machinery here
builds feature- and model-monitoring dashboards: chart a feature's distribution
over a recent detection window against its training-time reference, or track a
KPI over time to spot degradation. Build the drift jobs themselves (univariate /
multivariate, deviation-from-mean, NannyML) in the inference pipeline; use this
skill to surface their outputs.

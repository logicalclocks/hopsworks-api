# Metrics from the semantic layer in Superset

How a chart reads a metric that a semantic project ([hops-semantic](../../../data/hops-semantic/SKILL.md)) defines: through the project's semantic view on Superset 7.0, or through a dataset over the metric's SQL on Superset 6.
The parent skill's "Metrics from the semantic layer" section says when; this file is the detail.

```bash
hops semantic metrics sales                       # the metrics, and the tag schemas each sets
hops semantic dimensions sales --metrics revenue  # what they can be grouped by
```

## Which Superset

Superset 7.0 lists the Hopsworks semantic layer type in `/api/v1/semantic_layer/types`.
Only an explicit answer that semantic views are unsupported selects Superset 6's datasets over metric SQL:

- that endpoint answering 404, or 200 without `extensions.hopsworks.semantic-layer.hopsworks` in its types;
- from an external client, which cannot reach Superset, `hops semantic dashboard` (or `create_dashboard`) failing with code 590014, `SEMANTIC_LAYER_DASHBOARDS_DISABLED` (HTTP 503).

An authentication failure, any other 4xx or 5xx, and 590015, `SEMANTIC_LAYER_SUPERSET_ERROR` (Superset did not answer), are errors to report: retry or fix them, and never fall back on them.

```python
import requests

def semantic_views_available(api) -> bool:
    try:
        types = api._request("GET", "/api/v1/semantic_layer/types").get("result", [])
    except requests.HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            return False  # Superset 6 has no semantic layer API
        raise
    return any(t.get("id") == "extensions.hopsworks.semantic-layer.hopsworks" for t in types)
```

## Superset 7.0: semantic views

Hopsworks keeps one semantic view per semantic project, named after it, which every project member with a Superset account can read.
Each saved query gets a view too, `<semantic project>.<saved query>`, which offers only its metrics and group-bys and runs every chart as that saved query.

### A dashboard of metrics

Let Hopsworks build it.
It needs no Superset client, so it also works from an external client:

```bash
hops semantic dashboard sales --metrics revenue,orders --group-by customer__country --title "Sales overview"
# Created dashboard 12 "Sales overview": https://<host>/hopsworks-api/superset/dashboard/12/?projectId=119
```

```python
sales = project.get_semantic_layer_api().get_project("sales")
sales.create_dashboard(["revenue", "orders"], group_by=["customer__country"], title="Sales overview")
# {"id": 12, "title": "Sales overview", "url": "https://<host>/hopsworks-api/superset/dashboard/12/?projectId=119"}
sales.create_dashboard(saved_query="revenue_by_country")   # on the saved query's view; also hops semantic dashboard sales --saved-query revenue_by_country
sales.explore_url("revenue")   # Explore, the metric over metric_time, nothing saved
```

- It holds a line chart per metric over `metric_time`, a big number for the first metric, a table broken down by the group-by (by default the first categorical dimension the metrics share) and native filters.
- The caller owns it, and a rerun with the same title updates it in place, keeping its id and URL.
- A 503 with 590014 means semantic dashboards are not available: Superset 6, or Superset, its semantic dashboards or the project's Superset service off.

### Other charts

They go in a dashboard program (SKILL.md §7), whose chart names `"semantic_view": "<semantic project>"` instead of a `dataset`.
A chart over a view:

- is created with `datasource_type="semantic_view"` and the view's integer id, from `GET /api/v1/datasource/` (entries with `kind: semantic_view`, `table_name` the semantic project);
- names metrics and dimensions as the view lists them: `"metrics": ["revenue"]`, `"groupby": ["customer__country"]`, and time as `"x_axis": "metric_time"` with `"time_grain_sqla": "P1M"`;
- filters on dimensions only: SQL `WHERE`/`HAVING` extras are refused with 400, and a filter on a metric is dropped;
- in a `query_context` stored with the chart, gives the BASE_AXIS column `"isColumnReference": true`, or its data request fails with 500 `Adhoc dimensions are not supported in Semantic Views.`

## Superset 6: datasets over metric SQL

A chart reads a virtual dataset over the SQL of a metric query:

```bash
hops semantic sql sales --metrics revenue,orders --group-by metric_time__month,customer__country
```

In a dashboard program (SKILL.md §7), list the query in `METRIC_DATASETS`; the program compiles its SQL with `SemanticProject.sql` on every run, writes it into the dataset (creating it the first time, refreshing its columns after), and a chart names the entry as its `dataset`.
The dataset in Superset is named after the dashboard and the entry (`<title slug>_<title digest>__<name>`), so two programs never write the same one:

```python
METRIC_DATASETS = [
    {
        "name": "sales_revenue_by_month",
        "semantic_project": "sales",
        "metrics": ["revenue", "orders"],
        "group_by": ["metric_time__month", "customer__country"],
    }
]
```

- One dataset per metric query: its columns are the group-bys and the metrics, one row per group.
  Group in the query by what the charts group by.
  A chart then reads a metric column without aggregating it again (`MAX(revenue)` at the dataset's grain); only an additive metric, a simple `sum` or `count`, may be summed when a chart groups coarser.
  A ratio or a distinct count summed again is wrong.
- Leave out `--limit` and `--order-by`; Superset applies its own.
- The SQL is fixed when the dataset is written.
  A dashboard program writes it again on every run, so rerun the program after the definitions change.
- The SQL names its tables in full (`"delta"."<project>_featurestore"."<fg>_<version>"`), so the Trino connection's default catalog does not matter.

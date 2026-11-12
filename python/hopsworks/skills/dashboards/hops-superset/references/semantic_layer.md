# Metrics from the semantic layer in Superset

How a chart reads a metric that a semantic project ([hops-semantic](../../../data/hops-semantic/SKILL.md)) defines: through the caller's own Superset database connection to the semantic project, or, where Superset has none, through a dataset over the metric's SQL on the Trino connection.
The parent skill's "Metrics from the semantic layer" section says when; this file is the detail.

```bash
hops semantic metrics sales                       # the metrics, and the tag schemas each sets
hops semantic dimensions sales --metrics revenue  # what they can be grouped by
```

## Which path

Each Data owner and Data scientist of a project with Superset has one connection per semantic project, `Semantic_<semantic project>__<superset user>`, where the Superset user is `<project>__<username>_superset`.
A query on it runs on Trino as the member's own Trino user, so it reads only what the member can read.
The connection shows the semantic project as tables over Arrow Flight SQL, in the schema named by the semantic project's id: `metrics`, and one table per saved query.

Look for the connection; only its absence selects datasets over metric SQL:

- no database of that name among those the caller sees, in a Hopsworks terminal or job;
- from an external client, which cannot reach Superset, `hops semantic dashboard` (or `create_dashboard`) failing with code 600014, `SEMANTIC_LAYER_DASHBOARDS_DISABLED` (HTTP 503): Superset without the Hopsworks image's driver, `superset_semantic_enabled` off, or the project without the Superset service.

An authentication failure, any other 4xx or 5xx, and 600015, `SEMANTIC_LAYER_SUPERSET_ERROR` (Superset did not answer), are errors to report: retry or fix them, and never fall back on them.

```python
def semantic_connection_id(api, semantic_project: str) -> int | None:
    """The caller's own Superset connection to a semantic project; None when Superset has none."""
    me = api._request("GET", "/api/v1/me/")["result"]["username"]  # <project>__<username>_superset
    name = f"Semantic_{semantic_project}__{me}"
    return next((db["id"] for db in list_all(api, "database") if db.get("database_name") == name), None)
```

`list_all` is the paging helper of the dashboard program (SKILL.md §7).

## Your connection: datasets of the semantic project's tables

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
sales.create_dashboard(saved_query="revenue_by_country")   # on the saved query's table; also hops semantic dashboard sales --saved-query revenue_by_country
sales.explore_url("revenue")   # Explore, the metric over metric_time, nothing saved
```

- It reads a dataset of the `metrics` table (or the saved query's table) on the caller's connection, which it creates the first time and reuses.
- It holds a line chart per metric over `metric_time`, a big number for the first metric, a table broken down by the group-by (by default the first categorical dimension the metrics share) and native filters.
- The caller owns it, and a rerun with the same title updates it in place, keeping its id and URL.
- A 503 with 600014 means semantic dashboards are not available, as above.

### Other charts

They go in a dashboard program (SKILL.md §7): list the table in `SEMANTIC_DATASETS`, and a chart names the entry as its `dataset`.

```python
SEMANTIC_DATASETS = [{"name": "sales_metrics", "semantic_project": "sales", "metric": "revenue"}]
```

The program gets the dataset of the `metrics` table from Hopsworks, which creates it on the caller's connection, with each metric as a saved metric of the dataset and the time columns marked temporal, when it makes an Explore link of the entry's `metric`.
A saved query's table gets its dataset with the saved query's dashboard, `create_dashboard(saved_query=...)`; a program charts the same metrics and group-bys on `metrics`.
A chart on it:

- reads a metric as the saved metric of its name, `"metrics": ["revenue"]`, or as the SQL expression `SUM("revenue")`: an aggregate around a metric column returns the metric itself, computed by its definition at the chart's group-bys;
- groups by the semantic project's dimensions and entities, named as in queries: `"groupby": ["customer__country"]`;
- puts time on a time column, `"x_axis": "metric_time__day"`, at the grain of `"time_grain_sqla"`;
- filters on dimensions, and on time with a time range.

Superset sends each chart as one SQL statement over the table, which the semantic layer runs as one metric query at the chart's group-bys and grain, so a ratio or a distinct count is right at every grain.
A statement selects each column once: `SUM(revenue)` and `AVG(revenue)` in one chart are refused.

## No connection: datasets over metric SQL on Trino

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

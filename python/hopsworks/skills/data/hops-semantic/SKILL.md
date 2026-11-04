---
name: hops-semantic
description: Use whenever metrics, a semantic layer, MetricFlow, dbt semantic models or dbt metrics are involved on Hopsworks. Author semantic YAML (semantic_models, measures, dimensions, entities, metrics) next to a dbt project, map semantic models to feature groups, compile and validate with `hops semantic compile`, list metrics and dimensions, and get SQL or rows for a metric query. Auto-invoke when the user wants to define a metric once and query it by dimension and time grain, or asks for `hops semantic` or `project.get_semantic_layer_api()`. Input a dbt project over feature groups; output a compiled semantic project and metric query results.
---

# Semantic Layer on Hopsworks

Define metrics once in YAML over feature groups, and query them by dimension and time grain without writing joins or aggregations.
Hopsworks compiles the YAML (dbt or MetricFlow format) into a build, and a gateway turns each metric query into Trino SQL that runs as the caller.

## Contract
- **Input:** a directory in the project, usually a dbt project in a Hopsworks git repo, with `semantic_models:` and `metrics:` YAML whose models are feature groups.
- **Output:** a semantic project with a valid build; metric SQL (`hops semantic sql`) and rows (`hops semantic query`, `SemanticProject.query`).
- **Pre-condition:** the feature groups exist offline with data; an API key used for queries has the `FEATURESTORE` and `TRINO` scopes.

## Smoke-test (cheap pre/post-flight)
```bash
hops semantic list                                   # semantic projects, current and newest build
hops semantic compile <name> --wait                  # status plus every error and warning, with file:line
hops semantic metrics <name>                         # what the current build serves
hops semantic sql <name> --metrics <m> --group-by metric_time__month   # SQL only, nothing runs
```

## Where the YAML goes

Put the semantic YAML inside the dbt project, in any `.yml` file under its `model-paths` (for example `models/semantic/orders.yml`), and register the directory that holds `dbt_project.yml`.
With `--format AUTO` (the default) a root `dbt_project.yml` selects the dbt format, a root `semantic_manifest.json` selects `MANIFEST_JSON`, and anything else is read as MetricFlow's native YAML.
A Factory analytics layer keeps its dbt project in `<layer dir>/dbt`; see [hops-dbt](../hops-dbt/SKILL.md) for the dbt side.

## Semantic models are feature groups

`model: ref('x')` resolves to a feature group, in this order:
1. `config.meta.hopsworks.feature_group` and `version` on the semantic model;
2. the feature group named `x`, at its **latest** version.

Anything else is a compile error at that file and line.
Use the `meta` override whenever the dbt model and the feature group have different names, or to pin a version so a new FG version does not silently change the metric.
In MetricFlow's native format, `node_relation` names the FG table as `<project>_featurestore.<fg>_<version>`.
Every `expr` is checked against the feature group's columns at compile time.
Two warnings point at likely modelling mistakes: a primary entity that is not the FG primary key, and an `agg_time_dimension` that is not the FG `event_time`.

```yaml
semantic_models:
  - name: orders
    model: ref('orders')                  # feature group `orders`, latest version
    defaults:
      agg_time_dimension: order_ts        # the FG event_time
    entities:
      - name: order
        type: primary
        expr: order_id                    # the FG primary key
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: order_ts
        type: time
        type_params:
          time_granularity: day
      - name: status
        type: categorical
    measures:
      - name: revenue
        agg: sum
        expr: amount
      - name: order_count
        agg: count
        expr: order_id
      - name: customers_with_orders
        agg: count_distinct
        expr: customer_id
        create_metric: true               # also a simple metric of the same name

  - name: customers
    model: ref('customers')
    config:
      meta:
        hopsworks:
          feature_group: customer_profile # the FG has another name than the dbt model
          version: 2
    entities:
      - name: customer
        type: primary
        expr: customer_id
    dimensions:
      - name: country
        type: categorical

metrics:
  - name: revenue
    label: Revenue
    type: simple
    type_params:
      measure: revenue
  - name: order_count
    type: simple
    type_params:
      measure: order_count
  - name: paid_revenue
    type: simple
    type_params:
      measure: revenue
    filter: "{{ Dimension('order__status') }} = 'paid'"
  - name: average_order_value
    type: ratio
    type_params:
      numerator: revenue
      denominator: order_count
  - name: revenue_per_customer
    type: derived
    type_params:
      expr: revenue / customers
      metrics:
        - name: revenue
        - name: customers_with_orders
          alias: customers
```

Rules that keep a compile valid:
- **Names** are unique, and lower-cased on compile; a group-by is addressed as `<entity>__<dimension>` (`order__status`, `customer__country`), and every metric's time axis as `metric_time`.
- **Joins** follow entities, at most two hops, and never fan out: `orders` reaches `customer__country` through the `customer` entity because `customers` has it as its primary entity.
- **Time dimensions** need `type_params.time_granularity`; every measure needs an `agg_time_dimension`, from `defaults` or its own.
- **Supported now:** simple, ratio and derived metrics, metric `filter:`, `create_metric: true`, and the aggregations `sum`, `count`, `count_distinct`, `min`, `max`, `average`, `sum_boolean`, `median` and `percentile` (approximate only on Trino; an exact percentile is a compile error).
- **Not yet:** cumulative and conversion metrics, `offset_window`/`offset_to_grain`, time spines, `fill_nulls_with`, `join_to_timespine`, SCD dimensions, semi-additive measures, saved queries, and `Metric(...)` inside a filter. They compile to an error that says so.

## Compile and deploy

```bash
hops semantic create sales --path Jupyter/analytics/dbt        # register; the first compile starts in the background
hops semantic compile sales --wait                             # see errors and warnings now
hops semantic compile sales --fail-on-error                    # CI: exit 1 on an INVALID or FAILED build
hops semantic builds sales                                     # * marks the build answering queries
```

- **When builds happen.** A clone, pull, checkout or reset through Hopsworks git (UI or REST) recompiles automatically.
  Edits made in Jupyter, a terminal or the file browser are noticed by a staleness check that later queries run, at most every 30 seconds, and compile in the background; run `hops semantic compile` to see the result at once.
- **Invalid builds never serve.** Queries keep the last valid build and log that the answer is stale. Fix the issues listed by `compile --wait` and compile again.
- **Deployment modes.** `FOLLOW` (default) serves every new valid build, for a developer's own clone. `PINNED` (`--mode PINNED`) serves one build until `hops semantic promote <name> <build id>`, for a production project.
- `--path` is relative to the project root; `/hopsfs/...` paths from a terminal and absolute `/Projects/<project>/...` paths also work.

## Discover

```bash
hops semantic metrics sales
hops semantic dimensions sales --metrics revenue,average_order_value   # only what every listed metric supports
```

```python
sales = project.get_semantic_layer_api().get_project("sales")
[m.name for m in sales.list_metrics()]
[g.name for g in sales.group_bys(metrics=["revenue"])]      # dimensions, entities and metric_time
sales.compatible_metrics(["customer__country"])            # metrics that can be grouped by it
sales.dimension_values("customer__country", limit=100)
```

## Query

Group-bys take a grain suffix (`metric_time__month`, `order__order_ts__week`) or a date part (`metric_time__extract_dow`).
`--where` takes MetricFlow's filter syntax, `{{ Dimension('customer__country') }} = 'SE'`, `{{ TimeDimension('metric_time', 'month') }} >= '2026-01-01'` or `{{ Entity('customer') }} = 42`, and repeats as `AND`.
A leading `-` in `--order-by` sorts descending; NULLs sort last.
`--start-time`/`--end-time` and `time_range` keep `metric_time` from start through end, both inclusive, each truncated to the query's time grain: a monthly query ending `2026-02-01` includes all of February.

```bash
hops semantic sql sales --metrics revenue --group-by metric_time__month,customer__country --order-by -revenue --limit 10
hops semantic query sales --metrics revenue,average_order_value --group-by metric_time__month \
  --where "{{ Dimension('order__status') }} = 'paid'" --start-time 2026-01-01 --end-time 2026-06-30 -o csv
```

```python
df = sales.query(
    metrics=["revenue", "average_order_value"],
    group_by=["metric_time__month", "customer__country"],
    filters=[("customer__country", "IN", ["SE", "NO"])],     # bound as literals, never parsed as SQL
    having=[("revenue", ">", 1000)],
    order_by=["-revenue"],
    time_range=("2026-01-01", "2026-06-30"),                 # both ends inclusive
    group_limit={"group_by": ["customer__country"], "top": 5, "metric": "revenue"},
    dataframe_type="pandas",                                 # or polars, pyarrow, python
)
sql = sales.sql(metrics=["revenue"], group_by=["metric_time__month"])
```

- Queries run on Trino as the caller's own `<project>__<user>` principal, so they see exactly what that member can read.
- The gateway caps a result at 1,000,000 rows by default; add a `limit` or narrow the query.
- It works from outside the cluster with an API key, through the same host as `hopsworks.login()`.

## Errors

| Error | Meaning | Fix |
|---|---|---|
| `SEMANTIC_LAYER_NO_VALID_BUILD` (409) | No build has compiled valid yet | `hops semantic compile <name> --wait`, fix the issues |
| `SEMANTIC_LAYER_INVALID_QUERY` (400) | Unknown metric, group-by or filter, or a group-by the metrics cannot reach | `hops semantic dimensions <name> --metrics ...` |
| `SEMANTIC_LAYER_EXECUTION_FAILED` (502) | Trino rejected the generated SQL; its message follows | Check the FG data and column types; `hops semantic sql` shows the SQL |
| `SEMANTIC_LAYER_FORBIDDEN` (403) | Not a project member, or the API key lacks `FEATURESTORE` or `TRINO` | Add the scope or the member |
| `SemanticBuildException` | `compile(wait=True)` ended `INVALID` or `FAILED` | Read `err.issues` (file, line, rule, message) |

## Related skills
- [hops-dbt](../hops-dbt/SKILL.md): the dbt project the semantic YAML lives in, and the models that write the feature groups.
- [hops-trino-sql](../hops-trino-sql/SKILL.md): table naming and ad-hoc SQL over the same feature groups.
- [hops-fg](../../ml/hops-fg/SKILL.md): primary keys and event time, which the semantic models' entities and time dimensions should match.

---
name: hops-semantic
description: Use whenever metrics, a semantic layer, MetricFlow, dbt semantic models or dbt metrics are involved on Hopsworks. Scaffold a semantic project from feature groups with `hops semantic init` (in HopsFS or a new private GitHub repository), author semantic YAML (semantic_models, measures, dimensions, entities, metrics) next to a dbt project, map semantic models to feature groups, compile and validate with `hops semantic compile`, test every metric on Trino with `hops semantic test`, tag semantic projects and metrics with schematised tags, list metrics, dimensions, entities and dimension values, get SQL or rows for a metric query, and build a Superset dashboard of metrics with `hops semantic dashboard`, from the CLI, the SDK or the MCP agent tools. Auto-invoke when the user wants to define a metric once and query it by dimension and time grain, or asks for `hops semantic` or `project.get_semantic_layer_api()`. Input feature groups or a dbt project over them; output a compiled semantic project and metric query results.
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
hops semantic init <name> --from-fg <fg>             # a new project: one semantic model per feature group
hops semantic compile <name> --wait                  # status plus every error and warning, with file:line
hops semantic metrics <name>                         # what the current build serves
hops semantic sql <name> --metrics <m> --group-by metric_time__month   # SQL only, nothing runs
```

## Where the YAML goes

Put the semantic YAML inside the dbt project, in any `.yml` file under its `model-paths` (for example `models/semantic/orders.yml`), and register the directory that holds `dbt_project.yml`.
With `--format AUTO` (the default) a root `dbt_project.yml` selects the dbt format, a root `semantic_manifest.json` selects `MANIFEST_JSON`, and anything else is read as MetricFlow's native YAML.
Factory analytics layers keep their dbt projects in the layer's directory: a silver layer in `<slug>/dbt`, each gold data mart in `<slug>/marts/<mart>`.
When that dbt project has `semantic_models:`, the layer's build registers it as the semantic project `<slug>_silver` or `<slug>_<mart>` (hyphens become underscores) and tests it; see [hops-dbt](../hops-dbt/SKILL.md) for the dbt side.

## Start a project

`hops semantic init` writes a dbt project with one semantic model per feature group, registers it and compiles it:

```bash
hops semantic init sales --from-fg orders --from-fg customers:2    # Resources/semantic/sales in HopsFS
hops semantic init sales --from-fg orders --repo new               # a new private GitHub repository, cloned through Hopsworks git
hops semantic init sales --repo https://github.com/acme/metrics.git   # an existing repository, cloned and registered as it is
```

- Each feature group becomes a semantic model pinned to its version (`config.meta.hopsworks.feature_group` and `version`): its primary key is the primary entity, a column named like another model's single-column key a foreign entity, its `event_time` the time dimension (day grain) behind `metric_time`, each string feature a categorical dimension, and a row count and a sum per numeric feature its measures.
- One simple metric per measure is written commented out: uncomment the ones to serve, give them labels and descriptions, then `hops semantic compile <name> --wait`.
- A feature group without an `event_time` gets dimensions but no measures, since every measure needs a time dimension; it still serves as a dimension table that other models join to.
- `--repo new` needs `gh auth status` to pass (`github-login` in a Hopsworks terminal). It creates `hops-semantic-<name>`, or the next free name, pushes the scaffold, clones it into `Resources/semantic/<repository>`, and registers the clone, so a pull recompiles and each build records its commit. Hopsworks clones with the GitHub token in your account (`hops git provider list`); when there is none, init registers `gh auth token` after asking. `--yes` answers both questions.
- `--path` moves the scaffold; `--mode PINNED` and `--tag NAME=VALUE` work as on `hops semantic create`.

## Semantic models are feature groups

`model: ref('x')` resolves to a feature group, in this order:
1. `config.meta.hopsworks.feature_group` and `version` on the semantic model;
2. the feature group named `x`, at its **latest** version.

Anything else is a compile error at that file and line.
Use the `meta` override whenever the dbt model and the feature group have different names, or to pin a version so a new FG version does not silently change the metric.
A feature group in another project's feature store, shared with this one, also needs `feature_store: <project>_featurestore` in that `meta`.
In MetricFlow's native format, `node_relation` names the FG table as `<project>_featurestore.<fg>_<version>`.
Only feature groups with an offline table resolve: cached and stream feature groups of the project, or shared with it; external feature groups and other tables do not.
Every `expr` is checked against the feature group's columns at compile time.
Nothing checks that a primary entity is the FG primary key or that `agg_time_dimension` is the FG `event_time`, so match them yourself: joins trust a primary entity to be unique, and `metric_time` is whatever column `agg_time_dimension` names.

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
- **Supported now:** simple, ratio and derived metrics, metric `filter:`, `create_metric: true`, and the aggregations `sum`, `count`, `count_distinct`, `min`, `max`, `average`, `sum_boolean` and `percentile` with `agg_params: {percentile: <0..1>, use_approximate_percentile: true}`; Trino has only approximate continuous percentiles, so an exact or discrete one is a `METRIC` compile error.
- **Median:** `agg: median` compiles to an exact percentile and so fails on Trino; write it as `agg: percentile` with `agg_params: {percentile: 0.5, use_approximate_percentile: true}`.
- **Not yet:** cumulative and conversion metrics, `offset_window`/`offset_to_grain`, time spines, `fill_nulls_with`, `join_to_timespine`, SCD dimensions, semi-additive measures, and `Metric(...)` inside a filter. They compile to an error that says so.
- **Saved queries** (`saved_queries:`) are ignored, with a `PHASE` warning, until a later release.

## Compile and deploy

```bash
hops semantic create sales --path Jupyter/analytics/dbt        # register; the first compile starts in the background
hops semantic create sales --path Jupyter/analytics/dbt --tag ownership='{"owner": "finance"}'   # with the mandatory tags
hops semantic compile sales --wait                             # see errors and warnings now
hops semantic compile sales --fail-on-error                    # CI: exit 1 on an INVALID or FAILED build
hops semantic test sales                                       # CI: compile, then run every metric once on Trino; exit 1 on any failure
hops semantic test sales --no-compile                          # PINNED: test the promoted build; a plain test exits 1 on an unpromoted build
hops semantic builds sales                                     # * marks the build answering queries
hops semantic build sales 13                                   # one build's errors and warnings at file:line
hops semantic show sales                                       # settings, the build queries use, and whether it is stale
hops semantic update sales --path Jupyter/analytics/dbt_v2     # a new root path or format compiles again
```

- **When builds happen.** A clone, pull, checkout or reset through Hopsworks git (UI or REST) recompiles automatically.
  Edits made in Jupyter, a terminal or the file browser are noticed by a staleness check that later queries run, at most every 30 seconds, and compile in the background; run `hops semantic compile` to see the result at once.
- **Invalid builds never serve.** Queries keep the last valid build and log that the answer is stale. Fix the issues listed by `compile --wait` and compile again.
- **Deployment modes.** `FOLLOW` (default) serves every new valid build, for a developer's own clone. `PINNED` (`--mode PINNED`) serves one build until `hops semantic promote <name> <build id>`, for a production project.
- `--path` is relative to the project root; `/hopsfs/...` paths from a terminal and absolute `/Projects/<project>/...` paths also work.

## Discover

```bash
hops search revenue --type metric      # metrics in every semantic project; DETAIL names the semantic project
hops search sales --type semantic_project            # semantic projects, with their root path
hops semantic metrics sales                          # TAGS lists the tag schemas each metric sets
hops semantic metrics sales --tag certified          # only metrics whose definitions set every --tag
hops semantic dimensions sales --metrics revenue,average_order_value   # only what every listed metric supports
hops semantic metrics sales --group-by customer__country               # only metrics that can be grouped by it
hops semantic entities sales
hops semantic dimension-values sales customer__country --limit 20
```

```python
sales = project.get_semantic_layer_api().get_project("sales")
[m.name for m in sales.list_metrics()]
[(m.name, m.tags) for m in sales.list_metrics(tag="certified")]   # tag schema name to value
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
hops semantic query sales --metrics revenue --group-by customer__country --explain   # the same SQL, as mf query --explain
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

## Tags

Semantic projects and metrics carry schematised tags, as feature groups and jobs do; an administrator defines the tag schemas, and Hopsworks Enterprise is needed.

```bash
hops semantic tag set sales ownership '{"owner": "finance", "certified": true}'
hops semantic tag list sales
hops semantic tag get sales ownership
hops semantic tag delete sales ownership
hops search --type metric --tag ownership:owner=finance        # tag filters work on metrics and semantic projects
```

```python
sales.add_tag("ownership", {"owner": "finance"})
sales.get_tags()                                   # {"ownership": {"owner": "finance"}}
sl.create_project("sales", "Resources/semantic/sales", tags={"ownership": {"owner": "finance"}})
```

A metric's tags live in its definition, under `config.meta.hopsworks.tags`, tag schema name to value, so they change with the YAML and its git history:

```yaml
metrics:
  - name: revenue
    type: simple
    type_params: {measure: revenue}
    config:
      meta:
        hopsworks:
          tags:
            ownership: {"owner": "finance", "certified": true}
            domain: "sales"
```

- A compile checks every metric's tags against the tag schemas; an unknown schema (`METRIC_TAG_UNKNOWN_SCHEMA`), a value that does not match it (`METRIC_TAG_INVALID_VALUE`) or a missing mandatory metric tag (`METRIC_TAG_MANDATORY_MISSING`) is an error at the metric's file, so the build is `INVALID` and the last valid build keeps serving.
- Mandatory tags for semantic projects are checked on create: pass them with `--tag` or `tags=`.
- Only metrics take tags in the YAML; semantic models, dimensions and entities do not.

## Dashboards

On Superset 7.0, Hopsworks keeps a semantic view per semantic project in Superset, so charts read the metric definitions themselves.

```bash
hops semantic dashboard sales --metrics revenue,orders --group-by customer__country --title "Sales overview"
# Created dashboard 12 "Sales overview": https://<host>/hopsworks-api/superset/superset/dashboard/12/?projectId=119
```

```python
dashboard = sales.create_dashboard(["revenue", "orders"], group_by=["customer__country"], title="Sales overview")
dashboard["url"]
sales.explore_url("revenue")   # the metric over metric_time in Superset's Explore view, unsaved
```

- The dashboard has a line chart per metric over `metric_time`, a big number for the first metric, a table broken down by the group-by (by default the first categorical dimension the metrics share) and native filters.
- The caller owns it, and a second run with the same title updates it in place, keeping its id and URL.
- It works from an external client too, since Hopsworks builds it.
- A 503 means Superset, its semantic dashboards or the project's Superset service are off; on Superset 6, build the charts as [hops-superset](../../dashboards/hops-superset/SKILL.md)'s "Metrics from the semantic layer" says.

## Agent tools

The Hopsworks MCP server (`hopsworks-mcp`) serves the semantic layer to agents with dbt-mcp's tool names, so an agent written for dbt finds them; they act on the logged-in project.

| Tool | Does |
|---|---|
| `list_semantic_projects` | name, root path, mode, serving build, stale |
| `list_metrics` | CSV of name, type, label, description, tags; `tag` filters |
| `get_dimensions`, `get_entities` | CSV of what the metrics can be grouped by |
| `get_dimension_values` | CSV of one dimension's values |
| `query_metrics` | CSV rows, `limit` 100 by default, cut at 50,000 characters |
| `get_metrics_compiled_sql` | the SQL, for a dashboard dataset or an explanation |
| `get_semantic_project_tags` | the project's tags |
| `validate_semantic_project`, `compile_semantic_project` | compile, and the build's status and issues (tagged `write`) |
| `create_dashboard_from_metrics` | a Superset 7.0 dashboard of the metrics: its id, title and URL (tagged `write`) |

Answer metric questions with these tools rather than SQL over feature groups: the definitions hold the joins, filters and aggregations the metric owners agreed on.

## Errors

| Error | Meaning | Fix |
|---|---|---|
| `SEMANTIC_LAYER_NO_VALID_BUILD` (409) | No build has compiled valid yet | `hops semantic compile <name> --wait`, fix the issues |
| `SEMANTIC_LAYER_INVALID_QUERY` (400) | Unknown metric, group-by or filter, or a group-by the metrics cannot reach | `hops semantic dimensions <name> --metrics ...` |
| `SEMANTIC_LAYER_EXECUTION_FAILED` (502) | Trino rejected the generated SQL; its message follows | Check the FG data and column types; `hops semantic sql` shows the SQL |
| `SEMANTIC_LAYER_FORBIDDEN` (403) | Not a project member, or the API key lacks `FEATURESTORE` or `TRINO` | Add the scope or the member |
| `SemanticBuildException` | `compile(wait=True)` ended `INVALID` or `FAILED` | Read `err.issues` (file, line, rule, message) |
| `METRIC_TAG_*` build issue | A metric's tag names no schema, does not match it, or a mandatory metric tag is missing | Fix `config.meta.hopsworks.tags` on that metric |

## Related skills
- [hops-dbt](../hops-dbt/SKILL.md): the dbt project the semantic YAML lives in, and the models that write the feature groups.
- [hops-superset](../../dashboards/hops-superset/SKILL.md): custom dashboards over the semantic view on Superset 7.0, or over metric SQL from `hops semantic sql` on Superset 6.
- [hops-analytics](../hops-analytics/SKILL.md): the Factory's silver and gold layers, which register their dbt projects as semantic projects.
- [hops-trino-sql](../hops-trino-sql/SKILL.md): table naming and ad-hoc SQL over the same feature groups.
- [hops-fg](../../ml/hops-fg/SKILL.md): primary keys and event time, which the semantic models' entities and time dimensions should match.

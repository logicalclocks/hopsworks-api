---
name: hops-semantic
description: Use whenever metrics, a semantic layer, MetricFlow, dbt semantic models or dbt metrics are involved on Hopsworks. Scaffold a semantic project from feature groups (`hops semantic init`, in HopsFS or a new GitHub repository), write semantic YAML and the time spine, compile, validate and test metrics, tag semantic projects and metrics, list metrics and dimensions, query metrics or saved queries, export saved queries into feature groups, read the query history, connect dbt Semantic Layer clients and BI tools over Arrow Flight SQL, edit the YAML in the UI editor, and build Superset dashboards of metrics, from the CLI, the SDK or the MCP agent tools. Auto-invoke when the user wants to define a metric once and query it by dimension and time grain, or asks for `hops semantic` or `project.get_semantic_layer_api()`. Input feature groups or a dbt project over them; output a compiled semantic project and metric query results.
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

- One simple metric per measure is written commented out: uncomment the ones to serve, give them labels and descriptions, then `hops semantic compile <name> --wait`.
- How feature groups map to entities, dimensions and measures, `--repo new` and the other flags: [references/init.md](references/init.md).

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
The compiler links the FG `event_time` to the model's time dimension on that column, adding one at `day` grain named after the column when none is declared, and a measure with no `agg_time_dimension`, its own or in `defaults`, aggregates over it.
Declare the time dimension for a finer grain, and set `agg_time_dimension` only to aggregate over another time; a FG without `event_time` needs it, else `AGG_TIME_DIMENSION`, and an `EVENT_TIME` warning means the link clashed with another element's name.
Nothing checks that a primary entity is the FG primary key, so match it yourself: joins trust a primary entity to be unique.

```yaml
semantic_models:
  - name: orders
    model: ref('orders')                  # feature group `orders`, latest version
    entities:
      - name: order
        type: primary
        expr: order_id                    # the FG primary key
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: order_ts                    # the FG event_time: the default agg time dimension
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
- **Names** are unique, lower snake case of at least two characters, never a time grain, `metric_time` or an SQL keyword ([references/naming.md](references/naming.md)); a group-by is addressed as `<entity>__<dimension>` (`order__status`, `customer__country`), and every metric's time axis as `metric_time`.
- **Joins** follow entities, at most two hops, and never fan out: `orders` reaches `customer__country` through the `customer` entity because `customers` has it as its primary entity.
- **Time dimensions** need `type_params.time_granularity`; every measure needs an `agg_time_dimension`, its own, from `defaults`, or else the FG `event_time`.
- **Metric types:** simple, ratio, derived, cumulative (`window`, `grain_to_date`) and conversion metrics, derived inputs with `offset_window`/`offset_to_grain`, `fill_nulls_with`, `join_to_timespine`, custom granularities, semi-additive measures, SCD type II dimensions, `Metric(...)` in a filter, and `saved_queries:`. YAML for each: [references/metric-types.md](references/metric-types.md).
- **Time spine:** cumulative metrics, offsets, `join_to_timespine` and custom granularities need the feature group `time_spine_day`, written once with `hops semantic time-spine create --start 2020-01-01 --end 2030-12-31` and declared as a dbt time spine model; without it such a metric fails the compile with `TIME_SPINE`.
- **Aggregations:** `sum`, `count`, `count_distinct`, `min`, `max`, `average`, `sum_boolean` and `percentile` with `agg_params: {percentile: <0..1>, use_approximate_percentile: true}`; Trino has only approximate continuous percentiles, so an exact or discrete one is a `METRIC` compile error.
- **Median:** `agg: median` compiles to an exact percentile and so fails on Trino; write it as `agg: percentile` with `agg_params: {percentile: 0.5, use_approximate_percentile: true}`.

## Compile and deploy

```bash
hops semantic create sales --path Jupyter/analytics/dbt        # register; the first compile starts in the background
hops semantic create sales --path Jupyter/analytics/dbt --tag ownership='{"owner": "finance"}'   # with the mandatory tags
hops semantic compile sales --wait                             # see errors and warnings now
hops semantic compile sales --fail-on-error                    # CI: exit 1 on an INVALID or FAILED build
hops semantic test sales                                       # CI: compile, then run every metric once on Trino (by metric_time if it requires it); exit 1 on any failure
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
- **UI editor.** **Edit definitions** on the semantic project's page edits the YAML in the browser and validates the unsaved buffers as a compile would; Save does not compile. A save is refused (412) when the file changed since it was opened, so an agent editing the same files never silently loses a human's edit, or the other way round. The editor is read only for a git-backed project (edit in git and pull), one compiled from `semantic_manifest.json`, and members who cannot write the root path or are not Data Owner or Data Scientist.

## Discover

```bash
hops search revenue --type metric      # metrics in every semantic project; DETAIL names the semantic project
hops search sales --type semantic_project            # semantic projects, with their root path
hops search revenue --type saved_query               # saved queries; DETAIL names the semantic project
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

A saved query is a named query from the YAML; `--group-by`, `--order-by` and `--limit` replace its own (`--group-by ""` for its totals), and `--where` adds to its filters:

```bash
hops semantic saved-queries sales                                        # NAME, LABEL, METRICS, GROUP-BYS
hops semantic query sales --saved-query revenue_by_country --group-by metric_time__quarter
```

```python
df = sales.query(saved_query="revenue_by_country", limit=10)   # its own metrics; metrics= as well raises ValueError
```

- A metric with `requires_metric_time` (`hops semantic metrics <name> --json`), a cumulative metric with a window or grain-to-date or a derived metric with an offset, must be grouped by `metric_time`.
- Queries run on Trino as the caller's own `<project>__<user>` principal, so they see exactly what that member can read.
- The gateway caps a result at 1,000,000 rows by default; add a `limit` or narrow the query.
- A repeated query may come from the gateway's result cache, which drops a result once a feature group it reads has a new commit; access changes reach it within a minute. `--no-cache` (`read_cache=False`) runs it on Trino.
- It works from outside the cluster with an API key, through the same host as `hopsworks.login()`.

## Exports

A saved query's `exports:` writes its rows into a Delta feature group that a job refreshes on a schedule or on demand, keeping the feature group's id; every group-by but `metric_time` is its primary key and `metric_time` its event time.
The YAML, the lookback window, and the statuses (`TRUNCATED`, `TARGET_EXISTS`, `OWNER_NOT_DATA_OWNER`): [references/exports.md](references/exports.md).

```bash
hops semantic export list sales                                   # last status, run, rows and message of each export
hops semantic export run sales revenue_by_country_daily --wait    # run now; needs the Data Owner role
```

While an export without `lookback` holds the exact rows of a query (same metrics, group-bys, grains, filters and time range, plus at most HAVING, ORDER BY and LIMIT), on unchanged Delta sources and an unchanged export feature group, the gateway reads them from the export's feature group: the REST answer carries `X-Semantic-Served-From: export:<name>`, and the history records `served_from`. Any write to a source, another write to the export, a changed definition or an access rule ends that until the next run. `"useExports": false` in a REST body skips it: [references/exports.md](references/exports.md).

## Query history

Every query run over REST, GraphQL, Arrow Flight or the BI tables is recorded, failed and cancelled ones included; members see their own, Data Owners everyone's.
Kept 30 days (`semantic_query_delete_after_days`); the request, SQL and error go after 7 (`semantic_query_strip_details_after_days`).
Best effort, not an audit log: a record can be lost (full queue, expired credential). Metadata listings and `sql`/`--explain` are not recorded.
Use it to find why a dashboard or BI tool query failed (`--status FAILED`, the `error` column) or which surface and cache outcome a query had.

```bash
hops semantic history sales --status FAILED --limit 20            # newest first: surface, duration, rows, cache, error
hops semantic history sales --user meb10000 --json                # a member's queries; Data Owners only
```

```python
df = sales.query_history(limit=100, status="FAILED")                # pandas, one row per execution attempt
```

## dbt Semantic Layer clients

`dbt-sl-sdk` (GraphQL and ADBC) and the Arrow Flight SQL JDBC driver, with dbt's `{{ semantic_layer.query(...) }}` syntax, reach a semantic project with the Hopsworks host, an API key with the `FEATURESTORE` and `TRINO` scopes, and the semantic project's id as the environment id:

```bash
eval "$(hops semantic connect sales)"   # DBT_SL_GRAPHQL_URL_FORMAT, DBT_SL_ADBC_URL_FORMAT and, with a private CA, SSL_CERT_FILE; URLs on stderr
```

```python
info = sales.connection_info()          # environment_id, graphql_url, adbc_url, jdbc_url, env and ca_path
```

### BI tools (experimental)

On the same Flight SQL port, a statement without `{{ }}` is plain SQL over tables: catalog `semantic_layer`, schema the environment id (`semantic_layer."7"`), the table `metrics` (`metric_time__<grain>`, then every group-by, then every metric) and one table per saved query.
DBeaver, DataGrip and other JDBC/ODBC tools browse them; dbt's Tableau and Power BI connectors need the admin's dbt-compatible layout (Flight and `/api/graphql` on port 443 of one host).

```sql
SELECT customer__country, DATE_TRUNC('month', metric_time__day) AS m, SUM(revenue)
FROM semantic_layer."7".metrics
WHERE customer__country IN ('SE', 'NO')
GROUP BY 1, 2 HAVING SUM(revenue) > 100 ORDER BY 3 DESC LIMIT 50
```

- `SUM`, `AVG`, `MIN`, `MAX`, `COUNT` and `ANY_VALUE` around a metric column all mean the metric itself: `AVG(revenue)` is revenue, not an average of it. Never tell a user it is an average.
- One table per statement; group-bys in WHERE, metrics in HAVING; `DATE_TRUNC` (both argument orders), `EXTRACT` (ISODOW, not DOW) and lossless `CAST`s only.
- Refused with `INVALID_ARGUMENT: the BI tables do not support <construct>; ...`: joins, unions, subqueries, window functions, expressions over columns, `COUNT(*)`, metrics in WHERE, parameter markers.
- Recorded in the history with surface `BI`; never answered from exports.

## Tags

Semantic projects and metrics carry schematised tags (`hops semantic tag set|list|get|delete`, `add_tag`), and a metric's tags live in its YAML under `config.meta.hopsworks.tags`, checked on every compile: [references/tags.md](references/tags.md).

## Dashboards

Hopsworks builds a dashboard of metrics on the caller's own Superset connection to the semantic project, owned by the caller, from an external client too; a rerun with the same title updates it:

```bash
hops semantic dashboard sales --metrics revenue,orders --group-by customer__country --title "Sales overview"   # prints its URL
hops semantic dashboard sales --saved-query revenue_by_country   # over the saved query's table on the connection
```

The SDK's `create_dashboard` and `explore_url`, the charts, the errors that mean no semantic connection, and other charts of metrics: [hops-superset/references/semantic_layer.md](../../dashboards/hops-superset/references/semantic_layer.md).

## Agent tools

The Hopsworks MCP server (`hopsworks-mcp`) serves the semantic layer to agents with dbt-mcp's tool names, so an agent written for dbt finds them: [references/agent-tools.md](references/agent-tools.md).
Answer metric questions with these tools rather than SQL over feature groups: the definitions hold the joins, filters and aggregations the metric owners agreed on.

## Errors

| Error | Meaning | Fix |
|---|---|---|
| `SEMANTIC_LAYER_NO_VALID_BUILD` (409) | No build has compiled valid yet | `hops semantic compile <name> --wait`, fix the issues |
| `SEMANTIC_LAYER_INVALID_QUERY` (400) | Unknown metric, group-by or filter, a group-by the metrics cannot reach, or a cumulative or offset metric without `metric_time` | `hops semantic dimensions <name> --metrics ...` |
| `SEMANTIC_LAYER_NOT_FOUND` (404) | Unknown saved query | `hops semantic saved-queries <name>` |
| `SEMANTIC_LAYER_EXECUTION_FAILED` (502) | Trino rejected the generated SQL; its message follows | Check the FG data and column types; `hops semantic sql` shows the SQL |
| `SEMANTIC_LAYER_FORBIDDEN` (403) | Not a project member, or the API key lacks `FEATURESTORE` or `TRINO` | Add the scope or the member |
| `SemanticBuildException` | `compile(wait=True)` ended `INVALID` or `FAILED` | Read `err.issues` (file, line, rule, message) |
| `TIME_SPINE` build issue | A metric needs a time spine and none is bound | `hops semantic time-spine create`, then a time spine model ([references/metric-types.md](references/metric-types.md)) |
| `METRIC_TAG_*` build issue | A metric's tag names no schema, does not match it, or a mandatory metric tag is missing | Fix `config.meta.hopsworks.tags` on that metric |

## Related skills
- [hops-semantic-views](../hops-semantic-views/SKILL.md): what goes in one semantic project, and keeping it under 100k tokens for agents.
- [hops-dbt](../hops-dbt/SKILL.md): the dbt project the semantic YAML lives in, and the models that write the feature groups.
- [hops-superset](../../dashboards/hops-superset/SKILL.md): charts and dashboards of the metrics, on Superset 7.0 or 6.
- [hops-analytics](../hops-analytics/SKILL.md): the Factory's silver and gold layers, which register their dbt projects as semantic projects.
- [hops-trino-sql](../hops-trino-sql/SKILL.md): table naming and ad-hoc SQL over the same feature groups.
- [hops-fg](../../ml/hops-fg/SKILL.md): primary keys and event time, which the semantic models' entities and time dimensions should match.

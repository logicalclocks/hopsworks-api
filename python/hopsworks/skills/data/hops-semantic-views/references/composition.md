# Composition in a semantic view

A view composes at three levels: semantic models join into a graph, metrics build on metrics, and queries combine metrics, filters and saved queries.
The examples build on the `orders` model and the `revenue` and `order_count` metrics in [hops-semantic](../../hops-semantic/SKILL.md): `customers` gains a `region` entity, and `regions` and `sessions` are new.
Together they compile, and every query below plans.

## Model composition

Semantic models compose through **entities**, the keys they share.
Each model is one feature group; an entity is `primary` or `unique` on the model it identifies, `foreign` where another model refers to it, and `natural` on an SCD type II model's key.

```yaml
semantic_models:
  - name: customers
    model: ref('customers')
    entities:
      - name: customer
        type: primary
        expr: customer_id
      - name: region
        type: foreign
        expr: region_id
    dimensions:
      - name: country
        type: categorical

  - name: regions
    model: ref('regions')
    entities:
      - name: region
        type: primary
        expr: region_id
    dimensions:
      - name: manager
        type: categorical

  - name: sessions
    model: ref('web_sessions')
    defaults:
      agg_time_dimension: session_ts
    entities:
      - name: session
        type: primary
        expr: session_id
      - name: customer
        type: foreign
        expr: customer_id
    dimensions:
      - name: session_ts
        type: time
        type_params:
          time_granularity: day
    measures:
      - name: session_count
        agg: count
        expr: session_id
        create_metric: true
```

- **Joins are automatic.** A query groups `revenue` (in `orders`) by `customer__country` and the planner joins `orders` to `customers` on `customer`; nobody writes the join.
- **Two hops at most.** `customer__region__manager` reaches `regions` from `orders` through `customers`; the name spells the path, entity by entity.
- **Joins never fan out.** A join goes from an entity to the model where that entity is `primary` or `unique` (or `natural` in an SCD model), so each metric row matches at most one row and no sum is counted twice.
  Two SCD models are never joined to each other, nor a natural key onto a natural key.
- **One graph per view.** A semantic project reads only the YAML under its root path: it cannot import another project's models or build on another view.
  To share a dimension table between views, put its semantic model in each; both read the same feature group.
- **Shared dimension models keep views small.** One `customers` model reached through the `customer` entity serves every fact model; a copy of `country` on each fact model adds a dimension per model to the lists.

## Metric composition

| Type | Composes | Inputs |
| --- | --- | --- |
| `simple` | one measure, optionally filtered | a measure |
| `ratio` | numerator over denominator | two metrics, from any models |
| `derived` | an expression over metrics, with offsets | simple, ratio, cumulative and derived metrics, from any models |
| `cumulative` | a running or windowed total | a simple metric or a measure |
| `conversion` | base events followed by conversion events per entity | two simple metrics or measures |

```yaml
metrics:
  - name: orders_per_session              # ratio across models: orders over sessions
    type: ratio
    type_params:
      numerator: order_count
      denominator: session_count
  - name: revenue_per_session             # derived across models
    type: derived
    type_params:
      expr: revenue / sessions
      metrics:
        - name: revenue
        - name: session_count
          alias: sessions
  - name: revenue_per_session_mom         # derived on a derived metric, with an offset
    type: derived
    type_params:
      expr: (this_month - last_month) / last_month
      metrics:
        - name: revenue_per_session
          alias: this_month
        - name: revenue_per_session
          alias: last_month
          offset_window: 1 month
```

- **Each input is computed in its own model and joined on the query's group-bys**, so inputs from different models combine without a join in the YAML.
  A query of `revenue_per_session` by `customer__country` needs `customer__country` to be reachable from both `orders` and `sessions`.
- **Derived metrics nest.** A derived metric's inputs can be derived metrics themselves, and an offset can apply at any level.
- **Accumulate the parts, then divide.** Cumulative and conversion metrics take simple inputs, so a 7-day average order value is two 7-day cumulative metrics (revenue and order count) divided in a derived metric.
- **An offset needs `metric_time` and a time spine.** A metric with an `offset_window` or `offset_to_grain` input, at any depth, is grouped by `metric_time` in every query, and the build needs a time spine (`hops semantic time-spine create`), or it reports a `TIME_SPINE` error.
- **Filters stack.** A filter on a simple metric applies wherever it is an input; a filter on a ratio or derived input applies to that input only.
- **Limits.** An offset with a custom granularity window, a derived metric with an offset grouped by a custom granularity or by a grain that cannot express the offset (2 days in a monthly query), and a `Metric()` filter on an input read at an offset are refused as `UNSUPPORTED`.
- **Time ranges bound cumulative inputs.** When an offset reads a cumulative metric, the query's time range also bounds the periods that metric is computed for, as in MetricFlow, so the first offset's worth of the range is empty; start the range one offset earlier.
- **One definition, many slices.** Prefer `revenue` grouped or filtered by `customer__country` over `revenue_se` and `revenue_no`: the view stays small and the slices stay consistent.

## Query composition

```bash
# metrics from several models in one query; only group-bys every metric reaches
hops semantic dimensions sales --metrics revenue,session_count
hops semantic query sales --metrics revenue,session_count,revenue_per_session --group-by metric_time__month,customer__country

# a filter on another metric's value per entity
hops semantic query sales --metrics revenue --group-by customer__country \
  --where "{{ Metric('session_count', group_by=['customer']) }} >= 10"

# a saved query, refined at query time
hops semantic query sales --saved-query revenue_by_country --group-by metric_time__quarter \
  --where "{{ Dimension('customer__country') }} = 'SE'"
```

- **Several metrics, several models.** Each metric is aggregated in its own model, and the results are combined with a full outer join on the group-bys, so a group with rows for one metric only still appears, with nulls for the others (or `fill_nulls_with`).
- **Metric filters.** `{{ Metric('<metric>', group_by=['<entity>']) }}` filters on a metric's value per entity, in a query's `where` or a metric's `filter`; a metric filter grouped by several entities is refused.
- **Saved queries name a composition** (metrics, group-bys, filters, order and limit) once.
  At query time `--group-by`, `--order-by` and `--limit` replace the saved ones, `--where` is added to the saved filters, and `--group-by ""` asks for totals.
  Agents run one with `query_saved_query`, and Superset shows each as its own table in the semantic project's schema.
- **No query reads another query's results.** A query composes metrics, never the output of another query.

## Reusing results as input

To build on a query's result, materialize it and model it again:

1. `hops semantic sql sales --metrics revenue,session_count --group-by metric_time__day,customer` prints the Trino SQL, without running it.
2. Put that SQL in a dbt model that writes a feature group ([hops-dbt](../../hops-dbt/SKILL.md)), or run the query from Python and insert the DataFrame into a feature group.
3. Add a semantic model over that feature group, in the same view or another one.

The copy does not follow later changes to the metric definitions: regenerate the SQL when they change, and prefer a derived metric whenever it can express the same thing.

## Capabilities at a glance

| Capability | On Hopsworks |
| --- | --- |
| Derived metrics from existing metrics | Yes |
| Derived metrics across logical tables | Yes, across semantic models |
| Derived metrics referencing other derived metrics | Yes |
| Relationships between entities | Yes, through `primary`, `unique`, `foreign` and `natural` entities |
| Automatic joins across semantic models | Yes, across the semantic graph, at most two hops, never fanning out |
| Semantic view built directly on another semantic view | No; semantic models form a graph within one view, and a model can be repeated in several views |
| Query multiple semantic models together | Yes, metrics from several models in one query |
| Reuse semantic query results as input | Yes, through SQL from `hops semantic sql` in a dbt model, or a feature group written from a query |

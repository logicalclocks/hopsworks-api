# Metric types, time spines and saved queries

YAML for the MetricFlow features beyond simple, ratio and derived metrics, in the dbt format of the parent skill.
Each block goes in a `.yml` file under the dbt project's `model-paths`, next to the semantic models it uses.

## Time spine

Cumulative metrics, offsets, `join_to_timespine` and custom granularities join to a time spine: a feature group with one row per day (or another grain).
Write it once per project, then declare it as a dbt time spine model:

```bash
hops semantic time-spine create --start 2020-01-01 --end 2030-12-31                       # time_spine_day, column date_day
hops semantic time-spine create --start 2020-01-01 --end 2030-12-31 --custom fiscal_quarter=Q-JAN
hops semantic time-spine create --start 2020-01-01 --end 2030-12-31 --grain hour          # time_spine_hour, column date_hour
```

```yaml
models:
  - name: time_spine_day                  # binds feature group time_spine_day; config.meta.hopsworks.feature_group names another
    time_spine:
      standard_granularity_column: date_day
      custom_granularities:
        - name: fiscal_quarter            # a column of the spine; group by it as metric_time__fiscal_quarter
    columns:
      - name: date_day
        granularity: day
```

- `--custom NAME=FREQ` takes a pandas period frequency (`Q-JAN`, `Y-JUN`, `W-SAT`); each row holds the start of its period.
  The SDK also takes a callable per column: `sl.create_time_spine("2020-01-01", "2030-12-31", custom_granularities={"fiscal_year": lambda d: f"FY{d.year}"})`.
- Running it again with a wider range extends the spine; the custom granularities must stay the same.
- A metric that needs a spine when none is bound fails the compile with a `TIME_SPINE` error at the metric.
- In MetricFlow's native format the spine is declared under `project_configuration.time_spines` and binds the same feature group.

## Cumulative metrics

```yaml
metrics:
  - name: revenue_mtd
    type: cumulative
    type_params:
      measure: revenue
      cumulative_type_params:
        grain_to_date: month              # month to date
  - name: revenue_7d
    type: cumulative
    type_params:
      measure: revenue
      cumulative_type_params:
        window: 7 days                    # trailing 7 days
        period_agg: last                  # first, last or average, when grouped coarser than the metric's grain
```

A cumulative metric with a `window` or `grain_to_date` has `requires_metric_time`: every query groups it by `metric_time`.
Without either it accumulates over all time.

## Offsets and filled gaps

```yaml
  - name: revenue_growth_mom
    type: derived
    type_params:
      expr: (revenue - revenue_prev_month) / revenue_prev_month
      metrics:
        - name: revenue
        - name: revenue
          alias: revenue_prev_month
          offset_window: 1 month          # or offset_to_grain: month, the value at the start of the month
  - name: orders_filled
    type: simple
    type_params:
      measure:
        name: order_count
        join_to_timespine: true           # a row for every period, including those without orders
        fill_nulls_with: 0
```

A derived metric with an offset input also requires `metric_time` in every query.

## Conversion metrics

```yaml
  - name: visit_to_order_rate
    type: conversion
    type_params:
      conversion_type_params:
        entity: customer                  # the visit and the order must share it
        base_measure: visits
        conversion_measure: order_count
        calculation: conversion_rate      # or conversions, the count
        window: 7 days                    # an order within 7 days of the visit counts
        constant_properties:              # optional: both events must agree on these
          - base_property: product
            conversion_property: product
```

## Semi-additive measures, SCD dimensions and metric filters

```yaml
    measures:
      - name: balance
        agg: sum
        expr: balance
        non_additive_dimension:
          name: snapshot_date             # summed across accounts, never across days
          window_choice: max              # the last snapshot of each period
          window_groupings: [account]
```

- **SCD type II:** a semantic model over a history feature group gives its validity columns `validity_params: {is_start: true}` and `{is_end: true}` on time dimensions, and its key `type: natural`; a query joins the version valid at each row's time.
- **Metric filters:** `"{{ Metric('revenue', group_by=['customer']) }} > 1000"` filters on a metric's value per entity, in a query's `where` or a metric's `filter`.

## Saved queries

```yaml
saved_queries:
  - name: revenue_by_country
    label: Revenue by country
    description: Monthly revenue per country, the finance board's view.
    query_params:
      metrics: [revenue, order_count]
      group_by:
        - TimeDimension('metric_time', 'month')
        - Dimension('customer__country')
      where:
        - "{{ Dimension('customer__country') }} != 'XX'"
      order_by:
        - Metric('revenue').descending(True)
      limit: 100
```

```bash
hops semantic saved-queries sales                                  # NAME, LABEL, METRICS, GROUP-BYS
hops semantic query sales --saved-query revenue_by_country -o csv
hops semantic query sales --saved-query revenue_by_country --group-by metric_time__quarter --where "{{ Dimension('customer__country') }} = 'SE'"
hops semantic sql sales --saved-query revenue_by_country
hops semantic dashboard sales --saved-query revenue_by_country     # Superset 7.0, over the view sales.revenue_by_country
```

```python
[q.name for q in sales.list_saved_queries()]
df = sales.query(saved_query="revenue_by_country", limit=10)
sales.create_dashboard(saved_query="revenue_by_country")
```

- `--group-by`, `--order-by` and `--limit` replace the saved query's own; `--where` adds to its filters.
- `--metrics` does not combine with `--saved-query`, and an unknown saved query is `SEMANTIC_LAYER_NOT_FOUND` (404).

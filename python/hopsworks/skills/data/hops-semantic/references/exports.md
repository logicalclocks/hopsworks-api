# Exports: saved queries written to feature groups

An export writes a saved query's rows into a Delta feature group, refreshed by a job on a schedule or on demand.
Feature views, models and pipelines then read the governed numbers as an ordinary feature group.

## YAML

The export is dbt's `exports:` on the saved query; the Hopsworks settings go in the saved query's `config.meta.hopsworks.exports.<export name>`.

```yaml
saved_queries:
  - name: revenue_by_country
    query_params:
      metrics: [revenue]
      group_by: ["TimeDimension('metric_time', 'day')", "Dimension('customer__country')"]
    exports:
      - name: revenue_by_country_daily
        config:
          export_as: table                      # a view is refused at compile
          alias: revenue_by_country_daily       # the feature group's name; the export name without it
    config:
      meta:
        hopsworks:
          exports:
            revenue_by_country_daily:
              schedule: "0 0 2 * * ?"           # Quartz cron; leave it out to run on demand only
              version: 1                        # the feature group's version, 1 by default
              lookback: 7 days                  # replace only the last 7 days on each run; needs a metric_time group-by
              enabled: true
```

Compile errors: `EXPORT_AS` (not `table`), `EXPORT_TARGET` (an invalid or duplicate feature group name), `EXPORT_SCHEDULE` (not Quartz), `EXPORT_LOOKBACK` (no `metric_time` group-by), `EXPORT_KEYS` (no group-by).

## The feature group

- Primary key: every group-by except `metric_time`; event time: the `metric_time` group-by.
- One feature per group-by and metric, named as in queries: `customer__country`, `metric_time__day`, `revenue`.
- Delta, offline only, created by the export's first run, which records its id; later runs write that feature group and no other.
- Changing the saved query's metrics or group-bys changes the schema, which the existing feature group refuses: raise `version` so the next run creates a new one.

## A run

1. Runs the whole saved query, past the gateway's result cache.
2. Writes nothing and ends `TRUNCATED` when the result reaches the gateway's row cap, since it may be incomplete.
3. Writes nothing and ends `TARGET_EXISTS` when a feature group of that name and version exists that the export did not create.
4. Replaces every row, or with a `lookback` the rows whose `metric_time` is at or after the window start, in one commit that keeps the feature group's id.
   The window starts at the run's logical time (the start of a scheduled run's interval) minus the lookback, truncated to the `metric_time` grain.
   Groups that vanished from the window are removed, rows before it are kept, and an empty result empties the window.
   A `metric_time` at a custom granularity cannot be truncated, so such a run replaces every row and says so.

The job runs as the semantic project's creator, who must be a Data Owner of the project; otherwise the export is `OWNER_NOT_DATA_OWNER` and has no job.
Runs of one export never overlap.

## Answered from the export

The gateway reads a query's rows from the export's feature group, instead of computing them, only while all hold:
- the export has no `lookback`, and its saved query has no `limit`, `order_by`, group limit or filter relative to the current date;
- the query has the saved query's metrics, group-bys and grains, filters and time range (named or spelled out), adding at most HAVING, ORDER BY and LIMIT; an extra filter misses;
- every source, time spine included, is a Delta feature group at the commit the last run read, and the export feature group at the commit it wrote;
- the saved query's definition is the one the run read, no Trino row filter or column mask applies, and no source is shared with only some of its features.

REST answers carry `X-Semantic-Served-From: export:<name>`, `X-Semantic-Export-Feature-Group` and `X-Semantic-Source-Versions`; Flight and GraphQL are served without headers; BI table SQL and `engine="spark"` never are.
The history's `served_from` shows it on every surface. `"useExports": false` in a REST query body computes from the sources.

## Commands

```bash
hops semantic export list sales                                   # schedule, lookback, last status, run, rows and message
hops semantic export run sales revenue_by_country_daily --wait    # run now (Data Owner), then print the outcome
```

```python
[(e.name, e.last_status, e.last_rows) for e in sales.exports()]
execution = sales.run_export("revenue_by_country_daily", wait=True)
```

| `last_status` | Meaning | Fix |
|---|---|---|
| `NEVER_RUN`, `RUNNING`, `SUCCEEDED` | As named | |
| `FAILED` | The query or the write failed; `last_message` has the error | Fix the cause, run again |
| `TRUNCATED` | The result reached the gateway's row cap | Fewer group-bys, a coarser grain, or a filter |
| `TARGET_EXISTS` | Another feature group has the target name and version | Delete it, or change `alias` or `version` |
| `OWNER_NOT_DATA_OWNER` | The semantic project's creator is not a Data Owner | Make them one, or recreate the project as a Data Owner |
| `JOB_ERROR` | Hopsworks could not create or update the export's job | `last_message` |
| `DISABLED` | `enabled: false` | |

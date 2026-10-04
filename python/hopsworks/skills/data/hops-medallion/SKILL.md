---
name: hops-medallion
description: Use when building a medallion layer on Hopsworks (bronze, silver, gold tables), tagging tables with the medallion_table tag, or building a silver layer from bronze feature groups with the Factory's "New Medallion Layer" or `hops medallion silver`. Auto-invoke on "silver layer", "bronze table", "medallion", "cleanse raw data", or `/hops-silver`. Input bronze feature groups and the silver tasks to perform; output materialized silver feature groups refreshed incrementally by a scheduled job.
---

# Medallion layers on Hopsworks

Bronze tables hold raw data exactly as it arrived; silver tables hold it cleansed and conformed (deduplicated, typed, standardized, validated, PII protected); gold tables hold consumption-ready models.
On Hopsworks every layer is a set of offline feature groups, and every silver and gold table is materialized: a feature group written by a job, never a view.
A silver layer is built by the Factory (**New Medallion Layer**) or `hops medallion silver`, which records the request in `<slug>/layer.yaml` and starts Claude Code on `/hops-silver <slug>`.

## Contract

- **Input:** one or more bronze feature groups, the silver tasks to perform, and the engine (dbt on Trino by default, PySpark when a task needs it).
- **Output:** silver feature groups tagged `medallion_table` `{"layer": "silver"}`, written by one scheduled Hopsworks job that processes only the bronze rows that arrived since its last run, and `layer.yaml` describing what runs.
- **Pre-condition:** the project has bronze feature groups with raw data.
  Without them there is nothing to build a silver or gold layer from: say that bronze tables with raw data are needed first (ingest them with a DLTHub data source, **hops-data-sources**, and tick "Tag as a bronze table").

## The medallion_table tag

Hopsworks installs a schematized tag `medallion_table`, archived, so every change of its value is kept in the tag history:

```json
{"type": "object",
 "properties": {
   "layer": {"type": "string", "enum": ["bronze", "silver", "gold"]},
   "lifecycle": {"type": "string", "enum": ["dev", "staging", "prod"]}},
 "required": ["layer"]}
```

```bash
hops fg add-tag crm_customers medallion_table --value '{"layer": "bronze", "lifecycle": "dev"}'
hops fg tags crm_customers
```

```python
fg.add_tag("medallion_table", {"layer": "silver", "lifecycle": "dev"})
```

A table's layer never changes; its lifecycle moves from `dev` to `staging` to `prod` as it is promoted, and the history records when.
A cluster installed before the tag existed gets it from a platform admin: `POST /hopsworks-api/api/tags?name=medallion_table&archive=true` with the schema above as the body.

## Silver tasks

Each task is one step of the silver transformation, chosen in the Factory form or listed in `layer.yaml` `tasks`; [references/silver-tasks.md](references/silver-tasks.md) has the SQL and PySpark for each.

| Task | What it does |
| --- | --- |
| `deduplicate` | One row per business key and event time; retries and full snapshots collapse. |
| `cast_types` | Columns get their real types: timestamps, decimals, booleans, integers. |
| `standardize` | One spelling per value: trimmed and cased strings, country and currency codes, units. |
| `handle_nulls` | Empty strings and sentinels become nulls; required columns are filled or the row is rejected. |
| `validate` | Rules each row must meet (ranges, formats, allowed values); failures go to a rejects table, never silently dropped. |
| `mask_pii` | Personal data is hashed (joinable) or masked (display) before it leaves bronze. |
| `conform_entities` | The same entity from several sources is matched into one table with one key. |
| `surrogate_keys` | A stable surrogate key per entity, independent of source key changes. |
| `referential_checks` | Foreign keys are checked against their entity tables; orphans are flagged. |

Additional tasks the user writes in free text (for example "anonymize the email column of crm_customers") are designed in the build like these, and recorded in `layer.yaml` `extra_tasks` and `decisions`.

## The engine

- **dbt on Trino** is the default, and is used whenever every task can be written in SQL, which all the tasks above can.
  dbt compiles ephemeral models; the runner executes the compiled SQL for the job's window on Trino and writes the rows to the silver feature group through the feature group API, so the table is materialized with its schema, statistics and lineage (**hops-dbt**).
- **PySpark** when a task needs code SQL cannot express well: fuzzy entity matching, a Python library (address parsing, language detection), a model, or bronze volumes Trino cannot process in one window.
  The job reads the bronze feature groups with `fg.read()` (the window applies itself, below) and inserts into the silver feature group (**hops-spark**).

## Incremental processing

The silver job is a Hopsworks job with a cron schedule.
Every scheduled run gets `HOPS_START_TIME` and `HOPS_END_TIME` (ISO-8601 UTC) in its environment: one cron interval, consecutive runs tiling with no gap or overlap, and a re-run of an execution gets the same window.
The job processes only the bronze rows whose arrival column falls in `[HOPS_START_TIME, HOPS_END_TIME)`:

- The arrival column is the bronze feature group's `event_time`, or a load timestamp the ingestion writes; the build records it per source in `layer.yaml` `sources[].arrival_column`.
  A bronze table with neither cannot be processed incrementally: ask the user which column marks arrival, or whether the table is small enough to reprocess in full each run, and record the answer.
- **PySpark:** `fg.read()` on a feature group with an `event_time` defaults its `start_time` and `end_time` to the two variables, so the read is already the window.
- **dbt:** the model filters on the variables, which the runner passes as dbt vars:
  `where {{ arrival }} >= from_iso8601_timestamp('{{ var("start_time") }}') and {{ arrival }} < from_iso8601_timestamp('{{ var("end_time") }}')`.
  Without the variables (the first run) the filter is left out and the whole bronze history is processed.
- The silver feature group's primary key is the business key, and its `event_time` the record's own time, so a Delta merge on the primary key plus `event_time` makes a replayed window idempotent.
- Deduplication within a window keeps the latest row per key and time; a key seen again in a later window adds its new version, which a feature view's point-in-time join reads correctly.

Order: deploy the job, run it once without a window to backfill the whole bronze history, then schedule it (`hops job schedule <job> "<cron>"`) with a start time at the end of that backfill, so the first scheduled window starts where the backfill ended.

## Building a silver layer

`/hops-silver <slug>` builds what `layer.yaml` asks for, phase by phase, recording each in `layer.yaml`:

1. **profile**: for each bronze source, its schema, row count, key candidates and duplicate rate on them, null rates, distinct values of low-cardinality strings, and its arrival column (`hops fg info`, `hops fg preview`, `hops trino query`).
2. **design**: the silver tables (one per entity or business event, named `<entity>` or `fct_<event>`/`dim_<entity>`, lowercase), each with its sources, business key, `event_time`, columns and the tasks applied to it; the engine; the cron.
3. **code**: the dbt project and runner, or the PySpark program, in the layer's directory, with unit tests of the transformations (DuckDB over sample rows for dbt SQL, pandas or local Spark for PySpark) and dbt data tests for `validate`.
4. **backfill**: deploy the job, run it once over the whole bronze history, and check the silver tables: row counts against bronze, no duplicate keys, no nulls in required columns, rejects counted.
5. **schedule**: schedule the job; tag every silver feature group `medallion_table` `{"layer": "silver", "lifecycle": <layer.yaml lifecycle>}`.
6. **verify**: run the job for one window (`hops job run <job> --start-time ... --end-time ... --wait`) and check that only that window's rows were processed.

## Rules

- Silver tables are materialized feature groups, never views or external feature groups over bronze.
- Bronze is never modified: no updates, deletes or masking in place; the silver layer reads it.
- The silver tables' names, keys and columns are recorded in `layer.yaml` before any code is written, and `layer.yaml` always reflects what runs.
- A silver feature group has a description, and each of its features a description of what was done to it.
- Logs never go into the layer's directory; see its `AGENTS.md`.

## Next Steps

- Bronze ingestion: **hops-data-sources**. dbt and its runner: **hops-dbt**. PySpark: **hops-spark**. Job schedules and windows: **hops-job**. Feature groups: **hops-fg**.

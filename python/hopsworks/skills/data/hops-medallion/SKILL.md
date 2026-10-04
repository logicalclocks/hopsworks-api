---
name: hops-medallion
description: Use when building a medallion layer on Hopsworks (bronze, silver, gold tables), tagging tables with the medallion_table tag, building a silver layer from bronze feature groups (`hops medallion silver`, `/hops-silver`), or a gold layer of Kimball data marts from silver tables (`hops medallion gold`, `/hops-gold`), with the Factory's "New Medallion Layer". Auto-invoke on "silver layer", "gold layer", "data mart", "star schema", "bronze table", "medallion", "cleanse raw data". Input bronze (for silver) or silver (for gold) feature groups; output materialized feature groups refreshed incrementally by scheduled jobs.
---

# Medallion layers on Hopsworks

Bronze tables hold raw data exactly as it arrived; silver tables hold it cleansed, conformed and normalized to third normal form (deduplicated, typed, standardized, validated, PII protected); gold tables hold consumption-ready models, denormalized (star schemas, aggregates, wide feature tables) for their consumers.
On Hopsworks every layer is a set of offline feature groups, and every silver and gold table is materialized: a feature group written by a job, never a view.
A silver or gold layer is built by the Factory (**New Medallion Layer**) or `hops medallion silver|gold`, which records the request in `<slug>/system.yaml` and starts Claude Code on `/hops-silver <slug>` or `/hops-gold <slug>`.
Each layer is its own Factory entry, directory and GitHub repository (`hops-<slug>`, recorded as `layer.repo.url`); a gold layer is built as data marts (Gold layers and data marts, below).

## Contract

- **Input:** one or more bronze feature groups, the silver tasks to perform, and the engine (dbt on Trino by default, PySpark when a task needs it).
- **Output:** silver feature groups tagged `medallion_table` `{"layer": "silver"}`, written by one scheduled Hopsworks job that processes only the bronze rows that arrived since its last run, and `system.yaml` describing what runs.
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

## Silver is in third normal form

Every silver layer is normalized to third normal form (3NF); denormalizing for a consumer is the gold layer's job.

- **One table per entity or event**, keyed by its business key (or a surrogate key): customers, products, orders, order lines, payments.
- **First normal form:** every column holds one atomic value; a repeating group or a list in a column (`item_1, item_2`, a JSON array of lines) becomes rows of a child table keyed by the parent's key plus its own.
- **Second normal form:** every non-key column depends on the whole key; in a table with a composite key, a column that depends on part of it (a product's name in an order line) moves to the table that part identifies.
- **Third normal form:** no non-key column depends on another non-key column; a descriptive attribute of a referenced entity (a customer's city in an order, a country's name next to its code) lives only in that entity's table, and the referencing table keeps just the foreign key.
- **Lookups are tables:** a set of codes with attributes (countries, currencies, channels, product types) is its own silver table, referenced by code.
- **No derived or aggregated columns:** totals, counts, rates and flags computed from other rows belong in gold; silver keeps the facts they are computed from.
- A bronze table that mixes entities (an order export with customer and product columns on every line) is split into one silver table per entity, each deduplicated on its own key.

The design records each table's key, its foreign keys (`references: {column: <table>.<key>}`) and the functional dependencies that justified the split, so a reviewer can check the normal form from `system.yaml`.

## Silver tasks

Each task is one step of the silver transformation, chosen in the Factory form or listed in `system.yaml` `tasks`; [references/silver-tasks.md](references/silver-tasks.md) has the SQL and PySpark for each.

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

Additional tasks the user writes in free text (for example "anonymize the email column of crm_customers") are designed in the build like these, and recorded in `system.yaml` `extra_tasks` and `decisions`.

## The engine

- **dbt on Trino** is the default, and is used whenever every task can be written in SQL, which all the tasks above can.
  dbt compiles ephemeral models; the runner executes the compiled SQL for the job's window on Trino and writes the rows to the silver feature group through the feature group API, so the table is materialized with its schema, statistics and lineage (**hops-dbt**).
- **PySpark** when a task needs code SQL cannot express well: fuzzy entity matching, a Python library (address parsing, language detection), a model, or bronze volumes Trino cannot process in one window.
  The job reads the bronze feature groups with `fg.read()` (the window applies itself, below) and inserts into the silver feature group (**hops-spark**).

## Refresh frequencies: one job per cadence

Bronze tables are not all updated at the same rate, so each source has its own `cadence` (hourly, daily or weekly) in `sources`, chosen per table in the Factory form and defaulting to the layer's `schedule.cadence`.
A silver table refreshes as often as its most frequently updated source: a table built only from daily sources is daily, one that joins an hourly source is hourly.
The build groups the silver tables by cadence and deploys one job per cadence, `<slug>-silver-<cadence>`, each writing only its tables from only their sources, with its own cron and catch-up limit from `schedule.cadences` and its own freshness target from `freshness.max_age_hours`.
One program serves every job: the runner takes `--cadence <cadence>` as the job's argument and builds that cadence's tables.
`outputs.tables[].cadence` and `outputs.jobs` (`{name, cadence, cron, tables}`) record the split.
A table that references one of a slower cadence (an hourly fact keyed on a daily entity) can see keys the entity has not loaded yet; `referential_checks` flags them as orphans until the slower job runs, never drops them.

## Incremental processing

Each silver job is a Hopsworks job with a cron schedule.
Every scheduled run gets `HOPS_START_TIME` and `HOPS_END_TIME` (ISO-8601 UTC) in its environment: one cron interval, consecutive runs tiling with no gap or overlap, and a re-run of an execution gets the same window.
A run can also get the window as program arguments, `-start_time <ts> -end_time <ts>`, from the scheduler and when started by hand on a scheduled job: the program reads the two variables, else those arguments, and parses its arguments with `parse_known_args`, so an argument it does not know never fails the run.
The job processes only the bronze rows whose arrival column falls in `[HOPS_START_TIME, HOPS_END_TIME)`:

- The arrival column is the bronze feature group's `event_time`, or a load timestamp the ingestion writes; the build records it per source in `system.yaml` `sources[].arrival_column`.
  A bronze table with neither cannot be processed incrementally: ask the user which column marks arrival, or whether the table is small enough to reprocess in full each run, and record the answer.
- **PySpark:** `fg.read()` on a feature group with an `event_time` defaults its `start_time` and `end_time` to the two variables, so the read is already the window.
- **dbt:** the model filters on the variables, which the runner passes as dbt vars:
  `where {{ arrival }} >= from_iso8601_timestamp('{{ var("start_time") }}') and {{ arrival }} < from_iso8601_timestamp('{{ var("end_time") }}')`.
  Without the variables (the build's first run, before the job is scheduled) the filter is left out and the whole bronze history is processed; after that, `hops medallion backfill` reprocesses it.
- The silver feature group's primary key is the business key; with `history: full` its `event_time` is the record's own time, and the merge is on both; with `history: latest` it has none, and the merge is on the key. Either way a replayed window is idempotent.

Order: deploy the job, run it once without a window to backfill the whole bronze history, then schedule it (`hops job schedule <job> "<cron>"`) with a start time at the end of that backfill, so the first scheduled window starts where the backfill ended.

## The layer's settings

The Factory form asks for these, with these defaults, and `system.yaml` records them; the build implements each in the job.

| Setting | Values | What the job does |
| --- | --- | --- |
| `history` | `latest` (default), `full` | `latest`: one row per business key, the newest; the silver feature group has no `event_time`, so the merge is on the key alone and a newer row replaces the older. `full`: every version, `event_time` the record's time, merged on key plus time, which a feature view's point-in-time join reads. |
| `deletes` | `ignore` (default), `propagate` | `propagate`: a row deleted from bronze, or flagged deleted (a soft-delete column the profile finds), is deleted from silver with `fg.commit_delete_record(df)` for its keys; a source reprocessed in full deletes the keys no longer in it. |
| `schema_changes` | `fail` (default), `evolve` | The job compares each bronze table's schema with the one `system.yaml` recorded at design. `fail`: it stops before writing, naming the change, for an apply to redesign. `evolve`: a new nullable column is added to the silver table it belongs to (`fg.append_features`) and recorded; a removed or retyped column still fails. |
| `late_data.lookback` | `0` (default), `1d`, `7d` | Each run also re-reads that much before its window: the read starts at `HOPS_START_TIME` minus the lookback. The upsert makes the overlap harmless. The schedule's window is never moved with offsets; the program derives the read window. |
| `quality.max_reject_pct` | 0 to 100, default 5 | A run whose rejected share of rows exceeds it fails after writing the rejects table and before writing silver, so bad data never reaches silver silently. |
| `quality.alert_on_failure` | `true` (default) | A Hopsworks alert on the job's failure: `hops alert job create <job> --receiver <receiver> --status failed --severity critical`. |
| `freshness.max_age_hours` | per cadence: 2 h hourly, 26 h daily, 170 h weekly | A silver table not written for longer than its cadence's target is stale; the layer's status report flags it. |

## Lineage, partitioning, schedule and status

- **Lineage.** Every silver feature group is created with `parents=[<bronze feature groups it reads>]`, and every rejects feature group with its silver table's sources, so Hopsworks' lineage shows bronze to silver (`hops fg lineage <silver>`).
- **Partitioning.** Decided per silver table at design with **hops-partitioning**, from the bronze table's files on the terminal's mount (`/hopsfs/featurestore/<project>_featurestore.db/<fg>_<version>`): none for small tables, else by hour, day or week, and recorded in `system.yaml` `partitioning` with the evidence.
- **Schedule.** Each job: `hops job schedule <slug>-silver-<cadence> "<schedule.cadences.<cadence>.cron>" --catchup --max-catchup-runs <its max_catchup_runs>`, so windows missed while the scheduler was down are replayed, one execution each, instead of skipped.
- **Status.** `hops medallion status <layer>` writes `status/report.html` in the layer's directory: the job's runs, and for every silver and rejects table its rows, last write against the freshness target, rejected share against the gate, and file layout (the hops-table-maintenance scanner), with Claude's summary. The layer's page shows it with **Status**.
- **Delete.** `hops medallion delete <layer> --assets` deletes a layer with its jobs, tables and directory, then its Factory entry; without `--assets` only the entry. `hops medallion job-delete <layer> <job> [--tables]` deletes one job, and with `--tables` the tables only it writes; in silver the job's cadence and its sources leave the spec with it. A layer never deletes what it reads: a source, or a table tagged as a lower layer (bronze, and silver from gold), stops the delete before anything is deleted.
- **Adding tables.** `hops medallion add-tables <silver layer> --answers FILE` adds bronze sources (each with its cadence) and a description of the tables wanted, recorded under `additions`, and starts `/hops-silver <slug> apply` to build them.
- **Backfill.** `hops medallion backfill <layer>` runs each job, slowest cadence first, over a window from the epoch to now, reprocessing every bronze row; once the job is scheduled, a plain `hops job run` gets the last cron interval as its window instead. The layer's page has **Backfill**.

## Changing a layer: system.yaml drives recomputation

`system.yaml` is the layer's specification, as an ML system's is.
The spec is `sources`, `tasks`, `extra_tasks`, `engine`, `schedule`, `layer.lifecycle` and the settings above; after every build or apply, `outputs.applied_spec` records the spec the tables were built from.
A spec that differs from `outputs.applied_spec` is a pending change, which the Factory shows on the layer's page with **Apply changes**, and `/hops-silver <slug> apply` applies:

| Change | What it recomputes |
| --- | --- |
| `layer.lifecycle` | nothing: every silver and rejects feature group is retagged, and the tag history records the promotion |
| `schedule` | nothing: the job is rescheduled with the new cron, continuing from the last window |
| a source's `cadence` | the tables it feeds move to that cadence's job, deployed and scheduled when the cadence is new, and a job left with no tables is deleted; no table gets a new version |
| `late_data`, `quality`, `freshness` | nothing: the job's settings and alert are changed and redeployed |
| `history`, `deletes` or `schema_changes` | the code, and for `history` each silver table, which gets a new version (the merge key changes), backfilled as below |
| `tasks`, `extra_tasks` or `engine` | the silver tables whose content changes: the code is changed and tested, each such table gets a new feature group version backfilled over the whole bronze history, and the job is switched to write the new versions |
| a source added | the tables that read it: profiled and designed like a new source, then built as above |
| a source removed | the tables that read only it stop being written; those that also read others are rebuilt as above |

Superseded versions are kept and named in `decisions`, never deleted by an apply, so a consumer can move to the new version when it is ready, and reverting the apply commit returns the job to them.
An apply ends with `outputs.applied_spec` set to the spec it applied, in one commit `[<slug>] apply: <what changed>`.

## Gold layers and data marts

A gold layer serves analysts with a Kimball dimensional model, star or snowflake schema as `layer.modeling` says, built from silver tables.
It is a set of data marts, each the unit that is added (`hops medallion mart-add`), changed (`hops medallion mart-update`) and deleted (`hops medallion mart-delete [--tables]`), with its own requirements, fact and dimension tables, and jobs at its own cadence, `<slug>-<mart>-<cadence>`.
A conformed dimension is built once and marked `shared` in every other mart that reads it; deleting a mart never deletes a table another mart lists.
Gold feature groups are tagged `{"layer": "gold"}` with their silver tables as `parents`.
The requirement questions every mart answers, the modeling rules and the standards are in [references/gold-marts.md](references/gold-marts.md); `/hops-gold <slug> <mart>` builds a mart or applies its changed requirements (`marts[].requirements` against `marts[].applied`).

## Building a silver layer

`/hops-silver <slug>` builds what `system.yaml` asks for, phase by phase, recording each in `system.yaml`:

1. **profile**: for each bronze source, its schema, row count, key candidates and duplicate rate on them, null rates, distinct values of low-cardinality strings, and its arrival column (`hops fg info`, `hops fg preview`, `hops trino query`).
2. **design**: the silver tables in third normal form (one per entity or event, named after it in lowercase, `customers`, `order_lines`), each with its sources, business key, foreign keys, `event_time`, columns and the tasks applied to it; the engine; the cron.
3. **code**: the dbt project and runner, or the PySpark program, in the layer's directory, with unit tests of the transformations (DuckDB over sample rows for dbt SQL, pandas or local Spark for PySpark) and dbt data tests for `validate`.
4. **backfill**: deploy the job, run it once over the whole bronze history, and check the silver tables: row counts against bronze, no duplicate keys, no nulls in required columns, rejects counted.
5. **schedule**: schedule the job; tag every silver feature group `medallion_table` `{"layer": "silver", "lifecycle": <system.yaml lifecycle>}`.
6. **verify**: run the job for one window (`hops job run <job> --start-time ... --end-time ... --wait`) and check that only that window's rows were processed.

## Rules

- Silver tables are materialized feature groups, never views or external feature groups over bronze.
- Every silver and gold feature group is created with `statistics_config=False`: from a Python job, Hopsworks otherwise starts a Spark statistics job (`<fg>_<version>_compute_stats`) after every insert; the layer's checks and status report measure what it needs.
- Silver tables are in third normal form; no star schema, wide table or aggregate in silver.
- A silver table never takes the name of an existing feature group, bronze above all: `get_or_create_feature_group` would return that group and the job would write into it. Prefix the names with the domain or the layer (`shop_orders`) when they would collide, and check `hops fg list` at design.
- Bronze is never modified: no updates, deletes or masking in place; the silver layer reads it.
- The silver tables' names, keys and columns are recorded in `system.yaml` before any code is written, and `system.yaml` always reflects what runs.
- A silver feature group has a description, and each of its features a description of what was done to it.
- Logs never go into the layer's directory; see its `AGENTS.md`.
- Every silver feature group records its bronze parents, and its partitioning is decided from the data, never by default.

## Next Steps

- Bronze ingestion: **hops-data-sources**. dbt and its runner: **hops-dbt**. PySpark: **hops-spark**. Job schedules and windows: **hops-job**. Feature groups: **hops-fg**.

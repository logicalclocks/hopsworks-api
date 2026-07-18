---
name: hops-data-sources
description: Mount or ingest a table from a supported datasource. Mount tables from a datasource as an external feature group or ingest data into a new feature group using DLTHub. Auto-invoke when user works with external data (Snowflake, BigQuery, Redshift, S3, ADLS, GCS, JDBC, SQL, Databricks Unity Catalog, Postgres, MySQL, Oracle, SAP, MongoDB, CRM, REST APIs).
---

Prefer the `hops` CLI for mounting or ingesting external tables from a datasource. Use the `hopsworks` Python SDK if the CLI is unsuccessful.

Mounting and ingesting both build a feature pipeline's input side: an external feature group leaves data in the source (no copy, no copy-time MITs); a DLTHub ingest copies the source into a managed feature group. Mounting is the lower-cost path when the source already holds the data you want, since reuse beats rebuilding a pipeline.

## Contract
- **Input:** a configured connector + a table from it.
- **Output:** an external feature group (mounted in place), or a managed feature group ingested via DLTHub.
- **Pre-condition:** a connector exists. Create one with `hops datasource create <type>` (see below), or in the UI.
- **Pick mount vs ingest:** mounting serves the offline store only. To load the online store or a vector index, ingest (an ETL path) instead.

## Smoke-test: what connectors exist

```bash
hops datasource list                 # configured connectors
hops datasource info <connector>     # detail for one
```

## Infer metadata (platform intelligence)

Use platform intelligence to (1) select the primary key and event_time columns and (2) write column descriptions for the mounted/ingested feature group. It returns, per column, a suggested snake_case feature name, an offline data type, and a one-line description, plus a suggested primary key and event-time column.

```bash
hops datasource infer-metadata <connector-name> <table> [--database <db>]
```

If platform intelligence is not configured on the cluster, the command exits with a clear error. Ask the cluster admin to set `PLATFORM_INTELLIGENCE_LLM_API_KEY` rather than guessing the metadata by hand.

Programmatic equivalent: `data_source.infer_metadata()` on a `DataSource` returned by `data_source.get_tables()`; it raises `hopsworks.client.exceptions.PlatformIntelligenceException` (with `.reason` of `NOT_CONFIGURED` or `INFERENCE_FAILED`) on the same failure modes.

## Preview source schema and data (CRM / Google Sheets / REST)

Non-SQL sources (CRM, Google Sheets, REST) have no catalog to read the schema from, so Hopsworks samples the source with a server-side schema-fetch job and infers it.
`StorageConnector.get_data(data_source)` fetches one resource and starts one job per call.
`StorageConnector.get_data_batch(data_sources)` fetches several resources with ONE job that processes them sequentially in a single container — prefer it whenever you preview more than one resource.

```python
sc = fs.get_storage_connector("my_crm_connector")
tables = sc.get_tables()                  # CRM: the connector's supported resources

by_name = sc.get_data_batch(tables[:3])   # N resources, ONE schema-fetch job
by_name["contacts"].features              # inferred schema of one resource
by_name["contacts"].preview               # sampled rows

data = sc.get_data(tables[0])             # single resource fallback
```

Both calls block until the fetch finishes and raise `hopsworks.client.exceptions.DataSourceException` with the job logs when it fails; `get_data_batch` reports every failed resource in one exception.
Results are cached server-side per resource — pass `use_cached=False` to force a refetch.
For REST connectors there is no `get_tables()`; build each entry yourself with `DataSource(table="issues", rest_endpoint=RestEndpointConfig(relative_url="v1/issues"))` so every endpoint carries its own request config.

## Mount a table as an external feature group

An external feature group leaves data in the source and queries it through the connector, no copy into Hopsworks. It is offline-only: reads serve training and batch inference. Set an `event_time` column so the feature store can read point-in-time correct snapshots and so polling can read a `start_time`/`end_time` range for backfill or incremental runs.

```python
import hopsworks

project = hopsworks.login()
fs = project.get_feature_store()

ds = fs.get_data_source("my_connector")          # the configured connector
table = ds.get_tables(database="my_db")[0]        # pick a table
meta = table.infer_metadata()                     # PK / event_time / descriptions

ext_fg = fs.create_external_feature_group(
    name="customers_external",
    version=1,
    data_source=table,                            # the source table
    primary_key=meta.primary_key,
    event_time=meta.event_time,
    online_enabled=False,
)
ext_fg.save()
```

## Create a connector

Connectors are not UI-only. Create one from the CLI; the subcommand sends the backend type discriminator for you:

```bash
hops datasource create jdbc <name> --url "jdbc:postgresql://host:5432/db" --user U --password P
hops datasource create s3 <name> --bucket my-bucket --access-key AK --secret-key SK --region eu-north-1
hops datasource create snowflake <name> --url https://acct.snowflakecomputing.com --user U --password P --database D --schema S --warehouse W
hops datasource create bigquery <name> --project-id proj --dataset ds --key-path /path/key.json
hops datasource delete <name> --yes
```

**Confirm before deleting.** `hops datasource delete` removes the storage connector irreversibly; confirm the exact name with the user, and never delete one that feature groups still read from unless they asked.

## Ingest into a new feature group with DLTHub

DLTHub copies a source into a managed feature group for the cases mounting cannot serve: loading the online store or a vector index, or pulling from an API/SaaS/REST endpoint. Ingestion runs server-side in the `dlthub-ingestion-pipeline` environment, driven by a `SinkJobConfiguration` attached to a sink-enabled feature group. It is configuration plus a server job, not an in-process `dlt` call.

The per-feature-group `sink_job` shown here is for ingesting a **single** table. When ingesting more than one table from the same connector, default to one multi-table job for all of them — see [Ingest many tables with one job](#ingest-many-tables-with-one-job) — instead of looping this pattern.

```python
import hopsworks
from hopsworks.core import SinkJobConfiguration
from hopsworks.core.rest_endpoint import RestEndpointConfig

project = hopsworks.login()
fs = project.get_feature_store()

fg = fs.create_feature_group(
    name="events",
    version=1,
    primary_key=["id"],
    data_source=fs.get_data_source("my_connector"),   # the source to pull from
    sink_enabled=True,                                 # provision a sink job
    sink_job_conf=SinkJobConfiguration(
        write_mode="APPEND",
        # REST sources: describe the endpoint to pull.
        endpoint_config=RestEndpointConfig(relative_url="v1/events", query_params={"limit": 1000}),
        # Optional custom transform: a SCRIPT PATH in HopsFS, not a local import.
        transform_script_path="Resources/ingest/transform.py",
    ),
)
fg.save()

fg.sink_job.run()   # runs the ingestion job server-side (see hops-job to monitor)
```

The custom transform is a Python file uploaded to HopsFS and executed inside the ingestion environment, where `dlt` is installed. Write it as a script and point at it with `transform_script_path`; do not import `dlt` in your interactive session.

```python
# Resources/ingest/transform.py: runs in dlthub-ingestion-pipeline, not locally.
import pandas as pd
from dlt.destinations.impl.hopsworks_fg import HopsIngestionTransformer

class MyTransformer(HopsIngestionTransformer):
    def transform(self, df: pd.DataFrame, context: dict) -> pd.DataFrame:
        return df  # model-independent only; do encoding/scaling in a feature view
```

`from dlt.destinations...` resolves only in that server environment. Importing it in the interactive venv raises `ModuleNotFoundError`, which is why the transform is referenced by path, never imported into your session.

## Ingest many tables with one job

**This is the default whenever more than one table is ingested from the same connector.** The per-feature-group `sink_job` above creates one ingestion job per feature group — use it only for a single table. To copy several source tables under a **single** job — one worker pod per table, at most `table_parallelism` at a time — create the ingestion job first and pass it in as each feature group is created. Only fall back to a loop of per-FG sink jobs when the tables come from different connectors or genuinely need independent schedules.

Create the empty job with `data_source.new_ingestion_job(name)`, then pass it as `sink_job=` to each feature group. Each `.save()` registers that feature group as a target **locally**; nothing is created on the server until you call the job's `.save()`, so a failed or partial set of feature groups never leaves a half-built job behind. It is one job to schedule, run, and monitor instead of N.

```python
import hopsworks
from hopsworks.core import SinkJobConfiguration

project = hopsworks.login()
fs = project.get_feature_store()

ds = fs.get_data_source("my_connector")   # the source all targets pull from

# 1. create the ingestion job first
job = ds.new_ingestion_job(
    name="crm_nightly_ingest",
    table_parallelism=3,                  # copy at most 3 tables concurrently
    write_mode="APPEND",                  # job-level default for every target
)

# 2. pass it in as each feature group is created
fs.get_or_create_feature_group(
    "accounts", version=1, data_source=ds,
    sink_enabled=True, sink_job=job,
).save()                                  # local: registers "accounts" as a target

fs.get_or_create_feature_group(
    "contacts", version=1, data_source=ds,
    sink_enabled=True, sink_job=job,
    sink_job_conf=SinkJobConfiguration(write_mode="MERGE"),   # override this table only
).save()                                  # local: registers "contacts" (with its override)

# 3. create the job with all its targets, then run it
job.save()   # ONE atomic create with both feature groups as targets
job.run()    # runs the multi-table job server-side (see hops-job to monitor)
```

A feature group's own `sink_job_conf` supplies that target's overrides; only the fields it changes from the defaults are applied, so a feature group with a bare config (or none) inherits the job-level defaults. Each target's column mappings are generated automatically from its feature group's schema — where the SDK sanitized a source column name (e.g. `"Total Amount"` -> `total_amount`), the mapping back to the original source column is added for you, just like a single-table sink job. For REST sources, the endpoint from the feature group's data source is carried onto its target automatically. Attach a schedule with `schedule_config=` on `new_ingestion_job` to run the whole set on a cadence.

Passing `sink_job=` to `get_or_create_feature_group` registers the feature group whether it is newly created or already exists, so re-running the same script rebuilds the full job. To add a feature group object you already hold (not via `get_or_create`), call `job.add_target(fg)` before `job.save()`.

### Control and monitor individual tables

A multi-table job runs one worker pod per table, and you can act on a single table by its index in the target list (the order you added them):

```python
execution = job.run()          # or job.job.get_executions()[0] for a running one

# Stream one table's live pod log (progress while it is still running), rather
# than the whole job's archived stdout/stderr from execution.download_logs().
podlog = execution.get_pod_logs(table_index=1, lines=200)
print(podlog.status, podlog.log)   # status: AVAILABLE / WAITING_FOR_POD / ...

execution.stop_table(1)        # stop just that table; the others keep running
execution.stop()               # stop the whole execution
```

To exclude a table from *future* runs (rather than stopping the current one), disable it on the job and re-run:

```python
job.set_table_enabled(fs.get_feature_group("contacts", 1), enabled=False)
job.run()                      # "contacts" is skipped; its config and mappings are kept
```

`set_table_enabled` works on a job object you hold; it re-saves the job when it has already been created. `enabled=False` at creation time (via a target or `sink_job_conf`) does the same up front.

### Size resources before ingesting

Rather than guessing the memory and CPU an ingestion needs, ask the backend with `data_source.estimate_ingestion_resources`. It derives a recommendation from the target feature group's schema and the runtime knobs you pass (the same `write_mode`, `batch_size`, worker counts you intend to run with — a bigger batch or more workers raises the estimate). Use it to set a target's `resource_config`, or a single sink job's worker resources, deliberately.

```python
est = ds.estimate_ingestion_resources(
    fs.get_feature_group("contacts", 1),
    write_mode="MERGE",
    batch_size=200000,
)
print(est["recommendedMemoryMb"], est["recommendedCpuCores"], est["confidence"])

# Feed the recommendation into a target's worker resources when you build the job:
job = ds.new_ingestion_job(name="crm_nightly_ingest")
job.add_target(
    fs.get_feature_group("contacts", 1),
    resource_config={"memory": est["recommendedMemoryMb"], "cores": est["recommendedCpuCores"]},
)
```

The returned dict also carries `peakStage`, `reasons`, and `warnings` explaining the recommendation. Pass `configured_memory_mb`/`configured_cpu_cores` to have the estimate compare against resources you already plan to give the job.

## Ingest from Google Sheets

Google Sheets is an independent top-level connector (type `GOOGLE_SHEETS`) authenticated by a GCP service-account JSON keyfile.
Ingestion uses DLTHub under the hood; the sheet tab name maps to `range_names` and the spreadsheet ID maps to `spreadsheet_url_or_id` in the dlt config.

**Prerequisites**

1. Upload the service-account JSON keyfile to HopsFS (e.g. `Resources/gsheets-key.json`).
2. Share the spreadsheet with the service account email (`... @<project>.iam.gserviceaccount.com`) and enable the Google Sheets API in your GCP project.

**Create the connector (UI or SDK)**

In the UI: Storage Connectors → New → Google Sheets.
Fill in the keyfile path.
The spreadsheet ID is optional at connector level — you can leave it blank and provide it per feature group instead.

```python
# The connector only needs to be created once.
connector = fs.get_storage_connector("my_google_sheets_connector")
```

**Ingest a sheet into a feature group**

Provide the sheet tab name via `DataSource.table`.
If the spreadsheet ID was not set on the connector, pass it via `DataSource.spreadsheet_id`.

```python
import hopsworks
from hsfs.core.data_source import DataSource

project = hopsworks.login()
fs = project.get_feature_store()

connector = fs.get_storage_connector("my_google_sheets_connector")

fg = fs.create_feature_group(
    name="budget_actuals",
    version=1,
    primary_key=["id"],
    data_source=DataSource(
        storage_connector=connector,
        table="Q1 Actuals",           # sheet tab name (required)
        spreadsheet_id="1BxiMVs…",   # omit if already set on the connector
    ),
    sink_enabled=True,
)
fg.save()

fg.sink_job.run()   # copies the sheet into the managed feature group
```

**Multiple sheet tabs → multiple feature groups**

Each feature group targets one sheet tab. Reuse the same connector and vary `table` (and `spreadsheet_id` if the tabs live in different spreadsheets). As with any multi-table ingest, default to ONE multi-table job for all tabs (see [Ingest many tables with one job](#ingest-many-tables-with-one-job)) rather than one sink job per tab:

```python
ds = fs.get_data_source("my_google_sheets_connector")

job = ds.new_ingestion_job(name="gsheets_ingest", table_parallelism=2)

for tab, fg_name in [("Users", "gs_users"), ("Orders", "gs_orders")]:
    fs.get_or_create_feature_group(
        name=fg_name,
        version=1,
        primary_key=["id"],
        data_source=DataSource(storage_connector=connector, table=tab),
        sink_enabled=True,
        sink_job=job,
    ).save()                # local: registers the tab as a target

job.save()   # ONE atomic create with every tab as a target
job.run()    # one job copies all tabs, one worker pod per tab
```

## Next Steps

- Discover connectors/tables first: **hops-data-discovery**.
- Work with the resulting feature group: **hops-fg** (read/insert), **hops-fv** (build a view).
- Query the mounted table via SQL: **hops-trino-sql**.

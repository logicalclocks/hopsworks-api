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

The per-feature-group `sink_job` above creates one ingestion job per feature group. To copy several source tables from the same connector under a single job, use `data_source.create_ingestion_job`. It builds one job that copies each target table into its feature group, running at most `table_parallelism` tables at a time (one worker pod per table). Prefer this over a loop of per-FG sink jobs when the sources share a connector: it is one job to schedule, run, and monitor instead of N.

Each target is a `TableIngestionTarget` naming a feature group that already exists. Job-level arguments (`write_mode`, `batch_size`, tuning) are the defaults; any field set on a target overrides the default for that table only.

```python
import hopsworks
from hopsworks.core import TableIngestionTarget

project = hopsworks.login()
fs = project.get_feature_store()

ds = fs.get_data_source("my_connector")   # the source all targets pull from

job = ds.create_ingestion_job(
    name="crm_nightly_ingest",
    table_parallelism=3,                  # copy at most 3 tables concurrently
    write_mode="APPEND",                  # job-level default for every target
    targets=[
        TableIngestionTarget(feature_group=fs.get_feature_group("accounts", 1)),
        TableIngestionTarget(
            feature_group=fs.get_feature_group("contacts", 1),
            write_mode="MERGE",           # override just for this table
        ),
        TableIngestionTarget(feature_group_id=42),   # or reference a FG by id
    ],
)

job.run()   # runs the multi-table job server-side (see hops-job to monitor)
```

Reference a target's feature group either by object (`feature_group=`) or by id (`feature_group_id=`); one of the two is required. To pause a table without removing it from the config, set `enabled=False` on its target. Attach a schedule with `schedule_config=` to run the whole set on a cadence.

### Size resources before ingesting

Rather than guessing the memory and CPU an ingestion needs, ask the backend with `data_source.estimate_ingestion_resources`. It derives a recommendation from the target feature group's schema and the runtime knobs you pass (the same `write_mode`, `batch_size`, worker counts you intend to run with — a bigger batch or more workers raises the estimate). Use it to set a target's `resource_config`, or a single sink job's worker resources, deliberately.

```python
est = ds.estimate_ingestion_resources(
    fs.get_feature_group("contacts", 1),
    write_mode="MERGE",
    batch_size=200000,
)
print(est["recommendedMemoryMb"], est["recommendedCpuCores"], est["confidence"])

# Feed the recommendation straight into a target:
target = TableIngestionTarget(
    feature_group=fs.get_feature_group("contacts", 1),
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

Each feature group targets one sheet tab.
Reuse the same connector and vary `table` (and `spreadsheet_id` if the tabs live in different spreadsheets):

```python
for tab, fg_name in [("Users", "gs_users"), ("Orders", "gs_orders")]:
    fg = fs.create_feature_group(
        name=fg_name,
        version=1,
        primary_key=["id"],
        data_source=DataSource(storage_connector=connector, table=tab),
        sink_enabled=True,
    )
    fg.save()
    fg.sink_job.run()
```

## Next Steps

- Discover connectors/tables first: **hops-data-discovery**.
- Work with the resulting feature group: **hops-fg** (read/insert), **hops-fv** (build a view).
- Query the mounted table via SQL: **hops-trino-sql**.

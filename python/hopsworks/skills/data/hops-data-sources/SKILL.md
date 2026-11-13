---
name: hops-data-sources
description: Mount or ingest a table from a supported datasource. Mount tables from a datasource as an external feature group or ingest data into a new feature group using DLTHub. Auto-invoke when user works with external data (Snowflake, BigQuery, Redshift, S3, ADLS, GCS, JDBC, SQL, Databricks Unity Catalog, Postgres, MySQL, Oracle, ClickHouse, Teradata, SQL Server, SAP, MongoDB, Elasticsearch, another Hopsworks cluster, CRM, REST APIs, Gmail) or copies files from SharePoint into a dataset. For searching copied emails use hops-email-search.
---

Prefer the `hops` CLI for mounting or ingesting external tables from a datasource. Use the `hopsworks` Python SDK if the CLI is unsuccessful.

A secret option of `hops datasource create` (`--password`, `--api-key`, ...) takes the value itself, `-` to read it from stdin, or `account:NAME` to read your account variable `NAME`. Use `account:NAME` for a credential the user saved in their account (the Factory's forms do): the value goes to the data source without a shell, a file or a printout, and never through `$(...)` around a command that also prints, such as `hopsworks.login()`.

Never ask the user for a password, token or key in the conversation, in a message or with AskUserQuestion: whatever is typed there reaches the model. In the Factory's chat, end your reply with a `hops-secret` block instead. It is a form: the secret as a hidden field, the plain settings that go with it (`fields`, never a secret) and, for a data source, a `test` command the form's Test button runs:

```hops-secret
{"name": "CRM_PASSWORD", "label": "CRM password", "description": "Password of the hopsworks_reader user",
 "fields": [{"name": "host", "label": "Host"}, {"name": "port", "label": "Port", "default": "5432"},
            {"name": "database", "label": "Database"}, {"name": "user", "label": "User"}],
 "test": "hops datasource create sql crm --database-type POSTGRESQL --host {{host}} --port {{port}} --database {{database}} --user {{user}} --password account:CRM_PASSWORD --test"}
```

`name` is in capitals, digits and underscores. `test` is a `hops datasource create ... --test` command, which checks the connection and creates nothing; `{{field}}` stands for a field's value. The chat saves the secret as the user's private account variable `name`, and replies with the settings and the secret's name, never its value. Then create the data source with the same command without `--test`. Outside the chat, ask the user to add the account variable in their account settings and tell you its name.

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
sc = fs.get_data_source("my_crm_connector").storage_connector
tables = sc.get_tables()                  # CRM: the connector's supported resources

by_name = sc.get_data_batch(tables[:3])   # N resources, ONE schema-fetch job
by_name["contacts"].features              # inferred schema of one resource
by_name["contacts"].preview               # sampled rows

data = sc.get_data(tables[0])             # single resource fallback
```

Both calls block until the fetch finishes and raise `hopsworks.client.exceptions.DataSourceException` with the job logs when it fails; `get_data_batch` reports every failed resource in one exception.
`get_data` raises the same exception for SQL sources, where the read is served inline rather than by a job: a refused read is answered with HTTP 200 and the source's own message, so the exception carries that message.
`DataSourceException` is a subclass of `FeatureStoreException`, so an `except FeatureStoreException` around `fg.save()` catches a failed schema fetch during schema inference.
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

`hops datasource create <type>` exists for every type the UI offers; `--help` lists its options and the secret variables. Every type's command line, secrets from stdin or `HOPSWORKS_DS_*` variables, HopsFS key paths and `--argument`: `hops skills info hops-data-sources references/create.md`. Oracle's `PROVIDED` credentials mode (each member reads with their own login): `hops skills info hops-data-sources references/provided-credentials.md`.

**Confirm before deleting.** `hops datasource delete` removes the storage connector irreversibly; confirm the exact name with the user, and never delete one that feature groups still read from unless they asked.

## Tag a data source

Schematized tags classify a data source (owner, source system, sensitivity) and make it findable in search.
A tag that an admin made mandatory for data sources must be passed when the data source is created.
`hops datasource create` cannot attach tags, so it fails while a tag is mandatory; create the data source from Python with `sc.save(tags=[...])` or `fs.create_data_source(sc, tags=[...])` instead.
The SDK calls, mandatory tags and search are in [references/tags.md](references/tags.md).

## Ingest into a new feature group with DLTHub

DLTHub copies a source into a managed feature group for the cases mounting cannot serve: loading the online store or a vector index, or pulling from an API/SaaS/REST endpoint. Ingestion runs server-side in the `dlthub-ingestion-pipeline` environment, driven by a `SinkJobConfiguration` attached to a sink-enabled feature group. It is configuration plus a server job, not an in-process `dlt` call.

The per-feature-group `sink_job` shown here is for ingesting a **single** table. When ingesting more than one table from the same connector, default to one multi-table job for all of them — see [Ingest many tables with one job](#ingest-many-tables-with-one-job) — instead of looping this pattern.

Many tables in one job with per-table control and resource estimates, Google Sheets tabs, and tagging an ingested table as bronze for **hops-analytics**: `hops skills info hops-data-sources references/ingest-sdk.md`.

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

## Next Steps

- Mount an Elasticsearch index: `hops skills info hops-data-sources references/elasticsearch.md`. A feature group of another Hopsworks cluster, SharePoint folders, Gmail and Outlook mailboxes: `hops skills info hops-data-sources references/other-sources.md`.

- Discover connectors/tables first: **hops-data-discovery**.
- Work with the resulting feature group: **hops-fg** (read/insert), **hops-fv** (build a view).
- Query the mounted table via SQL: **hops-trino-sql**.

---
name: hops-data-sources
description: Mount or ingest a table from a supported datasource. Mount tables from a datasource as an external feature group or ingest data into a new feature group using DLTHub. Auto-invoke when user works with external data (Snowflake, BigQuery, Redshift, S3, ADLS, GCS, JDBC, SQL, Databricks Unity Catalog, Postgres, MySQL, Oracle, SAP, MongoDB, CRM, REST APIs).
---

Prefer the `hops` CLI for mounting or ingesting external tables from a datasource. Use the `hopsworks` Python SDK if the CLI is unsuccessful.

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

If platform intelligence is not configured on the cluster, the command exits with a clear error — ask the cluster admin to set `PLATFORM_INTELLIGENCE_LLM_API_KEY` rather than guessing the metadata by hand.

Programmatic equivalent: `data_source.infer_metadata()` on a `DataSource` returned by `data_source.get_tables()`; it raises `hopsworks.client.exceptions.PlatformIntelligenceException` (with `.reason` of `NOT_CONFIGURED` or `INFERENCE_FAILED`) on the same failure modes.

## Mount a table as an external feature group

An external (on-demand) feature group leaves data in the source and queries it through the connector — no copy into Hopsworks.

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

## Ingest into a new feature group with DLTHub

DLTHub ingestion copies the source into a managed feature group and runs server-side in the `dlthub-ingestion-pipeline` environment (`dlt` is not installed in the interactive venv). You can attach a custom transform; the example just copies rows, but any Pandas-DataFrame transform works:

```python
import pandas as pd
from dlt.destinations.impl.hopsworks_fg import HopsIngestionTransformer

class MyTransformer(HopsIngestionTransformer):
    def transform(self, df: pd.DataFrame, context: dict) -> pd.DataFrame:
        return df.copy()

transformer = MyTransformer()
# Wire the transformer into the DLT ingestion job (SinkJobConfiguration) and run it
# as a Hopsworks job — see hops-job. The job materializes the source into the FG.
```

## Next Steps

- Discover connectors/tables first: **hops-data-discovery**.
- Work with the resulting feature group: **hops-fg** (read/insert), **hops-fv** (build a view).
- Query the mounted table via SQL: **hops-trino-sql**.

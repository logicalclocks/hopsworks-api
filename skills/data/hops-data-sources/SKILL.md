---
name: connect-data-source
description: Mount or ingest a table from a support datasource. Mount tables from a datasource as an external feature group or ingest data into a new feature group using DLTHub. Auto-invoke when user works with external data (Snowflake, BigQuery, Redshift, S3, ADLS, GCS, JDBC, SQL, Databricks Unity Catalog, Postgres, MySQL, Oracle, SAP, MongoDB, CRM, REST APIs).
---

Prefer the hops CLI mounting or ingesting external tables using a datasource. Use hopsworks-api and Python programs if hops CLI is unsuccessful.

Use platform intelligence to (1) select the primary key and event_time columns and (2) create descriptions for columns for the mounted or ingested feature group. This returns a suggested feature name (snake_case), an offline data type, a one-line description per column, iplus a suggested primary key and event-time column.

```bash
hops datasource infer-metadata <connector-name> <table> [--database <db>]
```

If platform intelligence is not configured on the cluster, the command exits with a clear error — ask the cluster admin to set `PLATFORM_INTELLIGENCE_LLM_API_KEY` rather than guessing the metadata by hand.

For programmatic use the equivalent Python is `data_source.infer_metadata()` on a `DataSource` returned by `data_source.get_tables()`; it raises `hopsworks.client.exceptions.PlatformIntelligenceException` (with `.reason` of `NOT_CONFIGURED` or `INFERENCE_FAILED`) on the same failure modes.

## DLTHub Ingestion

You can define custom transformations when ingestion data to a feature group. Here is an example that just copies the data, but any Pandas DF transformations can be applied in the transform method:

import pandas as pd

from dlt.destinations.impl.hopsworks_fg import HopsIngestionTransformer

class MyTransformer(HopsIngestionTransformer):
    def transform(self, df: pd.DataFrame, context: dict) -> pd.DataFrame:
        return df.copy()


transformer = MyTransformer()

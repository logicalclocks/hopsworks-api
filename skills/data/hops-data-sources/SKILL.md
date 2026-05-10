---
name: connect-data-source
description: Create a connection to one of Hopsworks available data sources. Mount tables from a data source as an external feature group or ingest data from an external table into a new feature group.  Use when writing Python code that creates storage connectors, data sources, or external feature groups in Hopsworks. Auto-invoke when user works with external data (Snowflake, BigQuery, Redshift, S3, ADLS, GCS, JDBC, SQL, Dataricks Unity Catalog, Postgres, MySQL, Oracle, CRM, REST APIs), configures data ingestion with DLTHub, or schedules materialization jobs.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

Prefer the `hops` CLI for data discovery over writing Python.

When mounting an external table as an external feature group — or when creating a new feature group from an external table — use platform intelligence to infer the metadata first:

```bash
hops datasource infer-metadata <connector-name> <table> [--database <db>]
```

This calls the same LLM-backed endpoint as the "Infer metadata" button in the UI: it returns a suggested feature name (snake_case), Hopsworks data type, and one-line description per column, plus a suggested primary key and event-time column.

Use the suggestions as the starting point for `primary_key`, `event_time`, and the per-feature `Feature(name=…, type=…, description=…)` you pass into `feature_store.create_external_feature_group(...)` or `feature_store.create_feature_group(...)`.

If platform intelligence is not configured on the cluster, the command exits with a clear error — ask the cluster admin to set `PLATFORM_INTELLIGENCE_LLM_API_KEY` rather than guessing the metadata by hand.

For programmatic use the equivalent Python is `data_source.infer_metadata()` on a `DataSource` returned by `data_source.get_tables()`; it raises `hopsworks.client.exceptions.PlatformIntelligenceException` (with `.reason` of `NOT_CONFIGURED` or `INFERENCE_FAILED`) on the same failure modes.

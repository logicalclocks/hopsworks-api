---
name: hopsworks-data-sources
description: Use when writing Python code that creates storage connectors, data
  sources, or external feature groups in Hopsworks. Auto-invoke when user works with
  external data (Snowflake, BigQuery, Redshift, S3, ADLS, GCS, JDBC, SQL, CRM, REST
  APIs), creates external feature groups, configures data ingestion, or schedules
  materialization jobs.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Data Sources — Python SDK Best Practices

Reference: `/tmp/hopsworks-api/python/hsfs/`

## Available Data Sources (Storage Connectors)

Hopsworks supports 14 storage connector types. Each connector wraps the configuration needed to read data from an external system.

| Connector | Type Constant | Category | External FG | Notes |
|---|---|---|---|---|
| **Snowflake** | `SNOWFLAKE` | Data Warehouse | Yes | Query-based, supports database/schema/table |
| **BigQuery** | `BIGQUERY` | Data Warehouse | Yes | Query-based, supports project/dataset/table |
| **Redshift** | `REDSHIFT` | Data Warehouse | Yes | Query-based, supports database/group/table |
| **JDBC** | `JDBC` | Database | Yes | Generic JDBC connections |
| **SQL** | `SQL` | Database | Yes | Generic SQL connector |
| **S3** | `S3` | Cloud Storage | Yes | Path-based, requires `data_format` |
| **ADLS** | `ADLS` | Cloud Storage | Yes | Path-based, requires `data_format` |
| **GCS** | `GCS` | Cloud Storage | Yes | Path-based, requires `data_format` |
| **HopsFS** | `HOPSFS` | Cloud Storage | Yes | Path-based, requires `data_format` |
| **CRM** | `CRM` | SaaS/Analytics | Yes | 7 CRM sources (see below) |
| **REST** | `REST` | REST API | Yes | Requires `rest_endpoint` config |
| **Kafka** | `KAFKA` | Streaming | **No** | Streaming only — use `read_stream()` |
| **OpenSearch** | `OPENSEARCH` | Search | **No** | Search/analytics only |

### CRM Sources (CRMSource Enum)

The CRM connector supports 7 SaaS platforms:

- `CRMSource.HUBSPOT`
- `CRMSource.FACEBOOK_ADS`
- `CRMSource.SALESFORCE`
- `CRMSource.PIPEDRIVE`
- `CRMSource.FRESHDESK`
- `CRMSource.GOOGLE_ADS`
- `CRMSource.GOOGLE_ANALYTICS`

### DLTHub Support

**No DLTHub/dlt integration exists** in the Hopsworks Python SDK. The CRM connector handles SaaS data ingestion natively on the Hopsworks server side. Data ingestion for online feature groups goes through Kafka. Offline materialization uses Spark jobs.

---

## DataSource Object

The `DataSource` class encapsulates where to read data from. It combines a storage connector with location details (query, table, path, or REST endpoint).

```python
from hsfs.core.data_source import DataSource

# Query-based (Snowflake, BigQuery, Redshift, JDBC, SQL)
data_source = DataSource(
    storage_connector=snowflake_connector,
    database="my_db",
    group="my_schema",       # schema name
    table="my_table",
)

# Or with a raw SQL query
data_source = DataSource(
    storage_connector=snowflake_connector,
    query="SELECT * FROM my_db.my_schema.my_table WHERE active = true",
)

# Path-based (S3, ADLS, GCS, HopsFS)
data_source = DataSource(
    storage_connector=s3_connector,
    path="s3://bucket/path/to/data/",
)

# REST-based
from hopsworks.core.rest_endpoint import RestEndpointConfig, OffsetPaginationConfig

data_source = DataSource(
    storage_connector=rest_connector,
    rest_endpoint=RestEndpointConfig(
        relative_url="/api/v1/users",
        query_params={"status": "active"},
        pagination_config=OffsetPaginationConfig(limit=100),
    ),
)
```

### DataSource Properties

| Property | Type | Description |
|---|---|---|
| `query` | `str` | SQL query string |
| `database` | `str` | Database name |
| `group` | `str` | Schema/group name |
| `table` | `str` | Table name |
| `path` | `str` | File system path |
| `storage_connector` | `StorageConnector` | Connection configuration |
| `metrics` | `list[str]` | Metric column names |
| `dimensions` | `list[str]` | Dimension column names |
| `rest_endpoint` | `RestEndpointConfig` | REST endpoint configuration |

### DataSource Methods

| Method | Returns | Description |
|---|---|---|
| `get_databases()` | `list[str]` | List databases (Snowflake, BigQuery, Redshift, JDBC, SQL) |
| `get_tables(database)` | `list[DataSource]` | List tables from a database |
| `get_data(use_cached=True)` | `DataSourceData` | Retrieve data. `use_cached` only for CRM/REST |
| `get_metadata()` | `dict` | Get metadata about the data source |
| `get_feature_groups()` | `list[FeatureGroup]` | Get FGs using this data source |

### Retrieving an Existing Data Source

```python
import hopsworks

project = hopsworks.login()
fs = project.get_feature_store()

data_source = fs.get_data_source("my_snowflake_connector")

# Browse databases and tables
databases = data_source.get_databases()
tables = data_source.get_tables(database="my_db")

# Preview data from a specific table
table_ds = tables[0]
data = table_ds.get_data()
```

---

## Creating External Feature Groups

External feature groups reference data stored outside Hopsworks. They support all connectors **except** Kafka and OpenSearch.

```python
from datetime import timedelta

# Create the external FG (lazy — metadata only)
external_fg = fs.create_external_feature_group(
    name="sales",
    version=1,
    description="Physical shop sales features from Snowflake",
    data_source=data_source,
    primary_key=["store_id"],
    event_time="sale_date",
    online_enabled=True,           # enable online serving
    ttl=timedelta(days=30),        # auto-expire online rows
)

# Persist metadata to Hopsworks
external_fg.save()
```

### External FG with Online Sync

External FGs store data in the external source (offline). To serve features online, you must manually sync:

```python
# Read from external source
df = external_fg.read()

# Optionally filter
df = df.filter(external_fg.customer_status == "active")

# Insert to online store
external_fg.insert(df)
```

### Parameters for create_external_feature_group

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Feature group name |
| `data_source` | `DataSource` | Data source with connector + location |
| `version` | `int` | Version number (auto-increments if None) |
| `description` | `str` | Human-readable description |
| `primary_key` | `list[str]` | Primary key columns (used as join key) |
| `foreign_key` | `list[str]` | Foreign key columns |
| `event_time` | `str` | Column for point-in-time joins |
| `features` | `list[Feature]` | Explicit schema (inferred if omitted) |
| `online_enabled` | `bool` | Enable online store sync |
| `online_config` | `dict` | Online table configuration |
| `online_disk` | `bool` | Store online data on disk vs memory |
| `ttl` | `timedelta` | Time-to-live for online rows |
| `embedding_index` | `EmbeddingIndex` | For vector similarity search |
| `statistics_config` | `bool/dict` | Statistics computation config |
| `data_format` | `str` | File format for path-based sources (e.g., `"parquet"`, `"csv"`) |
| `options` | `dict` | Engine read options (e.g., `{"header": True}` for CSV) |

### Connector-Specific DataSource Setup

**Snowflake:**
```python
ds = DataSource(storage_connector=sc, database="MY_DB", group="PUBLIC", table="SALES")
```

**BigQuery:**
```python
ds = DataSource(storage_connector=sc, database="my-gcp-project", group="my_dataset", table="sales")
```

**Redshift:**
```python
ds = DataSource(storage_connector=sc, database="mydb", group="public", table="sales")
```

**S3 / ADLS / GCS / HopsFS:**
```python
ds = DataSource(storage_connector=sc, path="/path/to/data/")
# Also set data_format on the external FG:
fg = fs.create_external_feature_group(name="sales", data_source=ds, data_format="parquet", ...)
```

---

## REST Endpoint Configuration

For REST connectors, configure how to access and paginate the API using `RestEndpointConfig`.

```python
from hopsworks.core.rest_endpoint import (
    RestEndpointConfig,
    QueryParams,
    OffsetPaginationConfig,
    CursorPaginationConfig,
    PageNumberPaginationConfig,
    SinglePagePaginationConfig,
    AutoPaginationConfig,
)
```

### Pagination Types

| Type | Class | Use Case |
|---|---|---|
| `offset` | `OffsetPaginationConfig` | Limit/offset APIs |
| `page_number` | `PageNumberPaginationConfig` | Page-based APIs |
| `cursor` | `CursorPaginationConfig` | Cursor-based pagination (in body) |
| `json_link` | `JsonLinkPaginationConfig` | Next page URL in JSON body |
| `header_link` | `HeaderLinkPaginationConfig` | Next page URL in Link header |
| `header_cursor` | `HeaderCursorPaginationConfig` | Cursor in response headers |
| `single_page` | `SinglePagePaginationConfig` | No pagination needed |
| `auto` | `AutoPaginationConfig` | Auto-detect pagination strategy |

### OffsetPaginationConfig Parameters (Batch Size Control)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | `int` | None | Page/batch size per request |
| `offset` | `int` | 0 | Starting offset |
| `offset_param` | `str` | `"offset"` | Query param name for offset |
| `limit_param` | `str` | `"limit"` | Query param name for limit |
| `offset_body_path` | `str` | None | JSON body path for offset |
| `limit_body_path` | `str` | None | JSON body path for limit |
| `total_path` | `str` | None | JSON path to total count |
| `maximum_offset` | `int` | None | Stop after this offset |
| `stop_after_empty_page` | `bool` | True | Stop when page is empty |
| `has_more_path` | `str` | None | JSON path to "has more" boolean |

### Example: REST External Feature Group with Pagination

```python
rest_endpoint = RestEndpointConfig(
    relative_url="/api/v2/contacts",
    query_params={"status": "active", "fields": "id,name,email,created_at"},
    pagination_config=OffsetPaginationConfig(
        limit=200,                  # fetch 200 records per request
        limit_param="per_page",     # API uses "per_page" not "limit"
        offset_param="offset",
        stop_after_empty_page=True,
    ),
)

data_source = DataSource(
    storage_connector=rest_connector,
    rest_endpoint=rest_endpoint,
)

external_fg = fs.create_external_feature_group(
    name="contacts",
    data_source=data_source,
    primary_key=["id"],
    event_time="created_at",
    online_enabled=True,
)
external_fg.save()
```

---

## Data Ingestion

### Online Ingestion (Kafka-Based)

When `online_enabled=True`, data inserted into feature groups flows through Kafka to the online store (RonDB). This applies to both regular and external feature groups.

```python
# External FG: read from external source, insert to online
df = external_fg.read()
external_fg.insert(df, write_options={
    "wait_for_online_ingestion": True,
    "online_ingestion_options": {
        "timeout": 120,              # seconds to wait
        "period": 2,                 # polling interval
        "upsert_if_newer": True,     # only update if newer event_time
    },
})
```

### Write Options for insert()

| Key | Type | Default | Description |
|---|---|---|---|
| `wait_for_job` | `bool` | True | Block until materialization job finishes |
| `wait_for_online_ingestion` | `bool` | False | Block until online ingestion finishes |
| `start_offline_materialization` | `bool` | True | Start materialization job immediately |
| `offline_backfill_every_hr` | `int\|str` | None | Schedule materialization (see below) |
| `kafka_producer_config` | `dict` | None | Confluent Kafka producer properties |
| `internal_kafka` | `bool` | False | Use internal Kafka listeners |
| `kafka_timeout` | `int` | None | Kafka timeout in seconds |

### Online Ingestion Options (nested in write_options)

| Key | Type | Default | Description |
|---|---|---|---|
| `timeout` | `int` | 60 | Seconds to wait (0 = indefinite) |
| `period` | `int` | 1 | Polling interval in seconds |
| `upsert_if_newer` | `bool` | False | Only update if event_time is newer |
| `mark_online_rows` | `bool` | True | Deduplicate by event_time + primary key |
| `disable_online_ingestion_count` | `bool` | False | Disable progress tracking |

---

## Scheduling Data Ingestion as a Job

For feature groups with `online_enabled=True` and `stream=True`, data written to Kafka must be materialized to the offline store (Delta Lake) via a Spark materialization job. You can schedule this job to run periodically.

### Option 1: Set Schedule at Feature Group Creation

```python
# Integer: run materialization every N hours
fg = fs.get_or_create_feature_group(
    name="user_features",
    version=1,
    primary_key=["user_id"],
    event_time="timestamp",
    online_enabled=True,
    stream=True,
    offline_backfill_every_hr=4,  # every 4 hours
)
fg.insert(df)  # first insert creates and schedules the materialization job
```

### Option 2: Cron Expression at Creation

```python
# Quartz cron expression for precise control
fg = fs.get_or_create_feature_group(
    name="user_features",
    version=1,
    primary_key=["user_id"],
    event_time="timestamp",
    online_enabled=True,
    stream=True,
    offline_backfill_every_hr="0 0 */6 ? * * *",  # every 6 hours at :00
)
fg.insert(df)
```

### Option 3: Via write_options on First Insert

```python
fg.insert(df, write_options={
    "offline_backfill_every_hr": 2,           # every 2 hours
    "start_offline_materialization": True,     # run immediately too
})
```

### Option 4: Schedule an Existing Feature Group's Materialization Job

```python
from datetime import datetime, timezone, timedelta

# Get the materialization job
job = fg.materialization_job

# Schedule with Quartz cron expression
job.schedule(
    cron_expression="0 0 */4 ? * * *",   # every 4 hours
    start_time=datetime.now(tz=timezone.utc) + timedelta(seconds=5),
    end_time=None,                        # run indefinitely
)

# Check schedule
print(job.job_schedule.cron_expression)
print(job.job_schedule.next_execution_date_time)

# Manage the schedule
job.pause_schedule()
job.resume_schedule()
job.unschedule()

# Manually run the job
execution = job.run(await_termination=True)
```

### Cron Expression Format (Quartz)

Format: `seconds minutes hours day-of-month month day-of-week year`

| Expression | Meaning |
|---|---|
| `0 0 */4 ? * * *` | Every 4 hours at :00:00 |
| `0 30 2 ? * * *` | Daily at 2:30 AM UTC |
| `0 0 0 ? * MON *` | Every Monday at midnight |
| `0 0 */1 ? * * *` | Every hour |

When using the integer format for `offline_backfill_every_hr`, the cron expression is generated as: `{current_second} {current_minute} {current_hour}/{interval} ? * * *`

---

## Complete Example: External Snowflake Feature Group with Scheduled Ingestion

```python
import hopsworks
from hsfs.core.data_source import DataSource
from datetime import timedelta, datetime, timezone

# 1. Connect
project = hopsworks.login()
fs = project.get_feature_store()

# 2. Get the Snowflake data source (connector must be pre-configured in Hopsworks)
data_source = fs.get_data_source("my_snowflake_connector")

# 3. Browse available tables
tables = data_source.get_tables(database="ANALYTICS_DB")
print([t.table for t in tables])

# 4. Configure the data source to point at a specific table
sales_source = DataSource(
    storage_connector=data_source.storage_connector,
    database="ANALYTICS_DB",
    group="PUBLIC",
    table="DAILY_SALES",
)

# 5. Create the external feature group
external_fg = fs.create_external_feature_group(
    name="daily_sales",
    version=1,
    description="Daily sales data from Snowflake warehouse",
    data_source=sales_source,
    primary_key=["store_id", "sale_date"],
    event_time="sale_date",
    online_enabled=True,
    ttl=timedelta(days=90),
    statistics_config=False,
)
external_fg.save()

# 6. Initial data load: read from Snowflake, insert to online store
df = external_fg.read()
external_fg.insert(df, write_options={
    "wait_for_online_ingestion": True,
    "online_ingestion_options": {
        "timeout": 300,
        "upsert_if_newer": True,
    },
})

# 7. Schedule periodic sync (every 6 hours)
# Create a regular FG that mirrors the external data for offline+online
sync_fg = fs.get_or_create_feature_group(
    name="daily_sales_synced",
    version=1,
    primary_key=["store_id", "sale_date"],
    event_time="sale_date",
    online_enabled=True,
    stream=True,
    offline_backfill_every_hr=6,
    statistics_config=False,
)

# Insert triggers job creation + scheduling
sync_fg.insert(df)

# 8. Verify the schedule
job = sync_fg.materialization_job
print(f"Scheduled: {job.job_schedule.cron_expression}")
print(f"Next run:  {job.job_schedule.next_execution_date_time}")
```

---

## Quick Reference

| Task | Code |
|---|---|
| Get data source | `fs.get_data_source("name")` |
| List databases | `data_source.get_databases()` |
| List tables | `data_source.get_tables(database="db")` |
| Preview data | `data_source.get_data()` |
| Create external FG | `fs.create_external_feature_group(name=..., data_source=..., ...)` |
| Save external FG | `external_fg.save()` |
| Read external data | `external_fg.read()` |
| Insert to online | `external_fg.insert(df)` |
| Get materialization job | `fg.materialization_job` |
| Schedule materialization | `fg.materialization_job.schedule(cron_expression="...")` |
| Pause schedule | `fg.materialization_job.pause_schedule()` |
| Resume schedule | `fg.materialization_job.resume_schedule()` |
| Remove schedule | `fg.materialization_job.unschedule()` |
| Run materialization now | `fg.materialization_job.run(await_termination=True)` |
| Check schedule | `fg.materialization_job.job_schedule.cron_expression` |

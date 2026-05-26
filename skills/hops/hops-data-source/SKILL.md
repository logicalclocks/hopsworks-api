---
name: hopsworks-data-sources
description: Use when writing Python code that creates data sources, external feature groups or ingests into feature groups in Hopsworks. Auto-invoke when user works with external data (Snowflake, BigQuery, Redshift, S3, ADLS, GCS, JDBC, SQL, CRM, REST APIs), creates external feature groups, configures data ingestion with DLTHub.
---

# Hopsworks Data Sources — Python SDK Best Practices

See hopsworks-api source code for details.

### DLTHub Support

**No DLTHub/dlt integration exists** in the Hopsworks Python SDK. The CRM connector handles SaaS data ingestion natively on the Hopsworks server side. Data ingestion for online feature groups goes through Kafka. Offline materialization uses Spark jobs.

---


## Creating External Feature Groups

External feature groups reference data stored outside Hopsworks. They support all connectors **except** Kafka and OpenSearch.



---

## Data Ingestion

Scheduling Data Ingestion as a Job.


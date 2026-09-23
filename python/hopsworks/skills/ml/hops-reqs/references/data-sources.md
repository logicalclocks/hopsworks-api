# Data sources: the three routes and what to gather for each

Every entry in `requirements.data_sources` takes exactly one route, and the
requirements conversation settles, per source, what the data phase will need.
Run `hops context`, `hops fg list` and `hops datasource list` before asking
anything, so the user picks from what exists rather than recalling names.

## Route 1: an existing feature group

The default, offered first. Show candidates with `hops fg list`,
`hops search <term>` and `hops fg info <name> --version <v>` (grain, primary
key, event time) and let the user pick.

```yaml
- {name: telco_customers, kind: feature_group, version: 1, grain: one row per customer, event_time: snapshot_date, status: present}
```

The data phase checks it exists, has rows (`hops fg preview <name> --n 5`, a
bounded read) and that its key and event time match the declared grain.

## Route 2: a new data source

A storage connector reaches the external system; `hops datasource create
<type>` exists for every type the UI offers. Name the type from what the user
says holds the data, list the options it requires (table below, and
`hops datasource create <type> --help`, which wins when they disagree), and
record them under `needs` so the user can gather them before the data phase.

**Secrets are never asked for in the conversation** and never passed as
arguments. Nothing of a secret lands in the transcript, `system.yaml` or the
repository. A secret option takes `-` to read one line from stdin, or its value
from `HOPSWORKS_DS_<TYPE>_<OPTION>`. The agent's shell has no terminal to type
into, so the data phase offers two ways, in this order:

- Print the exact `hops datasource create <type> <name> ... --password -` line
  for the user to run in their own terminal, then continue once
  `hops datasource info <name>` finds the connector.
- The user writes the secret to a file outside the repository (`chmod 600`),
  and the data phase runs the command with `--password - < <file>`, then asks
  the user to delete the file. The file's content never appears in a command or
  its output.

```yaml
- {name: billing, kind: datasource, type: snowflake, connector: acme_snowflake, table: BILLING.INVOICES,
   grain: one row per invoice, event_time: invoice_date, status: needs_connection,
   needs: [url, user, database, schema, warehouse; password by stdin or HOPSWORKS_DS_SNOWFLAKE_PASSWORD]}
```

Status: `needs_connection` until the connector exists (`hops datasource list`),
`connected` once it exists but the table is not yet a feature group, and
`present` only once it is mounted or ingested, since a connector alone gives the
pipelines nothing to read. The data phase then follows the mount-or-ingest rule
of **hops-data-sources**: mount as an external feature group when the system
reads it offline; ingest with a DLTHub job when the online store or a vector
index must be loaded, or the source is an API with no table to mount.

| Type | Holds | Required options | Secret options (stdin `-` or environment variable) |
| --- | --- | --- | --- |
| adls | Azure Data Lake Storage | `--account-name`, `--generation`, `--directory-id`, `--application-id` | `--service-credential` (`HOPSWORKS_DS_ADLS_SERVICE_CREDENTIAL`) |
| bigquery | Google BigQuery | `--parent-project`, `--key-path` | none; the key file is uploaded to HopsFS first |
| crm | CRM and analytics APIs | `--crm-type` | `--api-key`, `--password`, `--dev-token`, `--refresh-token`, `--private-app-password` (`HOPSWORKS_DS_CRM_<OPTION>`) |
| gcs | Google Cloud Storage | `--bucket`, `--key-path` | `--encryption-key` (`HOPSWORKS_DS_GCS_ENCRYPTION_KEY`) |
| glue | AWS Glue Data Catalog | `--database`, `--region` | `--secret-key`, `--session-token` (`HOPSWORKS_DS_GLUE_<OPTION>`) |
| google-sheets | Google Sheets | `--key-path` | none; the key file is uploaded to HopsFS first |
| hopsfs | A dataset in this project | `--dataset` | none |
| jdbc | Any JDBC database | `--url` | `--password` (`HOPSWORKS_DS_JDBC_PASSWORD`) |
| kafka | An external Kafka cluster | `--bootstrap-servers`, `--security-protocol` | `--ssl-truststore-password`, `--ssl-keystore-password`, `--ssl-key-password` (`HOPSWORKS_DS_KAFKA_<OPTION>`) |
| mongodb | MongoDB | `--connection-string`, `--database` | `--password` (`HOPSWORKS_DS_MONGODB_PASSWORD`) |
| opensearch | OpenSearch | `--host`, `--port` | `--password`, `--truststore-password` (`HOPSWORKS_DS_OPENSEARCH_<OPTION>`) |
| redshift | Amazon Redshift | `--cluster-identifier`, `--endpoint`, `--database`, `--port` | `--password` (`HOPSWORKS_DS_REDSHIFT_PASSWORD`) |
| rest | A REST API | `--base-url`, `--auth-type` | `--api-key`, `--bearer-token`, `--password`, `--client-secret`, `--access-token` (`HOPSWORKS_DS_REST_<OPTION>`) |
| s3 | Amazon S3 | `--bucket` | `--secret-key` (`HOPSWORKS_DS_S3_SECRET_KEY`) |
| sap-hana | SAP HANA | `--host`, `--user` | `--password` (`HOPSWORKS_DS_SAP_HANA_PASSWORD`) |
| snowflake | Snowflake | `--url`, `--user`, `--database`, `--schema`, `--warehouse` | `--password` (`HOPSWORKS_DS_SNOWFLAKE_PASSWORD`) |
| sql | MySQL, PostgreSQL, Oracle, SQL Server, ClickHouse, Teradata and other SQL databases | `--database-type`, `--port`, `--database`, `--user` | `--password`, `--wallet-password` (`HOPSWORKS_DS_SQL_<OPTION>`) |
| unity-catalog | Databricks Unity Catalog | `--workspace-url` | `--access-token`, `--client-secret` (`HOPSWORKS_DS_UNITY_CATALOG_<OPTION>`) |

`<OPTION>` is the option name upper-cased with hyphens as underscores
(`--refresh-token` is `HOPSWORKS_DS_CRM_REFRESH_TOKEN`). The auth mode decides
which secret a type needs: ask which mode the user has, then list only its
secrets under `needs`.

After creating the connector, find and inspect the table before deciding how it
enters the feature store:

```bash
hops datasource info acme_snowflake
hops datasource databases acme_snowflake
hops datasource tables acme_snowflake --database BILLING
hops datasource preview acme_snowflake                                   # a small sample through the connector
hops datasource infer-metadata acme_snowflake INVOICES --database BILLING  # primary key, event time, descriptions, when the cluster has it
```

## Route 3: a file or a URL

`kind: file` for a file on the laptop or already in HopsFS, `kind: url` for one
the cluster or the laptop can fetch; `status: needs_download` until it is under
`Resources/<slug>/data/`. A local file goes up with `hops files upload`; a URL
the cluster reaches is fetched by a small job; one it cannot is downloaded on the
laptop and uploaded. Nothing is read from the laptop at run time.

## Synthetic data

Offered when the user has no data yet, wants to try the system's shape before
connecting a real source, or is building a demo. Settle the shape (batch tables
or a stream of events), the entity and its cardinality, the event time, the
columns with types and rough distributions, and the **story**: one or two
sentences saying which columns carry the signal the target depends on and how
strongly. The story is a requirement: it bounds what any model can achieve.
Generation is **hops-synthetic-data**.

```yaml
- {name: usage_events, kind: synthetic, shape: events, grain: one row per call, entity: customer_id, event_time: ts, status: needs_generation}
```

## Sizing tier

Asked once, `small` proposed, recorded as `requirements.sizing.tier`. No `hops`
command reports the cluster's size, and a terminal's `kubectl` role is
namespace-scoped, so the tier is the user's statement:

| Tier | For | Batch, per table | Events | Online store at a 7-day TTL | Offline growth |
| --- | --- | --- | --- | --- | --- |
| small (default) | serverless, trial, a development cluster of a few workers | 10,000 entities, 100,000 history rows | 5 events/s over 2,000 entities, 10 s ticks | about 3 million rows | about 100 MB a day |
| medium | a shared team cluster | 100,000 entities, 1 million rows | 50 events/s over 20,000 entities, 10 s ticks | about 30 million rows | about 1 GB a day |
| large | a production cluster the user is willing to load | the user's numbers | the user's numbers, 1 s ticks allowed | rate x TTL | as computed |

The tier caps what the data phase accepts without a warning, and it bounds the
engine choice in the features phase. It never raises a default on its own.

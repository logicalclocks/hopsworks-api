# Ingest: many tables in one job, Google Sheets, bronze tags

One table with a `sink_job` is in SKILL.md.

### Tag the ingested table as bronze

A table ingested raw is a bronze table of an analytics layer; tag it so the Factory offers it as a source for a silver layer (**hops-analytics**).
The UI's ingestion review has a "Tag as a bronze table" checkbox, off by default; from code, tag each feature group after it is created:

```python
fg.add_tag("analytics_table", {"layer": "bronze", "lifecycle": "dev"})
```

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
connector = fs.get_data_source("my_google_sheets_connector").storage_connector
```

**Ingest a sheet into a feature group**

Provide the sheet tab name via `DataSource.table`.
If the spreadsheet ID was not set on the connector, pass it via `DataSource.spreadsheet_id`.

```python
import hopsworks
from hsfs.core.data_source import DataSource

project = hopsworks.login()
fs = project.get_feature_store()

connector = fs.get_data_source("my_google_sheets_connector").storage_connector

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

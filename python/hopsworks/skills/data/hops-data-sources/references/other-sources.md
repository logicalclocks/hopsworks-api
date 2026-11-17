# Other sources: another Hopsworks cluster, SharePoint, Gmail, Outlook

## Mount a feature group from another Hopsworks cluster

A `hopsworks` data source reads a project on another Hopsworks cluster.
Its databases are the feature stores the remote project can read, its own `<project>_featurestore` and the ones shared with it, and its tables are their feature groups, named `<name>_<version>`.
External feature groups are read through the remote cluster's Hopsworks Query Service, and DLTHub ingestion copies one into a local feature group.

```python
ds = fs.get_data_source("my_remote_cluster")
table = [t for t in ds.get_tables("sales_featurestore") if t.table == "transactions_1"][0]
ext_fg = fs.create_external_feature_group(
    name="remote_transactions", version=1, data_source=table, primary_key=["id"]
)
ext_fg.save()
df = ext_fg.read()   # Python engine: via the remote Hopsworks Query Service
```

The remote cluster must have its Hopsworks Query Service reachable from this one, through its `feature_query` load balancer.
There is no SQL query and no Trino catalog for this data source type.

## Copy or sync SharePoint folders into a dataset

A SharePoint data source has no tables: it copies a folder of a document library into a new top-level dataset of the project, once or on a cron schedule.
With `index_files` a sync also keeps a Delta feature group with one row per file in the dataset; search it with **hops-sharepoint-search**.
Each sync is an ordinary Python job, `sharepoint_<connector>_<dataset>`, running the script `DataSources/<connector>/sharepoint_sync.py` that data owners can edit; `reset_script()` restores the template.
Creating, running and changing syncs needs the Data Owner role; members read the dataset and the audit log.
With mirror deletions (the default for a schedule) the dataset equals the folder after each run; files edited or deleted in Hopsworks are restored, and data never flows back to SharePoint.

### Set up SharePoint access first

Hopsworks reads SharePoint as a Microsoft Entra ID application (client credentials, no user sign-in), so a tenant administrator prepares:

- An app registration (single tenant, no redirect URI); note its **tenant id** and **client id**.
- A Microsoft Graph **application** permission with admin consent: `Sites.Read.All`, or `Sites.Selected` plus a read grant on the site (`POST /sites/{site-id}/permissions`).
- A client secret: copy its **Value**, not the Secret ID (`AADSTS7000215` means the wrong one was used).
- A SharePoint Online license on the tenant and the site URL, e.g. `https://contoso.sharepoint.com/sites/Finance`.
- Outbound HTTPS from the cluster to `login.microsoftonline.com`, `graph.microsoft.com` and `<tenant>.sharepoint.com` (other hosts for `US_GOV` and `CHINA`).

Check the credentials before creating anything with `hops datasource create sharepoint ... --test`, which lists the site's libraries or exits 1.
Step-by-step registration, the `Sites.Selected` grant commands and a troubleshooting table by error code are in [references/sharepoint-setup.md](references/sharepoint-setup.md).

```python
from hsfs.storage_connector import SharePointConnector

sc = fs.create_data_source(SharePointConnector(
    id=None, name="finance_sharepoint", featurestore_id=fs.id,
    tenant_id="...", client_id="...", client_secret="...",
    site_url="https://contoso.sharepoint.com/sites/Finance",
    cloud="GLOBAL",                     # default; GLOBAL | US_GOV | CHINA
)).storage_connector
sc.list_drives()                        # [{"id": "b!...", "name": "Documents", "web_url": ...}, ...]
sc.list_folders(drive_id="b!...", path="Reports/2026")

copy = sc.copy_folder(drive_id="b!...", folder_path="Reports/2026", dataset_name="reports_2026")
# = create_sync(..., schedule=None, mirror_deletions=False, run_now=True)

sync = sc.create_sync(
    drive_id="b!...",
    folder_path="Reports/2026",         # "" for the library root
    dataset_name="sharepoint",          # default; must not exist
    schedule="0 0 * ? * *",             # Quartz cron in UTC, seconds first (hourly); None = copy once
    include=["*.pdf", "*.xlsx"],
    exclude=["drafts/**"],
    max_file_size_mb=1024,
    mirror_deletions=True,
    alert_receiver="ops-slack",         # optional; FAILED and KILLED alerts on the sync job
    run_now=True,
    index_files=True,                   # optional; index feature group sharepoint_<dataset>, version 1
)
sync.status()      # dict: state, paused, scheduled, last_run_*, files_total, bytes_total, last_error, ...
sync.run()         # returns the Execution
sync.pause(); sync.resume()
sync.set_schedule("0 0 2 ? * *"); sync.set_schedule(None)
sync.force_full_resync()               # next run compares checksums
sync.set_folder("Reports/2027")        # re-point; next run reconciles
sync.update(index_files=False)         # stop indexing; the feature group is kept
sync.get_index_feature_group()         # FeatureGroup, after the first indexed run
sync.audit_path                        # ".../DataSources/finance_sharepoint/audit/sharepoint"
sync.get_audit(day=None)               # changed paths of a day (default: today, UTC)
sync.delete(keep_files=True)           # without keep_files the dataset is deleted; False = still DELETING, sync.retry() finishes it
sc.get_syncs()
```

```bash
hops datasource sharepoint drives <name>
hops datasource sharepoint folders <name> --drive DRIVE_ID [--path P]
hops datasource sharepoint copy <name> --drive DRIVE_ID [--folder P] [--dataset N]
hops datasource sharepoint sync create <name> --drive DRIVE_ID [--folder P] [--cron "0 0 * ? * *"] [--include G]... [--exclude G]... [--no-mirror-deletions] [--no-run-now] [--index-files] [--index-feature-group FG]
hops datasource sharepoint sync index <name> SYNC_ID --on|--off [--feature-group FG]
hops datasource sharepoint sync list|status|run|pause|resume|resync|schedule|set-folder|delete|audit <name> [SYNC_ID] ...
hops datasource sharepoint reset-script <name>
```

The index feature group is partitioned by `ingestion_date`, the day a file was first indexed, and keyed by the SharePoint item id; its columns are listed in **hops-sharepoint-search**.
Each run writes the rows of new, changed, renamed and removed files with delta-rs, and compacts each earlier day that holds more than one file into files of about 200 MB.
Deleting a sync deletes its index feature group unless `keep_files=True`.

The audit log is CSV in `date=YYYY-MM-DD` partitions (columns `execution_id, op, kind, path, src_path`), written only by runs that changed something and kept 7 days by default; read it with DuckDB `read_csv(..., hive_partitioning = true)` to prune by date.

## Copy a Gmail mailbox into a feature group

A `gmail` data source is a mailbox, read with an OAuth refresh token or a Workspace service account with domain-wide delegation, both with the `gmail.readonly` scope.
Its credentials are write-only; code that calls Gmail itself asks for a short-lived token with `get_access_token()`.
It is not browsed or mounted: a sync job upserts its emails into a Delta feature group named after the mailbox (`jane.doe@gmail.com` → `jane_doe_gmails`), partitioned by the `day` of the UTC receive time (an hourly job adds a file per run, and the first run after midnight compacts the finished day), and writes attachments as files whose paths the `attachments` column lists.

```bash
hops datasource create gmail support_inbox --mailbox support@example.com --auth-type OAUTH --client-id CID --client-secret - --refresh-token - < secrets.txt
hops datasource labels support_inbox    # label ids and names: INBOX, SENT, Label_12 (receipts), ...
hops datasource sync support_inbox --label INBOX --label SENT --start-date 2026-01-01 --schedule hourly
hops datasource sync-jobs support_inbox
```

Each `--label` (SDK `labels=[...]`) adds the messages carrying that label; without any label, every message except spam, trash and drafts is copied.

```python
fs.get_data_source("support_inbox").sync(query="-category:promotions", schedule="daily")
emails = fs.get_feature_group("support_examples", version=1)
```

`INCREMENTAL_TIMESTAMP` (the default) upserts messages received since the previous run, and its first run backfills from `initial_value`; `FULL_LOAD` copies every selected message on every run, then removes the rows and attachments of the messages it did not copy.
The copy is an archive: deletions and label changes in Gmail after a message was copied do not reach it until a full load.
A sync job is a Python job; schedule it `hourly` or `daily`, and monitor it like any job.
To search the emails, use **hops-email-search**.

## Copy an Outlook mailbox into a feature group

An `outlook` data source is a Microsoft 365 or Outlook.com mailbox, read through Microsoft Graph with an app registration in Microsoft Entra ID:
- `OAUTH`: a refresh token the mailbox owner granted with `Mail.Read offline_access`;
- `CLIENT_CREDENTIALS`: a client secret of an app with the `Mail.Read` application permission in the mailbox's tenant, which needs `--tenant-id`.

Its credentials are write-only, and Hopsworks stores the new refresh token Microsoft returns on each use.
It syncs exactly like Gmail, into a feature group with the same columns (`jane.doe@outlook.com` → `jane_doe_outlooks`), so **hops-email-search** works on it too.
It selects folders instead of labels and takes no query:

```bash
hops datasource create outlook support_outlook --mailbox support@contoso.com --auth-type CLIENT_CREDENTIALS --tenant-id contoso.com --client-id APP_ID --client-secret - < secret.txt
hops datasource labels support_outlook    # folder ids and paths: Inbox, Inbox/Projects, Sent Items, ...
hops datasource sync support_outlook --label <inbox folder id> --start-date 2026-01-01 --schedule hourly
```

Without `--label` (SDK `folders=[...]`), every folder but Deleted Items, Junk Email, Drafts and Outbox and the folders below them is copied; a chosen folder's subfolders need choosing too.
A message keeps its key when moved between folders, but its `labels` keep the folder it was in when copied.

# Google Drive and Google Analytics sources

These two are connected only from Hopsable's Connect card: the user signs in at Google and picks the files or the GA4 property there. Hopsworks then reads them itself on the account's Google grant; no Google token reaches a job, a notebook or this CLI. Neither has tables: each sync copies the source into a project dataset.

- Google Drive: the picked files, copied once into the sync's dataset.
- Google Analytics: one CSV per day, `traffic/date=<yyyy-mm-dd>.csv` (date, sessionDefaultChannelGroup, country, sessions, activeUsers, conversions), at once and daily; each run rewrites the last 4 days, since Google revises them. Dates follow the property's time zone.

```bash
hops datasource google-analytics sync list <source>          # the dataset and the state of each sync
hops datasource google-analytics sync status <source> <id>   # last run, last error, the reports and the date each is loaded up to
hops datasource google-analytics sync run <source> <id>      # load now
hops datasource google-analytics sync retry <source> <id>    # finish a sync whose create failed half way
hops datasource google-drive sync list <source>              # same commands for Drive
```

`hops datasource info <source>` names each sync's dataset. Read the files as an external feature group, which always sees the latest load: a HopsFS data source on that dataset, then the feature group on its folder (column names come out lower case).

```bash
hops datasource create hopsfs <source>_files --dataset <dataset>
hops fg create-external <name> --connector <source>_files --path traffic --data-format csv \
  --primary-key date,sessiondefaultchannelgroup,country
```

To look at one day, `hops files download <dataset>/traffic/date=<yyyy-mm-dd>.csv`: one file at a time, a folder download fails (error 110021). A Google Drive source with no sync has no files yet: the user picks them in Hopsable's Connect card, so ask for that instead of running a sync. Never run an ingestion job on these sources: Hopsworks refuses it. A FAILED run says why in `status`; "cannot read Google Analytics property" means the account that connected has no access, so the user connects again with one that has. New files or another property need a new connect in Hopsable; this CLI does not create syncs. `sync delete <source> <id> [--keep-files] --yes` removes a sync, and its dataset unless `--keep-files`.

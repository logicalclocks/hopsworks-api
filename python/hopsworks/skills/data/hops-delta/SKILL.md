---
name: hops-delta
description: Call the Delta maintenance methods on a feature group from the Python or PySpark client, and choose a write mode. Auto-invoke when the user mentions delta_optimize, delta_compact, delta_vacuum, delta_checkpoint, delta_cleanup_metadata, the _delta_log, Delta time travel, or append versus upsert on a feature group. For diagnosing small files and slow queries across table formats, hops-table-maintenance is the skill.
---

# Delta feature group maintenance

## Concept

A Delta feature group is a directory of Parquet files plus a `_delta_log/` of
JSON commits. Every write appends a commit; nothing rewrites anything unless
asked. So a table that is written often accumulates two things: data files, which
every reader opens, and log commits, which every reader replays.

Four operations bound that growth, and the order they run in matters:

**compact → checkpoint → cleanup metadata → vacuum**

1. `delta_optimize()` rewrites many small files into fewer large ones. The old
   files stay on disk, still referenced by older table versions.
2. `delta_checkpoint()` records the compacted file list, so readers stop
   replaying the log from commit zero. It goes before the deletions, so what
   comes next prunes towards a state that is already written down.
3. `delta_cleanup_metadata()` expires the log entries that checkpoint now
   covers. Never run it without a checkpoint: the checkpoint is what a reader
   falls back to once the individual commits are gone.
4. `delta_vacuum(retention_hours=...)` deletes the data files no version inside
   the retention window references. This is the step that reclaims space.

**Optimize without vacuum frees nothing** — it only adds files. **Optimize costs
you time travel**: once the small files are vacuumed you cannot read a version
that referenced them.

**Set the vacuum retention comfortably longer than your longest-running reader.**
A vacuum deletes files an in-flight query may still be reading. The retention
window, not the ordering, is what protects that query, and it is also your time
travel window. Pick it from how long your slowest job runs, not as small as
possible.

## Key facts / rules

- **Both engines are supported and the client picks one for you.** With a Spark
  session the operations run as `OPTIMIZE` / `VACUUM` / a forced `DeltaLog`
  checkpoint; without one they run through delta-rs. Behaviour is the same; the
  Spark path handles liquid-clustered tables, delta-rs does not.
- **delta-rs vacuum defaults to `dry_run=True`.** Calling the delta-rs API
  directly without `dry_run=False` returns the file list and deletes nothing.
  `fg.delta_vacuum()` already passes `dry_run=False`; that is the reason to use
  it rather than reaching for `DeltaTable` yourself.
- **A retention below `delta.deletedFileRetentionDuration` (7 days) is refused**
  by both engines unless the check is lifted. `fg.delta_vacuum()` lifts it,
  because the caller naming the hours is the one choosing that trade.
- **A vacuum straight after a compaction usually deletes nothing.** The files it
  just orphaned are seconds old and the retention window has not passed. On a
  daily schedule, each run reclaims what earlier runs orphaned. That is expected,
  not a failure, and it is the same property that keeps in-flight readers safe.
- **Spark writes checkpoints on its own** every `delta.checkpointInterval`
  commits (10 by default). **delta-rs writes none at all**, so a table only ever
  written by a Python job will replay its whole log forever until something
  checkpoints it.
- **`after_ingest_date` needs a date partition column.** Only a partition column
  can select files without reading them, so a date-bounded compaction is refused
  on a table not partitioned by a date.
- Compaction runs with `max_concurrent_tasks=1` by default so it can run beside
  a writer without taking the whole CPU budget. Raise it for a dedicated job.
  On PySpark these become session settings for the statement, since OPTIMIZE
  takes neither as syntax, and are restored afterwards.
- `delta_compact()` is the same call under the other name: Delta's SQL says
  OPTIMIZE and delta-rs says `optimize.compact`, so both words work.

## Commands / API

```python
fg = fs.get_feature_group("transactions", version=1)

# The maintenance sequence, in the order that makes each step safe.
fg.delta_optimize(max_concurrent_tasks=1)   # fewer, larger files
fg.delta_checkpoint()                       # record the compacted file list
fg.delta_cleanup_metadata()                 # expire the log it now covers
fg.delta_vacuum(retention_hours=24)         # delete; dry_run=False is set for you

# Compact only what is new, on a table partitioned by a date column.
fg.delta_optimize(after_ingest_date="2026-09-10")
```

A reasonable schedule for an append-heavy table: compact when the active file
count crosses a threshold (~100 files is the low hundreds of megabytes at
typical commit sizes, near the engine's own target file size), and otherwise once
a day. Read the last compaction time from the table's own history rather than
keeping state, so the schedule survives restarts and multiple writers.

Bound the daily one with `after_ingest_date`. Only files written since the last
compaction need rewriting, and on a date-partitioned table they are all at or
after that date, so passing it keeps the cost flat. Without it every run rewrites
the whole table, including everything earlier runs already compacted, and the
cost grows with the table forever. Give it a day of slack for rows that arrived
late.

## Layout: partitioning, clustering, and write mode

- **Favour `append` when you know the rows are new.** An append writes new files
  and reads none of the existing data, so its cost and memory are bounded by the
  batch, not by the table. An upsert or merge must read the overlapping
  partitions to join against them, which grows with the table. Pass
  `write_options={"mode": "append"}` when the rows cannot collide; that one
  option is the difference between a bounded write and a growing one.
- **Partition on a low-cardinality column you filter on**, most often a date.
  Partitioning by a high-cardinality id is the classic mistake: it produces one
  tiny file per value and makes every read worse.
- **A partition key and liquid clustering are mutually exclusive.** Delta has no
  partition transforms, so a date partition needs its own materialised date
  column.
- **Liquid clustering** (`clustered_by`) gives locality without fixed
  directories and is the better default when queries filter on several columns
  or the key would be too granular to partition on. It requires Spark for every
  offline write: delta-rs does not implement the clustering writer features and
  refuses the table.
- Partitioning by something that never changes, such as a single model name,
  gives you one partition that grows forever. That still compacts, but nothing
  ages out of it; add a date column if you want old data to become inert.

## Docs

- [Feature group time travel and formats](https://docs.hopsworks.ai/latest/user_guides/fs/feature_group/)
- [Delta Lake OPTIMIZE and VACUUM](https://docs.delta.io/latest/optimizations-oss.html)

## Related skills

- [hops-table-maintenance](../hops-table-maintenance/SKILL.md) — evidence-driven
  diagnosis and a reviewable plan across Delta, Iceberg and Hudi, when you do not
  yet know which tables need work or which layout to choose.
- [hops-spark](../hops-spark/SKILL.md) — the Spark session these operations use
  when one is available.

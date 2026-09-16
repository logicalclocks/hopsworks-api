---
name: hops-delta
description: Maintain Delta feature groups from the Python or PySpark client — optimize, vacuum, checkpoint — and choose a write mode and table layout. Auto-invoke when the user mentions delta_optimize, delta_vacuum, delta_checkpoint, compaction, small files, the _delta_log, time travel, liquid clustering, partitioning a feature group, or append versus upsert.
---

# Delta feature group maintenance

## Concept

A Delta feature group is a directory of Parquet files plus a `_delta_log/` of
JSON commits. Every write appends a commit; nothing rewrites anything unless
asked. So a table that is written often accumulates two things: data files, which
every reader opens, and log commits, which every reader replays.

Three operations bound that growth, and they are only useful in this order:

1. `delta_optimize()` rewrites many small files into fewer large ones. The old
   files stay on disk, still referenced by older table versions.
2. `delta_vacuum(retention_hours=...)` deletes files no longer referenced by any
   version inside the retention window. This is what actually reclaims space.
3. `delta_checkpoint()` writes a checkpoint of the current file list, so readers
   stop replaying the log from commit zero.

**Optimize without vacuum frees nothing** — it adds files. **Optimize costs you
time travel**: once the small files are vacuumed you cannot read a version that
referenced them, so pick `retention_hours` as the time-travel window you are
willing to keep, not as small as possible.

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
  daily schedule, each run reclaims what the previous run orphaned. That is
  expected, not a failure.
- **Spark writes checkpoints on its own** every `delta.checkpointInterval`
  commits (10 by default). **delta-rs writes none at all**, so a table only ever
  written by a Python job will replay its whole log forever until something
  checkpoints it.
- **`after_ingest_date` needs a date partition column.** Only a partition column
  can select files without reading them, so a date-bounded compaction is refused
  on a table not partitioned by a date.
- Compaction runs with `max_concurrent_tasks=1` by default so it can run beside
  a writer without taking the whole CPU budget. Raise it for a dedicated job.

## Commands / API

```python
fg = fs.get_feature_group("transactions", version=1)

# Compact everything, then reclaim, then checkpoint. This order matters.
fg.delta_optimize(max_concurrent_tasks=1)
fg.delta_vacuum(retention_hours=24)      # deletes; dry_run=False is set for you
fg.delta_checkpoint()                    # readers stop replaying the log

# Compact only what is new, on a table partitioned by a date column.
fg.delta_optimize(after_ingest_date="2026-09-10")
```

A reasonable schedule for an append-heavy table: compact when the active file
count crosses a threshold (~100 files is the low hundreds of megabytes at
typical commit sizes, near the engine's own target file size), and otherwise once
a day. Read the last compaction time from the table's own history rather than
keeping state, so the schedule survives restarts and multiple writers.

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

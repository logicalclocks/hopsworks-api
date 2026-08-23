---
name: hops-table-maintenance
description: Diagnose and fix table-layout problems in offline feature groups (Delta, Iceberg, Hudi). Auto-invoke when the user mentions small files, slow feature group queries, compaction, OPTIMIZE, ZORDER, clustering, rewrite_data_files, snapshot cleanup, VACUUM, or table maintenance.
---

# Offline feature group table maintenance

Act as a table-optimization agent for the offline feature store: collect
evidence deterministically, diagnose layout problems, produce a plan with
estimated benefit and rewrite cost, generate Spark SQL, run it dry-run or
partition-limited first, then verify. Never ask an LLM (yourself) to "look at
the table and optimize it": the scanner computes the evidence, you interpret
it and generate a controlled, native maintenance program.

## Contract
- **Input:** one or more feature groups (or "the worst tables in this project").
- **Output:** an evidence report, a reviewable plan YAML, the executed Spark
  maintenance (after approval), and a before/after verification table.
- **Pre-condition:** a terminal in the project. Offline tables are browsable
  read-only at `/hopsfs/featurestore/<project>_featurestore.db/<fg>_<version>`.

## Phase 0: ask before scanning

Use AskUserQuestion to settle four things up front:

1. **Which tables** — a named feature group, the largest N by size (list them
   with `ls -s` on the mounted db dir), or every offline feature group.
2. **When** — run once now in this terminal, or install as a scheduled
   Hopsworks job (see "Scheduling" below). Maintenance competes with active
   writers; ask when the table's ingestion is quiet.
3. **How far** — compaction only; compaction plus metadata cleanup (manifests,
   checkpoints); or full layout review (partitioning, clustering, sort order).
4. **Approval mode** — every destructive action needs explicit approval per
   action (default), or the user pre-approves the non-destructive subset.

## Phase 1: collect evidence

Resolve the table format first: `_delta_log/` means Delta, `metadata/` with
`*.metadata.json` means Iceberg, `.hoodie/` means Hudi. The feature group's
`time_travel_format` says the same thing through the API.

Run the bundled scanner (install its deps once with
`uv pip install deltalake duckdb`; add `pyiceberg` for Iceberg tables):

```bash
python3 ~/.claude/skills/hops-table-maintenance/scripts/lakehouse_doctor.py analyze \
  /hopsfs/featurestore/<project>_featurestore.db/<fg>_<version> \
  --output /tmp/evidence_<fg>.json
```

It emits evidence JSON: file count and size histogram, small-file ratio,
oversized files, bytes and files per partition, partition skew and
cardinality, min/max overlap per column (how badly files interleave for a
filter on that column), snapshot and commit accumulation, and format
specifics (Iceberg delete-file and manifest counts, Hudi log-file ratio).

**Workload evidence is not optional.** File statistics alone cannot choose a
clustering or partition key; without access patterns you will produce
plausible but harmful recommendations (partitioning by a high-cardinality id
is the classic one). Gather it from:
- The project's feature views: their join keys and filters are declared
  workload. `hops fv list` and `hops fv get <name>` show them.
- The feature group's `event_time` column: time filters dominate training
  data reads, so it is the default sort/cluster candidate.
- The user: AskUserQuestion for the columns their queries filter and join on,
  and roughly how often.

Pass what you learn to the planner as `--filter-columns`; it refuses to
recommend clustering without it.

For deeper one-off analysis DuckDB is the right tool (analysis only, never
maintenance): `SELECT * FROM delta_scan('<table path>')` samples content,
`parquet_metadata()` inspects row groups, and SQL over the evidence JSON
tests whether a proposed clustering column would actually prune.

## Phase 2: plan

```bash
python3 ~/.claude/skills/hops-table-maintenance/scripts/lakehouse_doctor.py plan \
  /tmp/evidence_<fg>.json --filter-columns event_time,merchant_id
```

The planner applies these rules and emits plan YAML with per-action
confidence, estimated rewrite volume, and a `requires_approval` flag:

| Finding | Recommendation |
| --- | --- |
| Many files below 32 to 64 MB | Compact toward 256 to 512 MB |
| Extremely uneven partition sizes | Change partition transform or cluster |
| Thousands of tiny partitions | Coarsen time partitioning |
| Scans read nearly all files for equality predicates | Sort or cluster on the predicate columns |
| Too many Iceberg delete files | Rewrite data and delete files |
| Excessive Iceberg manifests | Rewrite manifests |
| Many old snapshots | Expire snapshots (approval required) |
| Repeated ingestion creates small files | Enable optimized writes or auto-compaction |
| High-cardinality partition key | Replace with bucket transform or clustering |

Show the user the plan before doing anything. Note that these formats have no
B-tree indexes: Delta prunes with file statistics, Z-ordering and liquid
clustering; Iceberg with partition transforms, sort order and manifest
statistics. "Add an index" is never the recommendation.

## Phase 3: guardrails

Never run these on your own recommendation, only after the user approves the
specific action in this session:

- `VACUUM` (Delta) and Iceberg orphan-file deletion
- Snapshot expiration
- Full-table rewrites and destructive partition migration
- Altering clustering or sort order on a production table
- Any maintenance concurrent with an active large writer

Two absolute rules: **never write through the `/hopsfs/featurestore` mount**
(it is a read window; rewriting parquet under a Delta, Iceberg or Hudi table
bypasses the transaction protocol and corrupts it), and **always scope the
first run** to a recent partition range or dry-run mode, verify, then widen.

## Phase 4: execute with Spark

Maintenance goes through Spark and the native table APIs, in a SPARK terminal
session or a Hopsworks Spark job. The table identifier is
`<project>_featurestore.<fg>_<version>`.

Delta:
```sql
-- Scoped compaction first, widen after verification
OPTIMIZE <project>_featurestore.<fg>_1
WHERE event_date >= current_date() - INTERVAL 7 DAYS;

-- With clustering, only when workload evidence supports the columns
OPTIMIZE <project>_featurestore.<fg>_1
WHERE event_date >= current_date() - INTERVAL 7 DAYS
ZORDER BY (merchant_id, card_id);
```

Iceberg (partition-spec changes are metadata-only and agent-friendly; they do
not rewrite existing files):
```sql
CALL spark_catalog.system.rewrite_data_files(
  table => '<project>_featurestore.<fg>_1',
  strategy => 'sort',
  sort_order => 'event_time ASC, merchant_id ASC',
  options => map('target-file-size-bytes', '536870912', 'min-input-files', '5')
);
CALL spark_catalog.system.rewrite_manifests(table => '<project>_featurestore.<fg>_1');
```

Hudi:
```sql
CALL run_clustering(table => '<project>_featurestore.<fg>_1', order => 'event_time');
CALL run_compaction(op => 'schedule', table => '<project>_featurestore.<fg>_1');
```

## Phase 5: verify

Re-run `analyze` on the same table and present before/after: file count,
median file size, small-file ratio, partition skew, snapshot count. Stop and
report rather than continue when the observed benefit is below what the plan
estimated; do not widen the scope of a rewrite that did not pay for itself.

## Scheduling

To make this recurring, create a scheduled Hopsworks job (see hops-agent-job)
that runs `analyze` plus `plan` and writes the report to the project; keep
`apply` interactive so destructive actions always pass a human. A PYTHON job
with this skill's script uploaded to `Resources/` is enough for the report.

## Engine note

The scanner is a self-contained Python fallback shaped after Adobe's
Lake Pulse health report (file organization, compaction opportunities,
partition statistics, snapshot history, storage efficiency). Lake Pulse is a
Rust library, not a CLI (`lake-pulse = "0.3"`, Hudi behind a feature flag),
so it cannot be invoked from the terminal directly; if a `lakehouse-doctor`
binary is ever on PATH, prefer it over the bundled script. Its evidence JSON
follows the same section names so the swap is transparent to the plan phase.

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
confidence and estimated rewrite volume. Every action that rewrites files
carries `requires_approval: true`; the only thing that shrinks the asking is
the user pre-approving the non-destructive subset in Phase 0.

| Finding | Recommendation |
| --- | --- |
| Many files below 32 to 64 MB | Compact toward 256 to 512 MB |
| Extremely uneven partition sizes | Change partition transform or cluster |
| Thousands of tiny partitions | Coarsen time partitioning |
| Scans read nearly all files for equality predicates | Sort or cluster on the predicate columns |
| Too many Iceberg delete files | Rewrite data and delete files (scoped) |
| Excessive Iceberg manifests | Rewrite manifests |
| Many old snapshots | Expire snapshots (approval required) |
| Repeated ingestion creates small files | Enable optimized writes or auto-compaction |
| High-cardinality partition key | Replace with bucket transform or clustering |

Show the user the plan before doing anything. Note that these formats have no
B-tree indexes: Delta prunes with file statistics, Z-ordering and liquid
clustering; Iceberg with partition transforms, sort order and manifest
statistics. "Add an index" is never the recommendation.

## Phase 3: guardrails

Nothing that rewrites files runs on the planner's word alone: every action in
the plan needs the user's approval in this session (or their Phase 0
pre-approval of the non-destructive subset). These need explicit per-action
approval even then:

- `VACUUM` / `delta_vacuum` (Delta) and Iceberg orphan-file deletion
- Snapshot expiration
- Full-table rewrites (`full=True`, `rewrite_all=True`) and destructive
  partition migration
- Altering clustering or sort order on a production table
- Any maintenance concurrent with an active large writer

Two absolute rules: **never write through the `/hopsfs/featurestore` mount**
(it is a read window; rewriting parquet under a Delta, Iceberg or Hudi table
bypasses the transaction protocol and corrupts it), and **always scope the
first run** with a `where` predicate on a recent partition range, verify,
then widen. The API defaults already lean safe: `optimize()` is incremental
on Delta and skips below-threshold file groups on Iceberg unless told
otherwise.

## Phase 4: execute through the Hopsworks API

The supported path is [`FeatureGroup.optimize()`](../../hops/hops-fg/SKILL.md):
it validates the format, follows the feature group's stored layout
(clustering, sort order, z-order columns), and defaults to the safe rewrite
(incremental `OPTIMIZE` on Delta; Iceberg `rewriteDataFiles` with
`rewrite_all=False`, so a routine call never rewrites the whole table by
accident). Run it in a SPARK terminal session or a Hopsworks Spark job;
clustered Delta and all Iceberg rewrites require the Spark engine.

```python
fg = fs.get_feature_group("<fg>", version=1)

# Scoped compaction first, widen after verification
metrics = fg.optimize(where="event_date >= '2026-08-14'")

# Layout rewrite, only when workload evidence supports the columns and the
# user approved it: zorder over the evidence columns
metrics = fg.optimize(strategy="zorder", columns=["merchant_id", "card_id"],
                      where="event_date >= '2026-08-14'")

# Full rewrites are the explicitly-approved exception, never the default
# fg.optimize(full=True)          # Delta: recluster everything
# fg.optimize(rewrite_all=True)   # Iceberg: rewrite every file
```

Raw Spark procedures are only for what `optimize()` does not cover, still
scoped and still approved. Iceberg manifests and snapshot expiry:

```sql
CALL spark_catalog.system.rewrite_manifests(table => '<project>_featurestore.<fg>_1');
-- destructive, removes time travel; explicit approval required
-- CALL spark_catalog.system.expire_snapshots(table => '<project>_featurestore.<fg>_1', ...);
```

Iceberg partition-spec changes are metadata-only (existing files are not
rewritten), so evolving the spec is cheap; the rewrite that applies it to old
data is the expensive, approval-gated part.

Hudi: do not run ad hoc maintenance procedures. Hopsworks runs Hudi layout
maintenance through inline clustering on writes, and `optimize()` rejects
HUDI feature groups for exactly that reason. When the evidence shows a poor
Hudi layout, the recommendation is to review the feature group's write
configuration, not to script `run_clustering`/`run_compaction` yourself.

## Phase 5: verify

Re-run `analyze` on the same table and present before/after: file count,
median file size, small-file ratio, partition skew, snapshot count. Stop and
report rather than continue when the observed benefit is below what the plan
estimated; do not widen the scope of a rewrite that did not pay for itself.

## Scheduling

To make this recurring, create a scheduled Hopsworks job (see hops-agent-task)
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

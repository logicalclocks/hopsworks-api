---
name: hops-fg
description: Use when writing Python code that creates, inserts into, or manages tables or feature groups. Auto-invoke when user writes feature pipelines, feature engineering code, or asks about feature group best practices (online vs offline, batching, OOM, materialization, embeddings, statistics).
---

When a user refers to tables, clarify that you interpret them as feature groups in Hopsworks.

# Hopsworks Feature Groups — Python SDK Best Practices

Writes computed features into a Hopsworks feature group — the storage backing the F (feature) stage of the FTI pattern.

A feature pipeline applies **model-independent transformations (MITs)** and writes the resulting **untransformed, reusable** feature data to feature groups. Do NOT store model-dependent transformations (MDTs — e.g. scaling, one-hot encoding) in a feature group: those are applied later, in the feature view, when reading for training/inference. Storing MDT output makes the data non-reusable across models, can cause write amplification (a parameterized MDT like standardization rewrites every existing row), and breaks EDA on raw values. Reuse is the payoff: the lowest-cost feature pipeline is the one you don't have to create, so write features other models can also select.

## Contract

- **Input:** a DataFrame (Pandas/Polars/PySpark) of computed features, plus the target name/version and key columns.
- **Output:** a feature group registered server-side (on first insert) and populated with rows; optionally online-enabled and materialized to the offline store.
- **Pre-condition:** a Hopsworks project login and a feature store handle (`fs = project.get_feature_store()`); any parent FGs used for provenance already exist.

## Smoke-test (cheap pre/post-flight)

Before writing Python, and to confirm results after, use the CLI. No Spark session needed:

```bash
hops fg list                              # is the name/version free? did it register? (note the STORE column)
hops fg info <name> --version 1           # metadata: id, online flag, primary key, event_time
hops fg features <name> --version 1       # schema with primary-key / partition flags
hops fg preview <name> --version 1 --n 10 # first rows (flag is --n, not -n)
hops fg preview <name> --columns a,b,c    # project away wide embedding/array columns
hops fg stats <name> --version 1          # null counts / ranges — spot bad data early
```

To preview an FG in a shared store from the CLI, pass `--featurestore <store>`
(the STORE value from `hops fg list`).

`hops fg list` shows a **STORE** column. Imported / public feature groups live in a
**shared** store, not this project's own — that distinction matters when you read
them (see "Reading from a shared store" below).

A feature group is registered server-side on its **first insert**, not at `get_or_create_feature_group(...)`. Until the first insert `fg.id` is `None` and `hops fg list` will not show it.

## Ask the user (before writing a feature pipeline)

Before creating a feature group, clarify these decisions with the user:

1. **Online or offline?**
   - **Offline only** (`online_enabled=False`, `stream=False`): writes directly to Delta Lake. Best for batch training data, historical analytics, large-volume cold storage.
   - **Online + offline** (`online_enabled=True`, `stream=True`): writes go to Kafka → RonDB (online), then a Spark materialization job copies to Delta (offline). Required for low-latency serving, real-time feature lookups, and feature views with `get_feature_vector()`.
   - Default to offline unless the user needs online serving.

2. **Does this FG derive from other FGs?** If so, pass `parents=[fg1, fg2, ...]` at creation time. This sets up explicit provenance/lineage tracking in the Hopsworks UI. Always pass the actual FeatureGroup objects, not names.

3. **Data volume** — estimate row count × column count × avg bytes per value. This drives decisions on batching, statistics, and materialization (see below).

4. **Time-series or not?** If features change over time, set `event_time` to the timestamp when the feature value was *valid* (not when the row was ingested). This is what lets the feature store build point-in-time correct training data (no future leakage, no stale values) via the feature view. Omit `event_time` only for immutable feature data.

---

## Feature data types & online-store constraints

Pick a supported type up front: a write with an unsupported type fails, and retrying the same type just loops.

**Supported feature types:**
- Scalars: `int`, `bigint`, `float`, `double`, `boolean`, `string`, `date`, `timestamp`, `binary`.
- Composite: `array<type>` and `struct<field:type,...>` — e.g. `array<float>`, `struct<lat:double,lon:double>`.
- `decimal` is **not** supported. Use `double`, or `string` when you need exact precision.

**Online store (`online_enabled=True`):**
- Scalars map straight to RonDB. Strings become `varchar(n)`, auto-sized to the longest value seen (rounded up to 100) and widened on later inserts; very long text falls back to `text`.
- Composite types (`array`, `struct`) **do** write online — they are stored Avro-encoded and decoded by the SDK on read. An online FG with an `array<float>` column is fine; you do not need to drop or flatten it.
- For **similarity search**, declare the vector as `array<float>` **and** attach an `EmbeddingIndex` (see Vector Embeddings): the FG is then backed by the vector DB (OpenSearch) instead of RonDB. Without an embedding index an `array<float>` is stored data, not a searchable index.
- Online is an upsert: one row per primary key, a new write for an existing key overwrites it.

Let the schema be inferred from the DataFrame when you can; pass an explicit `features=[Feature(name, type, ...)]` list only to pin a type (e.g. `bigint` over an inferred `int`, or an `array<float>` embedding column).

---

## Creating a Feature Group

```python
import hopsworks
from hsfs.feature import Feature

project = hopsworks.login()
fs = project.get_feature_store()

# Get parent FGs for provenance (if this is a derived FG)
parent_fg = fs.get_feature_group("source_table", version=1)

fg = fs.get_or_create_feature_group(
    name="my_feature_group",
    version=1,
    description="Clear description of what this FG contains",
    primary_key=["id_col"],
    event_time="event_ts",             # enables time-travel queries
    features=[                         # explicit schema (recommended)
        Feature("id_col", "bigint", description="..."),
        Feature("amount", "double", description="..."),
    ],
    online_enabled=True,               # True for online serving
    stream=True,                       # True when online_enabled=True
    parents=[parent_fg],               # provenance lineage
    statistics_config=False,           # see "Statistics" section below
    # offline_backfill_every_hr=4,     # see "Materialization" section below
)
```

**Always describe what you create.** Pass `description=` on the feature group and a `description=` on every `Feature(...)`. A feature group or column with no description shows as an empty envelope in the UI and is not discoverable in search. If the user gave none, write concise ones from what each feature means; never leave them blank.

### Key Parameters

| Parameter | When to use |
|---|---|
| `online_enabled=True` | Need online feature serving or real-time lookups |
| `stream=True` | Always set when `online_enabled=True`; enables unified write API |
| `parents=[...]` | FG is derived from other FGs — pass list of parent FG objects |
| `statistics_config=False` | Large data volumes (see Statistics section) |
| `event_time="col"` | Need time-travel queries or incremental reads |
| `embedding_index=EmbeddingIndex(...)` | FG contains vector embeddings (see Embeddings section) |
| `offline_backfill_every_hr=N` | Schedule automatic materialization every N hours |
| `ttl=timedelta(days=30)` | Auto-expire old rows from online store |
| `time_travel_format="ICEBERG"` | Offline table format: `"DELTA"` (default), `"ICEBERG"`, `"HUDI"`, or `None` |
| `partitioned_by=["day(event_ts)"]` | Native partition transforms (ICEBERG / HUDI only; see below) |
| `partition_key=["col"]` | Hive-style partitioning on real columns (any format) |
| `clustered_by=["a","b"]` | Delta liquid clustering, max 4 columns (DELTA only; see below) |
| `zorder_by=["a","b"]` | Z-order clustering, max 4 columns (ICEBERG / HUDI only) |
| `sort_order=["a asc","b desc"]` | Persistent Iceberg write sort order (ICEBERG only) |
| `bucket_index={"field":"id","num_buckets":16}` | Hudi bucket index (HUDI only) |

---

## Table format and layout

The offline table is a lakehouse table whose format is chosen at creation with
`time_travel_format`: `"DELTA"` (default), `"ICEBERG"` (needs `pyiceberg`),
`"HUDI"` (what online/`stream=True` FGs use), or `None`. The layout knobs are
**format-specific and not interchangeable**; the backend rejects the wrong
combination:

| Knob | DELTA | ICEBERG | HUDI | Note |
|---|---|---|---|---|
| `partition_key=["col"]` | ✅ | ✅ | ✅ | hive-style partitions on real columns |
| `partitioned_by=["day(ts)"]` | ❌ rejected | ✅ hidden partitioning, no new columns | ✅ grain columns materialized | transforms: `identity`, `bucket(N,c)`, `truncate(W,c)`, `year/month/week/day/hour(c)`, `void`; the bare-grain form `["year","month"]` was removed; requires `event_time` and a non-stream FG |
| `clustered_by=[...]` | ✅ liquid clustering (≤4 cols) | ❌ | ❌ | excludes `partition_key`; **Spark-only writes** (delta-rs cannot write clustered tables), so pass `stream=True` from Python |
| `zorder_by=[...]` | ❌ (use `clustered_by`) | ✅ via `fg.optimize()` | ✅ inline on write | ≤4 columns |
| `sort_order=[...]` | ❌ | ✅ | ❌ | excludes `zorder_by` |
| `fg.optimize()` | `OPTIMIZE` (incremental; `full=True` reclusters) | `rewriteDataFiles` (`rewrite_all=True` for the first pass after a backfill) | ❌ rejected | **needs Spark** for clustered Delta and all Iceberg |

Rules that bite:

- **Iceberg hidden partitioning is invisible in the schema.** `hops fg features` shows a blank PARTITION column and the UI no partition key even though the table *is* partitioned. Check `fg.get_partition_spec()`, not the column list.
- **The whole Iceberg write path needs a JVM** (PyIceberg reaches HopsFS through JNI `libhdfs`), including the empty first commit, so an Iceberg feature pipeline is a **PYSPARK job**, not a Python one. Spark Connect (the terminal) cannot create Iceberg tables or run Delta upserts either; see **hops-spark**.
- **Delta cannot partition and liquid-cluster at once.** Either derive a day column and cluster on both, or switch to Iceberg (`partitioned_by` + `zorder_by`). Say which trade-off you took.

Grammar, per-format behaviour, inspection (`get_partition_spec`, `get_clustering_columns`, `update_clustering`), and the shuffle-manager caveat: [references/table-layout.md](references/table-layout.md). Diagnosing an existing table: **hops-table-maintenance**.

---

## Inserting Data

### Simple Insert (default for most cases)

```python
fg.insert(df, wait=False)  # async — returns immediately
```

- Accepts: Pandas DataFrame, Polars DataFrame, PySpark DataFrame, NumPy array, or Python list
- `wait=False` (default): returns immediately; ingestion runs in background
- `wait=True`: blocks until online ingestion AND offline materialization complete

### When to Use `wait=True`

Use `insert(df, wait=True)` when:
- **Low on CPU/memory**: `wait=True` for online FGs ensures only one Spark materialization job runs at a time. Multiple concurrent async inserts can each launch a Spark job, exhausting cluster resources.
- **Pipeline ordering matters**: downstream steps depend on data being fully materialized.
- **Debugging insert failures**: async mode may silently swallow errors.

### Batch / Multi-Part Insert (for large datasets)

When inserting many small batches (e.g., streaming or chunked processing), use multi-part insert to avoid overhead per batch:

**Pattern 1 — Context manager (preferred):**
```python
with fg.multi_part_insert() as writer:
    for batch_df in data_batches:
        writer.insert(batch_df)
# Automatically finalized when context exits
```

**Pattern 2 — Manual:**
```python
for batch_df in data_batches:
    fg.multi_part_insert(batch_df)

fg.finalize_multi_part_insert()  # blocking — waits for all rows to transmit
```

After finalizing, trigger materialization manually (see Materialization section).

---

## Memory / OOM Prevention

Before writing a feature pipeline, estimate whether the data fits in RAM:

```
Memory ≈ rows × columns × avg_bytes_per_value × overhead_factor
```

- Numeric (int/float): ~8 bytes; string: ~50-200 bytes (varies)
- Overhead factor: ~2-3x (Polars/Pandas internal bookkeeping, intermediate results)

**Example:** 1M rows × 40 columns × 8 bytes × 3 ≈ 960 MB — fits in most environments.

If the data does not fit, or a pipeline OOMs (usually from reading every source FG at once, or rolling-window intermediates):

1. **Read in slices** by event time: `fg.read(start_time="2026-01-01", end_time="2026-02-01", dataframe_type="polars")`, and only the columns you need (a feature view `select()`, or `hops fg preview --columns` to check).
2. **Read one source FG at a time**, compute, `del` it before reading the next; do not keep copies of large frames.
3. **Insert in batches** with multi-part insert so the full output is never held in memory.
4. **Prefer Polars over Pandas** (columnar, zero-copy, lazy).
5. **Switch to PySpark** for very large data; it spills to disk (**hops-spark**).

---

## Statistics

By default, Hopsworks computes descriptive statistics on every insert. For large data volumes (GBs+), this adds significant overhead.

### Disable Statistics at Creation Time

```python
fg = fs.get_or_create_feature_group(
    name="large_fg",
    version=1,
    statistics_config=False,    # disables all statistics computation
    ...
)
```

### Selective Statistics

If you want some statistics but not full computation:

```python
from hsfs.statistics_config import StatisticsConfig

fg = fs.get_or_create_feature_group(
    name="my_fg",
    version=1,
    statistics_config=StatisticsConfig(
        enabled=True,
        correlations=False,     # skip correlation matrix
        histograms=False,       # skip histograms
        exact_uniqueness=False, # skip uniqueness/entropy
        columns=["col1", "col2"],  # only compute for these columns
    ),
    ...
)
```

**Guidance to user:** When data volume is large (> a few GBs), inform the user that statistics are disabled by default for performance and ask if they want to enable them. Statistics are useful for data quality monitoring but expensive to compute at scale.

---

## Materialization (Online → Offline)

For online FGs (`online_enabled=True, stream=True`), data written to Kafka/RonDB must be materialized to the offline store (Delta Lake) via a Spark job.

### Do NOT Start Materialization on Every Insert

If you are doing multiple inserts (e.g., batch pipeline, multi-part insert, iterative processing), **do not trigger a materialization job after each insert**. Each materialization launches a Spark job which consumes cluster resources. Instead:

**Pattern: Materialize once after all inserts complete:**
```python
# Do all inserts first
fg.insert(batch_1, wait=False)
fg.insert(batch_2, wait=False)
fg.insert(batch_3, wait=False)

# Then materialize once
fg.materialization_job.run(await_termination=True)
```

**Pattern: Schedule automatic materialization:**
```python
fg = fs.get_or_create_feature_group(
    name="my_fg",
    version=1,
    offline_backfill_every_hr=4,  # materialize every 4 hours
    ...
)
```

You can also pass a cron expression string to `offline_backfill_every_hr` for more control.

**Pattern: Check schedule:**
```python
schedule = fg.offline_backfill_every_hr  # returns cron expression or int
job = fg.materialization_job
print(job.job_schedule)  # full schedule details
```

### When to Use `await_termination=True` vs `False`

- `fg.materialization_job.run(await_termination=True)`: blocks until Spark job completes. Use when downstream steps need the offline data.
- `fg.materialization_job.run(await_termination=False)`: fires and forgets. Use when you just need the job scheduled.

---

## Vector Embeddings

A feature group with an `embedding_index` is backed by the **vector database**
(OpenSearch), not RonDB. Vector DBs are optimized for read-heavy similarity
search, not frequent updates, and every update to ANY feature in the FG triggers
a vector DB write (re-indexing). So keep embedding FGs **minimal**: the primary
key, the vector, and only static or rarely-changing metadata. Put
frequently-updated features (real-time counters, scores, `last_login`) in a
**separate** RonDB-backed online FG with the same primary key, and join them in
the feature view.

```python
from hsfs.embedding import EmbeddingIndex, EmbeddingFeature, SimilarityFunctionType

embedding_index = EmbeddingIndex()
embedding_index.add_embedding(
    name="text_embedding",
    dimension=384,
    similarity_function_type=SimilarityFunctionType.COSINE,  # or L2, DOT_PRODUCT
)

fg = fs.get_or_create_feature_group(
    name="document_embeddings",
    version=1,
    embedding_index=embedding_index,
    primary_key=["doc_id"],
    features=[
        Feature("doc_id", "bigint"),
        Feature("text_embedding", "array<float>"),
        # as few other features as possible, and only ones that rarely change
    ],
    online_enabled=True,
    stream=True,
)
```

---

## Reading Feature Groups

### Basic Read

```python
# Full read (offline store) — specify dataframe_type to get Polars, Pandas, etc.
df = fg.read(dataframe_type="polars")         # or "pandas", "spark", "numpy"

# Online store read
df = fg.read(online=True, dataframe_type="polars")
```

### Reading from a shared store

An FG in a **shared** store (the STORE column from `hops fg list` — e.g. imported
public tables) is NOT reachable through this project's default feature store
handle: `project.get_feature_store().get_feature_group(name, version=1)` returns
`None` for it. Pass the store name explicitly:

```python
shared_fs = project.get_feature_store(name="<that_store>")   # the STORE value
fg = shared_fs.get_feature_group("<name>", version=1)
df = fg.read(dataframe_type="polars")
```

In a job environment `fs.get_feature_groups()` / `get_all()` may be absent, so
resolve the shared FG by store name as above rather than enumerating.

### Time-Filtered Read

Read a slice by event time (requires `event_time` set on the FG):

```python
df = fg.read(start_time="2026-01-01", end_time="2026-03-01", dataframe_type="polars")
```

### Point-in-Time Read (Time Travel)

```python
df = fg.read(wallclock_time="2026-01-15", dataframe_type="polars")
```

### Filtered Read

Apply filters before reading to push predicates down and reduce data transfer:

```python
# Single filter
df = fg.filter(fg.amount > 100).read(dataframe_type="polars")

# Combined filters
df = fg.filter((fg.amount > 100) & (fg.status == "active")).read(dataframe_type="polars")
```

### Preview Rows

Quick preview without reading the entire FG:

```python
print(fg.show(n=10))  # show() RETURNS a DataFrame, it does not print — wrap in print() in scripts
```

### Similarity Search (Embedding FGs)

For FGs with an `embedding_index`, find nearest neighbors with optional filters:

```python
# Basic nearest neighbor search
results = fg.find_neighbors([0.1, 0.2, 0.3], k=5)

# With filters applied to the search
results = fg.find_neighbors(
    [0.1, 0.2, 0.3],
    k=5,
    filter=(fg.id1 > 10) & (fg.id1 < 30),
)
```

---

## Deleting Rows from a Feature Group

`fg.remove_rows(df)` deletes the rows matching the DataFrame's keys. Both stores
of an online-enabled FG are affected by default; `storage="offline"` /
`storage="online"` deletes from one store only, and a single-store delete is
never reconciled later.

```python
import polars as pl

# primary_key column(s) + event_time (+ partition columns, if any)
rows_to_delete = pl.DataFrame({
    "trans_id": [101, 202, 303],
    "event_ts": ["2026-01-01", "2026-01-02", "2026-01-03"],
})

fg.remove_rows(rows_to_delete)
```

- **Offline (Delta) with an `event_time`:** the merge key is the primary key **plus** `event_time` (plus partition columns). A primary-key-only DataFrame fails with `DeltaError: No field named <event_time>`.
- **Online:** matches on the primary key alone; the other columns are still required for the offline merge but do not affect which online rows go.
- **Stream FGs** (DELTA, ICEBERG, HUDI all supported): inserts reach the offline table through the materialization job while the delete hits it directly. Deleting a row whose insert is not yet materialized removes it online and the job then writes it offline; run the materialization job first, or re-run the delete after it.
- **Embedding-index FGs:** online delete is unsupported (the online data lives in the vector DB). The delete is offline-only with a warning; `storage="online"` raises.
- `fg.commit_delete_record(...)` is the deprecated name and always deletes offline only.

---

## Deleting a Feature Group

**Confirm before deleting.** `fg.delete()` (CLI `hops fg delete <name> --version N --yes`) drops the feature group and all its data irreversibly; confirm the exact name and version with the user first.
Never tear down a feature group you created as a side effect — temp or test ones included — unless the user asked; default to keeping resources.

---

## Evolving the Schema

Two cases, split by whether downstream consumers can be disturbed.

**Add a column: append in place, same version.** Appending keeps the feature
group version, so projects reading the FG downstream keep working.
`get_or_create_feature_group` returns the existing FG and ignores a changed
`features=` list, so re-running a pipeline never adds columns, and `fg.insert()`
with extra columns fails with `Features are not compatible with Feature Group
schema`. Append explicitly instead:

```python
from hsfs.feature import Feature

fg.append_features([Feature("score", "double"), Feature("tier", "string")])
```

CLI: `hops fg append-features <name> --features "score:double:Risk score,tier:string"` — the spec is `name:type[:description]`, so set a description per column.

Rules and consequences:
- Append-only. New columns cannot be primary or partition keys.
- Existing rows are not backfilled. They read null for the new column until reinserted.
- Feature views over this FG keep their old projection and do not see the new columns. Build a new feature view (via `fg.select(...)`) to use them.

**Drop a column, rename, or change a type: new version.** The backend rejects
these in place: a feature group used downstream must not change shape under its
consumers. Create the next version with the new schema and migrate readers to
it:

```python
fg_v2 = fs.get_or_create_feature_group(name="my_fg", version=2, primary_key=[...], features=[...])
```

`hops fg delete` then recreate is data loss, not schema evolution. Reserve it for a throwaway FG that nothing reads yet.

---

## Complete Feature Pipeline Template

```python
import polars as pl
import hopsworks
from hsfs.feature import Feature

project = hopsworks.login()
fs = project.get_feature_store()

# 1. Get source FGs (for reading + provenance)
source_fg = fs.get_feature_group("source_data", version=1)
source_df = source_fg.read(dataframe_type="polars")

# 2. Compute features
features_df = compute_my_features(source_df)

# 3. Create derived FG with provenance
derived_fg = fs.get_or_create_feature_group(
    name="derived_features",
    version=1,
    description="Features derived from source_data for XYZ model",
    primary_key=["id"],
    event_time="event_ts",
    features=[
        Feature("id", "bigint", description="Entity id"),
        Feature("event_ts", "timestamp", description="When the value was valid"),
        # one Feature(..., description=...) per column — never leave a feature undescribed
    ],
    online_enabled=True,        # ask user: online or offline?
    stream=True,
    parents=[source_fg],        # provenance
    statistics_config=False,    # inform user; disable for large data
)

# 4. Insert
derived_fg.insert(features_df, wait=False)

# 5. Materialize (once, after all inserts)
derived_fg.materialization_job.run(await_termination=True)
```

---

## Next Steps

- Serve these features for training/inference: **hops-fv** (build a feature view over this FG). The feature view, not the feature group, is where you attach MDTs and ODTs — it applies the same transformations in training and inference, preventing training/serving skew.
- Explore / query the data: **hops-data-discovery**, **hops-trino-sql**.
- Schedule the pipeline as a recurring job: **hops-job**.

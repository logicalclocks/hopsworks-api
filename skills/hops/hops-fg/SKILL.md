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

```python
# Safe pattern for resource-constrained environments
fg.insert(df, wait=True)
```

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

### Quick Estimate

```
Memory ≈ rows × columns × avg_bytes_per_value × overhead_factor
```

- Numeric (int/float): ~8 bytes
- String: ~50-200 bytes (varies)
- Overhead factor: ~2-3x (Polars/Pandas internal bookkeeping, intermediate results)

**Example:** 1M rows × 40 columns × 8 bytes × 3 ≈ 960 MB — fits in most environments.

### If Data Is Too Large for RAM

1. **Read in partitions** — use `fg.read()` with `start_time`/`end_time` to read slices by event time:
   ```python
   df = fg.read(start_time="2026-01-01", end_time="2026-02-01", dataframe_type="polars")
   ```

2. **Process in batches** — compute features on chunks and use multi-part insert:
   ```python
   with fg.multi_part_insert() as writer:
       for chunk in chunks:
           features = compute_features(chunk)
           writer.insert(features)
   ```

3. **Reduce intermediate DataFrames** — use `del df` aggressively after each step; avoid keeping multiple copies of large DataFrames.

4. **Use Polars over Pandas** — Polars is more memory-efficient (columnar, zero-copy operations, lazy evaluation).

### If a Feature Pipeline OOMs

1. **Identify the memory spike**: usually it's reading all source FGs into memory simultaneously, or computing rolling window features that create large intermediates.
2. **Reduce concurrent reads**: read one source FG at a time, compute what you need, then drop it before reading the next.
3. **Switch to batched inserts**: use multi-part insert so you don't need to hold the full output in memory.
4. **Reduce read scope**: read only the columns you need via a feature view with `select()`, or filter by time range.
5. **Use Spark**: for very large data, switch to PySpark which can spill to disk.

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

When a feature group contains vector embeddings (for similarity search), follow these rules:

### Keep Embedding FGs Minimal

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
        # Include as FEW other features as possible
        # Only include features that rarely change
    ],
    online_enabled=True,
    stream=True,
)
```

### Why Minimal?

- Embedding FGs are backed by a **vector database** (OpenSearch), not RonDB.
- Vector DBs are optimized for read-heavy similarity search, **not** frequent updates.
- Every update to ANY feature in the FG triggers a vector DB write (re-indexing).
- If you have frequently-updated features (e.g., real-time counters, scores), put them in a **separate** online FG backed by RonDB, which handles high-frequency small updates well.

### Anti-Pattern

```python
# BAD: mixing embeddings with frequently-updated features
fg = fs.get_or_create_feature_group(
    name="user_profile",
    embedding_index=embedding_index,
    features=[
        Feature("user_id", "bigint"),
        Feature("profile_embedding", "array<float>"),
        Feature("last_login", "timestamp"),        # updates often!
        Feature("session_count", "int"),            # updates often!
        Feature("recent_click_score", "double"),    # updates often!
    ],
    ...
)
```

```python
# GOOD: separate FGs for embeddings vs frequently-updated features
embedding_fg = fs.get_or_create_feature_group(
    name="user_embeddings",
    embedding_index=embedding_index,
    features=[
        Feature("user_id", "bigint"),
        Feature("profile_embedding", "array<float>"),
        # Only static/rarely-changing metadata here
    ],
    ...
)

activity_fg = fs.get_or_create_feature_group(
    name="user_activity",
    online_enabled=True, stream=True,  # RonDB-backed, handles frequent updates
    features=[
        Feature("user_id", "bigint"),
        Feature("last_login", "timestamp"),
        Feature("session_count", "int"),
        Feature("recent_click_score", "double"),
    ],
    ...
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

Pass a DataFrame identifying the rows to remove. For an **offline (Delta) FG with an `event_time`**, the merge key is the primary key **plus** the `event_time` column (plus any partition columns) — a primary-key-only DataFrame fails with `DeltaError: No field named <event_time>`. Include every key column:

```python
import polars as pl

# primary_key column(s) + event_time (+ partition columns, if any)
rows_to_delete = pl.DataFrame({
    "trans_id": [101, 202, 303],
    "event_ts": ["2026-01-01", "2026-01-02", "2026-01-03"],
})

fg.commit_delete_record(rows_to_delete)
```

Only rows matching on all key columns are deleted.

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

CLI: `hops fg append-features <name> --features "score:double,tier:string"`.

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
    features=[...],
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

## Quick Reference

| Task | Code |
|---|---|
| Create FG | `fs.get_or_create_feature_group(name=..., version=1, ...)` |
| Insert data | `fg.insert(df, wait=False)` |
| Insert safely (low resources) | `fg.insert(df, wait=True)` |
| Multi-part insert | `with fg.multi_part_insert() as w: w.insert(batch)` |
| Finalize multi-part | `fg.finalize_multi_part_insert()` |
| Read (Polars) | `fg.read(dataframe_type="polars")` |
| Read (time range) | `fg.read(start_time=..., end_time=..., dataframe_type="polars")` |
| Read (filtered) | `fg.filter(fg.col > X).read(dataframe_type="polars")` |
| Preview rows | `print(fg.show(n=10))` (returns a DataFrame) |
| Similarity search | `fg.find_neighbors(vector, k=5, filter=...)` |
| Delete rows | `fg.commit_delete_record(df)` (df = primary_key cols + event_time) |
| Add a column (same version) | `fg.append_features([Feature("c", "double")])` / `hops fg append-features <name> --features "c:double"` |
| Drop/retype a column | not in place: create a new FG version |
| Disable statistics | `statistics_config=False` |
| Set provenance | `parents=[parent_fg1, parent_fg2]` |
| Trigger materialization | `fg.materialization_job.run(await_termination=True)` |
| Schedule materialization | `offline_backfill_every_hr=4` (at creation) |
| Delete FG | `fg.delete()` |
| Get FG | `fs.get_feature_group("name", version=1)` |

---

## Next Steps

- Serve these features for training/inference: **hops-fv** (build a feature view over this FG). The feature view, not the feature group, is where you attach MDTs and ODTs — it applies the same transformations in training and inference, preventing training/serving skew.
- Explore / query the data: **hops-data-discovery**, **hops-trino-sql**.
- Schedule the pipeline as a recurring job: **hops-job**.

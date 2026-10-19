---
name: hops-fg
description: Use when writing Python code that creates, inserts into, or manages tables or feature groups. Auto-invoke when user writes feature pipelines, feature engineering code, or asks about feature group best practices (online vs offline, batching, OOM, materialization, embeddings, statistics).
---

When a user refers to tables, clarify that you interpret them as feature groups in Hopsworks.

# Hopsworks Feature Groups: Python SDK Best Practices

Writes computed features into a Hopsworks feature group, the storage backing the F (feature) stage of the FTI pattern.

A feature pipeline applies **model-independent transformations (MITs)** and writes the resulting **untransformed, reusable** feature data to feature groups. Do NOT store model-dependent transformations (MDTs, e.g. scaling, one-hot encoding) in a feature group: those are applied later, in the feature view, when reading for training/inference. Storing MDT output makes the data non-reusable across models, can cause write amplification (a parameterized MDT like standardization rewrites every existing row), and breaks EDA on raw values. Reuse is the payoff: the lowest-cost feature pipeline is the one you don't have to create, so write features other models can also select.

## Contract

- **Input:** a DataFrame (Pandas/Polars/PySpark) of computed features, plus the target name/version and key columns.
- **Output:** a feature group registered server-side (on first insert) and populated with rows; optionally online-enabled and materialized to the offline store.
- **Pre-condition:** a Hopsworks project login and a feature store handle (`fs = project.get_feature_store()`); any parent FGs used for provenance already exist.

## Smoke-test (cheap pre/post-flight)

Before writing Python, and to confirm results after, use the CLI. No Spark session needed:

```bash
hops fg list                              # is the name/version free? did it register? (note the STORE column)
hops fg info <name> --version 1           # metadata: id, online flag, primary key, event_time
hops fg feature list <name> --version 1       # schema with primary-key / partition flags
hops fg preview <name> --version 1 --limit 10 # first rows
hops fg preview <name> --columns a,b,c    # project away wide embedding/array columns
hops fg stats info <name> --version 1          # null counts / ranges: spot bad data early
```

To preview an FG in a shared store from the CLI, pass `--featurestore <store>`
(the STORE value from `hops fg list`).

`hops fg list` shows a **STORE** column. Imported / public feature groups live in a
**shared** store, not this project's own. That distinction matters when you read
them (see "Reading Feature Groups" below).

A feature group is registered server-side on its **first insert**, not at `get_or_create_feature_group(...)`. Until the first insert `fg.id` is `None` and `hops fg list` will not show it.

## Ask the user (before writing a feature pipeline)

Before creating a feature group, clarify these decisions with the user:

1. **Online or offline?**
   - **Offline only** (`online_enabled=False`, `stream=False`): writes directly to Delta Lake. Best for batch training data, historical analytics, large-volume cold storage.
   - **Online + offline** (`online_enabled=True`, `stream=True`): writes go to Kafka → RonDB (online), then a Spark materialization job copies to Delta (offline). Required for low-latency serving, real-time feature lookups, and feature views with `get_feature_vector()`.
   - Default to offline unless the user needs online serving.

2. **Does this FG derive from other FGs?** If so, pass `parents=[fg1, fg2, ...]` at creation time. This sets up explicit provenance/lineage tracking in the Hopsworks UI. Always pass the actual FeatureGroup objects, not names.

3. **Data volume**: estimate row count × column count × avg bytes per value. This drives decisions on batching, statistics, and materialization (see below).

4. **Time-series or not?** If features change over time, set `event_time` to the timestamp when the feature value was *valid* (not when the row was ingested). This is what lets the feature store build point-in-time correct training data (no future leakage, no stale values) via the feature view. Omit `event_time` only for immutable feature data.

---

## Feature data types & online-store constraints

Pick a supported type up front: a write with an unsupported type fails, and retrying the same type just loops.

**Supported feature types:**
- Scalars: `int`, `bigint`, `float`, `double`, `boolean`, `string`, `date`, `timestamp`, `binary`.
- Composite: `array<type>` and `struct<field:type,...>`, e.g. `array<float>`, `struct<lat:double,lon:double>`.
- `decimal` is **not** supported. Use `double`, or `string` when you need exact precision.

**Online store (`online_enabled=True`):**
- Scalars map straight to RonDB. Strings become `varchar(n)`, auto-sized to the longest value seen (rounded up to 100) and widened on later inserts; very long text falls back to `text`.
- Composite types (`array`, `struct`) **do** write online: they are stored Avro-encoded and decoded by the SDK on read. An online FG with an `array<float>` column is fine; you do not need to drop or flatten it.
- For **similarity search**, declare the vector as `array<float>` **and** attach an `EmbeddingIndex` (see Vector Embeddings): the FG is then backed by the vector DB (OpenSearch) instead of RonDB. Without an embedding index an `array<float>` is stored data, not a searchable index.
- Online is an upsert: one row per primary key, a new write for an existing key overwrites it.

Types are Hive names: `int` is 32-bit and `bigint` 64-bit, so a Polars or pandas `int64` column is `bigint` (an `int` column refuses it). The CLI spec `name:type[:description]` is comma-separated, so a description holds no comma.

Let the schema be inferred from the DataFrame when you can; pass an explicit `features=[Feature(name, type, ...)]` list (`from hsfs.feature import Feature`; there is no `hopsworks.feature`) only to pin a type (e.g. `bigint` over an inferred `int`, or an `array<float>` embedding column).

---

## Creating a Feature Group

A whole feature pipeline, with provenance and the sink created from it:

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
        # one Feature(..., description=...) per column; never leave a feature undescribed
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

**Always describe what you create.** Pass `description=` on the feature group and a `description=` on every `Feature(...)`. A feature group or column with no description shows as an empty envelope in the UI and is not discoverable in search. If the user gave none, write concise ones from what each feature means; never leave them blank.

### Key Parameters

| Parameter | When to use |
|---|---|
| `online_enabled=True` | Need online feature serving or real-time lookups |
| `stream=True` | Always set when `online_enabled=True`; enables unified write API |
| `parents=[...]` | FG is derived from other FGs; pass a list of parent FG objects |
| `statistics_config=False` | Large data volumes (see Statistics section), and every feature group written from a Python job: the Python engine computes statistics in a PySpark job after each insert |
| `event_time="col"` | Need time-travel queries or incremental reads |
| `embedding_index=EmbeddingIndex(...)` | FG contains vector embeddings (see Embeddings section) |
| `offline_backfill_every_hr=N` | Schedule automatic materialization every N hours |
| `ttl=timedelta(days=30)` | Auto-expire old rows from online store |
| `time_travel_format`, `partition_key`, `partitioned_by`, `clustered_by`, `zorder_by`, `sort_order`, `bucket_index` | Offline table format and layout (see Table format and layout) |

---

## Table format and layout

The offline table format is set at creation with `time_travel_format`: `"DELTA"` (default), `"ICEBERG"` (needs `pyiceberg`), `"HUDI"` (what online/`stream=True` FGs use), or `None`. The layout knobs (`partition_key`, `partitioned_by`, `clustered_by`, `zorder_by`, `sort_order`) are format-specific and the backend rejects the wrong combination. Before passing any of them, read `hops skills info hops-fg references/table-layout.md`. Diagnosing an existing table: **hops-table-maintenance**.

---

## Inserting Data

### Simple Insert (default for most cases)

```python
fg.insert(df, wait=False)  # async: returns immediately
```

- Accepts: Pandas DataFrame, Polars DataFrame, PySpark DataFrame, NumPy array, or Python list
- `wait=False` (default): returns immediately; ingestion runs in background
- `wait=True`: blocks until online ingestion AND offline materialization complete
- An offline insert upserts on the primary key **plus** `event_time` (plus partition columns): a row with a known key and a new event time is a new row. A table keyed by snapshot takes the snapshot as `event_time`, never the time it was written.

### When to Use `wait=True`

Use `insert(df, wait=True)` when:
- **Low on CPU/memory**: `wait=True` for online FGs ensures only one Spark materialization job runs at a time. Multiple concurrent async inserts can each launch a Spark job, exhausting cluster resources.
- **Pipeline ordering matters**: downstream steps depend on data being fully materialized.
- **Debugging insert failures**: async mode may silently swallow errors.

Many small batches go through one multi-part insert: `hops skills info hops-fg references/writes.md`.

---

## Memory / OOM Prevention

Estimate before writing: rows × columns × ~8 bytes (numeric) × 3. A 1M × 40 numeric frame is about 1 GB. When it does not fit, read in event-time slices, one source at a time, and insert with a multi-part insert: `hops skills info hops-fg references/writes.md`.

---

## Statistics

By default, Hopsworks computes descriptive statistics on every insert. For large data volumes (GBs+), this adds significant overhead. Hopsworks creates the job `<fg>_<version>_compute_stats` with every feature group; with statistics off it never runs (`hops job history` lists no execution).

### Disable Statistics at Creation Time

```python
fg = fs.get_or_create_feature_group(
    name="large_fg",
    version=1,
    statistics_config=False,    # disables all statistics computation
    ...
)
```

Statistics on chosen columns only (`StatisticsConfig`): `hops skills info hops-fg references/writes.md`.

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

Scheduled materialization (`offline_backfill_every_hr`) and running it without waiting: `hops skills info hops-fg references/writes.md`.

---

## Vector Embeddings

A feature group with an `embedding_index` is backed by the vector database (OpenSearch), and every update to any of its features re-indexes. Keep it to the key, the vector and rarely-changing metadata. Creating one and similarity search: `hops skills info hops-fg references/embeddings.md`.

---

## Reading Feature Groups

`fg.read(dataframe_type="polars")` reads the offline store; `start_time`/`end_time` slice it by event time, `fg.filter(...)` pushes predicates down. A feature group in a **shared** store (the STORE column of `hops fg list`) is reached through `project.get_feature_store(name="<that_store>")`, never the project's default handle, which returns `None` for it. Online, time-travel and filtered reads, and previews: `hops skills info hops-fg references/reads.md`.

---

## Deleting a Feature Group

**Confirm before deleting.** `fg.delete()` (CLI `hops fg delete <name> --version N --yes`) drops the feature group and all its data irreversibly; confirm the exact name and version with the user first.
Never tear down a feature group you created as a side effect (temp or test ones included) unless the user asked; default to keeping resources.

---

## Changing a feature group

Add a column in place with `fg.append_features([...])` (CLI `hops fg feature add`); `get_or_create_feature_group` ignores a changed `features=` list. Dropping, renaming or retyping a column takes a new version. Deleting rows takes the primary key **plus** `event_time`. Both, with their rules: `hops skills info hops-fg references/changes.md`.

---

## References

Read the one a step needs, with `hops skills info hops-fg references/<file>` (or from this
skill's `references/` directory):

- [references/writes.md](references/writes.md): multi-part insert, memory and OOM, statistics on
  chosen columns, scheduled materialization.
- [references/reads.md](references/reads.md): offline, online, shared-store, time-sliced,
  time-travel and filtered reads, and previews.
- [references/table-layout.md](references/table-layout.md): table format, partitioning,
  clustering and `fg.optimize()`.
- [references/embeddings.md](references/embeddings.md): an embedding feature group and
  similarity search.
- [references/changes.md](references/changes.md): deleting rows and evolving the schema.

---

## Next Steps

- Serve these features for training/inference: **hops-fv** (build a feature view over this FG). The feature view, not the feature group, is where you attach MDTs and ODTs: it applies the same transformations in training and inference, preventing training/serving skew.
- Explore / query the data: **hops-data-discovery**, **hops-trino-sql**.
- Schedule the pipeline as a recurring job: **hops-job**.

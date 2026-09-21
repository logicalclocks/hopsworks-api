# Offline table layout: format, partitioning, clustering, optimize

Companion to the parent skill's "Table format and layout" section. The parent
carries the rules table; this file carries the grammar, the per-format behaviour
and the runtime caveats. **hops-spark** covers which of these operations work
under Spark Connect, and **hops-table-maintenance** covers diagnosing and fixing
an existing table's layout.

## Table format (`time_travel_format`)

- `"DELTA"` (default) — Delta Lake. Direct offline write from the Python client.
- `"ICEBERG"` — Apache Iceberg. Direct offline write from the Python client. Requires the `pyiceberg` library (`pip install pyiceberg`, or install Hopsworks with the `python` extra); a missing library fails fast with a clear error.
- `"HUDI"` — Apache Hudi. Used by online-enabled (`stream=True`) feature groups, and also valid for offline direct write.
- `None` — no time travel.

```python
fg = fs.get_or_create_feature_group(
    name="events", version=1, primary_key=["id"], event_time="event_ts",
    time_travel_format="ICEBERG", online_enabled=False,
)
```

The Catalog UI shows the format as a badge (Delta / Hudi / Iceberg) on each feature group.

## Partitioning

Two mechanisms, mutually exclusive, and which one is available depends on the format:

- `partition_key=["col", ...]` — hive-style partitioning on **real columns**. Works on any format.
- `partitioned_by=["day(event_ts)", ...]` — **native partition transforms**. ICEBERG and HUDI only.

### `partitioned_by` grammar

Each element is a transform expression over a source column, optionally aliased with `as <field_name>` (Iceberg only). Supported: `identity(col)` (or a bare column name), `bucket(N, col)`, `truncate(W, col)`, `year(col)`, `month(col)`, `week(col)`, `day(col)`, `hour(col)`, `void(col)`.

```python
fg = fs.get_or_create_feature_group(
    name="clickstream", version=1, primary_key=["click_id"], event_time="event_timestamp",
    time_travel_format="ICEBERG",
    partitioned_by=["day(event_timestamp)"],
    online_enabled=False,
)
```

| Format | Accepted transforms | How it materializes |
|---|---|---|
| ICEBERG | `identity`, `bucket`, `truncate`, `void`, `year`, `month`, `day`, `hour` | Compiled into the table's `PartitionSpec` — **hidden partitioning**, no new columns. At most one temporal transform per source column (`day(ts)` already prunes at year level). |
| HUDI | `identity` plus `year`/`month`/`week`/`day`/`hour` | Grain columns are **materialized** as real hive-style partition columns named after the grain. Temporal transforms must use the `event_time` column. No aliases. |
| DELTA | **rejected** | Delta has no partition transforms. Use `partition_key` for identity partitions, or `clustered_by` for liquid clustering. |

The bare-grain form `partitioned_by=["year", "month", "day"]` was **removed** and is now rejected with a migration hint. Write transforms on the event-time column instead: `["year(event_ts)", "month(event_ts)", "day(event_ts)"]`. A feature group created with the old form can still be read but cannot be written by a current client. `partitioned_by` requires `event_time` and an offline (non-stream) FG; `hour` needs a `timestamp` event_time, not a `date`.

**Hidden partitioning is invisible in the schema.** On Iceberg the partition field (`event_timestamp_day`) lives in the partition spec, not the column list — so `hops fg features <name>` shows a blank PARTITION column and the UI shows no partition key even though the table *is* partitioned. Do not conclude the partitioning failed; ask the table:

```python
fg.get_partition_spec()   # ['event_timestamp_day: day(event_timestamp)']
```

From Spark, Iceberg's `partitions` metadata table is the ground truth (one row per partition value, with record and file counts):

```python
spark.read.format("iceberg").load(fg.location.replace("hopsfs://", "hdfs://") + "#partitions").show()
```

Reads filter transparently: a time-range read is rewritten to the partition predicates, so you query by `event_time` and the engine prunes.

```python
df = fg.read(start_time="2026-01-01", end_time="2026-02-01", dataframe_type="polars")
```

## Clustering (`clustered_by`, `zorder_by`, `sort_order`, `bucket_index`)

Partitioning prunes on one coarse dimension. Clustering co-locates rows on the columns you actually filter by, so the engine skips *files within* a partition. The available knob depends on the format — they are not interchangeable.

| Knob | Format | What it does |
|---|---|---|
| `clustered_by=[...]` | **DELTA only** | Delta **liquid clustering**. At most 4 columns. |
| `zorder_by=[...]` | **ICEBERG**, **HUDI** | Z-order curve over the columns, at most 4. Iceberg applies it through `fg.optimize()`; Hudi through inline clustering on write. DELTA rejects it — `clustered_by` covers that case. |
| `sort_order=[...]` | **ICEBERG only** | Persistent write sort order, e.g. `["merchant_id asc", "amount desc nulls last"]`. New writes are range-distributed and sorted; `fg.optimize(strategy="sort")` rewrites existing files. Mutually exclusive with `zorder_by`. |
| `bucket_index={...}` | **HUDI only** | `{"field": <primary key col>, "num_buckets": N}`, engine `"simple"`. |

### Delta liquid clustering (`clustered_by`)

```python
fg = fs.get_or_create_feature_group(
    name="clickstream", version=1,
    primary_key=["click_id"], event_time="event_timestamp",
    time_travel_format="DELTA",
    clustered_by=["event_day", "category_id"],   # at most 4 columns
    online_enabled=False, stream=True,           # Spark-only write, see below
)
```

Two rules that bite:

1. **`clustered_by` and `partition_key` are mutually exclusive.** Delta does not support liquid clustering on a hive-partitioned table — liquid clustering *replaces* partitioning. So "partition by day **and** liquid-cluster by category" is not expressible on Delta. Either derive a day column and cluster on both (`clustered_by=["event_day", "category_id"]`), or switch to Iceberg (`partitioned_by=["day(ts)"]` + `zorder_by=["category_id"]`) when you need genuine partitions. Say which trade-off you took; do not silently drop half the request.
2. **Clustered Delta feature groups are writable by Spark only.** Liquid clustering uses the Clustering and DomainMetadata Delta writer table features, which delta-rs does not implement. From a Python environment pass `stream=True` so writes route through the Spark materialization job.

Clustering columns need per-file statistics: Delta only indexes the first `delta.dataSkippingNumIndexedCols` columns (default 32), so keep clustering columns early in the schema.

Inspect and change the clustering (both require Spark):

```python
fg.get_clustering_columns()            # read the live Delta domain metadata
fg.update_clustering(["category_id"])  # change the columns...
fg.optimize(full=True)                 # ...then recluster existing data
fg.disable_clustering()
```

### Iceberg / Hudi z-order (`zorder_by`)

`zorder_by` is metadata at creation; on Iceberg the rewrite that actually reorders data files is `fg.optimize()`:

```python
fg = fs.get_or_create_feature_group(
    name="clickstream", version=1,
    primary_key=["click_id"], event_time="event_timestamp",
    time_travel_format="ICEBERG",
    partitioned_by=["day(event_timestamp)"],
    zorder_by=["category_id"],
    online_enabled=False,
)
fg.insert(df)
fg.optimize(rewrite_all=True)   # initial full z-order after a backfill
fg.optimize()                   # cheap incremental maintenance thereafter
```

`rewrite_all=True` matters for the first pass: the Iceberg rewrite planner skips file groups below its size thresholds, which right after a backfill is usually all of them. On Hudi there is nothing to call — inline clustering applies the order on write.

Z-order pays off when the clustering column is **skewed and selective**. A uniformly distributed column spreads every value across every file and buys nothing.

## `fg.optimize()` — layout maintenance

| Format | Behaviour |
|---|---|
| ICEBERG | Iceberg `rewriteDataFiles`. `strategy` is `"zorder"` (over `columns`, default `zorder_by`), `"sort"` (the table's `sort_order`), or `"binpack"`; left unset it follows the FG's stored layout in that order. `rewrite_all`, `target_file_size_mb`, `where` tune it. **Requires Spark.** |
| DELTA | `OPTIMIZE`, which incrementally clusters when `clustered_by` is set. `full=True` runs `OPTIMIZE FULL` to recluster everything after the clustering columns changed. `strategy="zorder"` is the legacy path and works only on *unclustered* tables — Delta rejects ZORDER BY on a clustered table. Clustered FGs require Spark; without Spark only unclustered compaction is available. |
| HUDI | Rejected — layout maintenance runs through Hudi inline clustering on writes. |

## Runtime caveats

- **The whole Iceberg write path needs a JVM.** PyIceberg reaches HopsFS through PyArrow's JNI `libhdfs`. A Python environment without a JVM fails with `OSError: Unable to load libjvm` — including on the initial *empty table creation*, so even `stream=True` does not rescue it. Run the pipeline as a **PYSPARK job**. Iceberg `optimize()` is a Spark action regardless.
- If the cluster runs a remote shuffle service (e.g. Apache Uniffle's `RssShuffleManager`), an Iceberg `rewriteDataFiles` shuffle can die with `IllegalAccessError` on Iceberg's shaded netty/parquet classes, or a JVM SIGSEGV. Force the built-in shuffle on the SparkSession **builder**, before the context exists — setting it in the job config does not stick:

  ```python
  spark = SparkSession.builder.config("spark.shuffle.manager", "sort").getOrCreate()
  ```

---
name: hops-spark
description: Use whenever you write PySpark code in a Hopsworks terminal (terminal-spark) or any project that runs PySpark. Auto-invoke when the user writes `spark` or `pyspark`. Covers Spark Connect vs classic Spark, and which Delta/Iceberg/Hudi feature-group writes work in each.
---

# PySpark on Hopsworks (Spark Connect)

## Concept

This skill covers **PySpark** (Python), which is the common path on Hopsworks: interactively in the terminal-spark image (Spark Connect) and as `--type PYSPARK` jobs. For **JVM/Scala/Java Spark** packaged as a JAR, there is no Spark Connect session to build — submit it as a `--type SPARK` job via the **hops-job** skill (`hops job deploy <name> <app.jar> --type SPARK`); the Delta/catalog wiring below is configured cluster-side for those, not in user code.

The terminal-spark image runs Spark in **Spark Connect** mode by default. The local Spark Connect server is started by the entrypoint and the connection URI is exported into the user's shell as `SPARK_REMOTE` (`sc://localhost:15002`, or a cluster-internal `sc://terminal-<project>--<user>-spark-connect-svc...:15002`). PySpark reads `SPARK_REMOTE` automatically, so user code must **not** hard-code `.remote(...)` — that path turns into a brittle smoke test that fails the moment the port shifts or the script is run inside a job pod (where the Connect server lives somewhere else).

Run Spark Connect programs with python3, not spark-submit. 

## Key facts / rules

### The two settings every Spark Connect session must enable

Hopsworks reads/writes offline feature groups through Delta Lake. A bare `SparkSession.builder.getOrCreate()` from PySpark gives you a session that can talk to Spark Connect but **does not** load the Delta extensions or rewire the default catalog to `DeltaCatalog`. The result: `spark.read.format("delta")...` fails with "no DataSource named delta", `spark.sql("CREATE TABLE ... USING delta")` errors, and `fg.read()` against any Delta-backed offline feature group returns nothing.

Always set:

| Config key | Value |
|---|---|
| `spark.sql.extensions` | `io.delta.sql.DeltaSparkSessionExtension` |
| `spark.sql.catalog.spark_catalog` | `org.apache.spark.sql.delta.catalog.DeltaCatalog` |

`hopsworks.build_spark()` does this for you (below). Note that on a Connect session these arrive as a **request** to a server that is already running: `spark.sql.extensions` is a static config, so you will see

```
UserWarning: Failed to set spark.sql.extensions to Some(io.delta.sql.DeltaSparkSessionExtension) due to
[CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark config: "spark.sql.extensions".
```

This warning is **benign in terminal-spark** — the Connect server already starts with the Delta extension loaded, so Delta reads and writes work. It is a real signal only if Delta operations then fail, which means you are pointed at a Connect server that was not started with Delta.

### Anti-patterns to fix on sight

```python
# WRONG: hard-coded Connect URI
SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# WRONG: missing Delta extensions/catalog — will silently misbehave on
# any read or write against an offline feature group
SparkSession.builder.appName("x").getOrCreate()

# WRONG: setting the catalog without the extension (or vice versa) —
# Delta needs both
SparkSession.builder.config(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
).getOrCreate()
```

### When to override `SPARK_REMOTE`

Only when running outside the terminal-spark pod and pointing at a remote Spark Connect server you control (uncommon for Hopsworks workloads). Even then, prefer setting `SPARK_REMOTE=sc://host:port` in the environment over hard-coding `.remote(...)` in the script — keeps the same script runnable inside the terminal where the env var already points at the right place.

**The inverse matters too:** to run a script on *classic* Spark you must **unset** it. `build_spark()` detects Connect through `pyspark.sql.utils.is_remote()`, which honours `SPARK_REMOTE`, so a `spark-submit` launched with the variable still set builds a thin Connect client instead of a local JVM session:

```bash
env -u SPARK_REMOTE -u SPARK_CONNECT_MODE_ENABLED spark-submit my_pipeline.py
```

### Notes

- `getOrCreate()` is fine; the image starts the Connect server before the user's first shell.
- Do not pass `master(...)` — Spark Connect does not honour it and it's a leftover from spark-submit-style code.
- Use `pyspark` from the venv (`PYSPARK_PYTHON` is preset), not a fresh `pip install pyspark` — that breaks the JAR/Python ABI Hopsworks bakes into the image.

## Commands / API

The hopsworks SDK ships ``hopsworks.build_spark`` so user code stays one line. In Spark Connect mode it applies the Delta extensions + DeltaCatalog automatically; outside Connect (spark-submit / classic clusters) it leaves session config to ``spark-defaults.conf`` and just sets ``app_name`` + any user overrides:

```python
from hopsworks import build_spark

spark = build_spark("my_pipeline")
```

Pass extra configs with ``extra_configs={...}``:

```python
spark = build_spark(
    "my_pipeline",
    extra_configs={"spark.sql.shuffle.partitions": "200"},
)
```

This works for Hopsworks feature group reads/writes, plain Delta paths under HopsFS, and the hopsworks SDK's `fg.read()` / `fg.insert(df)` paths that delegate to Spark.

---

## Table formats: what actually works under Spark Connect

The offline table format is chosen per feature group with `time_travel_format` (`"DELTA"` default, `"ICEBERG"`, `"HUDI"`, `None`). **The format decides whether the pipeline can run in the terminal at all**, because the Connect client is a thin gRPC client with no JVM bridge (`spark._sc._jvm` does not exist) and the Connect server only understands relation plugins it was started with.

| Operation | Spark Connect (terminal) | Classic Spark (PYSPARK job) |
|---|---|---|
| Delta read / plain `format("delta").save()` | ✅ | ✅ |
| Delta `fg.insert(..., operation="insert")` (append) | ✅ | ✅ |
| Delta `fg.insert(...)` default **upsert** (MERGE) | ❌ `InvalidPlanInput: No handler found for extension` | ✅ |
| Iceberg read / `fg.read()` | ✅ | ✅ |
| Iceberg **table creation** (i.e. the first insert) | ❌ raises: "not supported in Spark Connect mode because it requires JVM bridge access" | ✅ |
| Iceberg append into an existing table | ✅ | ✅ |
| Hudi (`stream=True` online FGs) | materialization job, not your session | ✅ |

### Delta + Spark Connect: use `operation="insert"` for append-only data

`fg.insert(df)` defaults to `operation="upsert"`, which for Delta runs `DeltaTable.merge(...)` through `delta.connect.tables`. That needs the delta-connect **server** plugin registered on the Connect server (`spark.connect.extensions.relation.classes`); terminal-spark does not register it, so the merge fails with:

```
pyspark.errors.exceptions.connect.InvalidPlanInput: [INTERNAL_ERROR] No handler found for extension
  ... org.apache.spark.sql.connect.planner.SparkConnectPlanner.transformRelationPlugin
```

For immutable events with a unique primary key, append is both correct and cheaper:

```python
fg.insert(events_df, operation="insert", wait=True)   # append, no MERGE
```

A genuine upsert into a Delta FG has to run on classic Spark (a PYSPARK job).

**Gotcha — the empty first commit.** The first `fg.insert()` writes a schema-only Delta commit (`numOutputRows: 0`) before the data write. If that run dies partway, the table exists but is empty, and *every later run takes the merge branch* (`_is_delta_table_at` is now true) and fails under Connect. Symptom to recognise:

```bash
hdfs dfs -cat <fg_path>/_delta_log/00000000000000000000.json   # commitInfo: numFiles 0, numOutputRows 0
```

Re-run with `operation="insert"`; it appends into the existing empty table and the schema still matches.

Reading commit metadata is Connect-safe: hsfs deliberately reads `_delta_log/*.json` with `spark.read.json` instead of `DESCRIBE HISTORY`, because the Connect server has no Hive client provisioned (`Unable to instantiate SessionHiveMetaStoreClient`).

### Partitioning is format-specific — this is the most common mistake

`partitioned_by` and `partition_key` are **not interchangeable**, and the transform grammar changed:

```python
# DELTA — no partition transforms exist. Materialize the grain as real
# columns yourself and use partition_key (identity partitioning):
df = df.withColumn("event_date", F.to_date("event_ts")) \
       .withColumn("event_hour", F.hour("event_ts"))
fs.get_or_create_feature_group(..., time_travel_format="DELTA",
                               partition_key=["event_date", "event_hour"])
# -> event_date=2026-08-31/event_hour=0..23 on disk
# (or use clustered_by=[...] for Delta liquid clustering instead)

# ICEBERG / HUDI — hidden partitioning via transform expressions on event_time.
# No partition columns are added to the schema; readers filter on event_ts.
fs.get_or_create_feature_group(..., time_travel_format="ICEBERG",
                               partitioned_by=["hour(event_ts)"])
# -> data/event_ts_hour=2026-08-31-00 .. -23 on disk
```

Rules learned the hard way:

- `partitioned_by` on `time_travel_format="DELTA"` raises: *"partitioned_by is not supported on DELTA: Delta has no partition transforms. Use clustered_by for liquid clustering or partition_key for identity partitions."*
- Bare grain names — `partitioned_by=["year","month","day","hour"]` — are the **removed** legacy form and raise *"looks like the removed grain form. Write it as a transform on your event_time column instead, e.g. 'hour(event_ts)'"*. A feature group created with the old form can still be read but **cannot be written** by a current client.
- Transforms: `identity(col)` (or a bare column name), `bucket(N, col)`, `truncate(W, col)`, `year/month/week/day/hour(col)`, `void(col)`, each optionally `as <field_name>` (alias is Iceberg-only).
- Iceberg allows **at most one temporal transform per source column** — `["year(ts)","month(ts)","day(ts)"]` is rejected. Pick the finest grain you need; `hour(ts)` already prunes coarser ranges.
- Set either `partition_key` or `partitioned_by`, never both. `partitioned_by` requires `event_time` and an offline (non-stream) FG; `hour` needs a `timestamp` event_time, not a `date`.

### Iceberg pipelines cannot run in the terminal

Creating a path-based (HadoopTables) Iceberg table goes through the JVM Iceberg API — `SparkSchemaUtil.convert(dataset._jdf.schema())` and `PartitionSpec.builderFor(...)` — which the Connect client cannot reach. hsfs fails fast and tells you what to do:

```
FeatureStoreException: Creating Iceberg tables is not supported in Spark Connect mode
because it requires JVM bridge access. Create the feature group from a classic Spark session instead.
```

So **an Iceberg feature pipeline is a `--type PYSPARK` job**, not a terminal script (once the table exists, appends and reads work from the terminal again).

Also note the Iceberg upsert cost: a path-based table has no catalog identifier for `MERGE INTO`, so hsfs implements the upsert as an anti-join of existing data against the incoming keys followed by an **atomic overwrite of the whole table**. For append-only data always pass `operation="insert"`.

## Running outside Spark Connect: prefer a job over spark-submit

The general fallback for "Connect can't do this" is classic Spark, but in the terminal-spark pod `spark-submit` is usually **not** available in practice. Check before recommending it:

```bash
ls /srv/hops/spark-kube-token/          # empty -> no k8s submit credentials
cat /sys/fs/cgroup/memory.max           # e.g. 2147483648 (2 GiB) -> local[*] driver is tiny
```

Two failures follow from that, both observed:

- `spark-submit` with the default `spark.master k8s://...`: `SparkException: External scheduler cannot be instantiated ... NoSuchFileException: /srv/hops/spark-kube-token/token`.
- `spark-submit --master "local[16]" --driver-memory 8g`: the driver JVM is OOM-killed mid-write and surfaces as `SparkException: Job N cancelled because SparkContext was shut down`. A 2 GiB cgroup cannot host an 8 GiB heap, and 24 concurrent parquet partition writers will not fit in the ~1.4 GiB of headroom either.

The reliable classic-Spark path is a Hopsworks job, which gets its own driver and executors:

```bash
hops job deploy clickstream-iceberg-pipeline my_pipeline.py \
  --type pyspark --env spark-feature-pipeline --run --wait --overwrite
hops job logs clickstream-iceberg-pipeline
```

`local[*]` in the terminal is only viable for genuinely small data — keep `--driver-memory` well under the cgroup cap and override the cluster-only plumbing that has no local equivalent:

```bash
env -u SPARK_REMOTE spark-submit --master "local[4]" --driver-memory 1g \
  --conf spark.plugins= --conf spark.extraListeners= \
  --conf spark.shuffle.manager=sort --conf spark.shuffle.service.enabled=false \
  my_pipeline.py
```

## Verifying an offline write

The engine is not the source of truth about what landed — check the physical layout and the row count:

```bash
hops fg info <name> --version 1
hdfs dfs -ls <fg_path>/                     # Delta: event_date=…/event_hour=…
hdfs dfs -ls <fg_path>/data/                # Iceberg: data/event_ts_hour=YYYY-MM-DD-HH
```

```python
df = fg.read()
print(df.count(), df.select(pk).distinct().count())     # rows vs. distinct keys catches double-appends
df.selectExpr("min(event_ts)", "max(event_ts)").show()  # the range you meant to write
```

An append-mode pipeline is **not idempotent** — re-running it doubles the rows. Guard a backfill with a time-range delete, or use the upsert path on classic Spark.

## Spark Program Advice

🔹 Filter and select early. The cheapest data to process is the data you never read. But verify with .explain(), Catalyst often pushes filters for you, except across outer joins and inside UDFs.
🔹 Combine aggregations into one groupBy. Two separate groupBys on the same key = two shuffles for no reason.
🔹 Never orderBy without limit on large data. orderBy().limit(10) is optimized together, a global sort IS NOT.
🔹 default cache() is MEMORY_AND_DISK for DataFrames, not MEMORY_ONLY. And your cache is a tenant, not an owner: execution memory evicts it whenever it needs room.
🔹 A cached DataFrame is an optimization barrier. Filter BEFORE caching: the order of two lines decides whether you cache 5% of your data or all of it.
🔹 Broadcast joins kill the shuffle. Under 10 MB Spark does it automatically; for slightly bigger reference tables, do it explicitly.
🔹 200 default shuffle partitions is wrong for almost everyone. Tune it to your data size or let AQE do it.
🔹 Scan once on hopsfs or S3 if possible

## Related skills

- **hops-fg** / **hops-fv** — the `fg.read()` / `fg.insert()` paths this session powers, and the `partitioned_by` / `partition_key` / `clustered_by` reference.
- **hops-job** — run a PySpark script as a scheduled Hopsworks job; the required path for Iceberg table creation and Delta upserts.

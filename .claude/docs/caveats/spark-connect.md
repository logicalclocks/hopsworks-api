# Spark Connect compatibility

`hsfs/engine/spark.py` runs in two modes:

- **Classic** — JVM-resident Spark, full `SparkContext`, `_jvm`, `.rdd`, etc.
- **Spark Connect** — gRPC client-only session. `SparkSession.builder.getOrCreate()` returns a thin client, DataFrames are `pyspark.sql.connect.dataframe.DataFrame`, no JVM access on the driver.

The engine detects Connect mode at startup via `is_spark_connect_session()` and exposes `self._is_connect`. JVM-touching code paths are gated on that flag; the predicate `is_spark_dataframe()` (in `hopsworks_common/spark_connect_utils.py`) is the single source of truth for "is this a Spark DataFrame I can operate on?" — it accepts both classic and Connect classes.

## Fix for the convert path

The original `convert_to_default_dataframe` used `self._spark_session.createDataFrame(df.rdd, nullable_schema)` to make every field nullable. `.rdd` is unavailable in Connect mode. The replacement is `df.to(nullable_schema)` (PySpark 3.4+) — same row-reconciliation semantics, no JVM round-trip, works in both modes.

## What's known to work in Connect

- `fg.insert(df)`, `fg.save(df)` for cached/Delta feature groups (Phase A unblocked these).
- Statistics computation falls back to a pandas-based implementation; see `engine/spark.py:profile`.
- Hadoop config knobs go through `_set_hadoop_conf` which writes them as `spark.hadoop.<key>` session conf in Connect mode.
- File distribution via `addFile` is a no-op (warns); the file stays driver-local.

## What's known to NOT work in Connect

- Any direct `_jvm`/`_jsc` access (Deequ, certain JDBC connector flows). Use the classic engine.
- `RDD` inputs — explicitly rejected with a clear error in `convert_to_default_dataframe`.
- `setJobGroup` is a no-op (Connect does not expose `sparkContext`).

## When in doubt

Run `engine.get_instance()._is_connect` — `True` means the path you're writing must avoid JVM/RDD APIs.

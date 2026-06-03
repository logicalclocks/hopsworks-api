---
name: hops-spark
description: Use whenever you write PySpark code in a Hopsworks terminal (terminal-spark) or any project that runs PySpark. Auto-invoke when the user writes `spark` or `pyspark`.
---

# PySpark on Hopsworks (Spark Connect)

This skill covers **PySpark** (Python), which is the common path on Hopsworks: interactively in the terminal-spark image (Spark Connect) and as `--type PYSPARK` jobs. For **JVM/Scala/Java Spark** packaged as a JAR, there is no Spark Connect session to build — submit it as a `--type SPARK` job via the **hops-job** skill (`hops job deploy <name> <app.jar> --type SPARK`); the Delta/catalog wiring below is configured cluster-side for those, not in user code.

The terminal-spark image runs Spark in **Spark Connect** mode by default. The local Spark Connect server is started by the entrypoint and the connection URI is exported into the user's shell as `SPARK_REMOTE` (`sc://localhost:15002`). PySpark reads `SPARK_REMOTE` automatically, so user code must **not** hard-code `.remote("sc://localhost:15002")` — that path turns into a brittle smoke test that fails the moment the port shifts or the script is run inside a job pod (where the Connect server lives somewhere else).

Run Spark Connect programs with python3, not spark-submit.

## The two settings every Spark Connect session must enable

Hopsworks reads/writes offline feature groups through Delta Lake. A bare `SparkSession.builder.getOrCreate()` from PySpark gives you a session that can talk to Spark Connect but **does not** load the Delta extensions or rewire the default catalog to `DeltaCatalog`. The result: `spark.read.format("delta")...` fails with "no DataSource named delta", `spark.sql("CREATE TABLE ... USING delta")` errors, and `fg.read()` against any Delta-backed offline feature group returns nothing.

Always set:

| Config key | Value |
|---|---|
| `spark.sql.extensions` | `io.delta.sql.DeltaSparkSessionExtension` |
| `spark.sql.catalog.spark_catalog` | `org.apache.spark.sql.delta.catalog.DeltaCatalog` |

## Canonical session builder

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

## Anti-patterns to fix on sight

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

## When to override `SPARK_REMOTE`

Only when running outside the terminal-spark pod and pointing at a remote Spark Connect server you control (uncommon for Hopsworks workloads). Even then, prefer setting `SPARK_REMOTE=sc://host:port` in the environment over hard-coding `.remote(...)` in the script — keeps the same script runnable inside the terminal where the env var already points at the right place.

## Notes

- `getOrCreate()` is fine; the image starts the Connect server before the user's first shell.
- Do not pass `master(...)` — Spark Connect does not honour it and it's a leftover from spark-submit-style code.
- Use `pyspark` from the venv (`PYSPARK_PYTHON` is preset), not a fresh `pip install pyspark` — that breaks the JAR/Python ABI Hopsworks bakes into the image.

## See also

- **hops-fg** / **hops-fv** — the `fg.read()` / `fg.insert()` paths this session powers.
- **hops-job** — run a PySpark script as a scheduled Hopsworks job.

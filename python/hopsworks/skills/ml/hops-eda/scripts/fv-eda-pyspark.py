# ruff: noqa: INP001
"""Exploratory data analysis for a Hopsworks Feature View (PySpark).

Same EDA as fv-eda.py but uses Spark so it scales to datasets that don't
fit in memory. Use this when the feature view is too large for Polars.

Usage:
    python3 fv-eda-pyspark.py <feature_view_name> [feature_view_version] [start_time] [end_time]

start_time/end_time accept the same formats as fv.query.read():
%Y-%m-%d, %Y-%m-%d %H:%M, %Y-%m-%d %H:%M:%S, or Unix epoch seconds (int).

When start_time and end_time are passed, the data is filtered at query
time via fv.query.read(start_time=..., end_time=...). This requires the
underlying feature group to have an event_time column.
"""

from __future__ import annotations

import sys

import hopsworks
from hopsworks import build_spark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NumericType,
    ShortType,
    StringType,
    TimestampType,
)


_NUMERIC_TYPES = (NumericType, IntegerType, LongType, ShortType, FloatType, DoubleType)
_TEMPORAL_TYPES = (TimestampType, DateType)


def _semantic_type(dtype, n_unique: int, n_rows: int) -> str:
    if isinstance(dtype, ArrayType):
        return "list/embedding"
    if isinstance(dtype, _TEMPORAL_TYPES):
        return "datetime"
    if isinstance(dtype, BooleanType):
        return "categorical (bool)"
    if isinstance(dtype, _NUMERIC_TYPES):
        if n_rows > 0 and n_unique <= max(20, n_rows // 100):
            return "categorical (ordinal)"
        return "numerical"
    if isinstance(dtype, StringType):
        return "categorical"
    return str(dtype.simpleString())


def _numeric_stats(df: DataFrame, name: str) -> dict[str, float | None]:
    quoted = f"`{name}`"
    row = df.select(
        F.mean(quoted).alias("mean"),
        F.stddev(quoted).alias("std"),
        F.min(quoted).alias("min"),
        F.max(quoted).alias("max"),
    ).collect()[0]
    pct = df.approxQuantile(name, [0.25, 0.5, 0.75], 0.01)
    p25, p50, p75 = (pct + [None] * 3)[:3]
    return {
        "mean": row["mean"],
        "std": row["std"],
        "min": row["min"],
        "p25": p25,
        "p50": p50,
        "p75": p75,
        "max": row["max"],
    }


def _top_categorical(df: DataFrame, name: str, top_k: int = 5) -> list[tuple[str, int]]:
    rows = (
        df.filter(F.col(name).isNotNull())
        .groupBy(name)
        .agg(F.count("*").alias("count"))
        .orderBy(F.col("count").desc())
        .limit(top_k)
        .collect()
    )
    return [(str(r[name]), int(r["count"])) for r in rows]


def _list_stats(df: DataFrame, name: str) -> dict[str, object]:
    quoted = f"`{name}`"
    lens = df.filter(F.col(name).isNotNull()).select(F.size(quoted).alias("len"))
    row = lens.agg(
        F.mean("len").alias("avg"),
        F.min("len").alias("min"),
        F.max("len").alias("max"),
    ).collect()[0]
    return {"avg_len": row["avg"], "min_len": row["min"], "max_len": row["max"]}


def _temporal_range(df: DataFrame, name: str) -> tuple[object, object]:
    quoted = f"`{name}`"
    row = df.agg(F.min(quoted).alias("min"), F.max(quoted).alias("max")).collect()[0]
    return row["min"], row["max"]


def analyze(df: DataFrame) -> None:
    df = df.cache()
    n_rows = df.count()
    n_cols = len(df.columns)
    print(f"\nRows: {n_rows:,}    Columns: {n_cols}")
    print("=" * 80)

    null_counts = (
        df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
        .collect()[0]
        .asDict()
    )

    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType
        null_count = int(null_counts.get(name) or 0)
        null_pct = (null_count / n_rows * 100) if n_rows else 0.0

        if isinstance(dtype, ArrayType):
            n_unique = 0
        else:
            n_unique = df.select(F.col(name)).distinct().count()

        sem = _semantic_type(dtype, n_unique, n_rows)
        print(f"\n[{name}]  dtype={dtype.simpleString()}  semantic={sem}")
        print(f"  nulls: {null_count:,} ({null_pct:.2f}%)    unique: {n_unique:,}")

        if isinstance(dtype, ArrayType):
            stats = _list_stats(df, name)
            print(
                f"  list lengths: avg={stats['avg_len']}  "
                f"min={stats['min_len']}  max={stats['max_len']}"
            )
        elif isinstance(dtype, _NUMERIC_TYPES) and sem == "numerical":
            stats = _numeric_stats(df, name)

            def fmt(x):
                return (
                    f"{x:.4g}"
                    if isinstance(x, (int, float)) and x is not None
                    else str(x)
                )

            print(
                f"  mean={fmt(stats['mean'])}  std={fmt(stats['std'])}  "
                f"min={fmt(stats['min'])}  p25={fmt(stats['p25'])}  "
                f"p50={fmt(stats['p50'])}  p75={fmt(stats['p75'])}  "
                f"max={fmt(stats['max'])}"
            )
        elif isinstance(dtype, _TEMPORAL_TYPES):
            tmin, tmax = _temporal_range(df, name)
            print(f"  range: {tmin} → {tmax}")
        else:
            top = _top_categorical(df, name)
            top_str = ", ".join(f"{v!r}={c}" for v, c in top)
            print(f"  top: {top_str}")

    print("\n" + "=" * 80)
    print("Missing data summary (columns with any nulls):")
    missing = [(c, n) for c, n in null_counts.items() if n and n > 0]
    if not missing:
        print("  none")
    else:
        for name, count in sorted(missing, key=lambda x: -x[1]):
            pct = count / n_rows * 100 if n_rows else 0.0
            print(f"  {name}: {count:,} ({pct:.2f}%)")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    fv_name = argv[1]
    fv_version = int(argv[2]) if len(argv) >= 3 else 1
    start_time = argv[3] if len(argv) >= 4 else None
    end_time = argv[4] if len(argv) >= 5 else None

    # Build the SparkSession before login so Hopsworks attaches to it.
    build_spark(app_name=f"fv-eda-{fv_name}")

    project = hopsworks.login()
    fs = project.get_feature_store()
    fv = fs.get_feature_view(name=fv_name, version=fv_version)

    print(f"Feature view: {fv_name} v{fv_version}")
    if start_time or end_time:
        print(f"Filter: start_time={start_time!r}  end_time={end_time!r}")
        df = fv.query.read(
            start_time=start_time,
            end_time=end_time,
            dataframe_type="spark",
        )
    else:
        df = fv.query.read(dataframe_type="spark")

    analyze(df)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

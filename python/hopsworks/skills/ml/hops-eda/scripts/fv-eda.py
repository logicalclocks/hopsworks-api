# ruff: noqa: INP001
"""Exploratory data analysis for a Hopsworks Feature View (Polars).

Usage:
    python3 fv-eda.py <feature_view_name> [feature_view_version] [start_time] [end_time]

start_time/end_time accept the same formats as fv.query.read():
%Y-%m-%d, %Y-%m-%d %H:%M, %Y-%m-%d %H:%M:%S, or Unix epoch seconds (int).

When start_time and end_time are passed, the data is filtered at query
time via fv.query.read(start_time=..., end_time=...). This requires the
underlying feature group to have an event_time column.
"""

from __future__ import annotations

import sys

import hopsworks
import polars as pl


_NUMERIC_DTYPES = (
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
    pl.Float32,
    pl.Float64,
)
_TEMPORAL_DTYPES = (pl.Date, pl.Datetime, pl.Time, pl.Duration)
_LIST_DTYPES = (pl.List, pl.Array)


def _semantic_type(dtype: pl.DataType, n_unique: int, n_rows: int) -> str:
    if isinstance(dtype, _LIST_DTYPES):
        return "list/embedding"
    if isinstance(dtype, _TEMPORAL_DTYPES):
        return "datetime"
    if dtype == pl.Boolean:
        return "categorical (bool)"
    if isinstance(dtype, _NUMERIC_DTYPES):
        # Heuristic: small int with low cardinality is likely categorical.
        if n_rows > 0 and n_unique <= max(20, n_rows // 100):
            return "categorical (ordinal)"
        return "numerical"
    if dtype == pl.Utf8 or dtype == pl.Categorical:
        return "categorical"
    return str(dtype)


def _describe_numeric(series: pl.Series) -> dict[str, float | None]:
    s = series.drop_nulls()
    if s.len() == 0:
        return dict.fromkeys(("mean", "std", "min", "p25", "p50", "p75", "max"))
    return {
        "mean": float(s.mean()),
        "std": float(s.std()) if s.len() > 1 else 0.0,
        "min": float(s.min()),
        "p25": float(s.quantile(0.25)),
        "p50": float(s.quantile(0.5)),
        "p75": float(s.quantile(0.75)),
        "max": float(s.max()),
    }


def _describe_categorical(series: pl.Series, top_k: int = 5) -> dict[str, object]:
    s = series.drop_nulls()
    n_unique = int(s.n_unique()) if s.len() > 0 else 0
    top = []
    if s.len() > 0:
        vc = s.value_counts(sort=True).head(top_k)
        value_col, count_col = vc.columns[0], vc.columns[1]
        top = [
            (str(row[0]), int(row[1]))
            for row in vc.select([value_col, count_col]).iter_rows()
        ]
    return {"n_unique": n_unique, "top": top}


def _describe_list(series: pl.Series) -> dict[str, object]:
    s = series.drop_nulls()
    if s.len() == 0:
        return {"avg_len": None, "min_len": None, "max_len": None}
    lengths = s.list.len()
    return {
        "avg_len": float(lengths.mean()),
        "min_len": int(lengths.min()),
        "max_len": int(lengths.max()),
    }


def analyze(df: pl.DataFrame) -> None:
    n_rows = df.height
    n_cols = df.width
    print(f"\nRows: {n_rows:,}    Columns: {n_cols}")
    print("=" * 80)

    for name in df.columns:
        col = df[name]
        dtype = col.dtype
        null_count = int(col.null_count())
        null_pct = (null_count / n_rows * 100) if n_rows else 0.0
        n_unique = int(col.n_unique()) if not isinstance(dtype, _LIST_DTYPES) else 0
        sem = _semantic_type(dtype, n_unique, n_rows)

        print(f"\n[{name}]  dtype={dtype}  semantic={sem}")
        print(f"  nulls: {null_count:,} ({null_pct:.2f}%)    unique: {n_unique:,}")

        if isinstance(dtype, _LIST_DTYPES):
            stats = _describe_list(col)
            print(
                f"  list lengths: avg={stats['avg_len']}  min={stats['min_len']}  max={stats['max_len']}"
            )
        elif isinstance(dtype, _NUMERIC_DTYPES) and sem == "numerical":
            stats = _describe_numeric(col)
            print(
                f"  mean={stats['mean']:.4g}  std={stats['std']:.4g}  "
                f"min={stats['min']:.4g}  p25={stats['p25']:.4g}  "
                f"p50={stats['p50']:.4g}  p75={stats['p75']:.4g}  max={stats['max']:.4g}"
            )
        elif isinstance(dtype, _TEMPORAL_DTYPES):
            s = col.drop_nulls()
            if s.len() > 0:
                print(f"  range: {s.min()} → {s.max()}")
        else:
            stats = _describe_categorical(col)
            top_str = ", ".join(f"{v!r}={c}" for v, c in stats["top"])
            print(f"  top: {top_str}")

    print("\n" + "=" * 80)
    print("Missing data summary (columns with any nulls):")
    missing = [
        (name, int(df[name].null_count()))
        for name in df.columns
        if df[name].null_count() > 0
    ]
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

    project = hopsworks.login()
    fs = project.get_feature_store()
    fv = fs.get_feature_view(name=fv_name, version=fv_version)

    print(f"Feature view: {fv_name} v{fv_version}")
    if start_time or end_time:
        print(f"Filter: start_time={start_time!r}  end_time={end_time!r}")
        df = fv.query.read(
            start_time=start_time,
            end_time=end_time,
            dataframe_type="polars",
        )
    else:
        df = fv.query.read(dataframe_type="polars")

    analyze(df)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

# ruff: noqa: INP001
"""Whether a table built from a bronze feature group should be partitioned by time.

Reads the bronze table's Parquet files where the Hopsworks terminal mounts the
feature store (/hopsfs/featurestore/<project>_featurestore.db/<fg>_<version>):
their sizes, row counts, and the minimum and maximum of the time column from
the Parquet footers, without reading any data. From the bytes per hour, day
and week over that span it recommends no partitioning, or partitioning by the
hour, day or week, and prints the evidence and the decision as JSON.

    python partition_advisor.py /hopsfs/featurestore/acme_featurestore.db/orders_1 --time-column ordered_at
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


MIB = 1024 * 1024
GIB = 1024 * MIB
# Below this a table is small enough for one partition kept compact.
SMALL_TABLE_BYTES = 1 * GIB
SMALL_TABLE_ROWS = 10_000_000
# A partition should hold at least this much, or partitioning makes small files.
MIN_PARTITION_BYTES = 256 * MIB
# More partitions than this strain the table's metadata and query planning.
MAX_PARTITIONS = 50_000
GRANULARITIES = (("hour", 3600), ("day", 86_400), ("week", 7 * 86_400))
METADATA_DIRS = {"_delta_log", "metadata", ".hoodie", "_SUCCESS"}


def data_files(table: Path) -> list[Path]:
    """The table's Parquet data files, metadata directories left out."""
    return sorted(
        p
        for p in table.rglob("*.parquet")
        if not METADATA_DIRS.intersection(p.relative_to(table).parts)
    )


def _seconds(value: object) -> float | None:
    if isinstance(value, datetime):
        moment = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return moment.timestamp()
    if hasattr(value, "toordinal"):  # a date
        return datetime(
            value.year, value.month, value.day, tzinfo=timezone.utc
        ).timestamp()
    if isinstance(value, (int, float)):
        # Epoch milliseconds when too large for seconds in this century.
        return value / 1000 if value > 1e11 else float(value)
    return None


def evidence(table: Path, time_column: str | None) -> dict:
    """Bytes, rows and the time span of the table's data files."""
    import pyarrow.parquet as pq

    files = data_files(table)
    total_bytes = rows = 0
    low = high = None
    for path in files:
        total_bytes += path.stat().st_size
        meta = pq.read_metadata(path)
        rows += meta.num_rows
        if not time_column:
            continue
        names = [meta.schema.column(i).name for i in range(meta.num_columns)]
        if time_column not in names:
            continue
        index = names.index(time_column)
        for group in range(meta.num_row_groups):
            stats = meta.row_group(group).column(index).statistics
            if not stats or not stats.has_min_max:
                continue
            lo, hi = _seconds(stats.min), _seconds(stats.max)
            if lo is not None:
                low = lo if low is None else min(low, lo)
            if hi is not None:
                high = hi if high is None else max(high, hi)
    span = (high - low) if low is not None and high is not None else None
    return {
        "table": str(table),
        "files": len(files),
        "total_bytes": total_bytes,
        "rows": rows,
        "time_column": time_column,
        "time_min": datetime.fromtimestamp(low, timezone.utc).isoformat()
        if low is not None
        else None,
        "time_max": datetime.fromtimestamp(high, timezone.utc).isoformat()
        if high is not None
        else None,
        "span_seconds": span,
    }


def recommend(facts: dict) -> dict:
    """No partitioning, or the finest time grain whose partitions are large enough."""
    total, rows, span = facts["total_bytes"], facts["rows"], facts["span_seconds"]
    if total < SMALL_TABLE_BYTES and rows < SMALL_TABLE_ROWS:
        return {
            "decision": "none",
            "why": f"{total / GIB:.2f} GiB and {rows:,} rows: small enough for one partition; keep its files compact instead",
        }
    if not span:
        return {
            "decision": "none",
            "why": "no time span could be read from the time column's Parquet statistics; partition only once the arrival or event time column is known",
        }
    for grain, seconds in GRANULARITIES:
        partitions = max(1.0, span / seconds)
        per_partition = total / partitions
        if per_partition >= MIN_PARTITION_BYTES and partitions <= MAX_PARTITIONS:
            return {
                "decision": "partition",
                "granularity": grain,
                "partitions": round(partitions),
                "bytes_per_partition": int(per_partition),
                "why": f"{total / GIB:.1f} GiB over {span / 86_400:.0f} days: about {per_partition / MIB:.0f} MiB per {grain}",
            }
    return {
        "decision": "none",
        "why": f"{total / GIB:.1f} GiB over {span / 86_400:.0f} days: even weekly partitions would hold under {MIN_PARTITION_BYTES // MIB} MiB; cluster on the time column instead",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table", type=Path)
    parser.add_argument("--time-column", help="The arrival or event time column.")
    args = parser.parse_args()
    if not args.table.is_dir():
        raise SystemExit(f"{args.table}: not a directory")
    facts = evidence(args.table, args.time_column)
    print(json.dumps({**facts, "recommendation": recommend(facts)}, indent=2))


if __name__ == "__main__":
    main()

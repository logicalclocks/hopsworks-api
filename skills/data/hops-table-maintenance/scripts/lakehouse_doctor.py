#!/usr/bin/env python3
"""Deterministic table-health scanner and planner for offline feature groups.

analyze: read Delta/Iceberg/Hudi metadata for one table and emit evidence JSON.
plan:    turn evidence JSON into a reviewable maintenance plan (YAML on stdout).

The section names in the evidence JSON follow Adobe Lake Pulse's health report
(file organization, partition statistics, snapshot history, storage
efficiency) so a native scanner can replace this script without changing the
plan phase. Execution is deliberately absent: maintenance runs through Spark
after human approval, never from here.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

SMALL_FILE_BYTES = 32 * 1024 * 1024
TARGET_FILE_BYTES = 512 * 1024 * 1024
OVERSIZED_FILE_BYTES = 2 * 1024 * 1024 * 1024


def detect_format(table: Path) -> str:
    if (table / "_delta_log").is_dir():
        return "delta"
    if (table / "metadata").is_dir() and list((table / "metadata").glob("*.metadata.json")):
        return "iceberg"
    if (table / ".hoodie").is_dir():
        return "hudi"
    raise SystemExit(f"{table}: no _delta_log/, metadata/*.metadata.json or .hoodie/ found")


def file_organization(sizes: list[int]) -> dict:
    if not sizes:
        return {"active_files": 0}
    return {
        "active_files": len(sizes),
        "total_bytes": sum(sizes),
        "median_file_bytes": int(statistics.median(sizes)),
        "small_file_ratio": round(sum(1 for s in sizes if s < SMALL_FILE_BYTES) / len(sizes), 3),
        "oversized_files": sum(1 for s in sizes if s > OVERSIZED_FILE_BYTES),
        "estimated_compaction_rewrite_bytes": sum(s for s in sizes if s < SMALL_FILE_BYTES),
    }


def partition_statistics(per_partition: dict[str, dict]) -> dict:
    if not per_partition:
        return {"partition_count": 0}
    byte_counts = [p["bytes"] for p in per_partition.values()]
    median_bytes = statistics.median(byte_counts) or 1
    return {
        "partition_count": len(per_partition),
        "max_partition_bytes": max(byte_counts),
        "median_partition_bytes": int(median_bytes),
        "skew_ratio": round(max(byte_counts) / median_bytes, 1),
        "partitions_below_small_file_size": sum(1 for b in byte_counts if b < SMALL_FILE_BYTES),
    }


def column_overlap(stats: dict[str, list[tuple]]) -> dict:
    """Fraction of adjacent file pairs whose [min,max] ranges interleave.

    Files sorted by min: a pair overlaps when the earlier file's max exceeds
    the later file's min. 0.0 means perfectly clustered for that column, 1.0
    means every file's range straddles its neighbour and an equality filter
    scans everything.
    """
    out = {}
    for col, ranges in stats.items():
        clean = [(lo, hi) for lo, hi in ranges if lo is not None and hi is not None]
        if len(clean) < 2:
            continue
        try:
            clean.sort(key=lambda r: r[0])
            overlapping = sum(1 for a, b in zip(clean, clean[1:]) if a[1] > b[0])
        except TypeError:
            continue
        out[col] = round(overlapping / (len(clean) - 1), 3)
    return out


def analyze_delta(table: Path) -> dict:
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise SystemExit("deltalake is not installed: uv pip install deltalake")

    dt = DeltaTable(str(table))
    # deltalake >= 1.0 returns an arro3 table with no to_pylist; pyarrow
    # accepts it through the Arrow C stream interface either way.
    import pyarrow
    adds = pyarrow.table(dt.get_add_actions(flatten=True)).to_pylist()
    sizes = [a["size_bytes"] for a in adds]

    per_partition: dict[str, dict] = {}
    part_cols = [k[len("partition."):] for k in (adds[0] if adds else {}) if k.startswith("partition.")]
    for a in adds:
        key = "/".join(f"{c}={a.get('partition.' + c)}" for c in part_cols) or "<unpartitioned>"
        slot = per_partition.setdefault(key, {"files": 0, "bytes": 0})
        slot["files"] += 1
        slot["bytes"] += a["size_bytes"]

    stat_cols = sorted({k[len("min."):] for k in (adds[0] if adds else {}) if k.startswith("min.")})
    ranges = {c: [(a.get(f"min.{c}"), a.get(f"max.{c}")) for a in adds] for c in stat_cols}

    history = dt.history()
    return {
        "file_organization": file_organization(sizes),
        "partition_statistics": partition_statistics(per_partition),
        "partition_columns": part_cols,
        "column_overlap": column_overlap(ranges),
        "snapshot_history": {"version": dt.version(), "commits_retained": len(history)},
        "format_specific": {
            "deletion_vectors": sum(1 for a in adds if a.get("deletionVector.storageType")),
        },
    }


def analyze_iceberg(table: Path) -> dict:
    try:
        from pyiceberg.table import StaticTable
    except ImportError:
        raise SystemExit("pyiceberg is not installed: uv pip install pyiceberg")

    metadata = sorted((table / "metadata").glob("*.metadata.json"))[-1]
    t = StaticTable.from_metadata(str(metadata))
    files = t.inspect.files().to_pylist()
    sizes = [f["file_size_in_bytes"] for f in files]

    per_partition: dict[str, dict] = {}
    for f in files:
        key = str(f.get("partition")) or "<unpartitioned>"
        slot = per_partition.setdefault(key, {"files": 0, "bytes": 0})
        slot["files"] += 1
        slot["bytes"] += f["file_size_in_bytes"]

    snapshots = t.inspect.snapshots().to_pylist()
    manifests = t.inspect.manifests().to_pylist()
    deletes = t.inspect.delete_files().to_pylist()
    return {
        "file_organization": file_organization(sizes),
        "partition_statistics": partition_statistics(per_partition),
        "partition_columns": [f.name for f in t.spec().fields],
        "column_overlap": {},
        "snapshot_history": {"snapshots": len(snapshots)},
        "format_specific": {
            "delete_files": len(deletes),
            "delete_file_ratio": round(len(deletes) / max(len(files), 1), 3),
            "manifests": len(manifests),
        },
    }


def _hudi_active_base_files(table: Path) -> list[Path]:
    """The latest base-file version per Hudi file group.

    Base files are named <fileId>_<writeToken>_<instantTime>.parquet and every
    rewrite leaves the previous versions on disk until cleaning. Counting all
    parquet objects therefore inflates "active files" with obsolete slices and
    recommends compaction a healthy table does not need; only the newest
    instant per fileId within its partition is live.
    """
    latest: dict[tuple, tuple] = {}
    for p in table.rglob("*.parquet"):
        if ".hoodie" in p.parts:
            continue
        parts = p.stem.split("_")
        if len(parts) < 3:
            latest[(p.parent, p.stem)] = ("", p)
            continue
        key = (p.parent, parts[0])
        instant = parts[-1]
        if key not in latest or instant > latest[key][0]:
            latest[key] = (instant, p)
    return [p for _, p in latest.values()]


def analyze_hudi(table: Path) -> dict:
    # Basic support: file-listing heuristics only. Deep stats (pending
    # compactions, clustering plans) belong to Spark's Hudi procedures.
    active = _hudi_active_base_files(table)
    parquet = [p.stat().st_size for p in active]
    logs = [p for p in table.rglob("*.log.*") if ".hoodie" not in p.parts]
    timeline = list((table / ".hoodie").glob("*.commit")) + list((table / ".hoodie").glob("*.deltacommit"))

    per_partition: dict[str, dict] = {}
    for p in active:
        key = str(p.parent.relative_to(table)) or "<unpartitioned>"
        slot = per_partition.setdefault(key, {"files": 0, "bytes": 0})
        slot["files"] += 1
        slot["bytes"] += p.stat().st_size

    return {
        "file_organization": file_organization(parquet),
        "partition_statistics": partition_statistics(per_partition),
        "partition_columns": [],
        "column_overlap": {},
        "snapshot_history": {"commits_on_timeline": len(timeline)},
        "format_specific": {
            "log_files": len(logs),
            "log_to_base_ratio": round(len(logs) / max(len(parquet), 1), 3),
        },
    }


def cmd_analyze(args: argparse.Namespace) -> None:
    table = Path(args.table).resolve()
    fmt = args.format if args.format != "auto" else detect_format(table)
    evidence = {"table": str(table), "format": fmt}
    evidence.update({"delta": analyze_delta, "iceberg": analyze_iceberg, "hudi": analyze_hudi}[fmt](table))
    text = json.dumps(evidence, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(text + "\n")
    print(text)


def yaml_action(action: dict) -> str:
    # json.dumps output is valid YAML for scalars and flow lists, and gets the
    # booleans lowercased, which YAML 1.2 requires.
    lines = ["  - " + f"type: {action.pop('type')}"]
    lines += [f"    {k}: {json.dumps(v)}" for k, v in action.items()]
    return "\n".join(lines)


def cmd_plan(args: argparse.Namespace) -> None:
    e = json.loads(Path(args.evidence).read_text())
    fo, ps = e.get("file_organization", {}), e.get("partition_statistics", {})
    fs = e.get("format_specific", {})
    filter_cols = [c for c in (args.filter_columns or "").split(",") if c]
    actions = []

    if fo.get("active_files", 0) > 16 and fo.get("small_file_ratio", 0) > 0.5:
        actions.append({
            "type": "compact",
            "target_file_size_mb": TARGET_FILE_BYTES // (1024 * 1024),
            "scope": "start with the most recent partitions, widen after verification",
            "estimated_rewrite_gb": round(fo.get("estimated_compaction_rewrite_bytes", 0) / 2**30, 1),
            "api": "FeatureGroup.optimize() (Delta/Iceberg; safe incremental defaults)",
            "confidence": 0.9,
            "requires_approval": True,
        })
    if ps.get("skew_ratio", 0) > 10 and ps.get("partition_count", 0) > 1:
        actions.append({
            "type": "review_partitioning",
            "detail": f"largest partition is {ps['skew_ratio']}x the median; "
                      "consider a coarser transform or bucket/clustering instead",
            "confidence": 0.6,
            "requires_approval": True,
        })
    if ps.get("partition_count", 0) > 5000 and ps.get("partitions_below_small_file_size", 0) > 2500:
        actions.append({
            "type": "coarsen_partitioning",
            "detail": "thousands of tiny partition directories",
            "confidence": 0.7,
            "requires_approval": True,
        })
    # Clustering needs workload evidence: file stats alone cannot tell a hot
    # filter column from an incidental one, and clustering on the wrong key
    # is an expensive full rewrite that helps nothing.
    overlap = e.get("column_overlap", {})
    hot = [c for c in filter_cols if overlap.get(c, 0) > 0.5]
    if hot:
        actions.append({
            "type": "sort_or_cluster",
            "columns": hot,
            "detail": "files interleave heavily on columns your queries filter by",
            "confidence": 0.8,
            "requires_approval": True,
        })
    if fs.get("delete_file_ratio", 0) > 0.1:
        actions.append({"type": "rewrite_data_and_delete_files",
                        "scope": "restrict with a where filter; widen after verification",
                        "confidence": 0.85, "requires_approval": True})
    if fs.get("manifests", 0) > 100:
        actions.append({"type": "rewrite_manifests", "confidence": 0.85, "requires_approval": True})
    snapshots = e.get("snapshot_history", {})
    if max(snapshots.get("snapshots", 0), snapshots.get("commits_retained", 0)) > 100:
        actions.append({"type": "expire_snapshots",
                        "detail": "destructive: removes time travel history",
                        "confidence": 0.7, "requires_approval": True})
    if fs.get("log_to_base_ratio", 0) > 0.5:
        # Hopsworks runs Hudi maintenance through inline clustering on writes and rejects
        # ad hoc procedures (FeatureGroup.optimize() refuses HUDI), so this is a review
        # item, never a command to run.
        actions.append({"type": "review_hudi_write_config",
                        "detail": "high log-to-base ratio; Hudi layout maintenance runs "
                                  "through inline clustering on writes in Hopsworks, so "
                                  "review the feature group's write configuration rather "
                                  "than running ad hoc procedures",
                        "confidence": 0.8, "requires_approval": True})

    print(f"table: {e['table']}")
    print(f"format: {e['format']}")
    if filter_cols:
        print(f"workload_filter_columns: {','.join(filter_cols)}")
    else:
        print("workload_filter_columns: none  # clustering suppressed without workload evidence")
    print("actions:" if actions else "actions: []  # table looks healthy")
    for a in actions:
        print(yaml_action(a))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("analyze", help="scan one table and emit evidence JSON")
    a.add_argument("table")
    a.add_argument("--format", choices=["auto", "delta", "iceberg", "hudi"], default="auto")
    a.add_argument("--output")
    a.set_defaults(func=cmd_analyze)
    p = sub.add_parser("plan", help="turn evidence JSON into a plan YAML")
    p.add_argument("evidence")
    p.add_argument("--filter-columns")
    p.set_defaults(func=cmd_plan)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())

"""The health of a silver or gold medallion layer, for ``hops factory medallion status``.

`collect` gathers the facts: the layer's job runs, and for each feature group
it built (silver and rejects, or each data mart's gold tables) its rows, when
it was last written against its freshness target, in silver its share of
rejected rows against the quality gate, and its file layout, read from the table's files under the terminal's HopsFS mount by
the hops-table-maintenance scanner. The page is the ML system status page
(`health.render`), which draws the tables section when the facts have one.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hopsworks.cli import health


SCANNER = (
    Path(__file__).resolve().parents[1]
    / "skills"
    / "data"
    / "hops-table-maintenance"
    / "scripts"
    / "lakehouse_doctor.py"
)
# More small files than this, in a table with at least SMALL_FILES_MIN files,
# is a layout problem for hops-table-maintenance.
SMALL_FILE_RATIO = 0.5
SMALL_FILES_MIN = 50


def _scanner() -> Any:
    import importlib.util

    spec = importlib.util.spec_from_file_location("lakehouse_doctor", SCANNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _table_dir(project_name: str, name: str, version: int) -> Path:
    """Where the terminal mounts an offline feature group's table."""
    root = Path(os.environ.get("HOPSFS_MOUNT", "/hopsfs"))
    return (
        root
        / "featurestore"
        / f"{project_name.lower()}_featurestore.db"
        / f"{name}_{version}"
    )


def _last_delta_commit(table: Path) -> datetime | None:
    """The time of the latest commit in a Delta table's log, or None."""
    log = table / "_delta_log"
    commits = sorted(log.glob("*.json")) if log.is_dir() else []
    for path in reversed(commits[-5:]):
        for line in path.read_text(encoding="utf-8").splitlines():
            info = json.loads(line).get("commitInfo") if line.strip() else None
            if info and info.get("timestamp"):
                return datetime.fromtimestamp(info["timestamp"] / 1000, timezone.utc)
    return None


def _layout(table: Path) -> dict | None:
    if not table.is_dir():
        return None
    doctor = _scanner()
    fmt = doctor.detect_format(table)
    evidence = {"delta": doctor.analyze_delta, "iceberg": doctor.analyze_iceberg}.get(
        fmt, doctor.analyze_hudi
    )(table)
    return {
        "format": fmt,
        **(evidence.get("file_organization") or {}),
        "partitions": (evidence.get("partition_statistics") or {}).get(
            "partition_count"
        ),
    }


def _rows(conn: Any, name: str, version: int) -> int | None:
    try:
        cur = conn.cursor()
        cur.execute(f'select count(*) from "{name}_{version}"')
        return int(cur.fetchone()[0])
    except Exception:  # noqa: BLE001 - a table Trino cannot read is reported without rows
        return None


def _freshness(doc: dict, table: dict) -> float | None:
    """How stale a table may be: its data mart's target in gold, its cadence's job's in silver."""
    if table.get("mart"):
        for mart in doc.get("marts") or []:
            if mart.get("slug") == table["mart"]:
                return mart.get("freshness_hours")
        return None
    target = (doc.get("freshness") or {}).get("max_age_hours")
    if isinstance(target, dict):
        target = target.get(table.get("cadence") or "") or max(
            target.values(), default=None
        )
    return target


def _table(project: Any, conn: Any, table: dict, doc: dict, now: datetime) -> dict:
    name, version, kind = table["name"], int(table.get("version", 1)), table["kind"]
    path = _table_dir(getattr(project, "name", ""), name, version)
    fact: dict[str, Any] = {"name": name, "version": version, "kind": kind}
    if table.get("mart"):
        fact["mart"] = table["mart"]
    if not path.is_dir():
        fact["problems"] = ["missing: no table in the feature store"]
        return fact
    fact["rows"] = _rows(conn, name, version) if conn else None
    written = _last_delta_commit(path)
    fact["last_write"] = written.isoformat() if written else None
    fact["layout"] = _layout(path)
    problems = []
    target = _freshness(doc, table)
    if kind in ("silver", "gold") and written and target:
        age = (now - written) / timedelta(hours=1)
        fact["age_hours"] = round(age, 1)
        fact["max_age_hours"] = target
        if age > target:
            problems.append(f"stale: last written {age:.0f} h ago, target {target} h")
    layout = fact["layout"] or {}
    if (
        layout.get("active_files", 0) >= SMALL_FILES_MIN
        and layout.get("small_file_ratio", 0) > SMALL_FILE_RATIO
    ):
        problems.append(
            f"small files: {layout['small_file_ratio']:.0%} of {layout['active_files']} files; compact with hops-table-maintenance"
        )
    fact["problems"] = problems
    return fact


def _reject_shares(tables: list[dict], doc: dict) -> None:
    """Flag each silver table whose rejects table holds more than the quality gate allows."""
    limit = (doc.get("quality") or {}).get("max_reject_pct")
    rejects = {t["name"]: t for t in tables if t["kind"] == "rejects"}
    for table in tables:
        reject = rejects.get(f"{table['name']}_rejects")
        if table["kind"] != "silver" or not reject:
            continue
        good, bad = table.get("rows"), reject.get("rows")
        if good is None or bad is None or good + bad == 0:
            continue
        pct = round(100 * bad / (good + bad), 2)
        table["reject_pct"] = pct
        if limit is not None and pct > limit:
            table["problems"].append(f"rejects: {pct}% of rows, gate {limit}%")


def collect(project: Any, doc: dict, slug: str, hours: int = 24) -> dict:
    """The facts of a silver or gold layer's status report."""
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    from hopsworks.cli.commands.medallion import layer_jobs, layer_tables

    jobs = []
    for spec in layer_jobs(doc):
        try:
            jobs.append(health._job_runs(project, spec["name"], since))
        except Exception as exc:  # noqa: BLE001
            jobs.append({"name": spec["name"], "error": str(exc), "runs": []})
    try:
        conn = project.get_trino_api().connect(
            catalog="delta",
            schema=f"{getattr(project, 'name', '').lower()}_featurestore",
        )
    except Exception:  # noqa: BLE001 - row counts are left out without Trino
        conn = None
    tables = [_table(project, conn, t, doc, now) for t in layer_tables(doc)]
    if conn:
        conn.close()
    _reject_shares(tables, doc)
    failed = sum(
        1
        for j in jobs
        for r in j["runs"]
        if r.get("final_status") in ("FAILED", "KILLED")
    )
    troubled = [t["name"] for t in tables if t.get("problems")]
    layer = doc.get("layer") or {}
    return {
        "system": {
            "slug": slug,
            "name": layer.get("name") or slug,
            "type": f"{layer.get('kind', 'silver')} layer",
        },
        "generated": now.isoformat(timespec="seconds"),
        "hours": hours,
        "overall": "failing"
        if failed and troubled
        else "degraded"
        if failed or troubled
        else "healthy",
        "counts": {
            "jobs": len(jobs),
            "runs": sum(len(j["runs"]) for j in jobs),
            "failed_runs": failed,
            "services": 0,
            "unhealthy_services": 0,
            "tables": len(tables),
            "table_problems": len(troubled),
        },
        "jobs": jobs,
        "services": [],
        "tables": tables,
    }

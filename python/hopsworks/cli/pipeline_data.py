"""What a system's feature pipelines wrote, for ``hops factory system status``.

For every pipeline in `features.pipelines` of system.yaml, each output it
`writes` is checked over the report's window:

- a feature group: the rows and bytes its commits added, read from the Delta
  log under the terminal's HopsFS mount; with Trino and its event time, the
  share of nulls per column and the hours or days with no rows; and its file
  layout, as the analytics layer report reads it;
- files under a project path: how many were written and their size.

What the pipeline `reads` is measured the same way, so the report sets the
rows (or bytes) that came in against what went out. A data source outside
Hopsworks is listed but not measured.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hopsworks.cli import silver_status


# The newest Delta commits read for a window; a table committing more often
# than this per window is reported on its latest ones.
_DELTA_COMMITS = 2000


def _mount() -> Path:
    return Path(os.environ.get("HOPSFS_MOUNT", "/hopsfs"))


def _as_list(value: Any) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _ref(item: Any) -> dict | None:
    """One read or write as a dict: a feature group, a project path, or a data source."""
    if isinstance(item, str):
        named = re.match(r"^(\w+)(?: v(\d+))?$", item.strip())
        if named:
            return {
                "feature_group": named.group(1),
                "version": int(named.group(2) or 1),
            }
        return {"path": item.strip()} if "/" in item else None
    if isinstance(item, dict):
        if isinstance(item.get("feature_group"), str):
            return {**item, "version": int(item.get("version") or 1)}
        if isinstance(item.get("path"), str):
            return item
        if item.get("data_source") or item.get("connector"):
            return item
    return None


def _project_path(project_name: str, path: str) -> Path:
    """A path the build recorded, under the mount: /Projects/<p>/X, hopsfs://.../X, or X relative to the project."""
    path = re.sub(r"^hopsfs://[^/]*", "", path)
    path = re.sub(rf"^/?Projects/{re.escape(project_name)}/", "", path)
    path = re.sub(r"^/hopsfs/", "", path)
    return _mount() / path.lstrip("/")


def _added(metrics: dict) -> tuple[int, int]:
    """The rows and bytes one Delta commit added, from Spark's camelCase or delta-rs's snake_case metrics."""
    found = {
        re.sub(r"_", "", k).lower(): int(v or 0)
        for k, v in metrics.items()
        if str(v).isdigit()
    }
    # A merge's output rows include the unchanged rows it copied; only inserts and updates are new.
    if "numtargetrowsinserted" in found:
        rows = found["numtargetrowsinserted"] + found.get("numtargetrowsupdated", 0)
    else:
        rows = found.get("numoutputrows", found.get("numaddedrows", 0))
    size = (
        found.get("numoutputbytes")
        or found.get("numtargetbytesadded")
        or found.get("numaddedbytes", 0)
    )
    return rows, size


def delta_written(table: Path, since: datetime) -> dict | None:
    """The commits of a Delta table since `since`: their count, rows and bytes added, and the last one's time.

    Args:
        table: The table's location.
        since: The start of the window.

    Returns:
        The facts.
    """
    log = table / "_delta_log"
    if not log.is_dir():
        return None
    commits = rows = size = 0
    last: datetime | None = None
    for path in sorted(log.glob("*.json"), reverse=True)[:_DELTA_COMMITS]:
        info = None
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                info = json.loads(line).get("commitInfo") or info
        if not info or not info.get("timestamp"):
            continue
        when = datetime.fromtimestamp(info["timestamp"] / 1000, timezone.utc)
        last = last or when
        if when < since:
            break
        rows_added, bytes_added = _added(info.get("operationMetrics") or {})
        commits += 1
        rows += rows_added
        size += bytes_added
    return {
        "commits": commits,
        "rows": rows,
        "bytes": size,
        "last_write": last.isoformat() if last else None,
    }


def files_written(directory: Path, since: datetime) -> dict | None:
    """The files under `directory` modified since `since`: how many, their bytes, and the latest time.

    Args:
        directory: The directory.
        since: The start of the window.

    Returns:
        The facts.
    """
    if not directory.exists():
        return None
    files = (
        [directory]
        if directory.is_file()
        else [p for p in directory.rglob("*") if p.is_file()]
    )
    cutoff = since.timestamp()
    recent = [p.stat() for p in files]
    newest = max((s.st_mtime for s in recent), default=None)
    recent = [s for s in recent if s.st_mtime >= cutoff]
    return {
        "files": len(recent),
        "bytes": sum(s.st_size for s in recent),
        "last_write": datetime.fromtimestamp(newest, timezone.utc).isoformat()
        if newest
        else None,
    }


def _bucket(pipeline: dict) -> str | None:
    """`hour` or `day`: how often a scheduled pipeline should write, from its job's cron; None if unscheduled."""
    job = pipeline.get("job") if isinstance(pipeline.get("job"), dict) else {}
    schedule = job.get("schedule")
    cron = schedule.get("cron") if isinstance(schedule, dict) else schedule
    cron = cron or job.get("cron")
    if not isinstance(cron, str) or not cron.split():
        return None
    fields = cron.split()
    # Quartz has seconds first: s m h dom mon dow; Unix cron starts at minutes.
    hour = fields[2] if len(fields) >= 6 else fields[1] if len(fields) >= 2 else "*"
    return "hour" if hour == "*" or "/" in hour else "day"


def column_checks(
    conn: Any,
    table: str,
    event_time: str | None,
    since: datetime,
    bucket: str | None,
    hours: int,
) -> dict:
    """With Trino: per column, the share of nulls among the window's rows, and the buckets with no rows.

    Args:
        conn: A Trino connection.
        table: The table.
        event_time: Its event time column.
        since: The start of the window.
        bucket: The bucket width.
        hours: The window's length.

    Returns:
        The checks.
    """
    cur = conn.cursor()
    cur.execute(f'select * from "{table}" limit 0')
    columns = [d[0] for d in cur.description]
    where = ""
    if event_time:
        where = f""" where "{event_time}" >= from_iso8601_timestamp('{since.isoformat()}')"""
    counts = ", ".join(f'count("{c}")' for c in columns)
    cur.execute(f'select count(*), {counts} from "{table}"{where}')
    row = cur.fetchone()
    total = int(row[0])
    nulls = {
        c: round(100 * (total - int(n)) / total, 2)
        for c, n in zip(columns, row[1:], strict=True)
        if total and int(n) < total
    }
    # Without an event time the window cannot be selected, so the whole table is checked.
    result: dict[str, Any] = {
        "checked_rows": total,
        "checked": "window" if event_time else "table",
        "nulls": nulls,
    }
    if event_time and bucket:
        cur.execute(
            f"""select date_trunc('{bucket}', "{event_time}") from "{table}"{where} group by 1"""
        )
        seen = {_hour(r[0], bucket) for r in cur.fetchall() if r[0] is not None}
        step = timedelta(hours=1 if bucket == "hour" else 24)
        now = since + timedelta(hours=hours)
        start = _hour(since, bucket) + step
        expected = []
        while start + step <= now:
            expected.append(start)
            start += step
        result["missing"] = [b.isoformat() for b in expected if b not in seen]
    return result


def _hour(value: Any, bucket: str) -> datetime:
    when = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    when = when if when.tzinfo else when.replace(tzinfo=timezone.utc)
    when = when.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return when.replace(hour=0) if bucket == "day" else when


def _measure(project_name: str, ref: dict, since: datetime) -> dict:
    """How much one read or write took in over the window, where it can be measured."""
    if "feature_group" in ref:
        table = silver_status._table_dir(
            project_name, ref["feature_group"], ref["version"]
        )
        found = delta_written(table, since)
        return (
            {"kind": "feature group", "table": table, **(found or {})}
            if found
            else {"kind": "feature group", "table": table}
        )
    if "path" in ref:
        found = files_written(_project_path(project_name, ref["path"]), since)
        return (
            {"kind": "files", **(found or {})}
            if found
            else {"kind": "files", "missing": True}
        )
    return {"kind": "data source"}


def _name(ref: dict) -> str:
    if "feature_group" in ref:
        return f"{ref['feature_group']} v{ref['version']}"
    if "path" in ref:
        return ref["path"]
    return str(ref.get("data_source") or ref.get("connector"))


def _output(
    project_name: str, conn: Any, ref: dict, pipeline: dict, since: datetime, hours: int
) -> dict:
    measured = _measure(project_name, ref, since)
    table = measured.pop("table", None)
    fact: dict[str, Any] = {"name": _name(ref), **measured, "problems": []}
    problems = fact["problems"]
    if measured["kind"] == "feature group":
        if table is None or not table.is_dir():
            problems.append("missing: no table in the feature store")
            return fact
        fact["layout"] = silver_status._layout(table)
        if conn is not None:
            event_time = ref.get("event_time")
            try:
                checks = column_checks(
                    conn,
                    f"{ref['feature_group']}_{ref['version']}",
                    event_time if isinstance(event_time, str) else None,
                    since,
                    _bucket(pipeline),
                    hours,
                )
            except Exception as exc:  # noqa: BLE001 - a table Trino cannot read is reported without its columns
                fact["columns_error"] = str(exc).splitlines()[0][:200]
                checks = {}
            fact.update(checks)
            # Entirely null, or a null key, is always missing data; any other
            # column only above the pipeline's own quality.max_null_pct.
            keys = {*_as_list(ref.get("primary_key")), *_as_list(event_time)}
            limit = (pipeline.get("quality") or {}).get("max_null_pct")
            for column, pct in (checks.get("nulls") or {}).items():
                if pct >= 100:
                    problems.append(f"missing data: {column} is null in every row")
                elif column in keys:
                    problems.append(
                        f"missing data: key column {column} is null in {pct}% of rows"
                    )
                elif limit is not None and pct > float(limit):
                    problems.append(
                        f"missing data: {column} is null in {pct}% of rows, limit {limit}%"
                    )
            if checks.get("missing"):
                gaps = checks["missing"]
                problems.append(
                    f"missing data: no rows for {len(gaps)} {_bucket(pipeline)}s of the window, from {gaps[0]}"
                )
        layout = fact.get("layout") or {}
        if (
            layout.get("active_files", 0) >= silver_status.SMALL_FILES_MIN
            and layout.get("small_file_ratio", 0) > silver_status.SMALL_FILE_RATIO
        ):
            problems.append(
                f"small files: {layout['small_file_ratio']:.0%} of {layout['active_files']} files; compact with hops-table-maintenance"
            )
    elif measured.get("missing"):
        problems.append("missing: no files at this path")
    if (
        measured["kind"] != "data source"
        and _bucket(pipeline)
        and not (measured.get("commits") or measured.get("files"))
    ):
        problems.append(
            f"nothing written in the last {hours} h, though the pipeline is scheduled"
        )
    return fact


def collect(project: Any, doc: dict, hours: int = 24) -> list[dict]:
    """One fact per pipeline that writes: what came in, what went out, and the problems in what went out.

    Args:
        project: The project.
        doc: The system's system.yaml.
        hours: How far back to look.

    Returns:
        The facts.
    """
    pipelines = [
        p
        for p in _as_list((doc.get("features") or {}).get("pipelines"))
        if isinstance(p, dict)
    ]
    pipelines = [p for p in pipelines if p.get("writes")]
    if not pipelines:
        return []
    project_name = getattr(project, "name", "")
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    try:
        conn = project.get_trino_api().connect(
            catalog="delta", schema=f"{project_name.lower()}_featurestore"
        )
    except Exception:  # noqa: BLE001 - the column checks are left out without Trino
        conn = None
    facts = []
    for pipeline in pipelines:
        inputs = []
        for ref in filter(None, map(_ref, _as_list(pipeline.get("reads")))):
            measured = _measure(project_name, ref, since)
            measured.pop("table", None)
            inputs.append({"name": _name(ref), **measured})
        outputs = [
            _output(project_name, conn, ref, pipeline, since, hours)
            for ref in filter(None, map(_ref, _as_list(pipeline.get("writes"))))
        ]
        rows_in = sum(i.get("rows") or 0 for i in inputs)
        rows_out = sum(o.get("rows") or 0 for o in outputs)
        bytes_in = sum(i.get("bytes") or 0 for i in inputs)
        bytes_out = sum(o.get("bytes") or 0 for o in outputs)
        flow = []
        if (
            rows_in
            and not rows_out
            and any(o["kind"] == "feature group" for o in outputs)
        ):
            flow.append(f"{rows_in:,} rows came in but none were written")
        facts.append(
            {
                "name": pipeline.get("name")
                or (pipeline.get("job") or {}).get("name")
                or "pipeline",
                "engine": pipeline.get("engine"),
                "inputs": inputs,
                "outputs": outputs,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "bytes_in": bytes_in,
                "bytes_out": bytes_out,
                "flow_problems": flow,
                "problems": [p for o in outputs for p in o["problems"]] + flow,
            }
        )
    if conn is not None:
        conn.close()
    return facts

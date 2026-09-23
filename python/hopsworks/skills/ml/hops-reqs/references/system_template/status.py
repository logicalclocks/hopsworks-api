# ruff: noqa: INP001
"""Print the progress of an ML system from its system.yaml.

Runs from any terminal without a Claude session or the system lock, and changes
nothing, so it answers while `/hops-ml` is mid-phase in another terminal:

    python <slug>/status.py                 # the phase table, once
    python <slug>/status.py --watch 30      # redraw every 30 seconds
    python <slug>/status.py --cluster       # add what the cluster says about running work

Estimates come from measurements first and defaults second, as
hops-reqs/references/system-yaml.md ("Progress and estimates") states.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


try:
    import yaml
except ImportError:  # pragma: no cover - exercised only without PyYAML
    sys.exit("status.py needs PyYAML: pip install pyyaml")


PHASES = [
    ("reqs", "requirements"),
    ("data", "data"),
    ("features", "features"),
    ("train", "training"),
    ("infer", "inference"),
    ("app", "app"),
    ("verify", "verify"),
]
SATISFIED = {"met", "skipped", "accepted", "pass"}

DEFAULT_MINUTES = {
    "reqs": 10,
    "app": 10,
    "verify": 3,
    "data_existing_source": 5,
    "data_new_source": 20,
    "features_pipeline": 15,
    "infer_attempt": 3,
}


def parse_duration(text: str | float | None) -> timedelta | None:
    """Parse `90s`, `20m`, `8h` or `1h30m`; a bare number is minutes."""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return timedelta(minutes=text)
    parts = re.findall(r"(\d+(?:\.\d+)?)\s*([hms])", str(text))
    if not parts:
        return None
    unit = {"h": 3600, "m": 60, "s": 1}
    return timedelta(seconds=sum(float(n) * unit[u] for n, u in parts))


def parse_time(value: object) -> datetime | None:
    """Read a YAML timestamp, which PyYAML leaves as a string without seconds."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def fmt_duration(delta: timedelta | None) -> str:
    """Render a duration as `38m` or `1h45m`."""
    if delta is None:
        return ""
    minutes = max(0, round(delta.total_seconds() / 60))
    if minutes < 60:
        return f"{minutes}m"
    hours, rest = divmod(minutes, 60)
    return f"{hours}h{rest:02d}m" if rest else f"{hours}h"


def _block(doc: dict, key: str) -> dict:
    value = doc.get(key)
    return value if isinstance(value, dict) else {}


def _status(key: str, block: dict) -> str:
    if key == "verify":
        return block.get("status", "pending")
    return block.get("status", "pending") or "pending"


def _took(block: dict, now: datetime) -> timedelta | None:
    started = parse_time(block.get("started"))
    if started is None:
        return None
    finished = parse_time(block.get("finished")) or now
    return finished - started


def _default_estimate(phase: str, doc: dict) -> timedelta:
    req = _block(doc, "requirements")
    budget = req.get("budget") or {}
    minutes = DEFAULT_MINUTES
    if phase == "data":
        sources = req.get("data_sources") or []
        existing = sum(1 for s in sources if s.get("kind") == "feature_group")
        new = len(sources) - existing
        return timedelta(
            minutes=existing * minutes["data_existing_source"]
            + new * minutes["data_new_source"]
        )
    if phase == "features":
        pipelines = _block(doc, "features").get("pipelines") or []
        count = len(pipelines) or sum(
            1
            for f in req.get("features") or []
            if f.get("computed_in") in ("feature_pipeline", "streaming")
        )
        return timedelta(minutes=max(1, count) * minutes["features_pipeline"])
    if phase == "train":
        wall = parse_duration((budget.get("training") or {}).get("wall_clock"))
        return wall or timedelta(minutes=60)
    if phase == "infer":
        attempts = (budget.get("inference") or {}).get("max_attempts", 5)
        return timedelta(minutes=attempts * minutes["infer_attempt"])
    if phase == "app":
        if _block(doc, "app").get("wanted") is False:
            return timedelta(0)
        return timedelta(minutes=minutes["app"])
    return timedelta(minutes=minutes.get(phase, 5))


def _running_estimate(
    phase: str, block: dict, doc: dict, now: datetime
) -> tuple[timedelta, str]:
    """Remaining time for the phase in progress, and what it is based on."""
    elapsed = _took(block, now) or timedelta(0)
    budget = _block(doc, "requirements").get("budget") or {}
    if phase == "train":
        train_budget = budget.get("training") or {}
        runs = block.get("runs") or []
        done = [r for r in runs if r.get("state") in ("finished", "failed")]
        wall = parse_duration(train_budget.get("wall_clock")) or timedelta(minutes=60)
        left_by_wall = max(timedelta(0), wall - elapsed)
        if not done:
            return left_by_wall, "budget wall_clock"
        per_run = elapsed / len(done)
        left_runs = max(0, int(train_budget.get("max_runs", 5)) - len(done))
        return min(per_run * left_runs, left_by_wall), "runs so far"
    if phase == "infer":
        measured = block.get("measured") or []
        attempts = int((budget.get("inference") or {}).get("max_attempts", 5))
        if not measured:
            return (
                timedelta(minutes=attempts * DEFAULT_MINUTES["infer_attempt"]),
                "default",
            )
        per_attempt = elapsed / len(measured)
        return per_attempt * max(0, attempts - len(measured)), "attempts so far"
    default = _default_estimate(phase, doc)
    return max(timedelta(0), default - elapsed), "default"


def rows(doc: dict, now: datetime) -> list[dict]:
    """Compute one row per phase: status, start, duration, remaining, now."""
    progress = _block(_block(doc, "system"), "progress")
    table = []
    for phase, key in PHASES:
        block = _block(doc, key)
        status = _status(key, block)
        started = parse_time(block.get("started") or block.get("last_run"))
        row = {
            "phase": phase,
            "status": status,
            "started": started,
            "took": _took(block, now),
            "remaining": None,
            "basis": "",
            "now": "",
        }
        if status in SATISFIED:
            row["remaining"] = timedelta(0)
        elif status == "running":
            row["remaining"], row["basis"] = _running_estimate(phase, block, doc, now)
            if progress.get("phase") == phase:
                row["now"] = str(progress.get("now") or "")
        else:
            previous = row["took"] if block.get("finished") else None
            row["remaining"] = previous or _default_estimate(phase, doc)
            row["basis"] = "measured" if previous else "default"
            row["took"] = None
            if phase == "infer":
                attempts = (
                    (_block(doc, "requirements").get("budget") or {}).get("inference")
                    or {}
                ).get("max_attempts", 5)
                row["now"] = (
                    f"{attempts} attempts x ~{DEFAULT_MINUTES['infer_attempt']}m"
                )
        table.append(row)
    return table


def render(doc: dict, now: datetime) -> str:
    """Render the phase table and the summary line."""
    table = rows(doc, now)
    lines = [
        f"{'phase':<10} {'status':<9} {'started':<8} {'took':<6} {'remaining':<11} now"
    ]
    for row in table:
        started = row["started"].strftime("%H:%M") if row["started"] else ""
        remaining = (
            f"~{fmt_duration(row['remaining'])}"
            if row["remaining"] and row["status"] not in SATISFIED
            else ""
        )
        lines.append(
            f"{row['phase']:<10} {row['status']:<9} {started:<8} "
            f"{fmt_duration(row['took']):<6} {remaining:<11} {row['now']}".rstrip()
        )
    done = sum(1 for r in table if r["status"] in SATISFIED)
    left = sum((r["remaining"] or timedelta(0) for r in table), timedelta(0))
    spent = sum((r["took"] or timedelta(0) for r in table), timedelta(0))
    if left == timedelta(0):
        lines.append(f"{done} of {len(table)} phases done; nothing left to run")
    else:
        ends = (now + left).strftime("%H:%M")
        lines.append(
            f"{done} of {len(table)} phases done; about {fmt_duration(left)} left of "
            f"~{fmt_duration(left + spent)}; ends about {ends} unless something escalates"
        )
    return "\n".join(lines)


def _named_workloads(doc: dict) -> tuple[list[str], list[str]]:
    """Every job and deployment the YAML names, for the --cluster view."""
    jobs: list[str] = []
    for source in _block(doc, "data").values():
        if isinstance(source, dict):
            for part in ("backfill", "live"):
                name = (source.get(part) or {}).get("job")
                if name:
                    jobs.append(name)
    for pipeline in _block(doc, "features").get("pipelines") or []:
        name = (pipeline.get("job") or {}).get("name")
        if name:
            jobs.append(name)
    retrain = _block(_block(doc, "training"), "retraining").get("job")
    if retrain:
        jobs.append(retrain)
    inference = _block(doc, "inference")
    batch_job = _block(_block(inference, "batch"), "job").get("name")
    if batch_job:
        jobs.append(batch_job)
    deployments = [
        name
        for name in (
            _block(inference, "realtime").get("deployment"),
            _block(inference, "agent").get("deployment"),
        )
        if name
    ]
    return list(dict.fromkeys(jobs)), deployments


def _hops_json(args: list[str]) -> object:
    try:
        done = subprocess.run(
            ["hops", *args, "--json"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"error": str(exc)}
    if done.returncode != 0:
        return {"error": (done.stderr or done.stdout).strip().splitlines()[-1:]}
    try:
        return json.loads(done.stdout)
    except json.JSONDecodeError:
        return {"error": "unparseable output"}


def render_cluster(doc: dict) -> str:
    """Latest execution per named job and the state of each named deployment."""
    jobs, deployments = _named_workloads(doc)
    lines = ["", f"{'workload':<32} state"]
    for job in jobs:
        history = _hops_json(["job", "history", job])
        if isinstance(history, list) and history:
            latest = history[0]
            state = latest.get("state") or latest.get("STATE") or latest
        elif isinstance(history, dict) and "error" in history:
            state = f"unavailable: {history['error']}"
        else:
            state = "no executions"
        lines.append(f"{'job ' + job:<32} {state}")
    for name in deployments:
        status = _hops_json(["deployment", "status", name])
        state = status.get("status", status) if isinstance(status, dict) else status
        lines.append(f"{'deployment ' + name:<32} {state}")
    if not jobs and not deployments:
        lines.append("(no jobs or deployments named in system.yaml yet)")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Print the table once, or keep redrawing it with --watch."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "system_yaml",
        nargs="?",
        default=str(Path(__file__).resolve().parent / "system.yaml"),
    )
    parser.add_argument("--watch", type=float, metavar="SECONDS")
    parser.add_argument("--cluster", action="store_true")
    parser.add_argument("--now", help="ISO time to render at, for reproducible output")
    args = parser.parse_args(argv)

    path = Path(args.system_yaml)
    if not path.exists():
        print(f"No system.yaml at {path}; this directory is not an ML system yet.")
        return 1
    while True:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        now = parse_time(args.now) if args.now else datetime.now(timezone.utc)
        out = render(doc, now)
        if args.cluster:
            out += "\n" + render_cluster(doc)
        if args.watch:
            print("\033[2J\033[H" + out, flush=True)
            time.sleep(args.watch)
            continue
        print(out)
        return 0


if __name__ == "__main__":
    sys.exit(main())

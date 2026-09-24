# ruff: noqa: INP001
"""Time one full window of a batch feature pipeline against the inference schedule.

Batch systems only. Runs the pipeline's job over one full window, the same way
a scheduled fire does, and checks that it lands before inference reads it: the
gap between the feature job's cron and the inference job's cron must exceed the
measured run time with margin. Runs from the terminal or laptop; the pipeline
itself runs in the cluster as its job.

    python benchmarks/benchmark_features.py telco_churn_features \
        --start 2026-08-01 --end 2026-09-01 --run-id bench-features-1

Writes runs/<run_id>/result.json, which the orchestrator imports as
`features.pipelines[].benchmark.last_run`. Exits 0 when the window fits.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
MARGIN = 1.5


def cron_gap_s(feature_cron: str, inference_cron: str) -> float | None:
    """Seconds from a feature job's fire to the inference job's fire in the same period.

    Reads the second, minute, hour and day-of-month fields of two Quartz crons.
    None when either fires at no fixed time, in which case ordering needs a DAG.
    """

    def offset(cron: str) -> float | None:
        fields = cron.split()
        if len(fields) < 4 or not all(f.isdigit() for f in fields[:3]):
            return None
        day = int(fields[3]) - 1 if fields[3].isdigit() else 0
        return (
            day * 86400 + int(fields[2]) * 3600 + int(fields[1]) * 60 + int(fields[0])
        )

    start, end = offset(feature_cron), offset(inference_cron)
    if start is None or end is None or end <= start:
        return None
    return end - start


def fits(duration_s: float, gap_s: float | None, margin: float = MARGIN) -> bool:
    """Whether a run of `duration_s`, with margin, lands before inference fires."""
    return gap_s is not None and duration_s * margin <= gap_s


def _hops(*args: str) -> str:
    done = subprocess.run(["hops", *args], capture_output=True, text=True, check=False)
    if done.returncode != 0:
        raise SystemExit(
            f"hops {' '.join(args)} failed: {done.stderr.strip() or done.stdout.strip()}"
        )
    return done.stdout


def main(argv: list[str] | None = None) -> int:
    """Backfill one window, time it, and record whether it fits before inference."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("pipeline", help="the name in features.pipelines")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    system = yaml.safe_load((ROOT / "system.yaml").read_text(encoding="utf-8"))
    pipeline = next(
        p for p in system["features"]["pipelines"] if p["name"] == args.pipeline
    )
    job = pipeline["job"]["name"]
    started = time.monotonic()
    _hops(
        "job",
        "backfill",
        job,
        "--start-time",
        args.start,
        "--end-time",
        args.end,
        "--wait",
    )
    wall_s = time.monotonic() - started
    history = json.loads(_hops("job", "history", job, "--json"))
    latest = history[0] if history else {}
    duration_s = (
        latest.get("DURATION_S")
        if isinstance(latest.get("DURATION_S"), int)
        else round(wall_s)
    )
    final = latest.get("FINAL") or latest.get("final_status")

    inference_job = ((system.get("inference") or {}).get("batch") or {}).get(
        "job"
    ) or {}
    gap_s = cron_gap_s(
        pipeline["job"]["schedule"]["cron"],
        (inference_job.get("schedule") or {}).get("cron", ""),
    )
    commit = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    result = {
        "run_id": args.run_id,
        "commit": commit,
        "execution": latest.get("ID") or latest.get("id"),
        "kind": "full_window",
        "window": [args.start, args.end],
        "full_window_s": duration_s,
        "wall_s": round(wall_s),
        "gap_s": gap_s,
        "succeeded": final == "SUCCEEDED",
        "fits_before_inference": final == "SUCCEEDED" and fits(duration_s, gap_s),
    }
    out = ROOT / "runs" / args.run_id / "result.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["fits_before_inference"] else 1


if __name__ == "__main__":
    sys.exit(main())

# ruff: noqa: INP001
"""Seeded synthetic data for one source of an ML system, as batch tables or a live event stream.

Copied into the system as `src/<slug_pkg>/synthetic_data.py` and run as a
Hopsworks job in `python-feature-pipeline`, which ships polars, never on the laptop:

    # the history the training phase needs, then materialized to the offline store
    hops job deploy <slug>-data-backfill synthetic_data.py --env python-feature-pipeline \
        --args "--bundle <bundle> --mode backfill --from 2026-06-01 --to 2026-09-22" --run --wait
    # the stream, left running; `hops job stop <slug>-events` ends it
    hops job deploy <slug>-events synthetic_data.py --env python-feature-pipeline \
        --args "--bundle <bundle> --mode live" --run

The story from `data.<source>.generator.story` is code: `entities` and `events`
below. Replace the telco example with the system's own story, keeping the
rules: the same seed gives the same data; the target depends on the signal
columns with noise; no column is a function of the target that the real world
would not have at prediction time.
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import numpy as np
import polars as pl


if TYPE_CHECKING:
    from pathlib import Path


# region bundle prelude: identical in every entrypoint, see hops-reqs/references/bundle.md
def _load_bundle(bundle: str) -> tuple[Path, dict, dict]:
    """Fetch and unpack a run bundle, check it against its manifest, load system.yaml.

    `bundle` is a HopsFS path (Resources/<slug>/runs/<run_id>/bundle.tar.gz) or a
    local file. The bundle's src/ goes first on sys.path, so the harness a run
    imports is the one its commit names. Returns (workdir, manifest, system).
    """
    import hashlib
    import json
    import sys
    import tarfile
    import tempfile
    from pathlib import Path

    import yaml

    archive = Path(bundle)
    if not archive.is_file():
        import hopsworks

        download_dir = tempfile.mkdtemp(prefix="bundle-download-")
        archive = Path(
            hopsworks.login()
            .get_dataset_api()
            .download(bundle, download_dir, overwrite=True)
        )
    workdir = Path(tempfile.mkdtemp(prefix="bundle-"))
    with tarfile.open(archive) as tar:
        tar.extractall(workdir, filter="data")
    manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
    present = {
        p.relative_to(workdir).as_posix() for p in workdir.rglob("*") if p.is_file()
    } - {"manifest.json"}
    if present != set(manifest["files"]):
        raise SystemExit(f"bundle {bundle}: its files differ from its manifest")
    for rel, digest in manifest["files"].items():
        if hashlib.sha256((workdir / rel).read_bytes()).hexdigest() != digest:
            raise SystemExit(f"bundle {bundle}: {rel} does not match its manifest")
    sys.path.insert(0, str(workdir / "src"))
    system = yaml.safe_load((workdir / "system.yaml").read_text(encoding="utf-8"))
    return workdir, manifest, system


def _write_result(manifest: dict, result: dict) -> str:
    """Write result.json beside the bundle; the orchestrator imports it into system.yaml.

    The run id and commit are echoed from the manifest so the orchestrator can
    check the result belongs to the row it wrote before submission. With
    HOPS_RESULT_DIR set the file is written there instead of uploaded.
    """
    import json
    import os
    import tempfile
    from pathlib import Path

    payload = {"run_id": manifest["run_id"], "commit": manifest["commit"], **result}
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    local_dir = os.environ.get("HOPS_RESULT_DIR")
    if local_dir:
        path = Path(local_dir) / "result.json"
        path.write_text(text, encoding="utf-8")
        return str(path)
    local = Path(tempfile.mkdtemp(prefix="result-")) / "result.json"
    local.write_text(text, encoding="utf-8")
    target = f"Resources/{manifest['slug']}/runs/{manifest['run_id']}"
    import hopsworks

    hopsworks.login().get_dataset_api().upload(str(local), target, overwrite=True)
    return f"{target}/result.json"


# endregion

SOURCE = "usage_events"  # this generator's entry in data and requirements.data_sources
PLANS = ["basic", "plus", "premium"]


def entities(n: int, seed: int, prevalence: float, as_of: datetime) -> pl.DataFrame:
    """The entity table: one row per customer, with the label the system predicts.

    Story: churn is likelier for short tenure and the basic plan, with noise, and
    the overall churn rate is `prevalence`.
    """
    rng = np.random.default_rng(seed)
    tenure = rng.gamma(shape=2.0, scale=12.0, size=n).round().clip(1, 120)
    plan = rng.choice(PLANS, size=n, p=[0.5, 0.3, 0.2])
    logit = (
        1.2
        - 0.05 * tenure
        + np.where(plan == "basic", 0.8, 0.0)
        + rng.normal(0, 0.8, n)
    )
    # Shift the intercept so the realised positive rate matches the declared prevalence.
    threshold = np.quantile(logit, 1 - prevalence)
    return pl.DataFrame(
        {
            "customer_id": np.arange(1, n + 1, dtype=np.int64),
            "plan": plan,
            "tenure_months": tenure.astype(np.int64),
            "churn": (logit >= threshold).astype(np.int64),
            "snapshot_date": [as_of] * n,
        }
    )


def events(
    ents: pl.DataFrame,
    start: datetime,
    end: datetime,
    rate_per_s: float,
    seed: int,
    id_prefix: str = "b",
) -> pl.DataFrame:
    """Events between `start` and `end` at `rate_per_s` on average, before the story thins them.

    Story: churners' call volume decays over the last 60 days before `end`; the
    decay is the signal a model can learn.
    """
    rng = np.random.default_rng(seed)
    seconds = (end - start).total_seconds()
    n = int(rate_per_s * seconds)
    offsets = np.sort(rng.uniform(0, seconds, n))
    raw = pl.DataFrame(
        {
            "event_id": [f"{id_prefix}-{seed}-{i}" for i in range(n)],
            "customer_id": rng.choice(ents["customer_id"].to_numpy(), size=n),
            "ts": [start + timedelta(seconds=float(o)) for o in offsets],
            "duration_s": rng.exponential(180.0, n).round(1),
            "u": rng.uniform(0, 1, n),
        }
    )
    decay_from = end - timedelta(days=60)
    # A churner's event survives with a probability falling from 1 to 0 over the window.
    elapsed = (pl.col("ts") - pl.lit(decay_from)).dt.total_seconds() / (60 * 86400.0)
    return (
        raw.join(ents.select("customer_id", "churn"), on="customer_id")
        .filter(
            (pl.col("churn") == 0)
            | (pl.col("ts") < decay_from)
            | (pl.col("u") > elapsed)
        )
        .sort("ts")
        .select("event_id", "customer_id", "ts", "duration_s")
    )


def tick(
    ents: pl.DataFrame, now: datetime, tick_s: int, rate_per_s: float, seed: int
) -> pl.DataFrame:
    """One live tick: about `rate_per_s * tick_s` events inside [now - tick_s, now).

    Seeded from the tick's index, so a restart continues from now without state.
    """
    index = int(now.timestamp()) // tick_s
    rng = np.random.default_rng([seed, index])
    n = int(rate_per_s * tick_s)
    begin = now - timedelta(seconds=tick_s)
    offsets = np.sort(rng.uniform(0, tick_s, n))
    return pl.DataFrame(
        {
            "event_id": [f"l-{seed}-{index}-{i}" for i in range(n)],
            "customer_id": rng.choice(ents["customer_id"].to_numpy(), size=n),
            "ts": [begin + timedelta(seconds=float(o)) for o in offsets],
            "duration_s": rng.exponential(180.0, n).round(1),
        }
    ).with_columns(pl.col("ts").dt.replace_time_zone("UTC"))


def _sink(fs, writes: dict):
    """The events feature group: online, keyed by event, with a TTL and scheduled materialization."""
    return fs.get_or_create_feature_group(
        name=writes["feature_group"],
        version=writes.get("version", 1),
        primary_key=[writes.get("primary_key", "event_id")],
        event_time=writes.get("event_time", "ts"),
        online_enabled=True,
        stream=True,
        ttl=timedelta(days=int(str(writes.get("ttl", "7d")).rstrip("d"))),
        offline_backfill_every_hr=int(
            str(writes.get("offline_backfill_every", "1h")).rstrip("h")
        ),
        description=f"Synthetic {SOURCE}, written by this system's synthetic data job",
    )


def _ensure_materialization_schedule(sink, writes: dict) -> None:
    """Attach the hourly (or `offline_backfill_every`) materialization schedule if missing.

    Older clients drop `offline_backfill_every_hr` when the group is created by a
    multi-part insert, which leaves the live stream online-only forever.
    """
    job = sink.materialization_job
    if job.job_schedule is not None:
        return
    hours = int(str(writes.get("offline_backfill_every", "1h")).rstrip("h"))
    job.schedule(
        cron_expression=f"0 0 */{hours} ? * * *",
        start_time=datetime.now(timezone.utc) + timedelta(seconds=5),
    )


def main(argv: list[str] | None = None) -> int:
    """Write the backfill once, or keep writing live ticks until stopped."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for one source."
    )
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--mode", choices=["backfill", "live"], required=True)
    parser.add_argument("--from", dest="start")
    parser.add_argument("--to", dest="end")
    parser.add_argument(
        "--ticks", type=int, help="stop after this many live ticks (tests)"
    )
    args = parser.parse_args(argv)

    _, manifest, system = _load_bundle(args.bundle)
    spec = system["data"][SOURCE]
    generator, writes, live = spec["generator"], spec["writes"], spec.get("live", {})
    seed = int(generator["seed"])
    prevalence = float(system["requirements"]["targets"].get("prevalence", 0.2))
    n_entities = int(live.get("entities", 2000))

    import hopsworks

    fs = hopsworks.login().get_feature_store()
    sink = _sink(fs, writes)
    now = datetime.now(timezone.utc)
    ents = entities(n_entities, seed, prevalence, now)

    if args.mode == "backfill":
        start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        entity_writes = spec.get("entities")
        if entity_writes:
            table = fs.get_or_create_feature_group(
                name=entity_writes["feature_group"],
                version=entity_writes.get("version", 1),
                primary_key=["customer_id"],
                event_time="snapshot_date",
                description=f"Synthetic entities behind {SOURCE}, written by this system's synthetic data job",
            )
            table.insert(ents.to_pandas(), write_options={"wait_for_job": True})
        # The history is sized by the tier's row count, not the live rate: five
        # events a second over months is millions of rows on a small cluster.
        rows = int((spec.get("backfill") or {}).get("rows", 100_000))
        rate = rows / (end - start).total_seconds()
        frame = events(ents, start, end, rate, seed)
        for chunk in frame.iter_slices(50_000):
            sink.multi_part_insert(chunk)
        sink.finalize_multi_part_insert()
        # Rows in the online store are not training data until they are materialized.
        sink.materialization_job.run(await_termination=True)
        _ensure_materialization_schedule(sink, writes)
        _write_result(
            manifest,
            {"mode": "backfill", "rows": frame.height, "from": start, "to": end},
        )
        return 0

    tick_s = int(live.get("tick_s", 10))
    rate = float(live.get("rate_per_s", 5))
    done = 0
    with sink.multi_part_insert() as writer:
        while args.ticks is None or done < args.ticks:
            now = datetime.now(timezone.utc)
            writer.insert(tick(ents, now, tick_s, rate, seed))
            done += 1
            time.sleep(max(0.0, tick_s - (time.time() % tick_s)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

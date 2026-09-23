"""A feature pipeline: read one data window, apply model-independent transformations, write.

One program serves the backfill and the scheduled increments. The window comes
from HOPS_START_TIME and HOPS_END_TIME, which the scheduler sets on every fire
and `hops job backfill <name> --start-time --end-time` sets for a past interval;
`--start`/`--end` override them for a manual run. Writes are idempotent over the
window: the sink's primary key and event time make a replayed window an upsert.

    hops job deploy <slug>-features feature_pipeline.py --env python-feature-pipeline \
        --args "--bundle Resources/<slug>/runs/<run_id>/bundle.tar.gz"

Copy one per pipeline, named after it, and keep its transformations as plain
functions: tests/unit/test_features.py imports them.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any


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

PIPELINE = "telco_churn_features"  # this pipeline's name in features.pipelines


def window(
    start: str | None = None, end: str | None = None
) -> tuple[datetime, datetime]:
    """The data window: explicit arguments, else HOPS_START_TIME and HOPS_END_TIME.

    The scheduler writes ISO-8601 instants with a trailing Z.
    """
    raw_start = start or os.environ.get("HOPS_START_TIME")
    raw_end = end or os.environ.get("HOPS_END_TIME")
    if not raw_start or not raw_end:
        raise SystemExit(
            "no window: pass --start/--end or run as a scheduled or backfill job"
        )

    def parse(text: str) -> datetime:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    begin, finish = parse(raw_start), parse(raw_end)
    if finish <= begin:
        raise SystemExit(f"empty window: {begin} to {finish}")
    return begin, finish


def transform(df: Any) -> Any:
    """The model-independent transformations; one function per feature, tested alone."""
    return df


def validate(df: Any, primary_key: list[str]) -> list[str]:
    """Reasons to refuse the write: a bad row now would break a training run later."""
    problems = []
    if df[primary_key].isna().any().any():
        problems.append("null primary key")
    if df.duplicated(subset=primary_key).any():
        problems.append("duplicate primary key within the window")
    return problems


def main(argv: list[str] | None = None) -> int:
    """Run the pipeline over one window and record what landed."""
    parser = argparse.ArgumentParser(description="Compute features over one window.")
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args(argv)

    _, manifest, system = _load_bundle(args.bundle)
    spec = next(p for p in system["features"]["pipelines"] if p["name"] == PIPELINE)
    begin, finish = window(args.start, args.end)

    import hopsworks

    fs = hopsworks.login().get_feature_store()
    source = fs.get_feature_group(spec["reads"][0], version=1)
    df = source.filter(
        (source.get_feature(source.event_time) >= begin)
        & (source.get_feature(source.event_time) < finish)
    ).read(dataframe_type="pandas")
    features = transform(df)

    sink = fs.get_feature_group(
        spec["writes"]["feature_group"], version=spec["writes"]["version"]
    )
    problems = validate(features, sink.primary_key)
    if problems:
        raise SystemExit("refusing to write: " + "; ".join(problems))
    sink.insert(features, write_options={"wait_for_job": True})
    _write_result(
        manifest,
        {
            "pipeline": PIPELINE,
            "window": [begin, finish],
            "rows_written": len(features),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

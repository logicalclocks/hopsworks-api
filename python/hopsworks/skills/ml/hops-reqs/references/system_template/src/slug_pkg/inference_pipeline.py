"""The batch inference pipeline: score one window and write the predictions.

Reads the window's feature vectors through the feature view, so the
model-dependent transformations match training, scores them with the model
version `training.model` names, and writes to `<ident>_predictions` only the
fields `requirements.data_policy.log_fields` allows. It times the read, the scoring
and the write, and its result.json carries the batch SLA verdict, so its first run over
a full scheduled window is the `full_window` measurement.

    hops job deploy <slug>-inference inference_pipeline.py --env <inference.environment> \
        --args "--bundle Resources/<slug>/runs/<run_id>/bundle.tar.gz"

A realtime system replaces this file with predictor.py (hops-online-inference).
"""

from __future__ import annotations

import argparse
import importlib
import time
from datetime import UTC, datetime
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
            hopsworks.login().get_dataset_api().download(bundle, download_dir, overwrite=True)
        )
    workdir = Path(tempfile.mkdtemp(prefix="bundle-"))
    with tarfile.open(archive) as tar:
        tar.extractall(workdir, filter="data")
    manifest = json.loads((workdir / "manifest.json").read_text(encoding="utf-8"))
    present = {p.relative_to(workdir).as_posix() for p in workdir.rglob("*") if p.is_file()} - {
        "manifest.json"
    }
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


def batch_budget_s(cron: str, must_finish_by: str, features_s: float = 0.0) -> float | None:
    """Seconds a scoring run has between its cron fire and `must_finish_by`.

    Reads the hour and minute of a Quartz cron (`sec min hour ...`) and a
    `HH:MM` deadline on the same day, less the measured feature job when it runs
    in the same window. None when the cron fires at no fixed time of day.
    """
    fields = cron.split()
    if len(fields) < 3 or not (fields[1].isdigit() and fields[2].isdigit()):
        return None
    fire = int(fields[2]) * 3600 + int(fields[1]) * 60
    hour, minute = must_finish_by.split()[0].split(":")
    deadline = int(hour) * 3600 + int(minute) * 60
    if deadline <= fire:
        deadline += 24 * 3600
    return deadline - fire - features_s


def batch_verdict(system: dict, rows: int, window_s: float, kind: str = "full_window") -> dict:
    """The batch SLA verdict for one timed window, as a `measured` line of system.yaml takes it."""
    sla = system["requirements"]["sla"]["batch"]
    cron = system["inference"]["batch"]["job"]["schedule"]["cron"]
    features_s = max(
        (
            ((p.get("benchmark") or {}).get("last_run") or {}).get("full_window_s", 0)
            for p in (system.get("features") or {}).get("pipelines") or []
        ),
        default=0,
    )
    budget_s = batch_budget_s(cron, sla["must_finish_by"], features_s)
    fits = budget_s is not None and window_s <= budget_s
    return {
        "kind": kind,
        "rows": rows,
        "window_s": round(window_s, 1),
        "rows_per_s": round(rows / window_s, 1) if window_s else 0.0,
        "budget_s": budget_s,
        "fits_window": fits,
        # A sample is a screening estimate and never the basis of met.
        "met": fits and kind == "full_window",
    }


def prediction_rows(keys: Any, scores: Any, predicted_at: datetime, log_fields: list[str]) -> Any:
    """The rows written to the prediction feature group, restricted to the allowed fields.

    `keys` carries the entity columns; `scores` is aligned with it row by row.
    A field is written only when the data policy lists it.
    """
    frame = keys.reset_index(drop=True).copy()
    frame["score"] = list(scores)
    frame["prediction"] = frame["score"]
    frame["predicted_at"] = predicted_at
    allowed = [c for c in frame.columns if c in log_fields or c == "predicted_at"]
    return frame[allowed]


def main(argv: list[str] | None = None) -> int:
    """Score one window and write the predictions."""
    parser = argparse.ArgumentParser(description="Score one window.")
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--start")
    parser.add_argument("--end")
    # A scheduled run appends -start_time <fire instant> (and may append -end_time):
    # the instant the schedule fired, which ends the window rather than starting it.
    # They are accepted and ignored; a scheduled run's window is HOPS_START_TIME/HOPS_END_TIME.
    parser.add_argument("-start_time", dest="_fired_at", help=argparse.SUPPRESS)
    parser.add_argument("-end_time", dest="_fired_end", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    _, manifest, system = _load_bundle(args.bundle)
    pkg = system["system"]["slug"].replace("-", "_")
    evaluate = importlib.import_module(f"{pkg}.evaluate")
    features = importlib.import_module(f"{pkg}.feature_pipeline")
    begin, finish = features.window(args.start, args.end)

    import hopsworks

    project = hopsworks.login()
    fs = project.get_feature_store()
    training = system["training"]
    fv = fs.get_feature_view(
        training["feature_view"]["name"], version=training["feature_view"]["version"]
    )
    entity = system["requirements"]["problem"]["entity"]
    model_spec = training["model"]
    model = project.get_model_registry().get_model(
        model_spec["name"], version=model_spec["version"]
    )
    predict = evaluate.load_predictor(model.download())

    started = time.monotonic()
    fv.init_batch_scoring(training_dataset_version=training["harness"]["training_dataset_version"])
    batch = fv.get_batch_data(start_time=begin, end_time=finish, primary_key=True)
    key_columns = [c for c in batch.columns if c == entity or c.endswith("_" + entity)]
    scores = predict(batch.drop(columns=key_columns))

    rows = prediction_rows(
        batch[key_columns].rename(columns={key_columns[0]: entity}),
        scores,
        datetime.now(UTC),
        system["requirements"]["data_policy"]["log_fields"],
    )
    writes = system["inference"]["batch"]["writes"]
    sink = fs.get_or_create_feature_group(
        name=writes["feature_group"],
        version=writes["version"],
        # predicted_at in the key keeps every run's predictions, not only the latest.
        primary_key=[entity, "predicted_at"],
        event_time="predicted_at",
        description=f"Predictions of {model_spec['name']} v{model_spec['version']}",
        statistics_config=False,
    )
    sink.insert(rows, write_options={"wait_for_job": True})
    _write_result(
        manifest,
        {
            "window": [begin, finish],
            "rows_written": len(rows),
            "model_version": model_spec["version"],
            **batch_verdict(system, len(rows), time.monotonic() - started),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""The training pipeline: the one file the training agent's research loop edits.

Three modes, one registration rule each:

- `research`: train on the train part within `per_run`, score the validation part
  with evaluate.py, register the next version of `<ident>_research`, always.
- `accept`: train on train plus validation, score the test part once, register a
  version of `<ident>_model` only when the target holds.
- `retrain`: the scheduled job; build a new training dataset version with the
  recorded split policy, run the short checks, then behave as `accept`.

    hops job deploy <slug>-train training_pipeline.py --env <training.environment> \
        --args "--mode research --bundle Resources/<slug>/runs/<run_id>/bundle.tar.gz \
                --description 'run 3, e04c7d2: +charges_ratio'" --run --overwrite

The metrics that decide a run come from evaluate.py, never from this file.
"""

from __future__ import annotations

import argparse
import importlib
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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

MODES = ("research", "accept", "retrain")

# Columns whose names suggest they are consequences of the outcome; the EDA
# checklist's list, extended by whatever eda.md recorded for this system.
SUSPICIOUS = (
    "churned",
    "outcome",
    "approved",
    "declined",
    "post_",
    "future_",
    "after_",
    "resolved",
    "closed",
    "chargeback",
    "defaulted",
)


def ident(system: dict) -> str:
    """The snake_case identifier feature groups, views and models are named from."""
    return system["system"]["slug"].replace("-", "_")


def model_name(mode: str, system: dict) -> str:
    """Research versions and accepted versions never share a name."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}")
    suffix = "research" if mode == "research" else "model"
    return f"{ident(system)}_{suffix}"


def should_register(mode: str, met: bool) -> bool:
    """Research registers every run as the log; accept and retrain only a passing one."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}")
    return mode == "research" or met


def model_features(
    columns: list[str], system: dict, leakage: list[str] = ()
) -> list[str]:
    """The feature list: never the label, identifiers, event time or a suspicious name."""
    req = system["requirements"]
    excluded = {req["problem"].get("target"), req["problem"].get("entity")}
    excluded |= {s.get("event_time") for s in req.get("data_sources") or []}
    excluded.discard(None)

    def bad(column: str) -> bool:
        lowered = column.lower()
        return (
            column in excluded
            or any(lowered.endswith("_" + e) for e in excluded)
            or any(s in lowered for s in (*SUSPICIOUS, *leakage))
        )

    return [c for c in columns if not bad(c)]


def train(x_train: Any, y_train: Any, system: dict, deadline: float) -> Any:
    """Fit the model; the loop's one idea per run goes here.

    The baseline is the simplest model that fits the task. Respect `deadline`
    (a time.monotonic() value) so runs stay comparable within `per_run`.
    """
    task = system["requirements"]["problem"]["task"]
    if task == "classification":
        from sklearn.linear_model import LogisticRegression

        model = LogisticRegression(max_iter=1000)
    else:
        from sklearn.linear_model import Ridge

        model = Ridge()
    model.fit(
        x_train, y_train.values.ravel() if hasattr(y_train, "values") else y_train
    )
    if time.monotonic() > deadline:
        raise TimeoutError("training exceeded per_run")
    return model


def predict(model: Any, x: Any) -> Any:
    """Scores for classification, predictions otherwise, in the form evaluate.py scores."""
    if hasattr(model, "predict_proba"):
        return model.predict_proba(x)[:, 1]
    return model.predict(x)


def short_checks(parts: dict, system: dict, leakage: list[str]) -> list[str]:
    """Schema, label maturity and leakage checks a retrain runs on a new dataset."""
    problems = []
    expected = (system["training"].get("feature_view") or {}).get("features")
    for name, (x, _) in parts.items():
        if len(x) == 0:
            problems.append(f"the {name} part is empty")
        if expected and sorted(
            model_features(list(x.columns), system, leakage)
        ) != sorted(expected):
            problems.append(
                f"the {name} part's features differ from the feature view's"
            )
        flagged = [c for c in x.columns if any(s in c.lower() for s in leakage)]
        if flagged:
            problems.append(
                f"the {name} part has columns eda.md flagged as leaky: {flagged}"
            )
    return problems


def _new_training_dataset(fv: Any, system: dict, evaluate: Any) -> int:
    """Create a training dataset version with the recorded split policy over the data now."""
    split = system["training"]["split"]
    if split["kind"] == "temporal":
        bounds = evaluate.split_boundaries(
            datetime.fromisoformat(str(split["start"])),
            datetime.now(timezone.utc).replace(tzinfo=None),
            split["validation_fraction"],
            split["test_fraction"],
            evaluate.parse_period(system["requirements"]["problem"]["label_maturity"]),
        )
        bounds.pop("label_cutoff")
        version, _ = fv.create_train_validation_test_split(
            **bounds, write_options={"wait_for_job": True}
        )
        return version
    if split["kind"] == "random":
        version, _ = fv.create_train_validation_test_split(
            validation_size=split["validation_fraction"],
            test_size=split["test_fraction"],
            seed=split.get("seed", 42),
            write_options={"wait_for_job": True},
        )
        return version
    version, _ = fv.create_training_data(write_options={"wait_for_job": True})
    return version


def _register(
    project: Any,
    fv: Any,
    name: str,
    model: Any,
    x_example: Any,
    metrics: dict,
    description: str,
    td_version: int,
) -> int:
    import tempfile

    import joblib

    model_dir = Path(tempfile.mkdtemp(prefix="model-"))
    joblib.dump(model, model_dir / "model.pkl")
    (model_dir / "features.txt").write_text(
        "\n".join(x_example.columns), encoding="utf-8"
    )
    registered = project.get_model_registry().python.create_model(
        name=name,
        metrics=metrics,
        description=description,
        input_example=x_example.head(1),
        feature_view=fv,
        training_dataset_version=td_version,
    )
    registered.save(str(model_dir))
    return registered.version


def main(argv: list[str] | None = None) -> int:
    """Run one training job in the given mode and write its result.json."""
    parser = argparse.ArgumentParser(description="Train, score and register one model.")
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--description", default="")
    args = parser.parse_args(argv)

    _, manifest, system = _load_bundle(args.bundle)
    evaluate = importlib.import_module(f"{ident(system)}.evaluate")
    per_run = evaluate.parse_period(
        system["requirements"]["budget"]["training"].get("per_run", "10m")
    ).total_seconds()
    deadline = time.monotonic() + per_run

    def _stop(*_: object) -> None:
        raise TimeoutError("training exceeded per_run")

    signal.signal(signal.SIGALRM, _stop)
    signal.alarm(int(per_run * 1.5))

    import hopsworks

    project = hopsworks.login()
    training = system["training"]
    fv = project.get_feature_store().get_feature_view(
        training["feature_view"]["name"], version=training["feature_view"]["version"]
    )
    leakage = list(training.get("leakage") or [])
    td_version = training["harness"]["training_dataset_version"]
    if args.mode == "retrain":
        td_version = _new_training_dataset(fv, system, evaluate)
        training = {
            **training,
            "harness": {**training["harness"], "training_dataset_version": td_version},
        }
    parts = evaluate.read_parts(fv, training, system["requirements"])
    if args.mode == "retrain":
        problems = short_checks(parts, system, leakage)
        if problems:
            _write_result(
                manifest, {"mode": args.mode, "registered": False, "checks": problems}
            )
            raise SystemExit("short checks failed: " + "; ".join(problems))

    features = model_features(list(parts["train"][0].columns), system, leakage)
    if args.mode == "research":
        x_fit, y_fit = parts["train"][0][features], parts["train"][1]
        x_eval, y_eval, split = (
            parts["validation"][0][features],
            parts["validation"][1],
            "validation",
        )
    else:
        import pandas as pd

        x_fit = pd.concat([parts["train"][0], parts["validation"][0]])[features]
        y_fit = pd.concat([parts["train"][1], parts["validation"][1]])
        x_eval, y_eval, split = parts["test"][0][features], parts["test"][1], "test"

    model = train(x_fit, y_fit, system, deadline)
    signal.alarm(0)
    metrics = evaluate.evaluate(system["requirements"], y_eval, predict(model, x_eval))
    met = evaluate.meets(
        system["requirements"], metrics[system["requirements"]["targets"]["metric"]]
    )

    result = {
        "mode": args.mode,
        "split": split,
        "metrics": metrics,
        "met": met,
        "features": features,
        "training_dataset_version": td_version,
        "registered": False,
    }
    if should_register(args.mode, met):
        name = model_name(args.mode, system)
        version = _register(
            project,
            fv,
            name,
            model,
            x_fit,
            metrics,
            args.description or f"{args.mode} at {manifest['commit']}",
            td_version,
        )
        result.update(registered=True, model={"name": name, "version": version})
    result["finished"] = datetime.now(timezone.utc).isoformat()
    _write_result(manifest, result)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

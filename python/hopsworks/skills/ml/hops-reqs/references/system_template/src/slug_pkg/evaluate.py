"""The frozen evaluation harness: the validation and test parts, and the metrics.

Imported by training_pipeline.py, and run on its own as the `<slug>-eval` job to
score a registry model (a pretrained candidate, a retrained model, or a check in
verify) on one part:

    python evaluate.py --bundle Resources/<slug>/runs/<run_id>/bundle.tar.gz \
        --model <name>:<version> --split validation

Frozen once `training.harness.commit` is recorded: the training loop never edits
this file, so every run is scored on the same rows with the same metric. Adapt
`load_predictor` to the model format before the harness is frozen.
"""

from __future__ import annotations

import argparse
import hashlib
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np


PARTS = ("train", "validation", "test")


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


# region split

_PERIOD_UNITS = {
    "s": 1,
    "sec": 1,
    "second": 1,
    "m": 60,
    "min": 60,
    "minute": 60,
    "h": 3600,
    "hour": 3600,
    "d": 86400,
    "day": 86400,
    "w": 7 * 86400,
    "week": 7 * 86400,
    "month": 30 * 86400,
    "y": 365 * 86400,
    "year": 365 * 86400,
}


def parse_period(text: str | float) -> timedelta:
    """Read a YAML duration: `90s`, `10m`, `8h`, `7d`, `2w`, `1 month`, `1 year`.

    A bare number is minutes; a month is 30 days and a year 365.
    """
    if isinstance(text, (int, float)):
        return timedelta(minutes=text)
    parts = re.findall(r"(\d+(?:\.\d+)?)\s*([a-z]+)", str(text).lower())
    if not parts:
        raise ValueError(f"not a duration: {text!r}")
    seconds = 0.0
    for number, unit in parts:
        unit = unit.rstrip("s") if unit not in ("s",) else unit
        if unit not in _PERIOD_UNITS:
            raise ValueError(f"unknown unit {unit!r} in {text!r}")
        seconds += float(number) * _PERIOD_UNITS[unit]
    return timedelta(seconds=seconds)


def split_boundaries(
    start: datetime,
    end: datetime,
    validation_fraction: float,
    test_fraction: float,
    label_maturity: timedelta,
) -> dict[str, datetime]:
    """Time boundaries for a time-ordered split with the test part last.

    Rows younger than `label_maturity` at `end` have no final label and fall in
    no part, so the usable range ends at the label cutoff. The keys are the
    arguments of `FeatureView.create_train_validation_test_split`, plus
    `label_cutoff` for the YAML.
    """
    if not 0 < validation_fraction < 1 or not 0 < test_fraction < 1:
        raise ValueError("validation_fraction and test_fraction must be in (0, 1)")
    if validation_fraction + test_fraction >= 1:
        raise ValueError("validation_fraction + test_fraction must leave a train part")
    cutoff = end - label_maturity
    if cutoff <= start:
        raise ValueError("label_maturity leaves no rows with a final label")
    span = cutoff - start
    validation_start = start + span * (1 - validation_fraction - test_fraction)
    test_start = start + span * (1 - test_fraction)
    return {
        "train_start": start,
        "train_end": validation_start,
        "validation_start": validation_start,
        "validation_end": test_start,
        "test_start": test_start,
        "test_end": cutoff,
        "label_cutoff": cutoff,
    }


def grouped_part(entity: Any, validation_fraction: float, test_fraction: float) -> str:
    """The part an entity belongs to, stable across runs and machines.

    A hash of the entity key rather than a random draw, so every row of an
    entity lands in the same part and the assignment never changes.
    """
    digest = hashlib.sha256(str(entity).encode()).digest()
    u = int.from_bytes(digest[:8], "big") / 2**64
    if u < test_fraction:
        return "test"
    if u < test_fraction + validation_fraction:
        return "validation"
    return "train"


def read_parts(fv: Any, training: dict, requirements: dict) -> dict[str, tuple]:
    """Read (X, y) for every part from the frozen training dataset version.

    A temporal or random split was materialized as three parts by the feature
    view; a grouped split is one training dataset partitioned here by entity.
    """
    version = training["harness"]["training_dataset_version"]
    split = training["split"]
    if split["kind"] in ("temporal", "random"):
        x_tr, x_va, x_te, y_tr, y_va, y_te = fv.get_train_validation_test_split(
            training_dataset_version=version
        )
        return {"train": (x_tr, y_tr), "validation": (x_va, y_va), "test": (x_te, y_te)}
    entity = requirements["problem"]["entity"]
    x, y = fv.get_training_data(training_dataset_version=version, primary_key=True)
    key_column = next(c for c in x.columns if c == entity or c.endswith("_" + entity))
    parts = x[key_column].map(
        lambda e: grouped_part(e, split["validation_fraction"], split["test_fraction"])
    )
    x = x.drop(columns=[key_column])
    return {p: (x[parts == p], y[parts == p]) for p in PARTS}


# endregion

# region metrics


def _binary_inputs(y_true: Any, y_score: Any) -> tuple[np.ndarray, np.ndarray]:
    y = np.asarray(y_true, dtype=float).ravel()
    s = np.asarray(y_score, dtype=float).ravel()
    if y.shape != s.shape:
        raise ValueError("y_true and y_score differ in length")
    return y, s


def pr_auc(y_true: Any, y_score: Any) -> float:
    """Average precision: precision averaged over the rank of every positive."""
    y, s = _binary_inputs(y_true, y_score)
    positives = y.sum()
    if positives == 0:
        return 0.0
    order = np.argsort(-s, kind="mergesort")
    hits = y[order]
    precision_at_hit = np.cumsum(hits) / np.arange(1, len(hits) + 1)
    return float((precision_at_hit * hits).sum() / positives)


def roc_auc(y_true: Any, y_score: Any) -> float:
    """Probability that a random positive outranks a random negative, ties halved."""
    y, s = _binary_inputs(y_true, y_score)
    pos, neg = s[y == 1], s[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return 0.5
    greater = (pos[:, None] > neg[None, :]).sum()
    ties = (pos[:, None] == neg[None, :]).sum()
    return float((greater + 0.5 * ties) / (len(pos) * len(neg)))


def precision_at_k(y_true: Any, y_score: Any, k: int) -> float:
    """Share of positives among the k highest scores."""
    y, s = _binary_inputs(y_true, y_score)
    top = np.argsort(-s, kind="mergesort")[:k]
    return float(y[top].mean()) if len(top) else 0.0


def rmse(y_true: Any, y_pred: Any) -> float:
    """Root mean squared error."""
    y, p = _binary_inputs(y_true, y_pred)
    return float(np.sqrt(np.mean((y - p) ** 2)))


def mae(y_true: Any, y_pred: Any) -> float:
    """Mean absolute error."""
    y, p = _binary_inputs(y_true, y_pred)
    return float(np.mean(np.abs(y - p)))


def mape(y_true: Any, y_pred: Any) -> float:
    """Mean absolute percentage error over the rows whose true value is not zero."""
    y, p = _binary_inputs(y_true, y_pred)
    mask = y != 0
    return float(np.mean(np.abs((y[mask] - p[mask]) / y[mask]))) if mask.any() else 0.0


def accuracy(y_true: Any, y_pred: Any) -> float:
    """Share of exact matches."""
    y, p = np.asarray(y_true).ravel(), np.asarray(y_pred).ravel()
    return float((y == p).mean()) if len(y) else 0.0


METRICS = {
    "pr_auc": pr_auc,
    "roc_auc": roc_auc,
    "rmse": rmse,
    "mae": mae,
    "mape": mape,
    "accuracy": accuracy,
}


def score(metric: str, y_true: Any, y_pred: Any) -> float:
    """Score predictions with a metric named in requirements.targets.

    `precision_at_<k>` and `recall_at_<k>` are parsed from the name.
    """
    if metric.startswith("precision_at_"):
        return precision_at_k(y_true, y_pred, int(metric.rsplit("_", 1)[1]))
    if metric.startswith("recall_at_"):
        k = int(metric.rsplit("_", 1)[1])
        y, s = _binary_inputs(y_true, y_pred)
        top = np.argsort(-s, kind="mergesort")[:k]
        return float(y[top].sum() / y.sum()) if y.sum() else 0.0
    if metric not in METRICS:
        raise ValueError(
            f"unknown metric {metric!r}; add it here before the harness is frozen"
        )
    return METRICS[metric](y_true, y_pred)


def evaluate(requirements: dict, y_true: Any, y_pred: Any) -> dict[str, float]:
    """Every metric the requirements name: the target metric and the secondary ones."""
    targets = requirements["targets"]
    names = [targets["metric"], *(targets.get("secondary") or [])]
    return {name: score(name, y_true, y_pred) for name in names}


def meets(requirements: dict, value: float) -> bool:
    """Whether a metric value meets the target in the target's direction."""
    targets = requirements["targets"]
    if targets["direction"] == "max":
        return value >= targets["target"]
    return value <= targets["target"]


# endregion


def load_predictor(model_dir: str) -> Any:
    """Return a callable from a feature frame to scores or predictions.

    The default reads the `model.pkl` and `features.txt` training_pipeline.py
    saves. A pretrained
    candidate needs its own loader here, written before the harness is frozen.
    """
    import joblib

    model = joblib.load(Path(model_dir) / "model.pkl")
    listed = Path(model_dir) / "features.txt"
    columns = (
        listed.read_text(encoding="utf-8").splitlines() if listed.exists() else None
    )

    def predictor(x: Any) -> Any:
        # The model sees exactly its training columns, in training order.
        x = x[columns] if columns else x
        if hasattr(model, "predict_proba"):
            return model.predict_proba(x)[:, 1]
        return model.predict(x)

    return predictor


def main(argv: list[str] | None = None) -> int:
    """Score one registry model on one part and write the metrics."""
    parser = argparse.ArgumentParser(description="Score a registry model on one part.")
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--model", required=True, help="<name>:<version>")
    parser.add_argument("--split", choices=["validation", "test"], required=True)
    args = parser.parse_args(argv)

    _, manifest, system = _load_bundle(args.bundle)
    import hopsworks

    project = hopsworks.login()
    training = system["training"]
    fv_spec = training["feature_view"]
    fv = project.get_feature_store().get_feature_view(
        fv_spec["name"], version=fv_spec["version"]
    )
    x, y = read_parts(fv, training, system["requirements"])[args.split]

    name, version = args.model.rsplit(":", 1)
    model = project.get_model_registry().get_model(name, version=int(version))
    predict = load_predictor(model.download())
    metrics = evaluate(system["requirements"], y, predict(x))

    evaluation = Path("evaluation") / f"{args.split}.json"
    evaluation.parent.mkdir(exist_ok=True)
    import json

    evaluation.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    project.get_dataset_api().upload(
        str(evaluation), f"{model.version_path}/evaluation", overwrite=True
    )
    _write_result(
        manifest,
        {
            "model": {"name": name, "version": int(version)},
            "split": args.split,
            "metrics": metrics,
            "met": meets(
                system["requirements"],
                metrics[system["requirements"]["targets"]["metric"]],
            ),
            "finished": datetime.now(timezone.utc).isoformat(),
        },
    )
    print(metrics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

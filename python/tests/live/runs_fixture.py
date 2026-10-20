#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Make the runs fixture of test_model_runs: one sweep of three candidates on deal_win_fv v1.

Registered as runs of model runs_fixture_dealwin in project blah, as hops-train "Runs and sweeps"
says. Rerun to add a sweep.
"""

import hashlib
import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import hopsworks
import joblib
from hopsworks_common import events
from sklearn.compose import make_column_selector, make_column_transformer
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, f1_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


NAME, SEED, METRIC, GOAL = "runs_fixture_dealwin", 42, "val_macro_f1", "max"
CANDIDATES = {
    "logistic regression": (LogisticRegression(max_iter=500), {"C": 1.0}),
    "random forest": (
        RandomForestClassifier(
            n_estimators=50, max_depth=8, random_state=SEED, n_jobs=-1
        ),
        {"n_estimators": 50, "max_depth": 8},
    ),
    "gradient boosting": (
        HistGradientBoostingClassifier(max_iter=100, random_state=SEED),
        {"max_iter": 100},
    ),
}

project = hopsworks.login()
fs = project.get_feature_store()
mr = project.get_model_registry()
fv = fs.get_feature_view("deal_win_fv", version=1)
sweep_id = f"{NAME}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"

with events.tracked("experiment", "create", long=True, name=NAME) as sweep:
    sweep.detail.update(
        sweep_id=sweep_id, candidates=len(CANDIDATES), metric=METRIC, metric_goal=GOAL
    )
    t = time.time()
    X, y = fv.training_data(primary_key=True, statistics_config=False)
    td_version = fv.get_last_accessed_training_dataset()
    y = y.iloc[:, 0]
    bucket = X["deal_id"].map(
        lambda k: int(hashlib.sha256(str(k).encode()).hexdigest(), 16) % 100
    )
    X = X.drop(columns=[c for c in ("deal_id", "snapshot_date") if c in X.columns])
    parts = {
        "train": bucket < 70,
        "val": (bucket >= 70) & (bucket < 85),
        "test": bucket >= 85,
    }
    print(
        f"[time] split {time.time() - t:.1f}s rows={len(X)} td_version={td_version}",
        flush=True,
    )
    majority = y[parts["train"]].mode()[0]
    for i, (candidate, (estimator, params)) in enumerate(CANDIDATES.items(), start=1):
        started = datetime.now(timezone.utc)
        model = make_pipeline(
            make_column_transformer(
                (StandardScaler(), make_column_selector(dtype_include="number")),
                (
                    OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                    make_column_selector(dtype_exclude="number"),
                ),
            ),
            estimator,
        )
        model.fit(X[parts["train"]], y[parts["train"]])
        metrics = {}
        for part in ("val", "test"):
            pred = model.predict(X[parts[part]])
            truth = y[parts[part]]
            metrics[f"{part}_accuracy"] = accuracy_score(truth, pred)
            metrics[f"{part}_macro_f1"] = f1_score(truth, pred, average="macro")
            metrics[f"{part}_balanced_accuracy"] = balanced_accuracy_score(truth, pred)
            metrics[f"{part}_baseline_accuracy"] = float((truth == majority).mean())
        model_dir = Path(tempfile.mkdtemp(prefix="run-"))
        joblib.dump(model, model_dir / "model.joblib")
        run = {
            "experiment": NAME,
            "run_id": f"{sweep_id}-{i}",
            "sweep_id": sweep_id,
            "candidate": candidate,
            "params": params,
            "metric": METRIC,
            "metric_goal": GOAL,
            "status": "finished",
            "started": started.isoformat(),
            "finished": datetime.now(timezone.utc).isoformat(),
            "seed": SEED,
            "training_dataset_version": td_version,
        }
        (model_dir / "run.json").write_text(json.dumps(run))
        registered = mr.python.create_model(
            name=NAME,
            metrics={k: round(v, 4) for k, v in metrics.items()},
            description=f"{candidate} ({', '.join(f'{k}={v}' for k, v in params.items())}); runs fixture for hops model runs",
            input_example=X.head(1),
            feature_view=fv,
            training_dataset_version=td_version,
        )
        registered.save(str(model_dir))
        print(
            f"[time] {candidate} v{registered.version} {METRIC}={metrics['val_macro_f1']:.4f}",
            flush=True,
        )
print(f"sweep {sweep_id}")

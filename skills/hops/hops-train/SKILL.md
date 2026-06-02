---
name: hopsworks-train
description: Use when training an ML model. Load training data from an existing feature view, train a model with an appropriate ML framework, evaluate it, and register the model and its evaluation (metrics, plots) in the Hopsworks model registry.
model: claude-sonnet-4-6
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

Train a model from a feature view and register it. This is the **T** of the
FTI pattern: it consumes a feature view (already created by the F side) and
produces a registered model the I side deploys.

**CLI vs SDK for this task:** training itself is SDK-only — write a `train.py`
that uses the `hopsworks` Python SDK (the `hops` CLI has no `train`). The CLI
does the surrounding steps: `hops fv list` / `hops td compute <fv> <v>` before,
and running the script as a job after:
`hops job deploy train.py --name train --env pandas-training-pipeline --run --wait`.

The algorithm, metrics, and label are **task-specific** — pick them per problem.
Everything below is the reusable shape, with one neutral example. Do not copy a
specific algorithm or threshold blindly.

## 1. Load training data from the feature view

The model trains from the **feature view**, never from a feature group directly
(`fg.read()` for training data breaks the FTI invariant and loses the
transforms + point-in-time correctness the FV provides).

```python
import hopsworks

project = hopsworks.login()
fs = project.get_feature_store()

fv = fs.get_feature_view(name="my_fv", version=1)
# Reuse the training dataset produced by `hops td compute <fv> 1`:
X_train, X_test, y_train, y_test = fv.get_train_test_split(training_dataset_version=1)
```

Hints:
- `get_train_test_split` returns `(X_train, X_test, y_train, y_test)`; `y` is
  empty when the FV declares no label.
- Retrieval-time transformations declared on the FV are applied automatically
  to the returned frames, using statistics stored with the training dataset.
- The FV's `select_all()` includes the serving key(s) and event time as columns.
  Drop them from the model inputs by name (e.g. `X_train.drop(columns=[key, event_time])`)
  — they are identifiers, not features.
- To create a fresh split instead of reusing one: `fv.train_test_split(test_size=0.2)`.

## 2. Train

Pick the framework and algorithm for the task (regression, classification,
anomaly detection, …). Neutral example:

```python
from xgboost import XGBRegressor

model = XGBRegressor(n_estimators=300, max_depth=6, random_state=42)
model.fit(X_train, y_train)
```

## 3. Evaluate

Compute the metrics that fit the task (R²/MAE for regression, precision/recall/
AUC for classification, …) and save plots as PNGs alongside the model so they
land in the registry.

```python
from sklearn.metrics import r2_score, mean_absolute_error

preds = model.predict(X_test)
metrics = {"r2": r2_score(y_test, preds), "mae": mean_absolute_error(y_test, preds)}
```

## 4. Register the model

```python
import os, joblib

model_dir = "my_model_dir"
os.makedirs(model_dir, exist_ok=True)
joblib.dump(model, f"{model_dir}/model.pkl")
# Save anything the predictor needs at serving time next to the model, e.g.
# the input feature order: json.dump(list(X_train.columns), open(f"{model_dir}/feature_names.json", "w"))

mr = project.get_model_registry()
hw_model = mr.python.create_model(
    name="my_model",
    metrics=metrics,                       # stored + queryable
    description="What this model predicts",
    input_example=X_train.head(1),
    feature_view=fv,                       # provenance link FV -> model
    training_dataset_version=1,
)
hw_model.save(model_dir)                    # uploads the whole dir (model + plots)
```

Hints:
- `save()` moves the local files into the registry; pass `keep_original_files=True`
  to also keep them locally.
- Query the best version later:
  `mr.get_best_model("my_model", metric="r2", direction="max")`.
- Download later: `model.download(local_path="./model")`.
- The `feature_view` + `training_dataset_version` arguments record provenance so
  the model is linked back to the exact data it was trained on.

## 5. Run it as a job

```bash
hops job deploy train.py --name train --env pandas-training-pipeline --run --wait
hops model list   # confirm the model registered
```

---
name: hops-train
description: Use when training an ML model. Load training data from an existing feature view, train a model with an appropriate ML framework, evaluate it, and register the model and its evaluation (metrics, plots) in the Hopsworks model registry. Input - a feature view; Output - a registered model.
---

Train a model from a feature view and register it. This is the **T** of the
FTI pattern: it consumes a feature view (already created by the F side) and
produces a registered model the I side deploys.

## Contract
- **Input:** a feature view.
- **Output:** a registered model (with metrics + plots) in the model registry.
- **Pre-condition:** the feature view already exists (create it first with **hops-fv**).

## Smoke-test (cheap pre/post-flight)
```bash
hops fv list                          # confirm the input FV exists before
hops model list                       # confirm the model registered after
```

## Ask the user (only when state is ambiguous)
The algorithm, metrics, and label are **task-specific** — pick them per problem.
Everything below is the reusable shape, with one neutral example. Do not copy a
specific algorithm or threshold blindly.

**CLI vs SDK for this task:** training itself is SDK-only — write a `train.py`
that uses the `hopsworks` Python SDK (the `hops` CLI has no `train`). The CLI
does the surrounding steps: `hops fv list` / `hops td compute <fv> <v>` before,
and running the script as a job after:
`hops job deploy train train.py --env pandas-training-pipeline --run --wait --overwrite`
(NAME then SCRIPT are positional; `--overwrite` replaces the uploaded script on re-run).

## 1. Load training data from the feature view

The model trains from the **feature view**, never from a feature group directly
(`fg.read()` for training data breaks the FTI invariant and loses the
transforms + point-in-time correctness the FV provides).

```python
import hopsworks

project = hopsworks.login()
fs = project.get_feature_store()

fv = fs.get_feature_view(name="my_fv", version=1)  # no FV yet? create one first (skill: hops-fv)

# Two split methods. They are NOT interchangeable; pick by data size and
# whether you need a reproducible, stored dataset.

# A. In-memory (default for small/static data). Returns the four frames
#    directly: no Spark, no stored version. Train right away.
X_train, X_test, y_train, y_test = fv.train_test_split(test_size=0.2)

# B. Materialized + versioned (large data, or a reproducible pinned dataset).
#    create_train_test_split returns (version, Job) and runs an ASYNC PySpark
#    job. The version pins the feature-group commits and split seed, so every
#    model trained on it sees identical rows. Block on the job, then read back
#    BY VERSION (the dataset 404s until the job finishes).
# td_version, td_job = fv.create_train_test_split(
#     test_size=0.2, write_options={"wait_for_job": True}
# )
# X_train, X_test, y_train, y_test = fv.get_train_test_split(training_dataset_version=td_version)
```

> `train_test_split()` returns `(X_train, X_test, y_train, y_test)`, not a version. `create_train_test_split()` returns `(version, Job)`. Unpacking one as the other (`td_version, _ = fv.train_test_split(...)`) raises `ValueError: too many values to unpack`, and passing the `(version, Job)` tuple to `get_train_test_split` URL-encodes it into the path and 404s. For a static dataset, prefer the in-memory `train_test_split()`.

Hints:
- `get_train_test_split` only works on a dataset created BY a split call. A plain
  dataset from `hops td compute <fv>` is read with
  `fv.get_training_data(training_dataset_version=N)` — calling `get_train_test_split`
  on it raises `Use feature_view.get_training_data instead`.
- `hops td compute <fv> <fv_version>` takes the FEATURE-VIEW version as a required
  positional; the training-dataset version it creates auto-increments — read it
  back from `hops td list <fv>` rather than hardcoding `1`.
- `get_train_test_split` returns `(X_train, X_test, y_train, y_test)`; `y` is
  empty when the FV declares no label.
- Model-dependent transformations (MDTs) declared on the FV are applied
  automatically to the returned frames, using statistics stored with the
  training dataset. The same MDTs run at inference time, so training and
  serving stay equivalent (no training/serving skew).
- Drop target-leaking columns: any value not known at prediction time.
  Post-outcome signals (counts, views, reactions on the thing you predict) and
  source-internal aggregates leak the label, and the store will not stop you.
  Decide per column whether a draft/request would have it; if not, exclude it.
- Keep identifier columns (primary keys, event time) out of the model inputs.
  They are leaky and break `fit` (a datetime column raises
  `DTypePromotionError: DateTime64 could not be promoted by Float64`).
  The clean fix is upstream: exclude them when you create the FV, with
  `fg.select_except([pk, event_time])` or
  `fg.select_all(include_primary_key=False, include_event_time=False)` (skill: hops-fv).
- If you do pull keys/event time into a split (e.g. `get_train_test_split(primary_keys=True, event_time=True)`
  to order a backtest plot), they come back **renamed** with a
  `<project>_<fg>_<version>_` prefix (e.g. `newproj_eth_blocks_1_timestamp`), not their bare name.
  Drop them by **suffix**, not exact name, before `fit`, or the prefixed
  event-time column slips into the training matrix:
  `X_train = X_train[[c for c in X_train.columns if not c.endswith(("_" + pk, "_" + event_time))]]`.

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

Evaluation (metrics on the test set) is distinct from **model validation**:
checking the model on slices of the data at risk of bias before it ships. Use
the FV's `training_helper_columns` (e.g. `gender`, `age`) to slice the test set
without feeding those columns to the model — they are dropped before `fit` and
not returned at inference. Store validation results alongside the metrics.

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
    training_dataset_version=td_version,    # the split materialized in step 1
)
hw_model.save(model_dir)                    # uploads the whole dir (model + plots)
```

**Set `description=`** on the model (and keep `metrics=`): an undescribed model is an empty envelope in the registry.

Hints:
- `save()` moves the local files into the registry; pass `keep_original_files=True`
  to also keep them locally.
- Query the best version later:
  `mr.get_best_model("my_model", metric="r2", direction="max")`.
- Download later: `model.download(local_path="./model")`.
- The `feature_view` + `training_dataset_version` arguments record provenance so
  the model is linked back to the exact data it was trained on.
- Metrics, plots, and validation results saved with the model form its **model
  card**: the handoff doc the I side reads to deploy. Aim to capture intended
  use, performance, and bias-test outcomes, not just raw metrics.

## 5. Run it as a job

```bash
hops job deploy train train.py --env pandas-training-pipeline --run --wait --overwrite
hops model list                       # confirm the model registered
hops model info <name> --version <v>  # metrics + description
```

`hops job deploy` takes NAME then SCRIPT as positionals (no `--name`); `--overwrite`
replaces the uploaded script so a re-run does not error on the existing file.

Non-interactive cleanup needs flags: `hops model delete <name> --yes`,
`hops fv delete <name> --version <v> --yes --force` (a FV with training data
needs `--force`).

**Confirm before deleting.** These remove the registered model or feature view
irreversibly; confirm the exact name and version with the user, and never tear
down a model or feature view you created as a side effect (temp or test ones
included) unless they asked.

## Next Steps

- Serve the model online: **hops-online-inference**. Batch scoring: **hops-batch-inference**.
- Build or fix the feature view it trains on: **hops-fv**.

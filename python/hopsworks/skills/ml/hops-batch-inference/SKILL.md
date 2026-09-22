---
name: hops-batch-inference
description: Use when writing Python or PySpark code for batch inference with Hopsworks. Auto-invoke when user wants to retrieve batch data from feature views, use spine groups for point-in-time joins, download models from the model registry for batch prediction, or build batch scoring pipelines. Input feature view + registered model → output predictions, logged or persisted.
---

# Hopsworks Batch Inference — Python SDK Best Practices

## Contract
- **Input:** a feature view + a registered model from the Model Registry.
- **Output:** predictions, either logged (monitoring) or persisted (downstream consumption).
- **Pre-condition:** the model is trained and registered; the feature view is materialized offline.

## Smoke-test (cheap pre/post-flight)
```bash
hops model list             # confirm the model exists before scoring
hops fv list                # confirm the feature view exists
hops td list <fv-name>      # confirm a training dataset version exists for batch scoring
```

## Ask the user (only when state is ambiguous)
- Which model version to score with (specific version vs best-by-metric).
- The time range for the batch (full FV vs `start_time`/`end_time` window).
- Persist vs log: write predictions to a prediction feature group, or log them for monitoring.

## Overview

A batch inference pipeline is one of the three FTI pipelines (feature, training, inference): a separate program that runs on a schedule, makes non-time-critical predictions, and writes them to an inference store (a feature group, database, or object store) for asynchronous consumers. It defines a batch AI system. Log its inputs and predictions so you can monitor and debug it.

Batch inference in Hopsworks follows this pattern:

1. Download a trained model from the Model Registry
2. Retrieve a batch of inference data from a Feature View
3. Apply model-dependent transformations (MDTs) and call the model to produce predictions

Two approaches for retrieving batch data:
- **`get_batch_data()`** — filter by event time range from offline feature store
- **`spine_df`** — supply the rows yourself: serving keys plus the time to compute features as of. Works on any feature view, and the times may be in the future
- **Spine groups** — the older form, only for a view created with one as its left side

Both Pandas and PySpark are supported. Spine groups require PySpark; `spine_df` does not.

---

## Downloading Models from the Model Registry

### Get a Model

```python
import hopsworks

project = hopsworks.login()
mr = project.get_model_registry()

# Get specific version
model = mr.get_model("fraud_detector", version=1)

# Get best model by metric
model = mr.get_best_model("fraud_detector", metric="f1_score", direction="max")

# List all versions
models = mr.get_models("fraud_detector")
```

### Download Model Files

```python
# Download to a temporary directory (returns absolute path)
model_dir = model.download()

# Download to a specific location
model_dir = model.download(local_path="./models/fraud_detector")
```

`download()` returns the absolute path to the local directory containing the model files.

### Load the Model

The loading pattern depends on the framework:

```python
# Scikit-learn
import joblib
clf = joblib.load(f"{model_dir}/model.pkl")

# XGBoost
import xgboost as xgb
clf = xgb.Booster()
clf.load_model(f"{model_dir}/model.json")

# PyTorch
import torch
net = torch.load(f"{model_dir}/model.pt")
net.eval()

# TensorFlow / Keras
import tensorflow as tf
net = tf.keras.models.load_model(f"{model_dir}/saved_model")

# Generic pickle
import pickle
with open(f"{model_dir}/model.pkl", "rb") as f:
    clf = pickle.load(f)
```

### Model Metadata

```python
print(model.name)                # model name
print(model.version)             # version number
print(model.framework)           # "SKLEARN", "PYTHON", "TORCH", "TENSORFLOW"
print(model.training_metrics)    # {"accuracy": 0.95, "f1": 0.92}
print(model.description)         # human-readable description
print(model.model_schema)        # input/output schema (if set)
```

---

## Retrieving Batch Data with get_batch_data()

`get_batch_data()` reads features from the offline feature store, optionally filtered by event time, and applies the model-dependent transformations (MDTs). The feature view applies the same filters and MDTs used at training time, so inference features match training features (no training/serving skew).

### Basic Usage

```python
fs = project.get_feature_store()
fv = fs.get_feature_view("fraud_features_fv", version=1)

# Initialize batch scoring with transformation statistics
fv.init_batch_scoring(training_dataset_version=1)

# Get all data
batch_df = fv.get_batch_data(dataframe_type="pandas")
```

### Filter by Event Time

```python
from datetime import datetime, timedelta

batch_df = fv.get_batch_data(
    start_time=datetime.now() - timedelta(days=1),  # inclusive
    end_time=datetime.now(),                          # exclusive
    dataframe_type="pandas",
)
```

Time formats supported: `datetime`, `date`, strings (`"2025-01-01"`, `"2025-01-01 12:00:00"`), or Unix epoch in seconds (int).

### get_batch_data() Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `start_time` | `str`, `int`, `datetime`, `date` | `None` | Start event time (inclusive) |
| `end_time` | `str`, `int`, `datetime`, `date` | `None` | End event time (exclusive) |
| `dataframe_type` | `str` | `"default"` | `"pandas"`, `"polars"`, `"spark"`, `"numpy"`, `"python"` |
| `transformed` | `bool` | `True` | Apply model-dependent transformations |
| `primary_key` | `bool` | `False` | Include primary key columns in output |
| `event_time` | `bool` | `False` | Include event time column in output |
| `inference_helper_columns` | `bool` | `False` | Include inference helper columns |
| `spine` | `DataFrame` or `SpineGroup` | `None` | Fills the SpineGroup a view was **created** with (Spark only) |
| `spine_df` | `DataFrame` | `None` | Rows to compute features for, on **any** view. Mutually exclusive with `spine` |
| `read_options` | `dict` | `None` | Engine options (e.g., `{"arrow_flight_config": {"timeout": 900}}`) |
| `transformation_context` | `dict` | `None` | Runtime context for transformation functions |

### Controlling Transformations

```python
# With transformations applied (default)
transformed_df = fv.get_batch_data(transformed=True)

# Raw features, no transformations
raw_df = fv.get_batch_data(transformed=False)
```

When `transformed=True`, model-dependent transformations (e.g., standard_scaler, label_encoder) are applied using statistics from the training dataset specified in `init_batch_scoring()`.

### Including Extra Columns

```python
batch_df = fv.get_batch_data(
    primary_key=True,               # include primary key columns
    event_time=True,                # include event time column
    inference_helper_columns=True,  # include helper columns
    dataframe_type="pandas",
)
```

Primary keys and event time are useful for joining predictions back to the source data. Inference helper columns provide extra context (e.g., customer name) not used by the model.

---

## Supplying the Rows Yourself with `spine_df`

`start_time`/`end_time` can only return rows the view's **root feature group** has already
observed. Pass `spine_df` instead and the query is anchored on rows you supply: one row per
entity and moment, carrying the serving keys and the time to compute features as of, under the
root feature group's event time column.

Each feature is taken from the most recent row at or before that time. The condition is never
clamped to now, so a time in the future resolves against a forecast row exactly as a past time
resolves against history. One row in, one row out, in the order you gave.

Unlike a spine group, the feature view does not have to have been created for this. `spine_df`
and `spine` both replace the left side of the query, so passing both is an error.

### Future prediction times from a schedule

`PredictionTimes` builds a schedule; `cross()` turns it plus a set of entities into the frame.
Prefer it over building the cross product by hand: it fixes the row order, entities as given and
ascending in time within each, which is the order the result comes back in, so predictions zip
back onto the frame positionally.

```python
import datetime
import pandas as pd
from hsfs.constructor.prediction_times import PredictionTimes

tomorrow = datetime.date.today() + datetime.timedelta(days=1)

entities = pd.DataFrame(
    [
        {"country": "sweden", "city": "stockholm", "street": "sveavagen"},
        {"country": "sweden", "city": "gothenburg", "street": "avenyn"},
    ]
)

# Every day at 08:00 for the next week. Also accepts a cron expression or an explicit list:
#   PredictionTimes.cron("0 8 * * 1-5", start=tomorrow, count=10)
#   PredictionTimes.of([datetime.datetime(2026, 3, 1, 8, 0)])
schedule = PredictionTimes.every("daily", offset="08:00", start=tomorrow, count=7)

batch_df = fv.get_batch_data(
    spine_df=schedule.cross(entities, event_time="date"),
    dataframe_type="pandas",
)
# 2 entities x 7 days = 14 rows, grouped by entity, ascending in time
```

The returned event time is the time you asked for, not the event time of the row that matched
it. A prediction time of 08:00 matching a forecast written at 00:00 comes back as 08:00.

### Get latest feature data

The offline equivalent of `get_feature_vectors`: one row per entity, all as of the same instant.
Capture the timestamp once so every entity is read at the same moment, rather than letting each
row drift.

**For every entity the feature store knows about**, read the entities off the feature view's
root feature group rather than listing them by hand. `get_root_fg()` returns the feature group
the view is anchored on, and `read_primary_keys()` returns its distinct primary key values, one
row per entity.

```python
import datetime
from hsfs.constructor.prediction_times import PredictionTimes

fg = fv.get_root_fg()
now = datetime.datetime.now(datetime.timezone.utc)

spine_df = PredictionTimes.of([now]).cross(
    fg.read_primary_keys(), event_time=fg.event_time
)
latest = fv.get_batch_data(spine_df=spine_df, dataframe_type="pandas")
```

`read_primary_keys()` alone is **not** a `spine_df`: it carries entities and no time, and a
batch read needs a prediction time per row, so passing it directly is refused with an error
naming the missing column. Crossing it with one instant is what makes it a spine, and the
result is each feature group's newest row at or before that instant, which is the latest
feature data.

**For a known list of entities**, build the frame directly.

```python
import datetime
import pandas as pd

entity_ids = [1, 2, 3, 4, 5]
now = datetime.datetime.now(datetime.timezone.utc)

spine_df = pd.DataFrame({"entity_id": entity_ids})
spine_df["event_time"] = now        # name it after the root feature group's event time column

latest = fv.get_batch_data(spine_df=spine_df, dataframe_type="pandas")
```

There is no implicit "as of now": the time is always in the frame. That is deliberate, because a
wall-clock default would make the same call return different rows on a re-run, and a materialized
training dataset built that way could never be reproduced.

Two things to know before reaching for `read_primary_keys()` on a large feature group. It reads
the key columns of the whole feature group and takes the distinct rows, so the cost scales with
the feature group, not with the number of entities; under the Spark engine the distinct runs in
Spark, under the Python engine the keys come back to the client first. And it returns the root's
keys only, so a joined feature group keyed on something the root does not carry is not covered
by it, and that lookup comes back NULL.

### Which frames `spine_df` accepts

A pandas DataFrame, a polars DataFrame, a list of dicts, and — under the Spark engine — a Spark
DataFrame. A Spark DataFrame is refused under the Python engine, which has no session to
evaluate it.

A Spark spine is collected to the driver to be registered as a session temporary view, which is
what the Spark spine path has always done, so size the frame to the entities you are scoring
rather than to a feature group. A Spark DataFrame also has no row order, so with one the
positional zip-back does not apply: join predictions back on the serving keys.

### Bounding staleness

An as-of lookup carries the last value forward for ever, so a feature group that stops producing
rows keeps answering with its final one and nothing in the result says so. Bound it when you create the view:

```python
fv = fs.create_feature_view(
    name="air_quality_fv",
    query=query,
    max_feature_age=datetime.timedelta(days=1),
)
```

One bound covers the whole view: every feature group it reads is held to the same limit. A
matched row older than the bound comes back `NULL` instead of a stale value, so the gap is
visible to you and to the model. It is a property of the view, so it applies to training data
built with `spine_df` as well; a training example built from a stale feature is worse than an
inference row built from one, because the model learns from it.

It is read-only afterwards, and stored with the view. That is deliberate: if it could be changed
per call, a training set and an inference read could be built with different bounds, which is the
training/serving skew a feature view exists to prevent. The backend reads the bound from the
view's own row rather than from the read, so there is no way to opt a read out of it.
`fv.max_feature_age` reads it back as a `timedelta`, or `None` when the view is unbounded.

### Training data from the same rows

`training_data`, `train_test_split`, `train_validation_test_split` and the three `create_*`
methods all take `spine_df`. Columns the view does not define are carried through untouched,
which is how the label rides along; a batch read stays strict about unknown columns, because
inference has no labels and a mistyped column there is worth catching.

```python
train_x, test_x, train_y, test_y = fv.train_test_split(test_size=0.2, spine_df=labels)
```

### What is refused

| Mistake | What happens |
|---|---|
| No time column in `spine_df` | Error naming the event time column and pointing at `PredictionTimes.cross` |
| A column matching nothing in the view (batch read) | Error listing the accepted columns |
| `spine_df` with `start_time`/`end_time` | Error: the frame's timestamps define the time axis |
| `spine_df` with `spine` | Error: both replace the left side of the query |
| A Spark `spine_df` under the Python engine | Error: no Spark session to evaluate it |
| `max_feature_age` zero or negative | Error: a bound that matches no row is a caller mistake |
| `max_feature_age` as a per-feature-group dict | Error: one bound covers the whole view |
| A passthrough column whose name is not an identifier | Error: names are rendered into SQL |
| Over the row, byte or column limit | Refused before it runs, naming the limit. Rows and columns are checked client-side too, so an oversized frame is refused before it is uploaded |

An entity that matches nothing is **not** an error: the row comes back with `NULL` features,
the same as any left join, which is what makes a brand-new entity work.

---

## Spine groups (deprecated)

Spine groups are **deprecated**, superseded by `spine_df` above. Do not create one, and do not
reach for one when asked for point-in-time joins against a set of entities: `spine_df` does the
same thing on any feature view, needs no Spark, and is decided at read time rather than when the
view is created. `fs.get_or_create_spine_group()` and the `spine=` argument both warn.

A spine group is metadata only: it registers primary keys and an event time, holds no data, and
takes a fresh dataframe on every read. Its limitation is the reason for the replacement. It has
to be chosen when the feature view is created and cannot be added afterwards, so a view built
without one can never be driven by a caller's rows, and a view built with one *requires* `spine=`
on every read.

You will still meet them on feature views created before `spine_df`. Such a view refuses a plain
read:

```python
# Feature view created with a spine group: `spine` is mandatory, and the frame must carry the
# same features as the feature group it replaces.
X_train, X_test, y_train, y_test = fv.train_test_split(test_size=0.2, spine=entities_df)
```

Migrating such a view means recreating it from a query with no spine group on the left, after
which every read takes `spine_df` instead. There is no in-place conversion.

---

## Connecting Models to Feature Views

When saving a model, link it to a feature view for automatic batch scoring initialization:

```python
# Save model with feature view provenance
model = mr.python.create_model(
    name="fraud_detector",
    version=1,
    feature_view=fv,
    training_dataset_version=1,
    metrics={"f1": 0.92},
)
model.save("./model_dir")
```

Later, retrieve the feature view directly from the model:

```python
model = mr.get_model("fraud_detector", version=1)

# Get feature view, auto-initialized for batch scoring
fv = model.get_feature_view(init=True, online=False)

# fv.init_batch_scoring() already called with the right training_dataset_version
batch_df = fv.get_batch_data(dataframe_type="pandas")
```

`get_feature_view(init=True, online=False)` automatically calls `fv.init_batch_scoring(training_dataset_version=...)` using the training dataset version linked to the model.

---

## Complete Example: Pandas Batch Inference Pipeline

```python
import hopsworks
import joblib
from datetime import datetime, timedelta

# 1. Connect
project = hopsworks.login()
mr = project.get_model_registry()
fs = project.get_feature_store()

# 2. Download and load model
model_meta = mr.get_best_model("fraud_detector", metric="f1_score", direction="max")
model_dir = model_meta.download()
model = joblib.load(f"{model_dir}/model.pkl")

# 3. Get feature view and initialize batch scoring
fv = fs.get_feature_view("fraud_features_fv", version=1)
fv.init_batch_scoring(training_dataset_version=1)

# 4. Retrieve batch data for the last 24 hours
batch_df = fv.get_batch_data(
    start_time=datetime.now() - timedelta(hours=24),
    end_time=datetime.now(),
    dataframe_type="pandas",
    primary_key=True,     # keep primary keys for joining predictions
)

# 5. Separate primary keys from features
pk_columns = ["user_id"]
feature_columns = [c for c in batch_df.columns if c not in pk_columns]

# 6. Predict
predictions = model.predict(batch_df[feature_columns])
batch_df["prediction"] = predictions

# 7. Use results
print(f"Scored {len(batch_df)} records")
print(batch_df[["user_id", "prediction"]].head())
```

---

Two more complete pipelines — **PySpark with a spine group** and **scoring via model→feature-view provenance** — are in [references/examples.md](references/examples.md).

---

## Persisting Predictions

Two distinct destinations — pick by purpose:

**1. Log feature group (monitoring / audit / drift).** The idiomatic path: the
feature view's prediction logging writes inputs + predictions to a *managed* log
feature group. Enable it once at FV creation, then `log()` after each scoring run:

```python
# At FV creation (once):
fv = fs.create_feature_view(name="...", query=query, labels=[...], logging_enabled=True)

# After batch scoring:
batch_df = fv.get_batch_data(dataframe_type="pandas")
feature_cols = [c for c in batch_df.columns if c not in ("customer_id", "event_time")]
predictions = model.predict(batch_df[feature_cols])

fv.log(batch_df, predictions=predictions)   # -> managed log FG
fv.materialize_log()                         # flush now (otherwise written periodically)

# Read it back, optionally scoped to a model, for monitoring:
logged = fv.read_log(model=model)            # also: start_time/end_time/filter
```

**2. Inference-store feature group (downstream consumption).** When dashboards or
another pipeline read the scores, write them to a normal FG (the inference store)
instead (see **hops-fg**):

```python
preds_fg = fs.get_or_create_feature_group(
    name="customer_spend_predictions", version=1,
    primary_key=["customer_id"], event_time="event_time",
)
preds_fg.insert(predictions_df)   # predictions_df = keys + event_time + prediction column
```

---

## Next Steps

- Log predictions for monitoring: this skill's "Persisting Predictions" (`fv.log`).
- Train/register the model this scores: **hops-train**. Build the FV: **hops-fv**.
- Need a live endpoint instead of batch: **hops-online-inference**.
- PySpark for large offline reads/writes: **hops-spark**.

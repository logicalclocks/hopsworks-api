---
name: hopsworks-batch-inference
description: Use when writing Python or PySpark code for batch inference with
  Hopsworks. Auto-invoke when user wants to retrieve batch data from feature views,
  use spine groups for point-in-time joins, download models from the model registry
  for batch prediction, or build batch scoring pipelines.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Batch Inference — Python SDK Best Practices

Reference: `/tmp/hopsworks-api/python/hsfs/` and `/tmp/hopsworks-api/python/hsml/`

## Overview

Batch inference in Hopsworks follows this pattern:

1. Download a trained model from the Model Registry
2. Retrieve a batch of inference data from a Feature View
3. Apply the model to produce predictions

Two approaches for retrieving batch data:
- **`get_batch_data()`** — filter by event time range from offline feature store
- **Spine groups** — provide a specific set of entities (primary keys + event times) for point-in-time correct joins

Both Pandas and PySpark are supported. Spine groups require PySpark.

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

`get_batch_data()` reads features from the offline feature store, optionally filtered by event time, and applies model-dependent transformations.

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
| `spine` | `DataFrame` or `SpineGroup` | `None` | Spine for point-in-time joins (Spark only) |
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

## Spine Groups for Point-in-Time Joins

A spine group defines a specific set of entities (primary keys + event times) for which to fetch features. The offline feature store performs point-in-time correct joins: for each entity, it retrieves the latest feature values available **before** that entity's event time.

Spine groups are metadata-only — they don't materialize data. You provide a new dataframe each time.

**Spine groups require the Spark engine and Spark DataFrames.**

### Creating a Spine Group

```python
spine_group = fs.get_or_create_spine_group(
    name="scoring_entities",
    version=1,
    description="Entities for batch scoring",
    primary_key=["user_id"],
    event_time="prediction_time",
    dataframe=scoring_entities_df,  # Spark or Pandas DataFrame
)
```

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Spine group name |
| `version` | `int` | Version (auto-incremented if None) |
| `primary_key` | `list[str]` | Primary key columns (used for join) |
| `event_time` | `str` | Event time column (for point-in-time join) |
| `features` | `list[Feature]` | Explicit schema (inferred from dataframe if omitted) |
| `dataframe` | `DataFrame` | Spine dataframe with entities to score |

### Using Spines in Feature Views

**Option A: Feature view created with a spine group as the left side of the query.**

When the spine is on the left side of the query, you only select the label/target from it (not feature columns). At inference time, you must always provide a spine dataframe:

```python
# Create feature view with spine on the left
query = spine_group.select(["label"]).join(
    features_fg.select_all(),
    on=["user_id"],
)

fv = fs.create_feature_view(
    name="fv_with_spine",
    query=query,
    labels=["label"],
)

# At inference time — must provide spine
batch_df = fv.get_batch_data(spine=new_scoring_entities_df)
```

**Option B: Feature view created with a regular feature group, spine passed at query time.**

You can pass a spine group to `get_batch_data()` to replace the left side of the join. The spine group must have the same features (primary key, event time) as the original left feature group:

```python
# Feature view created normally
fv = fs.create_feature_view(
    name="normal_fv",
    query=transactions_fg.select_all().join(users_fg.select_all(), on=["user_id"]),
    labels=["is_fraud"],
)

# At scoring time — pass spine to fetch features for specific entities
scoring_spine = fs.get_or_create_spine_group(
    name="daily_scoring",
    primary_key=["user_id"],
    event_time="timestamp",
    dataframe=todays_entities_df,
)

batch_df = fv.get_batch_data(spine=scoring_spine)
```

### Spine Group Properties

```python
# Inspect the dataframe
spine_group.dataframe.show()

# Replace the dataframe (same schema required)
spine_group.dataframe = new_dataframe

# Properties
print(spine_group.name)
print(spine_group.primary_key)
print(spine_group.event_time)
print(spine_group.features)
```

### Spines for Training Data

Spines can also be used when creating training data:

```python
X_train, X_test, y_train, y_test = fv.train_test_split(
    test_size=0.2,
    spine=training_entities_df,
)
```

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

## Complete Example: PySpark Batch Inference with Spine Group

```python
import hopsworks
from pyspark.sql import SparkSession

# Spark Connect session with Delta extensions + DeltaCatalog (mandatory for
# Hopsworks offline feature group reads/writes — see hops-pyspark skill).
spark = (
    SparkSession.builder.appName("batch_inference")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# 1. Connect
project = hopsworks.login()
mr = project.get_model_registry()
fs = project.get_feature_store()

# 2. Download model
model_meta = mr.get_model("fraud_detector", version=1)
model_dir = model_meta.download()

# 3. Load model and broadcast to Spark executors
import joblib
model = joblib.load(f"{model_dir}/model.pkl")
bc_model = spark.sparkContext.broadcast(model)

# 4. Create spine with entities to score
scoring_entities = spark.sql("""
    SELECT user_id, current_timestamp() as prediction_time
    FROM active_users
    WHERE last_active > date_sub(current_date(), 1)
""")

spine_group = fs.get_or_create_spine_group(
    name="daily_scoring_spine",
    version=1,
    primary_key=["user_id"],
    event_time="prediction_time",
    dataframe=scoring_entities,
)

# 5. Get feature view and retrieve batch data with spine
fv = fs.get_feature_view("fraud_features_fv", version=1)
fv.init_batch_scoring(training_dataset_version=1)

batch_df = fv.get_batch_data(
    spine=spine_group,
    dataframe_type="spark",
)

# 6. Apply model using Spark UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def predict_udf(*features: pd.Series) -> pd.Series:
    import numpy as np
    X = np.column_stack([f.values for f in features])
    return pd.Series(bc_model.value.predict_proba(X)[:, 1])

feature_columns = [c for c in batch_df.columns if c != "user_id"]
predictions_df = batch_df.withColumn(
    "fraud_probability",
    predict_udf(*[batch_df[c] for c in feature_columns])
)

predictions_df.show()
```

---

## Complete Example: Using Model-Feature View Provenance

```python
import hopsworks
import joblib

# 1. Connect
project = hopsworks.login()
mr = project.get_model_registry()

# 2. Get model (linked to feature view at training time)
model_meta = mr.get_model("fraud_detector", version=1)
model_dir = model_meta.download()
model = joblib.load(f"{model_dir}/model.pkl")

# 3. Get the feature view directly from the model
#    init_batch_scoring() is called automatically with the correct training_dataset_version
fv = model_meta.get_feature_view(init=True, online=False)

# 4. Score
batch_df = fv.get_batch_data(dataframe_type="pandas")
predictions = model.predict(batch_df)
print(predictions)
```

---

## Quick Reference

| Task | Code |
|---|---|
| Get model | `mr.get_model("name", version=1)` |
| Get best model | `mr.get_best_model("name", metric="f1", direction="max")` |
| Download model | `model_dir = model.download()` |
| Init batch scoring | `fv.init_batch_scoring(training_dataset_version=1)` |
| Get batch data | `fv.get_batch_data(start_time=..., end_time=..., dataframe_type="pandas")` |
| Batch data (all) | `fv.get_batch_data(dataframe_type="pandas")` |
| With primary keys | `fv.get_batch_data(primary_key=True)` |
| Raw (untransformed) | `fv.get_batch_data(transformed=False)` |
| Create spine group | `fs.get_or_create_spine_group(name=..., primary_key=[...], event_time=..., dataframe=df)` |
| Batch with spine | `fv.get_batch_data(spine=spine_group)` |
| FV from model | `fv = model.get_feature_view(init=True, online=False)` |
| Model metrics | `model.training_metrics` |
| Model framework | `model.framework` |

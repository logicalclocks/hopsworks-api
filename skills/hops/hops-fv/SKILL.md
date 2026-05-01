---
name: hopsworks-fv
description: Use when writing Python code that creates, queries, or manages Hopsworks
  feature views via the hsfs SDK. Auto-invoke when user builds feature views, selects
  features, applies transformations, creates training data, retrieves feature vectors,
  or asks about feature view best practices (labels, filters, joins, transformations,
  online serving, embeddings).
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Feature Views — Python SDK Best Practices

Reference: `/tmp/hopsworks-api/python/hsfs/`

## What Is a Feature View

A feature view defines a set of features from one or more feature groups, joined together via a Query. It is the single interface used for:

- Creating reproducible training datasets
- Retrieving online feature vectors for model serving
- Batch scoring with offline data

---

## Creating a Feature View

### 1. Build a Query (Feature Selection)

Feature selection starts from feature groups. Use `select()`, `select_all()`, or `select_except()` on a feature group to create a Query, then join additional queries.

```python
import hopsworks

project = hopsworks.login()
fs = project.get_feature_store()

# Get feature groups
transactions_fg = fs.get_feature_group("transactions", version=1)
users_fg = fs.get_feature_group("users", version=1)

# Select specific features
query = (
    transactions_fg.select(["user_id", "amount", "merchant", "timestamp"])
    .join(users_fg.select(["user_id", "age", "country"]), on=["user_id"])
)
```

#### Feature Selection Methods

| Method | Description |
|---|---|
| `fg.select(["col1", "col2"])` | Select specific features |
| `fg.select_all()` | Select all features |
| `fg.select_except(["col1"])` | Select all except named features |
| `fg.select_all(include_primary_key=False, include_event_time=False)` | Select all, excluding keys/timestamps |

#### Join Types

```python
query = fg1.select_all().join(fg2.select_all(), on=["shared_key"], join_type="left")
```

Supported join types: `"left"` (default), `"inner"`, `"right"`, `"full"`, `"cross"`, `"left_semi_join"`.

For different key names on each side:

```python
query = fg1.select_all().join(
    fg2.select_all(),
    left_on=["user_id"],
    right_on=["customer_id"],
    join_type="inner",
)
```

Use `prefix` to avoid column name clashes when joining feature groups with overlapping column names:

```python
query = fg1.select_all().join(fg2.select_all(), on=["id"], prefix="fg2_")
```

### 2. Create the Feature View

```python
feature_view = fs.create_feature_view(
    name="fraud_detection_fv",
    version=1,
    description="Features for fraud detection model",
    query=query,
    labels=["is_fraud"],
    inference_helper_columns=["merchant"],
    training_helper_columns=["timestamp"],
    transformation_functions=[...],  # see Transformations section
)
```

Or get-or-create (idempotent):

```python
feature_view = fs.get_or_create_feature_view(
    name="fraud_detection_fv",
    version=1,
    query=query,
    labels=["is_fraud"],
)
```

### Key Parameters

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Feature view name |
| `query` | `Query` | Query defining feature selection and joins |
| `version` | `int` | Version number (auto-increments if None) |
| `labels` | `list[str]` | Feature names used as prediction target. Excluded from feature vectors at inference |
| `inference_helper_columns` | `list[str]` | Features not used in model but available during inference (e.g., for post-processing). Excluded from `get_feature_vector()`, available via `get_inference_helper()` |
| `training_helper_columns` | `list[str]` | Features not in model schema but useful during training (e.g., for sampling). Excluded at inference time |
| `transformation_functions` | `list` | Model-dependent transformations (see below) |
| `logging_enabled` | `bool` | Enable feature vector logging |

---

## Online Model Serving Requirement

**All feature groups in the feature view must be `online_enabled` for online feature vector retrieval.** If any feature group is not online-enabled, `init_serving()` will raise an error.

The only exception: if **all** features in the feature view are on-demand (computed at runtime by transformation functions, not stored in any feature group), then online-enabled is not required.

```python
# This will fail at init_serving() if users_fg is NOT online_enabled
feature_view = fs.create_feature_view(
    name="my_fv",
    query=transactions_fg.select_all().join(users_fg.select_all()),
    ...
)
feature_view.init_serving()  # raises FeatureStoreException
```

---

## Vector Embeddings in Feature Views

Feature groups with an `embedding_index` (vector embeddings) **can** be included in a feature view. However:

- Embedding feature groups are **skipped from standard online feature store lookups** — their features are retrieved from the vector database instead
- You **cannot use embedding features as precomputed features** (via `passed_features`) in `get_feature_vector()` — they are resolved from the vector DB by primary key
- Use `find_neighbors()` on the feature view to perform similarity search, which then automatically retrieves the full feature vector for each neighbor

```python
from hsfs.embedding import EmbeddingIndex, EmbeddingFeature, SimilarityFunctionType

# Feature group with embeddings
embedding_index = EmbeddingIndex()
embedding_index.add_embedding(name="user_vector", dimension=384)

embedding_fg = fs.get_or_create_feature_group(
    name="user_embeddings",
    embedding_index=embedding_index,
    primary_key=["user_id"],
    online_enabled=True,
    stream=True,
    ...
)

# Include in feature view
fv = fs.create_feature_view(
    name="recommendation_fv",
    query=embedding_fg.select_all().join(profile_fg.select_all(), on=["user_id"]),
    ...
)

# Similarity search — returns full feature vectors for nearest neighbors
fv.init_serving()
neighbors = fv.find_neighbors(
    embedding=[0.1, 0.2, ...],  # query vector
    k=10,
    filter=(embedding_fg.active == True),  # optional filter
)
```

---

## Filters

Filters can be applied at multiple levels: on queries (which define feature views), when creating training data, and during batch/online retrieval.

### Filter Operators

Features support Python comparison operators that produce `Filter` objects:

| Operator | Meaning |
|---|---|
| `fg.col == value` | Equals |
| `fg.col != value` | Not equals |
| `fg.col > value` | Greater than |
| `fg.col >= value` | Greater than or equal |
| `fg.col < value` | Less than |
| `fg.col <= value` | Less than or equal |
| `fg.col.isin([v1, v2])` | In list |
| `fg.col.like("pattern%")` | SQL LIKE pattern |

Combine with `&` (AND) and `|` (OR). Always use parentheses:

```python
combined = (fg.amount > 100) & (fg.status == "active")
either = (fg.country == "US") | (fg.country == "CA")
```

### Filters on the Query (Feature View Definition)

Filters applied to the query are baked into the feature view and always active:

```python
query = (
    transactions_fg.select_all()
    .filter(transactions_fg.amount > 0)
    .join(
        users_fg.select_all().filter(users_fg.active == True),
        on=["user_id"],
    )
)
fv = fs.create_feature_view(name="my_fv", query=query, ...)
```

### Filters on Training Data (`extra_filter`)

Apply additional filters when creating training data without changing the feature view definition:

```python
version, job = fv.create_training_data(
    extra_filter=(transactions_fg.merchant != "test_merchant") & (users_fg.age >= 18),
    start_time="2025-01-01",
    end_time="2025-06-01",
)
```

The `extra_filter` is stored with the training dataset metadata and automatically reapplied when reading with `get_batch_data()`.

### Time-Based Filters

Training data methods support `start_time` / `end_time` parameters that filter on the feature group's `event_time` column:

```python
version, job = fv.create_training_data(
    start_time="2025-01-01",
    end_time="2025-06-01",
    description="H1 2025 training data",
)
```

### Filters on Vector Similarity Search

```python
neighbors = fv.find_neighbors(
    embedding=[0.1, 0.2, 0.3],
    k=5,
    filter=(fg.category == "electronics") & (fg.price < 1000),
)
```

---

## Transformations

Transformations in feature views come in two types:

1. **Model-dependent transformations** — use training statistics, applied during both training and inference
2. **On-demand transformations** — compute features at runtime from request parameters, no statistics

### Built-in Transformations

Import from `hsfs.builtin_transformations`. All built-in transformations are model-dependent (they learn statistics from training data).

**Scaling & Normalization:**

| Function | Description |
|---|---|
| `min_max_scaler(feature)` | Scale to [0, 1] using training min/max |
| `standard_scaler(feature)` | Standardize using training mean/stddev |
| `robust_scaler(feature)` | Scale using median/IQR (outlier-robust) |

**Distribution Transforms:**

| Function | Description |
|---|---|
| `log_transform(feature)` | Natural log (values <= 0 become NaN) |
| `quantile_transformer(feature)` | Map to uniform [0, 1] via training percentiles |
| `rank_normalizer(feature)` | Percentile rank in training distribution |

**Outlier Handling:**

| Function | Description | Context |
|---|---|---|
| `winsorize(feature)` | Clip at percentile boundaries | `{"p_low": 5, "p_high": 95}` |

**Binning / Discretization:**

| Function | Description | Context |
|---|---|---|
| `equal_width_binner(feature)` | Equal-width bins (default 10) | `{"n_bins": 20}` |
| `equal_frequency_binner(feature)` | Quartile-based bins (4 bins) | |
| `quantile_binner(feature)` | Quantile-based bins | |

**Encoding:**

| Function | Description | Context |
|---|---|---|
| `label_encoder(feature)` | Categorical to integer (unseen -> -1) | |
| `one_hot_encoder(feature)` | One-hot boolean columns (unseen -> all False) | |
| `top_k_categorical_binner(feature)` | Group rare categories to "Other" | `{"top_n": 20, "other_label": "Rare"}` |

**Imputation:**

| Function | Description | Context |
|---|---|---|
| `impute_mean(feature)` | Fill NaN with training mean | |
| `impute_median(feature)` | Fill NaN with training median | |
| `impute_constant(feature)` | Fill NaN with constant | `{"value": -1.0}` |
| `impute_mode(feature)` | Fill NaN with most frequent category | |
| `impute_category(feature)` | Fill NaN with sentinel string | `{"value": "Unknown"}` |

**Usage:**

```python
from hsfs.builtin_transformations import standard_scaler, label_encoder, impute_mean

fv = fs.create_feature_view(
    name="my_fv",
    version=1,
    query=query,
    labels=["target"],
    transformation_functions=[
        impute_mean("age"),
        standard_scaler("age"),
        label_encoder("country"),
    ],
)
```

**Setting transformation context** (for built-ins that accept it):

```python
from hsfs.builtin_transformations import winsorize, equal_width_binner

w = winsorize("income")
w.transformation_context = {"p_low": 5, "p_high": 95}

b = equal_width_binner("score")
b.transformation_context = {"n_bins": 20}

fv = fs.create_feature_view(
    name="my_fv",
    query=query,
    transformation_functions=[w, b],
    ...
)
```

### Custom Transformations — the `@udf` Decorator

Define custom transformations using `@udf` from `hsfs`:

```python
from hsfs import udf
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `return_type` | `type` or `list[type]` | Output type(s): `float`, `int`, `str`, `bool`, `datetime`, `date` |
| `drop` | `str` or `list[str]` | Feature names to drop after transformation |
| `mode` | `"default"`, `"python"`, `"pandas"` | Execution mode |

### Execution Modes

| Mode | Offline (Batch) | Online (Single Value) | Use Case |
|---|---|---|---|
| `"default"` | Receives `pd.Series`, returns `pd.Series` | Receives scalar, returns scalar | Best for most cases — auto-adapts |
| `"pandas"` | Always `pd.Series` → `pd.Series` | Always `pd.Series` → `pd.Series` | Vectorized operations, Spark pandas UDF |
| `"python"` | Always scalar → scalar | Always scalar → scalar | Simple per-row logic |

### Custom UDF Examples

**Simple transformation (no statistics):**

```python
@udf(float, drop=["amount"])
def log_amount(amount):
    import math
    return math.log1p(amount) if amount > 0 else 0.0
```

**Transformation with training statistics:**

```python
from hsfs.transformation_statistics import TransformationStatistics

stats = TransformationStatistics("price")

@udf(float, drop=["price"])
def z_score(price, statistics=stats):
    return (price - statistics.price.mean) / statistics.price.stddev
```

Available statistics properties: `mean`, `stddev`, `min`, `max`, `percentiles`, `unique_values`, `histogram`, `count`, `completeness`, `distinctness`, `entropy`.

**Transformation with context:**

```python
@udf(float)
def apply_discount(price, context):
    return price * (1 - context["discount_rate"])

tf = apply_discount("price")
tf.transformation_context = {"discount_rate": 0.1}
```

**Multiple outputs:**

```python
@udf([float, float], drop=["timestamp"])
def time_features(timestamp):
    import pandas as pd
    hour = timestamp.hour if not isinstance(timestamp, pd.Series) else timestamp.dt.hour
    day = timestamp.dayofweek if not isinstance(timestamp, pd.Series) else timestamp.dt.dayofweek
    return hour, day
```

**Pandas mode (vectorized):**

```python
@udf(float, drop=["feature"], mode="pandas")
def clip_outliers(feature: pd.Series) -> pd.Series:
    return feature.clip(lower=0, upper=1000)
```

### Custom Output Column Names

```python
tf = log_amount("price")
tf.alias("log_price")  # custom output name
```

### On-Demand Transformations

On-demand transformations compute features at inference time from request parameters. They **cannot** use training statistics.

On-demand transformations are attached to features at the feature group level (not at the feature view level). They are automatically included in feature views that select those features.

```python
# When retrieving a feature vector, provide request_parameters
vector = fv.get_feature_vector(
    entry={"user_id": 123},
    request_parameters={"current_location": "NYC"},
)
```

---

## Labels

Labels are features used as the prediction target. They are:

- **Included** in training data (returned as a separate DataFrame)
- **Excluded** from feature vectors at inference time
- **Excluded** from `get_batch_data()` output by default

```python
fv = fs.create_feature_view(
    name="churn_fv",
    query=query,
    labels=["churned"],     # single label
    # labels=["label1", "label2"],  # multi-label
)

# Training: labels returned separately
X_train, X_test, y_train, y_test = fv.get_train_test_split(training_dataset_version=1)

# Inference: labels excluded automatically
vector = fv.get_feature_vector(entry={"customer_id": 42})
```

---

## Training Data

### Create and Materialize Training Data

```python
# Full dataset
version, job = fv.create_training_data(
    description="Q1 2025 training data",
    start_time="2025-01-01",
    end_time="2025-04-01",
    data_format="parquet",
    statistics_config=True,
    extra_filter=(fg.active == True),
)

# Train/test split (random)
version, job = fv.create_train_test_split(
    test_size=0.2,
    seed=42,
    data_format="parquet",
)

# Train/test split (time-based)
version, job = fv.create_train_test_split(
    train_start="2024-01-01",
    train_end="2025-01-01",
    test_start="2025-01-01",
    test_end="2025-04-01",
)

# Train/validation/test split
version, job = fv.create_train_validation_test_split(
    validation_size=0.1,
    test_size=0.1,
    seed=42,
)
```

### Retrieve Materialized Training Data

```python
X, y = fv.get_training_data(
    training_dataset_version=1,
    dataframe_type="polars",
    primary_key=False,       # exclude primary keys
    event_time=False,        # exclude event time
    training_helper_columns=False,
)

X_train, X_test, y_train, y_test = fv.get_train_test_split(
    training_dataset_version=1,
    dataframe_type="polars",
)

X_train, X_val, X_test, y_train, y_val, y_test = fv.get_train_validation_test_split(
    training_dataset_version=1,
    dataframe_type="pandas",
)
```

### In-Memory Training Data (No Materialization)

For quick iteration, get training data directly as DataFrames without materializing to storage:

```python
X, y = fv.training_data(
    start_time="2025-01-01",
    end_time="2025-04-01",
    dataframe_type="polars",
)

X_train, X_test, y_train, y_test = fv.train_test_split(
    test_size=0.2,
    dataframe_type="polars",
)
```

These still create metadata for reproducibility but skip writing to storage.

### Training Data Parameters

| Parameter | Type | Description |
|---|---|---|
| `start_time` / `end_time` | `str`, `datetime`, `int` | Filter by event time (inclusive start, exclusive end) |
| `extra_filter` | `Filter` / `Logic` | Additional filter expression |
| `data_format` | `str` | `"parquet"`, `"csv"`, `"tfrecord"`, `"avro"`, `"orc"`, `"json"` |
| `coalesce` | `bool` | Write to single file (default: False) |
| `seed` | `int` | Random seed for reproducible splits |
| `statistics_config` | `bool` / `dict` | Compute statistics for transformations |
| `spine` | `DataFrame` | Spine for point-in-time joins (Spark only) |
| `test_size` | `float` | Fraction for test set (0-1) |
| `validation_size` | `float` | Fraction for validation set (0-1) |

---

## Online Feature Vector Retrieval

### Initialize Serving

```python
fv.init_serving(
    training_dataset_version=1,  # version with transformation statistics
    external=None,               # auto-detect environment
)
```

### Single Feature Vector

```python
vector = fv.get_feature_vector(
    entry={"user_id": 123},
    return_type="pandas",     # "list" (default), "pandas", "polars", "numpy"
)
```

### Batch Feature Vectors

```python
vectors = fv.get_feature_vectors(
    entry=[{"user_id": 123}, {"user_id": 456}],
    return_type="pandas",
)
```

### Passed Features (Runtime Values)

Provide feature values from the application that override or supplement stored features:

```python
vector = fv.get_feature_vector(
    entry={"user_id": 123},
    passed_features={"device_type": "mobile", "session_duration": 45.2},
)
```

Feature value priority (highest to lowest):
1. `request_parameters` — on-demand transformation inputs
2. `passed_features` — runtime application values
3. Online feature store — stored values
4. On-demand computation — computed features

### Request Parameters (On-Demand Features)

```python
vector = fv.get_feature_vector(
    entry={"user_id": 123},
    request_parameters={"query_text": "running shoes"},
)
```

### Inference Helper Columns

Retrieved separately from the feature vector:

```python
helpers = fv.get_inference_helper(
    entry={"user_id": 123},
    return_type="dict",   # "pandas" (default), "dict", "polars"
)
```

### Control Transformations

```python
# Get untransformed feature vector
raw_vector = fv.get_feature_vector(
    entry={"user_id": 123},
    transform=False,
)

# Skip on-demand features
vector = fv.get_feature_vector(
    entry={"user_id": 123},
    on_demand_features=False,
)
```

---

## Batch Scoring (Offline)

```python
# Initialize batch scoring (optional — called automatically)
fv.init_batch_scoring(training_dataset_version=1)

# Get batch data with transformations applied
batch_df = fv.get_batch_data(
    start_time="2025-03-01",
    end_time="2025-04-01",
    dataframe_type="polars",
    transformed=True,           # apply model-dependent transformations
    primary_key=False,
    event_time=False,
    inference_helper_columns=False,
)
```

---

## Complete Example: End-to-End Feature View Pipeline

```python
import hopsworks
from hsfs.builtin_transformations import standard_scaler, label_encoder, impute_mean
from hsfs import udf

# 1. Connect
project = hopsworks.login()
fs = project.get_feature_store()

# 2. Get feature groups (all must be online_enabled for online serving)
transactions_fg = fs.get_feature_group("transactions", version=1)
users_fg = fs.get_feature_group("users", version=1)

# 3. Build query with feature selection and filters
query = (
    transactions_fg.select(["user_id", "amount", "merchant", "category", "timestamp", "is_fraud"])
    .filter(transactions_fg.amount > 0)
    .join(
        users_fg.select(["user_id", "age", "country", "account_age_days"]),
        on=["user_id"],
    )
)

# 4. Define custom transformation
@udf(float, drop=["amount"])
def log_amount(amount):
    import math
    return math.log1p(amount)

# 5. Create feature view
fv = fs.get_or_create_feature_view(
    name="fraud_detection_fv",
    version=1,
    query=query,
    labels=["is_fraud"],
    inference_helper_columns=["merchant"],
    training_helper_columns=["timestamp"],
    transformation_functions=[
        impute_mean("age"),
        standard_scaler("age"),
        standard_scaler("account_age_days"),
        log_amount("amount"),
        label_encoder("category"),
        label_encoder("country"),
    ],
)

# 6. Create training data
version, job = fv.create_train_test_split(
    test_size=0.2,
    seed=42,
    statistics_config=True,
    description="Fraud detection training data",
)

# 7. Retrieve training data
X_train, X_test, y_train, y_test = fv.get_train_test_split(
    training_dataset_version=version,
    dataframe_type="pandas",
)

# 8. Train model (user's code)
model = train_model(X_train, y_train)

# 9. Online serving
fv.init_serving(training_dataset_version=version)

vector = fv.get_feature_vector(
    entry={"user_id": 42},
    return_type="list",
)
prediction = model.predict([vector])

# 10. Batch scoring
batch_df = fv.get_batch_data(
    start_time="2025-03-01",
    end_time="2025-04-01",
    dataframe_type="pandas",
)
batch_predictions = model.predict(batch_df)
```

---

## Quick Reference

| Task | Code |
|---|---|
| Create feature view | `fs.create_feature_view(name=..., query=..., labels=[...])` |
| Get feature view | `fs.get_feature_view("name", version=1)` |
| Select features | `fg.select(["col1", "col2"])` |
| Select all except | `fg.select_except(["col1"])` |
| Join feature groups | `fg1.select_all().join(fg2.select_all(), on=["key"])` |
| Filter query | `.filter((fg.col > 10) & (fg.col2 == "x"))` |
| Create training data | `fv.create_training_data(start_time=..., end_time=...)` |
| Train/test split | `fv.create_train_test_split(test_size=0.2)` |
| Get training data | `fv.get_training_data(training_dataset_version=1)` |
| In-memory training data | `fv.training_data(start_time=..., end_time=...)` |
| Init online serving | `fv.init_serving(training_dataset_version=1)` |
| Get feature vector | `fv.get_feature_vector(entry={"pk": val})` |
| Batch feature vectors | `fv.get_feature_vectors(entry=[{"pk": v1}, {"pk": v2}])` |
| Get inference helpers | `fv.get_inference_helper(entry={"pk": val})` |
| Batch scoring | `fv.get_batch_data(start_time=..., end_time=...)` |
| Similarity search | `fv.find_neighbors(embedding=[...], k=10)` |
| Delete feature view | `fv.delete()` |

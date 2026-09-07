---
name: hops-fv
description: Use when writing Python code that creates, queries, or manages Hopsworks feature views via the hsfs SDK. Auto-invoke when user builds feature views, selects features, applies transformations, creates training data, retrieves feature vectors, or asks about feature view best practices (labels, filters, joins, transformations, online serving, embeddings).
---

# Hopsworks Feature Views — Python SDK Best Practices

A feature view defines a set of features from one or more feature groups, joined together via a Query. It is metadata-only (stores no data) and is the **read interface** of the feature store — the V between F and T/I in the FTI pattern — used for reproducible training datasets, online feature vectors for serving, and offline batch data. It is the feature store's mechanism for preventing training/serving skew: it returns the same ordered features and applies the same model-dependent (MDT) and on-demand (ODT) transformations in training and inference pipelines.

## Contract

- **Input:** one or more feature groups, a feature selection (`select`/`join`) forming a Query, an optional label column, and optional transformation functions.
- **Output:** a named, versioned feature view that produces reproducible training datasets, online feature vectors for serving, and offline batch data.
- **Pre-condition:** the source feature groups already exist. For online serving every feature group in the view must be `online_enabled` (sole exception: all-on-demand views). The label, if any, must be in the query selection.

## Smoke-test

Verify state with the `hops` CLI (cheap pre/post-flight):

```bash
hops fv list                                    # list feature views (id, name, version, labels)
hops fv info <name> --version 1                 # metadata + schema; flags the label column
hops td list <fv-name> --version 1              # training-dataset versions
hops fv get <name> --version 1 --entry "pk=val" # one online feature vector, no Python
```

Non-interactive delete needs flags: `hops fv delete <name> --version 1 --yes --force`.

### Build the view + training data from the CLI

The whole F→T handoff can run from the CLI, no Python — this is what the terminal
kickoff flow uses. Register any custom transforms first (the `--transform` flag
resolves them **by name** from the transformation store, so an unregistered udf
fails):

```bash
hops transformation create --file transformations.py            # register udfs first
hops fv create <name> --feature-group <derived_fg>:1 \
  --transform <fn>:<col> --labels <label>                       # --join "<fg> LEFT <on>" repeatable
hops td compute <fv> <fv_version> --split "train:0.8,test:0.2"  # positional = FEATURE-VIEW version
hops td list <fv>                                               # TD version auto-increments; read it back here
```

`hops fv create --feature-group` takes `name[:version]`; `--transform` and `--join`
are repeatable. `hops td compute` takes the FV version as a required positional —
the training-dataset version it writes auto-increments, so read it from `hops td list`
rather than assuming `1`.

---

## Ask the user (only when state is ambiguous)

- **Label column** — which selected feature is the prediction target (or none, for an unsupervised / retrieval view). It must be in the query selection.
- **Which features** — which feature groups and columns to select, and how they join.
- **Online vs offline source FGs** — whether the view needs online serving. If yes, every source feature group must be `online_enabled`; confirm before relying on `init_serving()`.
- **Before deleting** — `fv.delete()` / `hops fv delete --yes --force` is irreversible; confirm the exact name and version with the user, and never tear down a feature view you created as a side effect (temp or test ones included) unless they asked.

---

## Creating a Feature View

### 1. Build a Query (Feature Selection)

Feature selection starts from a **root feature group** (often, but not necessarily, the one holding the label). From the root you can reach any feature group connected by a join key path; a feature group with no path from the root cannot be included. Use `select()`, `select_all()`, or `select_except()` on a feature group to create a Query, then join additional queries.

```python
import hopsworks

project = hopsworks.login()
fs = project.get_feature_store()

transactions_fg = fs.get_feature_group("transactions", version=1)
users_fg = fs.get_feature_group("users", version=1)

query = (
    transactions_fg.select(["user_id", "amount", "merchant", "timestamp", "is_fraud"])
    .join(users_fg.select(["user_id", "age", "country"]), on=["user_id"])
)
```

| Method | Description |
|---|---|
| `fg.select(["col1", "col2"])` | Select specific features |
| `fg.select_all()` | Select all features |
| `fg.select_except(["col1"])` | Select all except named features |
| `fg.select_all(include_primary_key=False, include_event_time=False)` | Select all, excluding keys/timestamps |

Joins: `join_type` is `"left"` (default), `"inner"`, `"right"`, `"full"`, `"cross"`, or `"left_semi_join"`; `left_on`/`right_on` join on differently named keys; `prefix` avoids column-name clashes.

```python
query = fg1.select_all().join(fg2.select_all(), on=["shared_key"], join_type="left")
query = fg1.select_all().join(fg2.select_all(), left_on=["user_id"], right_on=["customer_id"], join_type="inner")
query = fg1.select_all().join(fg2.select_all(), on=["id"], prefix="fg2_")
```

### 2. Create the Feature View

```python
feature_view = fs.create_feature_view(          # or fs.get_or_create_feature_view(...) for idempotent creation
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

**Set a description.** Pass `description=` on the feature view so it is not an empty envelope in the UI. Per-feature descriptions come from the source feature groups, so describe the columns at the FG (see hops-fg), not here.

> **The label must be in the query selection.** `labels=[...]` only marks which *already-selected* columns are targets; it does not add them. If the label is not in your `select(...)` (or is dropped by `select_except([...])`), create fails with `FeatureStoreException: Feature name '<label>' could not be found in query`. Select the label, then name it in `labels=`.

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Feature view name |
| `query` | `Query` | Query defining feature selection and joins |
| `version` | `int` | Version number (auto-increments if None) |
| `labels` | `list[str]` | Which *selected* features are the prediction target (one or several). Returned as a separate `y` by the training-data methods; excluded from feature vectors at inference and from `get_batch_data()` by default |
| `inference_helper_columns` | `list[str]` | Features not used in model but available during inference (e.g., for post-processing). Excluded from `get_feature_vector()`, available via `get_inference_helper()` |
| `training_helper_columns` | `list[str]` | Features not in model schema but useful during training (e.g., for sampling, or for slicing evaluation data by a sensitive attribute like gender to check for bias without training on it). Excluded at inference time |
| `transformation_functions` | `list` | Model-dependent transformations (see below) |
| `logging_enabled` | `bool` | Enable feature vector logging |

---

## Online Model Serving Requirement

**All feature groups in the feature view must be `online_enabled` for online feature vector retrieval.** If any feature group is not online-enabled, `init_serving()` raises `FeatureStoreException`. The only exception: if **all** features in the feature view are on-demand (computed at runtime by transformation functions, not stored in any feature group), then online-enabled is not required.

---

## Vector Embeddings in Feature Views

Feature groups with an `embedding_index` (vector embeddings) **can** be included in a feature view. However:

- Embedding feature groups are **skipped from standard online feature store lookups** — their features are retrieved from the vector database instead
- You **cannot use embedding features as precomputed features** (via `passed_features`) in `get_feature_vector()` — they are resolved from the vector DB by primary key
- Use `find_neighbors()` on the feature view to perform similarity search, which then automatically retrieves the full feature vector for each neighbor

```python
# embedding_fg was created with embedding_index=EmbeddingIndex(...) (see hops-fg)
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

### Where filters apply

```python
# On the query: baked into the feature view and always active
query = (
    transactions_fg.select_all()
    .filter(transactions_fg.amount > 0)
    .join(users_fg.select_all().filter(users_fg.active == True), on=["user_id"])
)
fv = fs.create_feature_view(name="my_fv", query=query, ...)

# On training data: extra_filter is stored with the training dataset metadata
# and reapplied by get_batch_data(); start_time/end_time filter on event_time
version, job = fv.create_training_data(
    extra_filter=(transactions_fg.merchant != "test_merchant") & (users_fg.age >= 18),
    start_time="2025-01-01",
    end_time="2025-06-01",
    description="H1 2025 training data",
)

# On similarity search
neighbors = fv.find_neighbors(embedding=[0.1, 0.2, 0.3], k=5, filter=(fg.category == "electronics") & (fg.price < 1000))
```

---

## Transformations

Transformations on a feature view are the **T** in FTI. Two kinds:
- **Model-dependent (MDT)** — statistics-based (scalers, encoders, imputers), specific to one model, attached here via `transformation_functions=` on `create_feature_view`; applied as the last step before the model, at both training and serving. Training-dataset statistics (mean, min/max, encoding maps) are stored with the training dataset, so `init_serving`/`init_batch_scoring` take a `training_dataset_version` to apply the exact same MDT at inference and avoid skew.
- **On-demand (ODT)** — computed at request time from `request_parameters`, registered at the feature group (not the FV, since they also run in feature pipelines); auto-included when this FV selects them.

```python
from hsfs.builtin_transformations import standard_scaler, label_encoder, impute_mean

fv = fs.create_feature_view(
    name="my_fv", version=1, query=query, labels=["target"],
    transformation_functions=[impute_mean("age"), standard_scaler("age"), label_encoder("country")],
)
# on-demand at serving:
fv.get_feature_vector(entry={"user_id": 123}, request_parameters={"current_location": "NYC"})
```

> A transform **renames its output** to `<fn>_<col>_`, and a udf is **frozen into the FV at create** (fixing it forces a new FV version + retrain). Custom udfs import from `hopsworks` (`from hopsworks import udf`), and a default-mode udf must run on **both** a scalar (online) and a Series (offline) or it 500s on the first online predict.

**Full transformation reference** — built-in tables, the `@udf` decorator, execution modes, statistics, on-demand, and the transformation store: see [hops-transformations](../hops-transformations/SKILL.md).

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
version, job = fv.create_train_test_split(test_size=0.2, seed=42, data_format="parquet")

# Train/test split (time-based)
version, job = fv.create_train_test_split(
    train_start="2024-01-01", train_end="2025-01-01",
    test_start="2025-01-01", test_end="2025-04-01",
)

# Train/validation/test split
version, job = fv.create_train_validation_test_split(validation_size=0.1, test_size=0.1, seed=42)
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

X_train, X_test, y_train, y_test = fv.get_train_test_split(training_dataset_version=1, dataframe_type="polars")

X_train, X_val, X_test, y_train, y_val, y_test = fv.get_train_validation_test_split(
    training_dataset_version=1, dataframe_type="pandas",
)
```

### In-Memory Training Data (No Materialization)

For quick iteration, get training data directly as DataFrames without materializing to storage. These still create metadata for reproducibility but skip writing to storage.

```python
X, y = fv.training_data(start_time="2025-01-01", end_time="2025-04-01", dataframe_type="polars")
X_train, X_test, y_train, y_test = fv.train_test_split(test_size=0.2, dataframe_type="polars")
```

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

```python
fv.init_serving(
    training_dataset_version=1,  # version with transformation statistics
    external=None,               # auto-detect environment
)

vector = fv.get_feature_vector(
    entry={"user_id": 123},
    return_type="pandas",     # "list" (default), "pandas", "polars", "numpy"
)
vectors = fv.get_feature_vectors(entry=[{"user_id": 123}, {"user_id": 456}], return_type="pandas")

# Runtime values from the application that override or supplement stored features
vector = fv.get_feature_vector(entry={"user_id": 123}, passed_features={"device_type": "mobile", "session_duration": 45.2})

# On-demand transformation inputs
vector = fv.get_feature_vector(entry={"user_id": 123}, request_parameters={"query_text": "running shoes"})

# Inference helper columns are retrieved separately from the feature vector
helpers = fv.get_inference_helper(entry={"user_id": 123}, return_type="dict")   # "pandas" (default), "dict", "polars"

# Control transformations
raw_vector = fv.get_feature_vector(entry={"user_id": 123}, transform=False)          # untransformed
vector = fv.get_feature_vector(entry={"user_id": 123}, on_demand_features=False)     # skip on-demand features
```

Feature value priority (highest to lowest):
1. `request_parameters` — on-demand transformation inputs
2. `passed_features` — runtime application values
3. Online feature store — stored values
4. On-demand computation — computed features

---

## Batch Scoring (Offline)

```python
fv.init_batch_scoring(training_dataset_version=1)   # optional — called automatically

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
from hopsworks import udf

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

## Next Steps

- Train a model on this view's training data: **hops-train**.
- Batch scoring: **hops-batch-inference**. Online serving: **hops-online-inference**.
- Need to create or fix the source feature groups first: **hops-fg**.

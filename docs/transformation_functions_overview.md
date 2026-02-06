# Transformation Functions

Transformation functions enable feature engineering that is consistent between training and inference,
preventing training-serving skew in your ML pipelines.

## Overview

Hopsworks supports two types of transformation functions:

| Type | Attached To | Statistics Access | When Executed | Use Cases |
|------|-------------|-------------------|---------------|-----------|
| **Model-Dependent** | Feature Views | Yes (training data) | Training, batch inference, online inference | Scaling, encoding, normalization |
| **On-Demand** | Feature Groups | No | Data insertion, online inference | Real-time calculations, time-based features |

## Quick Start

### Creating a Transformation Function

Use the [`@udf`][hsfs.hopsworks_udf.udf] decorator to create transformation functions:

```python
from hopsworks import udf

# Simple transformation
@udf(return_type=float)
def add_one(feature):
    return feature + 1

# Transformation with statistics (model-dependent)
from hsfs.transformation_statistics import TransformationStatistics

stats = TransformationStatistics("feature")

@udf(return_type=float, drop=["feature"])
def normalize(feature, statistics=stats):
    return (feature - statistics.feature.mean) / statistics.feature.stddev
```

### Using as Model-Dependent Transformation

Attach to a feature view for consistent transformations across training and inference:

```python
from hsfs.builtin_transformations import min_max_scaler, label_encoder

feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[
        min_max_scaler("numeric_feature"),
        label_encoder("category_feature")
    ]
)

# Transformations applied automatically during:
# - Training data creation
train_data = feature_view.create_training_data()

# - Batch inference
batch_data = feature_view.get_batch_data()

# - Online inference
vector = feature_view.get_feature_vector(entry={"id": 1})
```

### Using as On-Demand Transformation

Attach to a feature group for real-time feature computation:

```python
@udf(return_type=int, drop=["event_time"])
def days_since_event(event_time, context):
    return (context["current_time"] - event_time).dt.days

fg = fs.create_feature_group(
    name="events",
    primary_key=["event_id"],
    transformation_functions=[days_since_event]
)

# On insert: transformation computes and stores the feature
fg.insert(df)

# At inference: provide request_parameters for real-time computation
vector = fv.get_feature_vector(
    entry={"event_id": 1},
    request_parameters={"current_time": datetime.now()}
)
```

## Supported Types

The `return_type` parameter accepts these Python types:

| Python Type | Spark Type | Description |
|-------------|------------|-------------|
| `str` | `string` | Text data |
| `int` | `bigint` | Integer numbers |
| `float` | `double` | Floating-point numbers |
| `bool` | `boolean` | Boolean values |
| `datetime.datetime` | `timestamp` | Timestamp with date and time |
| `datetime.date` | `date` | Date without time |
| `datetime.time` | `timestamp` | Time without date |

## Execution Modes

Control how transformations execute with the `mode` parameter:

| Mode | Online Inference | Batch/Training | Best For |
|------|------------------|----------------|----------|
| `"default"` | Python UDF | Pandas UDF | Most use cases (auto-optimizes) |
| `"python"` | Python UDF | Python UDF | Non-vectorizable operations |
| `"pandas"` | Pandas UDF | Pandas UDF | Always-vectorized operations |

## API Reference

### Core Classes

- [`@udf`](udf.md) - Decorator to create transformation functions
- [`TransformationFunction`](transformation_functions_api.md) - Wrapper for saved transformations
- [`HopsworksUdf`](hopsworks_udf.md) - Metadata container for UDFs

### Statistics Classes

- [`TransformationStatistics`](transformation_statistics.md) - Declare statistics requirements
- [`FeatureTransformationStatistics`](feature_transformation_statistics.md) - Access computed statistics

### Built-in Transformations

- [`min_max_scaler`](builtin_transformations.md#min_max_scaler) - Scale to [0, 1]
- [`standard_scaler`](builtin_transformations.md#standard_scaler) - Standardize (zero mean, unit variance)
- [`robust_scaler`](builtin_transformations.md#robust_scaler) - Outlier-robust scaling
- [`label_encoder`](builtin_transformations.md#label_encoder) - Encode categories as integers
- [`one_hot_encoder`](builtin_transformations.md#one_hot_encoder) - One-hot encode categories

### Feature Store Methods

- [`FeatureStore.create_transformation_function()`][hsfs.feature_store.FeatureStore.create_transformation_function] - Create transformation metadata
- [`FeatureStore.get_transformation_function()`][hsfs.feature_store.FeatureStore.get_transformation_function] - Retrieve saved transformation
- [`FeatureStore.get_transformation_functions()`][hsfs.feature_store.FeatureStore.get_transformation_functions] - List all transformations

### Integration Points

- [`FeatureView.transformation_functions`][hsfs.feature_view.FeatureView.transformation_functions] - Model-dependent transformations
- [`FeatureGroup.transformation_functions`][hsfs.feature_group.FeatureGroup.transformation_functions] - On-demand transformations

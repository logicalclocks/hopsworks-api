# UDF Decorator

The `@udf` decorator is the primary way to create transformation functions in Hopsworks.
It wraps a Python function with metadata that enables execution across different engines
(Spark, Python) and contexts (training, batch inference, online inference).

## Basic Usage

```python
from hopsworks import udf

@udf(return_type=float)
def my_transform(feature):
    return feature * 2
```

## Transformation Patterns

| Pattern | Input | Output | Example |
|---------|-------|--------|---------|
| One-to-one | 1 feature | 1 feature | Scaling a single column |
| Many-to-one | N features | 1 feature | Combining multiple columns |
| One-to-many | 1 feature | N features | Splitting a column |
| Many-to-many | N features | M features | Complex transformations |

## Key Parameters

- **`return_type`**: Python type(s) for output features (`float`, `int`, `str`, `bool`, `datetime`, etc.)
- **`drop`**: Input features to remove after transformation (e.g., `drop=["original_feature"]`)
- **`mode`**: Execution mode (`"default"`, `"python"`, `"pandas"`)

## Special Arguments

### Statistics (for model-dependent transformations)

```python
from hsfs.transformation_statistics import TransformationStatistics

stats = TransformationStatistics("feature")

@udf(return_type=float)
def normalize(feature, statistics=stats):
    return (feature - statistics.feature.mean) / statistics.feature.stddev
```

### Context (for dynamic values)

```python
@udf(return_type=int)
def days_ago(event_time, context):
    return (context["current_time"] - event_time).dt.days
```

## See Also

- [Transformation Functions Overview](transformation_functions_api.md) - Complete guide
- [Built-in Transformations](builtin_transformations.md) - Pre-built scalers and encoders
- [TransformationStatistics](transformation_statistics.md) - Using training data statistics

---

::: hsfs.hopsworks_udf.udf

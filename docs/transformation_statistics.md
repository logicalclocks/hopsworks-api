# Transformation Statistics

`TransformationStatistics` declares which features require training dataset statistics
in a transformation function. Statistics are automatically computed when you create
a training dataset and injected into model-dependent transformations.

## Usage

```python
from hopsworks import udf
from hsfs.transformation_statistics import TransformationStatistics

# Declare statistics for one feature
stats = TransformationStatistics("salary")

@udf(return_type=float, drop=["salary"])
def standardize(salary, statistics=stats):
    return (salary - statistics.salary.mean) / statistics.salary.stddev

# Declare statistics for multiple features
multi_stats = TransformationStatistics("feature1", "feature2")

@udf(return_type=float)
def combine(feature1, feature2, statistics=multi_stats):
    return (feature1 / statistics.feature1.max) + (feature2 / statistics.feature2.max)
```

## Available Statistics

Each feature provides access to these statistics via
[`FeatureTransformationStatistics`][hsfs.transformation_statistics.FeatureTransformationStatistics]:

| Property | Description | Used By |
|----------|-------------|---------|
| `mean` | Arithmetic mean | `standard_scaler` |
| `stddev` | Standard deviation | `standard_scaler` |
| `min` | Minimum value | `min_max_scaler` |
| `max` | Maximum value | `min_max_scaler` |
| `percentiles[N]` | Nth percentile (0-99) | `robust_scaler` |
| `unique_values` | Set of distinct values | `label_encoder`, `one_hot_encoder` |

## See Also

- [Transformation Functions Overview](transformation_functions_api.md)
- [FeatureTransformationStatistics](feature_transformation_statistics.md) - Full statistics reference
- [Built-in Transformations](builtin_transformations.md) - Pre-built functions using statistics

---

::: hsfs.transformation_statistics.TransformationStatistics

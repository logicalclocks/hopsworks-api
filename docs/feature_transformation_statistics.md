# Feature Transformation Statistics

`FeatureTransformationStatistics` provides access to computed statistics for a single feature.
These statistics are computed from the training dataset and used by model-dependent transformations.

## Accessing Statistics

Statistics are accessed through the [`TransformationStatistics`][hsfs.transformation_statistics.TransformationStatistics]
object in your transformation function:

```python
from hopsworks import udf
from hsfs.transformation_statistics import TransformationStatistics

stats = TransformationStatistics("age", "income")

@udf(return_type=[float, float])
def normalize_features(age, income, statistics=stats):
    # Access statistics for each feature
    norm_age = (age - statistics.age.min) / (statistics.age.max - statistics.age.min)
    norm_income = (income - statistics.income.mean) / statistics.income.stddev
    return norm_age, norm_income
```

## Available Properties

### Basic Counts
- `count` - Total number of rows
- `num_non_null_values` - Count of non-null values
- `num_null_values` - Count of null values
- `completeness` - Fraction of non-null values (0.0 to 1.0)

### Numerical Statistics
- `min` - Minimum value
- `max` - Maximum value
- `sum` - Sum of all values
- `mean` - Arithmetic mean
- `stddev` - Standard deviation
- `percentiles` - Dictionary mapping percentile index (0-99) to value

### Uniqueness Metrics
- `approx_num_distinct_values` - Approximate count of unique values
- `exact_num_distinct_values` - Exact count of unique values
- `distinctness` - Fraction of distinct values
- `uniqueness` - Fraction of values appearing exactly once
- `entropy` - Shannon entropy

### Extended Statistics
- `correlations` - Correlations with other features
- `histogram` - Histogram bin data
- `unique_values` - Set of unique values (for categorical features)

## See Also

- [TransformationStatistics](transformation_statistics.md) - Container for declaring statistics requirements
- [Built-in Transformations](builtin_transformations.md) - See how statistics are used

---

::: hsfs.transformation_statistics.FeatureTransformationStatistics

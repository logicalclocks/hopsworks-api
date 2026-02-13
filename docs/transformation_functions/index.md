# Transformation Functions

## Quick Start

Define a transformation function using the [`@udf`](../udf.md) decorator:

```python
from hopsworks import udf

@udf(return_type=float)
def double(feature):
    return feature * 2
```

### Model-Dependent Transformation

Attach to a [`FeatureView`](../feature_view_api.md) for transformations that stay consistent between training and inference:

```python
feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[double("my_feature")]
)
```

### On-Demand Transformation

Attach to a [`FeatureGroup`](../feature_group_api.md) for features computed at request time:

```python
fg = fs.create_feature_group(
    name="my_fg",
    primary_key=["id"],
    transformation_functions=[double]
)
```

## API Reference

- [`@udf`](../udf.md) - Decorator to create transformation functions
- [`TransformationFunction`](../transformation_functions_api.md) - Wrapper for saved transformations
- [`HopsworksUdf`](../hopsworks_udf.md) - Metadata container for UDFs
- [`TransformationStatistics`](../transformation_statistics.md) - Access training dataset statistics in transformations
- [`FeatureTransformationStatistics`](../feature_transformation_statistics.md) - Statistics for a single feature
- [`Built-in Transformations`](../builtin_transformations.md) - Pre-built scalers and encoders

# HopsworksUdf

`HopsworksUdf` is the metadata container created by the [`@udf`][hsfs.hopsworks_udf.udf] decorator.
It manages the transformation function's source code, input/output mappings, and execution across
different engines (Spark, Python).

## When You'll Use This

You typically don't create `HopsworksUdf` directly. Instead, you interact with it through:

1. **The `@udf` decorator** - Creates the `HopsworksUdf` instance
2. **Calling the UDF with feature names** - Binds to specific features
3. **The `alias()` method** - Sets custom output column names

## Common Operations

```python
from hopsworks import udf

# Create via decorator
@udf(return_type=float)
def my_transform(feature):
    return feature * 2

# Bind to specific features
bound_transform = my_transform("my_feature_column")

# Set custom output names
my_transform.alias("doubled_feature")

# Access metadata
print(my_transform.function_name)          # "my_transform"
print(my_transform.transformation_features) # ["feature"]
print(my_transform.output_column_names)     # ["doubled_feature"]
print(my_transform.return_types)            # ["double"]
```

## See Also

- [`@udf`](udf.md) - Decorator to create transformation functions
- [Transformation Functions Overview](transformation_functions_api.md) - Complete guide
- [`TransformationFunction`](transformation_functions_api.md#transformationfunction-class) - Wrapper for saved transformations

---

::: hsfs.hopsworks_udf.HopsworksUdf

::: hsfs.hopsworks_udf.TransformationFeature

::: hsfs.hopsworks_udf.UDFExecutionMode

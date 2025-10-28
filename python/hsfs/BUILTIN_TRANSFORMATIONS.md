# Built-in Transformation Functions

The HSFS library includes several ready-to-use transformation functions in `hsfs.builtin_transformations`. These transformations are model-dependent and compute statistics on your training data during the first training dataset creation, then apply those same statistics at serving time.

## Available Transformations

### Scaling & Normalization

- **`min_max_scaler(feature)`**: Scale feature to [0, 1] using training min/max.
- **`standard_scaler(feature)`**: Standardize feature using training mean and standard deviation.
- **`robust_scaler(feature)`**: Scale using median and IQR (interquartile range). Robust to outliers. If IQR is zero, centers by median only.

### Distribution Transforms

- **`log_transform(feature)`**: Apply natural logarithm (ln) to reduce skewness. Values ≤ 0 become NaN.
- **`quantile_transformer(feature)`**: Map values to a uniform [0, 1] distribution using training percentiles with linear interpolation.
- **`rank_normalizer(feature)`**: Replace each value with its percentile rank in the training distribution (0 to 1).

### Outlier Handling

- **`winsorize(feature)`**: Clip extreme values at specified percentiles (default: 1st and 99th). Override thresholds via transformation context:
  ```python
  from hsfs.builtin_transformations import winsorize
  
  tf = winsorize("my_feature")
  tf.hopsworks_udf.transformation_context = {"p_low": 5, "p_high": 95}
  ```

### Discretization / Binning

- **`equal_width_binner(feature)`**: Discretize into 10 equal-width bins using training min/max.
- **`equal_frequency_binner(feature)`**: Discretize into 4 bins using training quartiles (Q1/Q2/Q3).
- **`quantile_binner(feature)`**: Quantile-based binning (default: quartiles).

### Encoding

- **`label_encoder(feature)`**: Encode categorical values as integers based on sorted unique values from training. Unseen categories → -1.
- **`one_hot_encoder(feature)`**: One-hot encode categorical features. Unseen categories → all False.
<!-- 
- **`target_mean_encoder(feature, label)`**: Replace categories with the mean of the target variable.
  - **Training**: Computes per-category target means from `feature` and `label` Series.
  - **Serving**: Use a precomputed mapping via transformation context:
    ```python
    from hsfs.builtin_transformations import target_mean_encoder
    
    tf = target_mean_encoder("category_col", "label_col")
    tf.hopsworks_udf.transformation_context = {
        "target_means": {"A": 1.5, "B": 0.8},
        "global_mean": 1.2
    }
    ```
  - Unseen categories fall back to `global_mean` if provided, otherwise NaN.
  - Only the feature column is dropped; the label column is preserved.
-->

## Usage

Import any built-in transformation and attach it to your Feature View:

```python
from hsfs.builtin_transformations import robust_scaler, winsorize

# Attach transformations to features
fv.create_training_data(
    transformation_functions=[
        robust_scaler("age"),
        winsorize("income")
    ]
)
```

Statistics are computed automatically during training dataset creation and applied during batch scoring or online serving.

## Custom Transformations

You can also define your own UDFs using the `@udf` decorator. See the [Transformation Functions API documentation](https://docs.hopsworks.ai) for details.

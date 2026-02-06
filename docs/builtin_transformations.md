# Built-in Transformation Functions

Hopsworks provides pre-built transformation functions for common feature engineering tasks.
All built-in transformations are **model-dependent**, meaning they use statistics computed
from the training dataset to ensure consistency between training and inference.

## Available Transformations

| Function | Type | Description | Drops Input |
|----------|------|-------------|-------------|
| [`min_max_scaler`](#min_max_scaler) | Numerical | Scale to [0, 1] using min/max | Yes |
| [`standard_scaler`](#standard_scaler) | Numerical | Standardize to zero mean, unit variance | Yes |
| [`robust_scaler`](#robust_scaler) | Numerical | Scale using median and IQR (outlier-robust) | Yes |
| [`label_encoder`](#label_encoder) | Categorical | Map categories to integers (0, 1, 2, ...) | Yes |
| [`one_hot_encoder`](#one_hot_encoder) | Categorical | Create binary columns per category | Yes |

## Usage

### From Feature Store

```python
# Retrieve built-in transformations
min_max = fs.get_transformation_function(name="min_max_scaler")
label_enc = fs.get_transformation_function(name="label_encoder")

# Attach to feature view
feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[
        min_max("age"),
        label_enc("category")
    ]
)
```

### Direct Import

```python
from hsfs.builtin_transformations import (
    min_max_scaler,
    standard_scaler,
    robust_scaler,
    label_encoder,
    one_hot_encoder
)

feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[
        min_max_scaler("age"),
        standard_scaler("salary"),
        label_encoder("department")
    ]
)
```

## Handling Unknown Categories

For `label_encoder` and `one_hot_encoder`:

- **label_encoder**: Unknown categories (not in training data) are encoded as `-1`
- **one_hot_encoder**: Unknown categories result in all-`False` vectors

---

## min_max_scaler

Scale features to the [0, 1] range using min-max normalization.

**Formula:**
```
scaled = (value - min) / (max - min)
```

where `min` and `max` are computed from the training dataset.

**When to use:** Features that need to be bounded within a specific range, particularly for algorithms sensitive to feature scales (e.g., neural networks, KNN).

**Warning:** Min-max scaling is sensitive to outliers since they affect the min and max values. Consider using `robust_scaler` if your data contains outliers.

```python
from hsfs.builtin_transformations import min_max_scaler

feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[min_max_scaler("age")]
)
```

---

## standard_scaler

Standardize features to zero mean and unit variance (z-score normalization).

**Formula:**
```
standardized = (value - mean) / std_dev
```

where `mean` and `std_dev` are computed from the training dataset.

**When to use:** Algorithms that assume normally distributed features or are sensitive to feature scales (e.g., SVM, logistic regression, PCA).

**Note:** Unlike min-max scaling, standardized values are not bounded to a specific range. Values can be negative or greater than 1 depending on the distribution.

```python
from hsfs.builtin_transformations import standard_scaler

feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[standard_scaler("salary")]
)
```

---

## robust_scaler

Scale features using median and interquartile range (IQR) for outlier robustness.

**Formula:**
```
scaled = (value - median) / IQR
```

where `median` is the 50th percentile and `IQR` (interquartile range) is the difference between the 75th and 25th percentiles.

**When to use:** Data containing outliers that would skew min-max or standard scaling. Common use cases include financial data, sensor readings, or any feature with extreme values.

```python
from hsfs.builtin_transformations import robust_scaler

feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[robust_scaler("transaction_amount")]
)
```

---

## label_encoder

Encode categorical features as integer labels.

Maps each unique category to an integer value (0, 1, 2, ...) based on **alphabetically sorted** categories from the training dataset.

**Encoding behavior:**

| Input | Output |
|-------|--------|
| "apple" | 0 |
| "banana" | 1 |
| "cherry" | 2 |
| "dragonfruit" (unseen) | -1 |
| NaN | NaN |

**Warning:** Categories not present in the training dataset are encoded as `-1`. Ensure your model can handle this unknown category indicator.

**Note:** Label encoding introduces an implicit ordering (0 < 1 < 2). For nominal (unordered) categories, consider using `one_hot_encoder` instead.

```python
from hsfs.builtin_transformations import label_encoder

feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[label_encoder("product_category")]
)
```

---

## one_hot_encoder

Encode categorical features as binary (one-hot) vectors.

Creates one boolean column per category found in the training dataset. Each row has exactly one `True` value (for its category) and `False` for all others.

**Encoding behavior** (for categories ["blue", "green", "red"]):

| Input | blue | green | red |
|-------|------|-------|-----|
| "red" | False | False | True |
| "green" | False | True | False |
| "blue" | True | False | False |
| "yellow" (unseen) | False | False | False |

**Note:** The number of output columns equals the number of unique categories in the training data. The output schema is determined at training time and remains fixed during inference.

**Warning:** Categories not present in the training dataset result in all-`False` vectors (no category activated).

**Warning:** For features with many unique values, one-hot encoding creates many columns. Consider using `label_encoder` or embedding-based approaches for high-cardinality features.

```python
from hsfs.builtin_transformations import one_hot_encoder

feature_view = fs.create_feature_view(
    name="my_view",
    query=fg.select_all(),
    transformation_functions=[one_hot_encoder("color")]
)
```

---

## See Also

- [Transformation Functions Overview](transformation_functions_api.md) - Complete guide to transformation functions
- [TransformationStatistics](transformation_statistics.md) - Statistics these functions use
- [UDF Decorator](udf.md) - Create custom transformation functions

#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Built-in transformation functions for common feature engineering operations.

This module provides pre-defined transformation functions for scaling numerical features
and encoding categorical features.
All built-in transformations are model-dependent, meaning they use statistics computed
from the training dataset to ensure consistency between training and inference.

These transformations can be:

1. Retrieved from the feature store using `FeatureStore.get_transformation_function()`
2. Imported directly from this module

Example: Using built-in transformations from the feature store
    ```python
    # Retrieve from feature store
    min_max_scaler = fs.get_transformation_function(name="min_max_scaler")
    label_encoder = fs.get_transformation_function(name="label_encoder")

    # Attach to feature view
    feature_view = fs.create_feature_view(
        name="my_view",
        query=fg.select_all(),
        transformation_functions=[
            min_max_scaler("numeric_feature"),
            label_encoder("category_feature")
        ]
    )
    ```

Example: Importing built-in transformations directly
    ```python
    from hsfs.builtin_transformations import min_max_scaler, standard_scaler

    feature_view = fs.create_feature_view(
        name="my_view",
        query=fg.select_all(),
        transformation_functions=[
            min_max_scaler("age"),
            standard_scaler("salary")
        ]
    )
    ```

Available Transformations:
    - `min_max_scaler`: Scale to [0, 1] range
    - `standard_scaler`: Standardize to zero mean, unit variance
    - `robust_scaler`: Scale using median and IQR (outlier-robust)
    - `label_encoder`: Encode categories as integers
    - `one_hot_encoder`: Encode categories as binary vectors

Note: Model-Dependent Transformations
    All built-in transformations require training dataset statistics and are therefore
    model-dependent.
    Statistics are automatically computed when you create a training dataset
    from a feature view that uses these transformations.
"""

import math

import pandas as pd
from hsfs.hopsworks_udf import udf
from hsfs.transformation_statistics import TransformationStatistics


feature_statistics = TransformationStatistics("feature")


@udf(float, drop=["feature"])
def min_max_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Scale features to the [0, 1] range using min-max normalization.

    Transforms each value using the formula:

        scaled = (value - min) / (max - min)

    where `min` and `max` are computed from the training dataset.
    This transformation is useful for features that need to be bounded within a specific range,
    particularly for algorithms sensitive to feature scales (e.g., neural networks, KNN).

    Example: Attach to a feature view
        ```python
        from hsfs.builtin_transformations import min_max_scaler

        feature_view = fs.create_feature_view(
            name="my_view",
            query=fg.select_all(),
            transformation_functions=[min_max_scaler("age")]
        )
        ```

    Warning: Sensitivity to Outliers
        Min-max scaling is sensitive to outliers since they affect the min and max values.
        Consider using [`robust_scaler`][hsfs.builtin_transformations.robust_scaler] if your data contains outliers.

    Parameters:
        feature: The numerical feature to scale.
        statistics: Training dataset statistics (automatically provided).

    Returns:
        Scaled feature values in the range [0, 1].

    See Also:
        - [`standard_scaler`][hsfs.builtin_transformations.standard_scaler]: Alternative scaling to zero mean, unit variance.
        - [`robust_scaler`][hsfs.builtin_transformations.robust_scaler]: Outlier-robust scaling using percentiles.
    """
    return (feature - statistics.feature.min) / (
        statistics.feature.max - statistics.feature.min
    )


@udf(float, drop=["feature"])
def standard_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Standardize features to zero mean and unit variance (z-score normalization).

    Transforms each value using the formula:

        standardized = (value - mean) / std_dev

    where `mean` and `std_dev` are computed from the training dataset.
    This transformation is useful for algorithms that assume normally distributed features
    or are sensitive to feature scales (e.g., SVM, logistic regression, PCA).

    Example: Attach to a feature view
        ```python
        from hsfs.builtin_transformations import standard_scaler

        feature_view = fs.create_feature_view(
            name="my_view",
            query=fg.select_all(),
            transformation_functions=[standard_scaler("salary")]
        )
        ```

    Note: Output Range
        Unlike min-max scaling, standardized values are not bounded to a specific range.
        Values can be negative or greater than 1 depending on the distribution.

    Parameters:
        feature: The numerical feature to standardize.
        statistics: Training dataset statistics (automatically provided).

    Returns:
        Standardized feature values with approximately zero mean and unit variance.

    See Also:
        - [`min_max_scaler`][hsfs.builtin_transformations.min_max_scaler]: Scale to bounded [0, 1] range.
        - [`robust_scaler`][hsfs.builtin_transformations.robust_scaler]: Outlier-robust scaling.
    """
    return (feature - statistics.feature.mean) / statistics.feature.stddev


@udf(float, drop=["feature"])
def robust_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Scale features using median and interquartile range (IQR) for outlier robustness.

    Transforms each value using the formula:

        scaled = (value - median) / IQR

    where `median` is the 50th percentile and `IQR` (interquartile range) is the difference
    between the 75th and 25th percentiles, computed from the training dataset.

    This transformation is robust to outliers because the median and IQR are less affected
    by extreme values than the mean and standard deviation.

    Example: Attach to a feature view
        ```python
        from hsfs.builtin_transformations import robust_scaler

        feature_view = fs.create_feature_view(
            name="my_view",
            query=fg.select_all(),
            transformation_functions=[robust_scaler("transaction_amount")]
        )
        ```

    Note: When to Use
        Use robust_scaler when your data contains outliers that would skew min-max or standard scaling.
        Common use cases include financial data, sensor readings, or any feature with extreme values.

    Parameters:
        feature: The numerical feature to scale.
        statistics: Training dataset statistics (automatically provided).

    Returns:
        Scaled feature values centered around 0, with IQR normalized to 1.

    See Also:
        - [`min_max_scaler`][hsfs.builtin_transformations.min_max_scaler]: Scale to [0, 1] range.
        - [`standard_scaler`][hsfs.builtin_transformations.standard_scaler]: Standardize using mean and std_dev.
    """
    return (feature - statistics.feature.percentiles[49]) / (
        statistics.feature.percentiles[74] - statistics.feature.percentiles[24]
    )


@udf(int, drop=["feature"], mode="pandas")
def label_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Encode categorical features as integer labels.

    Maps each unique category to an integer value (0, 1, 2, ...) based on alphabetically
    sorted categories from the training dataset. Categories not seen during training
    are encoded as -1.

    Example: Attach to a feature view
        ```python
        from hsfs.builtin_transformations import label_encoder

        feature_view = fs.create_feature_view(
            name="my_view",
            query=fg.select_all(),
            transformation_functions=[label_encoder("product_category")]
        )
        ```

    Example: Encoding behavior
        If training data contains categories ["apple", "banana", "cherry"]:

        - "apple" → 0
        - "banana" → 1
        - "cherry" → 2
        - "dragonfruit" (unseen) → -1
        - NaN → NaN

    Warning: Unknown Categories
        Categories not present in the training dataset are encoded as -1.
        Ensure your model can handle this unknown category indicator.

    Note: Ordinal vs Nominal
        Label encoding introduces an implicit ordering (0 < 1 < 2).
        For nominal (unordered) categories, consider using
        [`one_hot_encoder`][hsfs.builtin_transformations.one_hot_encoder] instead.

    Parameters:
        feature: The categorical feature to encode.
        statistics: Training dataset statistics (automatically provided).

    Returns:
        Integer-encoded feature values (0 to n_categories-1), or -1 for unknown categories.

    See Also:
        - [`one_hot_encoder`][hsfs.builtin_transformations.one_hot_encoder]: Create binary columns for each category.
    """
    unique_data = sorted(statistics.feature.unique_values)
    value_to_index = {value: index for index, value in enumerate(unique_data)}
    return pd.Series(
        [
            value_to_index.get(data, -1) if not pd.isna(data) else math.nan
            for data in feature
        ]
    )


@udf(bool, drop=["feature"], mode="pandas")
def one_hot_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Encode categorical features as binary (one-hot) vectors.

    Creates one boolean column per category found in the training dataset.
    Each row has exactly one True value (for its category) and False for all others.
    Categories not seen during training result in all-False vectors.

    The output columns are named `{feature_name}_{category}` and sorted alphabetically by category.

    Example: Attach to a feature view
        ```python
        from hsfs.builtin_transformations import one_hot_encoder

        feature_view = fs.create_feature_view(
            name="my_view",
            query=fg.select_all(),
            transformation_functions=[one_hot_encoder("color")]
        )
        ```

    Example: Encoding behavior
        If training data contains categories ["red", "green", "blue"]:

        - "red" → [False, True, False] (columns: blue, green, red)
        - "green" → [False, True, False]
        - "yellow" (unseen) → [False, False, False]

    Note: Output Column Count
        The number of output columns equals the number of unique categories in the training data.
        The output schema is determined at training time and remains fixed during inference.

    Warning: Unknown Categories
        Categories not present in the training dataset result in all-False vectors (no category activated).
        This is a safe default but may affect model predictions for unseen categories.

    Warning: High Cardinality
        For features with many unique values, one-hot encoding creates many columns.
        Consider using [`label_encoder`][hsfs.builtin_transformations.label_encoder] or
        embedding-based approaches for high-cardinality features.

    Parameters:
        feature: The categorical feature to encode.
        statistics: Training dataset statistics (automatically provided).

    Returns:
        DataFrame with boolean columns, one per category from training data.

    See Also:
        - [`label_encoder`][hsfs.builtin_transformations.label_encoder]: Encode as single integer column.
    """
    unique_data = list(statistics.feature.unique_values)

    one_hot = pd.get_dummies(feature, dtype="bool").reindex(
        unique_data, axis=1, fill_value=False
    )

    return one_hot.reindex(sorted(one_hot.columns), axis=1)

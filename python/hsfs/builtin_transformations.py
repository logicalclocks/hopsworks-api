#
#   Copyright 2024, 2025 Hopsworks AB
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

from __future__ import annotations

import contextlib

import numpy as np
import pandas as pd
from hopsworks_common import constants
from hsfs.hopsworks_udf import udf
from hsfs.transformation_statistics import TransformationStatistics


feature_statistics = TransformationStatistics("feature")


@udf(float, drop=["feature"])
def min_max_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    min_val = statistics.feature.min
    max_val = statistics.feature.max
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return feature - (min_val if not pd.isna(min_val) else 0)
    return (feature - min_val) / (max_val - min_val)


@udf(float, drop=["feature"])
def standard_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    mean = statistics.feature.mean
    stddev = statistics.feature.stddev
    if pd.isna(mean):
        return feature
    if pd.isna(stddev) or stddev == 0:
        return feature - mean
    return (feature - mean) / stddev


@udf(float, drop=["feature"], mode="pandas")
def robust_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Robust scaling using median and IQR.

    Scales a feature by removing the median and dividing by the interquartile range (IQR = Q3 - Q1).
    This makes the transformation robust to outliers.
    If IQR is zero (constant feature), the function centers the data by the median without scaling to avoid division by zero.

    Parameters:
        feature: Numeric feature to scale.
        statistics: Training statistics providing percentile values; populated automatically.

    Returns:
        Series of float64 values centered on the median and scaled by IQR.
    """
    percentiles = statistics.feature.percentiles
    scaled_feature = feature.astype("float64")
    if not percentiles or len(percentiles) < 75:
        return scaled_feature

    q1 = percentiles[24]
    q2 = percentiles[49]
    q3 = percentiles[74]
    iqr = q3 - q1

    if pd.isna(iqr) or iqr == 0:
        # Constant feature or invalid IQR: center only
        return scaled_feature - q2

    return (scaled_feature - q2) / iqr


@udf(int, drop=["feature"], mode="pandas")
def label_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    unique_data = sorted(statistics.feature.unique_values)
    value_to_index = {value: index for index, value in enumerate(unique_data)}
    # Unknown categories not present in training dataset are encoded as -1.
    return pd.Series(
        [
            value_to_index.get(data, -1) if not pd.isna(data) else pd.NA
            for data in feature
        ],
        dtype="Int64",
        index=feature.index,
    )


@udf(bool, drop=["feature"], mode="pandas")
def one_hot_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Encode a categorical feature as a boolean one-hot DataFrame.

    Creates one boolean column per category seen during training.
    Categories absent from training statistics are encoded as all-False.
    Output columns are sorted alphabetically for consistent ordering.

    Parameters:
        feature: Categorical feature to encode.
        statistics: Training statistics providing the set of known categories; populated automatically.

    Returns:
        Boolean DataFrame with one column per category seen during training.
    """
    unique_data = list(statistics.feature.unique_values)

    # One hot encode features. Re-indexing to set missing categories to False and drop categories not in training data statistics.
    # Hence one-hot encoded features would have all categories as False when a category not in training dataset is encountered.
    one_hot = pd.get_dummies(feature, dtype="bool").reindex(
        unique_data, axis=1, fill_value=False
    )

    # Sorting by columns so as to maintain consistency in column order.
    return one_hot.reindex(sorted(one_hot.columns), axis=1)


@udf(float, drop=["feature"], mode="pandas")
def log_transform(feature: pd.Series) -> pd.Series:
    """Apply natural logarithm to a numeric feature.

    Only strictly positive values are transformed: `y = ln(x) if x > 0 else nan`.
    This is useful to reduce skewness or model exponential relationships.

    Parameters:
        feature: Numeric feature with positive values to transform.

    Returns:
        Series of float64 values with natural log applied; non-positive inputs become NaN.
    """
    # Ensure float dtype and handle NaNs; values <= 0 become NaN
    numeric_feature = feature.astype("float64")
    return pd.Series(
        np.where(numeric_feature > 0, np.log(numeric_feature), np.nan),
        index=feature.index,
    )


@udf(int, drop=["feature"], mode="pandas")
def equal_width_binner(
    feature: pd.Series, statistics=feature_statistics, context: dict | None = None
) -> pd.Series:
    """Discretize numeric values into equal-width bins using training min/max.

    Default number of bins is 10, configurable via `context["n_bins"]`.
    Values below min are placed in the first bin; values above max in the last bin.
    NaN inputs remain NaN.

    Parameters:
        feature: Numeric feature to discretize.
        statistics: Training statistics providing min and max; populated automatically.
        context: Optional configuration dict; supports `"n_bins"` to set the number of bins (default `10`).

    Returns:
        Series of nullable integer bin indices starting from 0.

    Example: Using 20 bins
        ```python
        tf = equal_width_binner("feature")
        tf.transformation_context = {"n_bins": 20}
        ```
    """
    numerical_feature = feature.astype("float64")
    min_v = statistics.feature.min
    max_v = statistics.feature.max

    # Handle degenerate/constant features
    if pd.isna(min_v) or pd.isna(max_v) or min_v == max_v:
        result = pd.Series(0, index=feature.index, dtype="Int64")
        result[numerical_feature.isna()] = pd.NA
        return result

    # Get number of bins from context, default to 10
    ctx = context or {}

    bins = 10
    with contextlib.suppress(ValueError, TypeError):
        bins = max(2, int(ctx.get("n_bins", bins)))  # Ensure at least 2 bins

    edges = np.linspace(min_v, max_v, num=bins + 1)
    edges[0] = -np.inf
    edges[-1] = np.inf
    binned = pd.cut(
        numerical_feature,
        bins=edges,
        labels=False,
        include_lowest=True,
        right=True,
        duplicates="drop",
    )

    # Convert to nullable integer type
    return binned.astype("Int64")


@udf(int, drop=["feature"], mode="pandas")
def equal_frequency_binner(
    feature: pd.Series, statistics=feature_statistics
) -> pd.Series:
    """Discretize numeric values into equal-frequency bins using training quartiles.

    Uses Q1/Q2/Q3 percentiles as boundaries to form up to 4 bins.
    If quartiles have duplicates (constant regions), fewer bins are created.
    NaN inputs remain NaN.

    Parameters:
        feature: Numeric feature to discretize.
        statistics: Training statistics providing quartile percentiles; populated automatically.

    Returns:
        Series of nullable integer bin indices from 0 to 3.
    """
    s = feature.astype("float64")
    percentiles = statistics.feature.percentiles
    if not percentiles or len(percentiles) < 75:
        result = pd.Series(0, index=feature.index, dtype="Int64")
        result[s.isna()] = pd.NA
        return result

    q1 = percentiles[24]
    q2 = percentiles[49]
    q3 = percentiles[74]

    # Check if we have valid quartiles
    if any(pd.isna([q1, q2, q3])):
        # Invalid statistics: assign all to bin 0
        result = pd.Series(0, index=feature.index, dtype="Int64")
        result[s.isna()] = pd.NA
        return result

    # Create bin edges with infinity boundaries
    edges = np.array([-np.inf, q1, q2, q3, np.inf])

    # Remove duplicate edges (handles constant features)
    edges = np.unique(edges)

    if len(edges) < 2:
        # Degenerate case: assign all to bin 0
        result = pd.Series(0, index=feature.index, dtype="Int64")
        result[s.isna()] = pd.NA
        return result

    binned = pd.cut(
        s,
        bins=edges,
        labels=False,
        include_lowest=True,
        right=True,
        duplicates="drop",  # Safety net, though we've already handled this
    )

    # Convert to nullable integer
    return binned.astype("Int64")


@udf(int, drop=["feature"], mode="pandas")
def quantile_binner(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Discretize numeric values using quantile-based boundaries from training statistics.

    Default quantiles are quartiles (0%, 25%, 50%, 75%, 100%).
    Creates up to 4 bins based on Q1, Q2 (median), and Q3.
    NaN inputs remain NaN.

    Parameters:
        feature: Numeric feature to discretize.
        statistics: Training statistics providing quartile percentiles; populated automatically.

    Returns:
        Series of nullable integer bin indices from 0 to 3.
    """
    numerical_feature = feature.astype("float64")

    # Use quartiles: 25th, 50th, 75th percentiles
    p = statistics.feature.percentiles
    if not p or len(p) < 75:
        result = pd.Series(0, index=feature.index, dtype="Int64")
        result[numerical_feature.isna()] = pd.NA
        return result

    q25 = p[24]  # Q1
    q50 = p[49]  # Q2 (median)
    q75 = p[74]  # Q3

    # Check if we have valid quartiles
    if any(pd.isna([q25, q50, q75])):
        result = pd.Series(0, index=feature.index, dtype="Int64")
        result[numerical_feature.isna()] = pd.NA
        return result

    # Create bin edges with infinity boundaries
    edges = np.array([-np.inf, q25, q50, q75, np.inf])

    # Remove duplicate edges (handles constant features)
    edges = np.unique(edges)

    if len(edges) < 2:
        # Degenerate case: assign all to bin 0
        result = pd.Series(0, index=feature.index, dtype="Int64")
        result[numerical_feature.isna()] = pd.NA
        return result

    binned = pd.cut(
        numerical_feature,
        bins=edges,
        labels=False,
        include_lowest=True,
        right=True,
        duplicates="drop",
    )

    # Convert to nullable integer
    return binned.astype("Int64")


@udf(float, drop=["feature"], mode="pandas")
def quantile_transformer(
    feature: pd.Series, statistics=feature_statistics
) -> pd.Series:
    """Transform features using quantile information to map to a uniform [0, 1] distribution.

    Maps the input feature to a uniform distribution using percentiles computed during training.
    Values are mapped to their quantile position in the training distribution.
    Useful for normalizing non-Gaussian distributions.
    Maps outliers to the edges of the [0, 1] interval.
    Output range is [0, 1] where 0 = minimum, 0.5 = median, 1 = maximum.

    Parameters:
        feature: Numeric feature to transform.
        statistics: Training statistics providing percentile values; populated automatically.

    Returns:
        Series of float64 values in [0, 1] representing the quantile position of each input.
    """
    numerical_feature = feature.astype("float64")
    percentiles = statistics.feature.percentiles

    if not percentiles or len(percentiles) < 2:
        return numerical_feature

    # Deduplicate percentiles to ensure strictly increasing xp for np.interp.
    # Constant/low-cardinality features can produce duplicate percentile values.
    pct_array = np.asarray(percentiles, dtype="float64")
    quantile_pos = np.linspace(0, 1, len(pct_array))
    unique_mask = np.concatenate(([True], np.diff(pct_array) > 0))
    pct_unique = pct_array[unique_mask]
    pos_unique = quantile_pos[unique_mask]

    if len(pct_unique) < 2:
        # All percentiles identical (constant feature) — map everything to 0.5
        result = np.where(numerical_feature.isna(), np.nan, 0.5)
        return pd.Series(result, index=feature.index)

    # Handle NaN values
    result = np.full(len(numerical_feature), np.nan)
    valid_mask = ~numerical_feature.isna()

    if valid_mask.any():
        valid_values = numerical_feature[valid_mask].values
        quantile_positions = np.interp(valid_values, pct_unique, pos_unique)
        result[valid_mask] = quantile_positions

    return pd.Series(result, index=feature.index)


@udf(float, drop=["feature"], mode="pandas")
def rank_normalizer(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Replace each value with its percentile rank in the training distribution.

    Assigns each value a rank between 0 and 1 based on its position in the sorted training data distribution.
    The rank represents the percentage of training values that are less than or equal to the given value.
    Robust to outliers (outliers get ranks near 0 or 1) and preserves the relative ordering of values.
    Output range is [0, 1] where 0 = at or below minimum, 1 = at or above maximum.

    Parameters:
        feature: Numeric feature to rank.
        statistics: Training statistics providing percentile values; populated automatically.

    Returns:
        Series of float64 values in [0, 1] representing the percentile rank of each input.
    """
    numerical_feature = feature.astype("float64")
    percentiles = statistics.feature.percentiles

    if not percentiles or len(percentiles) < 2:
        return numerical_feature

    # Handle NaN values
    result = np.full(len(numerical_feature), np.nan)
    valid_mask = ~numerical_feature.isna()

    if valid_mask.any():
        valid_values = numerical_feature[valid_mask].values
        # For each value, find what percentile it corresponds to
        # Using searchsorted to find the position in the sorted percentiles
        ranks = np.searchsorted(percentiles, valid_values, side="right") / len(
            percentiles
        )
        # Clip to [0, 1] range
        ranks = np.clip(ranks, 0.0, 1.0)
        result[valid_mask] = ranks

    return pd.Series(result, index=feature.index)


@udf(float, drop=["feature"], mode="pandas")
def winsorize(
    feature: pd.Series, statistics=feature_statistics, context: dict | None = None
) -> pd.Series:
    """Winsorization (clipping) to limit extreme values and reduce outlier influence.

    Outliers are replaced with percentile boundary values instead of removing rows.
    Defaults to [1st, 99th] percentiles unless overridden via `context` with `{"p_low": 5, "p_high": 95}`.

    Parameters:
        feature: Numeric feature to clip.
        statistics: Training statistics providing percentile values; populated automatically.
        context: Optional configuration dict; supports `"p_low"` and `"p_high"` as percentile indices (defaults `1` and `99`).

    Returns:
        Series of float64 values with extremes clipped to the specified percentile boundaries.
    """
    numerical_feature = feature.astype("float64")
    percentiles = statistics.feature.percentiles

    if not percentiles or len(percentiles) < 2:
        return numerical_feature

    # Defaults: 1 and 99 percentiles
    ctx = context or {}
    li = 1
    with contextlib.suppress(ValueError, TypeError):
        li = int(round(float(ctx.get("p_low", li))))
    ui = 99
    with contextlib.suppress(ValueError, TypeError):
        ui = int(round(float(ctx.get("p_high", ui))))

    # Bound indices
    max_idx = len(percentiles) - 1
    li = max(0, min(max_idx, li))
    ui = max(0, min(max_idx, ui))

    # Ensure li < ui
    if li >= ui:
        li, ui = 1, min(99, max_idx)

    lower = percentiles[li]
    upper = percentiles[ui]

    # Invalid bounds -> return unchanged
    if pd.isna(lower) or pd.isna(upper) or lower >= upper:
        return numerical_feature

    # Winsorize (no rows dropped), preserving NaN values
    clipped = numerical_feature.clip(lower=lower, upper=upper)

    return pd.Series(clipped, index=feature.index)


@udf(str, drop=["feature"], mode="pandas")
def top_k_categorical_binner(
    feature: pd.Series, statistics=feature_statistics, context: dict | None = None
) -> pd.Series:
    """Bin categorical features by grouping rare categories into an `"Other"` bucket.

    Groups low-frequency categories together based on training data frequencies.
    Keeps the top N most frequent categories and maps all others (including unseen categories) to a single label.
    Useful for high-cardinality categorical features to reduce dimensionality and prevent overfitting.
    Preserves NaN values as NaN.
    Unseen categories in production data are treated as rare and grouped.
    Configure via context: `"top_n"` sets the number of categories to keep (default `10`), `"other_label"` sets the bucket label (default `"Other"`).

    Parameters:
        feature: Categorical feature to bin.
        statistics: Training statistics providing category frequencies; populated automatically.
        context: Optional configuration dict; supports `"top_n"` (default `10`) and `"other_label"` (default `"Other"`).

    Returns:
        Series of strings where rare and unseen categories are replaced by `other_label`.

    Example: Keeping top 20 countries
        ```python
        tf = top_k_categorical_binner("country")
        tf.transformation_context = {"top_n": 20, "other_label": "Rare"}
        ```
    """
    ctx = context or {}
    # Get parameters from context
    top_n = ctx.get("top_n", 10)
    other_label = ctx.get("other_label", "Other")

    # Get histogram from training statistics (contains value counts)
    histogram = statistics.feature.histogram

    # Determine which categories to keep based on frequency
    if histogram is not None and isinstance(histogram, list):
        # Sort categories by count (descending) and keep top N
        sorted_categories = sorted(
            histogram, key=lambda x: x.get("count", 0), reverse=True
        )
        frequent_categories = {item["value"] for item in sorted_categories[:top_n]}
    else:
        # Fallback: if no histogram available, use unique_values (keeps all)
        unique_values = statistics.feature.unique_values
        if unique_values is not None:
            frequent_categories = set(list(unique_values)[:top_n])
        else:
            # No statistics available: return feature unchanged
            return feature

    # Map rare and unseen categories to other_label
    def map_category(value):
        if pd.isna(value):
            return pd.NA
        return value if value in frequent_categories else other_label

    return feature.map(map_category)


# region Imputation


@udf(float, drop=["feature"], mode="pandas")
def impute_mean(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Replace NaN values with the training mean for numeric features.

    If the training mean is itself NaN (no non-null training data), NaN values are left unchanged.

    Parameters:
        feature: Numeric feature with NaN values to fill.
        statistics: Training statistics providing the mean; populated automatically.

    Returns:
        Series of float64 values with NaN replaced by the training mean.
    """
    fill = statistics.feature.mean
    s = feature.astype("float64")
    return s if pd.isna(fill) else s.fillna(fill)


@udf(float, drop=["feature"], mode="pandas")
def impute_median(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Replace NaN values with the training median (50th percentile) for numeric features.

    If the training median is NaN (no non-null training data), NaN values are left unchanged.

    Parameters:
        feature: Numeric feature with NaN values to fill.
        statistics: Training statistics providing percentiles; populated automatically.

    Returns:
        Series of float64 values with NaN replaced by the training median.
    """
    percentiles = statistics.feature.percentiles
    median = percentiles[49] if percentiles and len(percentiles) > 49 else None
    s = feature.astype("float64")
    return s if median is None or pd.isna(median) else s.fillna(median)


@udf(float, drop=["feature"], mode="pandas")
def impute_constant(
    feature: pd.Series,
    context: dict | None = None,
) -> pd.Series:
    """Replace NaN values with a constant numeric fill value for numeric features.

    The fill value is taken from `context["value"]` (default: `0.0`).

    Parameters:
        feature: Numeric feature with NaN values to fill.
        context: Optional configuration dict; supports `"value"` to set the fill value (default `0.0`).

    Returns:
        Series of float64 values with NaN replaced by the configured constant.

    Example: Using a custom fill value
        ```python
        tf = impute_constant("age")
        tf.transformation_context = {"value": -1.0}
        ```
    """
    ctx = context or {}
    fill = 0.0
    with contextlib.suppress(ValueError, TypeError):
        fill = float(ctx.get("value", fill))
    return feature.astype("float64").fillna(fill)


@udf(str, drop=["feature"], mode="pandas")
def impute_mode(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """Replace NaN values with the most frequent category from the training histogram for categorical features.

    The mode is derived from the training-time histogram (the category with the highest count).
    Because the mode was seen during training, this chains safely into [`label_encoder`][hsfs.builtin_transformations.label_encoder] and [`one_hot_encoder`][hsfs.builtin_transformations.one_hot_encoder] without producing unseen-category fallback values.
    If no histogram is available (statistics not computed), NaN values are left unchanged.

    Parameters:
        feature: Categorical feature with NaN values to fill.
        statistics: Training statistics providing the histogram; populated automatically.

    Returns:
        Series of strings with NaN replaced by the most frequent training category.
    """
    histogram = statistics.feature.histogram
    if not histogram:
        return feature.astype(str).where(feature.notna(), other=pd.NA)
    sorted_hist = sorted(histogram, key=lambda x: x["count"], reverse=True)
    mode_value = str(sorted_hist[0]["value"])
    return feature.where(feature.notna(), other=mode_value)


@udf(str, drop=["feature"], mode="pandas")
def impute_category(
    feature: pd.Series,
    context: dict | None = None,
) -> pd.Series:
    """Replace NaN values with a sentinel category string for categorical features.

    The sentinel defaults to `"__MISSING__"` and can be overridden via `context["value"]`.

    Warning: Encoder chaining order matters
        Downstream encoders ([`label_encoder`][hsfs.builtin_transformations.label_encoder], [`one_hot_encoder`][hsfs.builtin_transformations.one_hot_encoder]) trained before imputation will treat this sentinel as an unseen category and encode it as -1 / all-False.
        To get a dedicated encoding for the missing category, compute encoder statistics on already-imputed training data.

    Parameters:
        feature: Categorical feature with NaN values to fill.
        context: Optional configuration dict; supports `"value"` to override the sentinel string (default `"__MISSING__"`).

    Returns:
        Series of strings with NaN replaced by the configured sentinel value.

    Example: Using a custom sentinel
        ```python
        tf = impute_category("country")
        tf.transformation_context = {"value": "Unknown"}
        ```
    """
    ctx = context or {}
    sentinel = str(ctx.get("value", constants.TRANSFORMATIONS.MISSING))
    return feature.where(feature.notna(), other=sentinel)

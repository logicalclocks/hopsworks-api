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

import math

import numpy as np
import pandas as pd
from hsfs.hopsworks_udf import udf
from hsfs.transformation_statistics import TransformationStatistics


feature_statistics = TransformationStatistics("feature")


@udf(float, drop=["feature"])
def min_max_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    return (feature - statistics.feature.min) / (
        statistics.feature.max - statistics.feature.min
    )


@udf(float, drop=["feature"])
def standard_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    return (feature - statistics.feature.mean) / statistics.feature.stddev


@udf(float, drop=["feature"], mode="pandas")
def robust_scaler(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """
    Robust scaling using median and IQR.

    Scales a feature by removing the median and dividing by the interquartile
    range (IQR = Q3 - Q1). This makes the transformation robust to outliers.

    If IQR is zero (constant feature), the function centers the data by the
    median without scaling to avoid division by zero.
    """
    q1 = statistics.feature.percentiles[25]
    q2 = statistics.feature.percentiles[50]
    q3 = statistics.feature.percentiles[75]
    iqr = q3 - q1

    scaled_feature = feature.astype("float64")
    if pd.isna(iqr) or iqr == 0:
        # Constant feature or invalid IQR: center only
        return scaled_feature - q2

    return (scaled_feature - q2) / iqr


@udf(int, drop=["feature"], mode="pandas")
def label_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    unique_data = sorted([value for value in statistics.feature.unique_values])
    value_to_index = {value: index for index, value in enumerate(unique_data)}
    # Unknown categories not present in training dataset are encoded as -1.
    return pd.Series(
        [
            value_to_index.get(data, -1) if not pd.isna(data) else math.nan
            for data in feature
        ]
    )


@udf(bool, drop=["feature"], mode="pandas")
def one_hot_encoder(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    unique_data = [value for value in statistics.feature.unique_values]

    # One hot encode features. Re-indexing to set missing categories to False and drop categories not in training data statistics.
    # Hence one-hot encoded features would have all categories as False when a category not in training dataset is encountered.
    one_hot = pd.get_dummies(feature, dtype="bool").reindex(
        unique_data, axis=1, fill_value=False
    )

    # Sorting by columns so as to maintain consistency in column order.
    return one_hot.reindex(sorted(one_hot.columns), axis=1)


@udf(float, drop=["feature"], mode="pandas")
def log_transform(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """
    Apply natural logarithm to a numeric feature.

    Notes:
    - Only strictly positive values are transformed: y = ln(x) if x > 0, otherwise NaN.
    - This is useful to reduce skewness or model exponential relationships.
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
    """
    Discretize numeric values into equal-width bins using training min/max.

    - Default bins: 10 (configurable via context["n_bins"])
    - Values below min are placed in the first bin; values above max in the last bin.
    - NaN inputs remain NaN.

    Example to use 20 bins:
        tf = equal_width_binner("feature")
        tf.hopsworks_udf.transformation_context = {"n_bins": 20}
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
    bins = 10
    if isinstance(context, dict):
        try:
            n_bins = context.get("n_bins")
            if n_bins is not None:
                bins = max(2, int(n_bins))  # Ensure at least 2 bins
        except (ValueError, TypeError):
            pass  # Use default if conversion fails

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
    """
    Discretize numeric values into equal-frequency bins using training quartiles.

    - Uses Q1/Q2/Q3 percentiles as boundaries to form up to 4 bins.
    - If quartiles have duplicates (constant regions), fewer bins are created.
    - NaN inputs remain NaN.
    """
    s = feature.astype("float64")
    q1 = statistics.feature.percentiles[25]
    q2 = statistics.feature.percentiles[50]
    q3 = statistics.feature.percentiles[75]

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


@udf(int, drop=["feature"])
def quantile_binner(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """
    Discretize numeric values using quantile-based boundaries from training statistics.

    - Default quantiles: quartiles (0%, 25%, 50%, 75%, 100%).
    - Creates up to 4 bins based on Q1, Q2 (median), and Q3.
    - NaN inputs remain NaN.
    """
    numerical_feature = feature.astype("float64")

    # Use quartiles: 25th, 50th, 75th percentiles
    p = statistics.feature.percentiles
    q25 = p[25]  # Q1
    q50 = p[50]  # Q2 (median)
    q75 = p[75]  # Q3

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
    """
    Transform features using quantile information to map to a uniform [0, 1] distribution.

    This transformation maps the input feature to a uniform distribution by using
    the percentiles computed during training. Values are mapped to their quantile
    position in the training distribution.

    Notes:
    - Useful for normalizing non-Gaussian distributions
    - Maps outliers to the edges of the [0, 1] interval
    - Requires percentiles computed from training data statistics
    - Output range: [0, 1] where 0 = minimum, 0.5 = median, 1 = maximum
    """
    numerical_feature = feature.astype("float64")
    percentiles = statistics.feature.percentiles

    # Handle NaN values
    result = np.full(len(numerical_feature), np.nan)
    valid_mask = ~numerical_feature.isna()

    if valid_mask.any():
        valid_values = numerical_feature[valid_mask].values
        # Map each value to its quantile position using linear interpolation
        # percentiles[i] = i-th percentile (0th through 100th percentile = 101 values)
        quantile_positions = np.interp(
            valid_values, percentiles, np.linspace(0, 1, len(percentiles))
        )
        result[valid_mask] = quantile_positions

    return pd.Series(result, index=feature.index)


@udf(float, drop=["feature"], mode="pandas")
def rank_normalizer(feature: pd.Series, statistics=feature_statistics) -> pd.Series:
    """
    Replace each value with its percentile rank in the training distribution.

    This transformation assigns each value a rank between 0 and 1 based on its
    position in the sorted training data distribution. The rank represents the
    percentage of training values that are less than or equal to the given value.

    Notes:
    - Useful for rank-based normalization of features
    - Robust to outliers (outliers get ranks near 0 or 1)
    - Preserves the relative ordering of values
    - Requires percentiles computed from training data statistics
    - Output range: [0, 1] where 0 = at or below minimum, 1 = at or above maximum
    """
    numerical_feature = feature.astype("float64")  # Fixed: singular
    percentiles = statistics.feature.percentiles

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
    """
    Winsorization (clipping) to limit extreme values and reduce outlier influence.

    By default, clips values to the [1st, 99th] percentiles computed on the
    training data. You can override thresholds by passing a context with
    keys `p_low` and `p_high` (percentile values in [0, 100]).

    Example to clip at [5th, 95th]:
        tf = winsorize("feature")
        tf.hopsworks_udf.transformation_context = {"p_low": 5, "p_high": 95}
    """
    numerical_feature = feature.astype("float64")
    percentiles = statistics.feature.percentiles

    # Defaults: 1st and 99th percentiles
    p_low = 1
    p_high = 99
    if isinstance(context, dict):
        p_low = context.get("p_low", p_low)
        p_high = context.get("p_high", p_high)

    # Convert percentile values to array indices
    # Since percentiles[i] = i-th percentile, we use the value directly as index
    try:
        li = int(round(float(p_low)))
        ui = int(round(float(p_high)))
    except Exception:
        li, ui = 1, 99  # Default fallback

    # Ensure indices are within valid range [0, len(percentiles)-1]
    max_idx = len(percentiles) - 1
    li = max(0, min(max_idx, li))
    ui = max(0, min(max_idx, ui))

    # Ensure lower index < upper index
    if li >= ui:
        li, ui = 1, min(99, max_idx)

    lower = percentiles[li]
    upper = percentiles[ui]

    # Ensure proper ordering and finiteness
    if pd.isna(lower) or pd.isna(upper) or lower > upper:
        return numerical_feature  # no-op if invalid thresholds

    clipped = numerical_feature.clip(lower=lower, upper=upper)
    return pd.Series(clipped, index=feature.index)


@udf(float, drop=["feature"], mode="pandas")
def target_mean_encoder(
    feature: pd.Series,
    label: pd.Series,
    statistics=feature_statistics,
    context: dict | None = None,
) -> pd.Series:
    """
    Target / Mean Encoding for categorical features.

    Replaces each category in `feature` with the mean of the target variable `label`.

    Usage notes:
    - During training (offline): provide both `feature` and `label`; the encoder computes
      the per-category mean on-the-fly from these two Series.
    - During serving/online or when labels are unavailable: provide a precomputed mapping via
      the transformation context as `{"target_means": {category: mean, ...}, "global_mean": <float>}`.
      Unseen categories fall back to `global_mean` when provided, otherwise NaN.
    - Only the input `feature` column is dropped. The `label` column is preserved in outputs.

    Edge cases:
    - If `label` is entirely null or not provided (e.g., serving), a context mapping is required.
    - If `feature` contains NaN, the encoded value will be NaN for those rows.
    """

    # Ensure pandas Series with appropriate dtype
    f = feature
    y = label if label is not None else None

    mapping: dict | None = None
    global_mean: float | None = None

    if isinstance(context, dict):
        mapping = context.get("target_means") or context.get("mapping")
        global_mean = context.get("global_mean")

    # Training/offline path: compute mapping from data if label provided and non-empty
    if y is not None and not (isinstance(y, pd.Series) and y.isna().all()):
        # Attempt numeric conversion for label; errors='coerce' will turn non-numeric into NaN
        y_num = pd.to_numeric(y, errors="coerce")
        # Compute category -> mean(label)
        df = pd.DataFrame({"__cat__": f, "__y__": y_num})
        means = df.groupby("__cat__")["__y__"].mean()
        mapping = means.to_dict()
        # Global mean for fallback on unseen categories at serve-time
        global_mean = float(y_num.mean()) if not pd.isna(y_num.mean()) else None

    if mapping is None:
        # No mapping available: try to use just global mean for all known categories
        if global_mean is not None:
            return pd.Series(
                [global_mean if pd.notna(v) else np.nan for v in f], index=f.index
            )
        # As a last resort, return NaNs (cannot encode)
        return pd.Series([np.nan for _ in f], index=f.index)

    # Map categories to target means; unseen -> global_mean (if provided) else NaN
    def _map_val(v):
        if pd.isna(v):
            return np.nan
        return mapping.get(v, global_mean)

    encoded = f.map(_map_val)
    return pd.Series(encoded, index=f.index)

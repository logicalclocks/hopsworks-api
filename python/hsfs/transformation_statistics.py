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

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import humps


if TYPE_CHECKING:
    from collections.abc import Mapping


class FeatureTransformationStatistics:
    """Statistics for a single feature, computed from training data for use in transformation functions.

    This class provides access to descriptive statistics that can be used in model-dependent
    transformation functions. Statistics are computed when a training dataset is created from
    a feature view and are automatically injected into transformation functions that declare
    a `statistics` parameter.

    All properties return `None` until the statistics are populated by creating a training dataset.

    Example: Accessing statistics in a transformation function
        ```python
        from hopsworks import udf
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("salary")

        @udf(return_type=float)
        def normalize_salary(salary, statistics=stats):
            # Access mean and stddev for this feature
            return (salary - statistics.salary.mean) / statistics.salary.stddev
        ```

    Available Statistics:
        **Basic counts:**
        `count`, `num_non_null_values`, `num_null_values`, `completeness`

        **Numerical statistics:**
        `min`, `max`, `sum`, `mean`, `stddev`, `percentiles`

        **Uniqueness metrics:**
        `approx_num_distinct_values`, `exact_num_distinct_values`, `distinctness`, `uniqueness`, `entropy`

        **Extended statistics:**
        `correlations`, `histogram`, `kll`, `unique_values`

    See Also:
        - [`TransformationStatistics`][hsfs.transformation_statistics.TransformationStatistics]: Container for multiple feature statistics.
        - [`udf`][hsfs.hopsworks_udf.udf]: Decorator to create transformation functions with statistics access.
    """

    extended_statistics: dict | str | None = None

    def __init__(
        self,
        feature_name: str,
        count: int | None = None,
        completeness: float | None = None,
        num_non_null_values: int | None = None,
        num_null_values: int | None = None,
        approx_num_distinct_values: int | None = None,
        min: float | None = None,
        max: float | None = None,
        sum: float | None = None,
        mean: float | None = None,
        stddev: float | None = None,
        percentiles: Mapping[str, float] | None = None,
        distinctness: float | None = None,
        entropy: float | None = None,
        uniqueness: float | None = None,
        exact_num_distinct_values: int | None = None,
        extended_statistics: dict | str | None = None,
        **kwargs,
    ):
        self._feature_name = feature_name
        self._count = count
        self._completeness = completeness
        self._num_non_null_values = num_non_null_values
        self._num_null_values = num_null_values
        self._approx_num_distinct_values = approx_num_distinct_values
        self._min = min
        self._max = max
        self._sum = sum
        self._mean = mean
        self._stddev = stddev
        self._percentiles = percentiles
        self._distinctness = distinctness
        self._entropy = entropy
        self._uniqueness = uniqueness
        self._exact_num_distinct_values = exact_num_distinct_values
        extended_statistics = (
            extended_statistics
            if not isinstance(extended_statistics, str)
            else json.loads(extended_statistics)
        )
        if extended_statistics:
            self._correlations = extended_statistics.get("correlations", None)
            self._histogram = extended_statistics.get("histogram", None)
            self._kll = extended_statistics.get("kll", None)
            self._unique_values = extended_statistics.get("unique_values", None)

    @property
    def feature_name(self) -> str:
        """Name of the feature these statistics describe."""
        return self._feature_name

    @property
    def count(self) -> int | None:
        """Total number of rows in the training dataset for this feature."""
        return self._count

    # for any feature type

    @property
    def completeness(self) -> float | None:
        """Fraction of non-null values (0.0 to 1.0).

        Calculated as `num_non_null_values / count`.
        A completeness of 1.0 means no null values.
        """
        return self._completeness

    @property
    def num_non_null_values(self) -> int | None:
        """Count of rows with non-null values for this feature."""
        return self._num_non_null_values

    @property
    def num_null_values(self) -> int | None:
        """Count of rows with null/missing values for this feature."""
        return self._num_null_values

    @property
    def approx_num_distinct_values(self) -> int | None:
        """Approximate count of unique values using HyperLogLog algorithm.

        Use this for large datasets where exact counting would be expensive.
        For exact counts, use `exact_num_distinct_values` instead.
        """
        return self._approx_num_distinct_values

    # for numerical features

    @property
    def min(self) -> float | None:
        """Minimum value in the training dataset.

        Used by [`min_max_scaler`][hsfs.builtin_transformations.min_max_scaler] for normalization.
        """
        return self._min

    @property
    def max(self) -> float | None:
        """Maximum value in the training dataset.

        Used by [`min_max_scaler`][hsfs.builtin_transformations.min_max_scaler] for normalization.
        """
        return self._max

    @property
    def sum(self) -> float | None:
        """Sum of all non-null values in the training dataset."""
        return self._sum

    @property
    def mean(self) -> float | None:
        """Arithmetic mean (average) of the feature values.

        Used by [`standard_scaler`][hsfs.builtin_transformations.standard_scaler] for standardization.
        """
        return self._mean

    @property
    def stddev(self) -> float | None:
        """Standard deviation of the feature values.

        Measures the spread of values around the mean.
        Used by [`standard_scaler`][hsfs.builtin_transformations.standard_scaler] for standardization.
        """
        return self._stddev

    @property
    def percentiles(self) -> Mapping[str, float] | None:
        """Percentile values as a dictionary mapping percentile index to value.

        Keys are integers 0-99 representing the percentile.
        Common percentiles:

        - `percentiles[24]`: 25th percentile (Q1)
        - `percentiles[49]`: 50th percentile (median)
        - `percentiles[74]`: 75th percentile (Q3)

        Used by [`robust_scaler`][hsfs.builtin_transformations.robust_scaler] which uses
        the median (50th) and IQR (75th - 25th) for outlier-robust scaling.
        """
        return self._percentiles

    # with exact uniqueness

    @property
    def distinctness(self) -> float | None:
        """Fraction of distinct values over total values (0.0 to 1.0).

        Distinct values are those that appear at least once.

        Example: Calculating distinctness
            For values `[a, a, b]`:

            - 2 distinct values: `a` and `b`
            - 3 total values
            - distinctness = 2/3 ≈ 0.67
        """
        return self._distinctness

    @property
    def entropy(self) -> float | None:
        """Shannon entropy measuring the randomness/unpredictability of values.

        Higher entropy indicates more uniform distribution of values.
        Lower entropy indicates some values dominate.
        Calculated as: `-sum(p * log(p))` where `p` is the probability of each value.

        Example: Calculating entropy
            For values `[a, b, b, c, c]` with counts `[1, 2, 2]`:

            - Probabilities: `[1/5, 2/5, 2/5]`
            - Entropy = `-(1/5*log(1/5) + 2/5*log(2/5) + 2/5*log(2/5))` ≈ 1.055
        """
        return self._entropy

    @property
    def uniqueness(self) -> float | None:
        """Fraction of values that appear exactly once (0.0 to 1.0).

        Unique values are those that occur exactly one time in the dataset.

        Example: Calculating uniqueness
            For values `[a, a, b]`:

            - 1 unique value: `b` (appears exactly once)
            - 3 total values
            - uniqueness = 1/3 ≈ 0.33
        """
        return self._uniqueness

    @property
    def exact_num_distinct_values(self) -> int | None:
        """Exact count of unique values in the training dataset.

        More accurate than `approx_num_distinct_values` but more expensive to compute.
        """
        return self._exact_num_distinct_values

    @property
    def correlations(self) -> dict | None:
        """Pearson correlation coefficients with other numerical features.

        Returns a dictionary mapping feature names to correlation values (-1.0 to 1.0).
        Only available when extended statistics are computed.
        """
        return self._correlations

    @property
    def histogram(self) -> dict | None:
        """Histogram data showing the distribution of values.

        Contains bin edges and counts for visualizing the feature distribution.
        Only available when extended statistics are computed.
        """
        return self._histogram

    @property
    def kll(self) -> dict | None:
        """KLL (Karnin-Lang-Liberty) sketch for approximate quantile estimation.

        A probabilistic data structure for efficient percentile computation on large datasets.
        Only available when extended statistics are computed.
        """
        return self._kll

    @property
    def unique_values(self) -> dict | None:
        """Set of unique categorical values found in the training dataset.

        Used by [`label_encoder`][hsfs.builtin_transformations.label_encoder] and
        [`one_hot_encoder`][hsfs.builtin_transformations.one_hot_encoder] to map
        categories to encoded values.

        Returns a collection of the distinct values. Only available when extended statistics are computed.
        """
        return self._unique_values

    @classmethod
    def from_response_json(
        cls: FeatureTransformationStatistics, json_dict: dict[str, Any]
    ) -> FeatureTransformationStatistics:
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)


class TransformationStatistics:
    """Container for training dataset statistics used in model-dependent transformation functions.

    This class declares which features require statistics in a transformation function.
    When you create a training dataset from a feature view using such transformations,
    Hopsworks automatically computes the statistics and injects them into the transformation.

    Statistics are initially empty (all properties return `None`) and are populated when:

    1. A training dataset is created via `feature_view.create_training_data()`
    2. The feature view is initialized for serving via `feature_view.init_serving(training_dataset_version)`

    Example: Declaring statistics in a transformation function
        ```python
        from hopsworks import udf
        from hsfs.transformation_statistics import TransformationStatistics

        # Declare which features need statistics
        stats = TransformationStatistics("feature1", "feature2")

        @udf(return_type=float)
        def my_transform(feature1, feature2, statistics=stats):
            # Access statistics for each declared feature
            mean1 = statistics.feature1.mean
            mean2 = statistics.feature2.mean
            return (feature1 - mean1) + (feature2 - mean2)
        ```

    Example: Accessing individual feature statistics
        ```python
        stats = TransformationStatistics("age", "salary")

        # After training dataset creation, access statistics like:
        # stats.age.mean, stats.age.stddev, stats.age.min, stats.age.max
        # stats.salary.mean, stats.salary.stddev, stats.salary.min, stats.salary.max
        ```

    Note: Automatic Population
        You don't need to manually populate statistics.
        Hopsworks handles this automatically when you create training data or initialize serving.

    Parameters:
        *features: Names of the features that require statistics in the transformation function.
            Each feature name becomes an attribute on this object, providing access to its
            [`FeatureTransformationStatistics`][hsfs.transformation_statistics.FeatureTransformationStatistics].

    See Also:
        - [`FeatureTransformationStatistics`][hsfs.transformation_statistics.FeatureTransformationStatistics]: Statistics for a single feature.
        - [`udf`][hsfs.hopsworks_udf.udf]: Decorator to create transformation functions.
        - [`TransformationType.MODEL_DEPENDENT`][hsfs.transformation_function.TransformationType]: Transformations that use statistics.
    """

    def __init__(self, *features: str):
        self._features = features
        self.__dict__.update(
            {feature: self.init_statistics(feature) for feature in features}
        )

    def init_statistics(self, feature_name: str) -> FeatureTransformationStatistics:
        return FeatureTransformationStatistics(feature_name=feature_name)

    def set_statistics(
        self,
        feature_name: str,
        statistics: dict[str, Any] | FeatureTransformationStatistics,
    ) -> None:
        if isinstance(statistics, FeatureTransformationStatistics):
            self.__dict__[feature_name] = statistics
        else:
            self.__dict__[feature_name] = (
                FeatureTransformationStatistics.from_response_json(statistics)
            )

    def __repr__(self) -> str:
        return ",\n ".join([repr(self.__dict__[feature]) for feature in self._features])

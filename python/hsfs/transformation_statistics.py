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

from hopsworks_apigen import public
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import humps


if TYPE_CHECKING:
    from collections.abc import Mapping


@public
@dataclass
class FeatureTransformationStatistics:
    """Data class that contains all the statistics parameters that can be used for transformations inside a custom transformation function."""

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

    @public
    @property
    def feature_name(self) -> str:
        """Name of the feature."""
        return self._feature_name

    @public
    @property
    def count(self) -> int | None:
        """Number of values."""
        return self._count

    # for any feature type

    @public
    @property
    def completeness(self) -> float | None:
        """Fraction of non-null values in a column."""
        return self._completeness

    @public
    @property
    def num_non_null_values(self) -> int | None:
        """Number of non-null values."""
        return self._num_non_null_values

    @public
    @property
    def num_null_values(self) -> int | None:
        """Number of null values."""
        return self._num_null_values

    @public
    @property
    def approx_num_distinct_values(self) -> int | None:
        """Approximate number of distinct values."""
        return self._approx_num_distinct_values

    # for numerical features

    @public
    @property
    def min(self) -> float | None:
        """Minimum value."""
        return self._min

    @public
    @property
    def max(self) -> float | None:
        """Maximum value."""
        return self._max

    @public
    @property
    def sum(self) -> float | None:
        """Sum of all feature values."""
        return self._sum

    @public
    @property
    def mean(self) -> float | None:
        """Mean value."""
        return self._mean

    @public
    @property
    def stddev(self) -> float | None:
        """Standard deviation of the feature values."""
        return self._stddev

    @public
    @property
    def percentiles(self) -> Mapping[str, float] | None:
        """Percentiles."""
        return self._percentiles

    # with exact uniqueness

    @public
    @property
    def distinctness(self) -> float | None:
        """Fraction of distinct values of a feature over the number of all its values. Distinct values occur at least once.

        Note: Example
            $[a, a, b]$ contains two distinct values $a$ and $b$, so distinctness is $2/3$.
        """
        return self._distinctness

    @public
    @property
    def entropy(self) -> float | None:
        """Entropy is a measure of the level of information contained in an event (feature value) when considering all possible events (all feature values).

        Entropy is estimated using observed value counts as the negative sum of (value_count/total_count) * log(value_count/total_count).

        Note: Example
            $[a, b, b, c, c]$ has three distinct values with counts $[1, 2, 2]$.

            Entropy is then $(-1/5*log(1/5)-2/5*log(2/5)-2/5*log(2/5)) = 1.055$.
        """
        return self._entropy

    @public
    @property
    def uniqueness(self) -> float | None:
        """Fraction of unique values over the number of all values of a column. Unique values occur exactly once.

        Note: Example
            $[a, a, b]$ contains one unique value $b$, so uniqueness is $1/3$.
        """
        return self._uniqueness

    @public
    @property
    def exact_num_distinct_values(self) -> int | None:
        """Exact number of distinct values."""
        return self._exact_num_distinct_values

    @public
    @property
    def correlations(self) -> dict | None:
        """Correlations of feature values."""
        return self._correlations

    @public
    @property
    def histogram(self) -> dict | None:
        """Histogram of feature values."""
        return self._histogram

    @public
    @property
    def kll(self) -> dict | None:
        """KLL of feature values."""
        return self._kll

    @public
    @property
    def unique_values(self) -> dict | None:
        """Number of Unique Values."""
        return self._unique_values

    @classmethod
    def from_response_json(
        cls: FeatureTransformationStatistics, json_dict: dict[str, Any]
    ) -> FeatureTransformationStatistics:
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)


@public
class TransformationStatistics:
    """Class that stores feature transformation statistics of all features that require training dataset statistics in a transformation function.

    All statistics for a feature is initially initialized with null values and will be populated with values when training dataset is created for the soe.

    Parameters:
        *features: `str`.
            The features for which training dataset statistics need to be computed.

    Example:
        ```python
        # Defining transformation statistics
        transformation_statistics = TransformationStatistics("feature1", "feature2")

        # Accessing feature transformation statistics for a specific feature
        feature_transformation_statistics_feature1 = transformation_statistics.feature1
        ```
    """

    def __init__(self, *features: str):
        self._features = features
        self.__dict__.update(
            {feature: self.init_statistics(feature) for feature in features}
        )

    def init_statistics(self, feature_name: str) -> FeatureTransformationStatistics:
        return FeatureTransformationStatistics(feature_name=feature_name)

    def set_statistics(self, feature_name: str, statistics: dict[str, Any]) -> None:
        self.__dict__[feature_name] = (
            FeatureTransformationStatistics.from_response_json(statistics)
        )

    def __repr__(self) -> str:
        return ",\n ".join([repr(self.__dict__[feature]) for feature in self._features])

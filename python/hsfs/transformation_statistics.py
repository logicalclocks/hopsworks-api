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
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Union

import humps


@dataclass
class FeatureTransformationStatistics:
    """
    Data class that contains all the statistics parameters that can be used for transformations inside a custom transformation function.
    """

    feature_name: str
    count: int = None
    # for any feature type
    completeness: Optional[float] = None
    num_non_null_values: Optional[int] = None
    num_null_values: Optional[int] = None
    approx_num_distinct_values: Optional[int] = None
    # for numerical features
    min: Optional[float] = None
    max: Optional[float] = None
    sum: Optional[float] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    percentiles: Optional[Mapping[str, float]] = None
    # with exact uniqueness
    distinctness: Optional[float] = None
    entropy: Optional[float] = None
    uniqueness: Optional[float] = None
    exact_num_distinct_values: Optional[int] = None
    extended_statistics: Optional[Union[dict, str]] = None

    def __init__(
        self,
        feature_name: str,
        count: int = None,
        completeness: Optional[float] = None,
        num_non_null_values: Optional[int] = None,
        num_null_values: Optional[int] = None,
        approx_num_distinct_values: Optional[int] = None,
        min: Optional[float] = None,
        max: Optional[float] = None,
        sum: Optional[float] = None,
        mean: Optional[float] = None,
        stddev: Optional[float] = None,
        percentiles: Optional[Mapping[str, float]] = None,
        distinctness: Optional[float] = None,
        entropy: Optional[float] = None,
        uniqueness: Optional[float] = None,
        exact_num_distinct_values: Optional[int] = None,
        extended_statistics: Optional[Union[dict, str]] = None,
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
        """Name of the feature."""
        return self._feature_name

    @property
    def count(self) -> int:
        """Number of values."""
        return self._count

    @property
    def completeness(self) -> Optional[float]:
        """Fraction of non-null values in a column."""
        return self._completeness

    @property
    def num_non_null_values(self) -> Optional[int]:
        """Number of non-null values."""
        return self._num_non_null_values

    @property
    def num_null_values(self) -> Optional[int]:
        """Number of null values."""
        return self._num_null_values

    @property
    def approx_num_distinct_values(self) -> Optional[int]:
        """Approximate number of distinct values."""
        return self._approx_num_distinct_values

    @property
    def min(self) -> Optional[float]:
        """Minimum value."""
        return self._min

    @property
    def max(self) -> Optional[float]:
        """Maximum value."""
        return self._max

    @property
    def sum(self) -> Optional[float]:
        """Sum of all feature values."""
        return self._sum

    @property
    def mean(self) -> Optional[float]:
        """Mean value."""
        return self._mean

    @property
    def stddev(self) -> Optional[float]:
        """Standard deviation of the feature values."""
        return self._stddev

    @property
    def percentiles(self) -> Optional[Mapping[str, float]]:
        """Percentiles."""
        return self._percentiles

    @property
    def distinctness(self) -> Optional[float]:
        """Fraction of distinct values of a feature over the number of all its values. Distinct values occur at least once.

        !!! note "Example"
            $[a, a, b]$ contains two distinct values $a$ and $b$, so distinctness is $2/3$.
        """
        return self._distinctness

    @property
    def entropy(self) -> Optional[float]:
        """Entropy is a measure of the level of information contained in an event (feature value) when considering all possible events (all feature values).
        Entropy is estimated using observed value counts as the negative sum of (value_count/total_count) * log(value_count/total_count).

        !!! note "Example"
            $[a, b, b, c, c]$ has three distinct values with counts $[1, 2, 2]$.

            Entropy is then $(-1/5*log(1/5)-2/5*log(2/5)-2/5*log(2/5)) = 1.055$.
        """
        return self._entropy

    @property
    def uniqueness(self) -> Optional[float]:
        """Fraction of unique values over the number of all values of a column. Unique values occur exactly once.

        !!! note "Example"
            $[a, a, b]$ contains one unique value $b$, so uniqueness is $1/3$.
        """
        return self._uniqueness

    @property
    def exact_num_distinct_values(self) -> Optional[int]:
        """Exact number of distinct values."""
        return self._exact_num_distinct_values

    @property
    def correlations(self) -> Optional[dict]:
        """Correlations of feature values."""
        return self._correlations

    @property
    def histogram(self) -> Optional[dict]:
        """Histogram of feature values."""
        return self._histogram

    @property
    def kll(self) -> Optional[dict]:
        """KLL of feature values."""
        return self._kll

    @property
    def unique_values(self) -> Optional[dict]:
        """Number of Unique Values."""
        return self._unique_values

    @classmethod
    def from_response_json(
        cls: FeatureTransformationStatistics, json_dict: Dict[str, Any]
    ) -> FeatureTransformationStatistics:
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)


class TransformationStatistics:
    """
    Class that stores feature transformation statistics of all features that require training dataset statistics in a transformation function.

    All statistics for a feature is initially initialized with null values and will be populated with values when training dataset is created for the soe.

    # Arguments
        *features: `str`.
            The features for which training dataset statistics need to be computed.

    !!! example
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

    def set_statistics(self, feature_name: str, statistics: Dict[str, Any]) -> None:
        self.__dict__[feature_name] = (
            FeatureTransformationStatistics.from_response_json(statistics)
        )

    def __repr__(self) -> str:
        return ",\n ".join([repr(self.__dict__[feature]) for feature in self._features])

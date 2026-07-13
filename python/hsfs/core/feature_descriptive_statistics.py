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
import math
from typing import TYPE_CHECKING

import humps
from hopsworks_apigen import deprecated, public
from hsfs import util


if TYPE_CHECKING:
    from collections.abc import Mapping


def _finite_or_none(value):
    """Map non-finite numbers (NaN, Infinity) to None.

    Non-finite values are not valid JSON and not valid SQL numerics: a single
    one aborts the whole statistics registration on the backend.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


@public
class FeatureDescriptiveStatistics:
    # TODO: Add docstring
    _SINGLE_VALUE_STATISTICS = [
        "count",
        "completeness",
        "num_non_null_values",
        "num_null_values",
        "approx_num_distinct_values",
        "min",
        "max",
        "sum",
        "mean",
        "std_dev",
        "stddev",  # deprecated alias for std_dev, kept for backward compatibility
        "distinctness",
        "entropy",
        "uniqueness",
        "exact_num_distinct_values",
    ]

    def __init__(
        self,
        feature_name: str,
        feature_type: str = None,
        count: int = None,
        # for any feature type
        completeness: float | None = None,
        num_non_null_values: int | None = None,
        num_null_values: int | None = None,
        approx_num_distinct_values: int | None = None,
        # for numerical features
        min: float | None = None,
        max: float | None = None,
        sum: float | None = None,
        mean: float | None = None,
        std_dev: float | None = None,
        percentiles: Mapping[str, float] | None = None,
        # with exact uniqueness
        distinctness: float | None = None,
        entropy: float | None = None,
        uniqueness: float | None = None,
        exact_num_distinct_values: int | None = None,
        extended_statistics: dict | str | None = None,
        id: int | None = None,
        **kwargs,
    ):
        self._id = id
        self._feature_type = feature_type
        self._feature_name = util._autofix_feature_name(feature_name)
        self._count = count
        self._completeness = completeness
        self._num_non_null_values = num_non_null_values
        self._num_null_values = num_null_values
        self._approx_num_distinct_values = approx_num_distinct_values
        self._min = min
        self._max = max
        self._sum = sum
        self._mean = mean
        # Accept the legacy lowercase ``stddev`` key (e.g. from backend payloads
        # or older callers) so the rename to ``std_dev`` stays backward compatible.
        self._std_dev = std_dev if std_dev is not None else kwargs.get("stddev")
        self._percentiles = percentiles
        self._distinctness = distinctness
        self._entropy = entropy
        self._uniqueness = uniqueness
        self._exact_num_distinct_values = exact_num_distinct_values
        self._extended_statistics = (
            extended_statistics
            if not isinstance(extended_statistics, str)
            else json.loads(extended_statistics)
        )

    @public
    def get_value(self, name):
        stat_name = name.lower()
        if stat_name not in self._SINGLE_VALUE_STATISTICS:
            return None
        try:
            return getattr(self, stat_name)
        except KeyError as err:
            raise AttributeError(f"'{name}' statistic has not been computed") from err

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @classmethod
    def _from_deequ_json(cls, json_dict: dict) -> FeatureDescriptiveStatistics:
        stats_dict = {"feature_name": json_dict["column"]}

        if "dataType" in json_dict:
            stats_dict["feature_type"] = json_dict["dataType"]

        if "count" in json_dict and json_dict["count"] == 0:
            # if empty data, ignore the rest of statistics
            stats_dict["count"] = 0
            return cls(**stats_dict)

        # common for all data types
        if "numRecordsNull" in json_dict:
            stats_dict["num_null_values"] = json_dict["numRecordsNull"]
        if "numRecordsNonNull" in json_dict:
            stats_dict["num_non_null_values"] = json_dict["numRecordsNonNull"]
        if "numRecordsNull" in json_dict and "numRecordsNonNull" in json_dict:
            stats_dict["count"] = (
                json_dict["numRecordsNull"] + json_dict["numRecordsNonNull"]
            )
        if "count" in json_dict:
            stats_dict["count"] = json_dict["count"]
        if "completeness" in json_dict:
            stats_dict["completeness"] = json_dict["completeness"]
        if "approximateNumDistinctValues" in json_dict:
            stats_dict["approx_num_distinct_values"] = json_dict[
                "approximateNumDistinctValues"
            ]

        # commmon for all data types if exact_uniqueness is enabled
        if "uniqueness" in json_dict:
            stats_dict["uniqueness"] = json_dict["uniqueness"]
        if "entropy" in json_dict:
            stats_dict["entropy"] = json_dict["entropy"]
        if "distinctness" in json_dict:
            stats_dict["distinctness"] = json_dict["distinctness"]
        if "exactNumDistinctValues" in json_dict:
            stats_dict["exact_num_distinct_values"] = json_dict[
                "exactNumDistinctValues"
            ]

        # fractional / integral features
        if "minimum" in json_dict:
            stats_dict["min"] = json_dict["minimum"]
        if "maximum" in json_dict:
            stats_dict["max"] = json_dict["maximum"]
        if "sum" in json_dict:
            stats_dict["sum"] = json_dict["sum"]
        if "mean" in json_dict:
            stats_dict["mean"] = json_dict["mean"]
        if "stdDev" in json_dict:
            stats_dict["std_dev"] = json_dict["stdDev"]
        if "approxPercentiles" in json_dict:
            stats_dict["percentiles"] = json_dict["approxPercentiles"]

        extended_statistics = {}
        if "unique_values" in json_dict:
            extended_statistics["unique_values"] = json_dict["unique_values"]
        if "correlations" in json_dict:
            extended_statistics["correlations"] = json_dict["correlations"]
        if "histogram" in json_dict:
            extended_statistics["histogram"] = json_dict["histogram"]
        if "kll" in json_dict:
            extended_statistics["kll"] = json_dict["kll"]
        stats_dict["extended_statistics"] = (
            extended_statistics if extended_statistics else None
        )

        return cls(**stats_dict)

    def to_dict(self):
        percentiles = self._percentiles
        if isinstance(percentiles, dict):
            percentiles = {k: _finite_or_none(v) for k, v in percentiles.items()}
        elif isinstance(percentiles, list):
            percentiles = [_finite_or_none(v) for v in percentiles]
        _dict = {
            "id": self._id,
            "featureName": self._feature_name,
            "count": self._count,
            "featureType": self._feature_type,
            "min": _finite_or_none(self._min),
            "max": _finite_or_none(self._max),
            "sum": _finite_or_none(self._sum),
            "mean": _finite_or_none(self._mean),
            "stdDev": _finite_or_none(self._std_dev),
            "completeness": _finite_or_none(self._completeness),
            "numNonNullValues": self._num_non_null_values,
            "numNullValues": self._num_null_values,
            "distinctness": _finite_or_none(self._distinctness),
            "entropy": _finite_or_none(self._entropy),
            "uniqueness": _finite_or_none(self._uniqueness),
            "approxNumDistinctValues": self._approx_num_distinct_values,
            "exactNumDistinctValues": self._exact_num_distinct_values,
            "percentiles": percentiles,
        }
        if self._extended_statistics is not None:
            _dict["extendedStatistics"] = json.dumps(self._extended_statistics)
        return _dict

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self) -> str:
        return f"FeatureDescriptiveStatistics({self._feature_name!r})"

    @public
    @property
    def id(self) -> int | None:
        """ID of the feature descriptive statistics object."""
        return self._id

    @public
    @property
    def feature_type(self) -> str:
        """Data type of the feature. It can be one of Boolean, Fractional, Integral, or String."""
        return self._feature_type

    @public
    @property
    def feature_name(self) -> str:
        """Name of the feature."""
        return self._feature_name

    @public
    @property
    def count(self) -> int:
        """Number of values."""
        return self._count

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
    def std_dev(self) -> float | None:
        """Standard deviation of the feature values."""
        return self._std_dev

    @public
    @property
    @deprecated(
        "hsfs.core.feature_descriptive_statistics.FeatureDescriptiveStatistics.std_dev"
    )
    def stddev(self) -> float | None:
        """Standard deviation of the feature values.

        Deprecated alias for [`std_dev`][hsfs.core.feature_descriptive_statistics.FeatureDescriptiveStatistics.std_dev].
        """
        return self._std_dev

    @public
    @property
    def percentiles(self) -> Mapping[str, float] | None:
        """Percentiles."""
        return self._percentiles

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
    def extended_statistics(self) -> dict | None:
        """Additional statistics computed on the feature values such as histograms and correlations."""
        return self._extended_statistics

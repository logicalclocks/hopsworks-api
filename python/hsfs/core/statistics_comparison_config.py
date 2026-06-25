#
#   Copyright 2026 Hopsworks AB
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

import humps
from hopsworks_apigen import public
from hsfs import util


VALID_DISTRIBUTION_METRICS = {
    "PSI",
    "KL_DIVERGENCE",
    "JS_DIVERGENCE",
    "WASSERSTEIN",
    "HELLINGER",
    "KOLMOGOROV_SMIRNOV",
}

VALID_BINNING_STRATEGIES = {
    "EQUI_WIDTH",
    "EQUI_FREQUENCY",
    "CUSTOM_EDGES",
    "CATEGORICAL",
}


@public
class StatisticsComparisonConfig:
    """Defines a single metric comparison for feature monitoring.

    Each instance specifies the metric to evaluate, a threshold for the difference,
    and whether the comparison is relative, strict, or against a fixed reference value.

    Exactly one of `metric` (scalar) or `distribution_metric` (distribution) must be set.
    """

    def __init__(
        self,
        metric: str | None = None,
        threshold: float | None = None,
        relative: bool | None = False,
        strict: bool | None = False,
        specific_value: float | None = None,
        id: int | None = None,
        href: str | None = None,
        distribution_metric: str | None = None,
        binning_strategy: str | None = None,
        bin_count: int | None = None,
        smoothing_epsilon: float | None = None,
        custom_bin_edges: list[float] | None = None,
        **kwargs,
    ):
        # Enforce XOR: exactly one of metric or distribution_metric must be set.
        if metric is not None and distribution_metric is not None:
            raise ValueError(
                "Exactly one of 'metric' or 'distribution_metric' must be set, not both."
            )
        if metric is None and distribution_metric is None:
            raise ValueError(
                "Exactly one of 'metric' or 'distribution_metric' must be set."
            )

        if metric is not None:
            self._metric = metric.upper()
        else:
            self._metric = None

        if distribution_metric is not None:
            distribution_metric_upper = distribution_metric.upper()
            if distribution_metric_upper not in VALID_DISTRIBUTION_METRICS:
                raise ValueError(
                    f"Invalid distribution_metric '{distribution_metric}'. "
                    f"Allowed values: {sorted(VALID_DISTRIBUTION_METRICS)}."
                )
            self._distribution_metric = distribution_metric_upper
        else:
            self._distribution_metric = None

        if binning_strategy is not None:
            binning_strategy_upper = binning_strategy.upper()
            if binning_strategy_upper not in VALID_BINNING_STRATEGIES:
                raise ValueError(
                    f"Invalid binning_strategy '{binning_strategy}'. "
                    f"Allowed values: {sorted(VALID_BINNING_STRATEGIES)}."
                )
            self._binning_strategy = binning_strategy_upper
        else:
            self._binning_strategy = None

        if smoothing_epsilon is not None and smoothing_epsilon <= 0:
            raise ValueError(
                f"smoothing_epsilon must be a positive number, got {smoothing_epsilon}. "
                "A value of zero or below causes log(0) in PSI/KL/JS distance metrics."
            )

        self._threshold = threshold
        self._relative = relative
        self._strict = strict
        self._specific_value = specific_value
        self._id = id
        self._bin_count = bin_count
        self._smoothing_epsilon = smoothing_epsilon
        self._custom_bin_edges = custom_bin_edges

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**config) for config in json_decamelized["items"]]
        return cls(**json_decamelized)

    def to_dict(self):
        the_dict = {
            "id": self._id,
            "metric": self._metric,
            "threshold": self._threshold,
            "relative": self._relative,
            "strict": self._strict,
            "specificValue": self._specific_value,
        }
        if self._distribution_metric is not None:
            the_dict["distributionMetric"] = self._distribution_metric
        if self._binning_strategy is not None:
            the_dict["binningStrategy"] = self._binning_strategy
        if self._bin_count is not None:
            the_dict["binCount"] = self._bin_count
        if self._smoothing_epsilon is not None:
            the_dict["smoothingEpsilon"] = self._smoothing_epsilon
        if self._custom_bin_edges is not None:
            the_dict["customBinEdges"] = self._custom_bin_edges
        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        if self._distribution_metric is not None:
            return (
                f"StatisticsComparisonConfig(distribution_metric={self._distribution_metric!r},"
                f" binning_strategy={self._binning_strategy!r})"
            )
        return f"StatisticsComparisonConfig({self._metric!r})"

    def __eq__(self, other):
        if not isinstance(other, StatisticsComparisonConfig):
            return False

        return (
            self._metric,
            self._threshold,
            self._relative,
            self._strict,
            self._specific_value,
            self._distribution_metric,
            self._binning_strategy,
            self._bin_count,
            self._smoothing_epsilon,
            self._custom_bin_edges,
        ) == (
            other._metric,
            other._threshold,
            other._relative,
            other._strict,
            other._specific_value,
            other._distribution_metric,
            other._binning_strategy,
            other._bin_count,
            other._smoothing_epsilon,
            other._custom_bin_edges,
        )

    @public
    @property
    def id(self) -> int | None:
        """Identifier assigned by the backend upon registration."""
        return self._id

    @public
    @property
    def metric(self) -> str | None:
        """Scalar metric used for the statistics comparison."""
        return self._metric

    @public
    @property
    def threshold(self) -> float | None:
        """Threshold above which the difference triggers a shift alert."""
        return self._threshold

    @public
    @property
    def relative(self) -> bool | None:
        """Whether to compute the relative difference instead of the absolute one."""
        return self._relative

    @public
    @property
    def strict(self) -> bool | None:
        """Whether the threshold comparison is strict (>) or non-strict (>=)."""
        return self._strict

    @public
    @property
    def specific_value(self) -> float | None:
        """Fixed reference value to compare against instead of reference statistics."""
        return self._specific_value

    @public
    @property
    def distribution_metric(self) -> str | None:
        """Distribution distance metric for PDF-based comparison."""
        return self._distribution_metric

    @public
    @property
    def binning_strategy(self) -> str | None:
        """Strategy used to discretize continuous features into histogram bins."""
        return self._binning_strategy

    @public
    @property
    def bin_count(self) -> int | None:
        """Number of bins for the histogram."""
        return self._bin_count

    @public
    @property
    def smoothing_epsilon(self) -> float | None:
        """Small additive constant applied to bin probabilities to avoid log(0)."""
        return self._smoothing_epsilon

    @public
    @property
    def custom_bin_edges(self) -> list[float] | None:
        """Custom bin edges (required when binning_strategy is CUSTOM_EDGES)."""
        return self._custom_bin_edges

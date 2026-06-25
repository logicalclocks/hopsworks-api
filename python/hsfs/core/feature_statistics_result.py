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
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.statistics_comparison_result import StatisticsComparisonResult


@public
class FeatureStatisticsResult:
    """Holds per-feature statistics and comparison outcomes from a single monitoring run.

    Each instance contains the detection and optional reference descriptive statistics
    for one feature, together with the individual
    [`StatisticsComparisonResult`][hsfs.core.statistics_comparison_result.StatisticsComparisonResult]
    entries produced by evaluating the configured metrics.
    """

    def __init__(
        self,
        feature_name: str,
        shifted_metric_names: set[str] | list[str] | None = None,
        statistics_comparison_results: list[StatisticsComparisonResult]
        | list[dict]
        | None = None,
        detection_statistics_id: int | None = None,
        reference_statistics_id: int | None = None,
        detection_statistics: FeatureDescriptiveStatistics | dict | None = None,
        reference_statistics: FeatureDescriptiveStatistics | dict | None = None,
        id: int | None = None,
        href: str | None = None,
        **kwargs,
    ):
        self._id = id
        self._href = href
        self._feature_name = feature_name
        self._shifted_metric_names = self._parse_shifted_metric_names(
            shifted_metric_names
        )
        self._statistics_comparison_results = self._parse_statistics_comparison_result(
            statistics_comparison_results
        )
        self._detection_statistics_id = detection_statistics_id
        self._reference_statistics_id = reference_statistics_id
        self._detection_statistics = self._parse_descriptive_statistics(
            detection_statistics
        )
        self._reference_statistics = self._parse_descriptive_statistics(
            reference_statistics
        )

    def _parse_descriptive_statistics(
        self,
        statistics: FeatureDescriptiveStatistics | dict | None,
    ) -> FeatureDescriptiveStatistics | None:
        if statistics is None:
            return None
        return (
            statistics
            if isinstance(statistics, FeatureDescriptiveStatistics)
            else FeatureDescriptiveStatistics.from_response_json(statistics)
        )

    def _parse_statistics_comparison_result(
        self,
        statistics_comparison_results: list[StatisticsComparisonResult | dict]
        | None = None,
    ) -> list[StatisticsComparisonResult] | None:
        if statistics_comparison_results is None:
            return None
        sc_results = []
        for sc_result in statistics_comparison_results:
            sc_results.append(
                sc_result
                if isinstance(sc_result, StatisticsComparisonResult)
                else StatisticsComparisonResult.from_response_json(sc_result)
            )
        return sc_results

    def _parse_shifted_metric_names(self, shifted_metric_names) -> set[str] | None:
        return (
            set(shifted_metric_names)
            if isinstance(shifted_metric_names, list)
            else shifted_metric_names
        )

    @classmethod
    def from_response_json(
        cls, json_dict
    ) -> FeatureStatisticsResult | list[FeatureStatisticsResult]:
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**result) for result in json_decamelized["items"]]
        return cls(**json_decamelized)

    def to_dict(self):
        the_dict = {
            "id": self._id,
            "featureName": self._feature_name,
        }
        if self._statistics_comparison_results:
            the_dict["statisticsComparisonResults"] = [
                sc_result.to_dict() for sc_result in self._statistics_comparison_results
            ]
        if self._detection_statistics_id is not None:
            the_dict["detectionStatisticsId"] = self._detection_statistics_id
        if self._reference_statistics_id is not None:
            the_dict["referenceStatisticsId"] = self._reference_statistics_id
        if self._detection_statistics is not None:
            the_dict["detectionStatistics"] = self._detection_statistics.to_dict()
        if self._reference_statistics is not None:
            the_dict["referenceStatistics"] = self._reference_statistics.to_dict()
        if self._shifted_metric_names:
            the_dict["shiftedMetricNames"] = list(self._shifted_metric_names)

        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self) -> str:
        shift_detected_msg = ""
        if self.shift_detected:
            shift_detected_msg = f", shift detected: {self.shift_detected!r}"
        return f"FeatureStatisticsResult({self._feature_name!r}{shift_detected_msg})"

    @public
    @property
    def id(self) -> int | None:
        """Identifier assigned by the backend upon registration."""
        return self._id

    @public
    @property
    def feature_name(self) -> str:
        """Name of the feature these statistics describe."""
        return self._feature_name

    @public
    @property
    def detection_statistics_id(self) -> int | None:
        """Identifier of the descriptive statistics computed on the detection window."""
        return self._detection_statistics_id

    @public
    @property
    def reference_statistics_id(self) -> int | None:
        """Identifier of the descriptive statistics computed on the reference window."""
        return self._reference_statistics_id

    @public
    @property
    def detection_statistics(self) -> FeatureDescriptiveStatistics | None:
        """Descriptive statistics computed on the detection window."""
        return self._detection_statistics

    @public
    @property
    def reference_statistics(self) -> FeatureDescriptiveStatistics | None:
        """Descriptive statistics computed on the reference window."""
        return self._reference_statistics

    @public
    @property
    def shifted_metric_names(self) -> set[str] | None:
        """Names of metrics for which the difference exceeded the configured threshold."""
        return self._shifted_metric_names

    @public
    @property
    def shift_detected(self) -> bool:
        """Whether any metric exceeded its configured threshold."""
        return (
            self._shifted_metric_names is not None
            and len(self._shifted_metric_names) > 0
        )

    @public
    @property
    def statistics_comparison_results(
        self,
    ) -> list[StatisticsComparisonResult] | None:
        """Individual comparison results for each configured metric."""
        return self._statistics_comparison_results

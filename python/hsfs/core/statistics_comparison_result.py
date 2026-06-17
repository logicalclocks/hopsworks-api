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


@public
class StatisticsComparisonResult:
    """Outcome of a single metric comparison from a feature monitoring run.

    Each instance records the computed difference and whether shift was detected
    for a specific
    [`StatisticsComparisonConfig`][hsfs.core.statistics_comparison_config.StatisticsComparisonConfig].
    """

    def __init__(
        self,
        statistics_comparison_config_id: int,
        shift_detected: bool | None = None,
        difference: float | None = None,
        id: int | None = None,
        feature_statistics_result_id: int | None = None,
        href: str | None = None,
        **kwargs,
    ):
        self._id = id
        self._feature_statistics_result_id = feature_statistics_result_id
        self._statistics_comparison_config_id = statistics_comparison_config_id
        self._shift_detected = shift_detected
        self._difference = difference
        self._href = href

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**result) for result in json_decamelized["items"]]
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "id": self._id,
            "featureStatisticsResultId": self._feature_statistics_result_id,
            "statisticsComparisonConfigId": self._statistics_comparison_config_id,
            "shiftDetected": self._shift_detected,
            "difference": self._difference,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self) -> str:
        shift_detected_msg = ""
        if self._shift_detected:
            shift_detected_msg = f"shift detected: {self._shift_detected!r}"
        return f"StatisticsComparisonResult({shift_detected_msg})"

    @public
    @property
    def id(self) -> int | None:
        """Identifier assigned by the backend upon registration."""
        return self._id

    @public
    @property
    def feature_statistics_result_id(self) -> int | None:
        """Identifier of the parent feature statistics result."""
        return self._feature_statistics_result_id

    @public
    @property
    def statistics_comparison_config_id(self) -> int:
        """Identifier of the statistics comparison configuration that produced this result."""
        return self._statistics_comparison_config_id

    @public
    @property
    def shift_detected(self) -> bool | None:
        """Whether the difference exceeded the configured threshold."""
        return self._shift_detected

    @public
    @property
    def difference(self) -> float | None:
        """Computed difference between detection and reference statistics."""
        return self._difference

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
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hsfs import util
from hsfs.core.feature_statistics_result import FeatureStatisticsResult


if TYPE_CHECKING:
    from datetime import date, datetime


@public
class FeatureMonitoringResult:
    """Outcome of a single execution of a feature monitoring configuration.

    Records the monitoring time and whether the detection and reference windows were
    empty, together with the per-feature statistics and comparison results produced
    by the run.
    """

    NOT_FOUND_ERROR_CODE = 270233

    def __init__(
        self,
        feature_store_id: int,
        execution_id: int,
        monitoring_time: int | datetime | date | str,
        feature_monitoring_config_id: int,
        shifted_feature_names: set[str] | list[str] | None = None,
        feature_statistics_results: list[FeatureStatisticsResult]
        | list[dict[str, Any]]
        | None = None,
        empty_detection_window: bool = False,
        empty_reference_window: bool | None = None,
        raised_exception: bool = False,
        detection_window_commit_time: int | None = None,
        id: int | None = None,
        href: str | None = None,
        **kwargs,
    ):
        self._id = id
        self._href = href
        self._feature_store_id = feature_store_id
        self._execution_id = execution_id
        self._feature_monitoring_config_id = feature_monitoring_config_id
        self._shifted_feature_names = self._parse_shifted_feature_names(
            shifted_feature_names
        )
        self._feature_statistics_results = self._parse_feature_statistics_results(
            feature_statistics_results
        )
        self._monitoring_time = util._convert_event_time_to_timestamp(monitoring_time)
        self._empty_detection_window = empty_detection_window
        self._empty_reference_window = empty_reference_window
        self._raised_exception = raised_exception
        self._detection_window_commit_time = detection_window_commit_time

    def _parse_feature_statistics_results(
        self,
        feature_statistics_results: list[FeatureStatisticsResult]
        | list[dict[str, Any]]
        | None,
    ) -> list[FeatureStatisticsResult] | None:
        if feature_statistics_results is None:
            return None
        fs_results = []
        for fs_result in feature_statistics_results:
            fs_results.append(
                fs_result
                if isinstance(fs_result, FeatureStatisticsResult)
                else FeatureStatisticsResult.from_response_json(fs_result)
            )
        return fs_results

    def _parse_shifted_feature_names(self, shifted_feature_names) -> set[str] | None:
        return (
            set(shifted_feature_names)
            if isinstance(shifted_feature_names, list)
            else shifted_feature_names
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**result) for result in json_decamelized["items"]]
        return cls(**json_decamelized)

    def to_dict(self):
        the_dict = {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "featureMonitoringConfigId": self._feature_monitoring_config_id,
            "executionId": self._execution_id,
            "monitoringTime": self._monitoring_time,
            "emptyDetectionWindow": self._empty_detection_window,
            "emptyReferenceWindow": self._empty_reference_window,
            "raisedException": self._raised_exception,
        }
        if self._detection_window_commit_time is not None:
            the_dict["detectionWindowCommitTime"] = self._detection_window_commit_time
        if self._feature_statistics_results is not None:
            the_dict["featureStatisticsResults"] = [
                fs_result.to_dict() for fs_result in self._feature_statistics_results
            ]
        if self._shifted_feature_names:
            the_dict["shiftedFeatureNames"] = list(self._shifted_feature_names)

        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self) -> str:
        shift_detected_msg = ""
        if self.shift_detected:
            shift_detected_msg = f", shift detected: {self.shift_detected!r}"
        return f"FeatureMonitoringResult({self._monitoring_time!r}{shift_detected_msg})"

    @public
    @property
    def id(self) -> int | None:
        """Id of the feature monitoring result."""
        return self._id

    @public
    @property
    def feature_monitoring_config_id(self) -> int:
        """Id of the feature monitoring configuration containing this result."""
        return self._feature_monitoring_config_id

    @public
    @property
    def feature_store_id(self) -> int:
        """Id of the Feature Store."""
        return self._feature_store_id

    @public
    @property
    def execution_id(self) -> int | None:
        """Execution id of the feature monitoring job."""
        return self._execution_id

    @public
    @property
    def monitoring_time(self) -> int:
        """Time at which this feature monitoring result was created."""
        return self._monitoring_time

    @public
    @property
    def empty_detection_window(self) -> bool:
        """Whether or not the detection window was empty in this feature monitoring run."""
        return self._empty_detection_window

    @public
    @property
    def empty_reference_window(self) -> bool | None:
        """Whether or not the reference window was empty in this feature monitoring run."""
        return self._empty_reference_window

    @public
    @property
    def shifted_feature_names(self) -> set[str] | None:
        """Names of features for which at least one metric exceeded its threshold."""
        return self._shifted_feature_names

    @public
    @property
    def shift_detected(self) -> bool:
        """Whether any feature had a metric exceeding its configured threshold."""
        return (
            self._shifted_feature_names is not None
            and len(self._shifted_feature_names) > 0
        )

    @property
    def detection_window_commit_time(self) -> int | None:
        """Commit timestamp (ms) to which the detection window end was anchored.

        Set only for model-monitoring configs on logging feature groups.
        Used to detect whether a new commit has arrived since the last run.
        """
        return self._detection_window_commit_time

    @public
    @property
    def feature_statistics_results(self) -> list[FeatureStatisticsResult] | None:
        """Per-feature statistics and comparison results produced by this monitoring run."""
        return self._feature_statistics_results

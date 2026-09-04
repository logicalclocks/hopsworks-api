#
#   Copyright 2020 Logical Clocks AB
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
from typing import Any

import humps
from hopsworks_apigen import public
from hsfs import util
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.split_statistics import SplitStatistics


@public
class Statistics:
    # TODO: Add docstring
    DEFAULT_ROW_PERCENTAGE = 1.0
    NOT_FOUND_ERROR_CODE = 270228

    def __init__(
        self,
        computation_time: int,
        row_percentage: float = 1.0,
        feature_descriptive_statistics: FeatureDescriptiveStatistics
        | list[FeatureDescriptiveStatistics]
        | dict[str, Any]
        | None = None,
        # feature group
        feature_group_id: int | None = None,
        window_start_commit_time: int | None = None,
        window_end_commit_time: int | None = None,
        window_start_event_time: int | None = None,
        window_end_event_time: int | None = None,
        event_time: str | None = None,
        # training dataset
        feature_view_name: str | None = None,
        feature_view_version: int | None = None,
        training_dataset_version: int | None = None,
        split_statistics: list[dict[str, Any]] | list[SplitStatistics] | None = None,
        before_transformation: bool = False,
        href: str | None = None,
        expand: str | None = None,
        items: dict[str, Any] | None = None,
        count: int | None = None,
        type: str | None = None,
        **kwargs,
    ) -> None:
        self._computation_time = computation_time
        self._feature_descriptive_statistics = self._parse_descriptive_statistics(
            feature_descriptive_statistics
        )
        self._row_percentage = row_percentage
        # feature group
        self._feature_group_id = feature_group_id
        self._window_start_commit_time = window_start_commit_time
        self._window_end_commit_time = window_end_commit_time
        # FSTORE-2106: event-time window bounds, mutually exclusive with the commit-time
        # bounds above. Set when the owning monitoring config has an event_time feature.
        self._window_start_event_time = window_start_event_time
        self._window_end_event_time = window_end_event_time
        self._event_time = event_time
        # training dataset
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._training_dataset_version = training_dataset_version
        self._split_statistics = self._parse_split_statistics(split_statistics)
        self._before_transformation = before_transformation

    def _parse_descriptive_statistics(
        self,
        desc_statistics: dict[str, Any]
        | FeatureDescriptiveStatistics
        | list[dict[str, Any]]
        | list[FeatureDescriptiveStatistics],
    ) -> list[FeatureDescriptiveStatistics] | None:
        if desc_statistics is None:
            return None
        if isinstance(desc_statistics, FeatureDescriptiveStatistics):
            return [desc_statistics]
        if isinstance(desc_statistics, dict) and "items" not in desc_statistics:
            return [FeatureDescriptiveStatistics.from_response_json(desc_statistics)]
        if isinstance(desc_statistics, dict) and "items" in desc_statistics:
            return [
                FeatureDescriptiveStatistics.from_response_json(fds)
                for fds in desc_statistics["items"]
            ]
        if isinstance(desc_statistics, list):
            return [
                (
                    fds
                    if isinstance(fds, FeatureDescriptiveStatistics)
                    else FeatureDescriptiveStatistics.from_response_json(fds)
                )
                for fds in desc_statistics
            ]
        raise ValueError(
            "Descriptive statistics must be a FeatureDescriptiveStatistics object or a dictionary"
        )

    def _parse_split_statistics(
        self,
        split_statistics: list[dict[str, Any]] | list[SplitStatistics] | None,
    ) -> list[SplitStatistics] | None:
        if split_statistics is None:
            return None
        return [
            (
                SplitStatistics.from_response_json(split)
                if isinstance(split, dict)
                else split
            )
            for split in split_statistics
        ]

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> Statistics | list[Statistics] | None:
        json_decamelized: dict = humps.decamelize(json_dict)
        # for consistency, if the json dict contains "count" and "items", we return a list
        # even when there is a single statistics in the list
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0 or len(json_decamelized["items"]) == 0:
                return None
            return [cls(**config) for config in json_decamelized["items"]]
        return cls(**json_decamelized)

    def to_dict(self) -> dict[str, Any]:
        # fg_id, fv_name, fv_version and td_version are already defined in the URI
        _dict = {
            "computationTime": self._computation_time,
            "rowPercentage": self._row_percentage,
            "beforeTransformation": self._before_transformation,
        }
        # Window bounds are emitted only when set: a row carries either the
        # commit-time family or the event-time family, never both.
        if self._window_start_commit_time is not None:
            _dict["windowStartCommitTime"] = self._window_start_commit_time
        if self._window_end_commit_time is not None:
            _dict["windowEndCommitTime"] = self._window_end_commit_time
        if self._window_start_event_time is not None:
            _dict["windowStartEventTime"] = self._window_start_event_time
        if self._window_end_event_time is not None:
            _dict["windowEndEventTime"] = self._window_end_event_time
        if self._event_time is not None:
            _dict["eventTime"] = self._event_time
        if self._feature_descriptive_statistics is not None:
            _dict["featureDescriptiveStatistics"] = [
                fds.to_dict() for fds in self._feature_descriptive_statistics
            ]
        if self._split_statistics is not None:
            _dict["splitStatistics"] = [sps.to_dict() for sps in self._split_statistics]
        return _dict

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"Statistics({self._computation_time!r})"

    @public
    @property
    def computation_time(self) -> int:
        """Time at which the statistics were computed."""
        return self._computation_time

    @public
    @property
    def row_percentage(self) -> float:
        """Percentage of data on which statistics were computed."""
        return self._row_percentage

    @row_percentage.setter
    def row_percentage(self, row_percentage: float | None):
        if isinstance(row_percentage, (int, float)):
            row_percentage = float(row_percentage)
            if row_percentage <= 0.0 or row_percentage > 1.0:
                raise ValueError("Row percentage must be a float between 0 and 1.")
            self._row_percentage = row_percentage
        elif row_percentage is None:
            self._row_percentage = self.DEFAULT_ROW_PERCENTAGE
        else:
            raise TypeError("Row percentage must be a float between 0 and 1.")

    @public
    @property
    def feature_descriptive_statistics(
        self,
    ) -> list[FeatureDescriptiveStatistics] | None:
        """List of feature descriptive statistics."""
        return self._feature_descriptive_statistics

    @public
    @property
    def feature_group_id(self) -> int | None:
        """Id of the feature group on whose data the statistics were computed."""
        return self._feature_group_id

    @public
    @property
    def feature_view_name(self) -> str | None:
        """Name of the feature view whose query was used to retrieve the data on which the statistics were computed."""
        return self._feature_view_name

    @public
    @property
    def feature_view_version(self) -> int | None:
        """Id of the feature view whose query was used to retrieve the data on which the statistics were computed."""
        return self._feature_view_version

    @public
    @property
    def window_start_commit_time(self) -> int | None:
        """Start time of the window of data on which statistics were computed."""
        return self._window_start_commit_time

    @public
    @property
    def window_end_commit_time(self) -> int | None:
        """End time of the window of data on which statistics were computed."""
        return self._window_end_commit_time

    @public
    @property
    def window_start_event_time(self) -> int | None:
        """Start time of the event-time window of data on which statistics were computed.

        `None` unless the owning monitoring configuration has an `event_time` feature,
        in which case this replaces `window_start_commit_time` as the window bound.
        """
        return self._window_start_event_time

    @public
    @property
    def window_end_event_time(self) -> int | None:
        """End time of the event-time window of data on which statistics were computed.

        `None` unless the owning monitoring configuration has an `event_time` feature,
        in which case this replaces `window_end_commit_time` as the window bound.
        """
        return self._window_end_event_time

    @public
    @property
    def event_time(self) -> str | None:
        """Name of the feature the event-time window was sliced by.

        `None` unless the owning monitoring configuration has an `event_time` feature.
        """
        return self._event_time

    @public
    @property
    def training_dataset_version(self) -> int | None:
        """Version of the training dataset on which statistics were computed."""
        return self._training_dataset_version

    @public
    @property
    def split_statistics(self) -> list[SplitStatistics] | None:
        """List of statistics computed on each split of a training dataset."""
        return self._split_statistics

    @public
    @property
    def before_transformation(self) -> bool:
        """Whether or not the statistics were computed on feature values before applying model-dependent transformations."""
        return self._before_transformation

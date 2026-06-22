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
from enum import Enum
from typing import TYPE_CHECKING

import humps
from hopsworks_apigen import public
from hsfs import util
from hsfs.core import monitoring_window_config_engine


if TYPE_CHECKING:
    import builtins


@public
class WindowConfigType(str, Enum):
    """Type of the window.

    It can be one of `"ALL_TIME"`, `"ROLLING_TIME"`, or `"TRAINING_DATASET"`.
    """

    ALL_TIME = "ALL_TIME"
    ROLLING_TIME = "ROLLING_TIME"
    TRAINING_DATASET = "TRAINING_DATASET"

    @classmethod
    def _list_str(cls) -> builtins.list[str]:
        return [c.value for c in cls]

    @classmethod
    def _list(cls) -> builtins.list[WindowConfigType]:
        return list(cls)

    @classmethod
    def _from_str(cls, value: str) -> WindowConfigType:
        if value in cls._list_str():
            return cls(value)
        raise ValueError(
            f"Invalid value {value} for WindowConfigType, allowed values are {cls._list_str()}"
        )

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, self.__class__):
            return self is other
        return False

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


@public
class MonitoringWindowConfig:
    """Defines the slice of feature data on which statistics are computed.

    A window is either a rolling-time or all-time range over the feature data, or a
    reference to a specific training dataset version.
    The detection and reference windows of a
    [`FeatureMonitoringConfig`][hsfs.core.feature_monitoring_config.FeatureMonitoringConfig]
    are both expressed as instances of this class.
    """

    DEFAULT_ROW_PERCENTAGE = 1.0

    def __init__(
        self,
        id: int | None = None,
        window_config_type: str | WindowConfigType | None = WindowConfigType.ALL_TIME,
        time_offset: str | None = None,
        window_length: str | None = None,
        training_dataset_version: int | None = None,
        row_percentage: float | None = None,
        **kwargs,
    ):
        """Configuration to define the slice of data to compute statistics on.

        Example:
            ```python3
            # Detection or reference window
            ## Rolling Time Window
            monitoring_window_config = MonitoringWindowConfig(
                time_offset="1d", # data inserted up to 1 day ago
                window_length="1h", # data inserted until an one hour after time_offset
                row_percentage=0.2, # include only 20% of the rows when computing statistics
            )

            # Only available for reference window
            ## Training dataset
            monitoring_window_config = MonitoringWindowConfig(
                training_dataset_version=my_training_dataset.version
            )
            ```

        Parameters:
            id: The id of the monitoring window config.
            window_config_type: The type of the monitoring window config.
                One of `ALL_TIME`, `ROLLING_TIME`, `TRAINING_DATASET`.
            time_offset: The time offset from the current time to the start of the window.
                Only used for `ROLLING_TIME` windows.
            window_length: The length of the time window.
                Only used for `ROLLING_TIME` windows.
            training_dataset_version: The version of the training dataset to use as reference.
                Only used for `TRAINING_DATASET` windows.
            row_percentage: The fraction of rows to use when computing statistics [0, 1.0].
                Only used for `ROLLING_TIME` and `ALL_TIME` windows.

        Raises:
            AttributeError: If window_config_type is not one of `ALL_TIME`, `ROLLING_TIME`,
                `TRAINING_DATASET`.
        """
        self._id = id
        self._window_config_type = None
        self.window_config_type = window_config_type
        self._time_offset = time_offset
        self._window_length = window_length
        self._training_dataset_version = training_dataset_version

        if self.window_config_type in [
            WindowConfigType.TRAINING_DATASET,
        ]:
            self.row_percentage = 1.0  # td stats are computed on the whole td
        else:
            self.row_percentage = row_percentage

        self._window_config_engine = (
            monitoring_window_config_engine.MonitoringWindowConfigEngine()
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        the_dict = {
            "id": self._id,
            "windowConfigType": self._window_config_type,
        }

        if (
            self._window_config_type == WindowConfigType.ROLLING_TIME
            or self._window_config_type == WindowConfigType.ALL_TIME
        ):
            the_dict["timeOffset"] = self._time_offset
            the_dict["windowLength"] = self._window_length
            the_dict["rowPercentage"] = self.row_percentage
        elif self._window_config_type == WindowConfigType.TRAINING_DATASET:
            the_dict["trainingDatasetVersion"] = self._training_dataset_version

        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"MonitoringWindowConfig({self._window_config_type!r})"

    @public
    @property
    def id(self) -> int | None:
        """Id of the window configuration."""
        return self._id

    @public
    @property
    def window_config_type(self) -> WindowConfigType:
        """Type of the window. It can be one of `ALL_TIME`, `ROLLING_TIME`, `TRAINING_DATASET`."""
        return self._window_config_type

    @window_config_type.setter
    def window_config_type(self, window_config_type: WindowConfigType | str):
        if self._window_config_type is not None:
            raise AttributeError("window_config_type is a read-only attribute.")

        if isinstance(window_config_type, WindowConfigType):
            self._window_config_type = window_config_type
            return

        if not isinstance(window_config_type, str):
            raise TypeError(
                "window_config_type must be a string or WindowConfigType. "
                "Allowed value are" + str(WindowConfigType._list_str())
            )
        if window_config_type not in WindowConfigType._list_str():
            raise ValueError(
                "window_config_type must be one of "
                + str(WindowConfigType._list_str())
                + "."
            )
        self._window_config_type = WindowConfigType._from_str(window_config_type)

    @public
    @property
    def time_offset(self) -> str | None:
        """The time offset from the current time to the start of the time window. Only used for windows of type `ROLLING_TIME`."""
        return self._time_offset

    @public
    @property
    def window_length(self) -> str | None:
        """The length of the time window. Only used for windows of type `ROLLING_TIME`."""
        return self._window_length

    @window_length.setter
    def window_length(self, window_length: str | None):
        if window_length is None:
            self._window_length = None
        elif self._window_config_type != WindowConfigType.ROLLING_TIME:
            raise AttributeError(
                "Window length can only be set for if window_config_type is ROLLING_TIME."
            )
        elif isinstance(window_length, str):
            self._window_config_engine._time_range_str_to_time_delta(window_length)
            self._window_length = window_length
        else:
            raise TypeError("window_length must be a string.")

    @public
    @property
    def training_dataset_version(self) -> int | None:
        """The version of the training dataset to use as reference. Only used for windows of type `TRAINING_DATASET`."""
        return self._training_dataset_version

    @training_dataset_version.setter
    def training_dataset_version(self, training_dataset_version: int | None):
        if (
            self._window_config_type != WindowConfigType.TRAINING_DATASET
            and training_dataset_version is not None
        ):
            raise AttributeError(
                "training_dataset_version can only be set for if window_config_type is TRAINING_DATASET."
            )
        self._training_dataset_version = training_dataset_version

    @public
    @property
    def row_percentage(self) -> float:
        """The percentage of rows to fetch and compute the statistics on. Only used for windows of type `ROLLING_TIME` and `ALL_TIME`."""
        return self._row_percentage

    @row_percentage.setter
    def row_percentage(self, row_percentage: float | None):
        if (
            self.window_config_type == WindowConfigType.TRAINING_DATASET
            and row_percentage is not None
            and row_percentage != 1.0
        ):
            raise AttributeError(
                "Row percentage can only be set for ROLLING_TIME and ALL_TIME"
                " window config types."
            )

        if isinstance(row_percentage, (int, float)):
            row_percentage = float(row_percentage)
            if row_percentage <= 0.0 or row_percentage > 1.0:
                raise ValueError("Row percentage must be a float between 0 and 1.")
            self._row_percentage = row_percentage
        elif row_percentage is None:
            self._row_percentage = (
                1.0
                if self.window_config_type == WindowConfigType.TRAINING_DATASET
                else self.DEFAULT_ROW_PERCENTAGE
            )
        else:
            raise TypeError("Row percentage must be a float between 0 and 1.")

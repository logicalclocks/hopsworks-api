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

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

import humps
from hopsworks_apigen import public
from hopsworks_common import client, util
from hopsworks_common.constants import PREDICTOR, SCALING_CONFIG


if TYPE_CHECKING:
    from hopsworks_common.constants import Default


@public
class ScaleMetric(Enum):
    """Scaling metric for a predictor or transformer. Can be either 'CONCURRENCY' or 'RPS'."""

    CONCURRENCY = "CONCURRENCY"
    RPS = "RPS"

    @classmethod
    def has_value(cls, value):
        return any(member.value == value for member in cls)

    def __str__(self):
        return self.value


@public
class ComponentScalingConfig(ABC):
    """Scaling configuration for a predictor or transformer."""

    @public
    def __init__(
        self,
        min_instances: int,
        max_instances: int | None = None,
        scale_metric: ScaleMetric | str | Default | None = None,
        target: int | None = None,
        panic_window_percentage: float | None = None,
        panic_threshold_percentage: float | None = None,
        stable_window_seconds: int | None = None,
        scale_to_zero_retention_seconds: int | None = None,
        **kwargs,
    ):
        """Initialize a ComponentScalingConfig instance.

        Parameters:
            min_instances: Minimum number of instances to scale to.
            max_instances: Maximum number of instances to scale to.
            scale_metric: Metric to use for scaling.
            target: Target value for the selected scaling metric.
            panic_window_percentage: Percentage of the stable window to use as the panic window.
            panic_threshold_percentage: Percentage of the scale metric threshold to trigger scaling.
            stable_window_seconds: Interval in seconds for calculating the average metric.
            scale_to_zero_retention_seconds: Time in seconds to retain the last instance before scaling to zero.
        """
        scale_metric = scale_metric
        if scale_metric:
            if isinstance(scale_metric, str):
                if not ScaleMetric.has_value(scale_metric.upper()):
                    raise ValueError(
                        f"Invalid scale_metric: {scale_metric}. Must be one of {[e.value for e in ScaleMetric]}"
                    )
                self._scale_metric = ScaleMetric(scale_metric.upper())
            elif isinstance(scale_metric, ScaleMetric):
                self._scale_metric = scale_metric
            else:
                raise ValueError(
                    f"scale_metric must be a string or ScaleMetric, got {type(scale_metric)}"
                )
        else:
            self._scale_metric = None

        self._min_instances = min_instances
        self._max_instances = max_instances
        self._target = target
        self._panic_window_percentage = panic_window_percentage
        self._panic_threshold_percentage = panic_threshold_percentage
        self._stable_window_seconds = stable_window_seconds
        self._scale_to_zero_retention_seconds = scale_to_zero_retention_seconds

    @public
    def describe(self):
        """Print a JSON description of the scaling configuration."""
        util.pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @public
    @staticmethod
    def get_default_scaling_configuration(
        serving_tool: str, min_instances: int | None, component_type: str = "predictor"
    ) -> ComponentScalingConfig:
        """Get the default scaling configuration based on the serving tool and number of instances."""
        if min_instances is None:
            min_instances = (
                0  # enable scale-to-zero by default if required
                if serving_tool == PREDICTOR.SERVING_TOOL_KSERVE
                and client.is_scale_to_zero_required()
                else SCALING_CONFIG.MIN_NUM_INSTANCES
            )
        if (
            serving_tool == PREDICTOR.SERVING_TOOL_KSERVE
            and min_instances != 0
            and client.is_scale_to_zero_required()
        ):
            # ensure scale-to-zero for kserve deployments when required
            raise ValueError(
                "Scale-to-zero is required for KServe deployments in this cluster. Please, set the minimum number of instances to 0."
            )
        if serving_tool != PREDICTOR.SERVING_TOOL_KSERVE and min_instances == 0:
            raise ValueError(
                "Minimum number of instances cannot be 0 for deployments not using KServe. Please, set the minimum number of instances to at least 1."
            )
        kwargs = {"min_instances": min_instances}
        if serving_tool == PREDICTOR.SERVING_TOOL_KSERVE:
            kwargs["scale_metric"] = SCALING_CONFIG.SCALE_METRIC_CONCURRENCY
            kwargs["target"] = SCALING_CONFIG.DEFAULT_CONCURRENCY_TARGET
            kwargs["panic_threshold_percentage"] = (
                SCALING_CONFIG.DEFAULT_PANIC_THRESHOLD_PERCENTAGE
            )
            kwargs["panic_window_percentage"] = (
                SCALING_CONFIG.DEFAULT_PANIC_WINDOW_PERCENTAGE
            )
            kwargs["stable_window_seconds"] = (
                SCALING_CONFIG.DEFAULT_STABLE_WINDOW_SECONDS
            )
            kwargs["scale_to_zero_retention_seconds"] = (
                SCALING_CONFIG.DEFAULT_SCALE_TO_ZERO_RETENTION_SECONDS
            )
        if component_type == "predictor":
            return PredictorScalingConfig(**kwargs)
        return TransformerScalingConfig(**kwargs)

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        kwargs = {}

        scaling_key = getattr(cls, "SCALING_CONFIG_KEY", None)
        if scaling_key and scaling_key in json_decamelized:
            json_decamelized = json_decamelized[scaling_key]
        elif "scaling_configuration" in json_decamelized:
            json_decamelized = json_decamelized["scaling_configuration"]

        kwargs["min_instances"] = util.extract_field_from_json(
            json_decamelized, "min_instances"
        )
        kwargs["max_instances"] = util.extract_field_from_json(
            json_decamelized, "max_instances"
        )
        scale_metric = util.extract_field_from_json(json_decamelized, "scale_metric")
        if scale_metric:
            kwargs["scale_metric"] = ScaleMetric(scale_metric)
        kwargs["target"] = util.extract_field_from_json(json_decamelized, "target")
        kwargs["panic_window_percentage"] = util.extract_field_from_json(
            json_decamelized, "panic_window_percentage"
        )
        kwargs["panic_threshold_percentage"] = util.extract_field_from_json(
            json_decamelized, "panic_threshold_percentage"
        )
        kwargs["stable_window_seconds"] = util.extract_field_from_json(
            json_decamelized, "stable_window_seconds"
        )
        kwargs["scale_to_zero_retention_seconds"] = util.extract_field_from_json(
            json_decamelized, "scale_to_zero_retention_seconds"
        )
        if kwargs["min_instances"] is None:
            expected_location = (
                f"'{scaling_key}' or 'scaling_configuration'"
                if scaling_key
                else "'scaling_configuration'"
            )
            raise ValueError(
                "Invalid scaling configuration JSON: missing 'min_instances' under "
                f"{expected_location}."
            )
        return kwargs

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**self.extract_fields_from_json(json_decamelized))
        return self

    @abstractmethod
    def to_dict(self):
        pass

    def to_json(self):
        json = {
            "min_instances": self._min_instances,
        }
        if self._scale_metric is not None:
            json["scale_metric"] = str(self._scale_metric)
        if self._target is not None:
            json["target"] = self._target
        if self._max_instances is not None:
            json["max_instances"] = self._max_instances
        if self._panic_window_percentage is not None:
            json["panic_window_percentage"] = self._panic_window_percentage
        if self._panic_threshold_percentage is not None:
            json["panic_threshold_percentage"] = self._panic_threshold_percentage
        if self._stable_window_seconds is not None:
            json["stable_window_seconds"] = self._stable_window_seconds
        if self._scale_to_zero_retention_seconds is not None:
            json["scale_to_zero_retention_seconds"] = (
                self._scale_to_zero_retention_seconds
            )
        return json

    @classmethod
    @abstractmethod
    def from_json(cls, json_decamelized):
        pass

    @public
    @property
    def scale_metric(self):
        """The metric to use for scaling. Can be either 'CONCURRENCY' or 'RPS'."""
        return self._scale_metric

    @scale_metric.setter
    def scale_metric(self, scale_metric: ScaleMetric | str):
        if isinstance(scale_metric, str):
            if not ScaleMetric.has_value(scale_metric.upper()):
                raise ValueError(
                    f"Invalid scale_metric: {scale_metric}. Must be one of {[e.value for e in ScaleMetric]}"
                )
            self._scale_metric = ScaleMetric(scale_metric.upper())
        elif isinstance(scale_metric, ScaleMetric):
            self._scale_metric = scale_metric
        else:
            raise ValueError(
                f"scale_metric must be a string or ScaleMetric, got {type(scale_metric)}"
            )

    @public
    @property
    def target(self):
        """Target value for the selected scaling metric that the autoscaler should try to maintain during the stable window. For RPS, this is requests per second. For CONCURRENCY, this is concurrent number of requests."""
        return self._target

    @target.setter
    def target(self, target: int):
        self._target = target

    @public
    @property
    def min_instances(self) -> int:
        """Minimum number of instances to scale to. For deployments using kserve, this must be set to 0 to enable scaling to zero. Default is 0 for deployments using kserve and 1 for deployments not using kserve."""
        return self._min_instances

    @min_instances.setter
    def min_instances(self, min_instances: int):
        self._min_instances = min_instances

    @public
    @property
    def max_instances(self):
        """Maximum number of instances to scale to. Maximum allowed is configured in the cluster settings by the cluster administrator. Must be at least 1 and greater than or equal to min_instances."""
        return self._max_instances

    @max_instances.setter
    def max_instances(self, max_instances: int):
        self._max_instances = max_instances

    @public
    @property
    def panic_window_percentage(self):
        """The percentage of the stable window to use as the panic window during high load situations. Min is 1. Max is 100. Default is 10."""
        return self._panic_window_percentage

    @panic_window_percentage.setter
    def panic_window_percentage(self, panic_window_percentage: float):
        self._panic_window_percentage = panic_window_percentage

    @public
    @property
    def panic_threshold_percentage(self):
        """The percentage of the scale metric threshold that, when exceeded during the panic window, will trigger a scale-up event. Min is 1. Max is 200. Default is 200."""
        return self._panic_threshold_percentage

    @panic_threshold_percentage.setter
    def panic_threshold_percentage(self, panic_threshold_percentage: float):
        self._panic_threshold_percentage = panic_threshold_percentage

    @public
    @property
    def stable_window_seconds(self):
        """The interval in seconds over which to calculate the average metric. Larger values result in smoother scaling but slower reaction times. Min is 1 second. Max is 3600 seconds."""
        return self._stable_window_seconds

    @stable_window_seconds.setter
    def stable_window_seconds(self, stable_window_seconds: int):
        self._stable_window_seconds = stable_window_seconds

    @public
    @property
    def scale_to_zero_retention_seconds(self):
        """The amount of time in seconds the last instance must be kept before being scaled down to zero. Default is 0."""
        return self._scale_to_zero_retention_seconds

    @scale_to_zero_retention_seconds.setter
    def scale_to_zero_retention_seconds(self, scale_to_zero_retention_seconds: int):
        self._scale_to_zero_retention_seconds = scale_to_zero_retention_seconds

    def __repr__(self):
        return f"ComponentScalingConfig(min_instances: {self._min_instances!r}, max_instances: {self._max_instances!r}, scale_metric: {self._scale_metric!r}, target: {self._target!r}, panic_window_percentage: {self._panic_window_percentage!r}, panic_threshold_percentage: {self._panic_threshold_percentage!r}, stable_window_seconds: {self._stable_window_seconds!r}, scale_to_zero_retention_seconds: {self._scale_to_zero_retention_seconds!r})"


@public
class PredictorScalingConfig(ComponentScalingConfig):
    """Scaling configuration for a predictor."""

    SCALING_CONFIG_KEY = "predictor_scaling_config"

    @public
    def __init__(self, **kwargs):
        """Initialize a PredictorScalingConfig instance.

        Parameters:
            **kwargs: Keyword arguments for the predictor scaling configuration.
                - min_instances (int): Minimum number of instances to scale to (required).
                - max_instances (int | None, optional): Maximum number of instances to scale to.
                - scale_metric (ScaleMetric | str | Default | None, optional): Metric to use for scaling.
                - target (int | None, optional): Target value for the selected scaling metric.
                - panic_window_percentage (float | None, optional): Percentage of the stable window to use as the panic window.
                - panic_threshold_percentage (float | None, optional): Percentage of the scale metric threshold to trigger scaling.
                - stable_window_seconds (int | None, optional): Interval in seconds for calculating the average metric.
                - scale_to_zero_retention_seconds (int | None, optional): Time in seconds to retain the last instance before scaling to zero.

        Raises:
            ValueError: If `min_instances` is not provided.
        """
        min_instances = kwargs.pop("min_instances", None)
        if min_instances is None:
            raise ValueError("min_instances is a required field")
        super().__init__(min_instances=min_instances, **kwargs)

    @classmethod
    def from_json(cls, json_decamelized):
        kwargs = cls.extract_fields_from_json(json_decamelized)
        return PredictorScalingConfig(**kwargs)

    def to_dict(self):
        return {
            humps.camelize(self.SCALING_CONFIG_KEY): humps.camelize(super().to_json())
        }

    def __repr__(self):
        return f"PredictorScalingConfig({super().__repr__()})"


@public
class TransformerScalingConfig(ComponentScalingConfig):
    """Scaling configuration for a transformer."""

    SCALING_CONFIG_KEY = "transformer_scaling_config"

    @public
    def __init__(self, **kwargs):
        """Initialize a TransformerScalingConfig instance.

        Parameters:
            **kwargs: Keyword arguments for the transformer scaling configuration.
                - min_instances (int): Minimum number of instances to scale to (required).
                - max_instances (int | None, optional): Maximum number of instances to scale to.
                - scale_metric (ScaleMetric | str | Default | None, optional): Metric to use for scaling.
                - target (int | None, optional): Target value for the selected scaling metric.
                - panic_window_percentage (float | None, optional): Percentage of the stable window to use as the panic window.
                - panic_threshold_percentage (float | None, optional): Percentage of the scale metric threshold to trigger scaling.
                - stable_window_seconds (int | None, optional): Interval in seconds for calculating the average metric.
                - scale_to_zero_retention_seconds (int | None, optional): Time in seconds to retain the last instance before scaling to zero.

        Raises:
            ValueError: If `min_instances` is not provided.
        """
        min_instances = kwargs.pop("min_instances", None)
        if min_instances is None:
            raise ValueError("min_instances is a required field")
        super().__init__(min_instances=min_instances, **kwargs)

    @classmethod
    def from_json(cls, json_decamelized):
        return TransformerScalingConfig(
            **cls.extract_fields_from_json(json_decamelized)
        )

    def to_dict(self):
        return {
            humps.camelize(self.SCALING_CONFIG_KEY): humps.camelize(super().to_json())
        }

    def __repr__(self):
        return f"TransformerScalingConfig({super().__repr__()})"

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

import warnings
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

import humps
from hopsworks_apigen import public
from hopsworks_common import client, util
from hopsworks_common.constants import DEFAULT, PREDICTOR, SCALING_CONFIG


if TYPE_CHECKING:
    from hopsworks_common.constants import Default


@public
class ScaleMetric(Enum):
    """Scaling metric for a predictor or transformer.

    `CONCURRENCY` and `RPS` are Knative-only metrics, valid for KServe Knative deployments.
    `CPU` and `MEMORY` drive CPU/memory-based autoscaling, valid for KServe Standard and non-KServe deployments,
    under either autoscaler (see `Autoscaler`).
    The vLLM engine metrics apply to LLM deployments in KServe Standard mode and are read from Prometheus by KEDA,
    so they need the `KEDA` autoscaler and a cluster with KEDA installed:
    `QUEUE_DEPTH` (requests waiting in the engine queue per replica), `RUNNING_REQUESTS` (requests being generated
    per replica), `KV_CACHE_USAGE` (KV-cache utilization in percent), `QUEUE_TIME` (average time a request waits
    before generation starts, in ms), `TIME_TO_FIRST_TOKEN` (average time to the first token, in ms) and
    `REQUEST_LATENCY` (average end-to-end latency, in ms). The latency metrics are one-minute averages and scale the
    deployment in proportion to how far the average sits from its target.
    In KServe Standard mode a deployment with `min_instances == max_instances` runs a fixed replica count and ignores the metric.
    """

    CONCURRENCY = "CONCURRENCY"
    RPS = "RPS"
    CPU = "CPU"
    MEMORY = "MEMORY"
    QUEUE_DEPTH = "QUEUE_DEPTH"
    KV_CACHE_USAGE = "KV_CACHE_USAGE"
    RUNNING_REQUESTS = "RUNNING_REQUESTS"
    QUEUE_TIME = "QUEUE_TIME"
    TIME_TO_FIRST_TOKEN = "TIME_TO_FIRST_TOKEN"
    REQUEST_LATENCY = "REQUEST_LATENCY"

    @classmethod
    def _has_value(cls, value):
        return any(member.value == value for member in cls)

    def __str__(self):
        return self.value


@public
class Autoscaler(Enum):
    """Which autoscaler runs the scale metric of a KServe Standard-mode component.

    `KEDA` scales on `CPU`, `MEMORY`, or the vLLM engine metrics (`QUEUE_DEPTH`, `KV_CACHE_USAGE`, `RUNNING_REQUESTS`,
    `QUEUE_TIME`, `TIME_TO_FIRST_TOKEN`, `REQUEST_LATENCY`) through a KEDA ScaledObject, and is the autoscaler
    wherever KEDA is installed (the default).
    `HPA` is KServe's own HorizontalPodAutoscaler on `CPU` or `MEMORY`, the fallback of a cluster without KEDA.
    Knative deployments always use the Knative autoscaler and reject this setting.
    """

    HPA = "HPA"
    KEDA = "KEDA"

    @classmethod
    def _has_value(cls, value):
        return any(member.value == value for member in cls)

    def __str__(self):
        return self.value


def _coerce_scale_metric(scale_metric: ScaleMetric | str) -> ScaleMetric:
    if isinstance(scale_metric, ScaleMetric):
        return scale_metric
    if isinstance(scale_metric, str):
        if not ScaleMetric._has_value(scale_metric.upper()):
            raise ValueError(
                f"Invalid scale_metric: {scale_metric}. Must be one of {[e.value for e in ScaleMetric]}"
            )
        return ScaleMetric(scale_metric.upper())
    raise ValueError(
        f"scale_metric must be a string or ScaleMetric, got {type(scale_metric)}"
    )


def _coerce_additional_scale_metrics(
    items: list[dict | tuple | ScaleMetric | str] | None,
) -> list[dict] | None:
    """Coerce the additional scale metrics into `{"scale_metric", "target"}` dicts.

    Each item is such a dict, a `(scale_metric, target)` tuple, or a bare metric
    (target left to the backend default).
    """
    if items is None:
        return None
    coerced = []
    for item in items:
        if isinstance(item, dict):
            metric, target = item.get("scale_metric"), item.get("target")
        elif isinstance(item, (tuple, list)) and len(item) == 2:
            metric, target = item
        else:
            metric, target = item, None
        if metric is None:
            raise ValueError("every additional scale metric must name a scale_metric")
        coerced.append({"scale_metric": _coerce_scale_metric(metric), "target": target})
    return coerced


def _coerce_autoscaler(autoscaler: Autoscaler | str | None) -> Autoscaler | None:
    if autoscaler is None:
        return None
    if isinstance(autoscaler, Autoscaler):
        return autoscaler
    if isinstance(autoscaler, str):
        if not Autoscaler._has_value(autoscaler.upper()):
            raise ValueError(
                f"Invalid autoscaler: {autoscaler}. Must be one of {[e.value for e in Autoscaler]}"
            )
        return Autoscaler(autoscaler.upper())
    raise ValueError(
        f"autoscaler must be a string or Autoscaler, got {type(autoscaler)}"
    )


@public
class LogPersistence(Enum):
    """Whether a component archives its logs to the project's Logs dataset when an instance stops.

    Each instance keeps its own logs on local disk and uploads them through the REST
    API when it stops, including stops the platform initiates such as scale-to-zero.
    Only deployments whose serving container runs a Hopsworks inference pipeline image
    support archiving, since the upload runs the hopsworks SDK from that image. The
    backend rejects `ALL_REPLICAS` for TensorFlow Serving and vLLM, and for a KServe
    Python deployment with no predictor script, which runs the sklearnserver runtime.
    """

    NONE = "NONE"
    ALL_REPLICAS = "ALL_REPLICAS"

    @classmethod
    def _has_value(cls, value):
        return any(member.value == value for member in cls)

    def __str__(self):
        return self.value


def _coerce_log_persistence(
    log_persistence: LogPersistence | str | Default | None,
) -> LogPersistence | None:
    # DEFAULT and None both mean "leave it to the backend".
    if log_persistence is None or log_persistence is DEFAULT:
        return None
    if isinstance(log_persistence, LogPersistence):
        return log_persistence
    if isinstance(log_persistence, str):
        if not LogPersistence._has_value(log_persistence.upper()):
            raise ValueError(
                f"Invalid log_persistence: {log_persistence}. Must be one of {[e.value for e in LogPersistence]}"
            )
        return LogPersistence(log_persistence.upper())
    raise ValueError(
        f"log_persistence must be a string or LogPersistence, got {type(log_persistence)}"
    )


@public
class ComponentScalingConfig(ABC):
    """Scaling configuration for a predictor or transformer."""

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
        log_persistence: LogPersistence | str | Default | None = None,
        autoscaler: Autoscaler | str | None = None,
        scale_down_stabilization_window_seconds: int | None = None,
        scale_up_stabilization_window_seconds: int | None = None,
        additional_scale_metrics: list[dict | tuple | ScaleMetric | str] | None = None,
        idle_scale_to_zero: bool | None = None,
        idle_cooldown_seconds: int | None = None,
        cold_start_timeout_seconds: int | None = None,
        **kwargs,
    ):
        """Initialize a ComponentScalingConfig instance.

        Parameters:
            min_instances: Minimum number of instances to scale to.
            max_instances: Maximum number of instances to scale to.
            scale_metric: Metric to use for scaling.
            target: Target value for the selected scaling metric.
            autoscaler: Which autoscaler runs the metric in KServe Standard mode, `HPA` (KServe) or `KEDA`.
                Unset means the backend default: `KEDA` wherever it is installed, `HPA` otherwise.
            scale_down_stabilization_window_seconds: KEDA only. How long (0-3600 s) the metric must stay below
                target before instances are removed. Unset means the cluster default (300 s).
            scale_up_stabilization_window_seconds: KEDA only. How long (0-3600 s) the metric must stay above
                target before instances are added. Unset means the cluster default (0 s).
            additional_scale_metrics: KEDA only. Further metrics to scale on next to `scale_metric`, each a
                `{"scale_metric": ..., "target": ...}` dict or a `(scale_metric, target)` tuple; the most demanding
                metric decides the instance count. A missing target takes the metric's default.
            idle_scale_to_zero: KEDA only, predictor only. Scale to zero instances when idle and wake on the
                first request, which the KEDA HTTP add-on holds until an instance is ready. Unset means the
                backend default: on for a Standard-mode predictor without a transformer on a cluster with KEDA,
                so pass `False` to keep at least one instance running. An LLM deployment reloads its model on
                every wake, so expect the first request after an idle period to take as long as a cold start.
            idle_cooldown_seconds: With `idle_scale_to_zero`: seconds (0-3600) without a request before the
                last instance is removed. Unset means 300.
            cold_start_timeout_seconds: With `idle_scale_to_zero`: seconds (1-3600) a request is held while the
                deployment wakes before it fails. Unset means 600. Set it above the model's load time.
            panic_window_percentage: Percentage of the stable window to use as the panic window.
            panic_threshold_percentage: Percentage of the scale metric threshold to trigger scaling.
            stable_window_seconds: Interval in seconds for calculating the average metric.
            scale_to_zero_retention_seconds: Time in seconds to retain the last instance before scaling to zero.
            log_persistence: Whether instances upload their logs to the Logs dataset when they stop.
                Unset means the backend default: `ALL_REPLICAS` for a Python predictor,
                `NONE` for everything else.
        """
        scale_metric = scale_metric
        if scale_metric:
            if isinstance(scale_metric, str):
                if not ScaleMetric._has_value(scale_metric.upper()):
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
        self._log_persistence = _coerce_log_persistence(log_persistence)
        self._autoscaler = _coerce_autoscaler(autoscaler)
        self._scale_down_stabilization_window_seconds = (
            scale_down_stabilization_window_seconds
        )
        self._scale_up_stabilization_window_seconds = (
            scale_up_stabilization_window_seconds
        )
        self._additional_scale_metrics = _coerce_additional_scale_metrics(
            additional_scale_metrics
        )
        self._idle_scale_to_zero = idle_scale_to_zero
        self._idle_cooldown_seconds = idle_cooldown_seconds
        self._cold_start_timeout_seconds = cold_start_timeout_seconds

    @public
    def describe(self):
        """Print a JSON description of the scaling configuration."""
        util._pretty_print(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @public
    @staticmethod
    def get_default_scaling_configuration(
        serving_tool: str,
        min_instances: int | None,
        component_type: str = "predictor",
        effective_knative_mode: bool = True,
        enforce_scale_to_zero: bool = True,
    ) -> ComponentScalingConfig:
        """Get the default scaling configuration based on the serving tool and number of instances.

        Parameters:
            serving_tool: the serving tool to use (e.g. kserve)
            min_instances: minimum number of instances, or None to use the default
            component_type: the component type (predictor or transformer)
            effective_knative_mode: whether the deployment runs in KServe Knative mode.
                Only meaningful when `serving_tool` is kserve.
                Standard mode does not scale to zero and does not default to Knative-only autoscaling metrics.
            enforce_scale_to_zero: whether to reject a non-zero minimum when the cluster requires scale-to-zero for Knative deployments.
                Transformers are built before the deployment mode is known and skip this check.
                The backend validates the assembled deployment mode-aware.

        Returns:
            The default scaling configuration for the given serving tool.
        """
        kserve_knative = (
            serving_tool == PREDICTOR.SERVING_TOOL_KSERVE and effective_knative_mode
        )
        if min_instances is None:
            min_instances = (
                0  # enable scale-to-zero by default if required
                if kserve_knative and client._is_scale_to_zero_required()
                else SCALING_CONFIG.MIN_NUM_INSTANCES
            )
        if (
            kserve_knative
            and enforce_scale_to_zero
            and min_instances != 0
            and client._is_scale_to_zero_required()
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
        if kserve_knative:
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

        kwargs["min_instances"] = util._extract_field_from_json(
            json_decamelized, "min_instances"
        )
        kwargs["max_instances"] = util._extract_field_from_json(
            json_decamelized, "max_instances"
        )
        scale_metric = util._extract_field_from_json(json_decamelized, "scale_metric")
        if scale_metric:
            # A newer backend may store metrics this client does not know yet (e.g. CPU/MEMORY on an older client).
            # Runtime containers parse the deployment JSON with the client bundled in their image, so an unknown metric must not crash them.
            if ScaleMetric._has_value(scale_metric):
                kwargs["scale_metric"] = ScaleMetric(scale_metric)
            else:
                warnings.warn(
                    f"Ignoring unknown scale metric '{scale_metric}' returned by the backend; upgrade the hopsworks client to manage it.",
                    stacklevel=2,
                )
        kwargs["target"] = util._extract_field_from_json(json_decamelized, "target")
        kwargs["panic_window_percentage"] = util._extract_field_from_json(
            json_decamelized, "panic_window_percentage"
        )
        kwargs["panic_threshold_percentage"] = util._extract_field_from_json(
            json_decamelized, "panic_threshold_percentage"
        )
        kwargs["stable_window_seconds"] = util._extract_field_from_json(
            json_decamelized, "stable_window_seconds"
        )
        kwargs["scale_to_zero_retention_seconds"] = util._extract_field_from_json(
            json_decamelized, "scale_to_zero_retention_seconds"
        )
        # Round-tripped rather than dropped: reading a deployment and saving it back must not
        # silently reset the stored choice to the backend default.
        log_persistence = util._extract_field_from_json(
            json_decamelized, "log_persistence"
        )
        if log_persistence:
            kwargs["log_persistence"] = LogPersistence(log_persistence)
        autoscaler = util._extract_field_from_json(json_decamelized, "autoscaler")
        if autoscaler:
            # Same tolerance as scale_metric: a runtime container must survive a value its bundled client predates.
            if Autoscaler._has_value(autoscaler):
                kwargs["autoscaler"] = Autoscaler(autoscaler)
            else:
                warnings.warn(
                    f"Ignoring unknown autoscaler '{autoscaler}' returned by the backend; upgrade the hopsworks client to manage it.",
                    stacklevel=2,
                )
        kwargs["scale_down_stabilization_window_seconds"] = (
            util._extract_field_from_json(
                json_decamelized, "scale_down_stabilization_window_seconds"
            )
        )
        kwargs["scale_up_stabilization_window_seconds"] = util._extract_field_from_json(
            json_decamelized, "scale_up_stabilization_window_seconds"
        )
        additional = util._extract_field_from_json(
            json_decamelized, "additional_scale_metrics"
        )
        if additional:
            # Entries whose metric this client predates are dropped with a warning, like scale_metric.
            known = []
            for item in additional:
                metric = item.get("scale_metric") if isinstance(item, dict) else None
                if metric and ScaleMetric._has_value(metric):
                    known.append(
                        {
                            "scale_metric": ScaleMetric(metric),
                            "target": item.get("target"),
                        }
                    )
                else:
                    warnings.warn(
                        f"Ignoring unknown additional scale metric '{metric}' returned by the backend; upgrade the hopsworks client to manage it.",
                        stacklevel=2,
                    )
            kwargs["additional_scale_metrics"] = known or None
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
        kwargs["idle_scale_to_zero"] = util._extract_field_from_json(
            json_decamelized, "idle_scale_to_zero"
        )
        kwargs["idle_cooldown_seconds"] = util._extract_field_from_json(
            json_decamelized, "idle_cooldown_seconds"
        )
        kwargs["cold_start_timeout_seconds"] = util._extract_field_from_json(
            json_decamelized, "cold_start_timeout_seconds"
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
        if self._log_persistence is not None:
            json["log_persistence"] = str(self._log_persistence)
        if self._autoscaler is not None:
            json["autoscaler"] = str(self._autoscaler)
        if self._scale_down_stabilization_window_seconds is not None:
            json["scale_down_stabilization_window_seconds"] = (
                self._scale_down_stabilization_window_seconds
            )
        if self._scale_up_stabilization_window_seconds is not None:
            json["scale_up_stabilization_window_seconds"] = (
                self._scale_up_stabilization_window_seconds
            )
        if self._additional_scale_metrics:
            json["additional_scale_metrics"] = [
                {"scale_metric": str(m["scale_metric"]), "target": m["target"]}
                for m in self._additional_scale_metrics
            ]
        if self._idle_scale_to_zero is not None:
            json["idle_scale_to_zero"] = self._idle_scale_to_zero
        if self._idle_cooldown_seconds is not None:
            json["idle_cooldown_seconds"] = self._idle_cooldown_seconds
        if self._cold_start_timeout_seconds is not None:
            json["cold_start_timeout_seconds"] = self._cold_start_timeout_seconds
        return json

    @classmethod
    @abstractmethod
    def from_json(cls, json_decamelized):
        pass

    @public
    @property
    def scale_metric(self):
        """The metric to use for scaling.

        `CONCURRENCY` and `RPS` are Knative-only metrics for KServe Knative deployments.
        `CPU` and `MEMORY` drive CPU/memory-based autoscaling in KServe Standard mode, under KServe's HPA or KEDA.
        The vLLM engine metrics (`QUEUE_DEPTH`, `KV_CACHE_USAGE`, `RUNNING_REQUESTS`, `QUEUE_TIME`, `TIME_TO_FIRST_TOKEN`, `REQUEST_LATENCY`) are for LLM deployments in KServe Standard mode, scaled by KEDA.
        Standard deployments default to `CPU` when `min_instances < max_instances` (to `QUEUE_DEPTH` for a vLLM predictor on a cluster with KEDA); with `min_instances == max_instances` no autoscaler is configured and the metric is cleared.
        """
        return self._scale_metric

    @scale_metric.setter
    def scale_metric(self, scale_metric: ScaleMetric | str):
        if isinstance(scale_metric, str):
            if not ScaleMetric._has_value(scale_metric.upper()):
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
    def autoscaler(self):
        """Which autoscaler runs the scale metric of a KServe Standard-mode component.

        `HPA` is KServe's own HorizontalPodAutoscaler, `KEDA` a KEDA ScaledObject (needs KEDA installed in the cluster).
        `CPU` and `MEMORY` run under either; the vLLM engine metrics only under `KEDA`.
        Unset means the backend default: `KEDA` for an engine metric, `HPA` otherwise. Cleared with the metric when
        `min_instances == max_instances`, and rejected for Knative deployments.
        """
        return self._autoscaler

    @autoscaler.setter
    def autoscaler(self, autoscaler: Autoscaler | str | None):
        self._autoscaler = _coerce_autoscaler(autoscaler)

    @public
    @property
    def scale_down_stabilization_window_seconds(self):
        """KEDA only: seconds (0-3600) the metric must stay below target before instances are removed.

        Unset means the cluster default (300 s). GPU-bound LLM replicas are slow to bring back, so a long
        window avoids churn; rejected with KServe's HPA, which ignores it.
        """
        return self._scale_down_stabilization_window_seconds

    @scale_down_stabilization_window_seconds.setter
    def scale_down_stabilization_window_seconds(self, seconds: int | None):
        self._scale_down_stabilization_window_seconds = seconds

    @public
    @property
    def scale_up_stabilization_window_seconds(self):
        """KEDA only: seconds (0-3600) the metric must stay above target before instances are added.

        Unset means the cluster default (0 s, react at once). Rejected with KServe's HPA, which ignores it.
        """
        return self._scale_up_stabilization_window_seconds

    @scale_up_stabilization_window_seconds.setter
    def scale_up_stabilization_window_seconds(self, seconds: int | None):
        self._scale_up_stabilization_window_seconds = seconds

    @public
    @property
    def idle_scale_to_zero(self):
        """KEDA only, predictor only: scale to zero instances when idle and wake on the first request.

        The KEDA HTTP add-on holds that request until an instance is ready, so the deployment stays reachable
        at zero. Unset means the backend default, on wherever it applies: a Standard-mode predictor without a
        transformer on a cluster with KEDA; `False` keeps at least one instance running. Rejected with KServe's
        HPA, a transformer, or in Knative mode (which scales to zero on its own with `min_instances=0`). An LLM
        deployment reloads its model on every wake: the first request after an idle period takes as long as a
        cold start.
        """
        return self._idle_scale_to_zero

    @idle_scale_to_zero.setter
    def idle_scale_to_zero(self, enabled: bool | None):
        self._idle_scale_to_zero = enabled

    @public
    @property
    def idle_cooldown_seconds(self):
        """With `idle_scale_to_zero`: seconds (0-3600) without a request before the last instance is removed.

        Unset means 300.
        """
        return self._idle_cooldown_seconds

    @idle_cooldown_seconds.setter
    def idle_cooldown_seconds(self, seconds: int | None):
        self._idle_cooldown_seconds = seconds

    @public
    @property
    def cold_start_timeout_seconds(self):
        """With `idle_scale_to_zero`: seconds (1-3600) a request is held while the deployment wakes.

        Unset means 600. A request held longer fails; set it above the time the model takes to load.
        """
        return self._cold_start_timeout_seconds

    @cold_start_timeout_seconds.setter
    def cold_start_timeout_seconds(self, seconds: int | None):
        self._cold_start_timeout_seconds = seconds

    @public
    @property
    def additional_scale_metrics(self):
        """KEDA only: further metrics scaled on next to `scale_metric`, as `{"scale_metric", "target"}` dicts.

        KEDA sizes the deployment by whichever metric asks for the most instances, e.g. queue depth or
        KV-cache usage, whichever fires first. Metrics must be distinct; engine metrics need a vLLM predictor.
        """
        return self._additional_scale_metrics

    @additional_scale_metrics.setter
    def additional_scale_metrics(
        self, items: list[dict | tuple | ScaleMetric | str] | None
    ):
        self._additional_scale_metrics = _coerce_additional_scale_metrics(items)

    @public
    @property
    def target(self):
        """Target value for the selected scaling metric that the autoscaler should try to maintain.

        For `RPS`, this is requests per second.
        For `CONCURRENCY`, this is the number of concurrent requests.
        For `CPU` and `MEMORY`, this is the utilization percentage.
        For `QUEUE_DEPTH`, this is the number of requests waiting in the vLLM engine queue per replica (default 5).
        For `KV_CACHE_USAGE`, this is the KV-cache utilization percentage per replica (default 80).
        For `RUNNING_REQUESTS`, this is the number of requests being generated per replica (default 32).
        For `QUEUE_TIME`, `TIME_TO_FIRST_TOKEN` and `REQUEST_LATENCY`, this is the average in milliseconds the
        autoscaler keeps the deployment under (defaults 1000, 2000 and 10000).
        """
        return self._target

    @target.setter
    def target(self, target: int):
        self._target = target

    @public
    @property
    def min_instances(self) -> int:
        """Minimum number of instances to scale to.

        KServe Knative deployments scale to zero when this is 0, and the cluster may require it.
        KServe Standard deployments do not scale to zero and need at least 1.
        Defaults to 0 for KServe Knative deployments when the cluster requires scale-to-zero, otherwise to 1.
        """
        return self._min_instances

    @min_instances.setter
    def min_instances(self, min_instances: int):
        self._min_instances = min_instances

    @public
    @property
    def max_instances(self):
        """Maximum number of instances to scale to.

        Maximum allowed is configured in the cluster settings by the cluster administrator. Must be at least 1 and greater than or equal to min_instances.
        Defaults to the cluster maximum, except for LLM deployments in KServe Standard mode, which default to `min_instances` (fixed replica count).
        """
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

    @public
    @property
    def log_persistence(self):
        """Whether every instance uploads its logs to the project's Logs dataset when it stops.

        'ALL_REPLICAS' or 'NONE'.
        The backend rejects 'ALL_REPLICAS' for TensorFlow Serving and vLLM, and for a KServe Python deployment with no predictor script, because those runtime images do not ship the hopsworks SDK the upload runs.
        Unset means the backend default: on where it is supported, off everywhere else.
        """
        return self._log_persistence

    @log_persistence.setter
    def log_persistence(self, log_persistence: LogPersistence | str):
        self._log_persistence = _coerce_log_persistence(log_persistence)

    def __repr__(self):
        return f"ComponentScalingConfig(min_instances: {self._min_instances!r}, max_instances: {self._max_instances!r}, scale_metric: {self._scale_metric!r}, target: {self._target!r}, panic_window_percentage: {self._panic_window_percentage!r}, panic_threshold_percentage: {self._panic_threshold_percentage!r}, stable_window_seconds: {self._stable_window_seconds!r}, scale_to_zero_retention_seconds: {self._scale_to_zero_retention_seconds!r}, log_persistence: {self._log_persistence!r}, autoscaler: {self._autoscaler!r}, scale_down_stabilization_window_seconds: {self._scale_down_stabilization_window_seconds!r}, scale_up_stabilization_window_seconds: {self._scale_up_stabilization_window_seconds!r}, additional_scale_metrics: {self._additional_scale_metrics!r}, idle_scale_to_zero: {self._idle_scale_to_zero!r}, idle_cooldown_seconds: {self._idle_cooldown_seconds!r}, cold_start_timeout_seconds: {self._cold_start_timeout_seconds!r})"


@public
class PredictorScalingConfig(ComponentScalingConfig):
    """Scaling configuration for a predictor."""

    SCALING_CONFIG_KEY = "predictor_scaling_config"

    def __init__(self, **kwargs):
        """Initialize a PredictorScalingConfig instance.

        Other Parameters: Keyword arguments for the predictor scaling configuration:
            min_instances (int): Minimum number of instances to scale to (required).
            max_instances (int | None, optional): Maximum number of instances to scale to.
            scale_metric (ScaleMetric | str | Default | None, optional): Metric to use for scaling.
            target (int | None, optional): Target value for the selected scaling metric.
            panic_window_percentage (float | None, optional): Percentage of the stable window to use as the panic window.
            panic_threshold_percentage (float | None, optional): Percentage of the scale metric threshold to trigger scaling.
            stable_window_seconds (int | None, optional): Interval in seconds for calculating the average metric.
            scale_to_zero_retention_seconds (int | None, optional): Time in seconds to retain the last instance before scaling to zero.
            log_persistence (LogPersistence | str | Default | None, optional): Whether instances upload their logs to the Logs dataset when they stop.
            autoscaler (Autoscaler | str | None, optional): Which autoscaler runs the metric in KServe Standard mode, `HPA` (KServe) or `KEDA` (the default where installed).
            scale_down_stabilization_window_seconds (int | None, optional): KEDA only. Seconds the metric must stay below target before scaling in.
            scale_up_stabilization_window_seconds (int | None, optional): KEDA only. Seconds the metric must stay above target before scaling out.
            additional_scale_metrics (list | None, optional): KEDA only. Further `{"scale_metric", "target"}` metrics; the most demanding one wins.
            idle_scale_to_zero (bool | None, optional): KEDA only. Scale to zero when idle and wake on the first request; on by default where KEDA is installed, `False` to keep an instance running.
            idle_cooldown_seconds (int | None, optional): With `idle_scale_to_zero`. Seconds without a request before the last instance is removed (default 300).
            cold_start_timeout_seconds (int | None, optional): With `idle_scale_to_zero`. Seconds a request is held while the deployment wakes (default 600).

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

    def __init__(self, **kwargs):
        """Initialize a TransformerScalingConfig instance.

        Other Parameters: Keyword arguments for the transformer scaling configuration:
            min_instances (int): Minimum number of instances to scale to (required).
            max_instances (int | None, optional): Maximum number of instances to scale to.
            scale_metric (ScaleMetric | str | Default | None, optional): Metric to use for scaling.
            target (int | None, optional): Target value for the selected scaling metric.
            panic_window_percentage (float | None, optional): Percentage of the stable window to use as the panic window.
            panic_threshold_percentage (float | None, optional): Percentage of the scale metric threshold to trigger scaling.
            stable_window_seconds (int | None, optional): Interval in seconds for calculating the average metric.
            scale_to_zero_retention_seconds (int | None, optional): Time in seconds to retain the last instance before scaling to zero.
            log_persistence (LogPersistence | str | Default | None, optional): Whether instances upload their logs to the Logs dataset when they stop.
            autoscaler (Autoscaler | str | None, optional): Which autoscaler runs the metric in KServe Standard mode, `HPA` (KServe) or `KEDA` (the default where installed).
            scale_down_stabilization_window_seconds (int | None, optional): KEDA only. Seconds the metric must stay below target before scaling in.
            scale_up_stabilization_window_seconds (int | None, optional): KEDA only. Seconds the metric must stay above target before scaling out.
            additional_scale_metrics (list | None, optional): KEDA only. Further `{"scale_metric", "target"}` metrics; the most demanding one wins.

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

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
from hopsworks_common import util


@public
class DeploymentLoggingConfig:
    """Feature logging configuration of a serving deployment.

    The default predictor logs every request to the feature view's logging feature group.
    This object sets, per deployment, how the predictor batches rows before it hands them over, and, on the `realtime` transport, the resources of the inference logger sidecar that produces them to Kafka.
    Every field is optional.
    A field left as `None` takes the platform default an administrator sets through the `serving_feature_logger_*` variables; the values themselves are the platform's, so they are not repeated here.
    Values are read when the deployment's pods start, so set them before `deployment.start()` or follow a change with `deployment.save()` and `deployment.restart()`.

    Example:
        ```python
        from hsml.deployment_logging_config import DeploymentLoggingConfig

        deployment = model.deploy(
            feature_logging=DeploymentLoggingConfig(
                flush_interval_seconds=600,
                flush_bytes=4 * 1024 * 1024,
            )
        )
        deployment.start()

        # tighten the loss bound later
        deployment.feature_logging.flush_interval_seconds = 60
        deployment.save()
        deployment.restart()
        ```

    Parameters:
        flush_interval_seconds: Age at which the job transport's file buffer rotates the open segment and uploads it, whether or not it reached `flush_bytes`.
        flush_bytes: Size at which the job transport's file buffer rotates the open segment, before the interval elapses.
        max_buffer_bytes: Upper bound on the bytes the job transport's buffer holds on the pod; rows beyond it are dropped and counted.
        max_event_bytes: Largest single batch the predictor posts; a larger batch is split into several posts. It must stay within the inference logger's own limit, which the chart sets.
        shutdown_seconds: Budget the predictor has to drain and upload what it holds when the deployment stops or its revision rolls.
        batch_rows: Rows the predictor collects before it posts one batch. It must stay within the inference logger's per-post row cap, which the chart sets, or every post over the cap is refused.
        batch_bytes: Coalesced batch bytes that force a post while the predictor has a logging backlog; an idle predictor posts at once.
        batch_seconds: Upper bound on the time the predictor spends coalescing one post; it drains what is already queued rather than waiting for more rows to arrive.
        queue_size: Rows the predictor keeps queued for logging, including rows in flight; beyond it rows are dropped and counted.
        sidecar_cpu: CPU request of the feature-log sidecar container, in cores.
        sidecar_memory_mb: Memory request of the feature-log sidecar container, in MiB.
        transport: How logged rows reach the logging feature group, `"realtime"` through the inference logger and Kafka with an online copy, or `"job"` through a file buffer, HopsFS and a Python job.
            The flush and buffer fields apply to `"job"` only and are rejected on a `"realtime"` deployment.
    """

    _INTEGER_FIELDS = (
        "flush_interval_seconds",
        "flush_bytes",
        "max_buffer_bytes",
        "max_event_bytes",
        "shutdown_seconds",
        "batch_rows",
        "batch_bytes",
        "batch_seconds",
        "queue_size",
        "sidecar_memory_mb",
    )
    _FIELDS = _INTEGER_FIELDS + ("sidecar_cpu", "transport")
    TRANSPORTS = ("realtime", "job")
    # Fields that only mean something to the offline-only job transport.
    _JOB_ONLY_FIELDS = (
        "flush_interval_seconds",
        "flush_bytes",
        "max_buffer_bytes",
        "shutdown_seconds",
    )

    def __init__(
        self,
        flush_interval_seconds: int | None = None,
        flush_bytes: int | None = None,
        max_buffer_bytes: int | None = None,
        max_event_bytes: int | None = None,
        shutdown_seconds: int | None = None,
        batch_rows: int | None = None,
        batch_seconds: int | None = None,
        queue_size: int | None = None,
        sidecar_cpu: float | None = None,
        sidecar_memory_mb: int | None = None,
        transport: str | None = None,
        batch_bytes: int | None = None,
    ):
        self._flush_interval_seconds = flush_interval_seconds
        self._flush_bytes = flush_bytes
        self._max_buffer_bytes = max_buffer_bytes
        self._max_event_bytes = max_event_bytes
        self._shutdown_seconds = shutdown_seconds
        self._batch_rows = batch_rows
        self._batch_bytes = batch_bytes
        self._batch_seconds = batch_seconds
        self._queue_size = queue_size
        self._sidecar_cpu = sidecar_cpu
        self._sidecar_memory_mb = sidecar_memory_mb
        self._transport = None if transport is None else str(transport).strip().lower()
        self._validate()

    @public
    def describe(self):
        """Print a JSON description of the logging configuration."""
        util.pretty_print(self)

    def _validate(self):
        for name in self._INTEGER_FIELDS:
            value = getattr(self, "_" + name)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer, got {value!r}")
        cpu = self._sidecar_cpu
        if cpu is not None and (
            isinstance(cpu, bool) or not isinstance(cpu, (int, float)) or cpu <= 0
        ):
            raise ValueError(f"sidecar_cpu must be a positive number, got {cpu!r}")
        if self._transport is not None and self._transport not in self.TRANSPORTS:
            raise ValueError(
                f"transport must be one of {', '.join(self.TRANSPORTS)}, "
                f"got {self._transport!r}"
            )
        if self._transport == "realtime":
            set_job_fields = [
                name
                for name in self._JOB_ONLY_FIELDS
                if getattr(self, "_" + name) is not None
            ]
            if set_job_fields:
                raise ValueError(
                    "The realtime transport has no sidecar buffer, so these fields "
                    f"only apply to transport='job': {', '.join(set_job_fields)}"
                )
        self._require_not_above("flush_bytes", "max_buffer_bytes")
        self._require_not_above("max_event_bytes", "max_buffer_bytes")
        self._require_not_above("batch_rows", "queue_size")
        self._require_not_above("batch_bytes", "max_event_bytes")

    def _require_not_above(self, smaller: str, larger: str):
        low = getattr(self, "_" + smaller)
        high = getattr(self, "_" + larger)
        if low is not None and high is not None and low > high:
            raise ValueError(f"{smaller} ({low}) cannot exceed {larger} ({high})")

    def _set(self, name: str, value):
        previous = getattr(self, "_" + name)
        setattr(self, "_" + name, value)
        try:
            self._validate()
        except ValueError:
            setattr(self, "_" + name, previous)
            raise

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return DeploymentLoggingConfig(**cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        config = json_decamelized
        for wrapper in ("feature_logging", "feature_logging_config"):
            if wrapper in json_decamelized:
                config = json_decamelized.pop(wrapper)
                break
        return {
            name: util._extract_field_from_json(config, name) for name in cls._FIELDS
        }

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**self.extract_fields_from_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            humps.camelize(name): getattr(self, "_" + name)
            for name in self._FIELDS
            if getattr(self, "_" + name) is not None
        }

    @public
    @property
    def flush_interval_seconds(self):
        """Longest time the sidecar keeps rows in memory before writing them."""
        return self._flush_interval_seconds

    @flush_interval_seconds.setter
    def flush_interval_seconds(self, flush_interval_seconds: int | None):
        self._set("flush_interval_seconds", flush_interval_seconds)

    @public
    @property
    def transport(self):
        """How logged rows reach the logging feature group, `"realtime"` or `"job"`."""
        return self._transport

    @transport.setter
    def transport(self, transport: str | None):
        self._set(
            "transport", None if transport is None else str(transport).strip().lower()
        )

    @public
    @property
    def batch_bytes(self):
        """Coalesced batch bytes that force a post while the predictor has a backlog."""
        return self._batch_bytes

    @batch_bytes.setter
    def batch_bytes(self, batch_bytes: int | None):
        self._set("batch_bytes", batch_bytes)

    @public
    @property
    def flush_bytes(self):
        """Buffered bytes that trigger a write before the interval elapses."""
        return self._flush_bytes

    @flush_bytes.setter
    def flush_bytes(self, flush_bytes: int | None):
        self._set("flush_bytes", flush_bytes)

    @public
    @property
    def max_buffer_bytes(self):
        """Upper bound on the bytes the job transport's buffer holds on the pod.

        Rows beyond it are dropped and counted. The sidecar is not what buffers for this
        transport: the file buffer is the predictor's own, on the pod's disk.
        """
        return self._max_buffer_bytes

    @max_buffer_bytes.setter
    def max_buffer_bytes(self, max_buffer_bytes: int | None):
        self._set("max_buffer_bytes", max_buffer_bytes)

    @public
    @property
    def max_event_bytes(self):
        """Largest single batch posted to the sidecar."""
        return self._max_event_bytes

    @max_event_bytes.setter
    def max_event_bytes(self, max_event_bytes: int | None):
        self._set("max_event_bytes", max_event_bytes)

    @public
    @property
    def shutdown_seconds(self):
        """Time the sidecar has to write its buffer when the deployment stops."""
        return self._shutdown_seconds

    @shutdown_seconds.setter
    def shutdown_seconds(self, shutdown_seconds: int | None):
        self._set("shutdown_seconds", shutdown_seconds)

    @public
    @property
    def batch_rows(self):
        """Rows the predictor collects before posting one batch."""
        return self._batch_rows

    @batch_rows.setter
    def batch_rows(self, batch_rows: int | None):
        self._set("batch_rows", batch_rows)

    @public
    @property
    def batch_seconds(self):
        """Longest time the predictor holds a partial batch before posting it."""
        return self._batch_seconds

    @batch_seconds.setter
    def batch_seconds(self, batch_seconds: int | None):
        self._set("batch_seconds", batch_seconds)

    @public
    @property
    def queue_size(self):
        """Rows the predictor keeps queued for logging before it drops new ones."""
        return self._queue_size

    @queue_size.setter
    def queue_size(self, queue_size: int | None):
        self._set("queue_size", queue_size)

    @public
    @property
    def sidecar_cpu(self):
        """CPU request of the feature-log sidecar container, in cores."""
        return self._sidecar_cpu

    @sidecar_cpu.setter
    def sidecar_cpu(self, sidecar_cpu: float | None):
        self._set("sidecar_cpu", sidecar_cpu)

    @public
    @property
    def sidecar_memory_mb(self):
        """Memory request of the feature-log sidecar container, in MiB."""
        return self._sidecar_memory_mb

    @sidecar_memory_mb.setter
    def sidecar_memory_mb(self, sidecar_memory_mb: int | None):
        self._set("sidecar_memory_mb", sidecar_memory_mb)

    def __repr__(self):
        fields = ", ".join(
            f"{name}: {getattr(self, '_' + name)!r}" for name in self._FIELDS
        )
        return f"DeploymentLoggingConfig({fields})"

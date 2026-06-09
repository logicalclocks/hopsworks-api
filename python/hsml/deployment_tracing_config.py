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
class DeploymentTracingConfig:
    """Tracing configuration for a serving deployment.

    Parameters:
        enabled: Whether tracing is enabled.
        provider: Tracing provider name.
        experiment_id: Experiment identifier attached to the traces.
        status: Deployment tracing status.
        sampling_percentage: Sampling percentage for trace collection.
        otel_tracing_storage: Where traces are stored. Allowed values are
            ``online``, ``offline`` and ``both``. Defaults to ``online``.
    """

    STORAGE_ONLINE = "online"
    STORAGE_OFFLINE = "offline"
    STORAGE_BOTH = "both"
    VALID_STORAGES = (STORAGE_ONLINE, STORAGE_OFFLINE, STORAGE_BOTH)

    def __init__(
        self,
        enabled: bool | None = None,
        provider: str | None = None,
        experiment_id: str | None = None,
        status: str | None = None,
        sampling_percentage: int | None = None,
        otel_tracing_storage: str | None = STORAGE_ONLINE,
        **kwargs,
    ):
        self._enabled = enabled
        self._provider = provider
        self._experiment_id = experiment_id
        self._status = status
        self._sampling_percentage = sampling_percentage
        self._otel_tracing_storage = self._validate_otel_tracing_storage(
            otel_tracing_storage
        )

    @public
    def describe(self):
        """Print a JSON description of the tracing configuration."""
        util.pretty_print(self)

    @classmethod
    def _validate_otel_tracing_storage(cls, otel_tracing_storage: str | None):
        if otel_tracing_storage is None:
            return cls.STORAGE_ONLINE
        if otel_tracing_storage not in cls.VALID_STORAGES:
            raise ValueError(
                "Tracing storage '{}' is not valid. Possible values are '{}'".format(
                    otel_tracing_storage, ", ".join(cls.VALID_STORAGES)
                )
            )
        return otel_tracing_storage

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        return DeploymentTracingConfig(**cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        config = (
            json_decamelized.pop("tracing")
            if "tracing" in json_decamelized
            else json_decamelized.pop("tracing_config")
            if "tracing_config" in json_decamelized
            else json_decamelized
        )

        kwargs = {}
        kwargs["enabled"] = util.extract_field_from_json(config, "enabled")
        kwargs["provider"] = util.extract_field_from_json(config, "provider")
        kwargs["experiment_id"] = util.extract_field_from_json(config, "experiment_id")
        kwargs["status"] = util.extract_field_from_json(config, "status")
        kwargs["sampling_percentage"] = util.extract_field_from_json(
            config, "sampling_percentage"
        )
        kwargs["otel_tracing_storage"] = util.extract_field_from_json(
            config, "otel_tracing_storage"
        )
        return kwargs

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**self.extract_fields_from_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        json = {"otelTracingStorage": self._otel_tracing_storage}
        if self._enabled is not None:
            json["enabled"] = self._enabled
        if self._provider is not None:
            json["provider"] = self._provider
        if self._experiment_id is not None:
            json["experimentId"] = self._experiment_id
        if self._status is not None:
            json["status"] = self._status
        if self._sampling_percentage is not None:
            json["samplingPercentage"] = self._sampling_percentage
        return json

    @public
    @property
    def enabled(self):
        """Whether tracing is enabled."""
        return self._enabled

    @enabled.setter
    def enabled(self, enabled: bool | None):
        self._enabled = enabled

    @public
    @property
    def provider(self):
        """Tracing provider name."""
        return self._provider

    @provider.setter
    def provider(self, provider: str | None):
        self._provider = provider

    @public
    @property
    def experiment_id(self):
        """Experiment identifier attached to the traces."""
        return self._experiment_id

    @experiment_id.setter
    def experiment_id(self, experiment_id: str | None):
        self._experiment_id = experiment_id

    @public
    @property
    def status(self):
        """Deployment tracing status."""
        return self._status

    @status.setter
    def status(self, status: str | None):
        self._status = status

    @public
    @property
    def sampling_percentage(self):
        """Sampling percentage for trace collection."""
        return self._sampling_percentage

    @sampling_percentage.setter
    def sampling_percentage(self, sampling_percentage: int | None):
        self._sampling_percentage = sampling_percentage

    @public
    @property
    def otel_tracing_storage(self):
        """Where traces are stored."""
        return self._otel_tracing_storage

    @otel_tracing_storage.setter
    def otel_tracing_storage(self, otel_tracing_storage: str | None):
        self._otel_tracing_storage = self._validate_otel_tracing_storage(
            otel_tracing_storage
        )

    def __repr__(self):
        return (
            "DeploymentTracingConfig("
            f"enabled: {self._enabled!r}, "
            f"otel_tracing_storage: {self._otel_tracing_storage!r})"
        )

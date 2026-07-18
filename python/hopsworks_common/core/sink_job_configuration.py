#
#   Copyright 2025 Hopsworks AB
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
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hopsworks_common.core.rest_endpoint import RestEndpointConfig
from hopsworks_common.job_schedule import JobSchedule


if TYPE_CHECKING:
    from collections.abc import Sequence
else:
    from hopsworks_common.core.rest_endpoint import RestEndpointConfig


class LoadingStrategy(Enum):
    FULL_LOAD = "FULL_LOAD"
    INCREMENTAL_ID = "INCREMENTAL_ID"
    INCREMENTAL_TIMESTAMP = "INCREMENTAL_TIMESTAMP"
    INCREMENTAL_DATE = "INCREMENTAL_DATE"


class WriteMode(Enum):
    APPEND = "APPEND"
    MERGE = "MERGE"


@public("hopsworks.core.FeatureColumnMapping")
class FeatureColumnMapping:
    def __init__(self, source_column: str, feature_name: str):
        self.source_column = source_column
        self.feature_name = feature_name

    def to_dict(self):
        return {
            "sourceColumn": self.source_column,
            "featureName": self.feature_name,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_json(self):
        return humps.decamelize(self.to_dict())

    @public
    @property
    def source_column(self) -> str:
        return self._source_column

    @source_column.setter
    def source_column(self, source_column: str) -> None:
        self._source_column = source_column

    @public
    @property
    def feature_name(self) -> str:
        return self._feature_name

    @feature_name.setter
    def feature_name(self, feature_name: str) -> None:
        self._feature_name = util._autofix_feature_name(feature_name, warn=True)


@public("hopsworks.core.FullLoadConfig")
class FullLoadConfig:
    def __init__(
        self,
        source_cursor_field: str | None = None,
        initial_value: str | None = None,
    ):
        self._source_cursor_field = source_cursor_field
        self._initial_value = initial_value

    def to_dict(self):
        return {
            "sourceCursorField": self._source_cursor_field,
            "initialValue": self._initial_value,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            source_cursor_field=json_decamelized.get("source_cursor_field"),
            initial_value=json_decamelized.get("initial_value"),
        )

    @public
    @property
    def source_cursor_field(self) -> str | None:
        return self._source_cursor_field

    @source_cursor_field.setter
    def source_cursor_field(self, source_cursor_field: str | None) -> None:
        self._source_cursor_field = source_cursor_field

    @public
    @property
    def initial_value(self) -> str | None:
        return self._initial_value

    @initial_value.setter
    def initial_value(self, initial_value: str | None) -> None:
        self._initial_value = initial_value


@public("hopsworks.core.LoadingConfig")
class LoadingConfig:
    def __init__(
        self,
        loading_strategy: LoadingStrategy | str = LoadingStrategy.FULL_LOAD,
        source_cursor_field: str | None = None,
        initial_value: str | None = None,
        rest_filter_param: str | None = None,
    ):
        if isinstance(loading_strategy, LoadingStrategy):
            self._loading_strategy = loading_strategy
        elif isinstance(loading_strategy, str):
            try:
                self._loading_strategy = LoadingStrategy(loading_strategy)
            except ValueError as exc:
                valid_values = ", ".join(strategy.value for strategy in LoadingStrategy)
                raise ValueError(
                    f"Invalid loading_strategy '{loading_strategy}'. "
                    f"Valid values: {valid_values}."
                ) from exc
        else:
            raise TypeError(
                "loading_strategy must be a LoadingStrategy or str, "
                f"got {type(loading_strategy).__name__}."
            )
        self._source_cursor_field = source_cursor_field
        self._initial_value = initial_value
        self._rest_filter_param = rest_filter_param

    def to_dict(self):
        incremental_config = None
        full_load_config = None

        if self._loading_strategy in [
            LoadingStrategy.INCREMENTAL_ID,
            LoadingStrategy.INCREMENTAL_TIMESTAMP,
            LoadingStrategy.INCREMENTAL_DATE,
        ]:
            incremental_config = {"sourceCursorField": self._source_cursor_field}
            if self._rest_filter_param is not None:
                incremental_config["restFilterParam"] = self._rest_filter_param
            if self._loading_strategy in [
                LoadingStrategy.INCREMENTAL_ID,
                LoadingStrategy.INCREMENTAL_TIMESTAMP,
            ]:
                incremental_config["initialValue"] = self._initial_value
            elif self._initial_value is not None:
                if isinstance(self._initial_value, (int, float)):
                    incremental_config["initialIngestionDate"] = int(
                        self._initial_value
                    )
                else:
                    initial_value = str(self._initial_value)
                    parsed = None
                    if "T" in initial_value:
                        if initial_value.endswith("Z"):
                            parsed = datetime.fromisoformat(initial_value[:-1])
                            if parsed.tzinfo is None:
                                parsed = parsed.replace(tzinfo=timezone.utc)
                        else:
                            parsed = datetime.fromisoformat(initial_value)
                            if parsed.tzinfo is None:
                                parsed = parsed.replace(tzinfo=timezone.utc)
                    if parsed is not None:
                        incremental_config["initialIngestionDate"] = int(
                            parsed.timestamp() * 1000
                        )
                    else:
                        incremental_config["initialIngestionDate"] = (
                            util._get_timestamp_from_date_string(initial_value)
                        )
            if all(value is None for value in incremental_config.values()):
                incremental_config = None

        if self._loading_strategy == LoadingStrategy.FULL_LOAD:
            full_load_config = {
                "sourceCursorField": self._source_cursor_field,
                "initialValue": self._initial_value,
            }
            if all(value is None for value in full_load_config.values()):
                full_load_config = None

        return {
            "loadingStrategy": self._loading_strategy.value,
            "incrementalLoadingConfig": incremental_config,
            "fullLoadConfig": full_load_config,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        incremental_config = json_decamelized.get("incremental_loading_config")
        full_load_config = json_decamelized.get("full_load_config")

        source_cursor_field = None
        initial_value = None
        rest_filter_param = None
        if incremental_config:
            source_cursor_field = incremental_config.get("source_cursor_field")
            rest_filter_param = incremental_config.get("rest_filter_param")
            if "initial_value" in incremental_config:
                initial_value = incremental_config.get("initial_value")
            elif "initial_ingestion_date" in incremental_config:
                initial_value = incremental_config.get("initial_ingestion_date")
        elif full_load_config:
            source_cursor_field = full_load_config.get("source_cursor_field")
            initial_value = full_load_config.get("initial_value")

        return cls(
            loading_strategy=json_decamelized.get(
                "loading_strategy", LoadingStrategy.FULL_LOAD.value
            ),
            source_cursor_field=source_cursor_field,
            initial_value=initial_value,
            rest_filter_param=rest_filter_param,
        )

    def to_json(self):
        return humps.decamelize(self.to_dict())


@public("hopsworks.core.SinkJobConfiguration")
class SinkJobConfiguration:
    DTO_TYPE = "ingestionJobConfiguration"
    DEFAULT_ENVIRONMENT_NAME = "dlthub-ingestion-pipeline"

    def __init__(
        self,
        name: str | None = None,
        environment_name: str | None = DEFAULT_ENVIRONMENT_NAME,
        transform_script_path: str | None = None,
        write_mode: WriteMode | str | None = WriteMode.APPEND,
        batch_size: int | None = 100000,
        sql_source_fetch_chunk_size: int | None = 50000,
        source_read_workers: int | None = 1,
        data_processing_workers: int | None = 1,
        max_upload_batch_size_mb: int | None = 128,
        sql_table_num_partitions: int | None = 2,
        loading_config: LoadingConfig | dict | None = None,
        column_mappings: Sequence[FeatureColumnMapping | dict] | None = None,
        endpoint_config: dict | RestEndpointConfig | None = None,
        schedule_config: JobSchedule | dict | None = None,
        targets: Sequence[TableIngestionTarget] | None = None,
        table_parallelism: int | None = 1,
    ):
        self._name = name
        self._environment_name = environment_name
        self._transform_script_path = transform_script_path
        self.write_mode = write_mode
        self._batch_size = batch_size
        self._sql_source_fetch_chunk_size = sql_source_fetch_chunk_size
        self._source_read_workers = source_read_workers
        self._data_processing_workers = data_processing_workers
        self._max_upload_batch_size_mb = max_upload_batch_size_mb
        self._sql_table_num_partitions = sql_table_num_partitions
        if isinstance(loading_config, dict):
            self._loading_config = LoadingConfig.from_response_json(loading_config)
        else:
            self._loading_config = loading_config or LoadingConfig()

        self.column_mappings = column_mappings
        self._featuregroup_id = None
        self._featurestore_id = None
        self._storage_connector_id = None
        if isinstance(endpoint_config, dict):
            self._endpoint_config = RestEndpointConfig.from_response_json(
                endpoint_config
            )
        else:
            self._endpoint_config = endpoint_config
        self._schedule_config = (
            JobSchedule.from_response_json(schedule_config)
            if isinstance(schedule_config, dict)
            else schedule_config
        )
        # multi-table ingestion: one job copies several feature groups, one worker
        # pod per table, up to table_parallelism tables at a time
        self._targets = list(targets) if targets else None
        self._table_parallelism = table_parallelism

    def to_dict(self):
        if self._targets:
            return self._to_multi_table_dict()
        return {
            "type": self.DTO_TYPE,
            "name": self._name,
            "environmentName": self._environment_name,
            "transformScriptPath": self._transform_script_path,
            "writeMode": self._write_mode.value,
            "batchSize": self._batch_size,
            "sqlSourceFetchChunkSize": self._sql_source_fetch_chunk_size,
            "sourceReadWorkers": self._source_read_workers,
            "dataProcessingWorkers": self._data_processing_workers,
            "maxUploadBatchSizeMB": self._max_upload_batch_size_mb,
            "sqlTableNumPartitions": self._sql_table_num_partitions,
            "loadingConfig": (
                self._loading_config.to_dict()
                if isinstance(self._loading_config, LoadingConfig)
                else self._loading_config
            ),
            "columnMappings": [
                self._normalize_column_mapping(mapping)
                for mapping in self._column_mappings
            ],
            "featuregroupId": self._featuregroup_id,
            "featurestoreId": self._featurestore_id,
            "storageConnectorId": self._storage_connector_id,
            "endpointConfig": (
                self._endpoint_config.to_dict()
                if hasattr(self._endpoint_config, "to_dict")
                else self._endpoint_config
            ),
            "jobSchedule": (
                self._schedule_config.to_dict()
                if isinstance(self._schedule_config, JobSchedule)
                else self._schedule_config
            ),
        }

    def _to_multi_table_dict(self):
        # Job-level scalar fields (write mode, batch sizes, workers, endpoint) are
        # the defaults the backend applies to any target that does not override
        # them; feature group, loading config and column mappings are per-target.
        return {
            "type": self.DTO_TYPE,
            "name": self._name,
            "environmentName": self._environment_name,
            "transformScriptPath": self._transform_script_path,
            "writeMode": self._write_mode.value,
            "batchSize": self._batch_size,
            "sqlSourceFetchChunkSize": self._sql_source_fetch_chunk_size,
            "sourceReadWorkers": self._source_read_workers,
            "dataProcessingWorkers": self._data_processing_workers,
            "maxUploadBatchSizeMB": self._max_upload_batch_size_mb,
            "sqlTableNumPartitions": self._sql_table_num_partitions,
            "featurestoreId": self._featurestore_id,
            "storageConnectorId": self._storage_connector_id,
            "tableParallelism": self._table_parallelism,
            "targets": [target.to_dict() for target in self._targets],
            "endpointConfig": (
                self._endpoint_config.to_dict()
                if hasattr(self._endpoint_config, "to_dict")
                else self._endpoint_config
            ),
            "jobSchedule": (
                self._schedule_config.to_dict()
                if isinstance(self._schedule_config, JobSchedule)
                else self._schedule_config
            ),
        }

    @public
    @property
    def targets(self) -> list[TableIngestionTarget] | None:
        return self._targets

    @targets.setter
    def targets(self, targets: Sequence[TableIngestionTarget] | None) -> None:
        self._targets = list(targets) if targets else None

    @public
    @property
    def table_parallelism(self) -> int | None:
        return self._table_parallelism

    @table_parallelism.setter
    def table_parallelism(self, table_parallelism: int | None) -> None:
        self._table_parallelism = table_parallelism

    @staticmethod
    def _normalize_column_mapping(mapping):
        if isinstance(mapping, FeatureColumnMapping):
            return mapping.to_dict()
        if isinstance(mapping, dict):
            if "sourceColumn" in mapping and "featureName" in mapping:
                return mapping
            if "source_column" in mapping and "feature_name" in mapping:
                return {
                    "sourceColumn": mapping["source_column"],
                    "featureName": mapping["feature_name"],
                }
        source_column = getattr(mapping, "source_column", None)
        feature_name = getattr(mapping, "feature_name", None)
        if source_column is not None and feature_name is not None:
            return {"sourceColumn": source_column, "featureName": feature_name}
        return mapping

    @staticmethod
    def _coerce_column_mapping(mapping) -> FeatureColumnMapping:
        if isinstance(mapping, FeatureColumnMapping):
            return mapping
        if isinstance(mapping, dict):
            return FeatureColumnMapping.from_response_json(mapping)

        source_column = getattr(mapping, "source_column", None)
        feature_name = getattr(mapping, "feature_name", None)
        if source_column is not None and feature_name is not None:
            return FeatureColumnMapping(
                source_column=source_column,
                feature_name=feature_name,
            )

        raise TypeError(
            "column_mappings entries must provide source_column and feature_name."
        )

    def json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        loading_config = LoadingConfig.from_response_json(
            json_decamelized.get("loading_config", {})
        )
        column_mappings = [
            FeatureColumnMapping.from_response_json(mapping)
            for mapping in json_decamelized.get("column_mappings", [])
        ]
        job_schedule = json_decamelized.get("job_schedule", None)
        endpoint_config = json_dict.get("endpointConfig", None)
        return SinkJobConfiguration(
            environment_name=json_decamelized.get(
                "environment_name", cls.DEFAULT_ENVIRONMENT_NAME
            ),
            transform_script_path=json_decamelized.get("transform_script_path"),
            write_mode=json_decamelized.get("write_mode", WriteMode.APPEND.value),
            batch_size=json_decamelized.get("batch_size", 100000),
            sql_source_fetch_chunk_size=json_decamelized.get(
                "sql_source_fetch_chunk_size", 50000
            ),
            source_read_workers=json_decamelized.get("source_read_workers", 1),
            data_processing_workers=json_decamelized.get("data_processing_workers", 1),
            max_upload_batch_size_mb=json_decamelized.get(
                "max_upload_batch_size_mb", 128
            ),
            sql_table_num_partitions=json_decamelized.get(
                "sql_table_num_partitions", 2
            ),
            name=json_decamelized.get("name", None),
            loading_config=loading_config,
            column_mappings=column_mappings,
            endpoint_config=(
                RestEndpointConfig.from_response_json(endpoint_config)
                if endpoint_config
                else None
            ),
            schedule_config=(
                JobSchedule.from_response_json(job_schedule) if job_schedule else None
            ),
        )

    def _set_extra_params(self, **kwargs) -> None:
        self._featuregroup_id = kwargs.get("featuregroup_id")
        self._featurestore_id = kwargs.get("featurestore_id")
        self._storage_connector_id = kwargs.get("storage_connector_id")
        endpoint_config = kwargs.get("endpoint_config")
        if isinstance(endpoint_config, dict):
            self._endpoint_config = RestEndpointConfig.from_response_json(
                endpoint_config
            )
        else:
            self._endpoint_config = endpoint_config
        self._name = kwargs.get("name", self._name)
        self._environment_name = kwargs.get("environment_name", self._environment_name)
        self._transform_script_path = kwargs.get(
            "transform_script_path", self._transform_script_path
        )

    @public
    @property
    def batch_size(self) -> int | None:
        return self._batch_size

    @batch_size.setter
    def batch_size(self, batch_size: int | None) -> None:
        self._batch_size = batch_size

    @public
    @property
    def sql_source_fetch_chunk_size(self) -> int | None:
        return self._sql_source_fetch_chunk_size

    @sql_source_fetch_chunk_size.setter
    def sql_source_fetch_chunk_size(
        self, sql_source_fetch_chunk_size: int | None
    ) -> None:
        self._sql_source_fetch_chunk_size = sql_source_fetch_chunk_size

    @public
    @property
    def source_read_workers(self) -> int | None:
        return self._source_read_workers

    @source_read_workers.setter
    def source_read_workers(self, source_read_workers: int | None) -> None:
        self._source_read_workers = source_read_workers

    @public
    @property
    def data_processing_workers(self) -> int | None:
        return self._data_processing_workers

    @data_processing_workers.setter
    def data_processing_workers(self, data_processing_workers: int | None) -> None:
        self._data_processing_workers = data_processing_workers

    @public
    @property
    def max_upload_batch_size_mb(self) -> int | None:
        return self._max_upload_batch_size_mb

    @max_upload_batch_size_mb.setter
    def max_upload_batch_size_mb(self, max_upload_batch_size_mb: int | None) -> None:
        self._max_upload_batch_size_mb = max_upload_batch_size_mb

    @public
    @property
    def sql_table_num_partitions(self) -> int | None:
        return self._sql_table_num_partitions

    @sql_table_num_partitions.setter
    def sql_table_num_partitions(self, sql_table_num_partitions: int | None) -> None:
        self._sql_table_num_partitions = sql_table_num_partitions

    @public
    @property
    def loading_config(self) -> LoadingConfig | dict | None:
        return self._loading_config

    @loading_config.setter
    def loading_config(self, loading_config: LoadingConfig | dict | None) -> None:
        self._loading_config = loading_config

    @public
    @property
    def column_mappings(self) -> list[FeatureColumnMapping]:
        return self._column_mappings

    @column_mappings.setter
    def column_mappings(
        self, column_mappings: Sequence[FeatureColumnMapping | dict] | None
    ) -> None:
        self._column_mappings = (
            [self._coerce_column_mapping(mapping) for mapping in column_mappings]
            if column_mappings
            else []
        )

    @public
    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, name: str | None) -> None:
        self._name = name

    @public
    @property
    def environment_name(self) -> str | None:
        return self._environment_name

    @environment_name.setter
    def environment_name(self, environment_name: str | None) -> None:
        self._environment_name = environment_name

    @public
    @property
    def transform_script_path(self) -> str | None:
        return self._transform_script_path

    @transform_script_path.setter
    def transform_script_path(self, transform_script_path: str | None) -> None:
        self._transform_script_path = transform_script_path

    @public
    @property
    def write_mode(self) -> WriteMode:
        return self._write_mode

    @write_mode.setter
    def write_mode(self, write_mode: WriteMode | str | None) -> None:
        if write_mode is None:
            self._write_mode = WriteMode.APPEND
        elif isinstance(write_mode, WriteMode):
            self._write_mode = write_mode
        elif isinstance(write_mode, str):
            try:
                self._write_mode = WriteMode(write_mode.upper())
            except ValueError as exc:
                valid_values = ", ".join(mode.value for mode in WriteMode)
                raise ValueError(
                    f"Invalid write_mode '{write_mode}'. Valid values: {valid_values}."
                ) from exc
        else:
            raise TypeError(
                "write_mode must be a WriteMode or str, "
                f"got {type(write_mode).__name__}."
            )

    @public
    @property
    def schedule_config(self) -> JobSchedule | None:
        return self._schedule_config

    @schedule_config.setter
    def schedule_config(self, schedule_config: JobSchedule | dict | None) -> None:
        self._schedule_config = (
            JobSchedule.from_response_json(schedule_config)
            if isinstance(schedule_config, dict)
            else schedule_config
        )


@public("hopsworks.core.TableIngestionTarget")
class TableIngestionTarget:
    """A single source table -> feature group of a multi-table ingestion job.

    Each target is copied by its own worker pod and can override the job-level
    ingestion settings; unset fields fall back to the job-level defaults.

    Parameters:
        feature_group: The (saved) feature group to ingest into. Either this or
            `feature_group_id` must be provided.
        feature_group_id: The id of the feature group, when the object is not at hand.
        enabled: Whether this table is ingested. A disabled target stays part of the
            job but is skipped for the run. Defaults to `True`.
        loading_config: Per-target loading strategy (full / incremental).
        column_mappings: Per-target source column -> feature name mappings.
        transform_script_path: Per-target transformation script path.
        write_mode: Per-target write mode (`APPEND` / `MERGE`).
        batch_size: Per-target write batch size.
        sql_source_fetch_chunk_size: Per-target source fetch chunk size for SQL sources.
        sql_table_num_partitions: Per-target number of read partitions for SQL sources.
        max_upload_batch_size_mb: Per-target maximum upload batch size in MB.
        source_read_workers: Per-target number of source read workers.
        data_processing_workers: Per-target number of data processing workers.
        resource_config: Per-target worker pod resources, e.g.
            `{"cores": 2, "memory": 4096}`.
        endpoint_config: Per-target REST endpoint configuration.
    """

    def __init__(
        self,
        feature_group: Any = None,
        *,
        feature_group_id: int | None = None,
        enabled: bool = True,
        loading_config: LoadingConfig | dict | None = None,
        column_mappings: Sequence[FeatureColumnMapping | dict] | None = None,
        transform_script_path: str | None = None,
        write_mode: WriteMode | str | None = None,
        batch_size: int | None = None,
        sql_source_fetch_chunk_size: int | None = None,
        sql_table_num_partitions: int | None = None,
        max_upload_batch_size_mb: int | None = None,
        source_read_workers: int | None = None,
        data_processing_workers: int | None = None,
        resource_config: dict | None = None,
        endpoint_config: dict | RestEndpointConfig | None = None,
    ):
        resolved_id = feature_group_id
        if feature_group is not None:
            resolved_id = getattr(feature_group, "id", None) or feature_group_id
        if resolved_id is None:
            raise ValueError(
                "TableIngestionTarget requires a saved feature_group (with an id) "
                "or an explicit feature_group_id."
            )
        self._feature_group_id = resolved_id
        self._enabled = enabled
        self._loading_config = (
            LoadingConfig.from_response_json(loading_config)
            if isinstance(loading_config, dict)
            else loading_config
        )
        self._column_mappings = column_mappings
        self._transform_script_path = transform_script_path
        if isinstance(write_mode, WriteMode):
            self._write_mode = write_mode.value
        elif isinstance(write_mode, str):
            self._write_mode = WriteMode(write_mode.upper()).value
        else:
            self._write_mode = None
        self._batch_size = batch_size
        self._sql_source_fetch_chunk_size = sql_source_fetch_chunk_size
        self._sql_table_num_partitions = sql_table_num_partitions
        self._max_upload_batch_size_mb = max_upload_batch_size_mb
        self._source_read_workers = source_read_workers
        self._data_processing_workers = data_processing_workers
        self._resource_config = resource_config
        self._endpoint_config = (
            RestEndpointConfig.from_response_json(endpoint_config)
            if isinstance(endpoint_config, dict)
            else endpoint_config
        )

    def to_dict(self):
        target = {
            "featuregroupId": self._feature_group_id,
            "enabled": self._enabled,
        }
        if self._loading_config is not None:
            target["loadingConfig"] = (
                self._loading_config.to_dict()
                if hasattr(self._loading_config, "to_dict")
                else self._loading_config
            )
        if self._column_mappings is not None:
            target["columnMappings"] = [
                SinkJobConfiguration._normalize_column_mapping(mapping)
                for mapping in self._column_mappings
            ]
        # only send overrides the caller set; the backend inherits the rest
        for key, value in (
            ("transformScriptPath", self._transform_script_path),
            ("writeMode", self._write_mode),
            ("batchSize", self._batch_size),
            ("sqlSourceFetchChunkSize", self._sql_source_fetch_chunk_size),
            ("sqlTableNumPartitions", self._sql_table_num_partitions),
            ("maxUploadBatchSizeMB", self._max_upload_batch_size_mb),
            ("sourceReadWorkers", self._source_read_workers),
            ("dataProcessingWorkers", self._data_processing_workers),
            ("resourceConfig", self._resource_config),
        ):
            if value is not None:
                target[key] = value
        if self._endpoint_config is not None:
            target["endpointConfig"] = (
                self._endpoint_config.to_dict()
                if hasattr(self._endpoint_config, "to_dict")
                else self._endpoint_config
            )
        return target

    def json(self):
        return json.dumps(self.to_dict())

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        loading_config = json_decamelized.get("loading_config")
        return cls(
            feature_group_id=json_decamelized.get("featuregroup_id"),
            enabled=json_decamelized.get("enabled", True),
            loading_config=(
                LoadingConfig.from_response_json(loading_config)
                if loading_config
                else None
            ),
            column_mappings=[
                FeatureColumnMapping.from_response_json(mapping)
                for mapping in json_decamelized.get("column_mappings", [])
            ]
            or None,
            transform_script_path=json_decamelized.get("transform_script_path"),
            write_mode=json_decamelized.get("write_mode"),
            batch_size=json_decamelized.get("batch_size"),
            sql_source_fetch_chunk_size=json_decamelized.get(
                "sql_source_fetch_chunk_size"
            ),
            sql_table_num_partitions=json_decamelized.get("sql_table_num_partitions"),
            max_upload_batch_size_mb=json_decamelized.get("max_upload_batch_size_mb"),
            source_read_workers=json_decamelized.get("source_read_workers"),
            data_processing_workers=json_decamelized.get("data_processing_workers"),
            resource_config=json_decamelized.get("resource_config"),
            endpoint_config=json_decamelized.get("endpoint_config"),
        )

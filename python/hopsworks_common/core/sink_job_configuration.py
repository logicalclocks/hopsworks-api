from enum import Enum
import json
from hopsworks_common import util
import humps
from hopsworks_common.job_schedule import JobSchedule


class LoadingStrategy(Enum):
    FULL_LOAD = "FULL_LOAD"
    INCREMENTAL_ID = "INCREMENTAL_ID"
    INCREMENTAL_TIMESTAMP = "INCREMENTAL_TIMESTAMP"
    INCREMENTAL_DATE = "INCREMENTAL_DATE"


class FeatureColumnMapping:

    def __init__(self, source_column: str, feature_name: str):
        self.source_column = source_column
        self.feature_name = feature_name

    def to_dict(self):
        return {
            "sourceColumn": self.source_column,
            "featureName": self.feature_name,
        }

    def from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return FeatureColumnMapping(**json_decamelized)

    def to_json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def source_column(self) -> str:
        return self._source_column

    @source_column.setter
    def source_column(self, source_column: str) -> None:
        self._source_column = source_column


class LoadingConfig:
    def __init__(
        self,
        loading_strategy: LoadingStrategy | str = LoadingStrategy.FULL_LOAD,
        source_cursor_field: str | None = None,
        initial_value: str | None = None,
    ):
        self._loading_strategy = (
            loading_strategy
            if isinstance(loading_strategy, LoadingStrategy)
            else LoadingStrategy(loading_strategy)
        )
        self._source_cursor_field = source_cursor_field
        self._initial_value = initial_value

    def to_dict(self):
        return {
            "loadingStrategy": self._loading_strategy.value,
            "sourceCursorField": self._source_cursor_field,
            "initialValue": (
                self._initial_value
                if (
                    self._loading_strategy == LoadingStrategy.INCREMENTAL_ID
                    or self._loading_strategy == LoadingStrategy.INCREMENTAL_TIMESTAMP
                )
                else None
            ),
            "initialValueDate": (
                self._initial_value
                if self._loading_strategy == LoadingStrategy.INCREMENTAL_DATE
                else None
            ),
        }

    def from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return LoadingConfig(**json_decamelized)

    def to_json(self):
        return json.dumps(self, cls=util.Encoder)


class SinkJobConfiguration:
    DTO_TYPE = "ingestionJobConfiguration"

    def __init__(
        self,
        name: str | None = None,
        batch_size: int | None = 100000,
        loading_config: LoadingConfig | dict | None = None,
        column_mappings: list[FeatureColumnMapping] | list[dict] | None = None,
        schedule_config: JobSchedule | dict | None = None,
    ):
        self._name = name
        self._batch_size = batch_size
        self._loading_config = loading_config or LoadingConfig()
        self._column_mappings = column_mappings or []
        self._featuregroup_id = None
        self._featurestore_id = None
        self._storage_connector_id = None
        self._endpoint_config = None
        self._schedule_config = (
            JobSchedule.from_response_json(schedule_config)
            if isinstance(schedule_config, dict)
            else schedule_config
        )

    def to_dict(self):
        config = {
            "type": self.DTO_TYPE,
            "name": self._name,
            "batchSize": self._batch_size,
            "loadingConfig": (
                self._loading_config.to_dict()
                if isinstance(self._loading_config, LoadingConfig)
                else self._loading_config
            ),
            "columnMappings": [
                (
                    mapping.to_dict()
                    if isinstance(mapping, FeatureColumnMapping)
                    else mapping
                )
                for mapping in self._column_mappings
            ],
            "featuregroupId": self._featuregroup_id,
            "featurestoreId": self._featurestore_id,
            "storageConnectorId": self._storage_connector_id,
            "endpointConfig": self._endpoint_config,
            "jobSchedule": self._schedule_config,
        }
        return config

    def json(self):
        return json.dumps(self.to_dict())

    def from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        loading_config = LoadingConfig().from_response_json(
            json_decamelized.get("loading_config", {})
        )
        column_mappings = [
            FeatureColumnMapping().from_response_json(mapping)
            for mapping in json_decamelized.get("column_mappings", [])
        ]
        job_schedule = json_decamelized.get("job_schedule", None)
        return SinkJobConfiguration(
            batch_size=json_decamelized.get("batch_size", 100000),
            name=json_decamelized.get("name", None),
            loading_config=loading_config,
            column_mappings=column_mappings,
            schedule_config=JobSchedule.from_response_json(job_schedule) if job_schedule else None,
        )

    def set_extra_params(self, **kwargs) -> None:
        self._featuregroup_id = kwargs.get("featuregroup_id", None)
        self._featurestore_id = kwargs.get("featurestore_id", None)
        self._storage_connector_id = kwargs.get("storage_connector_id", None)
        self._endpoint_config = kwargs.get("endpoint_config", None)
        self._name = kwargs.get("name", self._name)

    @property
    def batch_size(self) -> int | None:
        return self._batch_size

    @batch_size.setter
    def batch_size(self, batch_size: int | None) -> None:
        self._batch_size = batch_size

    @property
    def loading_config(self) -> LoadingConfig | dict | None:
        return self._loading_config

    @loading_config.setter
    def loading_config(self, loading_config: LoadingConfig | dict | None) -> None:
        self._loading_config = loading_config

    @property
    def column_mappings(self) -> list[FeatureColumnMapping] | list[dict] | None:
        return self._column_mappings

    @column_mappings.setter
    def column_mappings(
        self, column_mappings: list[FeatureColumnMapping] | list[dict] | None
    ) -> None:
        self._column_mappings = column_mappings

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, name: str | None) -> None:
        self._name = name

    @property
    def schedule_config(self) -> dict | None:
        return self._schedule_config

    @schedule_config.setter
    def schedule_config(self, schedule_config: JobSchedule | dict | None) -> None:
        self._schedule_config = (
            JobSchedule.from_response_json(schedule_config)
            if isinstance(schedule_config, dict)
            else schedule_config
        )

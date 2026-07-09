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
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hopsworks_common.core.rest_endpoint import RestEndpointConfig
from hsfs import storage_connector as sc


if TYPE_CHECKING:
    from collections.abc import Sequence

    from hopsworks_common import job
    from hopsworks_common.core.sink_job_configuration import TableIngestionTarget
    from hopsworks_common.job_schedule import JobSchedule
    from hsfs import feature_group as fg
    from hsfs.core import data_source_data as dsd
    from hsfs.core.explicit_provenance import Links
    from hsfs.core.inferred_metadata import InferredMetadata
    from hsfs.core.multi_table_ingestion import MultiTableIngestionJob
    from hsfs.training_dataset import TrainingDataset


@public
class DataSource:
    """Metadata object used to provide data source information.

    You can obtain data sources using [`FeatureStore.get_data_source`][hsfs.feature_store.FeatureStore.get_data_source].

    The DataSource class encapsulates the details of a data source that can be used for reading or writing data.
    It supports various types of sources, such as SQL queries, database tables, file paths, and storage connectors.

    For **Google Sheets** connectors, construct a DataSource directly and pass it to
    [`FeatureStore.create_feature_group`][hsfs.feature_store.FeatureStore.create_feature_group]
    with `sink_enabled=True`:

    ```python
    from hsfs.core.data_source import DataSource

    connector = fs.get_storage_connector("my_google_sheets_connector")

    fg = fs.create_feature_group(
        name="budget_actuals",
        version=1,
        primary_key=["id"],
        data_source=DataSource(
            storage_connector=connector,
            table="Q1 Actuals",          # sheet tab name (required)
            spreadsheet_id="1BxiMVs…",  # omit if already set on the connector
        ),
        sink_enabled=True,
    )
    fg.save()
    fg.sink_job.run()
    ```

    Parameters:
        query: SQL query string for the data source, if applicable.
        database: Name of the database containing the data source.
        group: Group or schema name for the data source.
        table: Table name for the data source.
            For Google Sheets connectors this is the sheet tab name.
        path: File system path for the data source.
        storage_connector: Storage connector object holds configuration for accessing the data source.
        metrics: List of metric column names for the data source.
        dimensions: List of dimension column names for the data source.
        rest_endpoint: REST endpoint configuration for the data source.
        spreadsheet_id: Google Spreadsheet ID for Google Sheets data sources.
            Only required when the spreadsheet ID was not set on the connector itself; the connector-level value is used otherwise.
    """

    def __init__(
        self,
        query: str | None = None,
        database: str | None = None,
        group: str | None = None,
        table: str | None = None,
        path: str | None = None,
        format: str | None = None,
        storage_connector: sc.StorageConnector | dict[str, Any] | None = None,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        rest_endpoint: RestEndpointConfig | dict | None = None,
        spreadsheet_id: str | None = None,
        **kwargs,
    ):
        """Initialize a DataSource object.

        Parameters:
            query: SQL query string for the data source, if applicable.
            database: Name of the database containing the data source.
            group: Group or schema name for the data source.
            table: Table name for the data source.
            path: File system path for the data source.
            format: Format of the data source (e.g., "hudi", "delta", "csv", "parquet", "orc", "avro").
            storage_connector: Storage connector object holds configuration for accessing the data source.
            metrics: List of metric column names for the data source.
            dimensions: List of dimension column names for the data source.
            rest_endpoint: REST endpoint configuration for the data source.
            spreadsheet_id: Google Spreadsheet ID for Google Sheets data sources.
            **kwargs: Additional keyword arguments.
        """
        self._query = query
        self._database = database
        self._group = group
        self._table = table
        self._path = path
        self._format = format
        if storage_connector is not None and isinstance(storage_connector, dict):
            self._storage_connector = sc.StorageConnector.from_response_json(
                storage_connector
            )
        else:
            self._storage_connector: sc.StorageConnector = storage_connector
        self._metrics = metrics or []
        self._dimensions = dimensions or []
        self._rest_endpoint = (
            RestEndpointConfig.from_response_json(rest_endpoint)
            if isinstance(rest_endpoint, dict)
            else rest_endpoint
        )
        self._spreadsheet_id = spreadsheet_id

    @classmethod
    def from_response_json(
        cls,
        json_dict: dict[str, Any],
        storage_connector: sc.StorageConnector | None = None,
    ) -> DataSource | list[DataSource] | None:
        """Create a DataSource object (or list of objects) from a JSON response.

        Parameters:
            json_dict: The JSON dictionary from the API response.
            storage_connector: The storage connector object.

        Returns:
            The created object(s), or None if input is None.
        """
        if json_dict is None:
            return None  # TODO: change to [] and fix the tests

        json_decamelized: dict = humps.decamelize(json_dict)

        if "items" not in json_decamelized:
            data_source = cls(**json_decamelized)
            if storage_connector is not None:
                data_source.storage_connector = storage_connector
            return data_source

        return [
            DataSource.from_response_json(item, storage_connector)
            for item in json_decamelized["items"]
        ]

    def to_dict(self) -> dict:
        """Convert the DataSource object to a dictionary.

        Returns:
            Dictionary representation of the object.
        """
        ds_meta_dict = {
            "query": self._query,
            "database": self._database,
            "group": self._group,
            "table": self._table,
            "path": self._path,
            "format": self._format,
            "metrics": self._metrics,
            "dimensions": self._dimensions,
            "restEndpoint": (
                self._rest_endpoint.to_dict() if self._rest_endpoint else None
            ),
            "spreadsheetId": self._spreadsheet_id,
        }
        if self._storage_connector:
            ds_meta_dict["storageConnector"] = self._storage_connector.to_dict()
        return ds_meta_dict

    def json(self) -> str:
        """Serialize the DataSource object to a JSON string.

        Returns:
            JSON string representation of the object.
        """
        return json.dumps(self, cls=util.Encoder)

    @public
    @property
    def query(self) -> str | None:
        """Get or set the SQL query string for the data source."""
        return self._query

    @query.setter
    def query(self, query: str) -> None:
        self._query = query

    @public
    @property
    def database(self) -> str | None:
        """Get or set the database name for the data source."""
        return self._database

    @database.setter
    def database(self, database: str) -> None:
        self._database = database

    @public
    @property
    def group(self) -> str | None:
        """Get or set the group/schema name for the data source."""
        return self._group

    @group.setter
    def group(self, group: str) -> None:
        self._group = group

    @public
    @property
    def table(self) -> str | None:
        """Get or set the table name for the data source.

        For Google Sheets connectors this is the sheet tab name.
        """
        return self._table

    @table.setter
    def table(self, table: str) -> None:
        self._table = table

    @public
    @property
    def path(self) -> str | None:
        """Get or set the file system path for the data source."""
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    @public
    @property
    def format(self) -> str | None:
        """Get or set the format of the data source."""
        return self._format

    @format.setter
    def format(self, format: str) -> None:
        self._format = format

    @public
    @property
    def storage_connector(self) -> sc.StorageConnector | None:
        """Get or set the storage connector for the data source."""
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector: sc.StorageConnector) -> None:
        self._storage_connector = storage_connector

    @public
    def get_databases(self) -> list[str]:
        """Retrieve the list of available databases.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            data_source = fs.get_data_source("test_data_source")

            databases = data_source.get_databases()
            ```

        Returns:
            A list of database names available in the data source.
        """
        return self._storage_connector.get_databases()

    @public
    def get_tables(self, database: str | None = None) -> list[DataSource]:
        """Retrieve the list of tables from the specified database.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            data_source = fs.get_data_source("test_data_source")

            tables = data_source.get_tables()
            ```

        Parameters:
            database:
                The name of the database to list tables from.
                If not provided, the default database is used.

        Returns:
            A list of DataSource objects representing the tables.
        """
        return self._storage_connector.get_tables(database)

    @public
    def get_data(self, use_cached: bool = True) -> dsd.DataSourceData:
        """Retrieve the data from the data source.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            table = fs.get_data_source("test_data_source").get_tables()[0]

            data = table.get_data()
            ```

        Parameters:
            use_cached:
                Whether to use cached data if available.
                Only supported for CRM, Google Sheets, and REST connectors.
                Defaults to `True`.

        Returns:
            An object containing the data retrieved from the data source.
        """
        return self._storage_connector.get_data(self, use_cached=use_cached)

    @public
    def get_metadata(self) -> dict:
        """Retrieve metadata information about the data source.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            table = fs.get_data_source("test_data_source").get_tables()[0]

            metadata = table.get_metadata()
            ```

        Returns:
            A dictionary containing metadata about the data source.
        """
        return self._storage_connector.get_metadata(self)

    @public
    def infer_metadata(
        self,
        preview_data: dsd.DataSourceData | None = None,
    ) -> InferredMetadata:
        """Use platform intelligence to infer feature metadata for this data source.

        Calls the same backend used by the "Infer metadata" button in the UI
        when creating an external feature group: an LLM proposes per-column
        renames, Hopsworks types, descriptions, and a suggested primary key
        and event time.

        Example:
            ```python
            fs = ...

            table = fs.get_data_source("test_data_source").get_tables()[0]

            inferred = table.infer_metadata()
            for f in inferred.features:
                print(f.original_name, "->", f.new_name, f.type, f.description)
            print("primary key:", inferred.suggested_primary_key)
            print("event time:", inferred.suggested_event_time)
            ```

        Parameters:
            preview_data: Pre-fetched preview data to skip a server round-trip; if `None`, a preview is fetched via `get_data`.

        Returns:
            An object containing the suggested feature renames, types, descriptions, primary key, and event time.

        Raises:
            hopsworks.client.exceptions.PlatformIntelligenceException: If platform intelligence is not enabled on the cluster, or the LLM call fails.
        """
        return self._storage_connector.infer_metadata(self, preview_data=preview_data)

    @public
    def create_ingestion_job(
        self,
        name: str,
        targets: Sequence[TableIngestionTarget],
        table_parallelism: int = 1,
        environment_name: str | None = None,
        transform_script_path: str | None = None,
        write_mode: str | None = None,
        batch_size: int | None = None,
        sql_source_fetch_chunk_size: int | None = None,
        source_read_workers: int | None = None,
        data_processing_workers: int | None = None,
        max_upload_batch_size_mb: int | None = None,
        sql_table_num_partitions: int | None = None,
        schedule_config: JobSchedule | dict | None = None,
    ) -> job.Job:
        """Create one ingestion job that copies several tables of this data source.

        Each target table is copied by its own worker pod into its feature group,
        running up to `table_parallelism` tables at a time.
        This is the multi-table counterpart of the per-feature-group sink job created
        when a feature group is saved with `sink_enabled=True`.

        The job-level arguments are the defaults applied to every target that does
        not override them on its [`TableIngestionTarget`][hopsworks.core.sink_job_configuration.TableIngestionTarget].

        Example:
            ```python
            fs = ...
            data_source = fs.get_data_source("hubspot")

            contacts_fg = fs.get_or_create_feature_group("contacts", version=1, ...)
            companies_fg = fs.get_or_create_feature_group("companies", version=1, ...)

            from hopsworks.core.sink_job_configuration import TableIngestionTarget

            job = data_source.create_ingestion_job(
                name="hubspot_ingestion",
                table_parallelism=2,
                targets=[
                    TableIngestionTarget(contacts_fg),
                    TableIngestionTarget(companies_fg, resource_config={"cores": 2, "memory": 4096}),
                ],
            )
            job.run()
            ```

        Parameters:
            name: Name of the ingestion job to create.
            targets: The tables to ingest, one [`TableIngestionTarget`][hopsworks.core.sink_job_configuration.TableIngestionTarget] per source table -> feature group.
            table_parallelism: How many tables run at the same time; `1` runs them sequentially.
            environment_name: Python environment the job runs in.
            transform_script_path: Default transformation script path for targets that do not set their own.
            write_mode: Default write mode (`APPEND` or `MERGE`) for targets that do not set their own.
            batch_size: Default write batch size.
            sql_source_fetch_chunk_size: Default source fetch chunk size for SQL sources.
            source_read_workers: Default number of source read workers.
            data_processing_workers: Default number of data processing workers.
            max_upload_batch_size_mb: Default maximum upload batch size in MB.
            sql_table_num_partitions: Default number of read partitions for SQL sources.
            schedule_config: Optional schedule for the job.

        Returns:
            The created ingestion job.
        """
        builder = self.new_ingestion_job(
            name,
            table_parallelism=table_parallelism,
            environment_name=environment_name,
            transform_script_path=transform_script_path,
            write_mode=write_mode,
            batch_size=batch_size,
            sql_source_fetch_chunk_size=sql_source_fetch_chunk_size,
            source_read_workers=source_read_workers,
            data_processing_workers=data_processing_workers,
            max_upload_batch_size_mb=max_upload_batch_size_mb,
            sql_table_num_partitions=sql_table_num_partitions,
            schedule_config=schedule_config,
        )
        for target in targets:
            builder._add_target_object(target)
        return builder.save()

    @public
    def new_ingestion_job(
        self,
        name: str,
        *,
        table_parallelism: int = 1,
        environment_name: str | None = None,
        transform_script_path: str | None = None,
        write_mode: str | None = None,
        batch_size: int | None = None,
        sql_source_fetch_chunk_size: int | None = None,
        source_read_workers: int | None = None,
        data_processing_workers: int | None = None,
        max_upload_batch_size_mb: int | None = None,
        sql_table_num_partitions: int | None = None,
        schedule_config: JobSchedule | dict | None = None,
    ) -> MultiTableIngestionJob:
        """Start assembling a multi-table ingestion job for this data source.

        Returns an empty [`MultiTableIngestionJob`][hsfs.core.multi_table_ingestion.MultiTableIngestionJob]
        you attach feature groups to, either by passing it as `sink_job` when creating
        each feature group or by calling
        [`MultiTableIngestionJob.add_target`][hsfs.core.multi_table_ingestion.MultiTableIngestionJob.add_target].
        Nothing is created on the server until you call
        [`MultiTableIngestionJob.save`][hsfs.core.multi_table_ingestion.MultiTableIngestionJob.save], so the
        job is built atomically from the full set of targets.

        The arguments here are the job-level defaults every target inherits unless it
        overrides them.

        Example:
            ```python
            fs = ...
            data_source = fs.get_data_source("hubspot")

            job = data_source.new_ingestion_job(name="hubspot_ingestion", table_parallelism=2)

            fs.get_or_create_feature_group(
                "contacts", version=1, data_source=data_source,
                sink_enabled=True, sink_job=job,
            ).save()
            fs.get_or_create_feature_group(
                "companies", version=1, data_source=data_source,
                sink_enabled=True, sink_job=job,
            ).save()

            job.save()   # creates one job with both feature groups as targets
            job.run()
            ```

        Parameters:
            name: Name of the ingestion job to create.
            table_parallelism: How many tables run at the same time; `1` runs them sequentially.
            environment_name: Python environment the job runs in.
            transform_script_path: Default transformation script path for targets that do not set their own.
            write_mode: Default write mode (`APPEND` or `MERGE`) for targets that do not set their own.
            batch_size: Default write batch size.
            sql_source_fetch_chunk_size: Default source fetch chunk size for SQL sources.
            source_read_workers: Default number of source read workers.
            data_processing_workers: Default number of data processing workers.
            max_upload_batch_size_mb: Default maximum upload batch size in MB.
            sql_table_num_partitions: Default number of read partitions for SQL sources.
            schedule_config: Optional schedule for the job.

        Returns:
            An empty ingestion job to collect targets on.
        """
        from hsfs.core.multi_table_ingestion import MultiTableIngestionJob

        if self._storage_connector is None:
            raise ValueError("The data source has no storage connector to ingest from.")

        return MultiTableIngestionJob(
            self,
            name,
            table_parallelism=table_parallelism,
            environment_name=environment_name,
            transform_script_path=transform_script_path,
            write_mode=write_mode,
            batch_size=batch_size,
            sql_source_fetch_chunk_size=sql_source_fetch_chunk_size,
            source_read_workers=source_read_workers,
            data_processing_workers=data_processing_workers,
            max_upload_batch_size_mb=max_upload_batch_size_mb,
            sql_table_num_partitions=sql_table_num_partitions,
            schedule_config=schedule_config,
        )

    @public
    def estimate_ingestion_resources(
        self,
        feature_group: fg.FeatureGroup | None = None,
        *,
        feature_group_id: int | None = None,
        write_mode: str | None = None,
        loading_strategy: str | None = None,
        batch_size: int | None = None,
        sql_source_fetch_chunk_size: int | None = None,
        source_read_workers: int | None = None,
        data_processing_workers: int | None = None,
        max_upload_batch_size_mb: int | None = None,
        sql_table_num_partitions: int | None = None,
        transform_script_path: str | None = None,
        configured_memory_mb: int | None = None,
        configured_cpu_cores: float | None = None,
    ) -> dict:
        """Estimate the memory and CPU an ingestion into a feature group needs.

        Calls the same backend the UI uses to size a DLTHub ingestion job.
        It derives a recommendation from the feature group's schema and the runtime
        knobs you pass, so you can set `resource_config` on a
        [`TableIngestionTarget`][hopsworks.core.sink_job_configuration.TableIngestionTarget]
        (or the worker resources of a single sink job) before running an ingestion,
        instead of guessing.
        Pass the same runtime knobs you intend to run the ingestion with, since a
        larger batch size or more workers raises the estimate.

        Example:
            ```python
            fs = ...
            data_source = fs.get_data_source("hubspot")
            contacts_fg = fs.get_feature_group("contacts", version=1)

            estimate = data_source.estimate_ingestion_resources(
                contacts_fg,
                write_mode="MERGE",
                batch_size=200000,
            )
            print(estimate["recommendedMemoryMb"], estimate["recommendedCpuCores"])
            ```

        Parameters:
            feature_group: The feature group the ingestion writes to; supplies the schema the estimate is based on.
            feature_group_id: Id of the target feature group, as an alternative to passing `feature_group`.
            write_mode: Write mode the ingestion will run with (`APPEND` or `MERGE`).
            loading_strategy: Loading strategy the ingestion will run with.
            batch_size: Write batch size the ingestion will run with.
            sql_source_fetch_chunk_size: Source fetch chunk size for SQL sources.
            source_read_workers: Number of source read workers.
            data_processing_workers: Number of data processing workers.
            max_upload_batch_size_mb: Maximum upload batch size in MB.
            sql_table_num_partitions: Number of read partitions for SQL sources.
            transform_script_path: Path of a transformation script the ingestion will run.
            configured_memory_mb: Memory you plan to give the job, to compare against the recommendation.
            configured_cpu_cores: CPU cores you plan to give the job, to compare against the recommendation.

        Returns:
            The backend estimation, including `recommendedMemoryMb`, `recommendedCpuCores`, `confidence`, `peakStage`, `reasons`, and `warnings`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        from hsfs.core.data_source_api import DataSourceApi

        if self._storage_connector is None:
            raise ValueError("The data source has no storage connector to ingest from.")

        resolved_id = feature_group_id
        if resolved_id is None and feature_group is not None:
            resolved_id = feature_group.id
        if resolved_id is None:
            raise ValueError(
                "Pass either a feature_group or a feature_group_id to estimate for."
            )

        payload = {
            "featuregroupId": resolved_id,
            "featurestoreId": self._storage_connector._featurestore_id,
        }
        optional = {
            "writeMode": write_mode,
            "loadingStrategy": loading_strategy,
            "batchSize": batch_size,
            "sqlSourceFetchChunkSize": sql_source_fetch_chunk_size,
            "sourceReadWorkers": source_read_workers,
            "dataProcessingWorkers": data_processing_workers,
            "maxUploadBatchSizeMB": max_upload_batch_size_mb,
            "sqlTableNumPartitions": sql_table_num_partitions,
            "transformScriptPath": transform_script_path,
            "configuredMemoryMb": configured_memory_mb,
            "configuredCpuCores": configured_cpu_cores,
        }
        payload.update(
            {key: value for key, value in optional.items() if value is not None}
        )

        return DataSourceApi()._estimate_ingestion_resources(
            self._storage_connector, payload
        )

    @public
    def get_feature_groups_provenance(self) -> Links | None:
        """Get the generated feature groups using this data source, based on explicit provenance.

        These feature groups can be accessible or inaccessible.
        Explicit provenance does not track deleted generated feature group links, so deleted will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        Returns:
            The feature groups generated using this data source or `None` if none were created.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        return self._storage_connector.get_feature_groups_provenance()

    @public
    def get_feature_groups(self) -> list[fg.FeatureGroup]:
        """Get the feature groups using this data source, based on explicit provenance.

        Only the accessible feature groups are returned.
        For more items use the base method, [`DataSource.get_feature_groups_provenance`][hsfs.core.data_source.DataSource.get_feature_groups_provenance].

        Returns:
            List of feature groups.
        """
        return self._storage_connector.get_feature_groups()

    @public
    def get_training_datasets_provenance(self) -> Links:
        """Get the generated training datasets using this data source, based on explicit provenance.

        These training datasets can be accessible or inaccessible.
        Explicit provenance does not track deleted generated training dataset links, so deleted will always be empty.
        For inaccessible training datasets, only a minimal information is returned.

        Returns:
            The training datasets generated using this data source or `None` if none were created.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        return self._storage_connector.get_training_datasets_provenance()

    @public
    def get_training_datasets(self) -> list[TrainingDataset]:
        """Get the training datasets using this data source, based on explicit provenance.

        Only the accessible training datasets are returned.
        For more items use the base method, [`get_training_datasets_provenance`][hsfs.core.data_source.DataSource.get_training_datasets_provenance].

        Returns:
            List of training datasets.
        """
        return self._storage_connector.get_training_datasets()

    @public
    @property
    def metrics(self) -> list[str]:
        return self._metrics

    @metrics.setter
    def metrics(self, metrics: list[str]) -> None:
        self._metrics = metrics

    @public
    @property
    def dimensions(self) -> list[str]:
        return self._dimensions

    @dimensions.setter
    def dimensions(self, dimensions: list[str]) -> None:
        self._dimensions = dimensions

    @public
    @property
    def spreadsheet_id(self) -> str | None:
        """Get or set the Google Spreadsheet ID for Google Sheets data sources.

        Only required when the spreadsheet ID was not configured on the connector itself.
        The connector-level value is used when this is not set.
        """
        return self._spreadsheet_id

    @spreadsheet_id.setter
    def spreadsheet_id(self, spreadsheet_id: str) -> None:
        self._spreadsheet_id = spreadsheet_id

    @public
    @property
    def rest_endpoint(self) -> RestEndpointConfig | None:
        return self._rest_endpoint

    @rest_endpoint.setter
    def rest_endpoint(self, rest_endpoint: RestEndpointConfig) -> None:
        self._rest_endpoint = rest_endpoint

    def _update_storage_connector(self, storage_connector: sc.StorageConnector):
        """Update the storage connector configuration using DataSource.

        This internal method updates the connectors target database, schema,
        and table to match the information stored in the provided DataSource object.

        Parameters:
            storage_connector: A StorageConnector instance to be updated depending on the connector type.
        """
        if not storage_connector:
            return

        if storage_connector.type == sc.StorageConnector.REDSHIFT:
            if self.database:
                storage_connector._database_name = self.database
            if self.group:
                storage_connector._database_group = self.group
            if self.table:
                storage_connector._table_name = self.table
        if storage_connector.type == sc.StorageConnector.SNOWFLAKE:
            if self.database:
                storage_connector._database = self.database
            if self.group:
                storage_connector._schema = self.group
            if self.table:
                storage_connector._table = self.table
        if storage_connector.type == sc.StorageConnector.BIGQUERY:
            if self.database:
                storage_connector._query_project = self.database
            if self.group:
                storage_connector._dataset = self.group
            if self.table:
                storage_connector._query_table = self.table
        if storage_connector.type == sc.StorageConnector.SQL and self.database:
            storage_connector._database = self.database
        if storage_connector.type == sc.StorageConnector.MONGODB:
            if self.database:
                storage_connector._database = self.database
            if self.table:
                storage_connector._collection = self.table

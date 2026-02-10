#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in
#   writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, TypeVar

import humps
from hsfs import (
    expectation_suite,
    feature,
    feature_group,
    feature_view,
    storage_connector,
    tag,
    training_dataset,
    usage,
    util,
)
from hsfs.core import (
    data_source as ds,
)
from hsfs.core import (
    feature_group_api,
    feature_group_engine,
    feature_view_engine,
    search_api,
    storage_connector_api,
    training_dataset_api,
    transformation_function_engine,
)
from hsfs.core.chart import Chart
from hsfs.core.chart_api import ChartApi
from hsfs.core.dashboard import Dashboard
from hsfs.core.dashboard_api import DashboardApi
from hsfs.core.job import Job
from hsfs.decorators import typechecked
from hsfs.transformation_function import TransformationFunction


if TYPE_CHECKING:
    from hopsworks_common.core.constants import HAS_NUMPY, HAS_POLARS

    if HAS_NUMPY:
        import numpy as np
    if HAS_POLARS:
        import polars as pl

    import datetime
    from datetime import timedelta

    import pandas as pd
    from hsfs.constructor.query import Query
    from hsfs.embedding import EmbeddingIndex
    from hsfs.hopsworks_udf import HopsworksUdf
    from hsfs.online_config import OnlineConfig
    from hsfs.statistics_config import StatisticsConfig


@typechecked
class FeatureStore:
    """Feature Store class used to manage feature store entities, like feature groups and feature views."""

    DEFAULT_VERSION = 1

    def __init__(
        self,
        featurestore_id: int,
        featurestore_name: str,
        created: str | datetime.datetime,
        project_name: str,
        project_id: int,
        offline_featurestore_name: str,
        online_enabled: bool,
        num_feature_groups: int | None = None,
        num_training_datasets: int | None = None,
        num_storage_connectors: int | None = None,
        num_feature_views: int | None = None,
        online_featurestore_name: str | None = None,
        online_featurestore_size: int | None = None,
        **kwargs,
    ) -> None:
        self._id = featurestore_id
        self._name = featurestore_name
        self._created = created
        self._project_name = project_name
        self._project_id = project_id
        self._online_feature_store_name = online_featurestore_name
        self._online_feature_store_size = online_featurestore_size
        self._offline_feature_store_name = offline_featurestore_name
        self._online_enabled = online_enabled
        self._num_feature_groups = num_feature_groups
        self._num_training_datasets = num_training_datasets
        self._num_storage_connectors = num_storage_connectors
        self._num_feature_views = num_feature_views

        self._feature_group_api: feature_group_api.FeatureGroupApi = (
            feature_group_api.FeatureGroupApi()
        )
        self._storage_connector_api: storage_connector_api.StorageConnectorApi = (
            storage_connector_api.StorageConnectorApi()
        )
        self._training_dataset_api: training_dataset_api.TrainingDatasetApi = (
            training_dataset_api.TrainingDatasetApi(self._id)
        )

        self._feature_group_engine: feature_group_engine.FeatureGroupEngine = (
            feature_group_engine.FeatureGroupEngine(self._id)
        )

        self._transformation_function_engine: transformation_function_engine.TransformationFunctionEngine = transformation_function_engine.TransformationFunctionEngine(
            self._id
        )
        self._feature_view_engine: feature_view_engine.FeatureViewEngine = (
            feature_view_engine.FeatureViewEngine(self._id)
        )
        self._search_api: search_api.SearchApi = search_api.SearchApi()

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> FeatureStore:
        json_decamelized = humps.decamelize(json_dict)
        # fields below are removed from 3.4. remove them for backward compatibility.
        json_decamelized.pop("hdfs_store_path", None)
        json_decamelized.pop("featurestore_description", None)
        json_decamelized.pop("inode_id", None)
        return cls(**json_decamelized)

    def get_feature_group(
        self, name: str, version: int = None
    ) -> (
        feature_group.FeatureGroup
        | feature_group.ExternalFeatureGroup
        | feature_group.SpineGroup
    ):
        """Get a feature group entity from the feature store.

        Getting a feature group from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            fg = fs.get_feature_group(
                    name="electricity_prices",
                    version=1,
                )
            ```

        Parameters:
            name: Name of the feature group to get.
            version: Version of the feature group to retrieve, defaults to `None` and will return the `version=1`.

        Returns:
            The feature group metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if version is None:
            warnings.warn(
                f"No version provided for getting feature group `{name}`, defaulting to `{self.DEFAULT_VERSION}`.",
                util.VersionWarning,
                stacklevel=1,
            )
            version = self.DEFAULT_VERSION
        feature_group_object = self._feature_group_api.get(self.id, name, version)
        if feature_group_object:
            feature_group_object.feature_store = self
            util.check_missing_mandatory_tags(
                feature_group_object.missing_mandatory_tags
            )
        return feature_group_object

    def get_feature_groups(
        self, name: str | None = None
    ) -> list[
        feature_group.FeatureGroup
        | feature_group.ExternalFeatureGroup
        | feature_group.SpineGroup
    ]:
        """Get all feature groups from the feature store, or all versions of a feature group specified by its name.

        Getting a feature group from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # retrieve all versions of electricity_prices feature group
            fgs_list = fs.get_feature_groups(
                    name="electricity_prices"
                )
            ```

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # retrieve all feature groups available in the feature store
            fgs_list = fs.get_feature_groups()
            ```

        Parameters:
            name: Name of the feature group to get the versions of; by default it is `None` and all feature groups are returned.

        Returns:
            List of feature group metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if name:
            feature_group_object = self._feature_group_api.get(self.id, name, None)
        else:
            feature_group_object = self._feature_group_api.get_all(self.id)
        for fg_object in feature_group_object:
            fg_object.feature_store = self
        return feature_group_object

    @usage.method_logger
    def get_on_demand_feature_group(
        self, name: str, version: int = None
    ) -> feature_group.ExternalFeatureGroup:
        """Get an external feature group entity from the feature store.

        Warning: Deprecated
            `get_on_demand_feature_group` method is deprecated.
            Use the `get_external_feature_group` method instead.

        Getting an external feature group from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        Parameters:
            name: Name of the external feature group to get.
            version: Version of the external feature group to retrieve, defaults to `None` and will return the `version=1`.

        Returns:
            The external feature group metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self.get_external_feature_group(name, version)

    @usage.method_logger
    def get_external_feature_group(
        self, name: str, version: int = None
    ) -> feature_group.ExternalFeatureGroup:
        """Get an external feature group entity from the feature store.

        Getting an external feature group from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            external_fg = fs.get_external_feature_group("external_fg_test")
            ```

        Parameters:
            name: Name of the external feature group to get.
            version: Version of the external feature group to retrieve, by defaults to `None` and will return the `version=1`.

        Returns:
            The external feature group metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if version is None:
            warnings.warn(
                f"No version provided for getting feature group `{name}`, defaulting to `{self.DEFAULT_VERSION}`.",
                util.VersionWarning,
                stacklevel=1,
            )
            version = self.DEFAULT_VERSION
        feature_group_object = self._feature_group_api.get(
            self.id,
            name,
            version,
        )
        if feature_group_object:
            feature_group_object.feature_store = self
            util.check_missing_mandatory_tags(
                feature_group_object.missing_mandatory_tags
            )
        return feature_group_object

    @usage.method_logger
    def get_on_demand_feature_groups(
        self, name: str
    ) -> list[feature_group.ExternalFeatureGroup]:
        """Get a list of all versions of an external feature group entity from the feature store.

        Warning: Deprecated
            `get_on_demand_feature_groups` method is deprecated.
            Use the `get_external_feature_groups` method instead.

        Getting an external feature group from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        Parameters:
            name: Name of the external feature group to get.

        Returns:
            List of external feature group metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self.get_external_feature_groups(name)

    @usage.method_logger
    def get_external_feature_groups(
        self, name: str | None = None
    ) -> list[feature_group.ExternalFeatureGroup]:
        """Get a list of all external feature groups from the feature store, or all versions of an external feature group.

        Getting an external feature group from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            external_fgs_list = fs.get_external_feature_groups("external_fg_test")
            ```

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # retrieve all external feature groups available in the feature store
            external_fgs_list = fs.get_external_feature_groups()
            ```

        Parameters:
            name: Name of the external feature group to get the versions of; by default it is `None` and all external feature groups are returned.

        Returns:
            List of external feature group metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        fgs = self.get_feature_groups(name)
        return [fg for fg in fgs if isinstance(fg, feature_group.ExternalFeatureGroup)]

    def get_training_dataset(
        self, name: str, version: int = None
    ) -> training_dataset.TrainingDataset:
        """Get a training dataset entity from the feature store.

        Warning: Deprecated
            `TrainingDataset` is deprecated, use `FeatureView` instead.
            You can still retrieve old training datasets using this method, but after upgrading the old training datasets will also be available under a Feature View with the same name and version.

            It is recommended to use this method only for old training datasets that have been created directly from Dataframes and not with Query objects.

        Getting a training dataset from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame.

        Parameters:
            name: Name of the training dataset to get.
            version: Version of the training dataset to retrieve, defaults to `None` and will return the `version=1`.

        Returns:
            The training dataset metadata object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if version is None:
            warnings.warn(
                f"No version provided for getting training dataset `{name}`, defaulting to `{self.DEFAULT_VERSION}`.",
                util.VersionWarning,
                stacklevel=1,
            )
            version = self.DEFAULT_VERSION
        training_dataset_object = self._training_dataset_api.get(name, version)
        if training_dataset_object:
            util.check_missing_mandatory_tags(
                training_dataset_object.missing_mandatory_tags
            )
        return training_dataset_object

    def get_training_datasets(
        self, name: str
    ) -> list[training_dataset.TrainingDataset]:
        """Get a list of all versions of a training dataset entity from the feature store.

        Warning: Deprecated
            `TrainingDataset` is deprecated, use `FeatureView` instead.

        Getting a training dataset from the Feature Store means getting its metadata handle so you can subsequently read the data into a Spark or Pandas DataFrame.

        Parameters:
            name: Name of the training dataset to get.

        Returns:
            List of training dataset metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._training_dataset_api.get(name, None)

    @usage.method_logger
    def get_storage_connector(self, name: str) -> storage_connector.StorageConnector:
        """Get a previously created storage connector from the feature store.

        Storage connectors encapsulate all information needed for the execution engine to read and write to specific storage.
        This storage can be S3, a JDBC compliant database or the distributed filesystem HOPSFS.

        If you want to connect to the online feature store, see the `get_online_storage_connector` method to get the JDBC connector for the Online Feature Store.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            sc = fs.get_storage_connector("demo_fs_meb10000_Training_Datasets")
            ```

        Parameters:
            name: Name of the storage connector to retrieve.

        Returns:
            Storage connector object.
        """
        return self.get_data_source(name).storage_connector

    @usage.method_logger
    def get_data_source(self, name: str) -> ds.DataSource:
        """Get a data source from the feature store.

        Data sources encapsulate all information needed for the execution engine
        to read and write to specific storage.

        If you want to connect to the online feature store, see the
        `get_online_data_source` method to get the JDBC connector for the Online
        Feature Store.

        Warning "Deprecated"
                    `get_storage_connector` method is deprecated. Use `get_data_source` instead.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            data_source = fs.get_data_source("test_data_source")
            ```

        Arguments:
            name: Name of the data source to retrieve.

        Returns:
            `DataSource`. Data source object.
        """
        return ds.DataSource(
            storage_connector=self._storage_connector_api.get(self._id, name)
        )

    def sql(
        self,
        query: str,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
        online: bool = False,
        read_options: dict | None = None,
    ) -> pd.DataFrame | pd.Series | np.ndarray | pl.DataFrame:
        """Execute SQL command on the offline or online feature store database.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # construct the query and show head rows
            query_res_head = fs.sql("SELECT * FROM `fg_1`").head()
            ```

        Parameters:
            query: The SQL query to execute.
            dataframe_type:
                The type of the returned dataframe.
                Defaults to `"default"`, which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            online: Set to true to execute the query against the online feature store.
            read_options:
                Additional options as key/value pairs to pass to the execution engine.

                For spark engine: Dictionary of read options for Spark.

                For python engine:
                If running queries on the online feature store, users can provide an entry `{'external': True}`, this instructs the library to use the `host` parameter in the [`hopsworks.login`][hopsworks.login] to establish the connection to the online feature store.
                If not set, or set to False, the online feature store storage connector is used which relies on the private ip.

        Returns:
            DataFrame depending on the chosen type.
        """
        return self._feature_group_engine.sql(
            query, self._name, dataframe_type, online, read_options or {}
        )

    @usage.method_logger
    def get_online_storage_connector(self) -> storage_connector.StorageConnector:
        """Get the storage connector for the Online Feature Store of the respective project's feature store.

        The returned storage connector depends on the project that you are connected to.

        Warning "Deprecated"
                    `get_online_storage_connector` method is deprecated. Use `get_online_data_source` instead.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            online_storage_connector = fs.get_online_storage_connector()
            ```

        Returns:
            JDBC storage connector to the Online Feature Store.
        """
        return self.get_online_data_source().storage_connector

    @usage.method_logger
    def get_online_data_source(self) -> ds.DataSource:
        """Get the data source for the Online Feature Store of the respective project's feature store.

        The returned data source depends on the project that you are connected to.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            online_data_source = fs.get_online_data_source()
            ```

        Returns:
            `DataSource`. JDBC data source to the Online Feature Store.
        """
        return ds.DataSource(
            storage_connector=self._storage_connector_api.get_online_connector(self._id)
        )

    def _normalize_tags(
        self,
        tags: tag.Tag | dict[str, Any] | list[tag.Tag | dict[str, Any]] | None,
    ) -> list[tag.Tag]:
        """Normalize tags input to a list of Tag objects.

        # Arguments
            tags: Tags in various formats (single Tag, dict, or list of Tags/dicts)

        # Returns
            `list[tag.Tag]`: List of Tag objects.
        """
        return tag.Tag.normalize(tags)

    @usage.method_logger
    def create_feature_group(
        self,
        name: str,
        version: int | None = None,
        description: str = "",
        online_enabled: bool = False,
        time_travel_format: str | None = None,
        partition_key: list[str] | None = None,
        primary_key: list[str] | None = None,
        foreign_key: list[str] | None = None,
        embedding_index: EmbeddingIndex | None = None,
        hudi_precombine_key: str | None = None,
        features: list[feature.Feature] | None = None,
        statistics_config: StatisticsConfig | bool | dict | None = None,
        event_time: str | None = None,
        stream: bool = False,
        expectation_suite: expectation_suite.ExpectationSuite
        | TypeVar("great_expectations.core.ExpectationSuite")
        | None = None,
        parents: list[feature_group.FeatureGroup] | None = None,
        topic_name: str | None = None,
        notification_topic_name: str | None = None,
        transformation_functions: list[TransformationFunction | HopsworksUdf]
        | None = None,
        online_config: OnlineConfig | dict[str, Any] | None = None,
        offline_backfill_every_hr: int | str | None = None,
        storage_connector: storage_connector.StorageConnector | dict[str, Any] = None,
        path: str | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        ttl: float | timedelta | None = None,
        ttl_enabled: bool | None = None,
        online_disk: bool | None = None,
        tags: tag.Tag | dict[str, Any] | list[tag.Tag | dict[str, Any]] | None = None,
    ) -> feature_group.FeatureGroup:
        """Create a feature group metadata object.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # define the on-demand transformation functions
            @udf(int)
            def plus_one(value):
                return value + 1

            @udf(int)
            def plus_two(value):
                return value + 2

            # construct list of "transformation functions" on features
            transformation_functions = [plus_one("feature1"), plus_two("feature2")]

            fg = fs.create_feature_group(
                name='air_quality',
                description='Air Quality characteristics of each day',
                version=1,
                primary_key=['city','date'],
                online_enabled=True,
                event_time='date',
                transformation_functions=transformation_functions,
                online_config={'online_comments': ['NDB_TABLE=READ_BACKUP=1']},
                online_disk=True,  # Online data will be stored on disk instead of in memory
                ttl=timedelta(days=7)  # features will be deleted after 7 days
            )
            ```

        Note: Lazy
            This method is lazy and does not persist any metadata or feature data in the feature store on its own.
            To persist the feature group and save feature data along the metadata in the feature store, call the `save()` method with a DataFrame.

        Parameters:
            name: Name of the feature group to create.
            version: Version of the feature group to create, defaults to `None` and will create the feature group with incremented version from the last version in the feature store.
            description: A string describing the contents of the feature group to improve discoverability for Data Scientists.
            online_enabled: Define whether the feature group should be made available also in the online feature store for low latency access.
            time_travel_format: Format used for time travel, defaults to `"HUDI"`.
            partition_key: A list of feature names to be used as partition key when writing the feature data to the offline storage, defaults to empty list `[]`.
            primary_key:
                A list of feature names to be used as primary key for the feature group.
                This primary key can be a composite key of multiple features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the feature group won't have any primary key.
            foreign_key:
                A list of feature names to be used as foreign key for the feature group.
                Foreign key is referencing the primary key of another feature group and can be used as joining key.
                Defaults to empty list `[]`, and the feature group won't have any foreign key.
            embedding_index:
                [`EmbeddingIndex`][hsfs.embedding.EmbeddingIndex]. If an embedding index is provided, vector database is used as online feature store.
                This enables similarity search by using [`FeatureGroup.find_neighbors`][hsfs.feature_group.FeatureGroup.find_neighbors].
            hudi_precombine_key:
                A feature name to be used as a precombine key for the `"HUDI"` feature group.
                If feature group has time travel format `"HUDI"` and hudi precombine key was not specified then the first primary key of the feature group will be used as hudi precombine key.
            features:
                Optionally, define the schema of the feature group manually as a list of `Feature` objects.
                Defaults to empty list `[]` and will use the schema information of the DataFrame provided in the `save` method.
            statistics_config:
                A configuration object, or a dictionary with keys:

                - `enabled` to generally enable descriptive statistics computation for this feature group,
                - `correlations` to turn on feature correlation computation,
                - `histograms` to compute feature value frequencies, and
                - `exact_uniqueness` to compute uniqueness, distinctness and entropy.

                The values should be booleans indicating the setting.
                To fully turn off statistics computation pass `statistics_config=False`.
                By default, it computes only descriptive statistics.
            event_time:
                Optionally, provide the name of the feature containing the event time for the features in this feature group.
                If event_time is set the feature group can be used for point-in-time joins.

                Note: Event time data type restriction
                    The supported data types for the event time column are: `timestamp`, `date` and `bigint`.

            stream:
                Optionally, define whether the feature group should support real time stream writing capabilities.
                Stream enabled Feature Groups have unified single API for writing streaming features transparently to both online and offline store.
            expectation_suite:
                Optionally, attach an expectation suite to the feature group which dataframes should be validated against upon insertion.
            parents:
                Optionally, define the parents of this feature group as the origin where the data is coming from.
            topic_name:
                Optionally, define the name of the topic used for data ingestion.
                If left undefined it defaults to using project topic.
            notification_topic_name:
                Optionally, define the name of the topic used for sending notifications when entries are inserted or updated on the online feature store.
                If left undefined no notifications are sent.
            transformation_functions:
                On-Demand Transformation functions attached to the feature group.
                It can be a list of list of user defined functions defined using the hopsworks `@udf` decorator.
                Defaults to `None`, no transformations.
            online_config: Optionally, define configuration which is used to configure online table.
            offline_backfill_every_hr:
                If specified, the materialization job will be scheduled to run periodically.
                The value can be either an integer representing the number of hours between each run or a string representing a cron expression.
                Set the value to None to avoid scheduling the materialization job.
                By default, no scheduling is done.
            storage_connector: The storage connector used to establish connectivity with the data source. **[DEPRECATED: Use `data_source` instead.]**
            path: The location within the scope of the storage connector, from where to read the data for the external feature group. **[DEPRECATED: Use `data_source` instead.]**
            data_source:
                The data source specifying the location of the data.
                Overrides the path and query arguments when specified.
            ttl:
                Optional time-to-live duration for features in this group.
                Can be specified as:

                - An integer or float representing seconds
                - A timedelta object

                This ttl value is added to the event time of the feature group and when the system time exceeds the event time + ttl, the entries will be automatically removed.
                The system time zone is in UTC.

                By default, no TTL is set.
            ttl_enabled:
                Optionally, enable TTL for this feature group.
                Defaults to True if ttl is set.
            online_disk:
                Optionally, specify online data storage for this feature group.
                When set to True data will be stored on disk, instead of in memory.
                Overrides online_config.table_space.
                Defaults to using cluster wide configuration 'featurestore_online_tablespace' to identify tablespace for disk storage.
            tags:
                Optionally, define tags for the feature group. Tags can be provided as:
                - A single Tag object
                - A dictionary with 'name' and 'value' keys (e.g., {"name": "tag1", "value": "value1"})
                - A list of Tag objects
                - A list of dictionaries with 'name' and 'value' keys
                Tags will be attached to the feature group after it is saved. Defaults to None.

        Returns:
            The feature group metadata object.
        """
        normalized_tags = self._normalize_tags(tags)

        if not data_source:
            data_source = ds.DataSource(storage_connector=storage_connector, path=path)
        feature_group_object = feature_group.FeatureGroup(
            name=name,
            version=version,
            description=description,
            online_enabled=online_enabled,
            time_travel_format=time_travel_format,
            partition_key=partition_key or [],
            primary_key=primary_key or [],
            foreign_key=foreign_key or [],
            hudi_precombine_key=hudi_precombine_key,
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features or [],
            embedding_index=embedding_index,
            statistics_config=statistics_config,
            event_time=event_time,
            stream=stream,
            expectation_suite=expectation_suite,
            parents=parents or [],
            topic_name=topic_name,
            notification_topic_name=notification_topic_name,
            transformation_functions=transformation_functions,
            online_config=online_config,
            offline_backfill_every_hr=offline_backfill_every_hr,
            data_source=data_source,
            ttl=ttl,
            ttl_enabled=ttl_enabled,
            online_disk=online_disk,
            tags=normalized_tags,
        )
        feature_group_object.feature_store = self
        return feature_group_object

    @usage.method_logger
    def get_or_create_feature_group(
        self,
        name: str,
        version: int,
        description: str | None = "",
        online_enabled: bool | None = False,
        time_travel_format: str | None = None,
        partition_key: list[str] | None = None,
        primary_key: list[str] | None = None,
        foreign_key: list[str] | None = None,
        embedding_index: EmbeddingIndex | None = None,
        hudi_precombine_key: str | None = None,
        features: list[feature.Feature] | None = None,
        statistics_config: StatisticsConfig | bool | dict | None = None,
        expectation_suite: expectation_suite.ExpectationSuite
        | TypeVar("great_expectations.core.ExpectationSuite")
        | None = None,
        event_time: str | None = None,
        stream: bool | None = False,
        parents: list[feature_group.FeatureGroup] | None = None,
        topic_name: str | None = None,
        notification_topic_name: str | None = None,
        transformation_functions: list[TransformationFunction | HopsworksUdf]
        | None = None,
        online_config: OnlineConfig | dict[str, Any] | None = None,
        offline_backfill_every_hr: int | str | None = None,
        storage_connector: storage_connector.StorageConnector | dict[str, Any] = None,
        path: str | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        ttl: float | timedelta | None = None,
        ttl_enabled: bool | None = None,
        online_disk: bool | None = None,
    ) -> (
        feature_group.FeatureGroup
        | feature_group.ExternalFeatureGroup
        | feature_group.SpineGroup
    ):
        """Get feature group metadata object or create a new one if it doesn't exist.

        This method doesn't update existing feature group metadata object.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            fg = fs.get_or_create_feature_group(
                name="electricity_prices",
                version=1,
                description="Electricity prices from NORD POOL",
                primary_key=["day", "area"],
                online_enabled=True,
                event_time="timestamp",
                transformation_functions=transformation_functions,
                online_config={'online_comments': ['NDB_TABLE=READ_BACKUP=1']},
                online_disk=True, # Online data will be stored on disk instead of in memory
                ttl=timedelta(days=30),
            )
            ```

        Note: Lazy
            This method is lazy and does not persist any metadata or feature data in the feature store on its own.
            To persist the feature group and save feature data along the metadata in the feature store, call the `insert()` method with a DataFrame.

        Parameters:
            name: Name of the feature group to create.
            version: Version of the feature group to retrieve or create.
            description: A string describing the contents of the feature group to improve discoverability for Data Scientists.
            online_enabled: Define whether the feature group should be made available also in the online feature store for low latency access.
            time_travel_format: Format used for time travel, defaults to `"HUDI"`.
            partition_key: A list of feature names to be used as partition key when writing the feature data to the offline storage, defaults to empty list `[]`.
            primary_key:
                A list of feature names to be used as primary key for the feature group.
                This primary key can be a composite key of multiple features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the feature group won't have any primary key.
            foreign_key:
                A list of feature names to be used as foreign key for the feature group.
                Foreign key is referencing the primary key of another feature group and can be used as joining key.
                Defaults to empty list `[]`, and the feature group won't have any foreign key.
            embedding_index:
                [`EmbeddingIndex`][hsfs.embedding.EmbeddingIndex]. If an embedding index is provided, vector database is used as online feature store.
                This enables similarity search by using [`FeatureGroup.find_neighbors`][hsfs.feature_group.FeatureGroup.find_neighbors].
            hudi_precombine_key:
                A feature name to be used as a precombine key for the `"HUDI"` feature group.
                If feature group has time travel format `"HUDI"` and hudi precombine key was not specified then the first primary key of the feature group will be used as hudi precombine key.
            features:
                Optionally, define the schema of the feature group manually as a list of `Feature` objects.
                Defaults to empty list `[]` and will use the schema information of the DataFrame provided in the `save` method.
            statistics_config:
                A configuration object, or a dictionary with keys:

                - `enabled` to generally enable descriptive statistics computation for this feature group,
                - `correlations` to turn on feature correlation computation,
                - `histograms` to compute feature value frequencies, and
                - `exact_uniqueness` to compute uniqueness, distinctness and entropy.

                The values should be booleans indicating the setting.
                To fully turn off statistics computation pass `statistics_config=False`.
                By default, it computes only descriptive statistics.
            event_time:
                Optionally, provide the name of the feature containing the event time for the features in this feature group.
                If event_time is set the feature group can be used for point-in-time joins.

                Note: Event time data type restriction
                    The supported data types for the event time column are: `timestamp`, `date` and `bigint`.

            stream:
                Optionally, define whether the feature group should support real time stream writing capabilities.
                Stream enabled Feature Groups have unified single API for writing streaming features transparently to both online and offline store.
            expectation_suite:
                Optionally, attach an expectation suite to the feature group which dataframes should be validated against upon insertion.
            parents:
                Optionally, define the parents of this feature group as the origin where the data is coming from.
            topic_name:
                Optionally, define the name of the topic used for data ingestion.
                If left undefined it defaults to using project topic.
            notification_topic_name:
                Optionally, define the name of the topic used for sending notifications when entries are inserted or updated on the online feature store.
                If left undefined no notifications are sent.
            transformation_functions:
                On-Demand Transformation functions attached to the feature group.
                It can be a list of list of user defined functions defined using the hopsworks `@udf` decorator.
                Defaults to `None`, no transformations.
            online_config: Optionally, define configuration which is used to configure online table.
            offline_backfill_every_hr:
                If specified, the materialization job will be scheduled to run periodically.
                The value can be either an integer representing the number of hours between each run or a string representing a cron expression.
                Set the value to None to avoid scheduling the materialization job.
                By default, no scheduling is done.
            storage_connector: The storage connector used to establish connectivity with the data source. **[DEPRECATED: Use `data_source` instead.]**
            path: The location within the scope of the storage connector, from where to read the data for the external feature group. **[DEPRECATED: Use `data_source` instead.]**
            data_source:
                The data source specifying the location of the data.
                Overrides the path and query arguments when specified.
            ttl:
                Optional time-to-live duration for features in this group.
                Can be specified as:

                - An integer or float representing seconds
                - A timedelta object

                This ttl value is added to the event time of the feature group and when the system time exceeds the event time + ttl, the entries will be automatically removed.
                The system time zone is in UTC.

                By default, no TTL is set.
            ttl_enabled:
                Optionally, enable TTL for this feature group.
                Defaults to True if ttl is set.
            online_disk:
                Optionally, specify online data storage for this feature group.
                When set to True data will be stored on disk, instead of in memory.
                Overrides online_config.table_space.
                Defaults to using cluster wide configuration 'featurestore_online_tablespace' to identify tablespace for disk storage.

        Returns:
            The feature group metadata object.
        """
        feature_group_object = self._feature_group_api.get(self.id, name, version)
        if not feature_group_object:
            if not data_source:
                data_source = ds.DataSource(
                    storage_connector=storage_connector, path=path
                )
            feature_group_object = feature_group.FeatureGroup(
                name=name,
                version=version,
                description=description,
                online_enabled=online_enabled,
                time_travel_format=time_travel_format,
                partition_key=partition_key or [],
                primary_key=primary_key or [],
                foreign_key=foreign_key or [],
                embedding_index=embedding_index,
                hudi_precombine_key=hudi_precombine_key,
                featurestore_id=self._id,
                featurestore_name=self._name,
                features=features or [],
                statistics_config=statistics_config,
                event_time=event_time,
                stream=stream,
                expectation_suite=expectation_suite,
                parents=parents or [],
                topic_name=topic_name,
                notification_topic_name=notification_topic_name,
                transformation_functions=transformation_functions,
                online_config=online_config,
                offline_backfill_every_hr=offline_backfill_every_hr,
                data_source=data_source,
                ttl=ttl,
                ttl_enabled=ttl_enabled,
                online_disk=online_disk,
            )
        feature_group_object.feature_store = self
        return feature_group_object

    @usage.method_logger
    def create_on_demand_feature_group(
        self,
        name: str,
        storage_connector: storage_connector.StorageConnector,
        query: str | None = None,
        data_format: str | None = None,
        path: str | None = "",
        options: dict[str, str] | None = None,
        version: int | None = None,
        description: str | None = "",
        primary_key: list[str] | None = None,
        foreign_key: list[str] | None = None,
        features: list[feature.Feature] | None = None,
        statistics_config: StatisticsConfig | bool | dict | None = None,
        event_time: str | None = None,
        expectation_suite: expectation_suite.ExpectationSuite
        | TypeVar("great_expectations.core.ExpectationSuite")
        | None = None,
        topic_name: str | None = None,
        notification_topic_name: str | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        online_enabled: bool = False,
        ttl: float | timedelta | None = None,
        ttl_enabled: bool | None = None,
    ) -> feature_group.ExternalFeatureGroup:
        """Create an external feature group metadata object.

        Warning: Deprecated
            `create_on_demand_feature_group` method is deprecated.
            Use the `create_external_feature_group` method instead.

        Note: Lazy
            This method is lazy and does not persist any metadata in the
            feature store on its own.
            To persist the feature group metadata in the feature store, call the `save()` method.

        Parameters:
            name: Name of the external feature group to create.
            storage_connector: The storage connector used to establish connectivity with the data source. **[DEPRECATED: Use `data_source` instead.]**
            query:
                A string containing a SQL query valid for the target data source.
                The query will be used to pull data from the data sources when the feature group is used. **[DEPRECATED: Use `data_source` instead.]**
            data_format: If the external feature groups refers to a directory with data, the data format to use when reading it.
            path: The location within the scope of the storage connector, from where to read the data for the external feature group. **[DEPRECATED: Use `data_source` instead.]**
            options:
                Additional options to be used by the engine when reading data from the specified storage connector.
                For example, `{"header": True}` when reading CSV files with column names in the first row.
            version: Version of the external feature group to retrieve, defaults to `None` and will create the feature group with incremented version from the last version in the feature store.
            description: A string describing the contents of the external feature group to improve discoverability for Data Scientists.
            primary_key:
                A list of feature names to be used as primary key for the feature group.
                This primary key can be a composite key of multiple features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the feature group won't have any primary key.
            foreign_key:
                A list of feature names to be used as foreign key for the feature group.
                Foreign key is referencing the primary key of another feature group and can be used as joining key.
                Defaults to empty list `[]`, and the feature group won't have any foreign key.
            features:
                Optionally, define the schema of the external feature group manually as a list of `Feature` objects.
                Defaults to empty list `[]` and will use the schema information of the DataFrame resulting by executing the provided query against the data source.
            statistics_config:
                A configuration object, or a dictionary with keys:

                - `"enabled"` to generally enable descriptive statistics computation for this external feature group,
                - `"correlations"` to turn on feature correlation
                computation,
                - `"histograms"` to compute feature value frequencies, and
                - `"exact_uniqueness"` to compute uniqueness, distinctness and entropy.

                The values should be booleans indicating the setting.
                To fully turn off statistics computation pass `statistics_config=False`.
                Defaults to `None` and will compute only descriptive statistics.
            event_time:
                Optionally, provide the name of the feature containing the event time for the features in this feature group.
                If event_time is set the feature group can be used for point-in-time joins.

                Note: Event time data type restriction
                    The supported data types for the event time column are: `timestamp`, `date` and `bigint`.

            topic_name:
                Optionally, define the name of the topic used for data ingestion.
                If left undefined it defaults to using project topic.
            notification_topic_name:
                Optionally, define the name of the topic used for sending notifications when entries are inserted or updated on the online feature store.
                If left undefined no notifications are sent.
            expectation_suite:
                Optionally, attach an expectation suite to the feature group which dataframes should be validated against upon insertion.
            data_source:
                The data source specifying the location of the data.
                Overrides the storage_connector, path and query arguments when specified.
            online_enabled:
                Define whether it should be possible to sync the feature group to the online feature store for low latency access.
            ttl:
                Optional time-to-live duration for features in this group.

                Can be specified as:

                - An integer or float representing seconds
                - A timedelta object

                This ttl value is added to the event time of the feature group and when the system time exceeds the event time + ttl, the entries will be automatically removed.
                The system time zone is in UTC.
                By default no TTL is set.
            ttl_enabled:
                Optionally, enable TTL for this feature group.
                Defaults to True if ttl is set.

        Returns:
            The external feature group metadata object.
        """
        if not data_source:
            if not storage_connector:
                raise ValueError(
                    "Data source must be provided to create an external feature group."
                )
            data_source = ds.DataSource(
                storage_connector=storage_connector, query=query, path=path
            )
        feature_group_object = feature_group.ExternalFeatureGroup(
            name=name,
            data_format=data_format,
            options=options or {},
            version=version,
            description=description,
            primary_key=primary_key or [],
            foreign_key=foreign_key or [],
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features or [],
            statistics_config=statistics_config,
            event_time=event_time,
            expectation_suite=expectation_suite,
            topic_name=topic_name,
            notification_topic_name=notification_topic_name,
            data_source=data_source,
            online_enabled=online_enabled,
            ttl=ttl,
            ttl_enabled=ttl_enabled,
        )
        feature_group_object.feature_store = self
        return feature_group_object

    @usage.method_logger
    def create_external_feature_group(
        self,
        name: str,
        storage_connector: storage_connector.StorageConnector,
        query: str | None = None,
        data_format: str | None = None,
        path: str | None = "",
        options: dict[str, str] | None = None,
        version: int | None = None,
        description: str | None = "",
        primary_key: list[str] | None = None,
        foreign_key: list[str] | None = None,
        embedding_index: EmbeddingIndex | None = None,
        features: list[feature.Feature] | None = None,
        statistics_config: StatisticsConfig | bool | dict | None = None,
        event_time: str | None = None,
        expectation_suite: expectation_suite.ExpectationSuite
        | TypeVar("great_expectations.core.ExpectationSuite")
        | None = None,
        online_enabled: bool = False,
        topic_name: str | None = None,
        notification_topic_name: str | None = None,
        online_config: OnlineConfig | dict[str, Any] | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        ttl: float | timedelta | None = None,
        ttl_enabled: bool | None = None,
        online_disk: bool | None = None,
    ) -> feature_group.ExternalFeatureGroup:
        """Create an external feature group metadata object.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            external_fg = fs.create_external_feature_group(
                name="sales",
                version=1,
                description="Physical shop sales features",
                data_source=data_source,
                primary_key=['ss_store_sk'],
                event_time='sale_date',
                ttl=timedelta(days=30),
            )
            ```

        Note: Lazy
            This method is lazy and does not persist any metadata in the
            feature store on its own.
            To persist the feature group metadata in the feature store, call the `save()` method.

        You can enable online storage for external feature groups, however, the sync from the external storage to Hopsworks online storage needs to be done manually:

        ```python
        external_fg = fs.create_external_feature_group(
            name="sales",
            version=1,
            description="Physical shop sales features",
            data_source=data_source,
            primary_key=['ss_store_sk'],
            event_time='sale_date',
            online_enabled=True,
            online_config={'online_comments': ['NDB_TABLE=READ_BACKUP=1']},
            online_disk=True, # Online data will be stored on disk instead of in memory
            ttl=timedelta(days=30),
        )
        external_fg.save()

        # read from external storage and filter data to sync to online
        df = external_fg.read().filter(external_fg.customer_status == "active")

        # insert to online storage
        external_fg.insert(df)
        ```

        Parameters:
            name: Name of the external feature group to create.
            storage_connector: The storage connector used to establish connectivity with the data source. **[DEPRECATED: Use `data_source` instead.]**
            query:
                A string containing a SQL query valid for the target data source.
                The query will be used to pull data from the data sources when the feature group is used. **[DEPRECATED: Use `data_source` instead.]**
            data_format:
                If the external feature groups refers to a directory with data, the data format to use when reading it.
            path:
                The location within the scope of the storage connector, from where to read the data for the external feature group. **[DEPRECATED: Use `data_source` instead.]**
            options:
                Additional options to be used by the engine when reading data from the specified storage connector.
                For example, `{"header": True}` when reading CSV files with column names in the first row.
            version:
                Version of the external feature group to retrieve, defaults to `None` and will create the feature group with incremented version from the last version in the feature store.
            description:
                A string describing the contents of the external feature group to improve discoverability for Data Scientists.
            primary_key:
                A list of feature names to be used as primary key for the feature group.
                This primary key can be a composite key of multiple features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the feature group won't have any primary key.
            foreign_key:
                A list of feature names to be used as foreign key for the feature group.
                Foreign key is referencing the primary key of another feature group and can be used as joining key.
                Defaults to empty list `[]`, and the feature group won't have any foreign key.
            features:
                Optionally, define the schema of the external feature group manually as a list of `Feature` objects.
                Defaults to empty list `[]` and will use the schema information of the DataFrame resulting by executing the provided query against the data source.
            statistics_config:
                A configuration object, or a dictionary with keys:

                - `"enabled"` to generally enable descriptive statistics computation for this external feature group,
                - `"correlations"` to turn on feature correlation
                computation,
                - `"histograms"` to compute feature value frequencies, and
                - `"exact_uniqueness"` to compute uniqueness, distinctness and entropy.

                The values should be booleans indicating the setting. To fully turn off statistics computation pass `statistics_config=False`.
                Defaults to `None` and will compute only descriptive statistics.
            event_time:
                Optionally, provide the name of the feature containing the event time for the features in this feature group.
                If event_time is set the feature group can be used for point-in-time joins.

                Note: Event time data type restriction
                    The supported data types for the event time column are: `timestamp`, `date` and `bigint`.

            online_enabled:
                Define whether it should be possible to sync the feature group to the online feature store for low latency access.
            expectation_suite:
                Optionally, attach an expectation suite to the feature group which dataframes should be validated against upon insertion.
            topic_name:
                Optionally, define the name of the topic used for data ingestion.
                If left undefined it defaults to using project topic.
            notification_topic_name:
                Optionally, define the name of the topic used for sending notifications when entries are inserted or updated on the online feature store.
                If left undefined no notifications are sent.
            online_config:
                Optionally, define configuration which is used to configure online table.
            data_source:
                The data source specifying the location of the data.
                Overrides the storage_connector, path and query arguments when specified.
            ttl:
                Optional time-to-live duration for features in this group.

                Can be specified as:

                - An integer or float representing seconds
                - A timedelta object

                This ttl value is added to the event time of the feature group and when the system time exceeds the event time + ttl, the entries will be automatically removed.
                The system time zone is in UTC.
                By default no TTL is set.
            ttl_enabled:
                Optionally, enable TTL for this feature group.
                Defaults to True if ttl is set.
            online_disk:
                Optionally, specify online data storage for this feature group.
                When set to True data will be stored on disk, instead of in memory.
                Overrides online_config.table_space.
                Defaults to using cluster wide configuration 'featurestore_online_tablespace' to identify tablespace for disk storage.

        Returns:
            The external feature group metadata object.
        """
        if not data_source:
            if not storage_connector:
                raise ValueError(
                    "Data source must be provided to create an external feature group."
                )
            data_source = ds.DataSource(
                storage_connector=storage_connector, query=query, path=path
            )
        feature_group_object = feature_group.ExternalFeatureGroup(
            name=name,
            data_format=data_format,
            options=options or {},
            version=version,
            description=description,
            primary_key=primary_key or [],
            foreign_key=foreign_key or [],
            embedding_index=embedding_index,
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features or [],
            statistics_config=statistics_config,
            event_time=event_time,
            expectation_suite=expectation_suite,
            online_enabled=online_enabled,
            topic_name=topic_name,
            notification_topic_name=notification_topic_name,
            online_config=online_config,
            data_source=data_source,
            ttl=ttl,
            ttl_enabled=ttl_enabled,
            online_disk=online_disk,
        )
        feature_group_object.feature_store = self
        return feature_group_object

    @usage.method_logger
    def get_or_create_spine_group(
        self,
        name: str,
        version: int | None = None,
        description: str | None = "",
        primary_key: list[str] | None = None,
        foreign_key: list[str] | None = None,
        event_time: str | None = None,
        features: list[feature.Feature] | None = None,
        dataframe: pd.DataFrame
        | TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | np.ndarray
        | list[list] = None,
    ) -> feature_group.SpineGroup:
        """Create a spine group metadata object.

        Instead of using a feature group to save a label/prediction target, you can use a spine together with a dataframe containing the labels.
        A Spine is essentially a metadata object similar to a feature group, however, the data is not materialized in the feature store.
        It only containes the needed metadata such as the relevant event time column and primary key columns to perform point-in-time correct joins.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            spine_df = pd.Dataframe()

            spine_group = fs.get_or_create_spine_group(
                name="sales",
                version=1,
                description="Physical shop sales features",
                primary_key=['ss_store_sk'],
                event_time='sale_date',
                dataframe=spine_df,
            )
            ```

        Note that you can inspect the dataframe in the spine group, or replace the dataframe:

        ```python
        spine_group.dataframe.show()

        spine_group.dataframe = new_df
        ```

        The spine can then be used to construct queries, with only one speciality:

        Note:
            Spines can only be used on the left side of a feature join, as this is the base set of entities for which features are to be fetched and the left side of the join determines the event timestamps to compare against.

        **If you want to use the query for a feature view to be used for online serving, you can only select the label or target feature from the spine.**
        For the online lookup, the label is not required, therefore it is important to only select label from the left feature group, so that we don't need to provide a spine for online serving.

        These queries can then be used to create feature views.
        Since the dataframe contained in the spine is not being materialized, every time you use a feature view created with spine to read data you will have to provide a dataframe with the same structure again.

        For example, to generate training data:

        ```python
        X_train, X_test, y_train, y_test = feature_view_spine.train_test_split(0.2, spine=training_data_entities)
        ```

        Or to get batches of fresh data for batch scoring:

        ```python
        feature_view_spine.get_batch_data(spine=scoring_entities_df).show()
        ```

        Here you have the chance to pass a different set of entities to generate the training dataset.

        Sometimes it might be handy to create a feature view with a regular feature group containing the label, but then at serving time to use a spine in order to fetch features for example only for a small set of primary key values.
        To do this, you can pass the spine group instead of a dataframe.
        Just make sure it contains the needed primary key, event time and label column.

        ```python
        feature_view.get_batch_data(spine=spine_group)
        ```

        Parameters:
            name: Name of the spine group to create.
            version: Version of the spine group to retrieve, defaults to `None` and will create the spine group with incremented version from the last version in the feature store.
            description: A string describing the contents of the spine group to improve discoverability for Data Scientists.
            primary_key:
                A list of feature names to be used as primary key for the spine group.
                This primary key can be a composite key of multiple features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the spine group won't have any primary key.
            foreign_key:
                A list of feature names to be used as foreign key for the feature group.
                Foreign key is referencing the primary key of another feature group and can be used as joining key.
                Defaults to empty list `[]`, and the feature group won't have any foreign key.
            event_time:
                Optionally, provide the name of the feature containing the event time for the features in this spine group.
                If event_time is set the spine group can be used for point-in-time joins.
            features:
                Optionally, define the schema of the spine group manually as a list of `Feature` objects.
                Defaults to empty list `[]` and will use the schema information of the DataFrame resulting by executing the provided query against the data source.

                Note: Event time data type restriction
                    The supported data types for the event time column are: `timestamp`, `date` and `bigint`.

            dataframe: Spine dataframe with primary key, event time and label column to use for point in time join when fetching features.

        Returns:
            The spine group metadata object.
        """
        spine = self._feature_group_api.get(self.id, name, version)
        if spine:
            spine.dataframe = dataframe
            spine.feature_store = self
            return spine
        spine = feature_group.SpineGroup(
            name=name,
            version=version,
            description=description,
            primary_key=primary_key or [],
            foreign_key=foreign_key or [],
            event_time=event_time,
            features=features or [],
            dataframe=dataframe,
            featurestore_id=self._id,
            featurestore_name=self._name,
        )
        spine.feature_store = self
        return spine._save()

    def create_training_dataset(
        self,
        name: str,
        version: int | None = None,
        description: str | None = "",
        data_format: str | None = "tfrecords",
        coalesce: bool | None = False,
        storage_connector: storage_connector.StorageConnector | None = None,
        splits: dict[str, float] | None = None,
        location: str | None = "",
        seed: int | None = None,
        statistics_config: StatisticsConfig | bool | dict | None = None,
        label: list[str] | None = None,
        transformation_functions: dict[str, TransformationFunction] | None = None,
        train_split: str = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        tags: tag.Tag | dict[str, Any] | list[tag.Tag | dict[str, Any]] | None = None,
    ) -> training_dataset.TrainingDataset:
        """Create a training dataset metadata object.

        Warning: Deprecated
            `TrainingDataset` is deprecated, use `FeatureView` instead.
            From version 3.0 training datasets created with this API are not visibile in the API anymore.

        Note: Lazy
            This method is lazy and does not persist any metadata or feature data in the feature store on its own.
            To materialize the training dataset and save feature data along the metadata in the feature store, call the `save()` method with a `DataFrame` or `Query`.

        Info: Data Formats
            The feature store currently supports the following data formats for
            training datasets:

            1. tfrecord
            2. csv
            3. tsv
            4. parquet
            5. avro
            6. orc

            Currently not supported petastorm, hdf5 and npy file formats.

        Parameters:
            name: Name of the training dataset to create.
            version: Version of the training dataset to retrieve, defaults to `None` and will create the training dataset with incremented version from the last version in the feature store.
            description: A string describing the contents of the training dataset to improve discoverability for Data Scientists.
            data_format: The data format used to save the training dataset.
            coalesce:
                If true the training dataset data will be coalesced into a single partition before writing.
                The resulting training dataset will be a single file per split.
            storage_connector:
                Storage connector defining the sink location for the training dataset, defaults to `None`, and materializes training dataset on HopsFS. **[DEPRECATED: Use `data_source` instead.]**
            splits:
                A dictionary defining training dataset splits to be created.
                Keys in the dictionary define the name of the split as `str`, values represent percentage of samples in the split as `float`.
                Currently, only random splits are supported.
                Defaults to empty dict`{}`, creating only a single training dataset without splits.
            location:
                Path to complement the sink storage connector with, e.g., if the storage connector points to an S3 bucket, this path can be used to define a sub-directory inside the bucket to place the training dataset.
                Defaults to `""`, saving the training dataset at the root defined by the storage connector. **[DEPRECATED: Use `data_source` instead.]**
            seed: Optionally, define a seed to create the random splits with, in order to guarantee reproducability.
            statistics_config:
                A configuration object, or a dictionary with keys:
                - `"enabled"` to generally enable descriptive statistics computation for this feature group,
                - `"correlations"` to turn on feature correlation computation, and
                - `"histograms"` to compute feature value frequencies.

                The values should be booleans indicating the setting.
                To fully turn off statistics computation pass `statistics_config=False`.
                Defaults to `None` and will compute only descriptive statistics.
            label:
                A list of feature names constituting the prediction label/feature of the training dataset.
                When replaying a `Query` during model inference, the label features can be omitted from the feature vector retrieval.
                Defaults to `[]`, no label.
            transformation_functions:
                A dictionary mapping transformation functions to the features they should be applied to before writing out the training data and at inference time.
                Defaults to `{}`, no transformations.
            train_split:
                If `splits` is set, provide the name of the split that is going to be used for training.
                The statistics of this split will be used for transformation functions if necessary.
            data_source: The data source specifying the location of the data. Overrides the storage_connector and location arguments when specified.
            tags:
                Optionally, define tags for the training dataset. Tags can be provided as:
                - A single Tag object
                - A dictionary with 'name' and 'value' keys (e.g., {"name": "tag1", "value": "value1"})
                - A list of Tag objects
                - A list of dictionaries with 'name' and 'value' keys
                Tags will be attached to the training dataset after it is saved. Defaults to None.

        Returns:
            The training dataset metadata object.
        """
        if not data_source:
            data_source = ds.DataSource(
                storage_connector=storage_connector, path=location
            )
        normalized_tags = self._normalize_tags(tags)

        return training_dataset.TrainingDataset(
            name=name,
            version=version,
            description=description,
            data_format=data_format,
            data_source=data_source,
            featurestore_id=self._id,
            splits=splits or {},
            seed=seed,
            statistics_config=statistics_config,
            label=label or [],
            coalesce=coalesce,
            transformation_functions=transformation_functions or {},
            train_split=train_split,
            tags=normalized_tags,
        )

    @usage.method_logger
    def create_transformation_function(
        self,
        transformation_function: HopsworksUdf,
        version: int | None = None,
    ) -> TransformationFunction:
        """Create a transformation function metadata object.

        Example:
            ```python
            # define the transformation function as a Hopsworks's UDF
            @udf(int)
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    version=1
                )

            # persist transformation function in backend
            plus_one_meta.save()
            ```

        Note: Lazy
            This method is lazy and does not persist the transformation function in the feature store on its own.
            To materialize the transformation function and save call the `save()` method of the transformation function metadata object.

        Parameters:
            transformation_function: Hopsworks UDF.

        Returns:
            The TransformationFunction metadata object.
        """
        return TransformationFunction(
            featurestore_id=self._id,
            hopsworks_udf=transformation_function,
            version=version,
        )

    @usage.method_logger
    def get_transformation_function(
        self,
        name: str,
        version: int | None = None,
    ) -> TransformationFunction:
        """Get  transformation function metadata object.

        Example: Get transformation function by name
            This will default to version 1.

            ```python
            # get feature store instance
            fs = ...

            # get transformation function metadata object
            plus_one_fn = fs.get_transformation_function(name="plus_one")
            ```

        Example: Get built-in transformation function min max scaler
            ```python
            # get feature store instance
            fs = ...

            # get transformation function metadata object
            min_max_scaler_fn = fs.get_transformation_function(name="min_max_scaler")
            ```

        Example: Get transformation function by name and version
            ```python
            # get feature store instance
            fs = ...

            # get transformation function metadata object
            min_max_scaler = fs.get_transformation_function(name="min_max_scaler", version=2)
            ```

        You can define in the feature view transformation functions as dict, where key is feature name and value is online transformation function instance.
        Then the transformation functions are applied when you read training data, get batch data, or get feature vector(s).

        Example: Attach transformation functions to the feature view
            ```python
            # get feature store instance
            fs = ...

            # define query object
            query = ...

            # get transformation function metadata object
            min_max_scaler = fs.get_transformation_function(name="min_max_scaler", version=1)

            # attach transformation functions
            feature_view = fs.create_feature_view(
                name='feature_view_name',
                query=query,
                labels=["target_column"],
                transformation_functions=[min_max_scaler("feature1")]
            )
            ```

        Built-in transformation functions are attached in the same way.
        The only difference is that it will compute the necessary statistics for the specific function in the background.
        For example min and max values for `min_max_scaler`; mean and standard deviation for `standard_scaler` etc.

        Example: Attach built-in transformation functions to the feature view
            ```python
            # get feature store instance
            fs = ...

            # define query object
            query = ...

            # retrieve transformation functions
            min_max_scaler = fs.get_transformation_function(name="min_max_scaler")
            standard_scaler = fs.get_transformation_function(name="standard_scaler")
            robust_scaler = fs.get_transformation_function(name="robust_scaler")
            label_encoder = fs.get_transformation_function(name="label_encoder")

            # attach built-in transformation functions while creating feature view
            feature_view = fs.create_feature_view(
                name='transactions_view',
                query=query,
                labels=["fraud_label"],
                transformation_functions = [
                    label_encoder("category_column"),
                    robust_scaler("weight"),
                    min_max_scaler("age"),
                    standard_scaler("salary")
                ]
            )
            ```

        Parameters:
            name: Name of transformation function.
            version:
                Version of transformation function.
                Optional, if not provided all functions that match to provided name will be retrieved.

        Returns:
            The TransformationFunction metadata object.
        """
        return self._transformation_function_engine.get_transformation_fn(name, version)

    @usage.method_logger
    def get_transformation_functions(self) -> list[TransformationFunction]:
        """Get  all transformation functions metadata objects.

        Example: Get all transformation functions
            ```python
            # get feature store instance
            fs = ...

            # get all transformation functions
            list_transformation_fns = fs.get_transformation_functions()
            ```

        Returns:
            List of transformation function instances.
        """
        return self._transformation_function_engine.get_transformation_fns()

    @usage.method_logger
    def create_feature_view(
        self,
        name: str,
        query: Query,
        version: int | None = None,
        description: str | None = "",
        labels: list[str] | None = None,
        inference_helper_columns: list[str] | None = None,
        training_helper_columns: list[str] | None = None,
        transformation_functions: list[TransformationFunction | HopsworksUdf]
        | None = None,
        logging_enabled: bool | None = False,
        extra_log_columns: list[feature.Feature] | list[dict[str, str]] | None = None,
        tags: tag.Tag | dict[str, Any] | list[tag.Tag | dict[str, Any]] | None = None,
    ) -> feature_view.FeatureView:
        """Create a feature view metadata object and saved it to hopsworks.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the feature group instances
            fg1 = fs.get_or_create_feature_group(...)
            fg2 = fs.get_or_create_feature_group(...)

            # construct the query
            query = fg1.select_all().join(fg2.select_all())

            # define the transformation function as a Hopsworks's UDF
            @udf(int)
            def plus_one(value):
                return value + 1

            # construct list of "transformation functions" on features
            transformation_functions = [plus_one("feature1"), plus_one("feature1"))]

            feature_view = fs.create_feature_view(
                name='air_quality_fv',
                version=1,
                transformation_functions=transformation_functions,
                query=query
            )
            ```

        Example:
            ```python
            # get feature store instance
            fs = ...

            # define query object
            query = ...

            # define list of transformation functions
            mapping_transformers = ...

            # create feature view
            feature_view = fs.create_feature_view(
                name='feature_view_name',
                version=1,
                transformation_functions=mapping_transformers,
                query=query
            )
            ```

        Warning:
            `as_of` argument in the `Query` will be ignored because feature view does not support time travel query.

        Parameters:
            name: Name of the feature view to create.
            query: Feature store `Query`.
            version: Version of the feature view to create, defaults to `None` and will create the feature view with incremented version from the last version in the feature store.
            description: A string describing the contents of the feature view to improve discoverability for Data Scientists.
            labels:
                A list of feature names constituting the prediction label/feature of the feature view.
                When replaying a `Query` during model inference, the label features can be omitted from the feature vector retrieval.
                Defaults to `[]`, no label.
            inference_helper_columns:
                A list of feature names that are not used in training the model itself but can be used during batch or online inference for extra information.
                Inference helper column name(s) must be part of the `Query` object.
                If inference helper column name(s) belong to feature group that is part of a `Join` with `prefix` defined, then this prefix needs to be prepended to the original column name when defining `inference_helper_columns` list.
                When replaying a `Query` during model inference, the inference helper columns optionally can be omitted during batch (`get_batch_data`) and will be omitted during online  inference (`get_feature_vector(s)`).
                To get inference helper column(s) during online inference use `get_inference_helper(s)` method.
                Defaults to `[], no helper columns.
            training_helper_columns:
                A list of feature names that are not the part of the model schema itself but can be used during training as a helper for extra information.
                Training helper column name(s) must be part of the `Query` object.
                If training helper column name(s) belong to feature group that is part of a `Join` with `prefix` defined, then this prefix needs to prepended to the original column name when defining `training_helper_columns` list.
                When replaying a `Query` during model inference, the training helper columns will be omitted during both batch and online inference.
                Training helper columns can be optionally fetched with training data.
                For more details see documentation for feature view's get training data methods.
                Defaults to `[]`, no training helper columns.
            transformation_functions:
                Model Dependent Transformation functions attached to the feature view.
                It can be a list of list of user defined functions defined using the hopsworks `@udf` decorator.
                Defaults to `None`, no transformations.
            logging_enabled: If true, enable feature logging for the feature view.
            extra_log_columns:
                Extra columns to be logged in addition to the features used in the feature view.
                It can be a list of Feature objects or list a dictionaries that contains the the name and type of the columns as keys.
                Defaults to `None`, no extra log columns. Setting this argument implicitly enables feature logging.
            tags:
                Optionally, define tags for the feature view. Tags can be provided as:
                - A single Tag object
                - A dictionary with 'name' and 'value' keys (e.g., {"name": "tag1", "value": "value1"})
                - A list of Tag objects
                - A list of dictionaries with 'name' and 'value' keys
                Tags will be attached to the feature view after it is saved. Defaults to None.

        Returns:
            The feature view metadata object.
        """
        normalized_tags = self._normalize_tags(tags)

        feat_view = feature_view.FeatureView(
            name=name,
            query=query,
            featurestore_id=self._id,
            version=version,
            description=description,
            labels=labels or [],
            inference_helper_columns=inference_helper_columns or [],
            training_helper_columns=training_helper_columns or [],
            transformation_functions=transformation_functions or {},
            featurestore_name=self._name,
            logging_enabled=logging_enabled,
            extra_log_columns=extra_log_columns,
            tags=normalized_tags,
        )
        return self._feature_view_engine.save(feat_view)

    @usage.method_logger
    def get_or_create_feature_view(
        self,
        name: str,
        query: Query,
        version: int,
        description: str | None = "",
        labels: list[str] | None = None,
        inference_helper_columns: list[str] | None = None,
        training_helper_columns: list[str] | None = None,
        transformation_functions: dict[str, TransformationFunction] | None = None,
        logging_enabled: bool | None = False,
        extra_log_columns: list[feature.Feature] | list[dict[str, str]] | None = None,
    ) -> feature_view.FeatureView:
        """Get feature view metadata object or create a new one if it doesn't exist.

        This method doesn't update existing feature view metadata object.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            feature_view = fs.get_or_create_feature_view(
                name='bitcoin_feature_view',
                version=1,
                transformation_functions=transformation_functions,
                query=query
            )
            ```

        Parameters:
            name: Name of the feature view to create.
            query: Feature store `Query`.
            version: Version of the feature view to create.
            description: A string describing the contents of the feature view to improve discoverability for Data Scientists.
            labels:
                A list of feature names constituting the prediction label/feature of the feature view.
                When replaying a `Query` during model inference, the label features can be omitted from the feature vector retrieval.
                Defaults to `[]`, no label.
            inference_helper_columns:
                A list of feature names that are not used in training the model itself but can be used during batch or online inference for extra information.
                Inference helper column name(s) must be part of the `Query` object.
                If inference helper column name(s) belong to feature group that is part of a `Join` with `prefix` defined, then this prefix needs to be prepended to the original column name when defining `inference_helper_columns` list.
                When replaying a `Query` during model inference, the inference helper columns optionally can be omitted during batch (`get_batch_data`) and will be omitted during online  inference (`get_feature_vector(s)`).
                To get inference helper column(s) during online inference use `get_inference_helper(s)` method.
                Defaults to `[], no helper columns.
            training_helper_columns:
                A list of feature names that are not the part of the model schema itself but can be used during training as a helper for extra information.
                Training helper column name(s) must be part of the `Query` object.
                If training helper column name(s) belong to feature group that is part of a `Join` with `prefix` defined, then this prefix needs to prepended to the original column name when defining `training_helper_columns` list.
                When replaying a `Query` during model inference, the training helper columns will be omitted during both batch and online inference.
                Training helper columns can be optionally fetched with training data.
                For more details see documentation for feature view's get training data methods.
                Defaults to `[]`, no training helper columns.
            transformation_functions:
                Model Dependent Transformation functions attached to the feature view.
                It can be a list of list of user defined functions defined using the hopsworks `@udf` decorator.
                Defaults to `None`, no transformations.
            logging_enabled: If true, enable feature logging for the feature view.
            extra_log_columns:
                Extra columns to be logged in addition to the features used in the feature view.
                It can be a list of Feature objects or list a dictionaries that contains the the name and type of the columns as keys.
                Defaults to `None`, no extra log columns.
                Setting this argument implicitly enables feature logging.

        Returns:
            The feature view metadata object.
        """
        fv_object = self._feature_view_engine.get(name, version)
        if not fv_object:
            fv_object = self.create_feature_view(
                name=name,
                query=query,
                version=version,
                description=description,
                labels=labels or [],
                inference_helper_columns=inference_helper_columns or [],
                training_helper_columns=training_helper_columns or [],
                transformation_functions=transformation_functions or [],
                logging_enabled=logging_enabled,
                extra_log_columns=extra_log_columns,
            )
        return fv_object

    @usage.method_logger
    def get_feature_view(
        self, name: str, version: int = None
    ) -> feature_view.FeatureView:
        """Get a feature view entity from the feature store.

        Getting a feature view from the Feature Store means getting its metadata.

        Example:
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(
                name='feature_view_name',
                version=1
            )
            ```

        Parameters:
            name: Name of the feature view to get.
            version: Version of the feature view to retrieve, defaults to `None` and will return the `version=1`.

        Returns:
            The feature view metadata object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if version is None:
            warnings.warn(
                f"No version provided for getting feature view `{name}`, defaulting to `{self.DEFAULT_VERSION}`.",
                util.VersionWarning,
                stacklevel=1,
            )
            version = self.DEFAULT_VERSION
        feature_view_object = self._feature_view_engine.get(name, version)
        if feature_view_object:
            util.check_missing_mandatory_tags(
                feature_view_object.missing_mandatory_tags
            )
        return feature_view_object

    @usage.method_logger
    def get_feature_views(self, name: str) -> list[feature_view.FeatureView]:
        """Get a list of all versions of a feature view entity from the feature store.

        Getting a feature view from the Feature Store means getting its metadata.

        Example:
            ```python
            # get feature store instance
            fs = ...

            # get a list of all versions of a feature view
            feature_view = fs.get_feature_views(
                name='feature_view_name'
            )
            ```

        Parameters:
            name: Name of the feature view to get.

        Returns:
            List of feature view metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._feature_view_engine.get(name)

    def _disable_hopsworks_feature_query_service_client(self):
        """Disable Hopsworks feature query service for the current session. This behaviour is not persisted on reset."""
        from hsfs.core import arrow_flight_client

        arrow_flight_client._disable_feature_query_service_client()

    def _reset_hopsworks_feature_query_service_client(self):
        """Reset Hopsworks feature query service for the current session."""
        from hsfs.core import arrow_flight_client

        arrow_flight_client.close()
        arrow_flight_client.get_instance()

    def create_chart(
        self, title: str, description: str, url: str, job_id: int | None = None
    ) -> None:
        """Create a chart in the feature store.

        Registers an HTML file as a chart in Hopsworks.
        This enables it to be used in a [`Dashboard`][hsfs.core.dashboard.Dashboard].

        Each chart with a set `job_id` has a refresh button which triggers the job and redraws the chart once the job finishes.
        You can use this job to conviniently extract and prepare the data from Hopsworks Feature Store using its Python API.
        Once the data is acquired, it can be put into JSON to simplify the Javascript code in the HTML.

        Note: Jobless charts
            Although charts can be created without a data preparation job, such charts are not suited to visualize data stored in Hopsworks.
            Jobless charts can be useful, for example, in case you want to display data which is already available in JSON via a REST API of an external service, or if the chart is completely static.
            Jobless charts do not have a refresh button attached to them.

        Example:
            ```python
            # get feature store instance
            fs = ...

            # create a chart
            fs.create_chart(
                title="My Chart",
                description="This is my chart description",
                url="/Resources/chart.html"
            )
            ```

        Arguments:
            title: Title of the chart.
            description: Description of the chart.
            url: URL where the chart is hosted or can be accessed.
            job_id: ID of the job that prepares the data to be displayed in the chart.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        chart = Chart(
            title=title,
            description=description,
            url=url,
            job=Job(id=job_id) if job_id else None,
        )
        return ChartApi().create_chart(chart)

    def get_charts(self) -> list[Chart]:
        """Get all charts in the feature store.

        Example:
            ```python
            # get feature store instance
            fs = ...

            # get all charts
            charts = fs.get_charts()
            ```

        Returns:
            List of chart metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return ChartApi().get_charts()

    def get_chart(self, chart_id: int) -> Chart:
        """Get a chart by its ID.

        Example:
            ```python
            # get feature store instance
            fs = ...

            # get a specific chart
            chart = fs.get_chart(chart_id=123)
            ```

        Arguments:
            chart_id: ID of the chart to retrieve.

        Returns:
            The chart metadata object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return ChartApi().get_chart(chart_id)

    def create_dashboard(self, name: str, charts: list[Chart] | None = None) -> None:
        """Create a dashboard in the feature store.

        Example:
            ```python
            # get feature store instance
            fs = ...

            chart = fs.get_chart(chart_id=321)
            chart.width = 12
            chart.height = 8
            chart.x = 0
            chart.y = 0

            # create a dashboard
            fs.create_dashboard(
                name="My Dashboard",
                charts=[chart]  # optional
            )
            ```

        Arguments:
            name: Name of the dashboard.
            charts: List of charts to include in the dashboard.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        dashboard = Dashboard(
            name=name,
            charts=charts,
        )
        return DashboardApi().create_dashboard(dashboard)

    def get_dashboards(self) -> list[Dashboard]:
        """Get all dashboards in the feature store.

        Example:
            ```python
            # get feature store instance
            fs = ...

            # get all dashboards
            dashboards = fs.get_dashboards()
            ```

        Returns:
            List of dashboard metadata objects.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return DashboardApi().get_dashboards()

    def get_dashboard(self, dashboard_id: int) -> Dashboard:
        """Get a dashboard by its ID.

        Example:
            ```python
            # get feature store instance
            fs = ...

            # get a specific dashboard
            dashboard = fs.get_dashboard(dashboard_id=123)
            ```

        Arguments:
            dashboard_id: ID of the dashboard to retrieve.

        Returns:
            The dashboard metadata object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return DashboardApi().get_dashboard(dashboard_id)

    @property
    def id(self) -> int:
        """Id of the feature store."""
        return self._id

    @property
    def name(self) -> str:
        """Name of the feature store."""
        return self._name

    @property
    def project_name(self) -> str:
        """Name of the project in which the feature store is located."""
        return self._project_name

    @property
    def project_id(self) -> int:
        """Id of the project in which the feature store is located."""
        return self._project_id

    @property
    def online_featurestore_name(self) -> str | None:
        """Name of the online feature store database."""
        return self._online_feature_store_name

    @property
    def online_enabled(self) -> bool:
        """Indicator whether online feature store is enabled."""
        return self._online_enabled

    @property
    def offline_featurestore_name(self) -> str:
        """Name of the offline feature store database."""
        return self._offline_feature_store_name

    @usage.method_logger
    def search(
        self,
        search_term: str = None,
        keyword_filter: str | list[str] | None = None,
        tag_filter: dict[str, str]
        | list[dict[str, str] | search_api.TagSearchFilter]
        | None = None,
        offset: int = 0,
        limit: int = 100,
        global_search: bool = False,
    ) -> search_api.FeaturestoreSearchResult:
        """Search for feature groups, feature views, training datasets and features.

        Parameters:
           search_term: the term to search for.
           keyword_filter: filter results by keywords. Can be a single string or an array of strings.
           tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries,
               or an array of TagSearchFilter objects. Each tag filter requires: ``name`` (the tag
               schema name as defined by Hopsworks Admin), ``key`` (the property within that tag
               schema), and ``value`` (the value to match).
           offset: the number of results to skip (default is 0).
           limit: the number of search results to return (default is 100).
           global_search: By default is false - search in current project only. Set to true if you want to search over all projects

        Returns:
           `FeaturestoreSearchResult`: The search results containing lists of metadata objects for feature groups, feature views, training datasets, and features.

        Raises:
           `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        Example:
        ```python
        import hopsworks

        project = hopsworks.login()
        fs = project.get_feature_store()

        # Simple search
        result = fs.search("search-term")

        # Access results
        for fg_meta in result.feature_groups:
           print(f"Feature Group: {fg_meta.name} v{fg_meta.version}")
           print(f"Description: {fg_meta.description}")
           print(f"Highlights: {fg_meta.highlights}")

           # Get the same FeatureGroup object as returned by featurestore.get_feature_group
           fg = fg_meta.get()

        # Search with a single keyword (string)
        result = fs.search("search-term", keyword_filter="ml")

        # Search with multiple keywords (array of strings)
        result = fs.search("search-term", keyword_filter=["ml", "production"])

        # Search with tag filter as a single dictionary
        result = fs.search(
           "search-term",
           tag_filter={"name": "tag1", "key": "environment", "value": "production"}
        )

        # Search with tag filter as an array of dictionaries
        result = fs.search(
           "search-term",
           tag_filter=[
               {"name": "tag1", "key": "environment", "value": "production"},
               {"name": "tag2", "key": "version", "value": "v1.0"}
           ]
        )

        # Search with TagSearchFilter objects
        from hsfs.core.search_api import TagSearchFilter
        tags = [
           TagSearchFilter(name="tag1", key="environment", value="production"),
           TagSearchFilter(name="tag2", key="version", value="v1.0")
        ]
        result = fs.search("search-term", tag_filter=tags)

        # Search with both keyword_filter and tag_filter
        result = fs.search(
           "search-term",
           keyword_filter=["ml", "production"],
           tag_filter=tags
        )
        ```
        """
        return self._search_api.feature_store(
            search_term=search_term,
            tag_filter=tag_filter,
            keyword_filter=keyword_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )

    @usage.method_logger
    def search_feature_groups(
        self,
        search_term: str = None,
        keyword_filter: str | list[str] | None = None,
        tag_filter: dict[str, str]
        | list[dict[str, str] | search_api.TagSearchFilter]
        | None = None,
        offset: int = 0,
        limit: int = 100,
        global_search: bool = False,
    ) -> list[search_api.FeatureGroupSearchResult]:
        """Search for feature groups only.

        Parameters:
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries,
               or an array of TagSearchFilter objects. Each tag filter requires: ``name`` (the tag
               schema name as defined by Hopsworks Admin), ``key`` (the property within that tag
               schema), and ``value`` (the value to match).
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects

        Returns:
            `List`: A list of metadata objects for feature groups matching the search criteria.

        Raises:
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        Example:
        ```python
        import hopsworks

        project = hopsworks.login()
        fs = project.get_feature_store()

        # Search for feature groups
        fg_metas = fs.search_feature_groups("customer")

        for fg_meta in fg_metas:
            print(f"Feature Group: {fg_meta.name} v{fg_meta.version}")

            # Get the same FeatureGroup object as returned by featurestore.get_feature_group
            fg = fg_meta.get()
        ```
        """
        return self._search_api.feature_groups(
            search_term=search_term,
            tag_filter=tag_filter,
            keyword_filter=keyword_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )

    @usage.method_logger
    def search_feature_views(
        self,
        search_term: str = None,
        keyword_filter: str | list[str] | None = None,
        tag_filter: dict[str, str]
        | list[dict[str, str] | search_api.TagSearchFilter]
        | None = None,
        offset: int = 0,
        limit: int = 100,
        global_search: bool = False,
    ) -> list[search_api.FeatureViewSearchResult]:
        """Search for feature views only.

        Parameters:
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries,
               or an array of TagSearchFilter objects. Each tag filter requires: ``name`` (the tag
               schema name as defined by Hopsworks Admin), ``key`` (the property within that tag
               schema), and ``value`` (the value to match).
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects

        Returns:
            `List`: A list of metadata objects for feature views matching the search criteria.

        Raises:
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        Example:
        ```python
        import hopsworks

        project = hopsworks.login()
        fs = project.get_feature_store()

        # Search for feature views
        fv_metas = fs.search_feature_views("customer")

        for fv_meta in fv_metas:
            print(f"Feature View: {fv_meta.name} v{fv_meta.version}")

            # Get the same FeatureView object as returned by featurestore.get_feature_view
            fv = fv_meta.get()
        ```
        """
        return self._search_api.feature_views(
            search_term=search_term,
            tag_filter=tag_filter,
            keyword_filter=keyword_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )

    @usage.method_logger
    def search_training_datasets(
        self,
        search_term: str = None,
        keyword_filter: str | list[str] | None = None,
        tag_filter: dict[str, str]
        | list[dict[str, str] | search_api.TagSearchFilter]
        | None = None,
        offset: int = 0,
        limit: int = 100,
        global_search: bool = False,
    ) -> list[search_api.TrainingDatasetSearchResult]:
        """Search for training datasets only.

        Parameters:
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries,
               or an array of TagSearchFilter objects. Each tag filter requires: ``name`` (the tag
               schema name as defined by Hopsworks Admin), ``key`` (the property within that tag
               schema), and ``value`` (the value to match).
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects

        Returns:
            `List`: A list of metadata objects for training datasets matching the search criteria.

        Raises:
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        Example:
        ```python
        import hopsworks

        project = hopsworks.login()
        fs = project.get_feature_store()

        # Search for training datasets
        td_metas = fs.search_training_datasets("model")

        for td_meta in td_metas:
            print(f"Training Dataset: {td_meta.name} v{td_meta.version}")

            # Get the same TrainingDataset object as returned by featurestore.get_training_dataset
            td = td_meta.get()
        ```
        """
        return self._search_api.training_datasets(
            search_term=search_term,
            tag_filter=tag_filter,
            keyword_filter=keyword_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )

    @usage.method_logger
    def search_features(
        self,
        search_term: str = None,
        keyword_filter: str | list[str] | None = None,
        tag_filter: dict[str, str]
        | list[dict[str, str] | search_api.TagSearchFilter]
        | None = None,
        offset: int = 0,
        limit: int = 100,
        global_search: bool = False,
    ) -> list[search_api.FeatureSearchResult]:
        """Search for features only.

        Parameters:
            search_term: the term to search for.
            keyword_filter: filter results by keywords. Can be a single string or an array of strings.
            tag_filter: filter results by tags. Can be a single dictionary, an array of dictionaries,
               or an array of TagSearchFilter objects. Each tag filter requires: ``name`` (the tag
               schema name as defined by Hopsworks Admin), ``key`` (the property within that tag
               schema), and ``value`` (the value to match).
            offset: the number of results to skip (default is 0).
            limit: the number of search results to return (default is 100).
            global_search: By default is false - search in current project only. Set to true if you want to search over all projects

        Returns:
            `List`: A list of features matching the search criteria.

        Raises:
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request

        Example:
        ```python
        import hopsworks

        project = hopsworks.login()
        fs = project.get_feature_store()

        # Search for features
        features = fs.search_features("age")

        for feature in features:
            print(f"Feature: {feature.name}")
        ```
        """
        return self._search_api.features(
            search_term=search_term,
            tag_filter=tag_filter,
            keyword_filter=keyword_filter,
            offset=offset,
            limit=limit,
            global_search=global_search,
        )

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

import copy
import json
import logging
import time
import warnings
from datetime import date, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeVar,
)

import avro.schema
import hsfs.expectation_suite
import humps
from hopsworks_apigen import public
from hopsworks_common import job
from hopsworks_common.client.exceptions import FeatureStoreException, RestAPIError
from hopsworks_common.core import alerts_api
from hopsworks_common.core.constants import (
    HAS_DELTALAKE_PYTHON,
    HAS_DELTALAKE_SPARK,
    HAS_NUMPY,
    HAS_POLARS,
)
from hopsworks_common.core.sink_job_configuration import SinkJobConfiguration
from hsfs import (
    engine,
    feature,
    feature_group_writer,
    tag,
    user,
    util,
)
from hsfs import (
    feature_store as feature_store_mod,
)
from hsfs import (
    storage_connector as sc,
)
from hsfs.constructor import filter, query
from hsfs.core import data_source as ds
from hsfs.core import (
    deltastreamer_jobconf,
    expectation_suite_engine,
    explicit_provenance,
    external_feature_group_engine,
    feature_group_engine,
    feature_monitoring_config_engine,
    feature_monitoring_result_engine,
    feature_store_api,
    great_expectation_engine,
    job_api,
    online_ingestion,
    online_ingestion_api,
    spine_group_engine,
    statistics_engine,
    validation_report_engine,
    validation_result_engine,
)
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_result as fmr
from hsfs.core.constants import (
    HAS_CONFLUENT_KAFKA,
    HAS_GREAT_EXPECTATIONS,
)
from hsfs.core.variable_api import VariableApi
from hsfs.core.vector_db_client import VectorDbClient

# if great_expectations is not installed, we will default to using native Hopsworks class as return values
from hsfs.decorators import typechecked, uses_great_expectations
from hsfs.embedding import EmbeddingIndex
from hsfs.online_config import OnlineConfig
from hsfs.statistics_config import StatisticsConfig
from hsfs.transformation_function import TransformationFunction, TransformationType
from hsfs.validation_report import ValidationReport


if TYPE_CHECKING:
    if HAS_CONFLUENT_KAFKA:
        import confluent_kafka
    if HAS_NUMPY:
        import numpy as np
    if HAS_POLARS:
        import polars as pl
    import pandas as pd
    from hopsworks_common.alert import FeatureGroupAlert
    from hsfs.constructor.filter import Filter, Logic
    from hsfs.core.job import Job
    from hsfs.ge_validation_result import ValidationResult
    from hsfs.hopsworks_udf import HopsworksUdf
    from hsfs.statistics import Statistics


if HAS_GREAT_EXPECTATIONS:
    import great_expectations

_logger = logging.getLogger(__name__)


@typechecked
class FeatureGroupBase:
    NOT_FOUND_ERROR_CODE = 270009

    def __init__(
        self,
        name: str | None,
        version: int | None,
        featurestore_id: int | None,
        location: str | None,
        event_time: str | int | date | datetime | None = None,
        online_enabled: bool = False,
        id: int | None = None,
        embedding_index: EmbeddingIndex | None = None,
        expectation_suite: (
            hsfs.expectation_suite.ExpectationSuite
            | great_expectations.core.ExpectationSuite
            | dict[str, Any]
            | None
        ) = None,
        online_topic_name: str | None = None,
        topic_name: str | None = None,
        notification_topic_name: str | None = None,
        deprecated: bool = False,
        online_config: OnlineConfig | dict[str, Any] | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        ttl: float | timedelta | None = None,
        ttl_enabled: bool | None = None,
        online_disk: bool | None = None,
        sink_enabled: bool | None = False,
        missing_mandatory_tags: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> None:
        """Initialize a feature group object.

        Parameters:
            name: Name of the feature group to create.
            version: Version number of the feature group.
            featurestore_id: ID of the feature store to create the feature group in.
            location: Location to store the feature group data.
            event_time: Event time column for the feature group.
            online_enabled: Whether to enable online serving for this feature group.
            id: ID of the feature group.
            embedding_index: Embedding index configuration for vector similarity search.
            expectation_suite: Great Expectations suite for data validation.
            online_topic_name: Name of the Kafka topic for online serving.
            topic_name: Name of the Kafka topic for streaming.
            notification_topic_name: Name of the Kafka topic for notifications.
            deprecated: Whether this feature group is deprecated.
            online_config: Configuration for online serving.
            data_source: Data source configuration.
            ttl: Time-to-live (TTL) configuration for this feature group.
            ttl_enabled:
                Whether to enable time-to-live (TTL) for this feature group.
                Defaults to True if `ttl` is set.
            online_disk:
                Whether to enable online disk storage for this feature group.
                Overrides `online_config.table_space`.
                Defaults to using cluster wide configuration `featurestore_online_tablespace` to identify tablespace for disk storage.
            sink_enabled: Whether to enable sink from data source to feature group. A storage connector and data source must be defined for the feature group.
            **kwargs: Additional keyword arguments
        """
        self._version = version
        self._name = name
        self.event_time = event_time
        self._online_enabled = online_enabled or embedding_index is not None
        self._location = location
        self._id = id
        self._subject = None
        self._online_topic_name = online_topic_name
        self._topic_name = topic_name
        self._notification_topic_name = notification_topic_name
        self._deprecated = deprecated
        self._feature_store_id = featurestore_id
        self._feature_store = None
        self._variable_api: VariableApi = VariableApi()
        self._alert_api = alerts_api.AlertsApi()
        self.ttl = ttl
        self._ttl_enabled = ttl_enabled if ttl_enabled is not None else ttl is not None
        self._sink_enabled = sink_enabled
        self._missing_mandatory_tags = missing_mandatory_tags or []

        self._online_config = (
            OnlineConfig.from_response_json(online_config)
            if isinstance(online_config, dict)
            else online_config
        )
        if online_disk is not None:
            if self._online_config is None:
                # Make sure online config is initialized
                self._online_config = OnlineConfig()

            if online_disk:
                self._online_config.table_space = (
                    self._variable_api.get_featurestore_online_tablespace()
                )
            else:
                # An empty string is interpreted as don't set table space, while None uses the cluster default
                self._online_config.table_space = ""

        if data_source:
            self.data_source = (
                ds.DataSource.from_response_json(data_source)
                if isinstance(data_source, dict)
                else data_source
            )
        else:
            self.data_source = ds.DataSource()

        self._multi_part_insert: bool = False
        self._embedding_index = embedding_index
        # use setter for correct conversion
        self.expectation_suite = expectation_suite

        self._feature_group_engine: feature_group_engine.FeatureGroupEngine | None = (
            None
        )
        self._statistics_engine: statistics_engine.StatisticsEngine = (
            statistics_engine.StatisticsEngine(featurestore_id, self.ENTITY_TYPE)
        )
        self._great_expectation_engine: great_expectation_engine.GreatExpectationEngine = great_expectation_engine.GreatExpectationEngine(
            featurestore_id
        )
        if self._id is not None:
            if expectation_suite:
                self._expectation_suite._init_expectation_engine(
                    feature_store_id=featurestore_id, feature_group_id=self._id
                )
            self._expectation_suite_engine: (
                expectation_suite_engine.ExpectationSuiteEngine | None
            ) = expectation_suite_engine.ExpectationSuiteEngine(
                feature_store_id=featurestore_id, feature_group_id=self._id
            )
            self._validation_report_engine: (
                validation_report_engine.ValidationReportEngine | None
            ) = validation_report_engine.ValidationReportEngine(
                featurestore_id, self._id
            )
            self._validation_result_engine: (
                validation_result_engine.ValidationResultEngine | None
            ) = validation_result_engine.ValidationResultEngine(
                featurestore_id, self._id
            )
            self._feature_monitoring_config_engine: (
                feature_monitoring_config_engine.FeatureMonitoringConfigEngine | None
            ) = feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
                feature_store_id=featurestore_id,
                feature_group_id=self._id,
            )
            self._feature_monitoring_result_engine: feature_monitoring_result_engine.FeatureMonitoringResultEngine = feature_monitoring_result_engine.FeatureMonitoringResultEngine(
                feature_store_id=self._feature_store_id,
                feature_group_id=self._id,
            )

        self.check_deprecated()

    def check_deprecated(self) -> None:
        """Print a warning if this feature group is deprecated."""
        if self.deprecated:
            warnings.warn(
                f"Feature Group `{self._name}`, version `{self._version}` is deprecated",
                stacklevel=1,
            )

    def delete(self) -> None:
        """Drop the entire feature group along with its feature data.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(
                    name='bitcoin_price',
                    version=1
                    )

            # delete the feature group
            fg.delete()
            ```

        Danger: Potentially dangerous operation
            This operation drops all metadata associated with **this version** of the feature group **and** all the feature data in offline and online storage associated with it.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        warnings.warn(
            f"All jobs associated to feature group `{self._name}`, version `{self._version}` will be removed.",
            util.JobWarning,
            stacklevel=1,
        )
        self._feature_group_engine.delete(self)

    def select_all(
        self,
        include_primary_key: bool = True,
        include_foreign_key: bool = True,
        include_partition_key: bool = True,
        include_event_time: bool = True,
    ) -> query.Query:
        """Select all features along with primary key and event time from the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a feature view.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instances
            fg1 = fs.get_or_create_feature_group(...)
            fg2 = fs.get_or_create_feature_group(...)

            # construct the query
            query = fg1.select_all().join(fg2.select_all())

            # show first 5 rows
            query.show(5)

            # select all features exclude primary key and event time
            from hsfs.feature import Feature
            fg = fs.create_feature_group(
                    "fg",
                    features=[
                            Feature("id", type="string"),
                            Feature("ts", type="bigint"),
                            Feature("f1", type="date"),
                            Feature("f2", type="double")
                            ],
                    primary_key=["id"],
                    event_time="ts")

            query = fg.select_all()
            query.features
            # [Feature('id', ...), Feature('ts', ...), Feature('f1', ...), Feature('f2', ...)]

            query = fg.select_all(include_primary_key=False, include_event_time=False)
            query.features
            # [Feature('f1', ...), Feature('f2', ...)]
            ```

        Parameters:
            include_primary_key: If `True`, include primary key of the feature group to the feature list.
            include_foreign_key: If `True`, include foreign key of the feature group to the feature list.
            include_partition_key: If `True`, include partition key of the feature group to the feature list.
            include_event_time: If `True`, include event time of the feature group to the feature list.

        Returns:
            A query object with all features of the feature group.
        """
        removed_keys = []

        if not include_event_time:
            removed_keys += [self.event_time]
        if not include_primary_key:
            removed_keys += self.primary_key
        if not include_foreign_key:
            removed_keys += self.foreign_key
        if not include_partition_key:
            removed_keys += self.partition_key

        if removed_keys:
            return self.select_except(removed_keys)
        return query.Query(
            left_feature_group=self,
            left_features=self._features,
            feature_store_name=self._feature_store_name,
            feature_store_id=self._feature_store_id,
        )

    def select_features(self) -> query.Query:
        """Select all the features in the feature group and return a query object.

        Queries define the schema of Feature View objects which can be used to create Training Datasets, read from the Online Feature Store, and more.
        They can also be composed to create more complex queries using the `join` method.

        Info:
            This method does not select the primary key and event time of the feature group.
            Use `select_all` to include them.
            Note that primary keys do not need to be included in the query to allow joining on them.

        Example:
            ```python
            # connect to the Feature Store
            fs = hopsworks.login().get_feature_store()

            # Some dataframe to create the feature group with
            # both an event time and a primary key column
            my_df.head()
            +------------+------------+------------+------------+
            |    id      | feature_1  |    ...     |    ts      |
            +------------+------------+------------+------------+
            |     8      |     8      |            |    15      |
            |     3      |     3      |    ...     |    6       |
            |     1      |     1      |            |    18      |
            +------------+------------+------------+------------+

            # Create the Feature Group instances
            fg1 = fs.create_feature_group(
                    name = "fg1",
                    version=1,
                    primary_key=["id"],
                    event_time="ts",
                )

            # Insert data to the feature group.
            fg1.insert(my_df)

            # select all features from `fg1` excluding primary key and event time
            query = fg1.select_features()

            # show first 3 rows
            query.show(3)

            # Output, no id or ts columns
            +------------+------------+------------+
            | feature_1  | feature_2  | feature_3  |
            +------------+------------+------------+
            |     8      |     7      |    15      |
            |     3      |     1      |     6      |
            |     1      |     2      |    18      |
            +------------+------------+------------+
            ```

        Example:
            ```python
            # connect to the Feature Store
            fs = hopsworks.login().get_feature_store()

            # Get the Feature Group from the previous example
            fg1 = fs.get_feature_group("fg1", 1)

            # Some dataframe to create another feature group
            # with a primary key column
            +------------+------------+------------+
            |    id_2    | feature_6  | feature_7  |
            +------------+------------+------------+
            |     8      |     11     |            |
            |     3      |     4      |    ...     |
            |     1      |     9      |            |
            +------------+------------+------------+

            # join the two feature groups on their indexes, `id` and `id_2`
            # but does not include them in the query
            query = fg1.select_features().join(fg2.select_features(), left_on="id", right_on="id_2")

            # show first 5 rows
            query.show(3)

            # Output
            +------------+------------+------------+------------+------------+
            | feature_1  | feature_2  | feature_3  | feature_6  | feature_7  |
            +------------+------------+------------+------------+------------+
            |     8      |     7      |    15      |    11      |    15      |
            |     3      |     1      |     6      |     4      |     3      |
            |     1      |     2      |    18      |     9      |    20      |
            +------------+------------+------------+------------+------------+
            ```

        Returns:
            A query object with all features of the feature group.
        """
        select_features = self.primary_key + self.foreign_key + [self.event_time]
        if not isinstance(self, ExternalFeatureGroup):
            select_features = select_features + self.partition_key

        query = self.select_except(select_features)

        _logger.info(
            f"Using {[f.name for f in query.features]} from feature group `{self.name}` as features for the query."
            " To include primary key and event time use `select_all`."
        )

        return query

    def select(self, features: list[str | feature.Feature]) -> query.Query:
        """Select a subset of features of the feature group and return a query object.

        The query can be used to construct joins of feature groups or create a feature view with a subset of features of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            from hsfs.feature import Feature
            fg = fs.create_feature_group(
                    "fg",
                    features=[
                            Feature("id", type="string"),
                            Feature("ts", type="bigint"),
                            Feature("f1", type="date"),
                            Feature("f2", type="double")
                            ],
                    primary_key=["id"],
                    event_time="ts")

            # construct query
            query = fg.select(["id", "f1"])
            query.features
            # [Feature('id', ...), Feature('f1', ...)]
            ```

        Parameters:
            features: A list of `Feature` objects or feature names as strings to be selected.

        Returns:
            A query object with the selected features of the feature group.
        """
        return query.Query(
            left_feature_group=self,
            left_features=features,
            feature_store_name=self._feature_store_name,
            feature_store_id=self._feature_store_id,
        )

    def select_except(
        self, features: list[str | feature.Feature] | None = None
    ) -> query.Query:
        """Select all features including primary key and event time feature of the feature group except provided `features` and return a query object.

        The query can be used to construct joins of feature groups or create a feature view with a subset of features of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            from hsfs.feature import Feature
            fg = fs.create_feature_group(
                    "fg",
                    features=[
                            Feature("id", type="string"),
                            Feature("ts", type="bigint"),
                            Feature("f1", type="date"),
                            Feature("f2", type="double")
                            ],
                    primary_key=["id"],
                    event_time="ts")

            # construct query
            query = fg.select_except(["ts", "f1"])
            query.features
            # [Feature('id', ...), Feature('f1', ...)]
            ```

        Parameters:
            features:
                A list of `Feature` objects or feature names as strings to be excluded from the selection.
                `None` or `[]` selects all features.

        Returns:
            A query object with the selected features of the feature group.
        """
        if features:
            except_features = [
                f.name if isinstance(f, feature.Feature) else f for f in features
            ]
            return query.Query(
                left_feature_group=self,
                left_features=[
                    f for f in self._features if f.name not in except_features
                ],
                feature_store_name=self._feature_store_name,
                feature_store_id=self._feature_store_id,
            )
        return self.select_all()

    def filter(self, f: filter.Filter | filter.Logic) -> query.Query:
        """Apply filter to the feature group.

        Selects all features and returns the resulting `Query` with the applied filter.

        Example:
            ```python
            from hsfs.feature import Feature

            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.filter(Feature("weekly_sales") > 1000)
            ```

        If you are planning to join the filtered feature group later on with another feature group, make sure to select the filtered feature explicitly from the respective feature group:

        Example:
            ```python
            fg.filter(fg.feature1 == 1).show(10)
            ```

        Composite filters require parenthesis and symbols for logical operands (e.g. `&`, `|`, ...):

        Example:
            ```python
            fg.filter((fg.feature1 == 1) | (fg.feature2 >= 2))
            ```

        Parameters:
            f: Filter object.

        Returns:
            The query object with the applied filter.
        """
        return self.select_all().filter(f)

    def add_tag(self, name: str, value: Any) -> None:
        """Attach a tag to a feature group.

        A tag consists of a name-value pair.
        Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json -- primitives, arrays or json objects.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.add_tag(name="example_tag", value="42")
            ```

        Parameters:
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.add_tag(self, name, value)

    def delete_tag(self, name: str) -> None:
        """Delete a tag attached to a feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.delete_tag("example_tag")
            ```

        Parameters:
            name: Name of the tag to be removed.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.delete_tag(self, name)

    def get_tag(self, name: str) -> tag.Tag | None:
        """Get the tags of a feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg_tag_value = fg.get_tag("example_tag")
            ```

        Parameters:
            name: Name of the tag to get.

        Returns:
            Tag value or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._feature_group_engine.get_tag(self, name)

    def get_tags(self) -> dict[str, tag.Tag]:
        """Retrieves all tags attached to a feature group.

        Returns:
            The dictionary of tags.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._feature_group_engine.get_tags(self)

    def get_parent_feature_groups(self) -> explicit_provenance.Links | None:
        """Get the parents of this feature group, based on explicit provenance.

        Parents are feature groups or external feature groups.
        These feature groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only minimal information is returned.

        Returns:
            Object containing the section of provenance graph requested or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._feature_group_engine.get_parent_feature_groups(self)

    def get_storage_connector_provenance(self) -> explicit_provenance.Links | None:
        """Get the parents of this feature group, based on explicit provenance.

        Parents are storage connectors.
        These storage connector can be accessible, deleted or inaccessible.
        For deleted and inaccessible storage connector, only minimal information is returned.

        !!! warning "Deprecated"
            `get_storage_connector_provenance` method is deprecated. Use `get_data_source_provenance` instead.

        Returns:
            The storage connector used to generate this feature group or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._feature_group_engine.get_storage_connector_provenance(self)

    def get_data_source_provenance(self) -> explicit_provenance.Links | None:
        """Get the parents of this feature group, based on explicit provenance.

        Parents are storage connectors. These storage connector can be accessible,
        deleted or inaccessible.
        For deleted and inaccessible storage connector, only minimal information is
        returned.

        # Returns
            `Links`: the data source used to generate this feature group or `None` if it does not exist.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_group_engine.get_storage_connector_provenance(self)

    def get_storage_connector(self) -> sc.StorageConnector | None:
        """Get the storage connector using this feature group, based on explicit provenance.

        Only the accessible storage connector is returned.
        For more items use the base method, see [`get_storage_connector_provenance`][hsfs.feature_group.FeatureGroup.get_storage_connector_provenance].

        !!! warning "Deprecated"
            `get_storage_connector_provenance` method is deprecated. Use `get_data_source_provenance` instead.

        Returns:
            Storage connector or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        storage_connector_provenance = self.get_storage_connector_provenance()

        if storage_connector_provenance and (
            storage_connector_provenance.inaccessible
            or storage_connector_provenance.deleted
        ):
            _logger.info(
                "The parent storage connector is deleted or inaccessible. For more details access `get_storage_connector_provenance`"
            )

        if storage_connector_provenance and storage_connector_provenance.accessible:
            return storage_connector_provenance.accessible[0]
        return None

    def get_data_source(self) -> ds.DataSource | None:
        """Get the data source using this feature group, based on explicit provenance.

        Only the accessible data source is returned.
        For more items use the base method - get_data_source_provenance.

        # Returns
            `DataSource`: Data source or `None` if it does not exist.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        data_source_provenance = self.get_data_source_provenance()

        if data_source_provenance and (
            data_source_provenance.inaccessible or data_source_provenance.deleted
        ):
            _logger.info(
                "The parent data source is deleted or inaccessible. For more details access `get_data_source_provenance`"
            )

        if data_source_provenance and data_source_provenance.accessible:
            return data_source_provenance.accessible[0]

        return None

    def get_generated_feature_views(self) -> explicit_provenance.Links | None:
        """Get the generated feature view using this feature group, based on explicit provenance.

        These feature views can be accessible or inaccessible.
        Explicit provenance does not track deleted generated feature view links, so deleted will always be empty.
        For inaccessible feature views, only a minimal information is returned.

        Returns:
            Object containing the section of provenance graph requested or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._feature_group_engine.get_generated_feature_views(self)

    def get_generated_feature_groups(self) -> explicit_provenance.Links | None:
        """Get the generated feature groups using this feature group, based on explicit provenance.

        These feature groups can be accessible or inaccessible.
        Explicit provenance does not track deleted generated feature group links, so deleted will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        Returns:
            Object containing the section of provenance graph requested or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._feature_group_engine.get_generated_feature_groups(self)

    def get_feature(self, name: str) -> feature.Feature | None:
        """Retrieve a `Feature` object from the schema of the feature group.

        There are several ways to access features of a feature group:

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # get Feature instanse
            fg.feature1
            fg["feature1"]
            fg.get_feature("feature1")
            ```

        Note:
            Attribute access to features works only for non-reserved names.
            For example, features named `id` or `name` will not be accessible via `fg.name`, instead this will return the name of the feature group itself.
            Fall back on using the `get_feature` method.

        Parameters:
            name: The name of the feature to retrieve

        Returns:
            The feature object or `None` if it does not exist.
        """
        try:
            return self.__getitem__(name)
        except KeyError:
            return None

    def update_statistics_config(
        self,
    ) -> FeatureGroup | ExternalFeatureGroup | SpineGroup | FeatureGroupBase:
        """Update the statistics configuration of the feature group.

        Change the `statistics_config` object and persist the changes by calling this method.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_statistics_config()
            ```

        Returns:
            The updated metadata object of the feature group.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If statistics are not supported for this feature group type.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        self._feature_group_engine.update_statistics_config(self)
        return self

    def update_description(
        self, description: str
    ) -> FeatureGroupBase | FeatureGroup | ExternalFeatureGroup | SpineGroup:
        """Update the description of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_description(description="Much better description.")
            ```

        Inof: Safe update
            This method updates the feature group description safely.
            In case of failure your local metadata object will keep the old description.

        Parameters:
            description: New description string.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.update_description(self, description)
        return self

    @public
    def update_topic_name(
        self, topic_name: str
    ) -> FeatureGroupBase | ExternalFeatureGroup | SpineGroup | FeatureGroup:
        """Update the kafka topic name of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_topic_name(topic_name="topic_name")
            ```

        Warning: Pending data will not be migrated automatically
            Any data already inserted into the current topic will not be materialized to the new topic.
            Ensure all in-flight processing has completed before switching.

        Warning: Offline materialization will restart from the beginning
            After switching, offline materialization will start consuming from the earliest offset of the new topic, which may result in duplicate or reprocessed data.

        Info: Safe update
            This method updates the feature group kafka topic name safely.
            In case of failure your local metadata object will keep the old topic name.

        Parameters:
            topic_name:
                Name of the Kafka topic for streaming.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        warnings.warn(
            "Pending data will not be migrated automatically: "
            "Any data already inserted into the current topic will not be materialized to the new topic. "
            "Ensure all in-flight processing has completed before switching.",
            util.FeatureGroupWarning,
            stacklevel=1,
        )
        warnings.warn(
            "Offline materialization will restart from the beginning: "
            "After switching, offline materialization will start consuming from the earliest offset of the new topic, "
            "which may result in duplicate or reprocessed data.",
            util.FeatureGroupWarning,
            stacklevel=1,
        )
        self._feature_group_engine.update_topic_name(self, topic_name)
        return self

    def update_notification_topic_name(
        self, notification_topic_name: str
    ) -> FeatureGroupBase | ExternalFeatureGroup | SpineGroup | FeatureGroup:
        """Update the notification topic name of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_notification_topic_name(notification_topic_name="notification_topic_name")
            ```

        Info: Safe update
            This method updates the feature group notification topic name safely.
            In case of failure your local metadata object will keep the old notification topic name.

        Parameters:
            notification_topic_name:
                Name of the topic used for sending notifications when entries are inserted or updated on the online feature store.
                If set to None no notifications are sent.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.update_notification_topic_name(
            self, notification_topic_name
        )
        return self

    def update_deprecated(
        self, deprecate: bool = True
    ) -> FeatureGroupBase | FeatureGroup | ExternalFeatureGroup | SpineGroup:
        """Deprecate the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_deprecated(deprecate=True)
            ```

        Info: Safe update
            This method updates the feature group safely.
            In case of failure your local metadata object will be kept unchanged.

        Parameters:
            deprecate: Whether the feature group should be deprecated.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.update_deprecated(self, deprecate)
        return self

    def update_features(
        self, features: feature.Feature | list[feature.Feature]
    ) -> FeatureGroupBase | FeatureGroup | ExternalFeatureGroup | SpineGroup:
        """Update metadata of features in this feature group.

        Currently it's only supported to update the description of a feature.

        Danger: Unsafe update
            Note that if you use an existing `Feature` object of the schema in the feature group metadata object, this might leave your metadata object in a corrupted state if the update fails.

        Parameters:
            features: A feature object or list thereof to be updated.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        new_features = []
        if isinstance(features, feature.Feature):
            new_features.append(features)
        elif isinstance(features, list):
            for feat in features:
                if isinstance(feat, feature.Feature):
                    new_features.append(feat)
                else:
                    raise TypeError(
                        "The argument `features` has to be of type `Feature` or "
                        f"a list thereof, but an element is of type: `{type(features)}`"
                    )
        else:
            raise TypeError(
                "The argument `features` has to be of type `Feature` or a list "
                f"thereof, but is of type: `{type(features)}`"
            )
        self._feature_group_engine.update_features(self, new_features)
        return self

    def update_feature_description(
        self, feature_name: str, description: str
    ) -> FeatureGroupBase | FeatureGroup | ExternalFeatureGroup | SpineGroup:
        """Update the description of a single feature in this feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.update_feature_description(
                feature_name="min_temp",
                description="Much better feature description.",
            )
            ```

        Info: Safe update
            This method updates the feature description safely. In case of failure
            your local metadata object will keep the old description.

        Parameters:
            feature_name: Name of the feature to be updated.
            description: New description string.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        f_copy = copy.deepcopy(self[feature_name])
        f_copy.description = description
        self._feature_group_engine.update_features(self, [f_copy])
        return self

    def append_features(
        self, features: feature.Feature | list[feature.Feature]
    ) -> FeatureGroupBase | FeatureGroup | ExternalFeatureGroup | SpineGroup:
        """Append features to the schema of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # define features to be inserted in the feature group
            features = [
                Feature(name="id",type="int",online_type="int"),
                Feature(name="name",type="string",online_type="varchar(20)")
            ]

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.append_features(features)
            ```

        Info: Safe append
            This method appends the features to the feature group description safely.
            In case of failure your local metadata object will contain the correct schema.

        It is only possible to append features to a feature group.
        Removing features is considered a breaking change.
        Note that feature views built on top of this feature group will not read appended feature data.
        Create a new feature view based on an updated query via `fg.select` to include the new features.

        Parameters:
            features: A feature object or list thereof to append to the schema of the feature group.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        new_features = []
        if isinstance(features, feature.Feature):
            new_features.append(features)
        elif isinstance(features, list):
            for feat in features:
                if isinstance(feat, feature.Feature):
                    new_features.append(feat)
                else:
                    raise TypeError(
                        "The argument `features` has to be of type `Feature` or "
                        f"a list thereof, but an element is of type: `{type(features)}`"
                    )
        else:
            raise TypeError(
                "The argument `features` has to be of type `Feature` or a list "
                f"thereof, but is of type: `{type(features)}`"
            )
        self._feature_group_engine.append_features(self, new_features)
        return self

    def get_expectation_suite(
        self, ge_type: bool = HAS_GREAT_EXPECTATIONS
    ) -> (
        hsfs.expectation_suite.ExpectationSuite
        | great_expectations.core.ExpectationSuite
        | None
    ):
        """Return the expectation suite attached to the feature group if it exists.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            exp_suite = fg.get_expectation_suite()
            ```

        Parameters:
            ge_type:
                If `True` returns a native Great Expectation type, Hopsworks custom type otherwise.
                Conversion can be performed via the `to_ge_type()` method on hopsworks type.

        Returns:
            The expectation suite attached to the feature group or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        # Avoid throwing an error if Feature Group not initialised.
        if self._id:
            self._expectation_suite = self._expectation_suite_engine.get()

        if self._expectation_suite is not None and ge_type is True:
            return self._expectation_suite.to_ge_type()
        return self._expectation_suite

    def save_expectation_suite(
        self,
        expectation_suite: (
            hsfs.expectation_suite.ExpectationSuite
            | great_expectations.core.ExpectationSuite
        ),
        run_validation: bool = True,
        validation_ingestion_policy: Literal["always", "strict"] = "always",
        overwrite: bool = False,
    ) -> (
        hsfs.expectation_suite.ExpectationSuite
        | great_expectations.core.ExpectationSuite
    ):
        """Attach an expectation suite to a feature group and saves it for future use.

        If an expectation suite is already attached, it is replaced.
        Note that the provided expectation suite is modified inplace to include expectationId fields.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.save_expectation_suite(expectation_suite, run_validation=True)
            ```

        Parameters:
            expectation_suite: The expectation suite to attach to the Feature Group.
            overwrite:
                If an Expectation Suite is already attached, overwrite it.
                The new suite will have its own validation history, but former reports are preserved.
            run_validation: Set whether the expectation_suite will run on ingestion.
            validation_ingestion_policy:
                Set the policy for ingestion to the Feature Group.

                - "STRICT" only allows DataFrame passing validation to be inserted into Feature Group.
                - "ALWAYS" always insert the DataFrame to the Feature Group, irrespective of overall validation result.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if HAS_GREAT_EXPECTATIONS and isinstance(
            expectation_suite, great_expectations.core.ExpectationSuite
        ):
            tmp_expectation_suite = (
                hsfs.expectation_suite.ExpectationSuite.from_ge_type(
                    ge_expectation_suite=expectation_suite,
                    run_validation=run_validation,
                    validation_ingestion_policy=validation_ingestion_policy,
                    feature_store_id=self._feature_store_id,
                    feature_group_id=self._id,
                )
            )
        elif isinstance(expectation_suite, hsfs.expectation_suite.ExpectationSuite):
            tmp_expectation_suite = expectation_suite.to_json_dict(decamelize=True)
            tmp_expectation_suite["feature_group_id"] = self._id
            tmp_expectation_suite["feature_store_id"] = self._feature_store_id
            tmp_expectation_suite = hsfs.expectation_suite.ExpectationSuite(
                **tmp_expectation_suite
            )
        else:
            raise TypeError(
                f"The provided expectation suite type `{type(expectation_suite)}` is not supported. Use Great Expectation `ExpectationSuite` or HSFS' own `ExpectationSuite` object."
            )

        if overwrite:
            self.delete_expectation_suite()

        if self._id:
            self._expectation_suite = self._expectation_suite_engine.save(
                tmp_expectation_suite
            )
            expectation_suite = self._expectation_suite.to_ge_type()
        else:
            # Added to avoid throwing an error if Feature Group is not initialised with the backend
            self._expectation_suite = tmp_expectation_suite

    def delete_expectation_suite(self) -> None:
        """Delete the expectation suite attached to the Feature Group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.delete_expectation_suite()
            ```

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if self.get_expectation_suite() is not None:
            self._expectation_suite_engine.delete(self._expectation_suite.id)
        self._expectation_suite = None

    def get_latest_validation_report(
        self, ge_type: bool = HAS_GREAT_EXPECTATIONS
    ) -> (
        ValidationReport
        | great_expectations.core.ExpectationSuiteValidationResult
        | None
    ):
        """Return the latest validation report attached to the Feature Group if it exists.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            latest_val_report = fg.get_latest_validation_report()
            ```

        Parameters:
            ge_type:
                If `True` returns a native Great Expectation type, Hopsworks custom type otherwise.
                Conversion can be performed via the `to_ge_type()` method on hopsworks type.

        Returns:
            The latest validation report attached to the Feature Group or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._validation_report_engine.get_last(ge_type=ge_type)

    def get_all_validation_reports(
        self, ge_type: bool = HAS_GREAT_EXPECTATIONS
    ) -> list[
        ValidationReport | great_expectations.core.ExpectationSuiteValidationResult
    ]:
        """Return the latest validation report attached to the feature group if it exists.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            val_reports = fg.get_all_validation_reports()
            ```

        Parameters:
            ge_type:
                If `True` returns a native Great Expectation type, Hopsworks custom type otherwise.
                Conversion can be performed via the `to_ge_type()` method on hopsworks type.

        Returns:
            All validation reports attached to the feature group.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If feature group is not registered with Hopsworks.
        """
        if self._id:
            return self._validation_report_engine.get_all(ge_type=ge_type)
        raise FeatureStoreException(
            "Only Feature Group registered with Hopsworks can fetch validation reports."
        )

    def save_validation_report(
        self,
        validation_report: (
            dict[str, Any]
            | ValidationReport
            | great_expectations.core.expectation_validation_result.ExpectationSuiteValidationResult
        ),
        ingestion_result: Literal[
            "UNKNOWN", "INGESTED", "REJECTED", "EXPERIMENT", "FG_DATA"
        ] = "UNKNOWN",
        ge_type: bool = HAS_GREAT_EXPECTATIONS,
    ) -> ValidationReport | great_expectations.core.ExpectationSuiteValidationResult:
        """Save validation report to hopsworks platform along previous reports of the same Feature Group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(..., expectation_suite=expectation_suite)

            validation_report = great_expectations.from_pandas(
                my_experimental_features_df,
                fg.get_expectation_suite()).validate()

            fg.save_validation_report(validation_report, ingestion_result="EXPERIMENT")
            ```

        Parameters:
            validation_report: The validation report to attach to the Feature Group.
            ingestion_result:
                Specify the fate of the associated data, defaults to `"UNKNOWN"`.
                Use `"INGESTED"` or `"REJECTED"` for validation of DataFrames to be inserted in the Feature Group.
                Use `"EXPERIMENT"` for testing and development and `"FG_DATA"` when validating data already in the Feature Group.
            ge_type:
                If `True` returns a native Great Expectation type, Hopsworks custom type otherwise.
                Conversion can be performed via the `to_ge_type()` method on hopsworks type.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If feature group is not registered with Hopsworks.
        """
        if self._id:
            if HAS_GREAT_EXPECTATIONS and isinstance(
                validation_report,
                great_expectations.core.expectation_validation_result.ExpectationSuiteValidationResult,
            ):
                report = ValidationReport(
                    **validation_report.to_json_dict(),
                    ingestion_result=ingestion_result,
                )
            elif isinstance(validation_report, dict):
                report = ValidationReport(
                    **validation_report, ingestion_result=ingestion_result
                )
            elif isinstance(validation_report, ValidationReport):
                report = validation_report
                if ingestion_result != "UNKNOWN":
                    report.ingestion_result = ingestion_result

            return self._validation_report_engine.save(
                validation_report=report, ge_type=ge_type
            )
        raise FeatureStoreException(
            "Only Feature Group registered with Hopsworks can upload validation reports."
        )

    def get_validation_history(
        self,
        expectation_id: int,
        start_validation_time: str | int | datetime | date | None = None,
        end_validation_time: str | int | datetime | date | None = None,
        filter_by: list[
            Literal["INGESTED", "REJECTED", "FG_DATA", "EXPERIMENT", "UNKNOWN"]
        ] = None,
        ge_type: bool = HAS_GREAT_EXPECTATIONS,
    ) -> (
        list[ValidationResult]
        | list[great_expectations.core.ExpectationValidationResult]
    ):
        """Fetch validation history of an Expectation specified by its id.

        Example:
            ```python3
            validation_history = fg.get_validation_history(
                expectation_id=1,
                filter_by=["REJECTED", "UNKNOWN"],
                start_validation_time="2022-01-01 00:00:00",
                end_validation_time=datetime.datetime.now(),
                ge_type=False,
            )
            ```

        Parameters:
            expectation_id: ID of the Expectation for which to fetch the validation history.
            filter_by: List of ingestion_result category to keep.
            start_validation_time:
                Fetch only validation result posterior to the provided time, inclusive.
                Supported format include timestamps(int), datetime, date or string formatted to be datutils parsable.
                See examples above.
            end_validation_time:
                Fetch only validation result prior to the provided time, inclusive.
                Supported format include timestamps(int), datetime, date or string formatted to be datutils parsable.
                See examples above.
            ge_type:
                If `True` returns a native Great Expectation type, Hopsworks custom type otherwise.
                Conversion can be performed via the `to_ge_type()` method on hopsworks type.

        Returns:
            A list of validation result connected to the expectation_id

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if self._id:
            return self._validation_result_engine.get_validation_history(
                expectation_id=expectation_id,
                start_validation_time=start_validation_time,
                end_validation_time=end_validation_time,
                filter_by=filter_by or [],
                ge_type=ge_type,
            )
        raise FeatureStoreException(
            "Only Feature Group registered with Hopsworks can fetch validation history."
        )

    @uses_great_expectations
    def validate(
        self,
        dataframe: pd.DataFrame | TypeVar("pyspark.sql.DataFrame") | None = None,
        expectation_suite: hsfs.expectation_suite.ExpectationSuite | None = None,
        save_report: bool = False,
        validation_options: dict[str, Any] | None = None,
        ingestion_result: Literal[
            "UNKNOWN", "INGESTED", "REJECTED", "EXPERIMENT", "FG_DATA"
        ] = "UNKNOWN",
        ge_type: bool = True,
    ) -> (
        great_expectations.core.ExpectationSuiteValidationResult
        | ValidationReport
        | None
    ):
        """Run validation based on the attached expectations.

        Runs the expectation suite attached to the feature group against the provided dataframe.
        Raise an error if the great_expectations package is not installed.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get feature group instance
            fg = fs.get_or_create_feature_group(...)

            ge_report = fg.validate(df, save_report=False)
            ```

        Parameters:
            dataframe: The dataframe to run the data validation expectations against.
            expectation_suite:
                Optionally provide an Expectation Suite to override the one that is possibly attached to the feature group.
                This is useful for testing new Expectation suites.
                When an extra suite is provided, the results will never be persisted.
            validation_options:
                Additional validation options as key-value pairs.

                - Key `"run_validation"` is a boolean value, set to `False` to skip validation temporarily on ingestion.
                - Key `"ge_validate_kwargs"` is a dictionary containing kwargs for the validate method of Great Expectations.

            ingestion_result:
                Specify the fate of the associated data.
                Use `"INGESTED"` or `"REJECTED"` for validation of DataFrames to be inserted in the Feature Group.
                Use `"EXPERIMENT"` for testing and development and `"FG_DATA"` when validating data already in the Feature Group.
            save_report:
                Whether to save the report to the backend.
                This is only possible if the Expectation suite is initialised and attached to the Feature Group.
            ge_type: Whether to return a Great Expectations object or Hopsworks own abstraction.

        Returns:
            A Validation Report produced by Great Expectations.
        """
        # Activity is logged only if the validation concerns the feature group and not a specific dataframe
        if dataframe is None:
            dataframe = self.read()
            if ingestion_result.upper() == "UNKNOWN":
                ingestion_result = "FG_DATA"

        return self._great_expectation_engine.validate(
            self,
            dataframe=engine.get_instance().convert_to_default_dataframe(dataframe),
            expectation_suite=expectation_suite,
            save_report=save_report,
            validation_options=validation_options or {},
            ingestion_result=ingestion_result.upper(),
            ge_type=ge_type,
        )

    @classmethod
    def from_response_json(
        cls, feature_group_json: dict[str, Any]
    ) -> FeatureGroup | ExternalFeatureGroup | SpineGroup:
        if (
            feature_group_json["type"] == "onDemandFeaturegroupDTO"
            and not feature_group_json["spine"]
        ):
            feature_group_obj = ExternalFeatureGroup.from_response_json(
                feature_group_json
            )
        elif (
            feature_group_json["type"] == "onDemandFeaturegroupDTO"
            and feature_group_json["spine"]
        ):
            feature_group_obj = SpineGroup.from_response_json(feature_group_json)
        else:
            feature_group_obj = FeatureGroup.from_response_json(feature_group_json)
        return feature_group_obj

    def get_feature_monitoring_configs(
        self,
        name: str | None = None,
        feature_name: str | None = None,
        config_id: int | None = None,
    ) -> fmc.FeatureMonitoringConfig | list[fmc.FeatureMonitoringConfig] | None:
        """Fetch all feature monitoring configs attached to the feature group, or fetch by name or feature name only.

        If no arguments are provided the method will return all feature monitoring configs attached to the feature group, meaning all feature monitoring configs that are attach to a feature in the feature group.
        If you wish to fetch a single config, provide the its name.
        If you wish to fetch all configs attached to a particular feature, provide the feature name.

        Example:
            ```python3
            # fetch your feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # fetch all feature monitoring configs attached to the feature group
            fm_configs = fg.get_feature_monitoring_configs()

            # fetch a single feature monitoring config by name
            fm_config = fg.get_feature_monitoring_configs(name="my_config")

            # fetch all feature monitoring configs attached to a particular feature
            fm_configs = fg.get_feature_monitoring_configs(feature_name="my_feature")

            # fetch a single feature monitoring config with a given id
            fm_config = fg.get_feature_monitoring_configs(config_id=1)
            ```

        Parameters:
            name: If provided fetch only the feature monitoring config with the given name.
            feature_name: If provided, fetch only configs attached to a particular feature.
            config_id: If provided, fetch only the feature monitoring config with the given id.

        Returns:
            A list of feature monitoring configs.
            If `name` is provided, returns either a single config or `None` if not found.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If feature group is not registered with Hopsworks.
            ValueError: If both name and feature_name are provided.
            TypeError: If name or feature_name are not string or None.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch feature monitoring configurations."
            )

        return self._feature_monitoring_config_engine.get_feature_monitoring_configs(
            name=name,
            feature_name=feature_name,
            config_id=config_id,
        )

    def get_feature_monitoring_history(
        self,
        config_name: str | None = None,
        config_id: int | None = None,
        start_time: int | str | datetime | date | None = None,
        end_time: int | str | datetime | date | None = None,
        with_statistics: bool = True,
    ) -> list[fmr.FeatureMonitoringResult]:
        """Fetch feature monitoring history for a given feature monitoring config.

        Example:
            ```python3
            # fetch your feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # fetch feature monitoring history for a given feature monitoring config
            fm_history = fg.get_feature_monitoring_history(
                config_name="my_config",
                start_time="2020-01-01",
            )

            # fetch feature monitoring history for a given feature monitoring config id
            fm_history = fg.get_feature_monitoring_history(
                config_id=1,
                start_time=datetime.now() - timedelta(weeks=2),
                end_time=datetime.now() - timedelta(weeks=1),
                with_statistics=False,
            )
            ```

        Parameters:
            config_name: The name of the feature monitoring config to fetch history for.
            config_id: The id of the feature monitoring config to fetch history for.
            start_time: The start date of the feature monitoring history to fetch.
            end_time: The end date of the feature monitoring history to fetch.
            with_statistics:
                Whether to include statistics in the feature monitoring history.
                If `False`, only metadata about the monitoring will be fetched.

        Returns:
            A list of feature monitoring results containing the monitoring metadata as well as the computed statistics for the detection and reference window if requested.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If feature group is not registered with Hopsworks.
            ValueError: If both config_name and config_id are provided.
            TypeError: If config_name or config_id are not respectively string, int or None.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch feature monitoring history."
            )

        return self._feature_monitoring_result_engine.get_feature_monitoring_results(
            config_name=config_name,
            config_id=config_id,
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

    def create_statistics_monitoring(
        self,
        name: str,
        feature_name: str | None = None,
        description: str | None = None,
        start_date_time: int | str | datetime | date | pd.Timestamp | None = None,
        end_date_time: int | str | datetime | date | pd.Timestamp | None = None,
        cron_expression: str | None = "0 0 12 ? * * *",
    ) -> fmc.FeatureMonitoringConfig:
        """Run a job to compute statistics on snapshot of feature data on a schedule.

        Experimental:
            Public API is subject to change, this feature is not suitable for production use-cases.

        Example:
            ```python3
            # fetch feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # enable statistics monitoring
            my_config = fg.create_statistics_monitoring(
                name="my_config",
                start_date_time="2021-01-01 00:00:00",
                description="my description",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                # Statistics computed on 10% of the last week of data
                time_offset="1w",
                row_percentage=0.1,
            ).save()
            ```

        Parameters:
            name:
                Name of the feature monitoring configuration.
                The name must be unique for all configurations attached to the feature group.
            feature_name:
                Name of the feature to monitor.
                If not specified, statistics will be computed for all features.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression:
                Cron expression to use to schedule the job.
                The cron expression must be in UTC and follow the Quartz specification.
                The default value means "every day at 12pm UTC".

        Returns:
            Configuration with minimal information about the feature monitoring.
            Additional information are required before feature monitoring is enabled.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If feature group is not registered with Hopsworks.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can enable scheduled statistics monitoring."
            )

        return self._feature_monitoring_config_engine._build_default_statistics_monitoring_config(
            name=name,
            feature_name=feature_name,
            description=description,
            start_date_time=start_date_time,
            valid_feature_names=[feat.name for feat in self._features],
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    def create_feature_monitoring(
        self,
        name: str,
        feature_name: str,
        description: str | None = None,
        start_date_time: int | str | datetime | date | pd.Timestamp | None = None,
        end_date_time: int | str | datetime | date | pd.Timestamp | None = None,
        cron_expression: str | None = "0 0 12 ? * * *",
    ) -> fmc.FeatureMonitoringConfig:
        """Enable feature monitoring to compare statistics on snapshots of feature data over time.

        Experimental:
            Public API is subject to change, this feature is not suitable for production use-cases.

        Example:
            ```python3
            # fetch feature group
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # enable feature monitoring
            my_config = fg.create_feature_monitoring(
                name="my_monitoring_config",
                feature_name="my_feature",
                description="my monitoring config description",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                # Data inserted in the last day
                time_offset="1d",
                window_length="1d",
            ).with_reference_window(
                # Data inserted last week on the same day
                time_offset="1w1d",
                window_length="1d",
            ).compare_on(
                metric="mean",
                threshold=0.5,
            ).save()
            ```

        Parameters:
            name:
                Name of the feature monitoring configuration.
                The name must be unique for all configurations attached to the feature group.
            feature_name: Name of the feature to monitor.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression:
                Cron expression to use to schedule the job.
                The cron expression must be in UTC and follow the Quartz specification.
                The default value means "every day at 12pm UTC".

        Returns:
            Configuration with minimal information about the feature monitoring.
            Additional information are required before feature monitoring is enabled.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If feature group is not registered with Hopsworks.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can enable feature monitoring."
            )

        return self._feature_monitoring_config_engine._build_default_feature_monitoring_config(
            name=name,
            feature_name=feature_name,
            description=description,
            start_date_time=start_date_time,
            valid_feature_names=[feat.name for feat in self._features],
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    def __getattr__(self, name: str) -> Any:
        try:
            return self.__getitem__(name)
        except KeyError as err:
            raise AttributeError(
                f"'FeatureGroup' object has no attribute '{name}'. "
                "If you are trying to access a feature, fall back on "
                "using the `get_feature` method."
            ) from err

    def __getitem__(self, name: str) -> feature.Feature:
        if not isinstance(name, str):
            raise TypeError(
                f"Expected type `str`, got `{type(name)}`. "
                "Features and transformations are accessible by name."
            )
        feature = [f for f in self.__getattribute__("_features") if f.name == name]
        transformations = [
            tf.hopsworks_udf
            for tf in self.__getattribute__("_transformation_functions")
            if tf.hopsworks_udf.function_name == name
        ]
        if len(feature) == 1:
            return feature[0]
        if len(transformations) == 1:
            return transformations[0]
        raise KeyError(
            f"'FeatureGroup' object has no feature or transformation called '{name}'."
        )

    @property
    def statistics_config(self) -> StatisticsConfig:
        """Statistics configuration object defining the settings for statistics computation of the feature group.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If statistics are not supported for this feature group type.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_config

    @statistics_config.setter
    def statistics_config(
        self,
        statistics_config: StatisticsConfig | dict[str, Any] | bool | None,
    ) -> None:
        self._check_statistics_support()  # raises an error if stats not supported
        if isinstance(statistics_config, StatisticsConfig):
            self._statistics_config = statistics_config
        elif isinstance(statistics_config, dict):
            self._statistics_config = StatisticsConfig(**statistics_config)
        elif isinstance(statistics_config, bool):
            self._statistics_config = StatisticsConfig(statistics_config)
        elif statistics_config is None:
            self._statistics_config = StatisticsConfig()
        else:
            raise TypeError(
                f"The argument `statistics_config` has to be `None` of type `StatisticsConfig, `bool` or `dict`, but is of type: `{type(statistics_config)}`"
            )

    def get_latest_online_ingestion(self) -> online_ingestion.OnlineIngestion:
        """Retrieve the latest online ingestion operation for this feature group.

        This method fetches metadata about the most recent online ingestion job, including its status and progress, if available.

        Returns:
            The latest OnlineIngestion object for this feature group.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.

        Example:
            ```python
            fg = fs.get_feature_group("my_fg", 1)
            latest_ingestion = fg.get_latest_online_ingestion()
            ```
        """
        return online_ingestion_api.OnlineIngestionApi().get_online_ingestion(
            self, query_params={"filter_by": "LATEST"}
        )

    def get_online_ingestion(self, id) -> online_ingestion.OnlineIngestion:
        """Retrieve a specific online ingestion operation by its ID for this feature group.

        This method fetches metadata about a particular online ingestion job, including its status and progress, if available.

        Parameters:
            id: The unique identifier of the online ingestion operation.

        Returns:
            The OnlineIngestion object with the specified ID.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.

        Example:
            ```python
            fg = fs.get_feature_group("my_fg", 1)
            ingestion = fg.get_online_ingestion(123)
            ```
        """
        return online_ingestion_api.OnlineIngestionApi().get_online_ingestion(
            self, query_params={"filter_by": f"ID:{id}"}
        )

    @property
    def feature_store_id(self) -> int | None:
        """ID of the feature store to which the feature group belongs."""
        return self._feature_store_id

    @property
    def feature_store(self) -> feature_store_mod.FeatureStore:
        """Feature store to which the feature group belongs."""
        if self._feature_store is None:
            self._feature_store = feature_store_api.FeatureStoreApi().get(
                self._feature_store_id
            )
        return self._feature_store

    @feature_store.setter
    def feature_store(self, feature_store: feature_store_mod.FeatureStore) -> None:
        self._feature_store = feature_store

    @property
    def id(self) -> int | None:
        """Feature group id."""
        return self._id

    @property
    def name(self) -> str | None:
        """Name of the feature group."""
        return self._name

    @property
    def version(self) -> int | None:
        """Version number of the feature group."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @property
    def missing_mandatory_tags(self) -> list[dict[str, Any]]:
        """List of missing mandatory tags for the feature group."""
        return self._missing_mandatory_tags

    def get_fg_name(self) -> str:
        """Returns the full feature group name, that is, its base name combined with its version."""
        return f"{self.name}_{self.version}"

    @property
    def statistics(self) -> Statistics | None:
        """Get the latest computed statistics for the whole feature group.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_engine.get(self)

    @property
    def primary_key(self) -> list[str]:
        """List of features building the primary key."""
        return self._primary_key

    @primary_key.setter
    def primary_key(self, new_primary_key: list[str]) -> None:
        self._primary_key = [
            util.autofix_feature_name(pk, warn=True) for pk in new_primary_key
        ]

    def get_statistics(
        self,
        computation_time: str | float | datetime | date | None = None,
        feature_names: list[str] | None = None,
    ) -> Statistics | None:
        """Returns the statistics computed at a specific time for the current feature group.

        If `computation_time` is `None`, the most recent statistics are returned.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg_statistics = fg.get_statistics(computation_time=None)
            ```

        Parameters:
            computation_time:
                Date and time when statistics were computed.
                Strings should be formatted in one of the following formats: `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            feature_names: List of feature names of which statistics are retrieved.

        Returns:
            `Statistics`. Statistics object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If statistics are not supported for this feature group type.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_engine.get(
            self, computation_time=computation_time, feature_names=feature_names
        )

    def get_all_statistics(
        self,
        computation_time: str | float | datetime | date | None = None,
        feature_names: list[str] | None = None,
    ) -> list[Statistics] | None:
        """Returns all the statistics metadata computed before a specific time for the current feature group.

        If `computation_time` is `None`, all the statistics metadata are returned.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg_statistics = fg.get_statistics(computation_time=None)
            ```

        Parameters:
            computation_time:
                Date and time when statistics were computed.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            feature_names: List of feature names of which statistics are retrieved.

        Returns:
            Statistics object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If statistics are not supported for this feature group type.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        return self._statistics_engine.get_all(
            self, computation_time=computation_time, feature_names=feature_names
        )

    def compute_statistics(self) -> None:
        """Recompute the statistics for the feature group and save them to the feature store.

        Statistics are only computed for data in the offline storage of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            statistics_metadata = fg.compute_statistics()
            ```

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If statistics are not supported for this feature group type.
        """
        self._check_statistics_support()  # raises an error if stats not supported
        if self.statistics_config.enabled:
            # Don't read the dataframe here, to avoid triggering a read operation
            # for the Python engine. The Python engine is going to setup a Spark Job
            # to update the statistics.
            self._statistics_engine.compute_and_save_statistics(self)
        else:
            warnings.warn(
                (
                    f"The statistics are not enabled of feature group `{self._name}`, with version"
                    f" `{self._version}`. No statistics computed."
                ),
                util.StorageWarning,
                stacklevel=1,
            )

    def get_alerts(self) -> list[FeatureGroupAlert]:
        """Get all alerts for this feature group.

        Returns:
            The list of FeatureGroupAlerts.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.

        Example:
            ```python
            # Get all alerts
            alerts = fg.get_alerts()
            ```
        """
        return self._alert_api.get_feature_group_alerts(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._id,
        )

    def get_alert(self, alert_id: int) -> FeatureGroupAlert:
        """Get an alert for this feature group by ID.

        Parameters:
            alert_id: The ID of the alert to get.

        Returns:
            The FeatureGroupAlert object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.

        Example:
            ```python
            # Get a specific alert
            alert = fg.get_alert(alert_id=1)
            ```
        """
        return self._alert_api.get_feature_group_alert(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._id,
            alert_id=alert_id,
        )

    def create_alert(
        self,
        receiver: str,
        status: Literal[
            "feature_validation_success",
            "feature_validation_warning",
            "feature_validation_failure",
            "feature_monitor_shift_undetected",
            "feature_monitor_shift_detected",
        ],
        severity: Literal["info", "warning", "critical"],
    ) -> FeatureGroupAlert:
        """Create an alert for this feature group.

        Parameters:
            receiver: The receiver of the alert.
            status: The status that will trigger the alert.
            severity: The severity of the alert.

        Returns:
            The created FeatureGroupAlert object.

        Raises:
            ValueError: If the status is not valid.
            ValueError: If the severity is not valid.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.

        Example:
            ```python
            fg.create_alert(
                receiver="email",
                status="feature_validation_failure",
                severity="critical",
            )
            ```
        """
        return self._alert_api.create_feature_group_alert(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._id,
            receiver=receiver,
            status=status,
            severity=severity,
        )

    @property
    def embedding_index(self) -> EmbeddingIndex | None:
        # TODO: Add docstring
        if self._embedding_index:
            self._embedding_index.feature_group = self
        return self._embedding_index

    @embedding_index.setter
    def embedding_index(self, embedding_index: EmbeddingIndex | None) -> None:
        if embedding_index is not None and self._id is None:
            self.online_enabled = True
        self._embedding_index = embedding_index

    @property
    def event_time(self) -> str | None:
        """Event time feature in the feature group."""
        return self._event_time

    @event_time.setter
    def event_time(self, feature_name: str | None) -> None:
        if feature_name is None:
            self._event_time = None
            return
        if isinstance(feature_name, str):
            self._event_time = util.autofix_feature_name(feature_name, warn=True)
            return
        if (
            isinstance(feature_name, list)
            and len(feature_name) == 1
            and isinstance(feature_name[0], str)
        ):
            warnings.warn(
                "Providing event_time as a single-element list is deprecated"
                " and will be dropped in future versions. Provide the feature_name string instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self._event_time = util.autofix_feature_name(feature_name[0], warn=True)
            return

        raise ValueError(
            "event_time must be a string corresponding to an existing feature name of the Feature Group."
        )

    @property
    def location(self) -> str | None:
        # TODO: Add docstring
        return self._location

    @property
    def expectation_suite(
        self,
    ) -> hsfs.expectation_suite.ExpectationSuite | None:
        """Expectation Suite configuration object defining the settings for data validation of the feature group."""
        return self._expectation_suite

    @expectation_suite.setter
    def expectation_suite(
        self,
        expectation_suite: (
            hsfs.expectation_suite.ExpectationSuite
            | great_expectations.core.ExpectationSuite
            | dict[str, Any]
            | None
        ),
    ) -> None:
        if isinstance(expectation_suite, hsfs.expectation_suite.ExpectationSuite):
            tmp_expectation_suite = expectation_suite.to_json_dict(decamelize=True)
            tmp_expectation_suite["feature_group_id"] = self._id
            tmp_expectation_suite["feature_store_id"] = self._feature_store_id
            self._expectation_suite = hsfs.expectation_suite.ExpectationSuite(
                **tmp_expectation_suite
            )
        elif HAS_GREAT_EXPECTATIONS and isinstance(
            expectation_suite,
            great_expectations.core.expectation_suite.ExpectationSuite,
        ):
            self._expectation_suite = hsfs.expectation_suite.ExpectationSuite(
                **expectation_suite.to_json_dict(),
                feature_store_id=self._feature_store_id,
                feature_group_id=self._id,
            )
        elif isinstance(expectation_suite, dict):
            tmp_expectation_suite = expectation_suite.copy()
            tmp_expectation_suite["feature_store_id"] = self._feature_store_id
            tmp_expectation_suite["feature_group_id"] = self._id
            self._expectation_suite = hsfs.expectation_suite.ExpectationSuite(
                **tmp_expectation_suite
            )
        elif expectation_suite is None:
            self._expectation_suite = None
        else:
            raise TypeError(
                f"The argument `expectation_suite` has to be `None` of type `ExpectationSuite` or `dict`, but is of type: `{type(expectation_suite)}`"
            )

    @property
    def online_enabled(self) -> bool:
        """Setting if the feature group is available in online storage."""
        return self._online_enabled

    @online_enabled.setter
    def online_enabled(self, online_enabled: bool) -> None:
        self._online_enabled = online_enabled

    @property
    def storage_connector(self) -> sc.StorageConnector:
        """Get the storage connector.

        !!! warning "Deprecated"
            `storage_connector` method is deprecated. Use
            `data_source` instead.
        """
        return self._data_source.storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector: sc.StorageConnector) -> None:
        if self._data_source is None:
            self._data_source = ds.DataSource()
        self._data_source.storage_connector = storage_connector

    @property
    def data_source(self) -> ds.DataSource:
        return self._data_source

    @data_source.setter
    def data_source(self, data_source: ds.DataSource) -> None:
        self._data_source = data_source
        if self._data_source is not None:
            self._data_source._update_storage_connector(self.storage_connector)

    def prepare_spark_location(self) -> str:
        # TODO: Add docstring
        location = self.location
        if self.data_source is not None and self.data_source.storage_connector:
            location = self.data_source.storage_connector.prepare_spark(location)
        return location

    @property
    def topic_name(self) -> str | None:
        """The topic used for feature group data ingestion."""
        return self._topic_name

    @topic_name.setter
    def topic_name(self, topic_name: str | None) -> None:
        self._topic_name = topic_name

    @property
    def notification_topic_name(self) -> str | None:
        """The topic used for feature group notifications."""
        return self._notification_topic_name

    @notification_topic_name.setter
    def notification_topic_name(self, notification_topic_name: str | None) -> None:
        self._notification_topic_name = notification_topic_name

    @property
    def deprecated(self) -> bool:
        """Setting if the feature group is deprecated."""
        return self._deprecated

    @deprecated.setter
    def deprecated(self, deprecated: bool) -> None:
        self._deprecated = deprecated

    @property
    def subject(self) -> dict[str, Any]:
        """Subject of the feature group."""
        if self._subject is None:
            # cache the schema
            self._subject = self._feature_group_engine.get_subject(self)
        return self._subject

    @property
    def avro_schema(self) -> str:
        """Avro schema representation of the feature group."""
        return self.subject["schema"]

    def get_complex_features(self) -> list[str]:
        """Returns the names of all features with a complex data type in this feature group.

        Example:
            ```python
            complex_dtype_features = fg.get_complex_features()
            ```
        """
        return [f.name for f in self.features if f.is_complex()]

    def _get_encoded_avro_schema(self) -> str:
        complex_features = self.get_complex_features()
        schema = json.loads(self.avro_schema)

        for field in schema["fields"]:
            if field["name"] in complex_features:
                field["type"] = ["null", "bytes"]

        schema_s = json.dumps(schema)
        try:
            avro.schema.parse(schema_s)
        except avro.schema.SchemaParseException as e:
            raise FeatureStoreException(f"Failed to construct Avro Schema: {e}") from e
        return schema_s

    def _get_feature_avro_schema(self, feature_name: str) -> str | None:
        for field in json.loads(self.avro_schema)["fields"]:
            if field["name"] == feature_name:
                return json.dumps(field["type"])
        return None

    @property
    def features(self) -> list[feature.Feature]:
        """Feature Group schema (alias)."""
        return self._features

    @property
    def schema(self) -> list[feature.Feature]:
        """Feature Group schema."""
        return self._features

    def _are_statistics_missing(self, statistics: Statistics) -> bool:
        if not self.statistics_config.enabled:
            return False
        if statistics is None:
            return True
        if (
            self.statistics_config.histograms
            or self.statistics_config.correlations
            or self.statistics_config.exact_uniqueness
        ):
            # if statistics are missing, recompute and update statistics.
            # We need to check for missing statistics because the statistics config can have been modified
            for fds in statistics.feature_descriptive_statistics:
                if fds.feature_type in ["Integral", "Fractional"]:
                    if self.statistics_config.histograms and (
                        fds.extended_statistics is None
                        or "histogram" not in fds.extended_statistics
                    ):
                        return True

                    if self.statistics_config.correlations and (
                        fds.extended_statistics is None
                        or "correlations" not in fds.extended_statistics
                    ):
                        return True

                if self.statistics_config.exact_uniqueness and fds.uniqueness is None:
                    return True

        return False

    def _are_statistics_supported(self) -> bool:
        """Whether statistics are supported or not for the current Feature Group type."""
        return not isinstance(self, SpineGroup)

    def _check_statistics_support(self) -> None:
        """Check for statistics support on the current Feature Group type."""
        if not self._are_statistics_supported():
            raise FeatureStoreException(
                "Statistics not supported for this Feature Group type"
            )

    @features.setter
    def features(self, new_features: list[feature.Feature]) -> None:
        self._features = new_features

    def _get_project_name(self) -> str:
        return util.strip_feature_store_suffix(self.feature_store_name)

    @property
    def ttl(self) -> int | None:
        """Get the time-to-live duration in seconds for features in this group.

        The TTL determines how long features should be retained before being automatically removed.
        The value is always returned in seconds, regardless of how it was originally specified.

        Returns:
            The TTL value in seconds, or `None` if no TTL is set.
        """
        return self._ttl

    @ttl.setter
    def ttl(self, new_ttl: float | timedelta | None) -> None:
        """Set the time-to-live duration for features in this group.

        The value is stored internally in seconds.

        Parameters:
            new_ttl:
                The new TTL value.
                Can be specified as:

                - An integer or float representing seconds
                - A timedelta object
                - `None` to remove TTL
        """
        if new_ttl is not None:
            if isinstance(new_ttl, timedelta):
                self._ttl = int(new_ttl.total_seconds())
            else:
                self._ttl = int(new_ttl)
        else:
            self._ttl = None

    @property
    def ttl_enabled(self) -> bool:
        """Get whether TTL (time-to-live) is enabled for this feature group.

        Returns:
            `True` if TTL is enabled, `False` otherwise
        """
        return self._ttl_enabled

    @ttl_enabled.setter
    def ttl_enabled(self, enabled: bool) -> None:
        """Set whether TTL (time-to-live) is enabled for this feature group.

        Parameters:
            enabled: Whether TTL should be enabled.
        """
        self._ttl_enabled = enabled

    def enable_ttl(
        self,
        ttl: float | timedelta | None = None,
    ) -> FeatureGroupBase | FeatureGroup | ExternalFeatureGroup | SpineGroup:
        """Enable or update the time-to-live (TTL) configuration of the feature group.

        If ttl is not set, the feature group will be enabled with the last TTL value being set.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # Enable TTL with a TTL of 7 days
            fg.enable_ttl(timedelta(days=7))

            # Disable TTL
            fg.disable_ttl()

            # Enable TTL again with a TTL of 7 days
            fg.enable_ttl()
            ```

        Info: Safe update
            This method updates the TTL configuration safely.
            In case of failure your local metadata object will keep the old configuration.

        Parameters:
            ttl:
                Optional new TTL value.
                Can be specified as:

                - An integer or float representing seconds
                - A timedelta object
                - `None` to keep current value

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.update_ttl(self, ttl, True)
        return self

    def disable_ttl(self) -> FeatureGroup:
        """Disable the time-to-live (TTL) configuration of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # Disable TTL
            fg.disable_ttl()
            ```

        Info: Safe update
            This method updates the TTL configuration safely.
            In case of failure your local metadata object will keep the old configuration.

        Returns:
            The updated feature group object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.update_ttl(self, None, False)
        return self


@public
@typechecked
class FeatureGroup(FeatureGroupBase):
    # TODO: Add docstring
    CACHED_FEATURE_GROUP = "CACHED_FEATURE_GROUP"
    STREAM_FEATURE_GROUP = "STREAM_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        name: str,
        version: int | None,
        featurestore_id: int,
        description: str | None = "",
        partition_key: list[str] | None = None,
        primary_key: list[str] | None = None,
        foreign_key: list[str] | None = None,
        hudi_precombine_key: str | None = None,
        featurestore_name: str | None = None,
        embedding_index: EmbeddingIndex | None = None,
        created: str | None = None,
        creator: dict[str, Any] | None = None,
        id: int | None = None,
        features: list[feature.Feature | dict[str, Any]] | None = None,
        location: str | None = None,
        online_enabled: bool = False,
        time_travel_format: str | None = None,
        statistics_config: StatisticsConfig | dict[str, Any] | None = None,
        online_topic_name: str | None = None,
        topic_name: str | None = None,
        notification_topic_name: str | None = None,
        event_time: str | None = None,
        stream: bool = False,
        expectation_suite: (
            great_expectations.core.ExpectationSuite
            | hsfs.expectation_suite.ExpectationSuite
            | dict[str, Any]
            | None
        ) = None,
        parents: list[explicit_provenance.Links] | None = None,
        href: str | None = None,
        delta_streamer_job_conf: (
            dict[str, Any] | deltastreamer_jobconf.DeltaStreamerJobConf | None
        ) = None,
        deprecated: bool = False,
        transformation_functions: (
            list[TransformationFunction | HopsworksUdf] | None
        ) = None,
        online_config: OnlineConfig | dict[str, Any] | None = None,
        offline_backfill_every_hr: str | int | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        ttl: float | timedelta | None = None,
        ttl_enabled: bool | None = None,
        online_disk: bool | None = None,
        sink_enabled: bool | None = False,
        sink_job_conf: SinkJobConfiguration | dict[str, Any] | None = None,
        sink_job: job.Job | dict[str, Any] | None = None,
        missing_mandatory_tags: list[dict[str, Any]] | None = None,
        tags: list[tag.Tag] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            name,
            version,
            featurestore_id,
            location,
            event_time=event_time,
            online_enabled=online_enabled,
            embedding_index=embedding_index,
            id=id,
            expectation_suite=expectation_suite,
            online_topic_name=online_topic_name,
            topic_name=topic_name,
            notification_topic_name=notification_topic_name,
            deprecated=deprecated,
            online_config=online_config,
            data_source=data_source,
            ttl=ttl,
            ttl_enabled=ttl_enabled,
            online_disk=online_disk,
            sink_enabled=sink_enabled,
            missing_mandatory_tags=missing_mandatory_tags,
        )

        self._feature_store_name: str | None = featurestore_name
        self._description: str | None = description
        self._created = created
        self._creator = user.User.from_response_json(creator)
        self._sink_job = sink_job

        if sink_job_conf is not None and isinstance(sink_job_conf, dict):
            self._sink_job_conf = SinkJobConfiguration.from_response_json(sink_job_conf)
        else:
            self._sink_job_conf = sink_job_conf

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._time_travel_format = (
            time_travel_format.upper() if time_travel_format is not None else None
        )

        self._stream = stream
        self._parents = parents
        self._deltastreamer_jobconf = delta_streamer_job_conf
        self._tags: list[tag.Tag] | None = tags

        self._materialization_job: Job = None

        if self._id:
            # initialized by backend
            self.primary_key: list[str] = [
                feat.name for feat in self._features if feat.primary is True
            ]
            self.foreign_key: list[str] = [
                feat.name for feat in self._features if feat.foreign is True
            ]
            self._partition_key: list[str] = [
                feat.name for feat in self._features if feat.partition is True
            ]
            if (
                time_travel_format is not None
                and time_travel_format.upper() == "HUDI"
                and self._features
            ):
                # hudi precombine key is always a single feature
                self._hudi_precombine_key: str | None = [
                    feat.name
                    for feat in self._features
                    if feat.hudi_precombine_key is True
                ][0]
            else:
                self._hudi_precombine_key: str | None = None

            self.statistics_config = statistics_config
            self._offline_backfill_every_hr = None

        else:
            self._resolve_sink_enabled()
            # Set time travel format and streaming based on engine type and online status
            self._init_time_travel_and_stream(
                stream,
                time_travel_format,
                self.online_enabled,  # use the getter of the super class to take into account embedding index
                self._is_hopsfs_storage(),
            )

            self.primary_key = primary_key
            self.foreign_key = foreign_key
            self.partition_key = partition_key
            self._hudi_precombine_key = (
                util.autofix_feature_name(hudi_precombine_key, warn=True)
                if hudi_precombine_key is not None
                and (
                    self._time_travel_format is None
                    or self._time_travel_format == "HUDI"
                )
                else None
            )
            self.statistics_config = statistics_config
            self._offline_backfill_every_hr = offline_backfill_every_hr

        self._feature_group_engine: feature_group_engine.FeatureGroupEngine = (
            feature_group_engine.FeatureGroupEngine(featurestore_id)
        )
        self._vector_db_client: VectorDbClient | None = None
        self._href: str | None = href

        # cache for optimized writes
        self._kafka_producer: confluent_kafka.Producer | None = None
        self._feature_writers: dict[str, callable] | None = None
        self._writer: callable | None = None
        self._kafka_headers: dict[str, bytes] | None = None
        # On-Demand Transformation Functions
        self._transformation_functions: list[TransformationFunction] = []

        if transformation_functions:
            for transformation_function in transformation_functions:
                if not isinstance(transformation_function, TransformationFunction):
                    self._transformation_functions.append(
                        TransformationFunction(
                            featurestore_id,
                            hopsworks_udf=transformation_function,
                            version=1,
                            transformation_type=TransformationType.ON_DEMAND,
                        )
                    )
                else:
                    if (
                        not transformation_function.transformation_type
                        or transformation_function.transformation_type
                        == TransformationType.UNDEFINED
                    ):
                        transformation_function.transformation_type = (
                            TransformationType.ON_DEMAND
                        )
                    self._transformation_functions.append(transformation_function)

        if self._transformation_functions:
            self._transformation_functions = (
                FeatureGroup._sort_transformation_functions(
                    self._transformation_functions
                )
            )

    def _init_time_travel_and_stream(
        self,
        stream: bool,
        time_travel_format: str | None,
        online_enabled: bool,
        is_hopsfs: bool,
    ) -> None:
        """Initialize `self._time_travel_format` and `self._stream` for new objects.

        Extracted into testable helpers to simplify unit testing.
        """
        self._time_travel_format = FeatureGroup._resolve_time_travel_format(
            time_travel_format=time_travel_format,
            online_enabled=online_enabled,
            is_hopsfs=is_hopsfs,
        )

        if engine.get_type() == "python" and not self._sink_enabled:
            self._stream = FeatureGroup._resolve_stream_python(
                stream=stream,
                time_travel_format=self._time_travel_format,
                is_hopsfs=is_hopsfs,
                online_enabled=online_enabled,
            )

    def _is_hopsfs_storage(self) -> bool:
        """Return True if storage is HopsFS."""
        return self.storage_connector is None or (
            self.storage_connector is not None
            and self.storage_connector.type == sc.StorageConnector.HOPSFS
        )

    def _resolve_sink_enabled(self):
        """Check if sink enabled must enabled based on storage connector."""
        self._sink_enabled = (
            self.storage_connector is not None
            and self.storage_connector.type
            in [sc.StorageConnector.CRM, sc.StorageConnector.REST]
        )

    @staticmethod
    def _resolve_stream_python(
        stream: bool,
        time_travel_format: str,
        is_hopsfs: bool,
        online_enabled: bool,
    ) -> bool | None:
        # If stream is explicitly set stream to True, use it.
        # Otherwise, resolve it based on time travel format and other flags.
        return stream or not (
            is_hopsfs and time_travel_format == "DELTA" and not online_enabled
        )

    @staticmethod
    def _resolve_time_travel_format(
        time_travel_format: str | None,
        online_enabled: bool,
        is_hopsfs: bool,
    ) -> str:
        """Resolve only the time travel format string."""
        fmt = time_travel_format.upper() if time_travel_format is not None else None
        if fmt is None:
            if not FeatureGroup._has_deltalake():
                return "HUDI"
            return "DELTA"
        return fmt

    @staticmethod
    def _has_deltalake():
        if engine.get_type() == "python":
            return HAS_DELTALAKE_PYTHON
        return HAS_DELTALAKE_SPARK

    @staticmethod
    def _sort_transformation_functions(
        transformation_functions: list[TransformationFunction],
    ) -> list[TransformationFunction]:
        """Function that sorts transformation functions in the order of the output column names.

        The list of transformation functions are sorted based on the output columns names to maintain consistent ordering.

        Parameters:
            transformation_functions: List of transformation functions to be sorted.

        Returns:
            The sorted list of transformation functions.
        """
        return sorted(transformation_functions, key=lambda x: x.output_column_names[0])

    @public
    def read(
        self,
        wallclock_time: str | int | datetime | date | None = None,
        online: bool = False,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
        read_options: dict | None = None,
    ) -> (
        pd.DataFrame
        | np.ndarray
        | list[list[Any]]
        | TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pl.DataFrame
    ):
        """Read the feature group into a dataframe.

        Reads the feature group by default from the offline storage as Spark DataFrame on Hopsworks and Databricks, and as Pandas dataframe on AWS Sagemaker and pure Python environments.

        Set `online` to `True` to read from the online storage, or change `dataframe_type` to read as a different format.

        Example: Reading feature group as of latest state
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)
            fg.read()
            ```

        Example: Reading feature group as of specific point in time:
            ```python
            fg = fs.get_or_create_feature_group(...)
            fg.read("2020-10-20 07:34:11")
            ```

        Parameters:
            wallclock_time:
                If specified, retrieves feature group as of specific point in time.
                If not specified, returns as of most recent time.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            online: If `True`, read from online feature store.
            dataframe_type:
                The type of the returned dataframe.
                By default, maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            read_options:
                Additional options as key/value pairs to pass to the execution engine.

                For spark engine: Dictionary of read options for Spark.

                For python engine:
                - key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`.
                - key `"pandas_types"` and value `True` to retrieve columns as [Pandas nullable types](https://pandas.pydata.org/docs/user_guide/integer_na.html) rather than numpy/object(string) types (experimental).

        Returns:
            One of the following:

            - `DataFrame`: The spark dataframe containing the feature data.
            - `pyspark.DataFrame`: A Spark DataFrame.
            - `pandas.DataFrame`: A Pandas DataFrame.
            - `polars.DataFrame`: A Polars DataFrame.
            - `numpy.ndarray`: A two-dimensional Numpy array.
            - `list`: A two-dimensional Python list.

        Raises:
            hopsworks.client.exceptions.RestAPIError: No data is available for feature group with this commit date, if time travel enabled.
        """
        if wallclock_time and self._time_travel_format is None:
            raise FeatureStoreException(
                "Time travel format is not set for the feature group, cannot read as of specific point in time."
            )
        if wallclock_time and engine.get_type() == "python":
            raise FeatureStoreException(
                "Python environments does not support incremental queries. "
                "Read feature group without timestamp to retrieve latest snapshot or switch to "
                "environment with Spark Engine."
            )

        engine.get_instance().set_job_group(
            "Fetching Feature group",
            f"Getting feature group: {self._name} from the featurestore {self._feature_store_name}",
        )

        if wallclock_time:
            return (
                self.select_all()
                .as_of(wallclock_time)
                .read(
                    online,
                    dataframe_type,
                    read_options or {},
                )
            )
        return self.select_all().read(
            online,
            dataframe_type,
            read_options or {},
        )

    @public
    def read_changes(
        self,
        start_wallclock_time: str | int | datetime | date,
        end_wallclock_time: str | int | datetime | date,
        read_options: dict | None = None,
    ) -> (
        pd.DataFrame
        | np.ndarray
        | list[list[Any]]
        | TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pl.DataFrame
    ):
        """Reads updates of this feature that occurred between specified points in time.

        Warning: Deprecated
            `read_changes` method is deprecated.
            Use `as_of(end_wallclock_time, exclude_until=start_wallclock_time).read(read_options=read_options)` instead.

        Warning: Pyspark/Spark Only
            Apache HUDI exclusively supports Time Travel and Incremental Query via Spark Context.

        Warning:
            This function only works for feature groups with time_travel_format='HUDI'.

        Parameters:
            start_wallclock_time:
                Start time of the time travel query.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            end_wallclock_time:
                End time of the time travel query.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            read_options:
                Additional options as key/value pairs to pass to the execution engine.
                For spark engine, it is a dictionary of read options for Spark.

        Returns:
            The spark dataframe containing the incremental changes of feature data.

        Raises:
            hopsworks.client.exceptions.RestAPIError: No data is available for feature group with this commit date.
            hopsworks.client.exceptions.FeatureStoreException: If the feature group does not have `HUDI` time travel format.
        """
        return (
            self.select_all()
            .pull_changes(start_wallclock_time, end_wallclock_time)
            .read(False, "default", read_options or {})
        )

    @public
    def find_neighbors(
        self,
        embedding: list[int | float],
        col: str | None = None,
        k: int | None = 10,
        filter: Filter | Logic | None = None,
        options: dict | None = None,
    ) -> list[tuple[float, list[Any]]]:
        """Finds the nearest neighbors for a given embedding in the vector database.

        If `filter` is specified, or if embedding feature is stored in default project index, the number of results returned may be less than k.
        Try using a large value of k and extract the top k items from the results if needed.

        Parameters:
            embedding: The target embedding for which neighbors are to be found.
            col:
                The column name used to compute similarity score.
                Required only if there are multiple embeddings.
            k: The number of nearest neighbors to retrieve.
            filter: A filter expression to restrict the search space.
            options:
                The options used for the request to the vector database.
                The keys are attribute values of the `hsfs.core.opensearch.OpensearchRequestOption` class.

        Returns:
            A list of tuples representing the nearest neighbors.
            Each tuple contains: `(The similarity score, A list of feature values)`

        Example:
            ```
            embedding_index = EmbeddingIndex()
            embedding_index.add_embedding(name="user_vector", dimension=3)
            fg = fs.create_feature_group(
                        name='air_quality',
                        embedding_index = embedding_index,
                        version=1,
                        primary_key=['id1'],
                        online_enabled=True,
                    )
            fg.insert(data)
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
            )

            # apply filter
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
                filter=(fg.id1 > 10) & (fg.id1 < 30)
            )
            ```
        """
        if self._vector_db_client is None and self._embedding_index:
            self._vector_db_client = VectorDbClient(self.select_all())
        results = self._vector_db_client.find_neighbors(
            embedding,
            feature=(self.__getattr__(col) if col else None),
            k=k,
            filter=filter,
            options=options,
        )
        return [
            (result[0], [result[1][f.name] for f in self.features])
            for result in results
        ]

    @public
    def show(self, n: int, online: bool = False) -> list[list[Any]]:
        """Show the first `n` rows of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # make a query and show top 5 rows
            fg.select(['date','weekly_sales','is_holiday']).show(5)
            ```

        Parameters:
            n: Number of rows to show.
            online: If `True` read from online feature store.
        """
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            f"Getting feature group: {self._name} from the featurestore {self._feature_store_name}",
        )
        return self.select_all().show(n, online)

    @public
    def save(
        self,
        features: (
            pd.DataFrame
            | pl.DataFrame
            | TypeVar("pyspark.sql.DataFrame")
            | TypeVar("pyspark.RDD")
            | np.ndarray
            | list[feature.Feature]
        ) = None,
        write_options: dict[str, Any] | None = None,
        validation_options: dict[str, Any] | None = None,
        wait: bool = False,
    ) -> tuple[
        Job | None,
        great_expectations.core.ExpectationSuiteValidationResult | None,
    ]:
        """Persist the metadata and materialize the feature group to the feature store.

        Warning: Changed in 3.3.0
            `insert` and `save` methods are now async by default in non-spark clients.
            To achieve the old behaviour, set `wait` argument to `True`.

        Calling `save` creates the metadata for the feature group in the feature store.
        If a Pandas DataFrame, Polars DatFrame, RDD or Ndarray is provided, the data is written to the online/offline feature store as specified.
        By default, this writes the feature group to the offline storage, and if `online_enabled` for the feature group, also to the online feature store.
        The `features` dataframe can be a Spark DataFrame or RDD, a Pandas DataFrame, or a two-dimensional Numpy array or a two-dimensional Python nested list.

        Parameters:
            features:
                Features to be saved.
                This argument is optional if the feature list is provided in the `create_feature_group` or in the `get_or_create_feature_group` method invokation.
            write_options:
                Additional write options as key-value pairs.

                When using the `python` engine, write_options can contain the following entries:

                - key `spark` and value an object of type [hsfs.core.job_configuration.JobConfiguration][hsfs.core.job_configuration.JobConfiguration] to configure the Hopsworks Job used to write data into the feature group.
                - key `wait_for_job` and value `True` or `False` to configure whether or not to the save call should return only after the Hopsworks Job has finished.
                  By default it does not wait.
                - key `wait_for_online_ingestion` and value `True` or `False` to configure whether or not to the save call should return only after the Hopsworks online ingestion has finished.
                  By default it does not wait.
                - key `start_offline_backfill` and value `True` or `False` to configure whether or not to start the materialization job to write data to the offline storage. `start_offline_backfill` is deprecated.
                  Use `start_offline_materialization` instead.
                - key `start_offline_materialization` and value `True` or `False` to configure whether or not to start the materialization job to write data to the offline storage.
                  By default the materialization job gets started immediately.
                - key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln) used to configure the Kafka client.
                  To optimize for throughput in high latency connection, consider changing the [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                - key `internal_kafka` and value `True` or `False` in case you established connectivity from you Python environment to the internal advertised listeners of the Hopsworks Kafka Cluster.
                  Defaults to `False` and will use external listeners when connecting from outside of Hopsworks.
                - key `delta.enableChangeDataFeed` set to a *string* value of true or false to enable or disable cdf operations on the feature group delta table.
                  Set to true by default on Feature Group creation.

            validation_options:
                Additional validation options as key-value pairs.

                - key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                - key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                - key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
                - key `schema_validation` boolean value, set to `True` to validate the schema.

            wait:
                Wait for job and online ingestion to finish before returning.
                Shortcut for write_options `{"wait_for_job": False, "wait_for_online_ingestion": False}`.

        Returns:
            When using the `python` engine, it returns the Hopsworks Job that was launched to ingest the feature group data.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._resolve_sink_enabled()
        if self._sink_enabled and self.storage_connector is None:
            raise FeatureStoreException(
                "Sink cannot be enabled for the feature group without a storage connector."
            )
        if self._sink_enabled and self._data_source is None:
            raise FeatureStoreException(
                "Sink cannot be enabled for the feature group without a data source."
            )
        if self._sink_enabled and self.time_travel_format != "DELTA":
            raise FeatureStoreException(
                "Sink can only be enabled for feature groups with time travel format DELTA."
            )

        if write_options is None:
            write_options = {}
        if all(
            [
                not self._id,
                self.time_travel_format == "DELTA",
                write_options.get("delta.enableChangeDataFeed") != "false",
            ]
        ):
            # New delta FG allow for change data capture query
            write_options["delta.enableChangeDataFeed"] = "true"

        if (
            (features is None and len(self._features) > 0)
            or (
                isinstance(features, list)
                and len(features) > 0
                and all(isinstance(f, feature.Feature) for f in features)
            )
            or (features is None and len(self.transformation_functions) > 0)
        ):
            # This is done for compatibility. Users can specify the feature list in the
            # (get_or_)create_feature_group. Users can also provide the feature list in the save().
            # Though it's an optional parameter.
            # For consistency reasons if the user specify both the feature list in the (get_or_)create_feature_group
            # and in the `save()` call, then the (get_or_)create_feature_group wins.
            # This is consistent with the behavior of the insert method where the feature list wins over the
            # dataframe structure
            self._features = (
                self._features
                if len(self._features) > 0
                else features
                if features
                else []
            )

            self._features = self._feature_group_engine._update_feature_group_schema_on_demand_transformations(
                self, self._features
            )

            self._feature_group_engine.save_feature_group_metadata(
                self, None, write_options or {}
            )

            return None, None

        if features is None:
            raise FeatureStoreException(
                "Feature list not provided in the create_feature_group or get_or_create_feature_group invokations."
                " Please provide a list of features or a Dataframe"
            )

        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        user_version = self._version

        if "wait_for_job" not in write_options:
            write_options["wait_for_job"] = wait
        if "wait_for_online_ingestion" not in write_options:
            write_options["wait_for_online_ingestion"] = wait

        # fg_job is used only if the python engine is used
        fg_job, ge_report = self._feature_group_engine.save(
            self, feature_dataframe, write_options, validation_options or {}
        )

        # Compute stats in client if there is no backfill job:
        # - spark engine: always compute in client
        # - python engine: only compute if FG is offline only (no backfill job)
        if self.statistics_config.enabled and engine.get_type().startswith("spark"):
            self._statistics_engine.compute_and_save_statistics(self, feature_dataframe)
        elif engine.get_type() == "python" and not self.stream:
            commit_id = list(self.commit_details(limit=1))[0]
            self._statistics_engine.compute_and_save_statistics(
                metadata_instance=self,
                feature_dataframe=feature_dataframe,
                feature_group_commit_id=commit_id,
            )

        if user_version is None:
            warnings.warn(
                f"No version provided for creating feature group `{self._name}`, incremented version to `{self._version}`.",
                util.VersionWarning,
                stacklevel=1,
            )
        return (
            fg_job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    @public
    def insert(
        self,
        features: (
            pd.DataFrame
            | pl.DataFrame
            | TypeVar("pyspark.sql.DataFrame")
            | TypeVar("pyspark.RDD")
            | np.ndarray
            | list[list]
        ),
        overwrite: bool = False,
        operation: Literal["insert", "upsert"] = "upsert",
        storage: str | None = None,
        write_options: dict[str, Any] | None = None,
        validation_options: dict[str, Any] | None = None,
        wait: bool = False,
        transformation_context: dict[str, Any] = None,
        transform: bool = True,
    ) -> tuple[Job | None, ValidationReport | None]:
        """Persist the metadata and materialize the feature group to the feature store or insert data from a dataframe into the existing feature group.

        Incrementally insert data to a feature group or overwrite all data contained in the feature group.
        By default, the data is inserted into the offline storage as well as the online storage if the feature group is `online_enabled=True`.

        The `features` dataframe can be a Spark DataFrame or RDD, a Pandas DataFrame, a Polars DataFrame or a two-dimensional Numpy array or a two-dimensional Python nested list.
        If statistics are enabled, statistics are recomputed for the entire feature group.
        If feature group's time travel format is `HUDI` then `operation` argument can be either `insert` or `upsert`.

        If feature group doesn't exist the insert method will create the necessary metadata the first time it is invoked and writes the specified `features` dataframe as feature group to the online/offline feature store.

        Warning: Changed in 3.3.0
            `insert` and `save` methods are now async by default in non-spark clients.
            To achieve the old behaviour, set `wait` argument to `True`.

        Example: Upsert new feature data with time travel format `HUDI`
            ```python
            # connect to the Feature Store
            fs = ...

            fg = fs.get_or_create_feature_group(
                name='bitcoin_price',
                description='Bitcoin price aggregated for days',
                version=1,
                primary_key=['unix'],
                online_enabled=True,
                event_time='unix'
            )

            fg.insert(df_bitcoin_processed)
            ```

        Example: Async insert
            ```python
            # connect to the Feature Store
            fs = ...

            fg1 = fs.get_or_create_feature_group(
                name='feature_group_name1',
                description='Description of the first FG',
                version=1,
                primary_key=['unix'],
                online_enabled=True,
                event_time='unix'
            )
            # async insertion in order not to wait till finish of the job
            fg.insert(df_for_fg1, write_options={"wait_for_job" : False})

            fg2 = fs.get_or_create_feature_group(
                name='feature_group_name2',
                description='Description of the second FG',
                version=1,
                primary_key=['unix'],
                online_enabled=True,
                event_time='unix'
            )
            fg.insert(df_for_fg2)
            ```

        Parameters:
            features: Features to be saved.
            overwrite:
                Drop all data in the feature group before inserting new data.
                This does not affect metadata.
            operation: Apache Hudi operation type `"insert"` or `"upsert"`.
            storage:
                Overwrite default behaviour, write to offline storage only with `"offline"` or online only with `"online"`.
                If the streaming APIs are enabled, specifying the storage option is not supported.
            write_options:
                Additional write options as key-value pairs.

                When using the `python` engine, write_options can contain the following entries:

                - key `spark` and value an object of type [hsfs.core.job_configuration.JobConfiguration][hsfs.core.job_configuration.JobConfiguration] to configure the Hopsworks Job used to write data into the feature group.
                - key `wait_for_job` and value `True` or `False` to configure whether or not to the insert call should return only after the Hopsworks Job has finished. By default it waits.
                - key `wait_for_online_ingestion` and value `True` or `False` to configure whether or not to the save call should return only after the Hopsworks online ingestion has finished. By default it does not wait.
                - key `online_ingestion_options` and value a dict to configure waiting on online ingestion.
                  Applied when `wait_for_online_ingestion` write option is `True` or the `wait` parameter is `True`.
                  Supported keys are `timeout` (seconds to wait, default `60`, set to `0` for indefinite) and `period` (polling interval in seconds, default `1`).
                - key `start_offline_backfill` and value `True` or `False` to configure whether or not to start the materialization job to write data to the offline storage.
                  `start_offline_backfill` is deprecated.
                  Use `start_offline_materialization` instead.
                - key `start_offline_materialization` and value `True` or `False` to configure whether or not to start the materialization job to write data to the offline storage.
                  By default the materialization job gets started immediately.
                - key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln) used to configure the Kafka client.
                  To optimize for throughput in high latency connection consider changing [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                - key `internal_kafka` and value `True` or `False` in case you established connectivity from you Python environment to the internal advertised listeners of the Hopsworks Kafka Cluster.
                  Defaults to `False` and will use external listeners when connecting from outside of Hopsworks.
                - key `delta.enableChangeDataFeed` set to a *string* value of true or false to enable or disable cdf operations on the feature group delta table.
                  Set to true by default on Feature Group creation.

            validation_options:
                Additional validation options as key-value pairs.

                - key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                - key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                - key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
                - key `fetch_expectation_suite` a boolean value, by default `True`, to control whether the expectation suite of the feature group should be fetched before every insert.
                - key `schema_validation` boolean value, set to `True` to validate the schema.

            wait:
                Wait for job and online ingestion to finish before returning.
                Shortcut for write_options `{"wait_for_job": False, "wait_for_online_ingestion": False}`.
            transformation_context:
                A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                The `context` variable must be explicitly defined as parameters in the transformation function for these to be accessible during execution.
            transform:
                When set to `False`, the dataframe is inserted without applying any on-demand transformations
                In this case, all required on-demand features must already exist in the provided dataframe.

        Returns:
            Job: The job information if python engine is used.
            ValidationReport: The validation report if validation is enabled.

        Raises:
            hopsworks.client.exceptions.RestAPIError: e.g., fail to create feature group, dataframe schema does not match existing feature group schema, etc.
            hsfs.client.exceptions.DataValidationException:
                If data validation fails and the expectation suite `validation_ingestion_policy` is set to `STRICT`.
                Data is NOT ingested.
        """
        if storage and self.stream:
            warnings.warn(
                "Specifying the storage option is not supported if the streaming APIs are enabled",
                stacklevel=1,
            )

        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        if validation_options is None:
            validation_options = {}
        if write_options is None:
            write_options = {}
        if "wait_for_job" not in write_options:
            write_options["wait_for_job"] = wait
        if "wait_for_online_ingestion" not in write_options:
            write_options["wait_for_online_ingestion"] = wait
        if not self._id and self._offline_backfill_every_hr is not None:
            write_options["offline_backfill_every_hr"] = self._offline_backfill_every_hr
        if all(
            [
                not self._id,
                self.time_travel_format == "DELTA",
                write_options.get("delta.enableChangeDataFeed") != "false",
            ]
        ):
            # New delta FG allow for change data capture query
            write_options["delta.enableChangeDataFeed"] = "true"

        job, ge_report = self._feature_group_engine.insert(
            self,
            feature_dataframe=feature_dataframe,
            overwrite=overwrite,
            operation=operation,
            storage=storage.lower() if storage is not None else None,
            write_options=write_options,
            validation_options={"save_report": True, **validation_options},
            transformation_context=transformation_context,
            transform=transform,
        )

        # Compute stats in client if there is no backfill job:
        # - spark engine: always compute in client
        # - python engine: only compute if FG is offline only (no backfill job)
        if engine.get_type().startswith("spark") and not self.stream:
            self.compute_statistics()
        elif engine.get_type() == "python" and not self.stream:
            commit_id = list(self.commit_details(limit=1))[0]
            self._statistics_engine.compute_and_save_statistics(
                metadata_instance=self,
                feature_dataframe=feature_dataframe,
                feature_group_commit_id=commit_id,
            )

        return (
            job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    @public
    def multi_part_insert(
        self,
        features: (
            pd.DataFrame
            | pl.DataFrame
            | TypeVar("pyspark.sql.DataFrame")
            | TypeVar("pyspark.RDD")
            | np.ndarray
            | list[list]
            | None
        ) = None,
        overwrite: bool = False,
        operation: Literal["insert", "upsert"] = "upsert",
        storage: str | None = None,
        write_options: dict[str, Any] | None = None,
        validation_options: dict[str, Any] | None = None,
        transformation_context: dict[str, Any] = None,
        transform: bool = True,
    ) -> (
        tuple[Job | None, ValidationReport | None]
        | feature_group_writer.FeatureGroupWriter
    ):
        """Get FeatureGroupWriter for optimized multi part inserts or call this method to start manual multi part optimized inserts.

        In use cases where very small batches (1 to 1000) rows per Dataframe need to be written to the feature store repeatedly, it might be inefficient to use the standard `feature_group.insert()` method as it performs some background actions to update the metadata of the feature group object first.

        For these cases, the feature group provides the `multi_part_insert` API, which is optimized for writing many small Dataframes after another.

        There are two ways to use this API:

        Example: Python Context Manager
            Using the Python `with` syntax you can acquire a FeatureGroupWriter object that implements the same `multi_part_insert` API.

            ```python
            feature_group = fs.get_or_create_feature_group("fg_name", version=1)

            with feature_group.multi_part_insert() as writer:
                # run inserts in a loop:
                while loop:
                    small_batch_df = ...
                    writer.insert(small_batch_df)
            ```

            The writer batches the small Dataframes and transmits them to Hopsworks efficiently.
            When exiting the context, the feature group writer is sure to exit only once all the rows have been transmitted.

        Example: Multi part insert with manual context management
            Instead of letting Python handle the entering and exiting of the multi part insert context, you can start and finalize the context manually.

            ```python
            feature_group = fs.get_or_create_feature_group("fg_name", version=1)

            while loop:
                small_batch_df = ...
                feature_group.multi_part_insert(small_batch_df)

            # IMPORTANT: finalize the multi part insert to make sure all rows
            # have been transmitted
            feature_group.finalize_multi_part_insert()
            ```

            Note that the first call to `multi_part_insert` initiates the context and be sure to finalize it.
            The `finalize_multi_part_insert` is a blocking call that returns once all rows have been transmitted.

            Once you are done with the multi part insert, it is good practice to start the materialization job in order to write the data to the offline storage:

            ```python
            feature_group.materialization_job.run(await_termination=True)
            ```

        Parameters:
            features: Features to be saved.
            overwrite:
                Drop all data in the feature group before inserting new data.
                This does not affect metadata.
            operation: Apache Hudi operation type `"insert"` or `"upsert"`.
            storage: Overwrite default behaviour, write to offline storage only with `"offline"` or online only with `"online"`.
            write_options:
                Additional write options as key-value pairs.

                When using the `python` engine, write_options can contain the following entries:

                - key `spark` and value an object of type [hsfs.core.job_configuration.JobConfiguration][hsfs.core.job_configuration.JobConfiguration] to configure the Hopsworks Job used to write data into the feature group.
                - key `wait_for_job` and value `True` or `False` to configure whether or not to the insert call should return only after the Hopsworks Job has finished.
                  By default it waits.
                - key `start_offline_backfill` and value `True` or `False` to configure whether or not to start the materialization job to write data to the offline storage.
                  `start_offline_backfill` is deprecated.
                  Use `start_offline_materialization` instead.
                - key `start_offline_materialization` and value `True` or `False` to configure whether or not to start the materialization job to write data to the offline storage.
                  By default the materialization job does not get started automatically for multi part inserts.
                - key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln) used to configure the Kafka client.
                  To optimize for throughput in high latency connection consider changing [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                - key `internal_kafka` and value `True` or `False` in case you established connectivity from you Python environment to the internal advertised listeners of the Hopsworks Kafka Cluster.
                  Defaults to `False` and will use external listeners when connecting from outside of Hopsworks.

            validation_options:
                Additional validation options as key-value pairs.

                - key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                - key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                - key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
                - key `fetch_expectation_suite` a boolean value, by default `False` for multi part inserts, to control whether the expectation suite of the feature group should be fetched before every insert.

            transformation_context:
                A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                The `context` variable must be explicitly defined as parameters in the transformation function for these to be accessible during execution.
            transform:
                When set to `False`, the dataframe is inserted without applying any on-demand transformations.
                In this case, all required on-demand features must already exist in the provided dataframe.

        Returns:
            One of:

            - A tuple with job information if python engine is used and the validation report if validation is enabled, or
            - `FeatureGroupWriter` when used as a context manager with Python `with` statement.
        """
        self._multi_part_insert = True
        multi_part_writer = feature_group_writer.FeatureGroupWriter(self)
        if features is None:
            return multi_part_writer
        # go through writer to avoid setting multi insert defaults again
        return multi_part_writer.insert(
            features,
            overwrite,
            operation,
            storage,
            write_options or {},
            validation_options or {},
            transformation_context,
            transform=transform,
        )

    @public
    def finalize_multi_part_insert(self) -> None:
        """Finalizes and exits the multi part insert context opened by `multi_part_insert` in a blocking fashion once all rows have been transmitted.

        Example: Multi part insert with manual context management
            Instead of letting Python handle the entering and exiting of the multi part insert context, you can start and finalize the context manually.

            ```python
            feature_group = fs.get_or_create_feature_group("fg_name", version=1)

            while loop:
                small_batch_df = ...
                feature_group.multi_part_insert(small_batch_df)

            # IMPORTANT: finalize the multi part insert to make sure all rows
            # have been transmitted
            feature_group.finalize_multi_part_insert()
            ```

            Note that the first call to `multi_part_insert` initiates the context and be sure to finalize it.
            The `finalize_multi_part_insert` is a blocking call that returns once all rows have been transmitted.
        """
        if self._kafka_producer is not None:
            self._kafka_producer.flush()
            self._kafka_producer = None
        self._feature_writers = None
        self._writer = None
        self._kafka_headers = None
        self._multi_part_insert = False

    @public
    def insert_stream(
        self,
        features: TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
        query_name: str | None = None,
        output_mode: Literal["append", "complete", "update"] = "append",
        await_termination: bool = False,
        timeout: int | None = None,
        checkpoint_dir: str | None = None,
        write_options: dict[str, Any] | None = None,
        transformation_context: dict[str, Any] = None,
        transform: bool = True,
    ) -> TypeVar("StreamingQuery"):
        """Ingest a Spark Structured Streaming Dataframe to the online feature store.

        This method creates a long running Spark Streaming Query, you can control the termination of the query through the arguments.

        It is possible to stop the returned query with the `.stop()` and check its status with `.isActive`.

        To get a list of all active queries, use:

        ```python
        sqm = spark.streams

        # get the list of active streaming queries
        [q.name for q in sqm.active]
        ```

        Warning: Engine Support
            **Spark only**

            Stream ingestion using Pandas/Python as engine is currently not supported.
            Python/Pandas has no notion of streaming.

        Warning: Data Validation Support
            `insert_stream` does not perform any data validation using Great Expectations even when a expectation suite is attached.

        Parameters:
            features: Features in Streaming Dataframe to be saved.
            query_name: It is possible to optionally specify a name for the query to make it easier to recognise in the Spark UI.
            output_mode:
                Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.

                - `"append"`: Only the new rows in the streaming DataFrame/Dataset will be written to the sink.
                - `"complete"`: All the rows in the streaming DataFrame/Dataset will be written to the sink every time there is some update.
                - `"update"`: Only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
                  If the query doesn't contain aggregations, it will be equivalent to append mode.

            await_termination:
                Waits for the termination of this query, either by query.stop() or by an exception.
                If the query has terminated with an exception, then the exception will be thrown.
                If timeout is set, it returns whether the query has terminated or not within the timeout seconds.
            timeout: Only relevant in combination with `await_termination=True`.
            checkpoint_dir:
                Checkpoint directory location.
                This will be used to as a reference to from where to resume the streaming job.
                If `None` then hsfs will construct as "insert_stream_" + online_topic_name.
            write_options: Additional write options for Spark as key-value pairs.
            transformation_context:
                A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                The `context` variable must be explicitly defined as parameters in the transformation function for these to be accessible during execution.
            transform:
                When set to `False`, the dataframe is inserted without applying any on-demand transformations.
                In this case, all required on-demand features must already exist in the provided dataframe.

        Returns:
            Spark Structured Streaming Query object.
        """
        if (
            not engine.get_instance().is_spark_dataframe(features)
            or not features.isStreaming
        ):
            raise TypeError(
                "Features have to be a streaming type spark dataframe. Use `insert()` method instead."
            )
        # lower casing feature names
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)
        warnings.warn(
            (
                f"Stream ingestion for feature group `{self._name}`, with version"
                f" `{self._version}` will not compute statistics."
            ),
            util.StatisticsWarning,
            stacklevel=1,
        )

        return self._feature_group_engine.insert_stream(
            self,
            feature_dataframe,
            query_name,
            output_mode,
            await_termination,
            timeout,
            checkpoint_dir,
            write_options or {},
            transformation_context=transformation_context,
            transform=transform,
        )

    @public
    def commit_details(
        self,
        wallclock_time: str | int | datetime | date | None = None,
        limit: int | None = None,
    ) -> dict[str, dict[str, str]]:
        """Retrieves commit timeline for this feature group.

        This method can only be used on time travel enabled feature groups

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            commit_details = fg.commit_details()
            ```

        Parameters:
            wallclock_time:
                Commit details as of specific point in time.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            limit: Number of commits to retrieve.

        Returns:
            Dictionary object of commit metadata timeline, where Key is commit id and value is `Dict[str, str]` with key value pairs of date committed on, number of rows updated, inserted and deleted.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If the feature group does not have `HUDI` time travel format.
        """
        return self._feature_group_engine.commit_details(self, wallclock_time, limit)

    @public
    def commit_delete_record(
        self,
        delete_df: TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
        write_options: dict[Any, Any] | None = None,
    ) -> None:
        """Drops records present in the provided DataFrame and commits it as update to this Feature group.

        This method can only be used on feature groups stored as HUDI or DELTA.

        Parameters:
            delete_df: dataFrame containing records to be deleted.
            write_options: User provided write options.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if self.time_travel_format == "HUDI" and not engine.get_type().startswith(
            "spark"
        ):
            raise NotImplementedError(
                "commit_delete_record is only supported for HUDI feature groups when using the Spark engine."
            )
        self._feature_group_engine.commit_delete(self, delete_df, write_options or {})

    @public
    def delta_vacuum(
        self,
        retention_hours: int = None,
    ) -> None:
        """Vacuum files that are no longer referenced by a Delta table and are older than the retention threshold.

        This method can only be used on feature groups stored as DELTA.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            commit_details = fg.delta_vacuum(retention_hours = 168)
            ```

        Parameters:
            retention_hours:
                User provided retention period.
                The default retention threshold for the files is 7 days.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        self._feature_group_engine.delta_vacuum(self, retention_hours)

    @public
    def as_of(
        self,
        wallclock_time: str | int | datetime | date | None = None,
        exclude_until: str | int | datetime | date | None = None,
    ) -> query.Query:
        """Get Query object to retrieve all features of the group at a point in the past.

        Warning: Pyspark/Spark Only
            Apache HUDI exclusively supports Time Travel and Incremental Query via Spark Context

        This method selects all features in the feature group and returns a Query object at the specified point in time.
        Optionally, commits before a specified point in time can be excluded from the query.
        The Query can then either be read into a Dataframe or used further to perform joins or construct a training dataset.

        Example: Reading features at a specific point in time
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # get data at a specific point in time and show it
            fg.as_of("2020-10-20 07:34:11").read().show()
            ```

        Example: Reading commits incrementally between specified points in time
            ```python
            fg.as_of("2020-10-20 07:34:11", exclude_until="2020-10-19 07:34:11").read().show()
            ```

        The first parameter is inclusive while the latter is exclusive.
        That means, in order to query a single commit, you need to query that commit time and exclude everything just before the commit.

        Example: Reading only the changes from a single commit
            ```python
            fg.as_of("2020-10-20 07:31:38", exclude_until="2020-10-20 07:31:37").read().show()
            ```

        When no wallclock_time is given, the latest state of features is returned. Optionally, commits before a specified point in time can still be excluded.

        Example: Reading the latest state of features, excluding commits before a specified point in time
            ```python
            fg.as_of(None, exclude_until="2020-10-20 07:31:38").read().show()
            ```

        Note that the interval will be applied to all joins in the query.
        If you want to query different intervals for different feature groups in the query, you have to apply them in a nested fashion:

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg1 = fs.get_or_create_feature_group(...)
            fg2 = fs.get_or_create_feature_group(...)

            fg1.select_all().as_of("2020-10-20", exclude_until="2020-10-19")
                .join(fg2.select_all().as_of("2020-10-20", exclude_until="2020-10-19"))
            ```

        If instead you apply another `as_of` selection after the join, all joined feature groups will be queried with this interval:

        Example:
            ```python
            fg1.select_all().as_of("2020-10-20", exclude_until="2020-10-19")  # as_of is not applied
                .join(fg2.select_all().as_of("2020-10-20", exclude_until="2020-10-15"))  # as_of is not applied
                .as_of("2020-10-20", exclude_until="2020-10-19")
            ```

        Warning:
            This function only works for feature groups with time_travel_format='HUDI'.

        Warning:
            Excluding commits via exclude_until is only possible within the range of the Hudi active timeline.
            By default, Hudi keeps the last 20 to 30 commits in the active timeline.
            If you need to keep a longer active timeline, you can overwrite the options: `hoodie.keep.min.commits` and `hoodie.keep.max.commits` when calling the `insert()` method.

        Parameters:
            wallclock_time:
                Read data as of this point in time.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.
            exclude_until:
                Exclude commits until this point in time.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, or `%Y-%m-%d %H:%M:%S`.

        Returns:
            The query object with the applied time travel condition.
        """
        return self.select_all().as_of(
            wallclock_time=wallclock_time, exclude_until=exclude_until
        )

    @public
    def get_statistics_by_commit_window(
        self,
        from_commit_time: str | int | datetime | date | None = None,
        to_commit_time: str | int | datetime | date | None = None,
        feature_names: list[str] | None = None,
    ) -> Statistics | list[Statistics] | None:
        """Returns the statistics computed on a specific commit window for this feature group.

        If time travel is not enabled, it raises an exception.

        If `from_commit_time` is `None`, the commit window starts from the first commit.
        If `to_commit_time` is `None`, the commit window ends at the last commit.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...
            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)
            fg_statistics = fg.get_statistics_by_commit_window(from_commit_time=None, to_commit_time=None)
            ```

        Parameters:
            to_commit_time:
                Date and time of the last commit of the window. Defaults to `None`.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            from_commit_time:
                Date and time of the first commit of the window. Defaults to `None`.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.
            feature_names: List of feature names of which statistics are retrieved.

        Returns:
            Statistics object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if not self._is_time_travel_enabled():
            raise ValueError("Time travel is not enabled for this feature group")
        return self._statistics_engine.get_by_time_window(
            self,
            start_commit_time=from_commit_time,
            end_commit_time=to_commit_time,
            feature_names=feature_names,
        )

    @public
    def compute_statistics(
        self, wallclock_time: str | int | datetime | date | None = None
    ) -> None:
        """Recompute the statistics for the feature group and save them to the feature store.

        Statistics are only computed for data in the offline storage of the feature group.

        Parameters:
            wallclock_time:
                If specified will recompute statistics on feature group as of specific point in time.
                If not specified then will compute statistics as of most recent time of this feature group.
                Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        if self.statistics_config.enabled and (
            self._is_time_travel_enabled() or wallclock_time is not None
        ):
            wallclock_time = wallclock_time or datetime.now()
            # Retrieve fg commit id related to this wall clock time and recompute statistics. It will throw
            # exception if its not time travel enabled feature group.
            fg_commit_id = list(
                self._feature_group_engine.commit_details(
                    self, wallclock_time, 1
                ).keys()
            )[0]
            registered_stats = self.get_statistics_by_commit_window(
                to_commit_time=fg_commit_id
            )
            if registered_stats is not None and self._are_statistics_missing(
                registered_stats
            ):
                registered_stats = None
            # Don't read the dataframe here, to avoid triggering a read operation
            # for the Python engine. The Python engine is going to setup a Spark Job
            # to update the statistics.
            return (
                registered_stats
                or self._statistics_engine.compute_and_save_statistics(
                    self,
                    feature_group_commit_id=fg_commit_id,
                )
            )
        return super().compute_statistics()

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any] | list[dict[str, Any]]
    ) -> FeatureGroup | list[FeatureGroup]:
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            if "type" in json_decamelized:
                json_decamelized["stream"] = (
                    json_decamelized["type"] == "streamFeatureGroupDTO"
                )
            _ = json_decamelized.pop("type", None)
            json_decamelized.pop("validation_type", None)
            if "embedding_index" in json_decamelized:
                json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                    json_decamelized["embedding_index"]
                )
            if "tags" in json_decamelized and json_decamelized["tags"]:
                json_decamelized["tags"] = tag.Tag.from_response_json(
                    json_decamelized["tags"]
                )
            if "transformation_functions" in json_decamelized:
                transformation_functions = json_decamelized["transformation_functions"]
                json_decamelized["transformation_functions"] = [
                    TransformationFunction.from_response_json(
                        {
                            **transformation_function,
                            "transformation_type": TransformationType.ON_DEMAND,
                        }
                    )
                    for transformation_function in transformation_functions
                ]
            if "sink_job" in json_decamelized:
                json_decamelized["sink_job"] = job.Job.from_response_json(
                    json_decamelized["sink_job"]
                )
            return cls(**json_decamelized)
        for fg in json_decamelized:
            if "type" in fg:
                fg["stream"] = fg["type"] == "streamFeatureGroupDTO"
            _ = fg.pop("type", None)
            fg.pop("validation_type", None)
            if "embedding_index" in fg:
                fg["embedding_index"] = EmbeddingIndex.from_response_json(
                    fg["embedding_index"]
                )
            if "transformation_functions" in fg:
                transformation_functions = fg["transformation_functions"]
                fg["transformation_functions"] = [
                    TransformationFunction.from_response_json(
                        {
                            **transformation_function,
                            "transformation_type": TransformationType.ON_DEMAND,
                        }
                    )
                    for transformation_function in transformation_functions
                ]
            if "sink_job" in fg:
                fg["sink_job"] = job.Job.from_response_json(fg["sink_job"])
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(self, json_dict: dict[str, Any]) -> FeatureGroup:
        json_decamelized = humps.decamelize(json_dict)
        json_decamelized["stream"] = json_decamelized["type"] == "streamFeatureGroupDTO"
        _ = json_decamelized.pop("type")
        if "embedding_index" in json_decamelized:
            json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                json_decamelized["embedding_index"]
            )
        if "transformation_functions" in json_decamelized:
            transformation_functions = json_decamelized["transformation_functions"]
            json_decamelized["transformation_functions"] = [
                TransformationFunction.from_response_json(
                    {
                        **transformation_function,
                        "transformation_type": TransformationType.ON_DEMAND,
                    }
                )
                for transformation_function in transformation_functions
            ]
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        """Get specific Feature Group metadata in json format.

        Example:
            ```python
            fg.json()
            ```
        """
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict[str, Any]:
        """Get structured info about specific Feature Group in python dictionary format.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            fg.to_dict()
            ```
        """
        fg_meta_dict = {
            "id": self._id,
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "onlineEnabled": self._online_enabled,
            "timeTravelFormat": self._time_travel_format,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "type": (
                "cachedFeaturegroupDTO" if not self._stream else "streamFeatureGroupDTO"
            ),
            "statisticsConfig": self._statistics_config,
            "eventTime": self.event_time,
            "expectationSuite": self._expectation_suite,
            "parents": self._parents,
            "topicName": self.topic_name,
            "notificationTopicName": self.notification_topic_name,
            "deprecated": self.deprecated,
            "transformationFunctions": [
                tf.to_dict() for tf in self._transformation_functions
            ],
            "ttl": self.ttl,
            "ttlEnabled": self._ttl_enabled,
            "sinkEnabled": self._sink_enabled,
        }
        if self.data_source:
            fg_meta_dict["dataSource"] = self.data_source.to_dict()
        if self._online_config:
            fg_meta_dict["onlineConfig"] = self._online_config.to_dict()
        if self.embedding_index:
            fg_meta_dict["embeddingIndex"] = self.embedding_index.to_dict()
        if self._stream:
            fg_meta_dict["deltaStreamerJobConf"] = self._deltastreamer_jobconf
        tags_dict = tag.Tag.tags_to_dict(self._tags)
        if tags_dict:
            fg_meta_dict["tags"] = tags_dict
        return fg_meta_dict

    def _get_table_name(self) -> str:
        return self.feature_store_name + "." + self.get_fg_name()

    def _is_time_travel_enabled(self) -> bool:
        """Whether time-travel is enabled or not."""
        return (
            self._time_travel_format is not None
            and self._time_travel_format.upper() != "NONE"
        )

    def execute_odts(
        self,
        data: pd.DataFrame | pl.DataFrame | dict[str, Any],
        online: bool | None = None,
        transformation_context: dict[str, Any] | list[dict[str, Any]] = None,
        request_parameters: dict[str, Any] | list[dict[str, Any]] = None,
    ) -> list[dict[str, Any]] | pd.DataFrame:
        """Apply on-demand transformations attached to the feature group on the provided data.

        This method allows you to test on-demand transformation functions locally.
        It executes all on-demand transformations(ODTs) attached to the feature group on the input data.

        !!! example "Testing on-demand transformations"
            ```python
            # Define and attach an on-demand transformation
            @udf(return_type=float)
            def compute_ratio(amount, quantity):
                return amount / quantity

            fg = fs.get_or_create_feature_group(name="transactions",
                                                version=1,
                                                primary_key=["pk"],
                                                transformation_functions=[compute_ratio("amount", "quantity")])

            # Test with a DataFrame (offline mode)
            test_df = pd.DataFrame({
                "amount": [100.0, 200.0, 300.0],
                "quantity": [2, 4, 5]
            })
            result_df = fg.execute_odts(test_df)

            # Test with a dictionary (online inference simulation)
            test_dict = {"amount": 100.0, "quantity": 2}
            result_dict = fg.execute_odts(test_dict, online=True)
            ```

        Parameters:
            data: Input data to apply transformations to. This can a dataframe or a dictionary.
            online: Whether to apply transformations in online mode (single values) or offline mode (batch/vectorized). Defaults to offline mode
            transformation_context: A dictionary mapping variable names to objects that provide contextual information to the transformation function at runtime.
                The `context` variables must be defined as parameters in the transformation function for these to be accessible during execution. For batch processing with different contexts per row, provide a list of dictionaries.
            request_parameters: Request parameters passed to the transformation functions. For batch processing with different parameters per row, provide a list of dictionaries.
                These parameters take **highest priority** when resolving feature values - if a key exists in both `request_parameters` and the input data, the value from `request_parameters` is used.

        Returns:
            The transformed data in the same format as the input:
                - `pd.DataFrame` if input was a DataFrame
                - `dict[str, Any]` if input was a dictionary
        """
        if self.transformation_functions:
            data = self._feature_group_engine.apply_on_demand_transformations(
                transformation_functions=self.transformation_functions,
                data=data,
                online=online,
                transformation_context=transformation_context,
                request_parameters=request_parameters,
            )
        else:
            _logger.info(
                "No on-demand transformation functions attached to the feature group, no transformations applied."
            )
        return data

    @public
    @property
    def id(self) -> int | None:
        """Feature group id."""
        return self._id

    @public
    @property
    def description(self) -> str | None:
        """Description of the feature group contents."""
        return self._description

    @public
    @property
    def time_travel_format(self) -> str | None:
        """Setting of the feature group time travel format."""
        return self._time_travel_format

    @public
    @property
    def partition_key(self) -> list[str]:
        """List of features building the partition key."""
        return self._partition_key

    @public
    @property
    def hudi_precombine_key(self) -> str | None:
        """Feature name that is the hudi precombine key."""
        return self._hudi_precombine_key

    @public
    @property
    def feature_store_name(self) -> str | None:
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name

    @public
    @property
    def creator(self) -> user.User | None:
        """Username of the creator."""
        return self._creator

    @public
    @property
    def created(self) -> str | None:
        """Timestamp when the feature group was created."""
        return self._created

    @public
    @property
    def stream(self) -> bool:
        """Whether to enable real time stream writing capabilities."""
        return self._stream

    @public
    @property
    def parents(self) -> list[explicit_provenance.Links]:
        """Parent feature groups as origin of the data in the current feature group.

        This is part of explicit provenance.
        """
        return self._parents

    @public
    @property
    def materialization_job(self) -> Job | None:
        """Get the Job object reference for the materialization job for this Feature Group."""
        if self._materialization_job is not None:
            return self._materialization_job
        feature_group_name = util.feature_group_name(self)
        job_suffix_list = ["materialization", "backfill"]
        for job_suffix in job_suffix_list:
            job_name = f"{feature_group_name}_offline_fg_{job_suffix}"
            for _ in range(3):  # retry starting job
                try:
                    self._materialization_job = job_api.JobApi().get(job_name)
                    return self._materialization_job
                except RestAPIError as e:
                    if e.response.status_code == 404:
                        if e.response.json().get("errorCode", "") == 130009:
                            break  # no need to retry, since no such job exists
                        time.sleep(1)  # backoff and then retry
                        continue
                    raise e
        raise FeatureStoreException("No materialization job was found")

    @public
    @property
    def statistics(self) -> Statistics:
        """Get the latest computed statistics for the whole feature group."""
        if self._is_time_travel_enabled():
            # retrieve the latests statistics computed on the whole Feature Group, including all the commits.
            now = util.convert_event_time_to_timestamp(datetime.now())
            return self._statistics_engine.get_by_time_window(
                self,
                start_commit_time=None,
                end_commit_time=now,
            )
        return super().statistics

    @public
    @property
    def transformation_functions(
        self,
    ) -> list[TransformationFunction]:
        """Get transformation functions."""
        return self._transformation_functions

    @description.setter
    def description(self, new_description: str | None) -> None:
        self._description = new_description

    @time_travel_format.setter
    def time_travel_format(self, new_time_travel_format: str | None) -> None:
        self._time_travel_format = new_time_travel_format

    @partition_key.setter
    def partition_key(self, new_partition_key: list[str]) -> None:
        self._partition_key = [
            util.autofix_feature_name(pk, warn=True) for pk in new_partition_key
        ]

    @hudi_precombine_key.setter
    def hudi_precombine_key(self, hudi_precombine_key: str) -> None:
        self._hudi_precombine_key = util.autofix_feature_name(
            hudi_precombine_key, warn=True
        )

    @stream.setter
    def stream(self, stream: bool) -> None:
        self._stream = stream

    @parents.setter
    def parents(self, new_parents: explicit_provenance.Links) -> None:
        self._parents = new_parents

    @transformation_functions.setter
    def transformation_functions(
        self,
        transformation_functions: list[TransformationFunction],
    ) -> None:
        self._transformation_functions = transformation_functions

    @public
    @property
    def offline_backfill_every_hr(self) -> int | str | None:
        """On Feature Group creation, used to set scheduled run of the materialisation job."""
        if self.id:
            job = self.materialization_job
            if job.job_schedule:
                print(
                    "You can checkout the full job schedule for the materialization job using `.materialization_job.job_schedule`"
                )
                return job.job_schedule.cron_expression
            warnings.warn(
                "No schedule found for the materialization job. Use `job = fg.materialization_job` "
                "to get the full job object and edit the schedule",
                stacklevel=1,
            )
            return None
        return self._offline_backfill_every_hr

    @offline_backfill_every_hr.setter
    def offline_backfill_every_hr(
        self, new_offline_backfill_every_hr: int | str | None
    ) -> None:
        if self.id:
            raise FeatureStoreException(
                "This property is read-only for existing Feature Groups. "
                "Use `job = fg.materialization_job` to get the full job object and edit the schedule"
            )
        self._offline_backfill_every_hr = new_offline_backfill_every_hr

    @property
    def sink_enabled(self) -> bool:
        """Get whether sink is enabled for this feature group."""
        return self._sink_enabled

    @property
    def sink_job(self) -> job.Job | None:
        """Return the sink job created for this feature group, if any."""
        return self._sink_job

    @property
    def sink_job_conf(self) -> SinkJobConfiguration:
        """Sink job configuration object defining the settings for sink job of the feature group."""
        return self._sink_job_conf

    @sink_job_conf.setter
    def sink_job_conf(
        self, sink_job_conf: SinkJobConfiguration | dict[str, Any] | None
    ):
        if isinstance(sink_job_conf, SinkJobConfiguration):
            self._sink_job_conf = sink_job_conf
        elif isinstance(sink_job_conf, dict):
            self._sink_job_conf = SinkJobConfiguration(**sink_job_conf)
        elif sink_job_conf is None:
            self._sink_job_conf = SinkJobConfiguration()
        else:
            raise TypeError(
                f"The argument `sink_job_conf` has to be of type `SinkJobConfiguration` or `dict`, or be `None`, but is of type: `{type(sink_job_conf)}`"
            )


@public
@typechecked
class ExternalFeatureGroup(FeatureGroupBase):
    """A feature group that references data stored outside Hopsworks."""

    EXTERNAL_FEATURE_GROUP = "ON_DEMAND_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        name: str | None = None,
        version: int | None = None,
        description: str | None = None,
        primary_key: list[str] | None = None,
        foreign_key: list[str] | None = None,
        featurestore_id: int | None = None,
        featurestore_name: str | None = None,
        created: str | None = None,
        creator: dict[str, Any] | None = None,
        id: int | None = None,
        features: list[dict[str, Any]] | list[feature.Feature] | None = None,
        location: str | None = None,
        statistics_config: StatisticsConfig | dict[str, Any] | None = None,
        event_time: str | None = None,
        expectation_suite: (
            hsfs.expectation_suite.ExpectationSuite
            | great_expectations.core.ExpectationSuite
            | dict[str, Any]
            | None
        ) = None,
        online_enabled: bool = False,
        href: str | None = None,
        online_topic_name: str | None = None,
        topic_name: str | None = None,
        notification_topic_name: str | None = None,
        spine: bool = False,
        deprecated: bool = False,
        embedding_index: EmbeddingIndex | None = None,
        online_config: OnlineConfig | dict[str, Any] | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        ttl: float | timedelta | None = None,
        ttl_enabled: bool | None = None,
        online_disk: bool | None = None,
        missing_mandatory_tags: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            name,
            version,
            featurestore_id,
            location,
            event_time=event_time,
            online_enabled=online_enabled,
            id=id,
            embedding_index=embedding_index,
            expectation_suite=expectation_suite,
            online_topic_name=online_topic_name,
            topic_name=topic_name,
            notification_topic_name=notification_topic_name,
            deprecated=deprecated,
            online_config=online_config,
            data_source=data_source,
            ttl=ttl,
            ttl_enabled=ttl_enabled,
            online_disk=online_disk,
            missing_mandatory_tags=missing_mandatory_tags,
        )

        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = user.User.from_response_json(creator)
        self._data_format = data_format.upper() if data_format else None

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._feature_group_engine: external_feature_group_engine.ExternalFeatureGroupEngine = external_feature_group_engine.ExternalFeatureGroupEngine(
            featurestore_id
        )

        if self._id:
            # Got from Hopsworks, deserialize features and storage connector
            self.primary_key = (
                [feat.name for feat in self._features if feat.primary is True]
                if self._features
                else []
            )
            self.foreign_key = (
                [feat.name for feat in self._features if feat.foreign is True]
                if self._features
                else []
            )
            self.statistics_config = statistics_config

            self._options = (
                {option["name"]: option["value"] for option in options}
                if options
                else None
            )
        else:
            self.primary_key = primary_key
            self.foreign_key = foreign_key
            self.statistics_config = statistics_config
            self._options = options or {}

        self._vector_db_client: VectorDbClient | None = None
        self._href: str | None = href

    @public
    def save(self) -> None:
        """Persist the metadata for this external feature group.

        Without calling this method, your feature group will only exist
        in your Python Kernel, but not in Hopsworks.

        ```python
        query = "SELECT * FROM sales"

        fg = feature_store.create_external_feature_group(name="sales",
            version=1,
            description="Physical shop sales features",
            data_source=ds,
            primary_key=['ss_store_sk'],
            event_time='sale_date'
        )

        fg.save()
        ```
        """
        self._feature_group_engine.save(self)

        if self.statistics_config.enabled:
            self._statistics_engine.compute_and_save_statistics(self)

    @public
    def insert(
        self,
        features: (
            pd.DataFrame
            | TypeVar("pyspark.sql.DataFrame")
            | TypeVar("pyspark.RDD")
            | np.ndarray
            | list[list]
        ),
        write_options: dict[str, Any] | None = None,
        validation_options: dict[str, Any] | None = None,
        wait: bool = False,
    ) -> tuple[None, great_expectations.core.ExpectationSuiteValidationResult | None]:
        """Insert the dataframe feature values ONLY in the online feature store.

        External Feature Groups contains metadata about feature data in an external storage system.
        External storage system are usually offline, meaning feature values cannot be retrieved in real-time.
        In order to use the feature values for real-time use-cases, you can insert them in Hopsoworks Online Feature Store via this method.

        The Online Feature Store has a single-entry per primary key value, meaining that providing a new value with for a given primary key will overwrite the existing value.
        No record of the previous value is kept.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the External Feature Group instance
            fg = fs.get_feature_group(name="external_sales_records", version=1)

            # get the feature values, e.g reading from csv files in a S3 bucket
            feature_values = ...

            # insert the feature values in the online feature store
            fg.insert(feature_values)
            ```

        Note:
            Data Validation via Great Expectation is supported if you have attached an expectation suite to your External Feature Group.
            However, as opposed to regular Feature Groups, this can lead to discrepancies between the data in the external storage system and the online feature store.

        Parameters:
            features: Features to be saved.
            write_options:
                Additional write options as key-value pairs.

                When using the `python` engine, write_options can contain the following entries:

                - key `wait_for_job` and value `True` or `False` to configure whether or not to the insert call should return only after the Hopsworks Job has finished.
                  By default it waits.
                - key `wait_for_online_ingestion` and value `True` or `False` to configure whether or not to the save call should return only after the Hopsworks online ingestion has finished.
                  By default it does not wait.
                - key `online_ingestion_options` and value a dict to configure waiting on online ingestion.
                  Applied when `wait_for_online_ingestion` write option is `True` or the `wait` parameter is `True`.
                  Supported keys are `timeout` (seconds to wait, default `60`, set to `0` for indefinite) and `period` (polling interval in seconds, default `1`).
                - key `kafka_producer_config` and value an object of type [properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.htmln) used to configure the Kafka client.
                  To optimize for throughput in high latency connection consider changing [producer properties](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html#producer).
                - key `internal_kafka` and value `True` or `False` in case you established connectivity from you Python environment to the internal advertised listeners of the Hopsworks Kafka Cluster.
                  Defaults to `False` and will use external listeners when connecting from outside of Hopsworks.

            validation_options:
                Additional validation options as key-value pairs.

                - key `run_validation` boolean value, set to `False` to skip validation temporarily on ingestion.
                - key `save_report` boolean value, set to `False` to skip upload of the validation report to Hopsworks.
                - key `ge_validate_kwargs` a dictionary containing kwargs for the validate method of Great Expectations.
                - key `fetch_expectation_suite` a boolean value, by default `True`, to control whether the expectation suite of the feature group should be fetched before every insert.
            wait:
                Wait for job and online ingestion to finish before returning.
                Shortcut for write_options `{"wait_for_job": False, "wait_for_online_ingestion": False}`.

        Returns:
            The validation report if validation is enabled.

        Raises:
            hopsworks.client.exceptions.RestAPIError: e.g., fail to create feature group, dataframe schema does not match existing feature group schema, etc.
            hsfs.client.exceptions.DataValidationException:
                If data validation fails and the expectation suite `validation_ingestion_policy` is set to `STRICT`.
                Data is NOT ingested.
        """
        feature_dataframe = engine.get_instance().convert_to_default_dataframe(features)

        if validation_options is None:
            validation_options = {}
        if write_options is None:
            write_options = {}
        if "wait_for_job" not in write_options:
            write_options["wait_for_job"] = wait
        if "wait_for_online_ingestion" not in write_options:
            write_options["wait_for_online_ingestion"] = wait

        job, ge_report = self._feature_group_engine.insert(
            self,
            feature_dataframe=feature_dataframe,
            write_options=write_options,
            validation_options={"save_report": True, **validation_options},
        )

        if self.statistics_config.enabled:
            warnings.warn(
                (
                    f"Statistics are not computed for insertion to online enabled external feature group `{self._name}`, with version"
                    f" `{self._version}`. Call `compute_statistics` explicitly to compute statistics over the data in the external storage system."
                ),
                util.StorageWarning,
                stacklevel=1,
            )

        return (
            job,
            ge_report.to_ge_type() if ge_report is not None else None,
        )

    @public
    def read(
        self,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
        online: bool = False,
        read_options: dict[str, Any] | None = None,
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | pl.DataFrame
        | np.ndarray
    ):
        """Get the feature group as a DataFrame.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            df = fg.read()
            ```

        Warning: Engine Support
            **Spark only**

            Reading an External Feature Group directly into a Pandas Dataframe using Python/Pandas as Engine is not supported, however, you can use the Query API to create Feature Views/Training Data containing External Feature Groups.

        Parameters:
            dataframe_type:
                The type of the returned dataframe.
                By default, maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            online: If `True` read from online feature store.
            read_options: Additional options as key/value pairs to pass to the spark engine.

        Returns:
            One of:

            - `DataFrame`: The spark dataframe containing the feature data.
            - `pyspark.DataFrame`: A Spark DataFrame.
            - `pandas.DataFrame`: A Pandas DataFrame.
            - `numpy.ndarray`: A two-dimensional Numpy array.
            - `list`: A two-dimensional Python list.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.FeatureStoreException: If trying to read an external feature group directly in.
        """
        if (
            engine.get_type() == "python"
            and not online
            and not engine.get_instance().is_flyingduck_query_supported(
                self.select_all()
            )
        ):
            raise FeatureStoreException(
                "Reading an External Feature Group directly into a Pandas Dataframe using "
                "Python/Pandas as Engine from the external storage system "
                "is not supported, however, if the feature group is online enabled, you can read "
                "from online storage or you can use the "
                "Query API to create Feature Views/Training Data containing External "
                "Feature Groups."
            )
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            f"Getting feature group: {self._name} from the featurestore {self._feature_store_name}",
        )
        return self.select_all().read(
            dataframe_type=dataframe_type,
            online=online,
            read_options=read_options or {},
        )

    @public
    def show(self, n: int, online: bool = False) -> list[list[Any]]:
        """Show the first `n` rows of the feature group.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            # make a query and show top 5 rows
            fg.select(['date','weekly_sales','is_holiday']).show(5)
            ```

        Parameters:
            n: Number of rows to show.
            online: If `True` read from online feature store.
        """
        engine.get_instance().set_job_group(
            "Fetching Feature group",
            f"Getting feature group: {self._name} from the featurestore {self._feature_store_name}",
        )
        return self.select_all().show(n, online)

    @public
    def find_neighbors(
        self,
        embedding: list[int | float],
        col: str | None = None,
        k: int | None = 10,
        filter: Filter | Logic | None = None,
        options: dict | None = None,
    ) -> list[tuple[float, list[Any]]]:
        """Finds the nearest neighbors for a given embedding in the vector database.

        If `filter` is specified, or if embedding feature is stored in default project index, the number of results returned may be less than k.
        Try using a large value of k and extract the top k items from the results if needed.

        Parameters:
            embedding: The target embedding for which neighbors are to be found.
            col:
                The column name used to compute similarity score.
                Required only if there are multiple embeddings.
            k: The number of nearest neighbors to retrieve.
            filter: A filter expression to restrict the search space.
            options:
                The options used for the request to the vector database.
                The keys are attribute values of the `hsfs.core.opensearch.OpensearchRequestOption` class.

        Returns:
            A list of tuples representing the nearest neighbors.
            Each tuple contains: `(The similarity score, A list of feature values)`.

        Example:
            ```
            embedding_index = EmbeddingIndex()
            embedding_index.add_embedding(name="user_vector", dimension=3)
            fg = fs.create_feature_group(
                        name='air_quality',
                        embedding_index = embedding_index,
                        version=1,
                        primary_key=['id1'],
                        online_enabled=True,
                    )
            fg.insert(data)
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
            )

            # apply filter
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
                filter=(fg.id1 > 10) & (fg.id1 < 30)
            )
            ```
        """
        if self._vector_db_client is None and self._embedding_index:
            self._vector_db_client = VectorDbClient(self.select_all())
        results = self._vector_db_client.find_neighbors(
            embedding,
            feature=(self.__getattr__(col) if col else None),
            k=k,
            filter=filter,
            options=options,
        )
        return [
            (result[0], [result[1][f.name] for f in self.features])
            for result in results
        ]

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> ExternalFeatureGroup | list[ExternalFeatureGroup]:
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            _ = json_decamelized.pop("type", None)
            if "embedding_index" in json_decamelized:
                json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                    json_decamelized["embedding_index"]
                )
            return cls(**json_decamelized)
        for fg in json_decamelized:
            _ = fg.pop("type", None)
            if "embedding_index" in fg:
                fg["embedding_index"] = EmbeddingIndex.from_response_json(
                    fg["embedding_index"]
                )
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(
        self, json_dict: dict[str, Any]
    ) -> ExternalFeatureGroup:
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:
            _ = json_decamelized.pop("type")
        if "embedding_index" in json_decamelized:
            json_decamelized["embedding_index"] = EmbeddingIndex.from_response_json(
                json_decamelized["embedding_index"]
            )
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict[str, Any]:
        fg_meta_dict = {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "dataFormat": self._data_format,
            "options": (
                [{"name": k, "value": v} for k, v in self._options.items()]
                if self._options
                else None
            ),
            "type": "onDemandFeaturegroupDTO",
            "statisticsConfig": self._statistics_config,
            "eventTime": self._event_time,
            "expectationSuite": self._expectation_suite,
            "onlineEnabled": self._online_enabled,
            "spine": False,
            "topicName": self.topic_name,
            "notificationTopicName": self.notification_topic_name,
            "deprecated": self.deprecated,
            "ttl": self.ttl,
            "ttlEnabled": self._ttl_enabled,
        }
        if self.data_source:
            fg_meta_dict["dataSource"] = self.data_source.to_dict()
        if self._online_config:
            fg_meta_dict["onlineConfig"] = self._online_config.to_dict()
        if self.embedding_index:
            fg_meta_dict["embeddingIndex"] = self.embedding_index
        return fg_meta_dict

    @public
    @property
    def id(self) -> int | None:
        """ID of the feature group, set by backend."""
        return self._id

    @public
    @property
    def description(self) -> str | None:
        """Description of the feature group, as it appears in the UI."""
        return self._description

    @public
    @property
    def data_format(self) -> str | None:
        # TODO: Add docstring
        return self._data_format

    @public
    @property
    def options(self) -> dict[str, Any] | None:
        # TODO: Add docstring
        return self._options

    @public
    @property
    def creator(self) -> user.User | None:
        """User who created the feature group."""
        return self._creator

    @public
    @property
    def created(self) -> str | None:
        # TODO: Add docstring
        return self._created

    @description.setter
    def description(self, new_description: str | None) -> None:
        self._description = new_description

    @public
    @property
    def feature_store_name(self) -> str | None:
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name


@public
@typechecked
class SpineGroup(FeatureGroupBase):
    # TODO: Add docstring
    SPINE_GROUP = "ON_DEMAND_FEATURE_GROUP"
    ENTITY_TYPE = "featuregroups"

    def __init__(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] = None,
        name: str | None = None,
        version: int | None = None,
        description: str | None = None,
        primary_key: list[str] | None = None,
        featurestore_id: int | None = None,
        featurestore_name: str | None = None,
        created: str | None = None,
        creator: dict[str, Any] | None = None,
        id: int | None = None,
        features: list[feature.Feature | dict[str, Any]] | None = None,
        location: str | None = None,
        statistics_config: StatisticsConfig | None = None,
        event_time: str | None = None,
        expectation_suite: (
            hsfs.expectation_suite.ExpectationSuite
            | great_expectations.core.ExpectationSuite
            | None
        ) = None,
        # spine groups are online enabled by default such that feature_view.get_feature_vector can be used
        online_enabled: bool = True,
        href: str | None = None,
        online_topic_name: str | None = None,
        topic_name: str | None = None,
        spine: bool = True,
        dataframe: str | None = None,
        deprecated: bool = False,
        online_config: OnlineConfig | dict[str, Any] | None = None,
        data_source: ds.DataSource | dict[str, Any] | None = None,
        online_disk: bool | None = None,
        missing_mandatory_tags: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            name,
            version,
            featurestore_id,
            location,
            event_time=event_time,
            online_enabled=online_enabled,
            id=id,
            expectation_suite=expectation_suite,
            online_topic_name=online_topic_name,
            topic_name=topic_name,
            deprecated=deprecated,
            online_config=online_config,
            data_source=data_source,
            online_disk=online_disk,
            missing_mandatory_tags=missing_mandatory_tags,
        )

        self._feature_store_name = featurestore_name
        self._description = description
        self._created = created
        self._creator = user.User.from_response_json(creator)

        self._features = [
            feature.Feature.from_response_json(feat) if isinstance(feat, dict) else feat
            for feat in (features or [])
        ]

        self._feature_group_engine: spine_group_engine.SpineGroupEngine = (
            spine_group_engine.SpineGroupEngine(featurestore_id)
        )
        self._statistics_config = None

        if self._id:
            # Got from Hopsworks, deserialize features and storage connector
            self._features = (
                [
                    (
                        feature.Feature.from_response_json(feat)
                        if isinstance(feat, dict)
                        else feat
                    )
                    for feat in features
                ]
                if features
                else None
            )
            self.primary_key = (
                [feat.name for feat in self._features if feat.primary is True]
                if self._features
                else []
            )
        else:
            self.primary_key = primary_key
            self._features = features

        self._href = href

        # has to happen last -> features and id are needed for schema verification
        # use setter to convert to default dataframe type for engine
        self.dataframe = dataframe

    def _save(self) -> SpineGroup:
        """Persist the metadata for this spine group.

        Without calling this method, your feature group will only exist
        in your Python Kernel, but not in Hopsworks.

        ```python
        query = "SELECT * FROM sales"

        fg = feature_store.create_spine_group(name="sales",
            version=1,
            description="Physical shop sales features",
            primary_key=['ss_store_sk'],
            event_time='sale_date',
            dataframe=df,
        )

        fg._save()
        ```
        """
        self._feature_group_engine.save(self)
        return self

    @public
    @property
    def dataframe(
        self,
    ) -> pd.DataFrame | TypeVar("pyspark.sql.DataFrame") | None:
        """Spine dataframe with primary key, event time and label column to use for point in time join when fetching features."""
        return self._dataframe

    @dataframe.setter
    def dataframe(
        self,
        dataframe: (
            pd.DataFrame
            | pl.DataFrame
            | np.ndarray
            | TypeVar("pyspark.sql.DataFrame")
            | TypeVar("pyspark.RDD")
            | None
        ),
    ) -> None:
        """Update the spine dataframe contained in the spine group."""
        if dataframe is None:
            warnings.warn(
                "Spine group dataframe is not set, use `spine_fg.dataframe = df` to set it"
                " before attempting to use it as a basis to join features. The dataframe will not"
                " be saved to Hopsworks when registering the spine group with `save` method.",
                UserWarning,
                stacklevel=1,
            )
            self._dataframe = (
                None  # if metadata fetched from backend the dataframe is not set
            )
        else:
            self._dataframe = engine.get_instance().convert_to_default_dataframe(
                dataframe
            )

        # in fs query the features are not sent, so then don't do validation
        if (
            self._id is not None
            and self._dataframe is not None
            and self._features is not None
        ):
            dataframe_features = engine.get_instance().parse_schema_feature_group(
                self._dataframe
            )
            self._feature_group_engine._verify_schema_compatibility(
                self._features, dataframe_features
            )

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> SpineGroup | list[SpineGroup]:
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, dict):
            _ = json_decamelized.pop("type", None)
            return cls(**json_decamelized)
        for fg in json_decamelized:
            _ = fg.pop("type", None)
        return [cls(**fg) for fg in json_decamelized]

    def update_from_response_json(self, json_dict: dict[str, Any]) -> SpineGroup:
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:
            _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "features": self._features,
            "featurestoreId": self._feature_store_id,
            "type": "onDemandFeaturegroupDTO",
            "statisticsConfig": self._statistics_config,
            "eventTime": self._event_time,
            "spine": True,
            "topicName": self.topic_name,
            "deprecated": self.deprecated,
            "onlineEnabled": self._online_enabled,
        }

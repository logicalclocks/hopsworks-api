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
from hopsworks_common import util
from hopsworks_common.core.rest_endpoint import RestEndpointConfig
from hsfs import storage_connector as sc


if TYPE_CHECKING:
    from hsfs.core import data_source_data as dsd


class DataSource:
    """Metadata object used to provide data source information.

    The DataSource class encapsulates the details of a data source that can be used
    for reading or writing data. It supports various types of sources,
    such as SQL queries, database tables, file paths, and storage connectors.

    Attributes:
        _query (Optional[str]): SQL query string for the data source, if applicable.
        _database (Optional[str]): Name of the database containing the data source.
        _group (Optional[str]): Group or schema name for the data source.
        _table (Optional[str]): Table name for the data source.
        _path (Optional[str]): File system path for the data source.
        _storage_connector (Optional[StorageConnector]): Storage connector object holds configuration for accessing the data source.
        _metrics (List[str]): List of metric column names for the data source.
        _dimensions (List[str]): List of dimension column names for the data source.
        _rest_endpoint (Optional[RestEndpointConfig]): REST endpoint configuration for the data source.
    """

    def __init__(
        self,
        query: str | None = None,
        database: str | None = None,
        group: str | None = None,
        table: str | None = None,
        path: str | None = None,
        storage_connector: sc.StorageConnector | dict[str, Any] | None = None,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        rest_endpoint: RestEndpointConfig | dict | None = None,
        **kwargs,
    ):
        """Initialize a DataSource object.

        Args:
            query (Optional[str]): SQL query string for the data source, if applicable.
            database (Optional[str]): Name of the database containing the data source.
            group (Optional[str]): Group or schema name for the data source.
            table (Optional[str]): Table name for the data source.
            path (Optional[str]): File system path for the data source.
            storage_connector (Union[StorageConnector, Dict[str, Any]], optional): Storage connector object holds configuration for accessing the data source.
            metrics (Optional[List[str]]): List of metric column names for the data source.
            dimensions (Optional[List[str]]): List of dimension column names for the data source.
            rest_endpoint (Union[RestEndpointConfig, Dict, None]): REST endpoint configuration for the data source.
            **kwargs: Additional keyword arguments.
        """
        self._query = query
        self._database = database
        self._group = group
        self._table = table
        self._path = path
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

    @classmethod
    def from_response_json(
        cls,
        json_dict: dict[str, Any],
        storage_connector: sc.StorageConnector | None = None,
    ) -> DataSource:
        """Create a DataSource object (or list of objects) from a JSON response.

        Args:
            json_dict (Dict[str, Any]): The JSON dictionary from the API response.
            storage_connector (Optional[sc.StorageConnector]): The storage connector object.

        Returns:
            DataSource or List[DataSource] or None: The created object(s), or None if input is None.
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

    def to_dict(self):
        """Convert the DataSource object to a dictionary.

        Returns:
            dict: Dictionary representation of the object.
        """
        ds_meta_dict = {
            "query": self._query,
            "database": self._database,
            "group": self._group,
            "table": self._table,
            "path": self._path,
            "metrics": self._metrics,
            "dimensions": self._dimensions,
            "restEndpoint": (
                self._rest_endpoint.to_dict() if self._rest_endpoint else None
            ),
        }
        if self._storage_connector:
            ds_meta_dict["storageConnector"] = self._storage_connector.to_dict()
        return ds_meta_dict

    def json(self):
        """Serialize the DataSource object to a JSON string.

        Returns:
            str: JSON string representation of the object.
        """
        return json.dumps(self, cls=util.Encoder)

    @property
    def query(self) -> str | None:
        """Get or set the SQL query string for the data source.

        Returns:
            Optional[str]: The SQL query string.
        """
        return self._query

    @query.setter
    def query(self, query: str) -> None:
        self._query = query

    @property
    def database(self) -> str | None:
        """Get or set the database name for the data source.

        Returns:
            Optional[str]: The database name.
        """
        return self._database

    @database.setter
    def database(self, database: str) -> None:
        self._database = database

    @property
    def group(self) -> str | None:
        """Get or set the group/schema name for the data source.

        Returns:
            Optional[str]: The group or schema name.
        """
        return self._group

    @group.setter
    def group(self, group: str) -> None:
        self._group = group

    @property
    def table(self) -> str | None:
        """Get or set the table name for the data source.

        Returns:
            Optional[str]: The table name.
        """
        return self._table

    @table.setter
    def table(self, table: str) -> None:
        self._table = table

    @property
    def path(self) -> str | None:
        """Get or set the file system path for the data source.

        Returns:
            Optional[str]: The file system path.
        """
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    @property
    def storage_connector(self) -> sc.StorageConnector | None:
        """Get or set the storage connector for the data source.

        Returns:
            Optional[StorageConnector]: The storage connector object.
        """
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector: sc.StorageConnector) -> None:
        self._storage_connector = storage_connector

    def get_databases(self) -> list[str]:
        """Retrieve the list of available databases.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            data_source = fs.get_data_source("test_data_source")

            databases = data_source.get_databases()
            ```

        Returns:
            list[str]: A list of database names available in the data source.
        """
        return self._storage_connector.get_databases()

    def get_tables(self, database: str = None) -> list[DataSource]:
        """Retrieve the list of tables from the specified database.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            data_source = fs.get_data_source("test_data_source")

            tables = data_source.get_tables()
            ```

        Args:
            database (str, optional): The name of the database to list tables from.
                If not provided, the default database is used.

        Returns:
            list[DataSource]: A list of DataSource objects representing the tables.
        """
        return self._storage_connector.get_tables(database)

    def get_data(self) -> dsd.DataSourceData:
        """Retrieve the data from the data source.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            table = fs.get_data_source("test_data_source").get_tables()[0]

            data = table.get_data()
            ```

        Returns:
            DataSourceData: An object containing the data retrieved from the data source.
        """
        return self._storage_connector.get_data(self)

    def get_metadata(self) -> dict:
        """Retrieve metadata information about the data source.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            table = fs.get_data_source("test_data_source").get_tables()[0]

            metadata = table.get_metadata()
            ```

        Returns:
            dict: A dictionary containing metadata about the data source.
        """
        return self._storage_connector.get_metadata(self)

    def get_feature_groups_provenance(self):
        """Get the generated feature groups using this data source, based on explicit provenance.

        These feature groups can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature group links, so deleted
        will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        # Returns
            `Links`: the feature groups generated using this data source or `None` if none were created

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: In case the backend encounters an issue
        """
        return self._storage_connector.get_feature_groups_provenance()

    def get_feature_groups(self):
        """Get the feature groups using this data source, based on explicit provenance.

        Only the accessible feature groups are returned.
        For more items use the base method - get_feature_groups_provenance.

        # Returns
            `List[FeatureGroup]`: List of feature groups.
        """
        return self._storage_connector.get_feature_groups()

    def get_training_datasets_provenance(self):
        """Get the generated training datasets using this data source, based on explicit provenance.

        These training datasets can be accessible or inaccessible. Explicit
        provenance does not track deleted generated training dataset links, so deleted
        will always be empty.
        For inaccessible training datasets, only a minimal information is returned.

        # Returns
            `Links`: the training datasets generated using this data source or `None` if none were created

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: In case the backend encounters an issue
        """
        return self._storage_connector.get_training_datasets_provenance()

    def get_training_datasets(self):
        """Get the training datasets using this data source, based on explicit provenance.

        Only the accessible training datasets are returned.
        For more items use the base method - get_training_datasets_provenance.

        # Returns
            `List[TrainingDataset]`: List of training datasets.
        """
        return self._storage_connector.get_training_datasets()

    @property
    def metrics(self) -> list[str]:
        return self._metrics

    @metrics.setter
    def metrics(self, metrics: list[str]) -> None:
        self._metrics = metrics

    @property
    def dimensions(self) -> list[str]:
        return self._dimensions

    @dimensions.setter
    def dimensions(self, dimensions: list[str]) -> None:
        self._dimensions = dimensions

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
        if storage_connector.type == sc.StorageConnector.RDS and self.database:
            storage_connector._database = self.database

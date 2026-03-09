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
from typing import (
    Any,
)

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hsfs import (
    storage_connector as sc,
)


@public
class DataSource:
    """Metadata object used to provide data source information.

    You can obtain data sources using [`FeatureStore.get_data_source`][hsfs.feature_store.FeatureStore.get_data_source].

    The DataSource class encapsulates the details of a data source that can be used for reading or writing data.
    It supports various types of sources, such as SQL queries, database tables, file paths, and storage connectors.
    """

    def __init__(
        self,
        query: str | None = None,
        database: str | None = None,
        group: str | None = None,
        table: str | None = None,
        path: str | None = None,
        **kwargs,
    ):
        """Initialize a DataSource object.

        Args:
            query: SQL query string for the data source, if applicable.
            database: Name of the database containing the data source.
            group: Group or schema name for the data source.
            table: Table name for the data source.
            path: File system path for the data source.
            **kwargs: Additional keyword arguments.
        """
        self._query = query
        self._database = database
        self._group = group
        self._table = table
        self._path = path

    @classmethod
    def from_response_json(
        cls,
        json_dict: dict[str, Any],
    ) -> DataSource | list[DataSource] | None:
        """Create a DataSource object (or list of objects) from a JSON response.

        Args:
            json_dict: The JSON dictionary from the API response.

        Returns:
            The created object(s), or None if input is None.
        """
        if json_dict is None:
            return None  # TODO: change to [] and fix the tests

        json_decamelized: dict = humps.decamelize(json_dict)

        if "items" not in json_decamelized:
            # TODO: change to [cls(**json_decamelized)] and fix the tests
            return cls(**json_decamelized)
        return [cls(**item) for item in json_decamelized["items"]]

    def to_dict(self) -> dict:
        """Convert the DataSource object to a dictionary.

        Returns:
            Dictionary representation of the object.
        """
        return {
            "query": self._query,
            "database": self._database,
            "group": self._group,
            "table": self._table,
            "path": self._path,
        }

    def json(self) -> str:
        """Serialize the DataSource object to a JSON string.

        Returns:
            JSON string representation of the object.
        """
        return json.dumps(self, cls=util.Encoder)

    @public
    @property
    def query(self) -> str | None:
        """Get or set the SQL query string for the data source.

        Returns:
            The SQL query string.
        """
        return self._query

    @query.setter
    def query(self, query: str) -> None:
        self._query = query

    @public
    @property
    def database(self) -> str | None:
        """Get or set the database name for the data source.

        Returns:
            The database name.
        """
        return self._database

    @database.setter
    def database(self, database: str) -> None:
        self._database = database

    @public
    @property
    def group(self) -> str | None:
        """Get or set the group/schema name for the data source.

        Returns:
            The group or schema name.
        """
        return self._group

    @group.setter
    def group(self, group: str) -> None:
        self._group = group

    @public
    @property
    def table(self) -> str | None:
        """Get or set the table name for the data source.

        Returns:
            The table name.
        """
        return self._table

    @table.setter
    def table(self, table: str) -> None:
        self._table = table

    @public
    @property
    def path(self) -> str | None:
        """Get or set the file system path for the data source.

        Returns:
            The file system path.
        """
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

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

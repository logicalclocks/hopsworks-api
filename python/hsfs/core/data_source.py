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
    Dict,
    Optional,
    Union,
)

import humps
from hopsworks_common import util
from hsfs import storage_connector as sc
from hsfs.core import data_source_api

class DataSource:
    """
    Metadata object used to provide data source information.

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
    """

    def __init__(
        self,
        query: Optional[str] = None,
        database: Optional[str] = None,
        group: Optional[str] = None,
        table: Optional[str] = None,
        path: Optional[str] = None,
        storage_connector: Union[sc.StorageConnector, Dict[str, Any]] = None,
        **kwargs,
    ):
        """
        Initialize a DataSource object.

        Args:
            query (Optional[str]): SQL query string for the data source, if applicable.
            database (Optional[str]): Name of the database containing the data source.
            group (Optional[str]): Group or schema name for the data source.
            table (Optional[str]): Table name for the data source.
            path (Optional[str]): File system path for the data source.
            storage_connector (Union[StorageConnector, Dict[str, Any]], optional): Storage connector object holds configuration for accessing the data source.
            **kwargs: Additional keyword arguments.
        """
        self._data_source_api = data_source_api.DataSourceApi()

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
            self._storage_connector: "sc.StorageConnector" = storage_connector

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any], storage_connector: Optional[sc.StorageConnector] = None
    ) -> "DataSource":
        """
        Create a DataSource object (or list of objects) from a JSON response.

        Args:
            json_dict (Dict[str, Any]): The JSON dictionary from the API response.

        Returns:
            DataSource or List[DataSource] or None: The created object(s), or None if input is None.
        """
        if json_dict is None:
            return None

        json_decamelized: dict = humps.decamelize(json_dict)

        if "items" not in json_decamelized:
            return cls(**json_decamelized, storage_connector=storage_connector)
        else:
            return [
                cls(**item, storage_connector=storage_connector)
                for item in json_decamelized["items"]
            ]

    def to_dict(self):
        """
        Convert the DataSource object to a dictionary.

        Returns:
            dict: Dictionary representation of the object.
        """
        return {
            "query": self._query,
            "database": self._database,
            "group": self._group,
            "table": self._table,
            "path": self._path,
            "storage_connector": self._storage_connector.to_dict()
        }

    def json(self):
        """
        Serialize the DataSource object to a JSON string.

        Returns:
            str: JSON string representation of the object.
        """
        return json.dumps(self, cls=util.Encoder)

    @property
    def query(self) -> Optional[str]:
        """
        Get or set the SQL query string for the data source.

        Returns:
            Optional[str]: The SQL query string.
        """
        return self._query

    @query.setter
    def query(self, query: str) -> None:
        self._query = query

    @property
    def database(self) -> Optional[str]:
        """
        Get or set the database name for the data source.

        Returns:
            Optional[str]: The database name.
        """
        return self._database

    @database.setter
    def database(self, database: str) -> None:
        self._database = database

    @property
    def group(self) -> Optional[str]:
        """
        Get or set the group/schema name for the data source.

        Returns:
            Optional[str]: The group or schema name.
        """
        return self._group

    @group.setter
    def group(self, group: str) -> None:
        self._group = group

    @property
    def table(self) -> Optional[str]:
        """
        Get or set the table name for the data source.

        Returns:
            Optional[str]: The table name.
        """
        return self._table

    @table.setter
    def table(self, table: str) -> None:
        self._table = table

    @property
    def path(self) -> Optional[str]:
        """
        Get or set the file system path for the data source.

        Returns:
            Optional[str]: The file system path.
        """
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    @property
    def storage_connector(self) -> Optional[sc.StorageConnector]:
        """
        Get or set the storage connector for the data source.

        Returns:
            Optional[StorageConnector]: The storage connector object.
        """
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector: sc.StorageConnector) -> None:
        self._storage_connector = storage_connector

    def get_databases(self):
        return self._storage_connector.get_databases()

    def get_tables(self, database: str):
        return self._storage_connector.get_tables(database)

    def get_data(self):
        return self._storage_connector.get_data(self)

    def get_metadata(self):
        return self._storage_connector.get_metadata(self)

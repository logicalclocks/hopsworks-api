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
)

import humps
from hopsworks_common import util
from hsfs import (
    storage_connector as sc,
)


class DataSource:
    """
    Metadata object used to provide Data source information for a feature group.
    """

    def __init__(
        self,
        query: Optional[str] = None,
        database: Optional[str] = None,
        group: Optional[str] = None,
        table: Optional[str] = None,
        path: Optional[str] = None,
        **kwargs,
    ):
        self._query = query
        self._database = database
        self._group = group
        self._table = table
        self._path = path

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> list[DataSource]:
        if json_dict is None:
            return None  # TODO: change to [] and fix the tests

        json_decamelized: dict = humps.decamelize(json_dict)

        if "items" not in json_decamelized:
            # TODO: change to [cls(**json_decamelized)] and fix the tests
            return cls(**json_decamelized)
        else:
            return [cls(**item) for item in json_decamelized["items"]]

    def to_dict(self):
        return {
            "query": self._query,
            "database": self._database,
            "group": self._group,
            "table": self._table,
            "path": self._path,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @property
    def query(self) -> Optional[str]:
        return self._query

    @query.setter
    def query(self, query: str) -> None:
        self._query = query

    @property
    def database(self) -> Optional[str]:
        return self._database

    @database.setter
    def database(self, database: str) -> None:
        self._database = database

    @property
    def group(self) -> Optional[str]:
        return self._group

    @group.setter
    def group(self, group: str) -> None:
        self._group = group

    @property
    def table(self) -> Optional[str]:
        return self._table

    @table.setter
    def table(self, table: str) -> None:
        self._table = table

    @property
    def path(self) -> Optional[str]:
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    def _update_storage_connector(self, storage_connector: sc.StorageConnector):
        """
        Update the storage connector configuration using DataSource.

        This internal method updates the connectors target database, schema,
        and table to match the information stored in the provided DataSource object.

        # Arguments
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
        if storage_connector.type == sc.StorageConnector.RDS:
            if self.database:
                storage_connector._database = self.database

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

from hsfs import storage_connector
from hsfs.core import data_source


class TestDataSource:
    def test_update_storage_connector_redshift(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.RedshiftConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._database_name == ds.database
        assert sc._database_group == ds.group
        assert sc._table_name == ds.table

    def test_update_storage_connector_snowflake(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.SnowflakeConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._database == ds.database
        assert sc._schema == ds.group
        assert sc._table == ds.table

    def test_update_storage_connector_bigquery(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.BigQueryConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._query_project == ds.database
        assert sc._dataset == ds.group
        assert sc._query_table == ds.table

    def test_update_storage_connector_rds(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        sc = storage_connector.RdsConnector(1, "test", 100)

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc._database == ds.database

    def test_update_storage_connector_other(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        # Remove attributes that are objects with changing memory addresses
        ignore_keys = ["_data_source_api", "_storage_connector_api"]

        sc = storage_connector.S3Connector(1, "test", 100)
        sc_dict = {k: v for k, v in vars(sc).items() if k not in ignore_keys}

        # Act
        ds._update_storage_connector(sc)

        # Assert
        assert sc_dict == {k: v for k, v in vars(sc).items() if k not in ignore_keys}

    def test_update_storage_connector_none(self):
        # Arrange
        ds = data_source.DataSource()
        ds.database = "test_database"
        ds.group = "test_group"
        ds.table = "test_table"

        # Act / Assert
        ds._update_storage_connector(None)

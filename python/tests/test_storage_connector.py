#
#   Copyright 2021 Logical Clocks AB
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

import base64
import os
from pathlib import WindowsPath

import pytest
from hsfs import engine, storage_connector
from hsfs.engine import python, spark
from hsfs.storage_connector import BigQueryConnector


class TestHopsfsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_hopsfs"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_hopsfs"
        assert sc._featurestore_id == 67
        assert sc.description == "HOPSFS connector description"
        assert sc._hopsfs_path == "test_path"
        assert sc._dataset_name == "test_dataset_name"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_hopsfs_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_hopsfs"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc._hopsfs_path is None
        assert sc._dataset_name is None


class TestS3Connector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_s3"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_s3"
        assert sc._featurestore_id == 67
        assert sc.description == "S3 connector description"
        assert sc.access_key == "test_access_key"
        assert sc.secret_key == "test_secret_key"
        assert sc.server_encryption_algorithm == "test_server_encryption_algorithm"
        assert sc.server_encryption_key == "test_server_encryption_key"
        assert sc.bucket == "test_bucket"
        assert sc.session_token == "test_session_token"
        assert sc.iam_role == "test_iam_role"
        assert sc.arguments == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_s3_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_s3"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.access_key is None
        assert sc.secret_key is None
        assert sc.server_encryption_algorithm is None
        assert sc.server_encryption_key is None
        assert sc.bucket is None
        assert sc.session_token is None
        assert sc.iam_role is None
        assert sc.arguments == {}

    def test_default_path(self, mocker):
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        mocker.patch(
            "hsfs.storage_connector.StorageConnector._refetch", return_value=None
        )
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine._read")

        # act
        sc = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )
        sc.read(data_format="csv")
        # assert
        assert "s3://test-bucket" in mock_engine_read.call_args[0][3]

    def test_get_path(self, mocker):
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        sc = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )

        # act
        result = sc._get_path("some/location")

        # assert
        assert result == "s3://test-bucket/some/location"

    def test_get_path_storage_connector_with_path(self, mocker):
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        sc = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            bucket="test-bucket",
            path="abc/def",
        )

        # act
        result = sc._get_path("some/location")

        # assert
        assert result == "s3://test-bucket/abc/def/some/location"


class TestGlueConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_glue"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert isinstance(sc, storage_connector.GlueConnector)
        assert sc.id == 2
        assert sc.name == "test_glue"
        assert sc._featurestore_id == 67
        assert sc.description == "Glue connector description"
        assert sc.access_key == "test_access_key"
        assert sc.secret_key == "test_secret_key"
        assert sc.session_token == "test_session_token"
        assert sc.iam_role == "test_iam_role"
        assert sc.region == "eu-north-1"
        assert sc.database == "test_database"
        assert sc.table == "test_table"
        assert sc.arguments == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_glue_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert isinstance(sc, storage_connector.GlueConnector)
        assert sc.id == 2
        assert sc.name == "test_glue"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.access_key is None
        assert sc.secret_key is None
        assert sc.session_token is None
        assert sc.iam_role is None
        assert sc.region is None
        assert sc.database is None
        assert sc.table is None
        assert sc.arguments == {}

    def test_to_dict_roundtrip(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_glue"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        payload = sc.to_dict()

        # Assert
        assert payload["storageConnectorType"] == "GLUE"
        assert payload["type"] == "featurestoreGlueConnectorDTO"
        assert payload["accessKey"] == "test_access_key"
        assert payload["region"] == "eu-north-1"
        assert payload["database"] == "test_database"
        assert payload["table"] == "test_table"
        assert payload["arguments"] == [{"name": "test_name", "value": "test_value"}]

    def test_setup_spark_reuses_s3_conf(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        mock_set_hadoop_conf = mocker.patch("hsfs.engine.spark.Engine._set_hadoop_conf")
        json = backend_fixtures["storage_connector"]["get_glue"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        result = spark.Engine()._setup_storage_connector(
            sc, "s3://ralfsbucket/iceberg-warehouse/ralfsglue.db/fg_1"
        )

        # Assert
        assert result == "s3a://ralfsbucket/iceberg-warehouse/ralfsglue.db/fg_1"
        conf_keys = {call.args[0] for call in mock_set_hadoop_conf.call_args_list}
        assert "fs.s3a.access.key" in conf_keys
        assert "fs.s3a.secret.key" in conf_keys
        # No fixed bucket -> only the global level is configured.
        assert not any(k.startswith("fs.s3a.bucket.") for k in conf_keys)

    def test_catalog_options(self):
        # Arrange
        sc = storage_connector.GlueConnector(
            id=2,
            name="test_glue",
            featurestore_id=67,
            region="eu-north-1",
        )

        # Act
        options = sc.catalog_options(warehouse="s3://ralfsbucket/iceberg-warehouse")

        # Assert
        assert options["catalog-impl"] == sc.GLUE_CATALOG_IMPL
        assert options["io-impl"] == sc.GLUE_IO_IMPL
        assert options["client.region"] == "eu-north-1"
        assert options["warehouse"] == "s3://ralfsbucket/iceberg-warehouse"

    def test_pyiceberg_catalog_options(self):
        # Arrange
        sc = storage_connector.GlueConnector(
            id=2,
            name="test_glue",
            featurestore_id=67,
            access_key="ak",
            secret_key="sk",
            region="eu-north-1",
        )

        # Act
        options = sc.pyiceberg_catalog_options(
            warehouse="s3://ralfsbucket/iceberg-warehouse"
        )

        # Assert — PyIceberg identifies the catalog by type, not impl class.
        assert options["type"] == "glue"
        assert "catalog-impl" not in options
        assert options["glue.region"] == "eu-north-1"
        assert options["s3.region"] == "eu-north-1"
        assert options["s3.access-key-id"] == "ak"
        assert options["warehouse"] == "s3://ralfsbucket/iceberg-warehouse"

    def test_get_tables_defaults_to_connector_database(self, mocker):
        # Arrange — get_tables() with no argument uses the connector's database.
        sc = storage_connector.GlueConnector(
            id=2,
            name="test_glue",
            featurestore_id=67,
            database="ralfsglue",
        )
        mock_get_tables = mocker.patch.object(
            sc._data_source_api, "_get_tables", return_value=[]
        )

        # Act
        sc.get_tables()

        # Assert
        mock_get_tables.assert_called_once_with(sc, "ralfsglue")

    def test_get_tables_explicit_database_overrides_default(self, mocker):
        # Arrange
        sc = storage_connector.GlueConnector(
            id=2,
            name="test_glue",
            featurestore_id=67,
            database="ralfsglue",
        )
        mock_get_tables = mocker.patch.object(
            sc._data_source_api, "_get_tables", return_value=[]
        )

        # Act
        sc.get_tables("otherdb")

        # Assert
        mock_get_tables.assert_called_once_with(sc, "otherdb")

    def test_get_tables_without_database_raises(self):
        # Arrange — no database on the connector and none passed.
        sc = storage_connector.GlueConnector(id=2, name="test_glue", featurestore_id=67)

        # Act & Assert
        with pytest.raises(ValueError, match="Database name is required for Glue"):
            sc.get_tables()


class TestUnityCatalogConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_unity_catalog"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_unity_catalog"
        assert sc._featurestore_id == 67
        assert sc.description == "Unity Catalog connector description"
        assert sc.type == storage_connector.StorageConnector.UNITY_CATALOG
        assert sc.workspace_url == "https://test.cloud.databricks.com"
        # access_token itself is write-only on the backend; the server never
        # returns it on GET. hasAccessToken signals that one is on file.
        assert sc.access_token is None
        assert sc.has_access_token is True
        assert sc.auth_method == "PAT"
        assert sc.default_catalog == "test_catalog"
        assert sc.aws_region == "us-west-2"
        assert sc.arguments == {"arg1": "val1"}

    def test_from_response_json_oauth_workspace(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"][
            "get_unity_catalog_oauth_workspace"
        ]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.auth_method == "OAUTH_M2M"
        assert sc.oauth_endpoint == "WORKSPACE"
        assert sc.client_id == "test-sp-client-id"
        assert sc.client_secret is None
        assert sc.has_client_secret is True
        assert sc.account_id is None
        assert sc.account_host is None

    def test_from_response_json_oauth_account(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_unity_catalog_oauth_account"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.auth_method == "OAUTH_M2M"
        assert sc.oauth_endpoint == "ACCOUNT"
        assert sc.client_id == "test-sp-client-id"
        assert sc.client_secret is None
        assert sc.has_client_secret is True
        assert sc.account_id == "12345678-1234-1234-1234-1234567890ab"
        assert sc.account_host == "accounts.cloud.databricks.com"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_unity_catalog_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_unity_catalog"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.workspace_url is None
        assert sc.access_token is None
        assert sc.default_catalog is None
        assert sc.aws_region is None
        assert sc.arguments == {}

    def test_connector_options(self):
        sc = storage_connector.UnityCatalogConnector(
            id=1,
            name="uc",
            featurestore_id=1,
            workspace_url="https://ws.cloud.databricks.com",
            access_token="dapi-xyz",
            default_catalog="sales",
        )
        opts = sc.connector_options()
        assert opts["workspace_url"] == "https://ws.cloud.databricks.com"
        assert opts["default_catalog"] == "sales"

    def test_spark_options_not_supported(self):
        sc = storage_connector.UnityCatalogConnector(
            id=1, name="uc", featurestore_id=1, workspace_url="https://x.com"
        )
        with pytest.raises(NotImplementedError):
            sc.spark_options()

    def test_legacy_construction_defaults_pat(self):
        # Connectors built before OAuth support landed have no auth_method
        # field at all. They must keep working as PAT.
        sc = storage_connector.UnityCatalogConnector(
            id=1,
            name="uc",
            featurestore_id=1,
            workspace_url="https://ws.cloud.databricks.com",
            access_token="dapi-xyz",
        )
        assert sc.auth_method == "PAT"
        assert sc.oauth_endpoint is None
        assert sc.client_id is None
        assert sc.has_access_token is True
        assert sc.has_client_secret is False

    def test_oauth_construction_defaults_workspace_endpoint(self):
        # auth_method=OAUTH_M2M without oauth_endpoint defaults to WORKSPACE,
        # matching the frontend default.
        sc = storage_connector.UnityCatalogConnector(
            id=1,
            name="uc",
            featurestore_id=1,
            workspace_url="https://ws.cloud.databricks.com",
            auth_method="OAUTH_M2M",
            client_id="cid",
            client_secret="csec",
        )
        assert sc.oauth_endpoint == "WORKSPACE"
        assert sc.client_secret == "csec"
        assert sc.has_client_secret is True


class TestRedshiftConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_redshift"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_redshift"
        assert sc._featurestore_id == 67
        assert sc.description == "Redshift connector description"
        assert sc.cluster_identifier == "test_cluster_identifier"
        assert sc.database_driver == "test_database_driver"
        assert sc.database_endpoint == "test_database_endpoint"
        assert sc.database_name == "test_database_name"
        assert sc.database_port == "test_database_port"
        assert sc.table_name == "test_table_name"
        assert sc.database_user_name == "test_database_user_name"
        assert sc.auto_create == "test_auto_create"
        assert sc.database_password == "test_database_password"
        assert sc.database_group == "test_database_group"
        assert sc.iam_role == "test_iam_role"
        assert sc.arguments == "test_arguments"
        assert sc.expiration == "test_expiration"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_redshift_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_redshift"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.cluster_identifier is None
        assert sc.database_driver is None
        assert sc.database_endpoint is None
        assert sc.database_name is None
        assert sc.database_port is None
        assert sc.table_name is None
        assert sc.database_user_name is None
        assert sc.auto_create is None
        assert sc.database_password is None
        assert sc.database_group is None
        assert sc.iam_role is None
        assert sc.arguments is None
        assert sc.expiration is None

    def test_read_query_option(self, mocker):
        sc = storage_connector.RedshiftConnector(
            id=1,
            name="redshiftconn",
            featurestore_id=1,
            table_name="abc",
            cluster_identifier="cluster",
            database_endpoint="us-east-2",
            database_port=5439,
            database_name="db",
        )

        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        mocker.patch("hsfs.core.storage_connector_api.StorageConnectorApi._refetch")
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine._read")

        query = "select * from table"
        sc.read(query=query)

        assert mock_engine_read.call_args[0][2].get("dbtable", None) is None
        assert mock_engine_read.call_args[0][2].get("query") == query


class TestAdlsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_adls"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_adls"
        assert sc._featurestore_id == 67
        assert sc.description == "Adls connector description"
        assert sc.generation == "test_generation"
        assert sc.directory_id == "test_directory_id"
        assert sc.application_id == "test_application_id"
        assert sc.service_credential == "test_service_credential"
        assert sc.account_name == "test_account_name"
        assert sc.container_name == "test_container_name"
        assert sc._spark_options == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_adls_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_adls"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.generation is None
        assert sc.directory_id is None
        assert sc.application_id is None
        assert sc.service_credential is None
        assert sc.account_name is None
        assert sc.container_name is None
        assert sc._spark_options == {}

    def test_default_path(self, mocker):
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine._read")
        # act
        sc = storage_connector.AdlsConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            account_name="test_account",
            container_name="test_container",
            generation=2,
        )
        sc.read(data_format="csv")
        # assert read path value
        print(mock_engine_read.call_args[0])
        assert (
            "abfss://test_container@test_account.dfs.core.windows.net"
            in mock_engine_read.call_args[0][3]
        )


class TestSnowflakeConnector:
    def test_read_query_option(self, mocker):
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine._read")

        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table="snowflake_table"
        )
        query = "select * from table"
        snowflake_connector.read(query=query)

        assert mock_engine_read.call_args[0][2].get("dbtable", None) is None
        assert mock_engine_read.call_args[0][2].get("query") == query

    def test_spark_options_db_table_none(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=None
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_empty(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table=""
        )

        spark_options = snowflake_connector.spark_options()

        assert "dbtable" not in spark_options

    def test_spark_options_db_table_value(self):
        snowflake_connector = storage_connector.SnowflakeConnector(
            id=1, name="test_connector", featurestore_id=1, table="test"
        )

        spark_options = snowflake_connector.spark_options()

        assert spark_options["dbtable"] == "test"

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_snowflake"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_snowflake"
        assert sc._featurestore_id == 67
        assert sc.description == "Snowflake connector description"
        assert sc.database == "test_database"
        assert sc.password == "test_password"
        assert sc.token == "test_token"
        assert sc.role == "test_role"
        assert sc.schema == "test_schema"
        assert sc.table == "test_table"
        assert sc.url == "test_url"
        assert sc.user == "test_user"
        assert sc.warehouse == "test_warehouse"
        assert sc.application == "test_application"
        assert sc._options == {"test_name": "test_value"}
        assert (
            sc.private_key
            == "-----BEGIN ENCRYPTED PRIVATE KEY-----\nranDomKey123\nAsdfsdfqweq9809==\n-----END ENCRYPTED PRIVATE KEY-----\n"
        )
        assert sc.passphrase == "test_passphrase"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_snowflake_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_snowflake"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.database is None
        assert sc.password is None
        assert sc.token is None
        assert sc.role is None
        assert sc.schema is None
        assert sc.table is None
        assert sc.url is None
        assert sc.user is None
        assert sc.warehouse is None
        assert sc.application is None
        assert sc._options == {}

    def test_spark_options_private_key(self, mocker, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_snowflake"]["response"]
        json.pop("password", None)
        json.pop("token", None)
        sc = storage_connector.StorageConnector.from_response_json(json)

        expected_bytes = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678901234567890"
        sc._read_private_key = mocker.Mock(return_value=expected_bytes)
        # act
        spark_options = sc.spark_options()
        # assert
        assert "pem_private_key" in spark_options
        assert spark_options["pem_private_key"] == expected_bytes


class TestJdbcConnector:
    def test_spark_options_arguments_none(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=None,
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string

    def test_spark_options_arguments_empty(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments="",
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string

    def test_spark_options_arguments_arguments(self):
        connection_string = (
            "jdbc:mysql://mysql_server_ip:1433;database=test;loginTimeout=30;"
        )
        arguments = [
            {"name": "arg1", "value": "value1"},
            {"name": "arg2", "value": "value2"},
        ]

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string=connection_string,
            arguments=arguments,
        )

        spark_options = jdbc_connector.spark_options()

        assert spark_options["url"] == connection_string
        assert spark_options["arg1"] == "value1"
        assert spark_options["arg2"] == "value2"

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_jdbc"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_jdbc"
        assert sc._featurestore_id == 67
        assert sc.description == "JDBC connector description"
        assert sc.connection_string == "test_conn_string"
        assert sc.arguments == [
            {"name": "sslTrustStore"},
            {"name": "trustStorePassword"},
            {"name": "sslKeyStore"},
            {"name": "keyStorePassword"},
        ]

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_jdbc_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_jdbc"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.connection_string is None
        assert sc.arguments is None


class TestKafkaConnector:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_kafka"
        assert sc._featurestore_id == 67
        assert sc.description == "Kafka connector description"
        assert sc._bootstrap_servers == "test_bootstrap_servers"
        assert sc.security_protocol == "test_security_protocol"
        assert sc.ssl_truststore_location == "test_ssl_truststore_location"
        assert sc._ssl_truststore_password == "test_ssl_truststore_password"
        assert sc.ssl_keystore_location == "test_ssl_keystore_location"
        assert sc._ssl_keystore_password == "test_ssl_keystore_password"
        assert sc._ssl_key_password == "test_ssl_key_password"
        assert (
            sc.ssl_endpoint_identification_algorithm
            == "test_ssl_endpoint_identification_algorithm"
        )
        assert sc.options == {"test_option_name": "test_option_value"}

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_kafka_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_kafka"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc._bootstrap_servers is None
        assert sc.security_protocol is None
        assert sc.ssl_truststore_location is None
        assert sc._ssl_truststore_password is None
        assert sc.ssl_keystore_location is None
        assert sc._ssl_keystore_password is None
        assert sc._ssl_key_password is None
        assert sc.ssl_endpoint_identification_algorithm is None
        assert sc.options == {}

    # Unit test for storage connector created by user (i.e. without the external flag)
    def test_kafka_options_user_sc(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]

        mock_engine_get_instance.return_value._add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.kafka_options()

        # Assert
        assert config == {
            "test_option_name": "test_option_value",
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.truststore.location": "result_from_add_file",
            "ssl.truststore.password": "test_ssl_truststore_password",
            "ssl.keystore.location": "result_from_add_file",
            "ssl.keystore.password": "test_ssl_keystore_password",
            "ssl.key.password": "test_ssl_key_password",
        }

    def test_kafka_options_internal(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine._get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "result_from_get_jks_trust_store_path"
        )
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = (
            "result_from_get_jks_key_store_path"
        )
        mock_client_get_instance.return_value._cert_key = "result_from_cert_key"

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.kafka_options()

        # Assert
        assert config == {
            "test_option_name": "test_option_value",
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.truststore.location": "result_from_get_jks_trust_store_path",
            "ssl.truststore.password": "result_from_cert_key",
            "ssl.keystore.location": "result_from_get_jks_key_store_path",
            "ssl.keystore.password": "result_from_cert_key",
            "ssl.key.password": "result_from_cert_key",
        }

    def test_kafka_options_external(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        sc = storage_connector.StorageConnector.from_response_json(json)

        mock_engine_get_instance.return_value._add_file.return_value = (
            "result_from_add_file"
        )

        # Act
        config = sc.kafka_options()

        # Assert
        assert config == {
            "test_option_name": "test_option_value",
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.truststore.location": "result_from_add_file",
            "ssl.truststore.password": "test_ssl_truststore_password",
            "ssl.keystore.location": "result_from_add_file",
            "ssl.keystore.password": "test_ssl_keystore_password",
            "ssl.key.password": "test_ssl_key_password",
        }

    def test_spark_options(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value._get_spark_version.return_value = "3.1.0"

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "result_from_get_jks_trust_store_path"
        )
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = (
            "result_from_get_jks_key_store_path"
        )
        mock_client_get_instance.return_value._cert_key = "result_from_cert_key"

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.spark_options()

        # Assert
        assert config == {
            "kafka.test_option_name": "test_option_value",
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.truststore.location": "result_from_get_jks_trust_store_path",
            "kafka.ssl.truststore.password": "result_from_cert_key",
            "kafka.ssl.keystore.location": "result_from_get_jks_key_store_path",
            "kafka.ssl.keystore.password": "result_from_cert_key",
            "kafka.ssl.key.password": "result_from_cert_key",
        }

    def test_spark_options_spark_35(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value._get_spark_version.return_value = "3.5.0"
        mock_engine_get_instance.return_value._add_file.return_value = (
            "result_from_add_file"
        )

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "result_from_get_jks_trust_store_path"
        )
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = (
            "result_from_get_jks_key_store_path"
        )
        mock_client_get_instance.return_value._cert_key = "result_from_cert_key"
        mock_client_get_instance.return_value._write_pem.return_value = (
            None,
            None,
            None,
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Mock the read pem method in the storage connector itself
        sc._read_pem = mocker.Mock()
        sc._read_pem.side_effect = [
            "test_ssl_ca",
            "test_ssl_certificate",
            "test_ssl_key",
        ]

        # Act
        config = sc.spark_options()

        # Assert
        assert config == {
            "kafka.test_option_name": "test_option_value",
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.truststore.type": "PEM",
            "kafka.ssl.keystore.type": "PEM",
            "kafka.ssl.truststore.certificates": "test_ssl_ca",
            "kafka.ssl.keystore.certificate.chain": "test_ssl_certificate",
            "kafka.ssl.keystore.key": "test_ssl_key",
        }

        mock_engine_get_instance.return_value._add_file.assert_not_called()
        mock_engine_get_instance.return_value._add_file.assert_not_called()

    def test_spark_options_external(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        mock_engine_get_instance.return_value._get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value._add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        config = sc.spark_options()

        # Assert
        assert config == {
            "kafka.test_option_name": "test_option_value",
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.truststore.location": "result_from_add_file",
            "kafka.ssl.truststore.password": "test_ssl_truststore_password",
            "kafka.ssl.keystore.location": "result_from_add_file",
            "kafka.ssl.keystore.password": "test_ssl_keystore_password",
            "kafka.ssl.key.password": "test_ssl_key_password",
        }

    def test_spark_options_spark_35_external(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        mock_engine_get_instance.return_value._get_spark_version.return_value = "3.5.0"
        mock_engine_get_instance.return_value._add_file.return_value = (
            "result_from_add_file"
        )

        mock_client_get_instance.return_value._get_jks_trust_store_path.return_value = (
            "result_from_get_jks_trust_store_path"
        )
        mock_client_get_instance.return_value._get_jks_key_store_path.return_value = (
            "result_from_get_jks_key_store_path"
        )
        mock_client_get_instance.return_value._cert_key = "result_from_cert_key"
        mock_client_get_instance.return_value._write_pem.return_value = (
            None,
            None,
            None,
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Mock the read pem method in the storage connector itself
        sc._read_pem = mocker.Mock()
        sc._read_pem.side_effect = [
            "test_ssl_ca",
            "test_ssl_certificate",
            "test_ssl_key",
        ]

        # Act
        config = sc.spark_options()

        # Assert
        assert config == {
            "kafka.test_option_name": "test_option_value",
            "kafka.bootstrap.servers": "test_bootstrap_servers",
            "kafka.security.protocol": "test_security_protocol",
            "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "kafka.ssl.truststore.type": "PEM",
            "kafka.ssl.keystore.type": "PEM",
            "kafka.ssl.truststore.certificates": "test_ssl_ca",
            "kafka.ssl.keystore.certificate.chain": "test_ssl_certificate",
            "kafka.ssl.keystore.key": "test_ssl_key",
        }

        mock_engine_get_instance.return_value._add_file.assert_any_call(
            "test_ssl_truststore_location", distribute=False
        )
        mock_engine_get_instance.return_value._add_file.assert_any_call(
            "test_ssl_keystore_location", distribute=False
        )

    def test_confluent_options(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value._add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        mock_client = mocker.patch("hopsworks_common.client._get_instance")
        mock_client.return_value._write_pem.return_value = (
            "test_ssl_ca_location",
            "test_ssl_certificate_location",
            "test_ssl_key_location",
        )

        # Act
        config = sc.confluent_options()

        # Assert
        assert config == {
            "bootstrap.servers": "test_bootstrap_servers",
            "security.protocol": "test_security_protocol",
            "ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
            "ssl.ca.location": "test_ssl_ca_location",
            "ssl.certificate.location": "test_ssl_certificate_location",
            "ssl.key.location": "test_ssl_key_location",
        }

    def test_confluent_options_jaas_single_quotes(self, mocker):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        mock_engine_get_instance.return_value._add_file.return_value = None
        mocker.patch("hopsworks_common.client._get_instance")
        sc = storage_connector.KafkaConnector(
            1,
            "kafka_connector",
            0,
            external_kafka=True,
            options=[
                {
                    "name": "sasl.jaas.config",
                    "value": "org.apache.kafka.common.security.plain.PlainLoginModule required username='222' password='111';",
                }
            ],
        )

        # Act
        config = sc.confluent_options()

        # Assert
        assert config == {
            "bootstrap.servers": None,
            "security.protocol": None,
            "ssl.endpoint.identification.algorithm": None,
            "sasl.mechanisms": "PLAIN",
            "sasl.password": "111",
            "sasl.username": "222",
        }

    def test_confluent_options_jaas_double_quotes(self, mocker):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine._get_instance")
        mock_engine_get_instance.return_value._add_file.return_value = None
        mocker.patch("hopsworks_common.client._get_instance")
        sc = storage_connector.KafkaConnector(
            1,
            "kafka_connector",
            0,
            external_kafka=True,
            options=[
                {
                    "name": "sasl.jaas.config",
                    "value": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="222" password="111";',
                }
            ],
        )

        # Act
        config = sc.confluent_options()

        # Assert
        assert config == {
            "bootstrap.servers": None,
            "security.protocol": None,
            "ssl.endpoint.identification.algorithm": None,
            "sasl.mechanisms": "PLAIN",
            "sasl.password": "111",
            "sasl.username": "222",
        }


class TestGcsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_gcs"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_gcs"
        assert sc._featurestore_id == 67
        assert sc.description == "Gcs connector description"
        assert sc.key_path == "test_key_path"
        assert sc.bucket == "test_bucket"
        assert sc.algorithm == "test_algorithm"
        assert sc.encryption_key == "test_encryption_key"
        assert sc.encryption_key_hash == "test_encryption_key_hash"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_gcs_basic_info"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_gcs"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.key_path is None
        assert sc.bucket is None
        assert sc.algorithm is None
        assert sc.encryption_key is None
        assert sc.encryption_key_hash is None

    def test_python_support_validation(self, backend_fixtures):
        engine._set_instance("python", python.Engine())
        json = backend_fixtures["storage_connector"]["get_gcs_basic_info"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        with pytest.raises(NotImplementedError):
            sc.read()

    def test_default_path(self, mocker):
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine._read")
        # act
        sc = storage_connector.GcsConnector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )
        sc.read(data_format="csv")
        # assert
        assert mock_engine_read.call_args[0][3] == "gs://test-bucket"


class TestGoogleSheetsConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_google_sheets"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert isinstance(sc, storage_connector.GoogleSheetsConnector)
        assert sc.id == 1
        assert sc.name == "test_google_sheets"
        assert sc._featurestore_id == 67
        assert sc.description == "Google Sheets connector description"
        assert sc.key_path == "test_key_path"
        assert sc.spreadsheet_id == "test_spreadsheet_id"

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_google_sheets_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert isinstance(sc, storage_connector.GoogleSheetsConnector)
        assert sc.id == 1
        assert sc.name == "test_google_sheets"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.key_path is None
        assert sc.spreadsheet_id is None

    def test_to_dict(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_google_sheets"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        payload = sc.to_dict()

        # Assert
        assert payload["id"] == 1
        assert payload["name"] == "test_google_sheets"
        assert payload["featurestoreId"] == 67
        assert payload["storageConnectorType"] == "GOOGLE_SHEETS"
        assert payload["type"] == "featurestoreGoogleSheetsConnectorDTO"
        assert payload["keyPath"] == "test_key_path"
        assert payload["spreadsheetId"] == "test_spreadsheet_id"

    def test_spark_options_empty(self):
        # Arrange
        sc = storage_connector.GoogleSheetsConnector(
            id=1, name="test_google_sheets", featurestore_id=67
        )

        # Act / Assert
        assert sc.spark_options() == {}


class TestBigQueryConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_big_query_table"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_big_query"
        assert sc._featurestore_id == 67
        assert sc.description == "BigQuery connector description"
        assert sc.key_path == "test_key_path"
        assert sc.parent_project == "test_parent_project"
        assert sc.dataset == "test_dataset"
        assert sc.query_table == "test_query_table"
        assert sc.query_project == "test_query_project"
        assert sc.arguments == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_big_query_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert sc.id == 1
        assert sc.name == "test_big_query"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.key_path is None
        assert sc.parent_project is None
        assert sc.dataset is None
        assert sc.query_table is None
        assert sc.query_project is None
        assert sc.materialization_dataset is None
        assert sc.arguments == {}

    def test_credentials_base64_encoded(self, mocker, backend_fixtures, tmp_path):
        # Arrange
        engine._set_instance("spark", spark.Engine())
        mocker.patch("hopsworks_common.client._is_external", return_value=False)

        credentials = '{"type": "service_account", "project_id": "test"}'

        credentialsFile = tmp_path / "bigquery.json"
        credentialsFile.write_text(credentials)

        json = backend_fixtures["storage_connector"]["get_big_query_table"]["response"]
        if isinstance(tmp_path, WindowsPath):
            json["key_path"] = "file:///" + str(credentialsFile.resolve()).replace(
                "\\", "/"
            )
        else:
            json["key_path"] = "file://" + str(credentialsFile.resolve())

        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        spark_options = sc.spark_options()

        # Assert - Credentials should be base64 encoded
        assert (
            base64.b64decode(spark_options[BigQueryConnector.BIGQ_CREDENTIALS]).decode(
                "utf-8"
            )
            == credentials
        )

    def test_python_support_validation(self, backend_fixtures):
        # Arrange
        engine._set_instance("python", python.Engine())
        json = backend_fixtures["storage_connector"]["get_big_query_basic_info"][
            "response"
        ]
        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)
        # Assert
        with pytest.raises(NotImplementedError):
            sc.read()

    def test_query_validation(self, mocker, backend_fixtures, tmp_path):
        # Arrange
        engine._set_instance("spark", spark.Engine())
        mocker.patch("hopsworks_common.client._is_external", return_value=False)

        credentials = '{"type": "service_account", "project_id": "test"}'
        credentialsFile = tmp_path / "bigquery.json"
        credentialsFile.write_text(credentials)
        json = backend_fixtures["storage_connector"]["get_big_query_table"]["response"]
        if isinstance(tmp_path, WindowsPath):
            json["key_path"] = "file:///" + str(credentialsFile.resolve()).replace(
                "\\", "/"
            )
        else:
            json["key_path"] = "file://" + str(credentialsFile.resolve())
        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)
        # Assert
        with pytest.raises(ValueError):
            sc.read(query="select * from")

    def test_connector_options(self, backend_fixtures):
        # Arrange
        engine._set_instance("python", python.Engine())
        json = backend_fixtures["storage_connector"]["get_big_query_query"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        options = sc.connector_options()

        # Assert
        assert options["project_id"] == "test_parent_project"


class TestSqlConnector:
    def _make_connector(self, database_type):
        return storage_connector.SqlConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            database_type=database_type,
            host="localhost",
            port=3306,
            database="testdb",
            user="user",
            password="pass",
        )

    @pytest.mark.parametrize(
        "database_type, expected_driver, expected_scheme",
        [
            ("MYSQL", "com.mysql.cj.jdbc.Driver", "mysql"),
            ("mysql", "com.mysql.cj.jdbc.Driver", "mysql"),  # normalised to uppercase
            ("POSTGRESQL", "org.postgresql.Driver", "postgresql"),
        ],
    )
    def test_spark_options_driver(
        self, database_type, expected_driver, expected_scheme
    ):
        # Arrange
        connector = self._make_connector(database_type)

        # Act
        options = connector.spark_options()

        # Assert
        assert options["driver"] == expected_driver
        assert options["url"] == f"jdbc:{expected_scheme}://localhost:3306/testdb"

    @pytest.mark.parametrize(
        "database_type, expected_driver, expected_scheme",
        [
            ("MYSQL", "com.mysql.cj.jdbc.Driver", "mysql"),
            ("POSTGRESQL", "org.postgresql.Driver", "postgresql"),
        ],
    )
    def test_read_jdbc_url_scheme(
        self, mocker, database_type, expected_driver, expected_scheme
    ):
        # Arrange
        connector = self._make_connector(database_type)
        mock_read = mocker.patch("hsfs.engine._get_instance")
        mocker.patch.object(connector, "_refetch")

        # Act
        connector.read(query="SELECT 1")

        # Assert
        call_options = mock_read.return_value._read.call_args[0][2]
        assert call_options["url"] == f"jdbc:{expected_scheme}://localhost:3306/testdb"

    def test_read_oracle_jdbc_url(self, mocker):
        """read() should pass the Oracle ``jdbc:oracle:thin:@host:port/service`` URL to the engine."""
        # Arrange
        connector = storage_connector.SqlConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
        )
        mock_engine = mocker.patch("hsfs.engine._get_instance")
        mocker.patch.object(connector, "_refetch")

        # Act
        connector.read(query="SELECT 1 FROM DUAL")

        # Assert
        call_options = mock_engine.return_value._read.call_args[0][2]
        assert call_options["url"] == "jdbc:oracle:thin:@myhost:1521/ORCL"
        assert call_options["driver"] == "oracle.jdbc.driver.OracleDriver"
        assert call_options["query"] == "SELECT 1 FROM DUAL"

    def test_unsupported_database_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported database_type"):
            self._make_connector("UNSUPPORTED_DB")

    def test_spark_options_includes_arguments(self):
        # Arrange
        connector = storage_connector.SqlConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            database_type="MYSQL",
            host="localhost",
            port=3306,
            database="testdb",
            user="user",
            password="pass",
            arguments=[{"name": "connectTimeout", "value": "5000"}],
        )

        # Act
        options = connector.spark_options()

        # Assert
        assert options["connectTimeout"] == "5000"
        # explicit fields take precedence over arguments
        assert options["user"] == "user"
        assert options["driver"] == "com.mysql.cj.jdbc.Driver"

    def test_spark_options_arguments_do_not_override_explicit_fields(self):
        # Arrange: argument tries to override driver
        connector = storage_connector.SqlConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            database_type="MYSQL",
            host="localhost",
            port=3306,
            database="testdb",
            user="user",
            password="pass",
            arguments=[{"name": "driver", "value": "com.other.Driver"}],
        )

        # Act
        options = connector.spark_options()

        # Assert: the explicit driver wins
        assert options["driver"] == "com.mysql.cj.jdbc.Driver"


class TestOracleConnector:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_oracle"]["response"]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert isinstance(sc, storage_connector.SqlConnector)
        assert sc.id == 1
        assert sc.name == "test_oracle"
        assert sc._featurestore_id == 67
        assert sc.description == "Oracle connector description"
        assert sc.database_type == "ORACLE"
        assert sc._host == "test_host"
        assert sc._port == 1521
        assert sc._database == "test_database"
        assert sc._user == "test_user"
        assert sc._password == "test_password"
        assert sc._wallet_path == "/Projects/test_project/Resources/wallet.zip"
        assert sc._wallet_password == "test_wallet_password"
        assert sc._arguments == {"test_name": "test_value"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_oracle_basic_info"][
            "response"
        ]

        # Act
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Assert
        assert isinstance(sc, storage_connector.SqlConnector)
        assert sc.id == 1
        assert sc.name == "test_oracle"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.database_type == "ORACLE"
        assert sc._host is None
        assert sc._database is None

    def test_spark_options(self):
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
        )
        opts = sc.spark_options()
        assert opts["url"] == "jdbc:oracle:thin:@myhost:1521/ORCL"
        assert opts["driver"] == "oracle.jdbc.driver.OracleDriver"
        assert opts["user"] == "scott"
        assert opts["password"] == "tiger"

    def test_connector_options(self):
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
        )
        opts = sc.connector_options()
        assert opts["host"] == "myhost"
        assert opts["port"] == 1521
        assert opts["service_name"] == "ORCL"
        assert opts["user"] == "scott"
        assert opts["password"] == "tiger"

    def test_spark_options_wallet_uses_tcps(self):
        """When wallet_path is set, spark_options should use tcps URL."""
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
            wallet_path="/Projects/myproj/Resources/wallet.zip",
        )
        opts = sc.spark_options()
        assert opts["url"] == "jdbc:oracle:thin:@tcps://myhost:1521/ORCL"
        # Wallet JDBC properties are set in read(), not spark_options()
        assert "oracle.net.wallet_location" not in opts

    def test_read_wallet_sets_jdbc_properties(self, mocker, tmp_path):
        """read() should download/extract wallet and set Oracle JDBC wallet properties."""
        import zipfile

        # Create a local wallet zip to simulate what add_file returns
        wallet_zip = tmp_path / "wallet.zip"
        with zipfile.ZipFile(str(wallet_zip), "w") as zf:
            zf.writestr("cwallet.sso", "fake")

        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
            wallet_path="/Projects/myproj/Resources/wallet.zip",
            wallet_password="walletpass",
        )

        mock_engine = mocker.patch("hsfs.engine._get_instance")
        mock_engine.return_value._add_file.return_value = str(wallet_zip)
        mocker.patch.object(sc, "_refetch")

        sc.read(query="SELECT 1")

        # Verify add_file was called with distribute=False
        mock_engine.return_value._add_file.assert_called_once_with(
            "/Projects/myproj/Resources/wallet.zip", distribute=False
        )

        # Oracle reads always go through _read_jdbc_on_driver so wallet files
        # (which only exist on the driver) are accessible.
        call_options = mock_engine.return_value._read_jdbc_on_driver.call_args[0][0]
        assert call_options["url"] == "jdbc:oracle:thin:@tcps://myhost:1521/ORCL"
        assert "oracle.net.wallet_location" in call_options
        expected_dir = str(tmp_path / "wallet")
        assert expected_dir in call_options["oracle.net.wallet_location"]
        assert call_options["oracle.net.tns_admin"] == expected_dir
        assert call_options["oracle.net.wallet_password"] == "walletpass"
        assert os.path.isfile(os.path.join(expected_dir, "cwallet.sso"))

    def test_connector_options_wallet(self):
        """connector_options should include wallet_path and wallet_password."""
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
            wallet_path="/Projects/myproj/Resources/wallet.zip",
            wallet_password="walletpass",
        )
        opts = sc.connector_options()
        assert opts["wallet_path"] == "/Projects/myproj/Resources/wallet.zip"
        assert opts["wallet_password"] == "walletpass"

    def test_spark_options_wallet_only_uses_tns_alias_url(self):
        """Wallet-only Oracle: URL carries the TNS alias only, no host:port."""
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            database="mydb_high",  # TNS alias from tnsnames.ora
            user="scott",
            password="tiger",
            wallet_path="/Projects/myproj/Resources/wallet.zip",
        )
        opts = sc.spark_options()
        assert opts["url"] == "jdbc:oracle:thin:@mydb_high"

    def test_inline_tns_url_prefers_configured_database_alias(self, tmp_path):
        """The database alias wins over _tp and its descriptor is inlined.

        Matching is case-insensitive; a bare alias URL is not a hostname.
        """
        (tmp_path / "tnsnames.ora").write_text(
            "mydb_low = (description=(address=(host=low.example.com)))\n"
            "mydb_tp = (description=(address=(host=tp.example.com)))\n"
        )
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            database="MYDB_low",  # TNS alias, different case than tnsnames.ora
            user="scott",
            password="tiger",
            wallet_path="/Projects/myproj/Resources/wallet.zip",
        )
        url = sc._inline_tns_url(str(tmp_path))
        assert url == "jdbc:oracle:thin:@(description=(address=(host=low.example.com)))"

    def test_inline_tns_url_falls_back_to_tp_alias(self, tmp_path):
        """When the database is not an alias, prefer the _tp alias."""
        (tmp_path / "tnsnames.ora").write_text(
            "mydb_low = (description=(address=(host=low.example.com)))\n"
            "mydb_tp = (description=(address=(host=tp.example.com)))\n"
        )
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            database="not_an_alias",
            user="scott",
            password="tiger",
            wallet_path="/Projects/myproj/Resources/wallet.zip",
        )
        url = sc._inline_tns_url(str(tmp_path))
        assert url == "jdbc:oracle:thin:@(description=(address=(host=tp.example.com)))"

    def test_spark_options_no_host_no_wallet_raises(self):
        """Oracle connector without host/port AND without wallet is invalid."""
        from hopsworks_common.client.exceptions import DataSourceException

        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            database="ORCL",
            user="scott",
            password="tiger",
        )
        with pytest.raises(DataSourceException):
            sc.spark_options()

    def test_get_tables_defaults_to_user_schema_for_oracle(self, mocker):
        """For Oracle, get_tables(None) defaults to the user's schema, not service_name."""
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
        )
        mock_get_tables = mocker.patch.object(
            sc._data_source_api, "_get_tables", return_value=[]
        )

        sc.get_tables()

        mock_get_tables.assert_called_once_with(sc, "SCOTT")

    def test_get_tables_oracle_explicit_database_overrides_default(self, mocker):
        """Explicit database to get_tables should not be replaced by the user."""
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            host="myhost",
            port=1521,
            database="ORCL",
            user="scott",
            password="tiger",
        )
        mock_get_tables = mocker.patch.object(
            sc._data_source_api, "_get_tables", return_value=[]
        )

        sc.get_tables("SH")

        mock_get_tables.assert_called_once_with(sc, "SH")

    def test_get_tables_oracle_without_user_raises(self):
        """Oracle without a configured user has no sensible default schema."""
        sc = storage_connector.SqlConnector(
            id=1,
            name="test",
            featurestore_id=1,
            database_type="ORACLE",
            database="mydb_high",
            wallet_path="/Projects/myproj/Resources/wallet.zip",
        )
        with pytest.raises(
            ValueError, match="schema/owner is required for Oracle connectors"
        ):
            sc.get_tables()


class TestSapHanaConnector:
    def test_from_response_json(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_sap_hana"]["response"]

        sc = storage_connector.StorageConnector.from_response_json(json)

        assert isinstance(sc, storage_connector.SapHanaConnector)
        assert sc.id == 1
        assert sc.name == "test_sap_hana"
        assert sc._featurestore_id == 67
        assert sc.description == "SAP HANA connector description"
        assert sc.host == "hana.example.com"
        assert sc.port == 39015
        assert sc.database == "HXE"
        assert sc.schema == "SYSTEM"
        assert sc.table == "TBL"
        assert sc.user == "SYSTEM"
        assert sc.password == "test_password"
        assert sc.application == "hopsworks"
        assert sc.options == {"fetchsize": "1000"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_sap_hana_basic_info"][
            "response"
        ]

        sc = storage_connector.StorageConnector.from_response_json(json)

        assert isinstance(sc, storage_connector.SapHanaConnector)
        assert sc.id == 1
        assert sc.name == "test_sap_hana"
        assert sc.host is None
        assert sc.port == storage_connector.SapHanaConnector.DEFAULT_PORT
        assert sc.database is None
        assert sc.schema is None
        assert sc.table is None
        assert sc.user is None
        assert sc.password is None
        assert sc.application is None
        assert sc.options == {}

    def test_spark_options_url_includes_database_and_schema(self):
        sc = storage_connector.SapHanaConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            host="hana.example.com",
            port=39015,
            database="HXE",
            schema="ANALYTICS",
            user="SYSTEM",
            password="pw",
            table="TBL",
        )

        opts = sc.spark_options()

        assert opts["driver"] == storage_connector.SapHanaConnector.DRIVER
        assert (
            opts["url"]
            == "jdbc:sap://hana.example.com:39015/?databaseName=HXE&currentschema=ANALYTICS"
        )
        assert opts["user"] == "SYSTEM"
        assert opts["password"] == "pw"
        assert opts["dbtable"] == "TBL"

    def test_spark_options_no_database_no_schema(self):
        sc = storage_connector.SapHanaConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            host="hana.example.com",
            user="SYSTEM",
            password="pw",
        )

        opts = sc.spark_options()

        assert opts["url"] == "jdbc:sap://hana.example.com:39015/"
        assert "dbtable" not in opts

    def test_default_port_applied(self):
        sc = storage_connector.SapHanaConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            host="hana.example.com",
        )
        assert sc.port == 39015

    def test_arguments_merged_into_spark_options(self):
        sc = storage_connector.SapHanaConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            host="hana.example.com",
            arguments=[{"name": "fetchsize", "value": "5000"}],
        )

        opts = sc.spark_options()

        assert opts["fetchsize"] == "5000"

    def test_read_query_overrides_dbtable(self, mocker):
        mocker.patch("hsfs.engine._get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine._read")

        sc = storage_connector.SapHanaConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            host="hana.example.com",
            user="SYSTEM",
            password="pw",
            table="TBL",
        )
        # SapHanaConnector.read() refetches before reading (so a connector
        # loaded as basic info refreshes its credentials); stub it out so
        # the test doesn't try to talk to a backend.
        mocker.patch.object(sc, "_refetch")
        query = "SELECT * FROM ANALYTICS.TBL"
        sc.read(query=query)

        called_options = mock_engine_read.call_args[0][2]
        assert called_options["query"] == query
        assert "dbtable" not in called_options

    def test_connector_options_minimal(self):
        sc = storage_connector.SapHanaConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            host="hana.example.com",
            user="SYSTEM",
            password="pw",
            database="HXE",
            schema="ANALYTICS",
        )

        props = sc.connector_options()

        assert props["address"] == "hana.example.com"
        assert props["port"] == 39015
        assert props["user"] == "SYSTEM"
        assert props["password"] == "pw"
        assert props["databaseName"] == "HXE"
        assert props["currentSchema"] == "ANALYTICS"

    def test_spark_options_without_host_raises(self):
        """Mirror the Oracle/JdbcConnector pattern.

        A connector loaded as basic info (no host yet) must fail fast on
        spark_options() instead of producing an unusable
        ``jdbc:sap://None:port/`` URL.
        """
        from hopsworks_common.client.exceptions import DataSourceException

        sc = storage_connector.SapHanaConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
        )
        with pytest.raises(DataSourceException, match="requires a host"):
            sc.spark_options()


class TestMongoDBConnector:
    """Cover the MongoDBConnector — from_response_json, URI builder, options."""

    def test_from_response_json(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_mongodb"]["response"]

        sc = storage_connector.StorageConnector.from_response_json(json)

        assert sc.id == 1
        assert sc.name == "test_mongodb"
        assert sc._featurestore_id == 67
        assert sc.description == "MongoDB connector description"
        assert sc.connection_string == "mongodb+srv://hopsworks.example.mongodb.net"
        assert sc.database == "sample_mflix"
        assert sc.collection == "comments"
        assert sc.user == "test_user"
        assert sc.password == "test_password"
        assert sc.auth_source == "admin"
        assert sc.auth_mechanism == "SCRAM-SHA-256"
        assert sc.options == {"maxPoolSize": "10"}

    def test_from_response_json_basic_info(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_mongodb_basic_info"][
            "response"
        ]

        sc = storage_connector.StorageConnector.from_response_json(json)

        assert sc.id == 1
        assert sc.name == "test_mongodb"
        assert sc._featurestore_id == 67
        assert sc.description is None
        assert sc.connection_string is None
        assert sc.database is None
        assert sc.collection is None
        assert sc.user is None
        assert sc.password is None
        assert sc.auth_source is None
        assert sc.auth_mechanism is None
        assert sc.options == {}

    def test_connection_uri_auth_params_without_user(self):
        # Reviewer note: authSource / authMechanism are valid independent
        # of userinfo (e.g. X.509 client-cert auth has no username).
        # Always append them when set.
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017",
            auth_mechanism="MONGODB-X509",
        )

        assert sc._connection_uri() == (
            "mongodb://host:27017/?authMechanism=MONGODB-X509"
        )

    def test_connection_uri_adds_path_separator_before_query(self):
        # Bug regression: previously `mongodb://host:27017?authSource=admin`
        # was produced (no `/` between host and `?`), which is invalid per
        # the MongoDB URI spec and rejected by pymongo's parser.
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017",
            user="alice",
            auth_source="admin",
        )

        uri = sc._connection_uri()
        # The path separator `/` precedes the query string `?`.
        assert "/?" in uri
        assert uri.endswith("/?authSource=admin")

    def test_connection_uri_preserves_existing_query_and_path(self):
        # If the stored connection string already carries query params or
        # a path, we preserve them and append our own params.
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017/dbX?retryWrites=true",
            user="alice",
            auth_source="admin",
        )

        uri = sc._connection_uri()
        assert uri == (
            "mongodb://alice@host:27017/dbX?retryWrites=true&authSource=admin"
        )

    def test_connection_uri_no_user_no_auth_returns_base(self):
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017",
        )
        # No userinfo, no extra params — but we still insert the path
        # separator so the URI is spec-valid.
        assert sc._connection_uri() == "mongodb://host:27017/"

    def test_connection_uri_missing_connection_string_raises(self):
        from hopsworks_common.client.exceptions import DataSourceException

        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
        )
        with pytest.raises(DataSourceException, match="requires a connection_string"):
            sc._connection_uri()

    def test_spark_options(self):
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb+srv://cluster.example.mongodb.net",
            database="sample_mflix",
            collection="comments",
            user="alice",
            password="secret",
            options={"maxPoolSize": "10"},
        )

        opts = sc.spark_options()
        # The connection URI is set under `connection.uri` (the
        # mongo-spark-connector option name); database + collection are
        # passed through as defaults; persisted options are merged.
        assert opts["connection.uri"].startswith("mongodb+srv://alice:secret@")
        assert opts["database"] == "sample_mflix"
        assert opts["collection"] == "comments"

    def test_get_tables_without_database_raises(self):
        """MongoDB get_tables() with no database configured and no argument raises."""
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017",
        )
        with pytest.raises(ValueError, match="Database name is required for MongoDB"):
            sc.get_tables()

    def test_get_tables_uses_connector_default_database(self, mocker):
        """MongoDB get_tables() defaults to the connector's database when none is passed."""
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017",
            database="sample_mflix",
        )
        mock_get_tables = mocker.patch.object(
            sc._data_source_api, "_get_tables", return_value=[]
        )

        sc.get_tables()

        mock_get_tables.assert_called_once_with(sc, "sample_mflix")

    def test_connector_options_forwards_self_options(self):
        # Reviewer note: connector_options() previously returned only
        # {host: uri}. self.options (operator-set pymongo kwargs) must be
        # forwarded so values like maxPoolSize / serverSelectionTimeoutMS
        # reach the driver.
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017",
            user="alice",
            options={
                "maxPoolSize": 10,
                "serverSelectionTimeoutMS": 5000,
                "host": "ignored",  # explicit-host key dropped to avoid clash
            },
        )

        opts = sc.connector_options()
        assert opts["host"].startswith("mongodb://alice@host:27017/")
        # `host` from self.options is dropped — already set from URI builder.
        assert opts["host"] != "ignored"
        assert opts["maxPoolSize"] == 10
        assert opts["serverSelectionTimeoutMS"] == 5000

    def test_connector_options_drops_none_values(self):
        sc = storage_connector.MongoDBConnector(
            id=1,
            name="m",
            featurestore_id=1,
            connection_string="mongodb://host:27017",
            options={"maxPoolSize": None, "tlsAllowInvalidCertificates": True},
        )
        opts = sc.connector_options()
        assert "maxPoolSize" not in opts


class TestStorageConnectorToDict:
    """Tests that to_dict() produces the camelCase payload the backend expects."""

    def test_hopsfs_to_dict(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_hopsfs"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        d = sc.to_dict()

        # Assert
        assert d["type"] == "featurestoreHopsfsConnectorDTO"
        assert d["storageConnectorType"] == "HOPSFS"
        assert d["name"] == "test_hopsfs"
        assert d["description"] == "HOPSFS connector description"
        assert d["hopsfsPath"] == "test_path"
        assert d["datasetName"] == "test_dataset_name"

    def test_s3_to_dict(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["storage_connector"]["get_s3"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        # Act
        d = sc.to_dict()

        # Assert
        assert d["type"] == "featurestoreS3ConnectorDTO"
        assert d["storageConnectorType"] == "S3"
        assert d["bucket"] == "test_bucket"
        assert d["accessKey"] == "test_access_key"
        assert d["secretKey"] == "test_secret_key"
        assert d["serverEncryptionAlgorithm"] == "test_server_encryption_algorithm"
        assert d["serverEncryptionKey"] == "test_server_encryption_key"
        assert d["sessionToken"] == "test_session_token"
        assert d["iamRole"] == "test_iam_role"
        assert d["arguments"] == [{"name": "test_name", "value": "test_value"}]

    def test_s3_to_dict_minimal(self):
        sc = storage_connector.S3Connector(
            id=None,
            name="my_s3",
            featurestore_id=67,
            bucket="my-bucket",
            region="eu-north-1",
        )

        d = sc.to_dict()

        assert d["type"] == "featurestoreS3ConnectorDTO"
        assert d["storageConnectorType"] == "S3"
        assert d["bucket"] == "my-bucket"
        assert d["region"] == "eu-north-1"
        assert d["arguments"] == []

    def test_redshift_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_redshift"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreRedshiftConnectorDTO"
        assert d["storageConnectorType"] == "REDSHIFT"
        assert d["clusterIdentifier"] == "test_cluster_identifier"
        assert d["databaseDriver"] == "test_database_driver"
        assert d["databaseEndpoint"] == "test_database_endpoint"
        assert d["databaseName"] == "test_database_name"
        assert d["databasePort"] == "test_database_port"
        assert d["tableName"] == "test_table_name"
        assert d["databaseUserName"] == "test_database_user_name"
        assert d["autoCreate"] == "test_auto_create"
        assert d["databasePassword"] == "test_database_password"
        assert d["databaseGroup"] == "test_database_group"
        assert d["iamRole"] == "test_iam_role"
        assert d["expiration"] == "test_expiration"
        # fixture passes arguments as a raw string (non-list); passed through as-is
        assert d["arguments"] == "test_arguments"

    def test_adls_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_adls"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreAdlsConnectorDTO"
        assert d["storageConnectorType"] == "ADLS"
        assert d["generation"] == "test_generation"
        assert d["directoryId"] == "test_directory_id"
        assert d["applicationId"] == "test_application_id"
        assert d["serviceCredential"] == "test_service_credential"
        assert d["accountName"] == "test_account_name"
        assert d["containerName"] == "test_container_name"
        assert d["sparkOptions"] == [{"name": "test_name", "value": "test_value"}]

    def test_snowflake_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_snowflake"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreSnowflakeConnectorDTO"
        assert d["storageConnectorType"] == "SNOWFLAKE"
        assert d["url"] == "test_url"
        assert d["user"] == "test_user"
        assert d["password"] == "test_password"
        assert d["token"] == "test_token"
        assert d["database"] == "test_database"
        assert d["schema"] == "test_schema"
        assert d["table"] == "test_table"
        assert d["warehouse"] == "test_warehouse"
        assert d["role"] == "test_role"
        assert d["application"] == "test_application"
        assert d["passphrase"] == "test_passphrase"
        assert d["sfOptions"] == [{"name": "test_name", "value": "test_value"}]

    def test_sap_hana_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_sap_hana"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featureStoreSapHanaConnectorDTO"
        assert d["storageConnectorType"] == "SAP_HANA"
        assert d["host"] == "hana.example.com"
        assert d["port"] == 39015
        assert d["database"] == "HXE"
        assert d["schema"] == "SYSTEM"
        assert d["table"] == "TBL"
        assert d["user"] == "SYSTEM"
        assert d["password"] == "test_password"
        assert d["application"] == "hopsworks"
        assert d["arguments"] == [{"name": "fetchsize", "value": "1000"}]

    def test_jdbc_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_jdbc"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreJdbcConnectorDTO"
        assert d["storageConnectorType"] == "JDBC"
        assert d["connectionString"] == "test_conn_string"
        assert len(d["arguments"]) == 4
        assert d["arguments"][0]["name"] == "sslTrustStore"

    def test_kafka_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featureStoreKafkaConnectorDTO"
        assert d["storageConnectorType"] == "KAFKA"
        assert d["bootstrapServers"] == "test_bootstrap_servers"
        assert d["securityProtocol"] == "test_security_protocol"
        assert d["sslTruststoreLocation"] == "test_ssl_truststore_location"
        assert d["sslTruststorePassword"] == "test_ssl_truststore_password"
        assert d["sslKeystoreLocation"] == "test_ssl_keystore_location"
        assert d["sslKeystorePassword"] == "test_ssl_keystore_password"
        assert d["sslKeyPassword"] == "test_ssl_key_password"
        assert (
            d["sslEndpointIdentificationAlgorithm"]
            == "test_ssl_endpoint_identification_algorithm"
        )
        assert d["options"] == [
            {"name": "test_option_name", "value": "test_option_value"}
        ]

    def test_kafka_external_flag_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["externalKafka"] is True

    def test_gcs_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_gcs"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featureStoreGcsConnectorDTO"
        assert d["storageConnectorType"] == "GCS"
        assert d["keyPath"] == "test_key_path"
        assert d["bucket"] == "test_bucket"
        assert d["algorithm"] == "test_algorithm"
        assert d["encryptionKey"] == "test_encryption_key"
        assert d["encryptionKeyHash"] == "test_encryption_key_hash"

    def test_bigquery_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_big_query_table"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreBigqueryConnectorDTO"
        assert d["storageConnectorType"] == "BIGQUERY"
        assert d["keyPath"] == "test_key_path"
        assert d["parentProject"] == "test_parent_project"
        assert d["dataset"] == "test_dataset"
        assert d["queryTable"] == "test_query_table"
        assert d["queryProject"] == "test_query_project"
        assert d["arguments"] == [{"name": "test_name", "value": "test_value"}]

    def test_sql_oracle_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_oracle"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreSqlConnectorDTO"
        assert d["storageConnectorType"] == "SQL"
        assert d["databaseType"] == "ORACLE"
        assert d["host"] == "test_host"
        assert d["port"] == 1521
        assert d["database"] == "test_database"
        assert d["user"] == "test_user"
        assert d["password"] == "test_password"
        assert d["walletPath"] == "/Projects/test_project/Resources/wallet.zip"
        assert d["walletPassword"] == "test_wallet_password"
        assert d["arguments"] == [{"name": "test_name", "value": "test_value"}]

    def test_mongodb_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_mongodb"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreMongoConnectorDTO"
        assert d["storageConnectorType"] == "MONGODB"
        assert d["connectionString"] == "mongodb+srv://hopsworks.example.mongodb.net"
        assert d["database"] == "sample_mflix"
        assert d["collection"] == "comments"
        assert d["user"] == "test_user"
        assert d["password"] == "test_password"
        assert d["authSource"] == "admin"
        assert d["authMechanism"] == "SCRAM-SHA-256"
        assert d["options"] == [{"name": "maxPoolSize", "value": "10"}]

    def test_unity_catalog_pat_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_unity_catalog"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["type"] == "featurestoreUnityCatalogConnectorDTO"
        assert d["storageConnectorType"] == "UNITY_CATALOG"
        assert d["workspaceUrl"] == "https://test.cloud.databricks.com"
        assert d["authMethod"] == "PAT"
        assert d["defaultCatalog"] == "test_catalog"
        assert d["awsRegion"] == "us-west-2"
        assert d["arguments"] == [{"name": "arg1", "value": "val1"}]
        # server-only boolean must not be sent back
        assert "hasAccessToken" not in d
        assert "hasClientSecret" not in d

    def test_unity_catalog_oauth_workspace_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"][
            "get_unity_catalog_oauth_workspace"
        ]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["authMethod"] == "OAUTH_M2M"
        assert d["oauthEndpoint"] == "WORKSPACE"
        assert d["clientId"] == "test-sp-client-id"
        assert "hasClientSecret" not in d

    def test_unity_catalog_oauth_account_to_dict(self, backend_fixtures):
        json = backend_fixtures["storage_connector"]["get_unity_catalog_oauth_account"][
            "response"
        ]
        sc = storage_connector.StorageConnector.from_response_json(json)

        d = sc.to_dict()

        assert d["authMethod"] == "OAUTH_M2M"
        assert d["oauthEndpoint"] == "ACCOUNT"
        assert d["accountId"] == "12345678-1234-1234-1234-1234567890ab"
        assert d["accountHost"] == "accounts.cloud.databricks.com"


class TestStorageConnectorSave:
    def test_save_calls_api_create(self, mocker):
        mock_create = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._create",
            return_value=storage_connector.S3Connector(
                id=42, name="my_s3", featurestore_id=67, bucket="my-bucket"
            ),
        )
        sc = storage_connector.S3Connector(
            id=None, name="my_s3", featurestore_id=67, bucket="my-bucket"
        )

        result = sc.save()

        mock_create.assert_called_once_with(sc)
        assert result.id == 42

    def test_update_calls_api_update(self, mocker):
        mock_update = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi._update",
            return_value=storage_connector.S3Connector(
                id=1, name="my_s3", featurestore_id=67, bucket="new-bucket"
            ),
        )
        sc = storage_connector.S3Connector(
            id=1, name="my_s3", featurestore_id=67, bucket="old-bucket"
        )

        result = sc.update()

        mock_update.assert_called_once_with(sc)
        assert result.bucket == "new-bucket"

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
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mocker.patch(
            "hsfs.storage_connector.StorageConnector.refetch", return_value=None
        )
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")

        # act
        sc = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )
        sc.read(data_format="csv")
        # assert
        assert "s3://test-bucket" in mock_engine_read.call_args[0][3]

    def test_get_path(self, mocker):
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        sc = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )

        # act
        result = sc._get_path("some/location")

        # assert
        assert result == "s3://test-bucket/some/location"

    def test_get_path_storage_connector_with_path(self, mocker):
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
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
        assert sc.access_token == "dapi-test-token"
        assert sc.default_catalog == "test_catalog"
        assert sc.aws_region == "us-west-2"
        assert sc.arguments == {"arg1": "val1"}

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

        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mocker.patch("hsfs.core.storage_connector_api.StorageConnectorApi.refetch")
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")

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
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")
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
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")

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
        mocker.patch("hopsworks_common.client.get_instance")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = (
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
        mocker.patch("hsfs.engine.get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client.get_instance")
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
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        sc = storage_connector.StorageConnector.from_response_json(json)

        mock_engine_get_instance.return_value.add_file.return_value = (
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
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"

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
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.5.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
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

        mock_engine_get_instance.return_value.add_file.assert_not_called()
        mock_engine_get_instance.return_value.add_file.assert_not_called()

    def test_spark_options_external(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
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
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_client_get_instance = mocker.patch("hopsworks_common.client.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]

        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.5.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
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

        mock_engine_get_instance.return_value.add_file.assert_any_call(
            "test_ssl_truststore_location", distribute=False
        )
        mock_engine_get_instance.return_value.add_file.assert_any_call(
            "test_ssl_keystore_location", distribute=False
        )

    def test_confluent_options(self, mocker, backend_fixtures):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        json = backend_fixtures["storage_connector"]["get_kafka_internal"]["response"]

        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        sc = storage_connector.StorageConnector.from_response_json(json)

        mock_client = mocker.patch("hopsworks_common.client.get_instance")
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
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = None
        mocker.patch("hopsworks_common.client.get_instance")
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
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.add_file.return_value = None
        mocker.patch("hopsworks_common.client.get_instance")
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
        engine.set_instance("python", python.Engine())
        json = backend_fixtures["storage_connector"]["get_gcs_basic_info"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        with pytest.raises(NotImplementedError):
            sc.read()

    def test_default_path(self, mocker):
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")
        # act
        sc = storage_connector.GcsConnector(
            id=1, name="test_connector", featurestore_id=1, bucket="test-bucket"
        )
        sc.read(data_format="csv")
        # assert
        assert mock_engine_read.call_args[0][3] == "gs://test-bucket"


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
        engine.set_instance("spark", spark.Engine())
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
        engine.set_instance("python", python.Engine())
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
        engine.set_instance("spark", spark.Engine())
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
        engine.set_instance("python", python.Engine())
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
        mock_read = mocker.patch("hsfs.engine.get_instance")
        mocker.patch.object(connector, "refetch")

        # Act
        connector.read(query="SELECT 1")

        # Assert
        call_options = mock_read.return_value.read.call_args[0][2]
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
        mock_engine = mocker.patch("hsfs.engine.get_instance")
        mocker.patch.object(connector, "refetch")

        # Act
        connector.read(query="SELECT 1 FROM DUAL")

        # Assert
        call_options = mock_engine.return_value.read.call_args[0][2]
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

        mock_engine = mocker.patch("hsfs.engine.get_instance")
        mock_engine.return_value.add_file.return_value = str(wallet_zip)
        mocker.patch.object(sc, "refetch")

        sc.read(query="SELECT 1")

        # Verify add_file was called with distribute=False
        mock_engine.return_value.add_file.assert_called_once_with(
            "/Projects/myproj/Resources/wallet.zip", distribute=False
        )

        # Verify the options passed to engine.read
        call_options = mock_engine.return_value.read.call_args[0][2]
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
            sc._data_source_api, "get_tables", return_value=[]
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
            sc._data_source_api, "get_tables", return_value=[]
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
        mocker.patch("hsfs.engine.get_instance", return_value=spark.Engine())
        mock_engine_read = mocker.patch("hsfs.engine.spark.Engine.read")

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
        mocker.patch.object(sc, "refetch")
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

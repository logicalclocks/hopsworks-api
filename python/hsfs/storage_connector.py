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

import base64
import logging
import os
import posixpath
import re
import time
import warnings
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, TypeVar

import humps
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from hopsworks_apigen import public
from hopsworks_common import client
from hopsworks_common.client.exceptions import DataSourceException
from hopsworks_common.core.constants import HAS_NUMPY, HAS_POLARS
from hopsworks_common.core.opensearch_api import OPENSEARCH_CONFIG
from hopsworks_common.core.rest_endpoint import RestEndpointConfig
from hsfs import engine
from hsfs.core import data_source as ds
from hsfs.core import data_source_api, storage_connector_api


if TYPE_CHECKING:
    from hopsworks_common.core.constants import HAS_NUMPY, HAS_POLARS

    if HAS_NUMPY:
        import numpy as np
    if HAS_POLARS:
        import polars as pl
    import pandas as pd
    from hsfs.core.data_source_data import DataSourceData
    from hsfs.core.explicit_provenance import Links
    from hsfs.feature_group import FeatureGroup
    from hsfs.training_dataset import TrainingDataset


_logger = logging.getLogger(__name__)


@public(order=1)
class StorageConnector(ABC):
    HOPSFS = "HOPSFS"
    S3 = "S3"
    JDBC = "JDBC"
    REDSHIFT = "REDSHIFT"
    ADLS = "ADLS"
    SNOWFLAKE = "SNOWFLAKE"
    KAFKA = "KAFKA"
    GCS = "GCS"
    BIGQUERY = "BIGQUERY"
    RDS = "RDS"
    OPENSEARCH = "OPENSEARCH"
    CRM = "CRM"
    REST = "REST"

    NOT_FOUND_ERROR_CODE = 270042

    def __init__(
        self,
        id: int | None,
        name: str,
        description: str | None,
        featurestore_id: int,
        **kwargs,
    ) -> None:
        self._id = id
        self._name = name
        self._description = description
        self._featurestore_id = featurestore_id

        self._storage_connector_api = storage_connector_api.StorageConnectorApi()
        self._data_source_api = data_source_api.DataSourceApi()

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> (
        StorageConnector
        | HopsFSConnector
        | S3Connector
        | RedshiftConnector
        | AdlsConnector
        | SnowflakeConnector
        | BigQueryConnector
        | RdsConnector
        | OpenSearchConnector
        | CRMAndAnalyticsConnector
        | RestConnector
    ):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type", None)
        for subcls in cls.__subclasses__():
            if subcls.type == json_decamelized["storage_connector_type"]:
                _ = json_decamelized.pop("storage_connector_type")
                return subcls(**json_decamelized)
        raise ValueError

    def update_from_response_json(
        self, json_dict: dict[str, Any]
    ) -> (
        StorageConnector
        | HopsFSConnector
        | S3Connector
        | RedshiftConnector
        | AdlsConnector
        | SnowflakeConnector
        | BigQueryConnector
        | RdsConnector
        | OpenSearchConnector
        | CRMAndAnalyticsConnector
        | RestConnector
    ):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type", None)
        if self.type == json_decamelized["storage_connector_type"]:
            _ = json_decamelized.pop("storage_connector_type")
            self.__init__(**json_decamelized)
        else:
            raise ValueError("Failed to update storage connector information.")
        return self

    def to_dict(self) -> dict[str, int | str | None]:
        return {
            "id": self._id,
            "name": self._name,
            "featurestoreId": self._featurestore_id,
            "storageConnectorType": self.type,
        }

    @public
    @property
    def type(self) -> str | None:
        """Type of the connector as string, e.g. "HOPFS, S3, ADLS, REDSHIFT, JDBC or SNOWFLAKE."""
        return self._type

    @public
    @property
    def id(self) -> int | None:
        """Id of the storage connector uniquely identifying it in the Feature store."""
        return self._id

    @public
    @property
    def name(self) -> str:
        """Name of the storage connector."""
        return self._name

    @public
    @property
    def description(self) -> str | None:
        """User provided description of the storage connector."""
        return self._description

    @public
    @abstractmethod
    def spark_options(self) -> dict[str, Any]:
        """Return prepared options to be passed to Spark, based on the additional arguments."""

    def prepare_spark(self, path: str | None = None) -> str | None:
        """Prepare Spark to use this Storage Connector.

        Parameters:
            path: Path to prepare for reading from cloud storage.
        """
        return path

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a query or a path into a dataframe using the storage connector.

        Note, paths are only supported for object stores like S3, HopsFS and ADLS, while queries are meant for JDBC or databases like Redshift and Snowflake.

        Parameters:
            query:
                By default, the storage connector will read the table configured together with the connector, if any.
                It's possible to overwrite this by passing a SQL query here.
            data_format: When reading from object stores such as S3, HopsFS and ADLS, specify the file format to be read, e.g., `csv`, `parquet`.
            options: Any additional key/value options to be passed to the connector.
            path:
                Path to be read from within the bucket of the storage connector.
                Not relevant for JDBC or database based connectors such as Snowflake, JDBC or Redshift.
            dataframe_type:
                The type of the returned dataframe.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            The read dataframe.
        """
        return engine.get_instance().read(
            self, data_format, options or {}, path, dataframe_type
        )

    def refetch(self) -> None:
        """Refetch storage connector."""
        self._storage_connector_api.refetch(self)

    def _get_path(self, sub_path: str) -> None:
        return None

    def connector_options(self) -> dict[str, Any]:
        """Return prepared options to be passed to an external connector library.

        Not implemented for this connector type.
        """
        return {}

    @public
    def get_feature_groups_provenance(self) -> Links | None:
        """Get the generated feature groups using this storage connector, based on explicit provenance.

        These feature groups can be accessible or inaccessible.

        Explicit provenance does not track deleted generated feature group links, so deleted will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        Returns:
            The feature groups generated using this storage connector or `None` if none were created.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        links = self._storage_connector_api.get_feature_groups_provenance(self)
        if not links.is_empty():
            return links
        return None

    @public
    def get_feature_groups(self) -> list[FeatureGroup]:
        """Get the feature groups using this storage connector, based on explicit rovenance.

        Only the accessible feature groups are returned.
        For more items use the base method, see get_feature_groups_provenance.

        Returns:
            List of feature groups.
        """
        feature_groups_provenance = self.get_feature_groups_provenance()

        if feature_groups_provenance and (
            feature_groups_provenance.inaccessible or feature_groups_provenance.deleted
        ):
            _logger.info(
                "There are deleted or inaccessible feature groups. For more details access `get_feature_groups_provenance`"
            )

        if feature_groups_provenance and feature_groups_provenance.accessible:
            return feature_groups_provenance.accessible
        return []

    @public
    def get_training_datasets_provenance(self) -> Links | None:
        """Get the generated training datasets using this storage connector, based on explicit provenance.

        These training datasets can be accessible or inaccessible. Explicit
        provenance does not track deleted generated training dataset links, so deleted
        will always be empty.
        For inaccessible training datasets, only a minimal information is returned.

        Returns:
            The training datasets generated using this storage connector or `None` if none were created.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        links = self._storage_connector_api.get_training_datasets_provenance(self)
        if not links.is_empty():
            return links
        return None

    @public
    def get_training_datasets(self) -> list[TrainingDataset]:
        """Get the training datasets using this storage connector, based on explicit provenance.

        Only the accessible training datasets are returned.
        For more items use the base method, [`get_training_datasets_provenance`][hsfs.core.data_source.DataSource.get_training_datasets_provenance].

        Returns:
            List of training datasets.
        """
        training_datasets_provenance = self.get_training_datasets_provenance()

        if training_datasets_provenance and (
            training_datasets_provenance.inaccessible
            or training_datasets_provenance.deleted
        ):
            _logger.info(
                "There are deleted or inaccessible training datasets. For more details access `get_training_datasets_provenance`"
            )

        if training_datasets_provenance and training_datasets_provenance.accessible:
            return training_datasets_provenance.accessible
        return []

    @public
    def get_databases(self) -> list[str]:
        """Retrieve the list of available databases.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            sc = fs.get_storage_connector("conn_name")

            databases = sc.get_databases()
            ```

        Returns:
            A list of database names available in the storage connector.
        """
        if self.type == StorageConnector.CRM or self.type == StorageConnector.REST:
            raise ValueError("This connector type does not support fetching databases.")
        return self._data_source_api.get_databases(self)

    @public
    def get_tables(self, database: str | None = None) -> list[ds.DataSource]:
        """Retrieve the list of tables from the specified database.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            sc = fs.get_storage_connector("conn_name")

            tables = sc.get_tables("database_name")
            ```

        Parameters:
            database:
                The name of the database to list tables from.
                If not provided, the default database is used.

        Returns:
            A list of DataSource objects representing the tables.
        """
        if self.type == StorageConnector.REST:
            raise ValueError("This connector type does not support fetching tables.")
        if not database and self.type != StorageConnector.CRM:
            if self.type == StorageConnector.REDSHIFT:
                database = self.database_name
            elif self.type == StorageConnector.SNOWFLAKE:
                database = self.database
            elif self.type == StorageConnector.BIGQUERY:
                database = self.query_project
            elif self.type == StorageConnector.RDS:
                database = self.database
            else:
                raise ValueError(
                    "Database name is required for this connector type. "
                    "Please provide a database name."
                )
        if self.type == StorageConnector.CRM:
            data: DataSourceData = self._data_source_api.get_crm_resources(self)
            return [
                ds.DataSource(table=resource)
                for resource in (data.supported_resources or [])
            ]

        return self._data_source_api.get_tables(self, database)

    @public
    def get_data(self, data_source: ds.DataSource) -> DataSourceData:
        """Retrieve the data from the data source.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            sc = fs.get_storage_connector("conn_name")

            tables = sc.get_tables("database_name")

            data = sc.get_data(tables[0])
            ```

        Parameters:
            data_source: The data source to retrieve data from.

        Returns:
            An object containing the data retrieved from the data source.
        """
        if self.type in [StorageConnector.REST, StorageConnector.CRM]:
            if not data_source.table:
                raise ValueError(
                    f"{self.type} data sources require a table name in data_source.table."
                )
            if self.type == StorageConnector.REST and data_source.rest_endpoint is None:
                data_source.rest_endpoint = RestEndpointConfig()
            return self._get_no_sql_data(data_source)
        return self._data_source_api.get_data(data_source)

    @public
    def get_metadata(self, data_source: ds.DataSource) -> dict:
        """Retrieve metadata information about the data source.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            sc = fs.get_storage_connector("conn_name")

            tables = sc.get_tables("database_name")

            metadata = sc.get_metadata(tables[0])
            ```

        Parameters:
            data_source: The data source to retrieve metadata from.

        Returns:
            A dictionary containing metadata about the data source.
        """
        if self.type in [StorageConnector.REST, StorageConnector.CRM]:
            raise ValueError("This connector type does not support fetching metadata.")
        return self._data_source_api.get_metadata(data_source)

    def _get_no_sql_data(self, data_source: ds.DataSource) -> DataSourceData:
        data: DataSourceData = self._data_source_api.get_no_sql_data(self, data_source)

        while data.schema_fetch_in_progress:
            time.sleep(3)
            data = self._data_source_api.get_no_sql_data(self, data_source)
            _logger.info("Schema fetch in progress...")

        if data.schema_fetch_failed:
            raise DataSourceException(f"Schema fetch failed:\n{data.schema_fetch_logs}")
        _logger.info("Schema fetch succeeded.")

        return data


@public
class HopsFSConnector(StorageConnector):
    type = StorageConnector.HOPSFS

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        hopsfs_path: str | None = None,
        dataset_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        # HopsFS
        self._hopsfs_path = hopsfs_path
        self._dataset_name = dataset_name

    def spark_options(self) -> dict[str, Any]:
        return {}

    def _get_path(self, sub_path: str) -> str:
        if sub_path:
            if self._hopsfs_path:
                return os.path.join(self._hopsfs_path, sub_path)
            return sub_path
        return self._hopsfs_path

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a path into a dataframe using the HopsFS storage connector.

        Parameters:
            query:
                Not used for HopsFS. Kept for interface consistency.
            data_format: The file format to be read, e.g., `csv`, `parquet`.
            options: Any additional key/value options to be passed to the connector.
            path:
                Path to be read within HopsFS. If the connector has a base path configured,
                relative paths will be resolved against it. Absolute `hopsfs://` paths are used as-is.
            dataframe_type:
                The type of the returned dataframe.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            The read dataframe.
        """
        if path is None:
            path = ""
        if not path.startswith("hopsfs://"):
            path = self._get_path(path)
        return engine.get_instance().read(
            self, data_format, options or {}, path, dataframe_type
        )


@public
class S3Connector(StorageConnector):
    type = StorageConnector.S3

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int | None,
        description: str | None = None,
        # members specific to type of connector
        access_key: str | None = None,
        secret_key: str | None = None,
        server_encryption_algorithm: str | None = None,
        server_encryption_key: str | None = None,
        bucket: str | None = None,
        path: str | None = None,
        region: str | None = None,
        session_token: str | None = None,
        iam_role: str | None = None,
        arguments: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        # S3
        self._access_key = access_key
        self._secret_key = secret_key
        self._server_encryption_algorithm = server_encryption_algorithm
        self._server_encryption_key = server_encryption_key
        self._bucket = bucket
        self._path = path
        self._region = region
        self._session_token = session_token
        self._iam_role = iam_role
        self._arguments = (
            {opt["name"]: opt["value"] for opt in arguments} if arguments else {}
        )

    @public
    @property
    def access_key(self) -> str | None:
        """Access key."""
        return self._access_key

    @public
    @property
    def secret_key(self) -> str | None:
        """Secret key."""
        return self._secret_key

    @public
    @property
    def server_encryption_algorithm(self) -> str | None:
        """Encryption algorithm if server-side S3 bucket encryption is enabled."""
        return self._server_encryption_algorithm

    @public
    @property
    def server_encryption_key(self) -> str | None:
        """Encryption key if server-side S3 bucket encryption is enabled."""
        return self._server_encryption_key

    @public
    @property
    def bucket(self) -> str | None:
        """Return the bucket for S3 connectors."""
        return self._bucket

    @public
    @property
    def region(self) -> str | None:
        """Return the region for S3 connectors."""
        return self._region

    @public
    @property
    def session_token(self) -> str | None:
        """Session token."""
        return self._session_token

    @public
    @property
    def iam_role(self) -> str | None:
        """IAM role."""
        return self._iam_role

    @public
    @property
    def path(self) -> str | None:
        """If the connector refers to a path (e.g. S3) - return the path of the connector."""
        return posixpath.join(
            "s3://" + self._bucket, *os.path.split(self._path if self._path else "")
        )

    @public
    @property
    def arguments(self) -> dict[str, Any] | None:
        """Additional spark options for the S3 connector, passed as a dictionary.

        These are set using the `Spark Options` field in the UI when creating the connector.
        Example: `{"fs.s3a.endpoint": "s3.eu-west-1.amazonaws.com", "fs.s3a.path.style.access": "true"}`.
        """
        return self._arguments

    def spark_options(self) -> dict[str, str]:
        return self._arguments

    @public
    def prepare_spark(self, path: str | None = None) -> str | None:
        """Prepare Spark to use this Storage Connector.

        ```python
        conn.prepare_spark()

        spark.read.format("json").load("s3a://[bucket]/path")

        # or
        spark.read.format("json").load(conn.prepare_spark("s3a://[bucket]/path"))
        ```

        Parameters:
            path: Path to prepare for reading from cloud storage.
        """
        self.refetch()
        return engine.get_instance().setup_storage_connector(self, path)

    @public
    def connector_options(self) -> dict[str, Any]:
        """Return options to be passed to an external S3 connector library."""
        self.refetch()
        options = {
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "session_token": self.session_token,
            "region": self.region,
        }
        if not self.arguments:
            return options
        if self.arguments.get("fs.s3a.endpoint"):
            options["endpoint"] = self.arguments.get("fs.s3a.endpoint")
        if self.arguments.get("fs.s3a.connection.ssl.enabled"):
            # use_ssl is used by s3 secrets in duckdb
            # where as fs.s3a.connection.ssl.enabled is used by spark s3a connector
            # hadoop : https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/connecting.html#Low-level_Network.2FHttp_Options
            # duckdb : https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
            options["use_ssl"] = self.arguments.get("fs.s3a.connection.ssl.enabled")
        if self.arguments.get("fs.s3a.path.style.access"):
            # url_style is used by duckdb s3 connector
            # where as fs.s3a.path.style.access is used by spark s3a connector
            # hadoop: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/connecting.html#Third_party_stores
            # duckdb: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
            options["url_style"] = (
                "path" if self.arguments.get("fs.s3a.path.style.access") else "vhost"
            )
        return options

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a query or a path into a dataframe using the storage connector.

        Note, paths are only supported for object stores like S3, HopsFS and ADLS, while queries are meant for JDBC or databases like Redshift and Snowflake.

        Parameters:
            query: Not relevant for S3 connectors.
            data_format: The file format of the files to be read, e.g. `csv`, `parquet`.
            options: Any additional key/value options to be passed to the S3 connector.
            path: Path within the bucket to be read.
            dataframe_type:
                The type of the returned dataframe.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            `DataFrame`.
        """
        if path is None:
            path = ""
        self.refetch()
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if not path.startswith(("s3://", "s3a://")):
            path = self._get_path(path)
            print(
                f"Prepending default bucket specified on connector, final path: {path}"
            )

        return engine.get_instance().read(
            self, data_format, options, path, dataframe_type
        )

    def _get_path(self, sub_path: str) -> str:
        return posixpath.join(self.path, *os.path.split(sub_path))


@public
class RedshiftConnector(StorageConnector):
    type = StorageConnector.REDSHIFT
    JDBC_FORMAT = "jdbc"

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        cluster_identifier: str | None = None,
        database_driver: str | None = None,
        database_endpoint: str | None = None,
        database_name: str | None = None,
        database_port: int | str | None = None,
        table_name: str | None = None,
        database_user_name: str | None = None,
        auto_create: bool | None = None,
        database_password: str | None = None,
        database_group: str | None = None,
        iam_role: Any | None = None,
        arguments: dict[str, Any] | None = None,
        expiration: int | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        # Redshift
        self._cluster_identifier = cluster_identifier
        self._database_driver = database_driver
        self._database_endpoint = database_endpoint
        self._database_name = database_name
        self._database_port = database_port
        self._table_name = table_name
        self._database_user_name = database_user_name
        self._auto_create = auto_create
        self._database_password = database_password
        self._database_group = database_group
        self._iam_role = iam_role
        self._arguments = (
            {arg["name"]: arg.get("value", None) for arg in arguments}
            if isinstance(arguments, list)
            else arguments
        )
        self._expiration = expiration

    @public
    @property
    def cluster_identifier(self) -> str | None:
        """Cluster identifier for redshift cluster."""
        return self._cluster_identifier

    @public
    @property
    def database_driver(self) -> str | None:
        """Database endpoint for redshift cluster."""
        return self._database_driver

    @public
    @property
    def database_endpoint(self) -> str | None:
        """Database endpoint for redshift cluster."""
        return self._database_endpoint

    @public
    @property
    def database_name(self) -> str | None:
        """Database name for redshift cluster."""
        return self._database_name

    @public
    @property
    def database_port(self) -> int | str | None:
        """Database port for redshift cluster."""
        return self._database_port

    @public
    @property
    def table_name(self) -> str | None:
        """Table name for redshift cluster."""
        return self._table_name

    @public
    @property
    def database_user_name(self) -> str | None:
        """Database username for redshift cluster."""
        return self._database_user_name

    @public
    @property
    def auto_create(self) -> bool | None:
        """Database username for redshift cluster."""
        return self._auto_create

    @public
    @property
    def database_group(self) -> str | None:
        """Database username for redshift cluster."""
        return self._database_group

    @public
    @property
    def database_password(self) -> str | None:
        """Database password for redshift cluster."""
        return self._database_password

    @public
    @property
    def iam_role(self) -> Any | None:
        """IAM role."""
        return self._iam_role

    @public
    @property
    def expiration(self) -> int | str | None:
        """Cluster temporary credential expiration time."""
        return self._expiration

    @public
    @property
    def arguments(self) -> str | None:
        """Additional JDBC, REDSHIFT, or Snowflake arguments."""
        if isinstance(self._arguments, dict):
            return ",".join(
                [k + ("" if v is None else "=" + v) for k, v in self._arguments.items()]
            )
        return self._arguments

    @public
    def connector_options(self) -> dict[str, Any]:
        """Return options to be passed to an external Redshift connector library."""
        props = {
            "host": self._cluster_identifier + "." + self._database_endpoint,
            "port": self._database_port,
            "database": self._database_name,
        }
        if self._database_user_name:
            props["user"] = self._database_user_name
        if self._database_password:
            props["password"] = self._database_password
        if self._iam_role:
            props["iam_role"] = self._iam_role
            props["iam"] = "True"
        return props

    def spark_options(self) -> dict[str, Any]:
        connstr = (
            "jdbc:redshift://"
            + self._cluster_identifier
            + "."
            + self._database_endpoint
            + ":"
            + str(self._database_port)
            + "/"
            + self._database_name
        )
        if isinstance(self.arguments, str):
            connstr = connstr + "?" + self.arguments
        props = {
            "url": connstr,
            "driver": self._database_driver,
            "user": self._database_user_name,
            "password": self._database_password,
        }
        if self._table_name is not None:
            props["dbtable"] = self._table_name

        return props

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a table or query into a dataframe using the storage connector.

        Parameters:
            query: By default, the storage connector will read the table configured together
                with the connector, if any. It's possible to overwrite this by passing a SQL
                query here.
            data_format: Not relevant for JDBC based connectors such as Redshift.
            options: Any additional key/value options to be passed to the JDBC connector.
            path: Not relevant for JDBC based connectors such as Redshift.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            `DataFrame`.
        """
        # refetch to update temporary credentials
        self._storage_connector_api.refetch(self)
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            options["query"] = query
            # if table also specified we override to use query
            options.pop("dbtable", None)

        return engine.get_instance().read(
            self, self.JDBC_FORMAT, options, None, dataframe_type
        )

    @public
    def refetch(self) -> None:
        """Refetch storage connector in order to retrieve updated temporary credentials."""
        self._storage_connector_api.refetch(self)


@public
class AdlsConnector(StorageConnector):
    type = StorageConnector.ADLS

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        generation: str | None = None,
        directory_id: str | None = None,
        application_id: str | None = None,
        service_credential: str | None = None,
        account_name: str | None = None,
        container_name: str | None = None,
        spark_options: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        # ADL
        self._generation = generation
        self._directory_id = directory_id
        self._application_id = application_id
        self._account_name = account_name
        self._service_credential = service_credential
        self._container_name = container_name

        self._spark_options = (
            {opt["name"]: opt["value"] for opt in spark_options}
            if spark_options
            else {}
        )

    @public
    @property
    def generation(self) -> str | None:
        """Generation of the ADLS storage connector."""
        return self._generation

    @public
    @property
    def directory_id(self) -> str | None:
        """Directory ID of the ADLS storage connector."""
        return self._directory_id

    @public
    @property
    def application_id(self) -> str | None:
        """Application ID of the ADLS storage connector."""
        return self._application_id

    @public
    @property
    def account_name(self) -> str | None:
        """Account name of the ADLS storage connector."""
        return self._account_name

    @public
    @property
    def container_name(self) -> str | None:
        """Container name of the ADLS storage connector."""
        return self._container_name

    @public
    @property
    def service_credential(self) -> str | None:
        """Service credential of the ADLS storage connector."""
        return self._service_credential

    @public
    @property
    def path(self) -> str | None:
        """If the connector refers to a path (e.g. ADLS) - return the path of the connector."""
        if self.generation == 2:
            return f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net"
        return f"adl://{self.account_name}.azuredatalakestore.net"

    def spark_options(self) -> dict[str, Any]:
        return self._spark_options

    @public
    def prepare_spark(self, path: str | None = None) -> str | None:
        """Prepare Spark to use this Storage Connector.

        ```python
        conn.prepare_spark()

        spark.read.format("json").load("abfss://[container-name]@[account_name].dfs.core.windows.net/[path]")

        # or
        spark.read.format("json").load(conn.prepare_spark("abfss://[container-name]@[account_name].dfs.core.windows.net/[path]"))
        ```

        Parameters:
            path: Path to prepare for reading from cloud storage.
        """
        return engine.get_instance().setup_storage_connector(self, path)

    def _get_path(self, sub_path: str) -> str:
        return os.path.join(self.path, sub_path)

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a path into a dataframe using the storage connector.

        Parameters:
            query: Not relevant for ADLS connectors.
            data_format: The file format of the files to be read, e.g. `csv`, `parquet`.
            options: Any additional key/value options to be passed to the ADLS connector.
            path:
                Path within the bucket to be read.
                For example, path=`path` will read directly from the container specified on connector by constructing the URI as 'abfss://[container-name]@[account_name].dfs.core.windows.net/[path]'.
                If no path is specified default container path will be used from connector.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            `DataFrame`.
        """
        if path is None:
            path = ""
        path = path.strip()
        if not path.startswith("abfss://") or path.startswith("adl://"):
            path = self._get_path(path)
            print(f"Using default container specified on connector, final path: {path}")

        return engine.get_instance().read(
            self, data_format, options or {}, path, dataframe_type
        )


@public
class SnowflakeConnector(StorageConnector):
    type = StorageConnector.SNOWFLAKE
    SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int | None,
        description: str | None = None,
        # members specific to type of connector
        database: str | None = None,
        password: str | None = None,
        token: str | None = None,
        role: Any | None = None,
        schema: str | None = None,
        table: str | None = None,
        url: str | None = None,
        user: Any | None = None,
        warehouse: str | None = None,
        application: Any | None = None,
        sf_options: dict[str, Any] | None = None,
        private_key: str | None = None,
        passphrase: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        # SNOWFLAKE
        self._url = url
        self._warehouse = warehouse
        self._database = database
        self._user = user
        self._password = password
        self._token = token
        self._schema = schema
        self._table = table
        self._role = role
        self._application = application
        self._private_key = private_key
        self._passphrase = passphrase

        self._options = (
            {opt["name"]: opt["value"] for opt in sf_options} if sf_options else {}
        )

    @public
    @property
    def url(self) -> str | None:
        """URL of the Snowflake storage connector."""
        return self._url

    @public
    @property
    def warehouse(self) -> str | None:
        """Warehouse of the Snowflake storage connector."""
        return self._warehouse

    @public
    @property
    def database(self) -> str | None:
        """Database of the Snowflake storage connector."""
        return self._database

    @public
    @property
    def user(self) -> Any | None:
        """User of the Snowflake storage connector."""
        return self._user

    @public
    @property
    def password(self) -> str | None:
        """Password of the Snowflake storage connector."""
        return self._password

    @public
    @property
    def token(self) -> str | None:
        """OAuth token of the Snowflake storage connector."""
        return self._token

    @public
    @property
    def schema(self) -> str | None:
        """Schema of the Snowflake storage connector."""
        return self._schema

    @public
    @property
    def table(self) -> str | None:
        """Table of the Snowflake storage connector."""
        return self._table

    @public
    @property
    def role(self) -> Any | None:
        """Role of the Snowflake storage connector."""
        return self._role

    @public
    @property
    def account(self) -> str | None:
        """Account of the Snowflake storage connector."""
        return self._url.replace("https://", "").replace(".snowflakecomputing.com", "")

    @public
    @property
    def application(self) -> Any:
        """Application of the Snowflake storage connector."""
        return self._application

    @public
    @property
    def options(self) -> dict[str, Any] | None:
        """Additional options for the Snowflake storage connector."""
        return self._options

    @public
    @property
    def private_key(self) -> str | None:
        """Path to the private key file for key pair authentication."""
        return self._private_key

    @public
    @property
    def passphrase(self) -> str | None:
        """Passphrase for the private key file."""
        return self._passphrase

    @public
    def snowflake_connector_options(self) -> dict[str, Any] | None:
        """Alias for `connector_options`."""
        return self.connector_options()

    @public
    def connector_options(self) -> dict[str, Any] | None:
        """Prepare a Python dictionary with the needed arguments for you to connect to a Snowflake database.

        It is useful for the `snowflake.connector` Python library.

        ```python
        import snowflake.connector

        sc = fs.get_storage_connector("snowflake_conn")
        ctx = snowflake.connector.connect(**sc.connector_options())
        ```
        """
        props = {
            "user": self._user,
            "account": self.account,
            "database": self._database + "/" + self._schema,
        }
        if self._password:
            props["password"] = self._password
        else:
            props["authenticator"] = "oauth"
            props["token"] = self._token
        if self._warehouse:
            props["warehouse"] = self._warehouse
        if self._application:
            props["application"] = self._application
        return props

    def spark_options(self) -> dict[str, Any]:
        props = self._options
        props["sfURL"] = self._url
        props["sfSchema"] = self._schema
        props["sfDatabase"] = self._database
        props["sfUser"] = self._user
        if self._password:
            props["sfPassword"] = self._password
        elif self._token:
            props["sfAuthenticator"] = "oauth"
            props["sfToken"] = self._token
        elif self._private_key:
            private_key_content = self._read_private_key()
            if private_key_content:
                props["pem_private_key"] = private_key_content

        if self._warehouse:
            props["sfWarehouse"] = self._warehouse
        if self._application:
            props["application"] = self._application
        if self._role:
            props["sfRole"] = self._role
        if self._table:
            props["dbtable"] = self._table

        return props

    def _read_private_key(self) -> str | None:
        """Reads the private key from the specified key path."""
        p_key = serialization.load_pem_private_key(
            self._private_key.encode(),
            password=self._passphrase.encode() if self._passphrase else None,
            backend=default_backend(),
        )

        private_key_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        private_key_content = private_key_bytes.decode("UTF-8")
        # remove both regular and encrypted PEM headers, e.g.
        # -----BEGIN PRIVATE KEY----- and -----BEGIN ENCRYPTED PRIVATE KEY-----
        return re.sub(
            r"-*\s*(BEGIN|END)(?: ENCRYPTED)? PRIVATE KEY-*\r?\n",
            "",
            private_key_content,
        ).replace("\n", "")

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a table or query into a dataframe using the storage connector.

        Parameters:
            query: By default, the storage connector will read the table configured together
                with the connector, if any. It's possible to overwrite this by passing a SQL
                query here.
            data_format: Not relevant for Snowflake connectors.
            options: Any additional key/value options to be passed to the engine.
            path: Not relevant for Snowflake connectors.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            `DataFrame`.
        """
        # validate engine supports connector type
        if not engine.get_instance().is_connector_type_supported(self.type):
            raise NotImplementedError(
                "Snowflake connector not yet supported for engine: " + engine.get_type()
            )

        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            options["query"] = query
            # if table also specified we override to use query
            options.pop("dbtable", None)

        return engine.get_instance().read(
            self, self.SNOWFLAKE_FORMAT, options, None, dataframe_type
        )

    @public
    def prepare_spark(self, path=None):
        return engine.get_instance().setup_storage_connector(self, path)


@public
class JdbcConnector(StorageConnector):
    type = StorageConnector.JDBC
    JDBC_FORMAT = "jdbc"

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        connection_string: str | None = None,
        arguments: dict[str, Any] = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        # JDBC
        self._connection_string = connection_string
        self._arguments = arguments

    @public
    @property
    def connection_string(self) -> str | None:
        """JDBC connection string."""
        return self._connection_string

    @public
    @property
    def arguments(self) -> dict[str, Any] | None:
        """Additional JDBC arguments.

        When running hsfs with PySpark/Spark in Hopsworks, the driver is automatically provided in the classpath but you need to set the `driver` argument to `com.mysql.cj.jdbc.Driver` when creating the Storage Connector.
        """
        return self._arguments

    def spark_options(self) -> dict[str, Any]:
        options = (
            {arg.get("name"): arg.get("value") for arg in self._arguments}
            if self._arguments
            else {}
        )

        options["url"] = self._connection_string

        return options

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a query into a dataframe using the storage connector.

        Parameters:
            query: A SQL query to be read.
            data_format: Not relevant for JDBC based connectors.
            options: Any additional key/value options to be passed to the JDBC connector.
            path: Not relevant for JDBC based connectors.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            `DataFrame`.
        """
        self.refetch()
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            options["query"] = query

        return engine.get_instance().read(
            self, self.JDBC_FORMAT, options, None, dataframe_type
        )


@public
class KafkaConnector(StorageConnector):
    type = StorageConnector.KAFKA
    SPARK_FORMAT = "kafka"

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        bootstrap_servers: list[str] | None = None,
        security_protocol: str | None = None,
        ssl_truststore_location: str | None = None,
        ssl_truststore_password: str | None = None,
        ssl_keystore_location: str | None = None,
        ssl_keystore_password: str | None = None,
        ssl_key_password: str | None = None,
        ssl_endpoint_identification_algorithm: str | None = None,
        options: dict[str, Any] | None = None,
        external_kafka: bool | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        # KAFKA
        self._bootstrap_servers = bootstrap_servers
        self._security_protocol = security_protocol
        self._ssl_truststore_location = ssl_truststore_location
        self._ssl_truststore_password = ssl_truststore_password
        self._ssl_keystore_location = ssl_keystore_location
        self._ssl_keystore_password = ssl_keystore_password
        self._ssl_key_password = ssl_key_password
        self._ssl_endpoint_identification_algorithm = (
            ssl_endpoint_identification_algorithm
        )
        self._options = (
            {option["name"]: option["value"] for option in options}
            if options is not None
            else {}
        )
        self._external_kafka = external_kafka
        self._pem_files_created = False

    @public
    @property
    def bootstrap_servers(self) -> list[str] | None:
        """Bootstrap servers string."""
        return self._bootstrap_servers

    @public
    @property
    def security_protocol(self) -> str | None:
        """Bootstrap servers string."""
        return self._security_protocol

    @public
    @property
    def ssl_truststore_location(self) -> str | None:
        """Bootstrap servers string."""
        return self._ssl_truststore_location

    @public
    @property
    def ssl_keystore_location(self) -> str | None:
        """Bootstrap servers string."""
        return self._ssl_keystore_location

    @public
    @property
    def ssl_endpoint_identification_algorithm(self) -> str | None:
        """Bootstrap servers string."""
        return self._ssl_endpoint_identification_algorithm

    @public
    @property
    def options(self) -> dict[str, Any]:
        """Bootstrap servers string."""
        return self._options

    @public
    def create_pem_files(self, kafka_options: dict[str, Any]) -> None:
        """Create PEM (Privacy Enhanced Mail) files for Kafka SSL authentication.

        This method writes the necessary PEM files for SSL authentication with Kafka,
        using the provided keystore and truststore locations and passwords. The generated
        file paths are stored as the following instance variables:

            - self.ca_chain_path: Path to the generated CA chain PEM file.
            - self.client_cert_path: Path to the generated client certificate PEM file.
            - self.client_key_path: Path to the generated client key PEM file.

        These files are used for configuring secure Kafka connections (e.g., with Spark or confluent_kafka).
        The method is idempotent and will only create the files once per connector instance.
        """
        if not self._pem_files_created:
            (
                self.ca_chain_path,
                self.client_cert_path,
                self.client_key_path,
            ) = client.get_instance()._write_pem(
                kafka_options["ssl.keystore.location"],
                kafka_options["ssl.keystore.password"],
                kafka_options["ssl.truststore.location"],
                kafka_options["ssl.truststore.password"],
                f"kafka_sc_{client.get_instance()._project_id}_{self._id}",
            )
            self._pem_files_created = True

    @public
    def kafka_options(self, distribute=True) -> dict[str, Any]:
        """Return prepared options to be passed to kafka, based on the additional arguments.

        See <https://kafka.apache.org/documentation/>.
        """
        config = {}

        # set kafka storage connector options
        config.update(self.options)

        # set connection properties
        config.update(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "security.protocol": self.security_protocol,
            }
        )

        # set ssl
        config["ssl.endpoint.identification.algorithm"] = (
            self._ssl_endpoint_identification_algorithm
        )

        # Here we cannot use `not self._external_kafka` as for normal kafka connectors
        # this option is not set and so the `not self._external_kafka` would return true
        # overwriting the user specified certificates
        if self._external_kafka is False:
            ssl_truststore_location = client.get_instance()._get_jks_trust_store_path()
            ssl_truststore_password = client.get_instance()._cert_key
            ssl_keystore_location = client.get_instance()._get_jks_key_store_path()
            ssl_keystore_password = client.get_instance()._cert_key
            ssl_key_password = client.get_instance()._cert_key
        else:
            ssl_truststore_location = engine.get_instance().add_file(
                self._ssl_truststore_location, distribute=distribute
            )
            ssl_truststore_password = self._ssl_truststore_password
            ssl_keystore_location = engine.get_instance().add_file(
                self._ssl_keystore_location, distribute=distribute
            )
            ssl_keystore_password = self._ssl_keystore_password
            ssl_key_password = self._ssl_key_password

        if ssl_truststore_location is not None:
            config["ssl.truststore.location"] = ssl_truststore_location
        if ssl_truststore_password is not None:
            config["ssl.truststore.password"] = ssl_truststore_password
        if ssl_keystore_location is not None:
            config["ssl.keystore.location"] = ssl_keystore_location
        if ssl_keystore_password is not None:
            config["ssl.keystore.password"] = ssl_keystore_password
        if ssl_key_password is not None:
            config["ssl.key.password"] = ssl_key_password

        if self._external_kafka:
            warnings.warn(
                "Getting connection details to externally managed Kafka cluster. "
                "Make sure that the topic being used exists.",
                stacklevel=1,
            )

        return config

    @public
    def confluent_options(self) -> dict[str, Any]:
        """Return prepared options to be passed to confluent_kafka, based on the provided apache spark configuration.

        Right now only producer values with Importance >= medium are implemented.

        See <https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html>.
        """
        pem_files_assigned = False
        config = {}
        kafka_options = self.kafka_options()
        for key, value in kafka_options.items():
            if (
                key
                in [
                    "ssl.truststore.location",
                    "ssl.truststore.password",
                    "ssl.keystore.location",
                    "ssl.keystore.password",
                ]
                and not pem_files_assigned
            ):
                self.create_pem_files(kafka_options)
                config["ssl.ca.location"] = (
                    kafka_options.get("ssl.ca.location") or self.ca_chain_path
                )
                config["ssl.certificate.location"] = self.client_cert_path
                config["ssl.key.location"] = self.client_key_path
                pem_files_assigned = True
            elif key == "sasl.jaas.config":
                groups = re.search(
                    "(.+?) .*username=[\"'](.+?)[\"'] .*password=[\"'](.+?)[\"']",
                    value,
                )
                if "sasl.mechanisms" not in config:
                    mechanism = groups.group(1)
                    mechanism_value = None
                    if (
                        mechanism
                        == "org.apache.kafka.common.security.plain.PlainLoginModule"
                    ):
                        mechanism_value = "PLAIN"
                    elif (
                        mechanism
                        == "org.apache.kafka.common.security.scram.ScramLoginModule"
                    ):
                        mechanism_value = "SCRAM-SHA-256"  # could also be SCRAM-SHA-512
                    elif (
                        mechanism
                        == "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
                    ):
                        mechanism_value = "OAUTHBEARER"
                config["sasl.mechanisms"] = mechanism_value
                config["sasl.username"] = groups.group(2)
                config["sasl.password"] = groups.group(3)
            elif key == "ssl.endpoint.identification.algorithm":
                config[key] = "none" if value == "" else value
            elif key == "queued.max.requests":
                config["queue.buffering.max.messages"] = value
            elif key == "queued.max.request.bytes":
                config["queue.buffering.max.kbytes"] = value
            elif key in [
                "bootstrap.servers",
                "security.protocol",
                "compression.type",
                "sasl.mechanism",
                "request.timeout.ms",
                "group.id",
                "transactional.id",
                "transaction.timeout.ms",
                "enable.idempotence",
                "message.max.bytes",
                "linger.ms",
                "retries",
                "retry.backoff.ms",
                "acks",
                "socket.connection.setup.timeout.ms",
                "connections.max.idle.ms",
                "reconnect.backoff.ms",
                "reconnect.backoff.max.ms",
                "delivery.timeout.ms",
            ]:
                # same between config
                config[key] = value
            else:
                # ignored values (if not specified then configuration is ignored)
                continue

        return config

    def _read_pem(self, file_name):
        with open(file_name) as file:
            return file.read()

    @public
    def spark_options(self) -> dict[str, Any]:
        """Return prepared options to be passed to Spark, based on the additional arguments.

        This is done by just adding 'kafka.' prefix to kafka_options.

        See <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations>.
        """
        from packaging import version

        kafka_client_supports_pem = version.parse(
            engine.get_instance().get_spark_version()
        ) >= version.parse("3.2.0")

        pem_files_assigned = False
        spark_config = {}
        # Only distribute the files if the Kafka client does not support being configured with PEM content
        kafka_options = self.kafka_options(distribute=not kafka_client_supports_pem)
        for key, value in kafka_options.items():
            if (
                key
                in [
                    "ssl.truststore.location",
                    "ssl.truststore.password",
                    "ssl.keystore.location",
                    "ssl.keystore.password",
                    "ssl.key.password",
                ]
                and kafka_client_supports_pem
            ):
                if not pem_files_assigned:
                    # We can only use this in the newer version of Spark which depend on Kafka > 2.7.0
                    # Kafka 2.7.0 adds support for providing the SSL credentials as PEM objects.
                    self.create_pem_files(kafka_options)
                    spark_config["kafka.ssl.truststore.certificates"] = self._read_pem(
                        self.ca_chain_path
                    )
                    spark_config["kafka.ssl.keystore.certificate.chain"] = (
                        self._read_pem(self.client_cert_path)
                    )
                    spark_config["kafka.ssl.keystore.key"] = self._read_pem(
                        self.client_key_path
                    )
                    spark_config["kafka.ssl.truststore.type"] = "PEM"
                    spark_config["kafka.ssl.keystore.type"] = "PEM"
                    pem_files_assigned = True
            else:
                spark_config[f"{KafkaConnector.SPARK_FORMAT}.{key}"] = value

        return spark_config

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> None:
        """\

        Failure:
            This operation is not supported.
            Use [`read_stream`][hsfs.storage_connector.KafkaConnector.read_stream] instead to read a Kafka stream into a streaming Spark Dataframe.

        Raises:
            NotImplementedError: Always, since this operation is not supported.
        """
        raise NotImplementedError(
            "Reading a Kafka Stream into a static Spark Dataframe is not supported."
        )

    @public
    def read_stream(
        self,
        topic: str,
        topic_pattern: bool = False,
        message_format: str = "avro",
        schema: str | None = None,
        options: dict[str, Any] | None = None,
        include_metadata: bool = False,
    ) -> TypeVar("pyspark.sql.DataFrame") | TypeVar(
        "pyspark.sql.streaming.StreamingQuery"
    ):
        """Reads a Kafka stream from a topic or multiple topics into a Dataframe.

        Warning: Engine Support
            **Spark only**

            Reading from data streams using Pandas/Python as engine is currently not supported.
            Python/Pandas has no notion of streaming.

        Parameters:
            topic: Name or pattern of the topic(s) to subscribe to.
            topic_pattern: Flag to indicate if `topic` string is a pattern.
            message_format: The format of the messages to use for decoding.
                Can be `"avro"` or `"json"`.
            schema: Optional schema, to use for decoding, can be an Avro schema string for
                `"avro"` message format, or for JSON encoding a Spark StructType schema,
                or a DDL formatted string.
            options: Additional options as key/value string pairs to be passed to Spark.
                Defaults to `{}`.
            include_metadata: Indicate whether to return additional metadata fields from
                messages in the stream. Otherwise, only the decoded value fields are
                returned.

        Raises:
            ValueError: Malformed arguments.

        Returns:
            A Spark streaming dataframe.
        """
        if message_format.lower() not in ["avro", "json", None]:
            raise ValueError("Can only read JSON and AVRO encoded records from Kafka.")
        if options is None:
            options = {}
        if topic_pattern is True:
            options["subscribePattern"] = topic
        else:
            options["subscribe"] = topic

        return engine.get_instance().read_stream(
            self,
            message_format.lower(),
            schema,
            options,
            include_metadata,
        )


@public
class GcsConnector(StorageConnector):
    """This storage connector provides integration to Google Cloud Storage (GCS).

    Once you create a connector in FeatureStore, you can transact data from a GCS bucket into a spark dataframe
    by calling the `read` API.

    Authentication to GCP is handled by uploading the `JSON keyfile for service account` to the Hopsworks Project. For more information
    on service accounts and creating keyfile in GCP, read [Google Cloud documentation](https://cloud.google.com/docs/authentication/production#create_service_account
    'creating service account keyfile').

    The connector also supports the optional encryption method `Customer Supplied Encryption Key` by Google.
    The encryption details are stored as `Secrets` in the FeatureStore for keeping it secure.
    Read more about encryption on [Google Documentation](https://cloud.google.com/storage/docs/encryption#customer-supplied_encryption_keys).

    The storage connector uses the Google `gcs-connector-hadoop` behind the scenes. For more information, check out [Google Cloud Storage Connector for Spark and Hadoop](
    https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs#google-cloud-storage-connector-for-spark-and-hadoop 'google-cloud-storage-connector-for-spark-and-hadoop').
    """

    type = StorageConnector.GCS
    GS_FS_PREFIX = "gs://"  # Google Storage Filesystem prefix

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        key_path: str | None = None,
        bucket: str | None = None,
        algorithm: str | None = None,
        encryption_key: str | None = None,
        encryption_key_hash: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)

        self._bucket = bucket
        self._key_path = key_path
        self._algorithm = algorithm
        self._encryption_key = encryption_key
        self._encryption_key_hash = encryption_key_hash

    @public
    @property
    def key_path(self) -> str | None:
        """JSON keyfile for service account."""
        return self._key_path

    @public
    @property
    def algorithm(self) -> str | None:
        """Encryption Algorithm."""
        return self._algorithm

    @public
    @property
    def encryption_key(self) -> str | None:
        """Encryption Key."""
        return self._encryption_key

    @public
    @property
    def encryption_key_hash(self) -> str | None:
        """Encryption Key Hash."""
        return self._encryption_key_hash

    @public
    @property
    def path(self) -> str | None:
        """The path of the connector along with gs file system prefixed."""
        return self.GS_FS_PREFIX + self._bucket

    @public
    @property
    def bucket(self) -> str | None:
        """GCS Bucket."""
        return self._bucket

    def _get_path(self, sub_path: str) -> str | None:
        if sub_path:
            return os.path.join(self.path, sub_path)
        return self.path

    def spark_options(self) -> dict[str, Any]:
        return {}

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads GCS path into a dataframe using the storage connector.

        To read directly from the default bucket, you can omit the path argument:
        ```python
        conn.read(data_format='spark_formats')
        ```
        Or to read objects from default bucket provide the object path without gsUtil URI schema. For example,
        following will read from a path gs://bucket_on_connector/Path/object :
        ```python
        conn.read(data_format='spark_formats', paths='Path/object')
        ```
        Or to read with full gsUtil URI path,
        ```python
        conn.read(data_format='spark_formats',path='gs://BUCKET/DATA')
        ```

        Parameters:
            query: Not relevant for GCS connectors.
            data_format: Spark data format.
            options: Spark options.
            path: GCS path.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Raises:
            ValueError: Malformed arguments.

        Returns:
            A Spark dataframe.
        """
        if path is None:
            path = ""
        # validate engine supports connector type
        if not engine.get_instance().is_connector_type_supported(self.type):
            raise NotImplementedError(
                "GCS connector not yet supported for engine: " + engine.get_type()
            )

        # validate path begins with gs://
        if not path.startswith(self.GS_FS_PREFIX):
            path = self._get_path(path)
            print(
                f"Prepending default bucket specified on connector, final path: {path}"
            )

        return engine.get_instance().read(
            self, data_format, options or {}, path, dataframe_type
        )

    @public
    def prepare_spark(self, path: str | None = None) -> str | None:
        """Prepare Spark to use this Storage Connector.

        ```python
        conn.prepare_spark()
        spark.read.format("json").load("gs://bucket/path")
        # or
        spark.read.format("json").load(conn.prepare_spark("gs://bucket/path"))
        ```

        Parameters:
            path: Path to prepare for reading from Google cloud storage.
        """
        return engine.get_instance().setup_storage_connector(self, path)


@public
class BigQueryConnector(StorageConnector):
    """The BigQuery storage connector provides integration to Google Cloud BigQuery.

    You can use it to run bigquery on your GCP cluster and load results into spark dataframe by calling the `read` API.

    Authentication to GCP is handled by uploading the `JSON keyfile for service account` to the Hopsworks Project. For more information
    on service accounts and creating keyfile in GCP, read [Google Cloud documentation](https://cloud.google.com/docs/authentication/production#create_service_account
    'creating service account keyfile').

    The storage connector uses the Google `spark-bigquery-connector` behind the scenes.
    To read more about the spark connector, like the spark options or usage, check [Apache Spark SQL connector for Google BigQuery](https://github.com/GoogleCloudDataproc/spark-bigquery-connector#usage
    'github.com/GoogleCloudDataproc/spark-bigquery-connector').
    """

    type = StorageConnector.BIGQUERY
    BIGQUERY_FORMAT = "bigquery"
    BIGQ_CREDENTIALS = "credentials"
    BIGQ_PARENT_PROJECT = "parentProject"
    BIGQ_MATERIAL_DATASET = "materializationDataset"
    BIGQ_VIEWS_ENABLED = "viewsEnabled"
    BIGQ_PROJECT = "project"
    BIGQ_DATASET = "dataset"

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        key_path: str | None = None,
        parent_project: str | None = None,
        dataset: str | None = None,
        query_table: str | None = None,
        query_project: str | None = None,
        materialization_dataset: str | None = None,
        arguments: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)
        self._key_path = key_path
        self._parent_project = parent_project
        self._dataset = dataset
        self._query_table = query_table
        self._query_project = query_project
        self._materialization_dataset = materialization_dataset
        self._arguments = (
            {opt["name"]: opt["value"] for opt in arguments} if arguments else {}
        )

    @public
    @property
    def key_path(self) -> str | None:
        """JSON keyfile for service account."""
        return self._key_path

    @public
    @property
    def parent_project(self) -> str | None:
        """BigQuery parent project (Google Cloud Project ID of the table to bill for the export)."""
        return self._parent_project

    @public
    @property
    def dataset(self) -> str | None:
        """BigQuery dataset (The dataset containing the table)."""
        return self._dataset

    @public
    @property
    def query_table(self) -> str | None:
        """BigQuery table name."""
        return self._query_table

    @public
    @property
    def query_project(self) -> str | None:
        """BigQuery project (The Google Cloud Project ID of the table)."""
        return self._query_project

    @public
    @property
    def materialization_dataset(self) -> str | None:
        """BigQuery materialization dataset (The dataset where the materialized view is going to be created, used in case of query)."""
        return self._materialization_dataset

    @public
    @property
    def arguments(self) -> dict[str, Any]:
        """Additional spark options."""
        return self._arguments

    @public
    def connector_options(self) -> dict[str, Any]:
        """Return options to be passed to an external BigQuery connector library."""
        return {
            "key_path": self._key_path,
            "project_id": self._parent_project,
            "dataset_id": self._dataset,
        }

    def spark_options(self) -> dict[str, Any]:
        properties = self._arguments
        properties[self.BIGQ_PARENT_PROJECT] = self._parent_project

        local_key_path = engine.get_instance().add_file(self._key_path)
        with open(local_key_path, "rb") as credentials_file:
            properties[self.BIGQ_CREDENTIALS] = str(
                base64.b64encode(credentials_file.read()), "utf-8"
            )

        if self._materialization_dataset:
            properties[self.BIGQ_MATERIAL_DATASET] = self._materialization_dataset
            properties[self.BIGQ_VIEWS_ENABLED] = "true"

        if self._query_project:
            properties[self.BIGQ_PROJECT] = self._query_project

        if self._dataset:
            properties[self.BIGQ_DATASET] = self._dataset

        return properties

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads results from BigQuery into a spark dataframe using the storage connector.

          Reading from bigquery is done via either specifying the BigQuery table or BigQuery query.
          For example, to read from a BigQuery table, set the BigQuery project, dataset and table on storage connector
          and read directly from the corresponding path.
            ```python
            conn.read()
            ```
          OR, to read results from a BigQuery query, set `Materialization Dataset` on storage connector,
           and pass your SQL to `query` argument.
            ```python
            conn.read(query='SQL')
            ```
          Optionally, passing `query` argument will take priority at runtime if the table options were also set
          on the storage connector. This allows user to run from both a query or table with same connector, assuming
          all fields were set.
          Also, user can set the `path` argument to a bigquery table path to read at runtime,
           if table options were not set initially while creating the connector.
            ```python
            conn.read(path='project.dataset.table')
            ```

        Parameters:
            query: BigQuery query.
            data_format: Spark data format.
            options: Spark options.
            path: BigQuery table path.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Raises:
            ValueError: Malformed arguments.

        Returns:
            A Spark dataframe.
        """
        # validate engine supports connector type
        if not engine.get_instance().is_connector_type_supported(self.type):
            raise NotImplementedError(
                "BigQuery connector not yet supported for engine: " + engine.get_type()
            )
        # merge user spark options on top of default spark options
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            if not {self.BIGQ_MATERIAL_DATASET, self.BIGQ_VIEWS_ENABLED}.issubset(
                options.keys()
            ):
                raise ValueError(
                    "BigQuery materialization views should be enabled for SQL query. "
                    "Set spark options viewsEnabled=True and "
                    + self.BIGQ_MATERIAL_DATASET
                    + "=<temporaryDatasetName> to options argument or instead use BigQuery Query type connector from UI."
                )
            path = query
        elif self._query_table:
            path = self._query_table
        elif path:
            pass
        else:
            raise ValueError(
                "Either query should be provided "
                "or Query Project,Dataset and Table should be set"
            )

        return engine.get_instance().read(
            self, self.BIGQUERY_FORMAT, options, path, dataframe_type
        )


class RdsConnector(StorageConnector):
    type = StorageConnector.RDS
    JDBC_FORMAT = "jdbc"

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        arguments: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._arguments = (
            {opt["name"]: opt["value"] for opt in arguments} if arguments else {}
        )

    @property
    def host(self) -> str | None:
        return self._host

    @property
    def port(self) -> int | None:
        return self._port

    @property
    def database(self) -> str | None:
        return self._database

    @property
    def user(self) -> str | None:
        return self._user

    @property
    def password(self) -> str | None:
        return self._password

    @property
    def arguments(self) -> dict[str, Any]:
        """Additional options."""
        return self._arguments

    def spark_options(self) -> dict[str, Any]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }

    def connector_options(self) -> dict[str, Any]:
        """Return options to be passed to an external RDS connector library."""
        props = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
        }
        if self.user:
            props["user"] = self.user
        if self.password:
            props["password"] = self.password
        return props

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """Reads a query into a dataframe using the storage connector.

        Parameters:
            query: A SQL query to be read.
            data_format: Not relevant for RDS based connectors.
            options: Any additional key/value options to be passed to the RDS connector.
            path: Not relevant for RDS based connectors.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.

        Returns:
            `DataFrame`.
        """
        self.refetch()
        options = (
            {**self.spark_options(), **options}
            if options is not None
            else self.spark_options()
        )
        if query:
            options["query"] = query

        options["url"] = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

        return engine.get_instance().read(
            self, self.JDBC_FORMAT, options, None, dataframe_type
        )


class OpenSearchConnector(StorageConnector):
    type = StorageConnector.OPENSEARCH

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        host: str | None = None,
        port: int | None = None,
        scheme: str | None = None,
        verify: bool | None = None,
        username: str | None = None,
        password: str | None = None,
        trust_store_path: str | None = None,
        trust_store_password: str | None = None,
        arguments: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)
        self._host = host
        self._port = port
        self._scheme = scheme
        self._verify = verify
        self._username = username
        self._password = password
        self._trust_store_path = trust_store_path
        self._trust_store_password = trust_store_password
        self._arguments = (
            {arg["name"]: arg.get("value", None) for arg in arguments}
            if isinstance(arguments, list)
            else (arguments if arguments else {})
        )

    @property
    def host(self) -> str | None:
        """OpenSearch host address."""
        return self._host

    @property
    def port(self) -> int | None:
        """OpenSearch port number."""
        return self._port

    @property
    def scheme(self) -> str | None:
        """Connection scheme (http or https)."""
        return self._scheme

    @property
    def verify(self) -> bool | None:
        """Whether to verify SSL certificates."""
        return self._verify

    @property
    def username(self) -> str | None:
        """OpenSearch username for authentication."""
        return self._username

    @property
    def password(self) -> str | None:
        """OpenSearch password for authentication."""
        return self._password

    @property
    def arguments(self) -> dict[str, Any]:
        """Additional OpenSearch connection options."""
        return self._arguments

    def spark_options(self) -> dict[str, Any]:
        return self.connector_options()

    def connector_options(self) -> dict[str, Any]:
        """Return options to be passed to an external OpenSearch connector library."""
        props = {
            "http_compress": False,
        }
        if self._host:
            props[OPENSEARCH_CONFIG.HOSTS] = [
                {"host": self._host, "port": self._port or 9200}
            ]
        if self._scheme:
            props[OPENSEARCH_CONFIG.USE_SSL] = self._scheme == "https"
        if self._verify is not None:
            props[OPENSEARCH_CONFIG.VERIFY_CERTS] = self._verify
        # do not set http_auth, the client use jwt for authentication
        ca_certs = self._create_ca_certs()
        if ca_certs:
            props[OPENSEARCH_CONFIG.CA_CERTS] = ca_certs
        # Merge additional arguments
        for key, value in self._arguments.items():
            if key.startswith("opensearchpy."):
                props[key.replace("opensearchpy.", "")] = value
        return props

    def _create_ca_certs(self) -> str | None:
        """Convert truststore JKS to PEM chain and return the PEM path.

        Uses the underlying Hopsworks client helper to convert the JKS truststore
        into a PEM CA chain file that can be consumed by Python libraries such as
        `opensearch-py`. If the `trust_store_path` is
        not configured on the connector, this method returns `None`.
        """
        # Return cached path if already created
        ca_attr = "_ca_certs_path"
        if hasattr(self, ca_attr):
            return getattr(self, ca_attr)

        # Only require a path; the password may legitimately be an empty string.
        if not self._trust_store_path:
            return None

        # Download the truststore from HDFS / remote storage to a local path first
        local_trust_store_path = engine.get_instance().add_file(self._trust_store_path)

        # Reuse the same truststore for both keystore and truststore inputs since
        # we only need a CA chain for server verification.
        ca_chain_path, _, _ = client.get_instance()._write_pem(
            local_trust_store_path,
            self._trust_store_password,
            local_trust_store_path,
            self._trust_store_password,
            f"opensearch_sc_{client.get_instance()._project_id}_{self._id}",
        )

        setattr(self, ca_attr, ca_chain_path)
        return ca_chain_path

    @public
    def read(
        self,
        query: str | None = None,
        data_format: str | None = None,
        options: dict[str, Any] | None = None,
        path: str | None = None,
        dataframe_type: Literal[
            "default", "spark", "pandas", "polars", "numpy", "python"
        ] = "default",
    ) -> (
        TypeVar("pyspark.sql.DataFrame")
        | TypeVar("pyspark.RDD")
        | pd.DataFrame
        | np.ndarray
        | pl.DataFrame
    ):
        """\

        Failure:
            This operation is not supported.
            Use [`feature_group.read`][hsfs.feature_group.FeatureGroup.read] instead.

        Raises:
            NotImplementedError: Always, since this operation is not supported.
        """
        raise NotImplementedError(
            "Cannot read from OpenSearch connector. Please use feature_group.read() instead."
        )


class CRMSource(Enum):
    HUBSPOT = "hubspot"
    FACEBOOK_ADS = "facebook_ads"
    SALESFORCE = "salesforce"
    PIPEDRIVE = "pipedrive"
    FRESHDESK = "freshdesk"
    GOOGLE_ADS = "google_ads"
    GOOGLE_ANALYTICS = "google_analytics"


class CRMAndAnalyticsConnector(StorageConnector):
    type = StorageConnector.CRM

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        crm_type: CRMSource,
        description: str | None = None,
        # members specific to type of connector
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        domain: str | None = None,
        account_id: str | None = None,
        key_path: str | None = None,
        property_id: str | None = None,
        dev_token: str | None = None,
        customer_id: str | None = None,
        impersonated_email: str | None = None,
        refresh_token: str | None = None,
        headers: dict[str, Any] | None = None,
        parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)
        self._api_key = api_key
        self._crm_type = crm_type
        self._username = username
        self._password = password
        self._domain = domain
        self._account_id = account_id
        self._key_path = key_path
        self._property_id = property_id
        self._dev_token = dev_token
        self._customer_id = customer_id
        self._impersonated_email = impersonated_email
        self._refresh_token = refresh_token
        self._headers = headers or {}
        self._parameters = parameters or {}

    @property
    def api_key(self):
        return self._api_key

    @property
    def crm_type(self):
        return self._crm_type

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def domain(self):
        return self._domain

    @property
    def account_id(self):
        return self._account_id

    @property
    def key_path(self):
        return self._key_path

    @property
    def property_id(self):
        return self._property_id

    @property
    def dev_token(self):
        return self._dev_token

    @property
    def customer_id(self):
        return self._customer_id

    @property
    def impersonated_email(self):
        return self._impersonated_email

    @property
    def refresh_token(self):
        return self._refresh_token

    @property
    def headers(self):
        """Additional headers."""
        return self._headers

    @property
    def parameters(self):
        """Additional parameters."""
        return self._parameters

    def spark_options(self) -> dict[str, Any]:
        return {}


class RestConnectorHeader:
    def __init__(self, name: str, value: str) -> None:
        self._name = name
        self._value = value

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> str:
        return self._value

    def to_dict(self) -> dict[str, str]:
        return {
            "name": self._name,
            "value": self._value,
        }


class RestConnectorClientConfig:
    def __init__(
        self, base_url: str, headers: list[RestConnectorHeader] | None = None
    ) -> None:
        self._base_url = base_url
        self._headers = self._normalize_headers(headers or [])

    def _normalize_headers(
        self, headers: list[RestConnectorHeader | dict[str, str]]
    ) -> list[RestConnectorHeader]:
        normalized_headers: list[RestConnectorHeader] = []
        for header in headers:
            if isinstance(header, RestConnectorHeader):
                normalized_headers.append(header)
            elif isinstance(header, dict):
                normalized_headers.append(RestConnectorHeader(**header))
            else:
                raise TypeError(
                    "REST connector headers must be RestConnectorHeader or dict."
                )
        return normalized_headers

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def headers(self):
        return self._headers

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseUrl": self._base_url,
            "headers": [header.to_dict() for header in self._headers],
        }


class RestConnectorAuthConfig:
    def __init__(
        self,
        auth_type: RestConnectorAuthType | str | None = None,
        bearer_token: str | None = None,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        access_token: str | None = None,
        access_token_url: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        access_token_request_params: list[dict[str, Any]] | None = None,
        default_token_timeout_minutes: int | None = None,
    ) -> None:
        self._auth_type = self._normalize_auth_type(auth_type)
        self._bearer_token = bearer_token
        self._api_key = api_key
        self._username = username
        self._password = password
        self._access_token = access_token
        self._access_token_url = access_token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token_request_params = access_token_request_params or []
        self._default_token_timeout_minutes = default_token_timeout_minutes

    def _normalize_auth_type(
        self, auth_type: RestConnectorAuthType | str | None
    ) -> RestConnectorAuthType | str | None:
        if isinstance(auth_type, RestConnectorAuthType) or auth_type is None:
            return auth_type
        if isinstance(auth_type, str):
            try:
                return RestConnectorAuthType[auth_type.upper()]
            except KeyError:
                try:
                    return RestConnectorAuthType(auth_type)
                except ValueError:
                    return auth_type
        return auth_type

    def _serialize_auth_type(self) -> str | None:
        if isinstance(self._auth_type, RestConnectorAuthType):
            return self._auth_type.name
        if isinstance(self._auth_type, str):
            return self._auth_type
        return None

    @property
    def auth_type(self):
        return self._auth_type

    @property
    def bearer_token(self):
        return self._bearer_token

    @property
    def api_key(self):
        return self._api_key

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def access_token(self):
        return self._access_token

    @property
    def access_token_url(self):
        return self._access_token_url

    @property
    def client_id(self):
        return self._client_id

    @property
    def client_secret(self):
        return self._client_secret

    @property
    def access_token_request_params(self):
        return self._access_token_request_params

    @property
    def default_token_timeout_minutes(self):
        return self._default_token_timeout_minutes

    def to_dict(self) -> dict[str, Any]:
        access_token_request_params = (
            self._access_token_request_params
            if self._access_token_request_params
            else None
        )
        payload = {
            "authType": self._serialize_auth_type(),
            "bearerToken": self._bearer_token,
            "apiKey": self._api_key,
            "username": self._username,
            "password": self._password,
            "accessToken": self._access_token,
            "accessTokenUrl": self._access_token_url,
            "clientId": self._client_id,
            "clientSecret": self._client_secret,
            "accessTokenRequestParams": access_token_request_params,
            "defaultTokenTimeoutMinutes": self._default_token_timeout_minutes,
        }
        return {key: value for key, value in payload.items() if value is not None}


class RestConnectorAuthType(Enum):
    NONE = "none"
    BASIC = "http_basic"
    BEARER = "bearer"
    API_KEY = "api_key"
    OAUTH2 = "oauth2_client_credentials"


class RestConnector(StorageConnector):
    type = StorageConnector.REST

    def __init__(
        self,
        id: int | None,
        name: str,
        featurestore_id: int,
        description: str | None = None,
        # members specific to type of connector
        client_config: RestConnectorClientConfig | None = None,
        auth_config: RestConnectorAuthConfig | None = None,
        auth_type: RestConnectorAuthType | None = RestConnectorAuthType.NONE,
        **kwargs,
    ) -> None:
        super().__init__(id, name, description, featurestore_id)
        self._client_config = self._normalize_client_config(client_config)
        self._auth_config = self._normalize_auth_config(auth_config, auth_type)
        self._auth_type = (
            self._auth_config.auth_type if self._auth_config else auth_type
        )

    def _normalize_client_config(
        self, client_config: RestConnectorClientConfig | dict[str, Any] | None
    ) -> RestConnectorClientConfig | None:
        if client_config is None:
            return None
        if isinstance(client_config, RestConnectorClientConfig):
            return client_config
        if isinstance(client_config, dict):
            return RestConnectorClientConfig(**humps.decamelize(client_config))
        raise TypeError("REST connector client config must be dict or object.")

    def _normalize_auth_config(
        self,
        auth_config: RestConnectorAuthConfig | dict[str, Any] | None,
        auth_type: RestConnectorAuthType | None,
    ) -> RestConnectorAuthConfig | None:
        if auth_config is None:
            if auth_type is None:
                return None
            return RestConnectorAuthConfig(auth_type=auth_type)
        if isinstance(auth_config, RestConnectorAuthConfig):
            return auth_config
        if isinstance(auth_config, dict):
            config = humps.decamelize(auth_config)
            if "auth_type" not in config and auth_type is not None:
                config["auth_type"] = auth_type
            return RestConnectorAuthConfig(**config)
        raise TypeError("REST connector auth config must be dict or object.")

    @property
    def client_config(self):
        return self._client_config

    @property
    def auth_config(self):
        return self._auth_config

    @property
    def auth_type(self):
        if self._auth_config and self._auth_config.auth_type is not None:
            return self._auth_config.auth_type
        return self._auth_type

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        payload.update(
            {
                "type": "featurestoreRESTConnectorDTO",
                "description": self._description,
                "authConfig": (
                    self._auth_config.to_dict() if self._auth_config else None
                ),
                "clientConfig": (
                    self._client_config.to_dict() if self._client_config else None
                ),
            }
        )
        return payload

    def spark_options(self) -> dict[str, Any]:
        return {}

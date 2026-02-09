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

import json
import logging
import math
import numbers
import os
import random
import re
import sys
import uuid
import warnings
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
)

from hopsworks_common import constants
from hsfs.core.type_systems import (
    cast_column_to_offline_type,
    cast_column_to_online_type,
)


if TYPE_CHECKING:
    import great_expectations
    from hsfs.training_dataset import TrainingDataset

import boto3
import hsfs
import pandas as pd
import pyarrow as pa
from botocore.response import StreamingBody
from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core import inode
from hopsworks_common.core.constants import HAS_POLARS, polars_not_installed_message
from hopsworks_common.core.type_systems import create_extended_type
from hopsworks_common.decorators import uses_great_expectations, uses_polars
from hopsworks_common.util import generate_fully_qualified_feature_name
from hsfs import (
    engine,
    feature,
    feature_view,
    util,
)
from hsfs import storage_connector as sc
from hsfs.constructor import query
from hsfs.constructor.fs_query import FsQuery
from hsfs.core import (
    dataset_api,
    delta_engine,
    feature_group_api,
    feature_view_api,
    ingestion_job_conf,
    job,
    job_api,
    kafka_engine,
    statistics_api,
    storage_connector_api,
    training_dataset_api,
    training_dataset_job_conf,
    transformation_function_engine,
)
from hsfs.core.constants import (
    HAS_AIOMYSQL,
    HAS_GREAT_EXPECTATIONS,
    HAS_NUMPY,
    HAS_PANDAS,
    HAS_PYARROW,
    HAS_SQLALCHEMY,
)
from hsfs.core.feature_logging import LoggingMetaData
from hsfs.core.type_systems import PYARROW_HOPSWORKS_DTYPE_MAPPING
from hsfs.core.vector_db_client import VectorDbClient
from hsfs.feature_group import ExternalFeatureGroup, FeatureGroup
from hsfs.hopsworks_udf import HopsworksUdf, UDFExecutionMode
from hsfs.training_dataset_split import TrainingDatasetSplit


if HAS_GREAT_EXPECTATIONS:
    import great_expectations

if HAS_NUMPY:
    import numpy as np

if HAS_AIOMYSQL and HAS_SQLALCHEMY:
    from hsfs.core import util_sql

if HAS_SQLALCHEMY:
    from sqlalchemy import sql

if HAS_PANDAS:
    from hsfs.core.type_systems import convert_pandas_dtype_to_offline_type

if HAS_POLARS:
    import polars as pl

_logger = logging.getLogger(__name__)


class Engine:
    def __init__(self) -> None:
        _logger.debug("Initialising Python Engine...")
        self._dataset_api: dataset_api.DatasetApi = dataset_api.DatasetApi()
        self._job_api: job_api.JobApi = job_api.JobApi()
        self._feature_group_api: feature_group_api.FeatureGroupApi = (
            feature_group_api.FeatureGroupApi()
        )
        self._storage_connector_api: storage_connector_api.StorageConnectorApi = (
            storage_connector_api.StorageConnectorApi()
        )

        # cache the sql engine which contains the connection pool
        self._mysql_online_fs_engine = None
        _logger.info("Python Engine initialized.")

    def sql(
        self,
        sql_query: str,
        feature_store: str,
        online_conn: sc.JdbcConnector | None,
        dataframe_type: str,
        read_options: dict[str, Any] | None,
        schema: list[feature.Feature] | None = None,
    ) -> pd.DataFrame | pl.DataFrame:
        if not online_conn:
            return self._sql_offline(
                sql_query,
                dataframe_type,
                schema,
                arrow_flight_config=read_options.get("arrow_flight_config", {})
                if read_options
                else {},
            )
        return self._jdbc(sql_query, online_conn, dataframe_type, read_options, schema)

    def is_flyingduck_query_supported(
        self, query: query.Query, read_options: dict[str, Any] | None = None
    ) -> bool:
        from hsfs.core import arrow_flight_client

        return arrow_flight_client.is_query_supported(query, read_options or {})

    def _validate_dataframe_type(self, dataframe_type: str):
        if not isinstance(dataframe_type, str) or dataframe_type.lower() not in [
            "pandas",
            "polars",
            "numpy",
            "python",
            "default",
        ]:
            raise FeatureStoreException(
                f'dataframe_type : {dataframe_type} not supported. Possible values are "default", "pandas", "polars", "numpy" or "python"'
            )

    def extract_logging_metadata(
        self,
        untransformed_features: pd.DataFrame | pl.DataFrame,
        transformed_features: pd.DataFrame | pl.DataFrame,
        feature_view: feature_view.FeatureView,
        transformed: bool,
        inference_helpers: bool,
        event_time: bool,
        primary_key: bool,
        request_parameters: pd.DataFrame | pl.DataFrame = None,
    ):
        """Extracts the logging data from the passed dataframes and returns the expected batch dataframe with the logging metadata attached.

        The logging metadata is created as a hidden attribute named `hopsworks_logging_metadata` of the returned dataframe.

        The return dataframe will only contain the features that the user requested (i.e. if the user requested to not include primary keys, event time or inference helpers, those features will be excluded).
        """
        # Get the fully qualified names for the event time and serving keys.
        # This required since the since Hopsworks returns primary keys and event time as fully qualified names, if they are not explicitly selected in the feature view query.
        fully_qualified_root_fg_event_time = generate_fully_qualified_feature_name(
            feature_view.query._left_feature_group,
            feature_view.query._left_feature_group.event_time,
        )

        fully_qualified_serving_key_mapper = {
            generate_fully_qualified_feature_name(
                key._feature_group, key.feature_name
            ): key.feature_name
            for key in feature_view.serving_keys
            if key.required
        }
        fully_qualified_event_time_mapper = {
            fully_qualified_root_fg_event_time: feature_view._root_feature_group_event_time_column_name
        }

        # Extract the serving key and rename if primary key has a fully qualified name, this is required since fully qualified names are not used for logging.
        serving_keys_df = untransformed_features[
            [
                col
                for col in feature_view._fully_qualified_primary_keys
                if col in fully_qualified_serving_key_mapper
                or col in fully_qualified_serving_key_mapper.values()
            ]
        ].rename(columns=fully_qualified_serving_key_mapper)

        # Extract event time and rename if necessary.
        event_time_df = untransformed_features[
            [
                fully_qualified_root_fg_event_time
                if fully_qualified_root_fg_event_time
                in feature_view._fully_qualified_event_time
                else feature_view._root_feature_group_event_time_column_name
            ]
        ].rename(columns=fully_qualified_event_time_mapper)

        # Extract inference_helpers
        inference_helpers_df = untransformed_features[
            feature_view.inference_helper_columns
        ]

        # Drop unneeded columns and prepare dataframe to be be returned to user
        dropped_columns = []

        if not inference_helpers:
            dropped_columns.extend(feature_view.inference_helper_columns)

        if not event_time:
            dropped_columns.extend(feature_view._fully_qualified_event_time)

        if not primary_key:
            dropped_columns.extend(feature_view._fully_qualified_primary_keys)

        if dropped_columns:
            untransformed_features = self.drop_columns(
                untransformed_features, drop_cols=dropped_columns
            )
            transformed_features = self.drop_columns(
                transformed_features, drop_cols=dropped_columns
            )

        # Create Logging metadata
        logging_meta_data = LoggingMetaData()
        logging_meta_data.inference_helper = inference_helpers_df
        logging_meta_data.untransformed_features = untransformed_features
        logging_meta_data.transformed_features = transformed_features
        logging_meta_data.request_parameters = request_parameters
        logging_meta_data.serving_keys = serving_keys_df
        logging_meta_data.event_time = event_time_df

        # Create extended dataframe that includes logging metadata and return to user
        batch_dataframe = (
            transformed_features if transformed else untransformed_features
        )

        extended_type = create_extended_type(type(batch_dataframe))
        batch_dataframe = extended_type(batch_dataframe)
        batch_dataframe.hopsworks_logging_metadata = logging_meta_data

        return batch_dataframe

    def _sql_offline(
        self,
        sql_query: str | FsQuery,
        dataframe_type: str,
        schema: list[feature.Feature] | None = None,
        arrow_flight_config: dict[str, Any] | None = None,
    ) -> pd.DataFrame | pl.DataFrame:
        self._validate_dataframe_type(dataframe_type)
        if isinstance(sql_query, FsQuery):
            from hsfs.core import arrow_flight_client

            result_df = util.run_with_loading_animation(
                "Reading data from Hopsworks, using Hopsworks Feature Query Service",
                arrow_flight_client.get_instance().read_query,
                sql_query,
                arrow_flight_config or {},
                dataframe_type,
            )
        else:
            raise ValueError(
                "Reading data with Hive is not supported when using hopsworks client version >= 4.0"
            )
        if schema:
            result_df = Engine.cast_columns(result_df, schema)
        return self._return_dataframe_type(result_df, dataframe_type)

    def _jdbc(
        self,
        sql_query: str,
        connector: sc.JdbcConnector,
        dataframe_type: str,
        read_options: dict[str, Any] | None,
        schema: list[feature.Feature] | None = None,
    ) -> pd.DataFrame | pl.DataFrame:
        self._validate_dataframe_type(dataframe_type)
        if self._mysql_online_fs_engine is None:
            self._mysql_online_fs_engine = util_sql.create_mysql_engine(
                connector,
                (
                    client._is_external()
                    if "external" not in read_options
                    else read_options["external"]
                ),
            )
        with self._mysql_online_fs_engine.connect() as mysql_conn:
            if "sqlalchemy" in str(type(mysql_conn)):
                sql_query = sql.text(sql_query)
            if dataframe_type.lower() == "polars":
                if not HAS_POLARS:
                    raise ModuleNotFoundError(polars_not_installed_message)
                result_df = pl.read_database(sql_query, mysql_conn)
            else:
                result_df = pd.read_sql(sql_query, mysql_conn)
            if schema:
                result_df = Engine.cast_columns(result_df, schema, online=True)
        return self._return_dataframe_type(result_df, dataframe_type)

    def read(
        self,
        storage_connector: sc.StorageConnector,
        data_format: str,
        read_options: dict[str, Any] | None,
        location: str | None,
        dataframe_type: Literal["polars", "pandas", "default"],
    ) -> pd.DataFrame | pl.DataFrame:
        if not data_format:
            raise FeatureStoreException("data_format is not specified")

        if storage_connector.type == storage_connector.HOPSFS:
            path = storage_connector._get_path(location)
            df_list = self._read_hopsfs(path, data_format, read_options, dataframe_type)
        elif storage_connector.type == storage_connector.S3:
            df_list = self._read_s3(
                storage_connector, location, data_format, dataframe_type
            )
        else:
            raise NotImplementedError(
                f"{storage_connector.type} Storage Connectors for training datasets are not supported yet for external environments."
            )
        if dataframe_type.lower() == "polars":
            if not HAS_POLARS:
                raise ModuleNotFoundError(polars_not_installed_message)
            # Below check performed since some files materialized when creating training data are empty
            # If empty dataframe is in df_list then polars cannot concatenate df_list due to schema mismatch
            # However if the entire split contains only empty files which can occur when the data size is very small then one of the empty dataframe is return so that the column names can be accessed.
            non_empty_df_list = [df for df in df_list if not df.is_empty()]
            if non_empty_df_list:
                return self._return_dataframe_type(
                    pl.concat(non_empty_df_list), dataframe_type=dataframe_type
                )
            return df_list[0]
        return self._return_dataframe_type(
            pd.concat(df_list, ignore_index=True), dataframe_type=dataframe_type
        )

    def _read_pandas(self, data_format: str, obj: Any) -> pd.DataFrame:
        if data_format.lower() == "csv":
            return pd.read_csv(obj)
        if data_format.lower() == "tsv":
            return pd.read_csv(obj, sep="\t")
        if data_format.lower() == "parquet" and isinstance(obj, StreamingBody):
            return pd.read_parquet(BytesIO(obj.read()))
        if data_format.lower() == "parquet":
            return pd.read_parquet(obj)
        raise TypeError(
            f"{data_format} training dataset format is not supported to read as pandas dataframe."
        )

    @uses_polars
    def _read_polars(
        self, data_format: Literal["csv", "tsv", "parquet"], obj: Any
    ) -> pl.DataFrame:
        if not HAS_POLARS:
            raise ModuleNotFoundError(polars_not_installed_message)
        if data_format.lower() == "csv":
            return pl.read_csv(obj)
        if data_format.lower() == "tsv":
            return pl.read_csv(obj, separator="\t")
        if data_format.lower() == "parquet" and isinstance(obj, StreamingBody):
            return pl.read_parquet(BytesIO(obj.read()), use_pyarrow=True)
        if data_format.lower() == "parquet":
            return pl.read_parquet(obj, use_pyarrow=True)
        raise TypeError(
            f"{data_format} training dataset format is not supported to read as polars dataframe."
        )

    def _is_metadata_file(self, path):
        return Path(path).stem.startswith("_")

    def _read_hopsfs(
        self,
        location: str,
        data_format: str,
        read_options: dict[str, Any] | None = None,
        dataframe_type: str = "default",
    ) -> list[pd.DataFrame | pl.DataFrame]:
        return self._read_hopsfs_remote(
            location, data_format, read_options or {}, dataframe_type
        )

    # This read method uses the Hopsworks REST APIs or Flyingduck Server
    # To read the training dataset content, this to allow users to read Hopsworks training dataset from outside
    def _read_hopsfs_remote(
        self,
        location: str,
        data_format: str,
        read_options: dict[str, Any] | None = None,
        dataframe_type: str = "default",
    ) -> list[pd.DataFrame | pl.DataFrame]:
        df_list = []
        if read_options is None:
            read_options = {}

        # Check if the location is a file or directory
        path_metadata = self._dataset_api._get(location)
        is_dir = path_metadata.get("attributes", {}).get("dir", False)

        if is_dir:
            # Location is a directory, list all files
            total_count = 10000
            offset = 0
            while offset < total_count:
                total_count, inode_list = self._dataset_api._list_dataset_path(
                    location, inode.Inode, offset=offset, limit=100
                )

                for inode_entry in inode_list:
                    if not self._is_metadata_file(inode_entry.path):
                        df = self._read_single_hopsfs_file(
                            inode_entry.path, data_format, read_options, dataframe_type
                        )
                        df_list.append(df)
                offset += len(inode_list)
        else:
            # Location is a single file, read it directly
            if not self._is_metadata_file(location):
                df = self._read_single_hopsfs_file(
                    location, data_format, read_options, dataframe_type
                )
                df_list.append(df)

        return df_list

    def _read_single_hopsfs_file(
        self,
        path: str,
        data_format: str,
        read_options: dict[str, Any],
        dataframe_type: str,
    ) -> pd.DataFrame | pl.DataFrame:
        from hsfs.core import arrow_flight_client

        if arrow_flight_client.is_data_format_supported(data_format, read_options):
            arrow_flight_config = read_options.get("arrow_flight_config")
            return arrow_flight_client.get_instance().read_path(
                path,
                arrow_flight_config,
                dataframe_type=dataframe_type,
            )
        content_stream = self._dataset_api.read_content(path)
        if dataframe_type.lower() == "polars":
            return self._read_polars(data_format, BytesIO(content_stream.content))
        return self._read_pandas(data_format, BytesIO(content_stream.content))

    def _read_s3(
        self,
        storage_connector: sc.S3Connector,
        location: str,
        data_format: str,
        dataframe_type: str = "default",
    ) -> list[pd.DataFrame | pl.DataFrame]:
        # get key prefix
        path_parts = location.replace("s3://", "").split("/")
        _ = path_parts.pop(0)  # pop first element -> bucket

        prefix = "/".join(path_parts)

        if storage_connector.session_token is not None:
            # This is only for AWS IAM role passthrough.
            # We don't need to set the endpoint_url and region here.
            s3 = boto3.client(
                "s3",
                aws_access_key_id=storage_connector.access_key,
                aws_secret_access_key=storage_connector.secret_key,
                aws_session_token=storage_connector.session_token,
            )
        else:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=storage_connector.access_key,
                aws_secret_access_key=storage_connector.secret_key,
                endpoint_url=storage_connector.arguments.get("fs.s3a.endpoint"),
                region_name=storage_connector.region,
            )

        df_list = []
        object_list = {"is_truncated": True}
        while object_list.get("is_truncated", False):
            if "NextContinuationToken" in object_list:
                object_list = s3.list_objects_v2(
                    Bucket=storage_connector.bucket,
                    Prefix=prefix,
                    MaxKeys=1000,
                    ContinuationToken=object_list["NextContinuationToken"],
                )
            else:
                object_list = s3.list_objects_v2(
                    Bucket=storage_connector.bucket,
                    Prefix=prefix,
                    MaxKeys=1000,
                )

            for obj in object_list["Contents"]:
                if not self._is_metadata_file(obj["Key"]) and obj["Size"] > 0:
                    obj = s3.get_object(
                        Bucket=storage_connector.bucket,
                        Key=obj["Key"],
                    )
                    if dataframe_type.lower() == "polars":
                        df_list.append(self._read_polars(data_format, obj["Body"]))
                    else:
                        df_list.append(self._read_pandas(data_format, obj["Body"]))
        return df_list

    def read_options(
        self, data_format: str | None, provided_options: dict[str, Any] | None
    ) -> dict[str, Any]:
        return provided_options or {}

    def read_stream(
        self,
        storage_connector: sc.StorageConnector,
        message_format: Any,
        schema: Any,
        options: dict[str, Any] | None,
        include_metadata: bool,
    ) -> Any:
        raise NotImplementedError(
            "Streaming Sources are not supported for pure Python Environments."
        )

    def show(
        self,
        sql_query: str,
        feature_store: str,
        n: int,
        online_conn: sc.JdbcConnector,
        read_options: dict[str, Any] | None = None,
    ) -> pd.DataFrame | pl.DataFrame:
        return self.sql(
            sql_query, feature_store, online_conn, "default", read_options or {}
        ).head(n)

    def read_vector_db(
        self,
        feature_group: hsfs.feature_group.FeatureGroup,
        n: int = None,
        dataframe_type: str = "default",
    ) -> pd.DataFrame | pl.DataFrame | np.ndarray | list[list[Any]]:
        dataframe_type = dataframe_type.lower()
        self._validate_dataframe_type(dataframe_type)

        results = VectorDbClient.read_feature_group(feature_group, n)
        feature_names = [f.name for f in feature_group.features]
        if dataframe_type == "polars":
            if not HAS_POLARS:
                raise ModuleNotFoundError(polars_not_installed_message)
            df = pl.DataFrame(results, schema=feature_names)
        else:
            df = pd.DataFrame(results, columns=feature_names, index=None)
        return self._return_dataframe_type(df, dataframe_type)

    def register_external_temporary_table(
        self, external_fg: ExternalFeatureGroup, alias: str
    ) -> None:
        # No op to avoid query failure
        pass

    def register_delta_temporary_table(
        self,
        delta_fg_alias,
        feature_store_id,
        feature_store_name,
        read_options,
        is_cdc_query: bool = False,
    ):
        # No op to avoid query failure
        pass

    def register_hudi_temporary_table(
        self,
        hudi_fg_alias: hsfs.constructor.hudi_feature_group_alias.HudiFeatureGroupAlias,
        feature_store_id: int,
        feature_store_name: str,
        read_options: dict[str, Any] | None,
    ) -> None:
        if hudi_fg_alias and (
            hudi_fg_alias.left_feature_group_end_timestamp is not None
            or hudi_fg_alias.left_feature_group_start_timestamp is not None
        ):
            raise FeatureStoreException(
                "Incremental queries are not supported in the python client."
                " Read feature group without timestamp to retrieve latest snapshot or switch to "
                "environment with Spark Engine."
            )

    def profile_by_spark(
        self,
        metadata_instance: FeatureGroup
        | ExternalFeatureGroup
        | feature_view.FeatureView
        | TrainingDataset,
    ) -> job.Job:
        stat_api = statistics_api.StatisticsApi(
            metadata_instance.feature_store_id, metadata_instance.ENTITY_TYPE
        )
        job = stat_api.compute(metadata_instance)
        print(
            f"Statistics Job started successfully, you can follow the progress at \n{util.get_job_url(job.href)}"
        )

        job._wait_for_job()
        return job

    def profile(
        self,
        df: pd.DataFrame | pl.DataFrame,
        relevant_columns: list[str],
        correlations: Any,
        histograms: Any,
        exact_uniqueness: bool = True,
    ) -> str:
        # TODO: add statistics for correlations, histograms and exact_uniqueness
        _logger.info("Computing insert statistics")
        if HAS_POLARS and (
            isinstance(df, (pl.DataFrame, pl.dataframe.frame.DataFrame))
        ):
            arrow_schema = df.to_arrow().schema
        else:
            arrow_schema = pa.Schema.from_pandas(df, preserve_index=False)

        # parse timestamp columns to string columns
        for field in arrow_schema:
            if not (
                pa.types.is_null(field.type)
                or pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_struct(field.type)
            ) and PYARROW_HOPSWORKS_DTYPE_MAPPING.get(field.type, None) in [
                "timestamp",
                "date",
            ]:
                if HAS_POLARS and (
                    isinstance(df, (pl.DataFrame, pl.dataframe.frame.DataFrame))
                ):
                    _logger.debug(
                        f"Casting polars dataframe column {field.name} from {field.type} to string"
                    )
                    df = df.with_columns(pl.col(field.name).cast(pl.String))
                else:
                    _logger.debug(
                        f"Casting column pandas dataframe column {field.name} from {field.type} to string"
                    )
                    df[field.name] = df[field.name].astype(str)

        if relevant_columns is None or len(relevant_columns) == 0:
            stats = df.describe().to_dict()
            relevant_columns = df.columns
        else:
            target_cols = [col for col in df.columns if col in relevant_columns]
            _logger.debug(f"Target columns for describe: {target_cols}")
            stats = df[target_cols].describe().to_dict()
        _logger.debug(f"Column stats computed via describe for: {stats.keys()}")
        # df.describe() does not compute stats for all col types (e.g., string)
        # we need to compute stats for the rest of the cols iteratively
        missing_cols = list(set(relevant_columns) - set(stats.keys()))
        _logger.debug(f"Columns missing stats from describe: {missing_cols}")
        for col in missing_cols:
            # for some datatypes (e.g list[int]) the describe method fails
            try:
                _logger.debug(f"Computing stats for column {col}")
                stats[col] = df[col].describe().to_dict()
            except Exception as e:
                warnings.warn(
                    f"Failed to compute stats for column {col}: {e}, adding empty stats",
                    util.FeatureGroupWarning,
                    stacklevel=1,
                )
                stats[col] = {}
        final_stats = []
        for col in relevant_columns:
            if HAS_POLARS and (
                isinstance(df, (pl.DataFrame, pl.dataframe.frame.DataFrame))
            ):
                stats[col] = dict(zip(stats["statistic"], stats[col]))
            # set data type
            arrow_type = arrow_schema.field(col).type
            if (
                pa.types.is_null(arrow_type)
                or pa.types.is_list(arrow_type)
                or pa.types.is_large_list(arrow_type)
                or pa.types.is_struct(arrow_type)
                or PYARROW_HOPSWORKS_DTYPE_MAPPING.get(arrow_type, None)
                in ["timestamp", "date", "binary", "string"]
            ):
                dataType = "String"
            elif PYARROW_HOPSWORKS_DTYPE_MAPPING.get(arrow_type, None) in [
                "float",
                "double",
            ]:
                dataType = "Fractional"
            elif PYARROW_HOPSWORKS_DTYPE_MAPPING.get(arrow_type, None) in [
                "int",
                "bigint",
            ]:
                dataType = "Integral"
            elif PYARROW_HOPSWORKS_DTYPE_MAPPING.get(arrow_type, None) == "boolean":
                dataType = "Boolean"
            else:
                print(
                    "Data type could not be inferred for column '"
                    + col.split(".")[-1]
                    + "'. Defaulting to 'String'",
                    file=sys.stderr,
                )
                dataType = "String"

            stat = self._convert_pandas_statistics(stats[col], dataType)
            stat["isDataTypeInferred"] = "false"
            stat["column"] = col.split(".")[-1]
            stat["completeness"] = 1

            final_stats.append(stat)

        return json.dumps(
            {"columns": final_stats},
        )

    def _convert_pandas_statistics(
        self, stat: dict[str, Any], dataType: str
    ) -> dict[str, Any]:
        # For now transformation only need 25th, 50th, 75th percentiles
        # TODO: calculate properly all percentiles
        _logger.debug(
            f"Converting pandas statistics: {stat} of type {dataType} to be similar to Deequ stats"
        )
        content_dict = {"dataType": dataType}
        if "count" in stat:
            content_dict["count"] = stat["count"]
        if dataType != "String":
            if "25%" in stat:
                percentiles = [0] * 100
                percentiles[24] = stat["25%"]
                percentiles[49] = stat["50%"]
                percentiles[74] = stat["75%"]
                content_dict["approxPercentiles"] = percentiles
            if "mean" in stat:
                content_dict["mean"] = stat["mean"]
            if (
                "mean" in stat
                and "count" in stat
                and isinstance(stat["mean"], numbers.Number)
            ):
                content_dict["sum"] = stat["mean"] * stat["count"]
            if "max" in stat:
                content_dict["maximum"] = stat["max"]
            if "std" in stat and not pd.isna(stat["std"]):
                content_dict["stdDev"] = stat["std"]
            if "min" in stat:
                content_dict["minimum"] = stat["min"]
        if "unique" in stat:
            content_dict["approximateNumDistinctValues"] = stat["unique"]
            content_dict["exactNumDistinctValues"] = stat["unique"]

        _logger.debug(f"Converted statistics: {content_dict}")

        return content_dict

    def validate(
        self, dataframe: pd.DataFrame, expectations: Any, log_activity: bool = True
    ) -> None:
        raise NotImplementedError(
            "Deequ data validation is only available with Spark Engine. Use validate_with_great_expectations"
        )

    @uses_great_expectations
    def validate_with_great_expectations(
        self,
        dataframe: pl.DataFrame | pd.DataFrame,
        expectation_suite: great_expectations.core.ExpectationSuite,
        ge_validate_kwargs: dict[Any, Any] | None = None,
    ) -> great_expectations.core.ExpectationSuiteValidationResult:
        # This conversion might cause a bottleneck in performance when using polars with greater expectations.
        # This patch is done becuase currently great_expecatations does not support polars, would need to be made proper when support added.
        if HAS_POLARS and (
            isinstance(dataframe, (pl.DataFrame, pl.dataframe.frame.DataFrame))
        ):
            warnings.warn(
                "Currently Great Expectations does not support Polars dataframes. This operation will convert to Pandas dataframe that can be slow.",
                util.FeatureGroupWarning,
                stacklevel=1,
            )
            dataframe = dataframe.to_pandas()
        if ge_validate_kwargs is None:
            ge_validate_kwargs = {}
        return great_expectations.from_pandas(
            dataframe, expectation_suite=expectation_suite
        ).validate(**ge_validate_kwargs)

    def set_job_group(self, group_id: str, description: str | None) -> None:
        pass

    def convert_to_default_dataframe(
        self, dataframe: pd.DataFrame | pl.DataFrame | pl.dataframe.frame.DataFrame
    ) -> pd.DataFrame | None:
        if isinstance(dataframe, pd.DataFrame) or (
            HAS_POLARS
            and (isinstance(dataframe, (pl.DataFrame, pl.dataframe.frame.DataFrame)))
        ):
            upper_case_features = [
                col for col in dataframe.columns if any(re.finditer("[A-Z]", col))
            ]
            space_features = [col for col in dataframe.columns if " " in col]

            # make shallow copy so the original df does not get changed
            # this is always needed to keep the user df unchanged
            if isinstance(dataframe, pd.DataFrame):
                dataframe_copy = dataframe.copy(deep=False)
            else:
                dataframe_copy = dataframe.clone()

            # making a shallow copy of the dataframe so that column names are unchanged
            if len(upper_case_features) > 0:
                warnings.warn(
                    f"The ingested dataframe contains upper case letters in feature names: `{upper_case_features}`. "
                    "Feature names are sanitized to lower case in the feature store.",
                    util.FeatureGroupWarning,
                    stacklevel=1,
                )
            if len(space_features) > 0:
                warnings.warn(
                    f"The ingested dataframe contains feature names with spaces: `{space_features}`. "
                    "Feature names are sanitized to use underscore '_' in the feature store.",
                    util.FeatureGroupWarning,
                    stacklevel=1,
                )
            dataframe_copy.columns = [
                util.autofix_feature_name(x) for x in dataframe_copy.columns
            ]

            # convert timestamps with timezone to UTC
            for col in dataframe_copy.columns:
                if isinstance(
                    dataframe_copy[col].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype
                ):
                    dataframe_copy[col] = dataframe_copy[col].dt.tz_convert(None)
                elif HAS_POLARS and isinstance(dataframe_copy[col].dtype, pl.Datetime):
                    dataframe_copy = dataframe_copy.with_columns(
                        pl.col(col).dt.replace_time_zone(None)
                    )
            return dataframe_copy
        if dataframe == "spine":
            return None

        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: pandas dataframe, polars dataframe. "
            f"The provided dataframe has type: {type(dataframe)}"
        )

    def parse_schema_feature_group(
        self,
        dataframe: pd.DataFrame | pl.DataFrame,
        time_travel_format: str | None = None,
        features: list[feature.Feature] | None = None,
    ) -> list[feature.Feature]:
        feature_type_map = {}
        if features:
            for _feature in features:
                feature_type_map[_feature.name] = _feature.type
        if isinstance(dataframe, pd.DataFrame):
            arrow_schema = pa.Schema.from_pandas(dataframe, preserve_index=False)
        elif (
            HAS_POLARS
            and isinstance(dataframe, pl.DataFrame)
            or isinstance(dataframe, pl.dataframe.frame.DataFrame)
        ):
            arrow_schema = dataframe.to_arrow().schema
        features = []
        for i in range(len(arrow_schema.names)):
            feat_name = arrow_schema.names[i]
            name = util.autofix_feature_name(feat_name)
            try:
                pd_type = arrow_schema.field(feat_name).type
                if pa.types.is_null(pd_type) and feature_type_map.get(name):
                    converted_type = feature_type_map.get(name)
                else:
                    converted_type = convert_pandas_dtype_to_offline_type(pd_type)
            except ValueError as e:
                raise FeatureStoreException(f"Feature '{name}': {str(e)}") from e
            features.append(feature.Feature(name, converted_type))

        return features

    def parse_schema_training_dataset(
        self, dataframe: pd.DataFrame | pl.DataFrame
    ) -> list[feature.Feature]:
        raise NotImplementedError(
            "Training dataset creation from Dataframes is not "
            "supported in Python environment. Use HSFS Query object instead."
        )

    def _to_arrow_table(self, dataframe: pd.DataFrame | pl.DataFrame):
        """Convert a pandas or polars DataFrame to a pyarrow.Table.

        Args:
            dataframe: Union[pd.DataFrame, pl.DataFrame]

        Returns:
            pyarrow.Table

        Raises:
            ImportError: if pyarrow is not installed
            TypeError: if the dataframe is not supported type
        """
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required to convert to Arrow table.") from e

        if isinstance(dataframe, pd.DataFrame):
            return pa.Table.from_pandas(dataframe, preserve_index=False)
        if HAS_POLARS and isinstance(dataframe, pl.DataFrame):
            return dataframe.to_arrow()
        raise TypeError(
            f"Unsupported dataframe type for arrow conversion: {type(dataframe)}"
        )

    def _check_duplicate_records(self, dataset, feature_group_instance):
        """Check for duplicate records within primary_key, event_time and partition_key columns.

        Raises FeatureStoreException if duplicates are found.

        Parameters:
        -----------
        dataset : Union[pd.DataFrame, pl.DataFrame]
            The dataset to check for duplicates
        feature_group_instance : FeatureGroup
            The feature group instance containing primary_key, event_time and partition_key
        """
        # Get the key columns to check (primary_key + partition_key)
        key_columns = list(feature_group_instance.primary_key)

        if not key_columns:
            # No keys to check, skip validation
            return

        if feature_group_instance.event_time:
            key_columns.append(feature_group_instance.event_time)

        if feature_group_instance.partition_key:
            key_columns.extend(feature_group_instance.partition_key)

        dataset = self._to_arrow_table(dataset)
        # Verify all key columns exist
        table_columns = dataset.column_names
        missing_columns = [col for col in key_columns if col not in table_columns]
        if missing_columns:
            raise FeatureStoreException(
                f"Key columns {missing_columns} are missing from the dataset. "
                f"Available columns: {table_columns}"
            )

        import pyarrow.compute as pc

        # Check for duplicates using PyArrow group_by
        # Group by key columns and count occurrences
        grouped = dataset.group_by(key_columns).aggregate(
            [
                # The aggregation tuple structure: ([], function_name, FunctionOptions)
                ([], "count_all", pc.CountOptions(mode="all"))
            ]
        )

        # Filter groups with count > 1 (duplicates)
        duplicate_groups = grouped.filter(pc.greater(grouped["count_all"], 1))

        duplicate_count = len(duplicate_groups)

        if duplicate_count > 0:
            # Get total number of duplicate rows (sum of counts - 1 for each duplicate group)
            # Since count includes the first occurrence, duplicates = count - 1 per group
            total_duplicate_rows = (
                sum(duplicate_groups["count_all"].to_pylist()) - duplicate_count
            )

            # Get sample duplicate records for error message
            # Take first 10 duplicate groups and get their key values
            sample_groups = duplicate_groups.slice(0, min(10, duplicate_count))

            # Build sample string showing the duplicate key combinations
            sample_rows = []
            for i in range(len(sample_groups)):
                row_dict = {}
                for col in key_columns:
                    row_dict[col] = sample_groups[col][i].as_py()
                row_dict["count_all"] = sample_groups["count_all"][i].as_py()
                sample_rows.append(str(row_dict))

            sample_str = "\n".join(sample_rows)

            raise FeatureStoreException(
                FeatureStoreException.DUPLICATE_RECORD_ERROR_MESSAGE
                + f"\nDataset contains {total_duplicate_rows} duplicate record(s) within "
                f"primary_key ({feature_group_instance.primary_key}) and "
                f"partition_key ({feature_group_instance.partition_key}). "
                f"Found {duplicate_count} duplicate group(s). "
                f"Sample duplicate key combinations:\n{sample_str}"
            )

    def save_dataframe(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame | pl.DataFrame,
        operation: str,
        online_enabled: bool,
        storage: str,
        offline_write_options: dict[str, Any],
        online_write_options: dict[str, Any],
        validation_id: int | None = None,
    ) -> job.Job | None:
        if (
            # Only `FeatureGroup` class has time_travel_format property
            isinstance(feature_group, FeatureGroup)
            and feature_group.time_travel_format == "DELTA"
        ):
            self._check_duplicate_records(dataframe, feature_group)
            _logger.debug("No duplicate records found. Proceeding with Delta write.")

        if (
            hasattr(feature_group, "EXTERNAL_FEATURE_GROUP")
            and feature_group.online_enabled
        ) or feature_group.stream:
            return self._write_dataframe_kafka(
                feature_group, dataframe, offline_write_options
            )
        if engine.get_type() == "python":
            if feature_group.time_travel_format == "DELTA":
                delta_engine_instance = delta_engine.DeltaEngine(
                    feature_store_id=feature_group.feature_store_id,
                    feature_store_name=feature_group.feature_store_name,
                    feature_group=feature_group,
                    spark_context=None,
                    spark_session=None,
                )
                delta_engine_instance.save_delta_fg(
                    dataframe,
                    write_options=offline_write_options,
                    validation_id=validation_id,
                )
        else:
            # for backwards compatibility
            return self.legacy_save_dataframe(
                feature_group,
                dataframe,
                operation,
                online_enabled,
                storage,
                offline_write_options,
                online_write_options,
                validation_id,
            )
        return None

    def legacy_save_dataframe(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame | pl.DataFrame,
        operation: str,
        online_enabled: bool,
        storage: str,
        offline_write_options: dict[str, Any],
        online_write_options: dict[str, Any],
        validation_id: int | None = None,
    ) -> job.Job | None:
        # App configuration
        app_options = self._get_app_options(offline_write_options)

        # Setup job for ingestion
        # Configure Hopsworks ingestion job
        print("Configuring ingestion job...")
        ingestion_job = self._feature_group_api.ingestion(feature_group, app_options)

        # Upload dataframe into Hopsworks
        print("Uploading Pandas dataframe...")
        self._dataset_api.upload_feature_group(
            feature_group, ingestion_job.data_path, dataframe
        )

        # run job
        ingestion_job.job.run(
            await_termination=offline_write_options is None
            or offline_write_options.get("wait_for_job", True)
        )

        return ingestion_job.job

    def get_training_data(
        self,
        training_dataset_obj: TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        query_obj: query.Query,
        read_options: dict[str, Any],
        dataframe_type: str,
        training_dataset_version: int = None,
        transformation_context: dict[str, Any] = None,
    ) -> pd.DataFrame | pl.DataFrame:
        """Function that creates or retrieves already created the training dataset.

        Parameters:
            training_dataset_obj `TrainingDataset`: The training dataset metadata object.
            feature_view_obj `FeatureView`: The feature view object for the which the training data is being created.
            query_obj `Query`: The query object that contains the query used to create the feature view.
            read_options `Dict[str, Any]`: Dictionary that can be used to specify extra parameters for reading data.
            dataframe_type `str`: The type of dataframe returned.
            training_dataset_version `int`: Version of training data to be retrieved.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                The `context` variable must be explicitly defined as parameters in the transformation function for these to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.

        Raises:
            ValueError: If the training dataset statistics could not be retrieved.
        """
        # dataframe_type of list and numpy are prevented here because statistics needs to be computed from the returned dataframe.
        # The daframe is converted into required types in the function split_labels
        if dataframe_type.lower() not in ["default", "polars", "pandas"]:
            dataframe_type = "default"

        if training_dataset_obj.splits:
            return self._prepare_transform_split_df(
                query_obj,
                training_dataset_obj,
                feature_view_obj,
                read_options,
                dataframe_type,
                training_dataset_version,
                transformation_context=transformation_context,
            )
        df = query_obj.read(read_options=read_options, dataframe_type=dataframe_type)
        # if training_dataset_version is None:
        transformation_function_engine.TransformationFunctionEngine.compute_and_set_feature_statistics(
            training_dataset_obj, feature_view_obj, df
        )
        # else:
        #    transformation_function_engine.TransformationFunctionEngine.get_and_set_feature_statistics(
        #        training_dataset_obj, feature_view_obj, training_dataset_version
        #    )
        return transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions(
            transformation_functions=feature_view_obj.transformation_functions,
            data=df,
            online=False,
            transformation_context=transformation_context,
            request_parameters=None,
        )

    def split_labels(
        self,
        df: pd.DataFrame | pl.DataFrame,
        labels: list[str],
        dataframe_type: str,
    ) -> tuple[pd.DataFrame | pl.DataFrame, pd.DataFrame | pl.DataFrame | None]:
        if labels:
            labels_df = df[labels]
            df_new = df.drop(columns=labels)
            return (
                self._return_dataframe_type(df_new, dataframe_type),
                self._return_dataframe_type(labels_df, dataframe_type),
            )
        return self._return_dataframe_type(df, dataframe_type), None

    def drop_columns(
        self, df: pd.DataFrame | pl.DataFrame, drop_cols: list[str]
    ) -> pd.DataFrame | pl.DataFrame:
        if HAS_POLARS and (
            isinstance(df, (pl.DataFrame, pl.dataframe.frame.DataFrame))
        ):
            return df.drop(*drop_cols)
        return df.drop(columns=drop_cols)

    def _prepare_transform_split_df(
        self,
        query_obj: query.Query,
        training_dataset_obj: TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        read_option: dict[str, Any],
        dataframe_type: str,
        training_dataset_version: int = None,
        transformation_context: dict[str, Any] = None,
    ) -> dict[str, pd.DataFrame | pl.DataFrame]:
        """Split a df into slices defined by `splits`. `splits` is a `dict(str, int)` which keys are name of split and values are split ratios.

        Parameters:
            query_obj `Query`: The query object that contains the query used to create the feature view.
            training_dataset_obj `TrainingDataset`: The training dataset metadata object.
            feature_view_obj `FeatureView`: The feature view object for the which the training data is being created.
            read_options `Dict[str, Any]`: Dictionary that can be used to specify extra parameters for reading data.
            dataframe_type `str`: The type of dataframe returned.
            training_dataset_version `int`: Version of training data to be retrieved.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                The `context` variable must be explicitly defined as parameters in the transformation function for these to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.

        Raises:
            ValueError: If the training dataset statistics could not be retrieved.
        """
        if (
            training_dataset_obj.splits[0].split_type
            == TrainingDatasetSplit.TIME_SERIES_SPLIT
        ):
            event_time = query_obj._left_feature_group.event_time

            event_time_feature = [
                _feature
                for _feature in query_obj.features
                if (
                    _feature.name == event_time
                    and _feature._feature_group_id == query_obj._left_feature_group.id
                )
            ]

            if not event_time_feature:
                # Event time feature not in query manually adding event_time of root feature group.
                # Using fully qualified name of the event time feature to avoid ambiguity.

                event_time_feature = query_obj._left_feature_group.__getattr__(
                    event_time
                )
                event_time_feature.use_fully_qualified_name = True

                query_obj.append_feature(event_time_feature)
                event_time = event_time_feature._get_fully_qualified_feature_name(
                    feature_group=query_obj._left_feature_group
                )

                result_dfs = self._time_series_split(
                    query_obj.read(
                        read_options=read_option, dataframe_type=dataframe_type
                    ),
                    training_dataset_obj,
                    event_time,
                    drop_event_time=True,
                )
            else:
                # Use the fully qualified name of the event time feature if required
                event_time = event_time_feature[0]._get_fully_qualified_feature_name(
                    feature_group=query_obj._left_feature_group
                )

                result_dfs = self._time_series_split(
                    query_obj.read(
                        read_options=read_option, dataframe_type=dataframe_type
                    ),
                    training_dataset_obj,
                    event_time,
                )
        else:
            result_dfs = self._random_split(
                query_obj.read(read_options=read_option, dataframe_type=dataframe_type),
                training_dataset_obj,
            )

        # TODO : Currently statistics always computed since in memory training dataset retrieved is not consistent
        # if training_dataset_version is None:
        transformation_function_engine.TransformationFunctionEngine.compute_and_set_feature_statistics(
            training_dataset_obj, feature_view_obj, result_dfs
        )
        # else:
        #    transformation_function_engine.TransformationFunctionEngine.get_and_set_feature_statistics(
        #        training_dataset_obj, feature_view_obj, training_dataset_version
        #    )
        # and the apply them
        for split_name in result_dfs:
            result_dfs[split_name] = (
                transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions(
                    transformation_functions=feature_view_obj.transformation_functions,
                    data=result_dfs.get(split_name),
                    online=False,
                    transformation_context=transformation_context,
                    request_parameters=None,
                )
                if feature_view_obj.transformation_functions
                else result_dfs.get(split_name)
            )

        return result_dfs

    def _random_split(
        self,
        df: pd.DataFrame | pl.DataFrame,
        training_dataset_obj: TrainingDataset,
    ) -> dict[str, pd.DataFrame | pl.DataFrame]:
        split_column = f"_SPLIT_INDEX_{uuid.uuid1()}"
        result_dfs = {}
        splits = training_dataset_obj.splits
        if (
            not math.isclose(
                sum([split.percentage for split in splits]), 1
            )  # relative tolerance = 1e-09
            or sum([split.percentage > 1 or split.percentage < 0 for split in splits])
            > 1
        ):
            raise ValueError(
                "Sum of split ratios should be 1 and each values should be in range (0, 1)"
            )

        df_size = len(df)
        groups = []
        for i, split in enumerate(splits):
            groups += [i] * int(df_size * split.percentage)
        groups += [len(splits) - 1] * (df_size - len(groups))
        random.shuffle(groups)
        if HAS_POLARS and (
            isinstance(df, (pl.DataFrame, pl.dataframe.frame.DataFrame))
        ):
            df = df.with_columns(pl.Series(name=split_column, values=groups))
        else:
            df[split_column] = groups
        for i, split in enumerate(splits):
            if HAS_POLARS and (
                isinstance(df, (pl.DataFrame, pl.dataframe.frame.DataFrame))
            ):
                split_df = df.filter(pl.col(split_column) == i).drop(split_column)
            else:
                split_df = df[df[split_column] == i].drop(split_column, axis=1)
            result_dfs[split.name] = split_df
        return result_dfs

    def _time_series_split(
        self,
        df: pd.DataFrame | pl.DataFrame,
        training_dataset_obj: TrainingDataset,
        event_time: str,
        drop_event_time: bool = False,
    ) -> dict[str, pd.DataFrame | pl.DataFrame]:
        result_dfs = {}
        for split in training_dataset_obj.splits:
            if len(df[event_time]) > 0:
                result_df = df[
                    [
                        split.start_time
                        <= util.convert_event_time_to_timestamp(t)
                        < split.end_time
                        for t in df[event_time]
                    ]
                ]
            else:
                # if df[event_time] is empty, it returns an empty dataframe
                result_df = df
            if drop_event_time:
                result_df = result_df.drop([event_time], axis=1)
            result_dfs[split.name] = result_df
        return result_dfs

    def write_training_dataset(
        self,
        training_dataset: TrainingDataset,
        dataset: query.Query | pd.DataFrame | pl.DataFrame,
        user_write_options: dict[str, Any],
        save_mode: str,
        feature_view_obj: feature_view.FeatureView | None = None,
        to_df: bool = False,
        transformation_context: dict[str, Any] = None,
    ) -> job.Job | Any:
        if not feature_view_obj and not isinstance(dataset, query.Query):
            raise Exception(
                "Currently only query based training datasets are supported by the Python engine"
            )

        try:
            from hsfs.core import arrow_flight_client

            arrow_flight_client_imported = True
        except ImportError:
            arrow_flight_client_imported = False

        if (
            arrow_flight_client_imported
            and arrow_flight_client.is_query_supported(dataset, user_write_options)
            and len(training_dataset.splits) == 0
            and feature_view_obj
            and len(feature_view_obj.transformation_functions) == 0
            and training_dataset.data_format == "parquet"
            and not transformation_context
        ):
            query_obj, _ = dataset._prep_read(False, user_write_options)
            return util.run_with_loading_animation(
                "Materializing data to Hopsworks, using Hopsworks Feature Query Service",
                arrow_flight_client.get_instance().create_training_dataset,
                feature_view_obj,
                training_dataset,
                query_obj,
                user_write_options.get("arrow_flight_config", {}),
            )

        # As for creating a feature group, users have the possibility of passing
        # a spark_job_configuration object as part of the user_write_options with the key "spark"
        spark_job_configuration = user_write_options.pop("spark", None)

        # Pass transformation context to the training dataset job
        if transformation_context:
            raise FeatureStoreException(
                "Cannot pass transformation context to training dataset materialization job from the Python Kernel. Please use the Spark Kernel."
            )

        td_app_conf = training_dataset_job_conf.TrainingDatasetJobConf(
            query=dataset,
            overwrite=(save_mode == "overwrite"),
            write_options=user_write_options,
            spark_job_configuration=spark_job_configuration,
        )

        if feature_view_obj:
            fv_api = feature_view_api.FeatureViewApi(feature_view_obj.featurestore_id)
            td_job = fv_api.compute_training_dataset(
                feature_view_obj.name,
                feature_view_obj.version,
                training_dataset.version,
                td_app_conf,
            )
        else:
            td_api = training_dataset_api.TrainingDatasetApi(
                training_dataset.feature_store_id
            )
            td_job = td_api.compute(training_dataset, td_app_conf)
        print(
            f"Training dataset job started successfully, you can follow the progress at \n{util.get_job_url(td_job.href)}"
        )

        td_job._wait_for_job(
            await_termination=user_write_options.get("wait_for_job", True)
        )
        return td_job

    def _return_dataframe_type(
        self, dataframe: pd.DataFrame | pl.DataFrame, dataframe_type: str
    ) -> pd.DataFrame | pl.DataFrame | np.ndarray | list[list[Any]]:
        """Returns a dataframe of particular type.

        Parameters:
            dataframe `Union[pd.DataFrame, pl.DataFrame]`: Input dataframe
            dataframe_type `str`: Type of dataframe to be returned
        Returns:
            `Union[pd.DataFrame, pl.DataFrame, np.array, list]`: DataFrame of required type.
        """
        if dataframe_type.lower() in ["default", "pandas"]:
            return dataframe
        if dataframe_type.lower() == "polars":
            if not HAS_POLARS:
                raise ModuleNotFoundError(polars_not_installed_message)
            if not (isinstance(dataframe, (pl.DataFrame, pl.Series))):
                return pl.from_pandas(dataframe)
            return dataframe
        if dataframe_type.lower() == "numpy":
            return dataframe.values
        if dataframe_type.lower() == "python":
            return dataframe.values.tolist()

        raise TypeError(
            f"Dataframe type `{dataframe_type}` not supported on this platform."
        )

    def is_spark_dataframe(
        self, dataframe: pd.DataFrame | pl.DataFrame
    ) -> Literal[False]:
        return False

    def save_stream_dataframe(
        self,
        feature_group: FeatureGroup | ExternalFeatureGroup,
        dataframe: pd.DataFrame | pl.DataFrame,
        query_name: str | None,
        output_mode: str | None,
        await_termination: bool,
        timeout: int | None,
        write_options: dict[str, Any] | None,
    ) -> None:
        raise NotImplementedError(
            "Stream ingestion is not available on Python environments, because it requires Spark as engine."
        )

    def update_table_schema(
        self, feature_group: FeatureGroup | ExternalFeatureGroup
    ) -> None:
        _job = self._feature_group_api.update_table_schema(feature_group)
        _job._wait_for_job(await_termination=True)

    def _get_app_options(
        self, user_write_options: dict[str, Any] | None = None
    ) -> ingestion_job_conf.IngestionJobConf:
        """Generate the options that should be passed to the application doing the ingestion.

        Options should be data format, data options to read the input dataframe and
        insert options to be passed to the insert method.

        Users can pass Spark configurations to the save/insert method
        Property name should match the value in the JobConfiguration.__init__
        """
        spark_job_configuration = (
            user_write_options.pop("spark", None) if user_write_options else None
        )

        return ingestion_job_conf.IngestionJobConf(
            data_format="PARQUET",
            data_options=[],
            write_options=user_write_options or {},
            spark_job_configuration=spark_job_configuration,
        )

    def add_file(self, file: str | None, distribute=True) -> str | None:
        if not file:
            return file

        # This is used for unit testing
        if not file.startswith("file://"):
            file = "hdfs://" + file

        local_file = os.path.join("/tmp", os.path.basename(file))
        if not os.path.exists(local_file):
            content_stream = self._dataset_api.read_content(
                file, util.get_dataset_type(file)
            )
            bytesio_object = BytesIO(content_stream.content)
            # Write the stuff
            with open(local_file, "wb") as f:
                f.write(bytesio_object.getbuffer())
        return local_file

    def shallow_copy_dataframe(
        self, dataframe: pd.DataFrame | pl.DataFrame
    ) -> pd.DataFrame | pl.DataFrame:
        if HAS_POLARS and (
            isinstance(dataframe, (pl.DataFrame, pl.dataframe.frame.DataFrame))
        ):
            return dataframe.clone()
        if HAS_PANDAS and isinstance(dataframe, pd.DataFrame):
            return dataframe.copy()
        raise ValueError(
            f"Dataframe type {type(dataframe)} not supported in the Python engine."
        )

    def apply_udf_on_dataframe(
        self,
        udf: HopsworksUdf,
        dataframe: pd.DataFrame | pl.DataFrame,
        online: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """Apply a udf to a dataframe."""
        if (
            udf.execution_mode.get_current_execution_mode(online=online)
            == UDFExecutionMode.PANDAS
        ):
            return self._apply_pandas_udf(
                hopsworks_udf=udf, dataframe=dataframe, online=online
            )
        return self._apply_python_udf(
            hopsworks_udf=udf, dataframe=dataframe, online=online
        )

    def _apply_python_udf(
        self,
        hopsworks_udf: HopsworksUdf,
        dataframe: pd.DataFrame | pl.DataFrame,
        online: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """Apply a python udf to a dataframe.

        Parameters:
            transformation_functions `List[transformation_function.TransformationFunction]` : List of transformation functions.
            dataset `Union[pd.DataFrame, pl.DataFrame]`: A pandas or polars dataframe.

        Returns:
            `DataFrame`: A pandas dataframe with the transformed data.

        Raises:
            `hopsworks.client.exceptions.FeatureStoreException`: If any of the features mentioned in the transformation function is not present in the Feature View.
        """
        udf = hopsworks_udf.get_udf(online=online)
        if isinstance(dataframe, pd.DataFrame):
            if len(hopsworks_udf.return_types) > 1:
                dataframe[hopsworks_udf.output_column_names] = dataframe.apply(
                    lambda x: udf(*x[hopsworks_udf.transformation_features]),
                    axis=1,
                    result_type="expand",
                )
            else:
                dataframe[hopsworks_udf.output_column_names[0]] = dataframe.apply(
                    lambda x: udf(*x[hopsworks_udf.transformation_features]),
                    axis=1,
                    result_type="expand",
                )
                if hopsworks_udf.output_column_names[0] in dataframe.columns:
                    # Overwriting features so reordering dataframe to move overwritten column to the end of the dataframe
                    cols = dataframe.columns.tolist()
                    cols.append(
                        cols.pop(cols.index(hopsworks_udf.output_column_names[0]))
                    )
                    dataframe = dataframe[cols]
        elif HAS_POLARS and (
            isinstance(dataframe, (pl.DataFrame, pl.dataframe.frame.DataFrame))
        ):
            # Dynamically creating lambda function so that we do not need to loop though to extract features required for the udf.
            # This is done because polars 'map_rows' provides rows as tuples to the udf.
            transformation_features = ", ".join(
                [
                    f"x[{dataframe.columns.index(feature)}]"
                    for feature in hopsworks_udf.transformation_features
                ]
            )
            feature_mapping_wrapper = eval(
                f"lambda x: udf({transformation_features})", locals()
            )
            transformed_features = dataframe.map_rows(feature_mapping_wrapper)
            dataframe = dataframe.with_columns(
                transformed_features.rename(
                    dict(
                        zip(
                            transformed_features.columns,
                            hopsworks_udf.output_column_names,
                        )
                    )
                )
            )
        return dataframe

    def _apply_pandas_udf(
        self,
        hopsworks_udf: HopsworksUdf,
        dataframe: pd.DataFrame | pl.DataFrame,
        online: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """Apply a pandas udf to a dataframe.

        Parameters:
            transformation_functions `List[transformation_function.TransformationFunction]` : List of transformation functions.
            dataset `Union[pd.DataFrame, pl.DataFrame]`: A pandas or polars dataframe.

        Returns:
            `DataFrame`: A pandas dataframe with the transformed data.

        Raises:
            `hopsworks.client.exceptions.FeatureStoreException`: If any of the features mentioned in the transformation function is not present in the Feature View.
        """
        # Cast to pandas if polars dataframe to avoid errors when applying the pandas UDF.
        if HAS_POLARS and (
            isinstance(dataframe, (pl.DataFrame, pl.dataframe.frame.DataFrame))
        ):
            # Converting polars dataframe to pandas because currently we support only pandas UDF's as transformation functions.
            if HAS_PYARROW:
                dataframe = dataframe.to_pandas(
                    use_pyarrow_extension_array=True
                )  # Zero copy if pyarrow extension can be used.
            else:
                dataframe = dataframe.to_pandas(use_pyarrow_extension_array=False)

        if len(hopsworks_udf.return_types) > 1:
            dataframe[hopsworks_udf.output_column_names] = hopsworks_udf.get_udf(
                online=online
            )(
                *(
                    [
                        dataframe[feature]
                        for feature in hopsworks_udf.transformation_features
                    ]
                )
            ).set_index(
                dataframe.index
            )  # Index is set to the input dataframe index so that pandas would merge the new columns without reordering them.
        else:
            dataframe[hopsworks_udf.output_column_names[0]] = hopsworks_udf.get_udf(
                online=online
            )(
                *(
                    [
                        dataframe[feature]
                        for feature in hopsworks_udf.transformation_features
                    ]
                )
            ).set_axis(
                dataframe.index
            )  # Index is set to the input dataframe index so that pandas would merge the new column without reordering it.
            if hopsworks_udf.output_column_names[0] in dataframe.columns:
                # Overwriting features also reordering dataframe to move overwritten column to the end of the dataframe
                cols = dataframe.columns.tolist()
                cols.append(cols.pop(cols.index(hopsworks_udf.output_column_names[0])))
                dataframe = dataframe[cols]
        return dataframe

    @staticmethod
    def get_unique_values(
        feature_dataframe: pd.DataFrame | pl.DataFrame, feature_name: str
    ) -> np.ndarray:
        return feature_dataframe[feature_name].unique()

    def _write_dataframe_kafka(
        self,
        feature_group: FeatureGroup | ExternalFeatureGroup,
        dataframe: pd.DataFrame | pl.DataFrame,
        offline_write_options: dict[str, Any],
    ) -> job.Job | None:
        initial_check_point = ""
        producer, headers, feature_writers, writer = kafka_engine.init_kafka_resources(
            feature_group,
            offline_write_options,
            num_entries=len(dataframe),
        )

        if not feature_group._multi_part_insert:
            # set initial_check_point to the current offset
            initial_check_point = kafka_engine.kafka_get_offsets(
                topic_name=feature_group._online_topic_name,
                feature_store_id=feature_group.feature_store_id,
                offline_write_options=offline_write_options,
                high=True,
            )

        acked, progress_bar = kafka_engine.build_ack_callback_and_optional_progress_bar(
            n_rows=dataframe.shape[0],
            is_multi_part_insert=feature_group._multi_part_insert,
            offline_write_options=offline_write_options,
        )

        if isinstance(dataframe, pd.DataFrame):
            row_iterator = dataframe.itertuples(index=False)
        else:
            row_iterator = dataframe.iter_rows(named=True)

        # loop over rows
        for row in row_iterator:
            if isinstance(dataframe, pd.DataFrame):
                # itertuples returns Python NamedTyple, to be able to serialize it using
                # avro, create copy of row only by converting to dict, which preserves datatypes
                row = row._asdict()
            encoded_row = kafka_engine.encode_row(feature_writers, writer, row)

            # assemble key
            key = "".join([str(row[pk]) for pk in sorted(feature_group.primary_key)])

            kafka_engine.kafka_produce(
                producer=producer,
                key=key,
                encoded_row=encoded_row,
                topic_name=feature_group._online_topic_name,
                headers=headers,
                acked=acked,
                debug_kafka=offline_write_options.get("debug_kafka", False),
            )

        # make sure producer blocks and everything is delivered
        if not feature_group._multi_part_insert:
            producer.flush()
            del producer
            progress_bar.close()

        # start materialization job if not an external feature group, otherwise return None
        if isinstance(feature_group, ExternalFeatureGroup):
            return None
        # if topic didn't exist, always run the materialization job to reset the offsets except if it's a multi insert
        if not initial_check_point and not feature_group._multi_part_insert:
            if self._start_offline_materialization(offline_write_options):
                warnings.warn(
                    "This is the first ingestion after an upgrade or backup/restore, running materialization job even though `start_offline_materialization` was set to `False`.",
                    util.FeatureGroupWarning,
                    stacklevel=1,
                )
            # set the initial_check_point to the lowest offset (it was not set previously due to topic not existing)
            initial_check_point = kafka_engine.kafka_get_offsets(
                topic_name=feature_group._online_topic_name,
                feature_store_id=feature_group.feature_store_id,
                offline_write_options=offline_write_options,
                high=False,
            )
            now = datetime.now(timezone.utc)
            feature_group.materialization_job.run(
                args=feature_group.materialization_job.config.get("defaultArgs", "")
                + (
                    f" -initialCheckPointString {initial_check_point}"
                    if initial_check_point
                    else ""
                ),
                await_termination=offline_write_options.get("wait_for_job", False),
            )
            offline_backfill_every_hr = offline_write_options.pop(
                "offline_backfill_every_hr", None
            )
            if offline_backfill_every_hr:
                if isinstance(offline_backfill_every_hr, str):
                    cron_expression = offline_backfill_every_hr
                elif isinstance(offline_backfill_every_hr, int):
                    cron_expression = f"{now.second} {now.minute} {now.hour}/{offline_backfill_every_hr} ? * * *"
                feature_group.materialization_job.schedule(
                    cron_expression=cron_expression,
                    # added 2 seconds after the current time to avoid retriggering the job directly
                    start_time=now + timedelta(seconds=2),
                )
            else:
                _logger.info("Materialisation job was not scheduled.")

        elif self._start_offline_materialization(offline_write_options):
            if not offline_write_options.get(
                "skip_offsets", False
            ) and self._job_api.last_execution(
                feature_group.materialization_job
            ):  # always skip offsets if executing job for the first time
                # don't provide the current offsets (read from where the job last left off)
                initial_check_point = ""
            # provide the initial_check_point as it will reduce the read amplification of materialization job
            feature_group.materialization_job.run(
                args=feature_group.materialization_job.config.get("defaultArgs", "")
                + (
                    f" -initialCheckPointString {initial_check_point}"
                    if initial_check_point
                    else ""
                ),
                await_termination=offline_write_options.get("wait_for_job", False),
            )

        # wait for online ingestion
        if feature_group.online_enabled and offline_write_options.get(
            "wait_for_online_ingestion", False
        ):
            feature_group.get_latest_online_ingestion().wait_for_completion(
                options=offline_write_options.get("online_ingestion_options", {})
            )

        return feature_group.materialization_job

    @staticmethod
    def cast_columns(
        df: pd.DataFrame, schema: list[feature.Feature], online: bool = False
    ) -> pd.DataFrame:
        for _feat in schema:
            if not online:
                df[_feat.name] = cast_column_to_offline_type(df[_feat.name], _feat.type)
            else:
                df[_feat.name] = cast_column_to_online_type(
                    df[_feat.name], _feat.online_type
                )
        return df

    @staticmethod
    def is_connector_type_supported(connector_type: str) -> bool:
        return connector_type in [
            sc.StorageConnector.HOPSFS,
            sc.StorageConnector.S3,
            sc.StorageConnector.KAFKA,
        ]

    @staticmethod
    def _start_offline_materialization(offline_write_options: dict[str, Any]) -> bool:
        if offline_write_options is not None:
            if "start_offline_materialization" in offline_write_options:
                return offline_write_options.get("start_offline_materialization")
            if "start_offline_backfill" in offline_write_options:
                return offline_write_options.get("start_offline_backfill")
            return True
        return True

    @staticmethod
    def _convert_feature_log_to_df(
        feature_log: list[Any]
        | list[list[Any]]
        | pd.DataFrame
        | pl.DataFrame
        | list[dict[str, Any]]
        | dict[str, Any],
        cols: list[str],
    ) -> pd.DataFrame:
        """Function that converts the feature log to a pandas dataframe.

        In case the feature log is a list of lists, list of numpy arrays, list of dictionaries, pandas dataframe, polars dataframe, pandas series or a polars series it is directly converted to a pandas dataframe.
        If the feature_log is `None` and cols are provided an empty dataframe with the provided columns is returned.

        Parameters:
            feature_log `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame, List[Dict[str, Any]], Dict[str, Any]]`: Feature log provided by the user.
            cols `List[str]`: List of expected features in the logging dataframe.

        Returns:
            `pd.DataFrame`: A pandas dataframe with the feature log that contains the expected features.
        """
        if feature_log is None and cols:
            return pd.DataFrame(columns=cols)
        if not (
            isinstance(feature_log, (list, pd.DataFrame, pd.Series))
            or (HAS_POLARS and isinstance(feature_log, (pl.DataFrame, pl.Series)))
            or (HAS_NUMPY and isinstance(feature_log, np.ndarray))
        ):
            raise ValueError(f"Type '{type(feature_log)}' not accepted")
        if isinstance(feature_log, list) or (
            HAS_NUMPY and isinstance(feature_log, np.ndarray)
        ):
            if all(isinstance(item, dict) for item in feature_log):
                return pd.DataFrame(feature_log)
            Engine._validate_logging_list(feature_log, cols)
            return pd.DataFrame(feature_log, columns=cols)
        if HAS_POLARS and isinstance(feature_log, pl.DataFrame):
            return feature_log.clone().to_pandas()
        if isinstance(feature_log, pd.DataFrame):
            return feature_log.copy(deep=False).reset_index(drop=True)
        if HAS_POLARS and isinstance(feature_log, pl.Series):
            return feature_log.to_frame().to_pandas().reset_index(drop=True)
        if isinstance(feature_log, pd.Series):
            return feature_log.to_frame().reset_index(drop=True)
        return None

    @staticmethod
    def _validate_logging_list(
        feature_log: list[list[Any]] | list[Any], cols: list[str]
    ) -> None:
        """Function to check if the provided list has the same number of features/labels as expected.

        If the feature_log provided is a list then it is considered as a single feature (column).

        Parameters:
            feature_log `Union[List[List[Any]], List[Any]]`: List of features/labels provided for logging.
            cols `List[str]`: List of expected features in the logging dataframe.
        """
        if isinstance(feature_log[0], list) or (
            HAS_NUMPY and isinstance(feature_log[0], np.ndarray)
        ):
            provided_len = len(feature_log[0])
        else:
            provided_len = 1
        assert provided_len == len(cols), (
            f"Expecting {len(cols)} features/labels but {provided_len} provided."
        )

    @staticmethod
    def get_logging_metadata(
        size=None,
        td_col_name: str | None = None,
        time_col_name: str | None = None,
        model_col_name: str | None = None,
        training_dataset_version: int | None = None,
        model_name: str | None = None,
        model_version: int | None = None,
    ):
        batch = True
        if size is None:
            size = 1
            batch = False

        now = datetime.now()
        metadata = {
            td_col_name: [
                training_dataset_version if training_dataset_version else pd.NA
                for _ in range(size)
            ],
            model_col_name: [model_name for _ in range(size)],
            constants.FEATURE_LOGGING.MODEL_VERSION_COLUMN_NAME: [
                str(model_version) for _ in range(size)
            ],
            time_col_name: pd.Series([now for _ in range(size)]),
            constants.FEATURE_LOGGING.LOG_ID_COLUMN_NAME: [
                str(uuid.uuid4()) for _ in range(size)
            ],
        }

        if not batch:
            for k, v in metadata.items():
                metadata[k] = v[0]
        return metadata

    def get_feature_logging_df(
        self,
        logging_data: pd.DataFrame
        | pl.DataFrame
        | list[list[Any]]
        | list[dict[str, Any]]
        | np.ndarray = None,
        logging_feature_group_features: list[feature.Feature] = None,
        logging_feature_group_feature_names: list[str] = None,
        logging_features: list[str] = None,
        transformed_features: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list[Any]]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[Any],
        ]
        | None = None,
        untransformed_features: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        predictions: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        serving_keys: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        helper_columns: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        request_parameters: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        event_time: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        request_id: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        extra_logging_features: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            str,
            list[str],
        ]
        | None = None,
        td_col_name: str | None = None,
        time_col_name: str | None = None,
        model_col_name: str | None = None,
        training_dataset_version: int | None = None,
        model_name: str | None = None,
        model_version: int | None = None,
    ) -> pd.DataFrame:
        """Function that combines all the logging components into a single pandas dataframe that can be logged to the feature store.

        The function

        Parameters:
            logging_data : Feature log provided by the user.
            logging_feature_group_features : List of features in the logging feature group.
            logging_feature_group_feature_names: `List[str]`. The names of the logging feature group features.
            logging_features: `List[str]`: The names of the logging features, this excludes the names of all metadata columns.
            transformed_features: Tuple of transformed features to be logged, transformed feature names and a log component name (a constant named "transformed_features").
            untransformed_features : Tuple of untransformed features, feature names and log component name (a constant named "untransformed_features").
            predictions : Tuple of predictions, prediction names and log component name (a constant named "predictions").
            serving_keys : Tuple of serving keys, serving key names and log component name (a constant named "serving_keys").
            helper_columns : Tuple of helper columns, helper column names and log component name (a constant named "helper_columns").
            request_parameters : Tuple of request parameters, request parameter names and log component name (a constant named "request_parameters").
            event_time : Tuple of event time, event time column name and log component name (a constant named "event_time").
            request_id : Tuple of request id, request id column name and log component name (a constant named "request_id").
            extra_logging_features : Tuple of extra logging features, extra logging feature names and log component name (a constant named "extra_logging_features").
            td_col_name : Name of the training dataset version column.
            time_col_name : Name of the event time column.
            model_col_name : Name of the model column.
            training_dataset_version : Version of the training dataset.
            hsml_model : Name of the model.

        Returns:
            `pd.DataFrame`: A pandas dataframe with all the logging components.
            `List[str]`: Names of additional logging features passed in the Logging Dataframe.
            `List[str]`: Names of missing logging features passed in the Logging Dataframe.
        """
        if logging_data is not None:
            try:
                logging_df = Engine._convert_feature_log_to_df(
                    logging_data, logging_features
                )
            except AssertionError as e:
                raise FeatureStoreException(
                    f"Error logging data `{constants.FEATURE_LOGGING.LOGGING_DATA}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.LOGGING_DATA}` to ensure that it has the following features : {logging_features}."
                ) from e
        else:
            logging_df = None

        # Iterate through all logging components validate them and collect them into a single dataframe.
        for data, feature_names, log_component_name in [
            transformed_features,
            untransformed_features,
            predictions,
            serving_keys,
            helper_columns,
            request_id,
            request_parameters,
            event_time,
            extra_logging_features,
        ]:
            try:
                df = (
                    Engine._convert_feature_log_to_df(data, feature_names)
                    if data is not None or feature_names
                    else None
                )
            except AssertionError as e:
                raise FeatureStoreException(
                    f"Error logging data `{log_component_name}` do not have all required features. Please check the `{log_component_name}` to ensure that it has the following features : {feature_names}."
                ) from e

            if df is None or df.empty:
                continue
            if logging_df is None or logging_df.empty:
                logging_df = df
            # If one of the logging components has only one row and the other has multiple rows, we repeat the single row to match the length of the other component.
            elif len(df) == 1 and len(logging_df) > 1:
                for col in df.columns:
                    if col not in logging_df.columns:
                        logging_df[col] = (
                            df[col]
                            .loc[df.index.repeat(len(logging_df))]
                            .reset_index(drop=True)
                        )
            elif len(df) != len(logging_df):
                raise FeatureStoreException(
                    f"Length of `{log_component_name}` provided do not match other arguments. Please check the logging data to make sure that all arguments have the same length."
                )
            else:
                for col in df.columns:
                    if col not in logging_df.columns:
                        logging_df[col] = df[col]
                    elif not df[col].isna().all():
                        # If the column already exists in the logging dataframe and the new column has some non-null values, we overwrite the existing column.
                        # Higher precedence is given if the user explicitly passed the logging component.
                        logging_df[col] = df[col]

        # Rename prediction columns
        _, predictions_feature_names, _ = predictions
        if predictions_feature_names:
            for feature_name in predictions_feature_names:
                logging_df = logging_df.rename(
                    columns={
                        feature_name: constants.FEATURE_LOGGING.PREFIX_PREDICTIONS
                        + feature_name
                    }
                )

        # Creating a json column for request parameters
        request_parameter_data, request_parameter_columns, _ = request_parameters
        missing_request_parameter_columns = set()
        if request_parameter_columns and (
            constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
            not in logging_df.columns
        ):
            request_parameter_data = Engine._convert_feature_log_to_df(
                request_parameter_data, request_parameter_columns
            )

            # If there are any request parameter columns that are not part of the request parameter data but are present in the logging dataframe, we add them to the request parameter data.
            for col in request_parameter_columns:
                if (
                    request_parameter_data.empty
                    or col not in request_parameter_data.columns
                    or request_parameter_data[col].isna().all()
                ):
                    if col in logging_df.columns:
                        request_parameter_data[col] = logging_df[col]
                    else:
                        request_parameter_data[col] = None
                        missing_request_parameter_columns.add(col)

            logging_df[constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME] = (
                constants.FEATURE_LOGGING.EMPTY_REQUEST_PARAMETER_COLUMN_VALUE
                if request_parameter_data.empty
                else request_parameter_data.apply(
                    lambda x: json.dumps(x.to_dict()), axis=1
                )
            )

            # Drop any request parameter columns that are not explicitly part of the logging feature group so that they are not logged.
            logging_df.drop(
                [
                    feature
                    for feature in request_parameter_data.columns
                    if feature in logging_df
                    and feature not in logging_feature_group_feature_names
                ],
                axis=1,
                inplace=True,
            )

        # Add meta data columns
        logging_metadata = Engine.get_logging_metadata(
            size=len(logging_df),
            td_col_name=td_col_name,
            time_col_name=time_col_name,
            model_col_name=model_col_name,
            training_dataset_version=training_dataset_version,
            model_name=model_name,
            model_version=model_version,
        )

        for k, v in logging_metadata.items():
            logging_df[k] = pd.Series(v)

        # Find any missing columns in the logging dataframe and set them to None
        # Find any additional columns in the logging dataframe that are not in the logging feature group and ignore them.
        missing_logging_features = (
            set(logging_feature_group_feature_names)
            .difference(set(logging_df.columns))
            .union(missing_request_parameter_columns)
        )
        additional_logging_features = set(logging_df.columns).difference(
            set(logging_feature_group_feature_names)
        )

        # Cast columns to the correct types
        for f in logging_feature_group_features:
            if f.name in logging_df.columns:
                logging_df[f.name] = cast_column_to_offline_type(
                    logging_df[f.name], f.type
                )

        if missing_logging_features:
            # Set missing columns to None
            for col in missing_logging_features:
                logging_df[col] = None

        missing_event_time_feature = [
            feature.name
            for feature in logging_feature_group_features
            if (feature.type == "timestamp" or feature.type == "date")
            and feature.name in missing_logging_features
        ]

        # Replace NaT with None for event time feature as pd.NaT is not serializable by fastavro.
        if missing_event_time_feature:
            logging_df[missing_event_time_feature] = logging_df[
                missing_event_time_feature
            ].replace({pd.NaT: None})

        return (
            logging_df[logging_feature_group_feature_names],
            additional_logging_features,
            missing_logging_features,
        )

    def get_feature_logging_list(
        self,
        logging_data: pd.DataFrame
        | pl.DataFrame
        | list[dict[str, Any]]
        | list[list]
        | np.ndarray = None,
        logging_feature_group_features: list[feature.Feature] = None,
        logging_feature_group_feature_names: list[str] = None,
        logging_features: list[str] = None,
        transformed_features: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        untransformed_features: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        predictions: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        serving_keys: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        helper_columns: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        request_parameters: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        request_id: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        event_time: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        extra_logging_features: tuple[
            pd.DataFrame
            | pl.DataFrame
            | list[list]
            | list[dict[str, Any]]
            | np.ndarray,
            list[str],
            str,
        ]
        | None = None,
        td_col_name: str | None = None,
        time_col_name: str | None = None,
        model_col_name: str | None = None,
        training_dataset_version: int | None = None,
        model_name: str | None = None,
        model_version: int | None = None,
    ) -> list[dict[str, Any]]:
        """Function that combines all the logging components into a single list of dictionaries that can be logged to send to the inference logger side cart for writing to the feature store.

        Parameters:
            logging_data : Feature log provided by the user.
            logging_feature_group_features : List of features in the logging feature group.
            logging_feature_group_feature_names: `List[str]`. The names of the logging feature group features.
            logging_features: `List[str]`: The names of the logging features, this excludes the names of all metadata columns.
            transformed_features: Tuple of transformed features to be logged, transformed feature names and a log component name (a constant named "transformed_features").
            untransformed_features : Tuple of untransformed features, feature names and log component name (a constant named "untransformed_features").
            predictions : Tuple of predictions, prediction names and log component name (a constant named "predictions").
            serving_keys : Tuple of serving keys, serving key names and log component name (a constant named "serving_keys").
            helper_columns : Tuple of helper columns, helper column names and log component name (a constant named "helper_columns").
            request_parameters : Tuple of request parameters, request parameter names and log component name (a constant named "request_parameters").
            event_time : Tuple of event time, event time column name and log component name (a constant named "event_time").
            request_id : Tuple of request id, request id column name and log component name (a constant named "request_id").
            extra_logging_features : Tuple of extra logging features, extra logging feature names and log component name
            td_col_name : Name of the training dataset version column.
            time_col_name : Name of the event time column.
            model_col_name : Name of the model column.
            training_dataset_version : Version of the training dataset.
            model_name : Name of the model.
            model_version : Version of the model.

        Returns:
            `List[Dict[str, Any]]`: A list of dictionaries with all the logging components
        """
        _, label_columns, _ = predictions
        # If any of the logging components is a dataframe, we use the get_feature_logging_df function to get a dataframe and then convert it to a list of dictionaries.
        if any(
            (HAS_PANDAS and isinstance(data, pd.DataFrame))
            or (HAS_POLARS and isinstance(data, pl.DataFrame))
            for data, _, _ in [
                (logging_data, logging_feature_group_feature_names, "logging_data"),
                transformed_features,
                untransformed_features,
                predictions,
                serving_keys,
                helper_columns,
                request_parameters,
                event_time,
                extra_logging_features,
                request_id,
            ]
        ):
            logging_data, additional_logging_features, missing_logging_features = (
                self.get_feature_logging_df(
                    logging_data=logging_data,
                    logging_feature_group_features=logging_feature_group_features,
                    logging_feature_group_feature_names=logging_feature_group_feature_names,
                    logging_features=logging_features,
                    transformed_features=transformed_features,
                    untransformed_features=untransformed_features,
                    predictions=predictions,
                    serving_keys=serving_keys,
                    helper_columns=helper_columns,
                    request_parameters=request_parameters,
                    event_time=event_time,
                    request_id=request_id,
                    extra_logging_features=extra_logging_features,
                    td_col_name=td_col_name,
                    time_col_name=time_col_name,
                    model_col_name=model_col_name,
                    training_dataset_version=training_dataset_version,
                    model_name=model_name,
                    model_version=model_version,
                )
            )

            return (
                logging_data.to_dict(orient="records"),
                additional_logging_features,
                missing_logging_features,
            )

        log_vectors: list[dict[Any, str]] = None
        all_missing_columns = set()
        for data, feature_names, log_component_name in [
            (logging_data, logging_features, constants.FEATURE_LOGGING.LOGGING_DATA),
            transformed_features,
            untransformed_features,
            predictions,
            serving_keys,
            helper_columns,
            request_parameters,
            event_time,
            extra_logging_features,
            request_id,
        ]:
            if not data:
                if log_component_name == constants.FEATURE_LOGGING.PREDICTIONS:
                    feature_names = [
                        constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + name
                        for name in feature_names
                    ]
                all_missing_columns.update(set(feature_names))
                continue
            try:
                # Handle cases where logging data is a list or a dictionary.
                if isinstance(data, (list)):
                    # If not all elements in the row are list or dict, then the data can be single row or a single column.
                    if not all(isinstance(element, (list, dict)) for element in data):
                        # If length of data is same as feature names, then it should be a single row else it is a single column.
                        if len(data) == len(feature_names):
                            data = [data]
                        else:
                            data = [[item] for item in data]
                # If data is dictionary, then it should be a dictionary representing a single row
                elif isinstance(data, dict):
                    data = [data]
                if not all(isinstance(row, dict) for row in data):
                    Engine._validate_logging_list(data, feature_names)
                else:
                    # If all elements in the row are dictionary, then we check if all required features are present in each row and set the missing features to None.
                    for row in data:
                        column_names = (
                            set(row.keys())
                            if log_component_name
                            != constants.FEATURE_LOGGING.LOGGING_DATA
                            else {
                                col
                                if col not in label_columns
                                else constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + col
                                for col in row
                            }
                        )
                        missing_columns = set(feature_names).difference(
                            set(column_names)
                        )
                        all_missing_columns.update(missing_columns)
                        additional_logging_features = set(column_names).difference(
                            feature_names
                        )
                        if missing_columns:
                            _logger.info(
                                f"The following columns : `{'`, `'.join(missing_columns)}` are missing in the logging data provided `{log_component_name}`. Setting them to None."
                            )
                            for col in missing_columns:
                                row[col] = None
                        if additional_logging_features:
                            _logger.info(
                                f"The following columns : `{'`, `'.join(additional_logging_features)}` are additional columns the logging data provided `{log_component_name}` and is not present in the logging feature groups. They will be ignored."
                            )
                            if (
                                log_component_name
                                != constants.FEATURE_LOGGING.REQUEST_PARAMETERS
                            ):  # We do not remove additional request parameter columns here as they are handled later.
                                for col in additional_logging_features:
                                    row.pop(col)

            except AssertionError as e:
                raise FeatureStoreException(
                    f"Error logging data `{log_component_name}` do not have all required features. Please check the `{log_component_name}` to ensure that it has the following features : {feature_names}."
                ) from e

            if log_vectors is None:
                log_vectors = [
                    dict(zip(feature_names, row)) if not isinstance(row, dict) else row
                    for row in data
                ]
            # If one of the logging components has only one row and the other has multiple rows, we repeat the single row to match the length of the other component.
            elif len(data) == 1:
                for log_vector in log_vectors:
                    log_vector.update(
                        dict(zip(feature_names, data[0]))
                        if not isinstance(data[0], dict)
                        else data[0]
                    )
            elif len(data) != len(log_vectors):
                if len(log_vectors) != len(data):
                    raise FeatureStoreException(
                        f"Length of `{log_component_name}` provided do not match other arguments. Please check the logging data to make sure that all arguments have the same length."
                    )
            else:
                for log_vector, row in zip(log_vectors, data):
                    log_vector.update(
                        dict(zip(feature_names, row))
                        if not isinstance(row, dict)
                        else row
                    )

        # rename prediction columns
        _, predictions_feature_names, _ = predictions
        if predictions_feature_names:
            for log_vector in log_vectors:
                for feature_name in predictions_feature_names:
                    if feature_name in log_vector:
                        log_vector[
                            constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + feature_name
                        ] = log_vector.pop(feature_name)

        # Create a json column for request parameters
        request_parameter_data, request_parameter_names, _ = request_parameters
        if request_parameter_names:
            # Get any request parameters that the user passed explicitly.
            if request_parameter_data is not None:
                request_parameter_data = [
                    dict(zip(request_parameter_names, row))
                    if not isinstance(row, dict)
                    else row
                    for row in request_parameter_data
                ]
            else:
                request_parameter_data = [{} for _ in range(len(log_vectors))]

            # Iterate through the log vectors and try to parse request parameters from the log vector if they are not explicitly passed by the user.
            for log_vector, passed_rp_data in zip(log_vectors, request_parameter_data):
                for col in request_parameter_names:
                    if col not in passed_rp_data and col in log_vector:
                        passed_rp_data[col] = log_vector[col]
                        if col not in logging_feature_group_feature_names:
                            # If the request parameter column is not part of the logging feature group, we remove it from the log vector so that it is not logged as a separate column.
                            log_vector.pop(col)

                if (
                    constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
                    not in log_vector
                    and passed_rp_data
                ):
                    log_vector[
                        constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
                    ] = json.dumps(passed_rp_data)
                else:
                    log_vector[
                        constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
                    ] = constants.FEATURE_LOGGING.EMPTY_REQUEST_PARAMETER_COLUMN_VALUE

        # get metadata
        for row in log_vectors:
            row.update(
                Engine.get_logging_metadata(
                    td_col_name=td_col_name,
                    time_col_name=time_col_name,
                    model_col_name=model_col_name,
                    training_dataset_version=training_dataset_version,
                    model_name=model_name,
                    model_version=model_version,
                )
            )

        # Set missing columns any missing columns to None.
        for row in log_vectors:
            for col in all_missing_columns:
                if col not in row:
                    row[col] = None

        # Additional and missing columns are null in this case since we would have thrown expections for these cases.
        return log_vectors, None, None

    @staticmethod
    def read_feature_log(query, time_col):
        df = query.read()
        return df.drop(["log_id", time_col], axis=1)

    def check_supported_dataframe(self, dataframe: Any) -> bool:
        """Check if a dataframe is supported by the engine.

        Both Pandas and Polars dataframes are supported in the Python Engine.

        Parameters:
            dataframe `Any`: A dataframe to check.

        Returns:
            `bool`: True if the dataframe is supported, False otherwise.
        """
        if (HAS_POLARS and isinstance(dataframe, pl.DataFrame)) or (
            HAS_PANDAS and isinstance(dataframe, pd.DataFrame)
        ):
            return True
        return None

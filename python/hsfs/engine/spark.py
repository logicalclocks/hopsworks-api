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
import os
import re
import uuid
import warnings
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, TypeVar, Union


if TYPE_CHECKING:
    import great_expectations
    from pyspark.rdd import RDD
    from pyspark.sql import DataFrame
    from python.hsfs.constructor import hudi_feature_group_alias

import pandas as pd
import tzlocal
from hopsworks_common import constants
from hopsworks_common.core.constants import HAS_NUMPY, HAS_PANDAS
from hsfs.constructor import query

# in case importing in %%local
from hsfs.core.vector_db_client import VectorDbClient


if HAS_NUMPY:
    import numpy as np


try:
    import pyspark
    from pyspark import SparkFiles
    from pyspark.rdd import RDD
    from pyspark.sql import DataFrame, SparkSession, SQLContext, Window
    from pyspark.sql.avro.functions import from_avro, to_avro
    from pyspark.sql.functions import (
        array,
        col,
        concat,
        count,
        from_json,
        lit,
        monotonically_increasing_id,
        regexp_replace,
        row_number,
        struct,
        udf,
    )
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        ByteType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    if pd.__version__ >= "2.0.0" and pyspark.__version__ < "3.2.3":

        def iteritems(self):
            return self.items()

        pd.DataFrame.iteritems = iteritems
except ImportError:
    pass

import logging

from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.util import generate_fully_qualified_feature_name
from hsfs import (
    feature,
    feature_view,
    training_dataset,
    training_dataset_feature,
    transformation_function,
    util,
)
from hsfs import feature_group as fg_mod
from hsfs.core import (
    dataset_api,
    delta_engine,
    hudi_engine,
    kafka_engine,
    transformation_function_engine,
)
from hsfs.core.constants import HAS_AVRO, HAS_GREAT_EXPECTATIONS
from hsfs.core.feature_logging import LoggingMetaData
from hsfs.decorators import uses_great_expectations
from hsfs.storage_connector import StorageConnector
from hsfs.training_dataset_split import TrainingDatasetSplit


if HAS_GREAT_EXPECTATIONS:
    import great_expectations

if HAS_AVRO:
    import avro

_logger = logging.getLogger(__name__)


class Engine:
    HIVE_FORMAT = "hive"
    KAFKA_FORMAT = "kafka"

    APPEND = "append"
    OVERWRITE = "overwrite"

    def __init__(self):
        self._spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        # self._spark_context.setLogLevel("DEBUG")
        self._jvm = self._spark_context._jvm

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
        self._spark_session.conf.set("spark.sql.session.timeZone", "UTC")
        self._dataset_api = dataset_api.DatasetApi()

    def sql(
        self,
        sql_query,
        feature_store,
        connector,
        dataframe_type,
        read_options,
        schema=None,
    ):
        if not connector:
            result_df = self._sql_offline(sql_query, feature_store)
        else:
            result_df = connector.read(sql_query, None, read_options, None)

        self.set_job_group("", "")
        return self._return_dataframe_type(result_df, dataframe_type)

    def is_flyingduck_query_supported(self, query, read_options=None):
        return False  # we do not support flyingduck on pyspark clients

    def _sql_offline(self, sql_query, feature_store):
        # set feature store
        self._spark_session.sql("USE {}".format(feature_store))
        return self._spark_session.sql(sql_query)

    def show(self, sql_query, feature_store, n, online_conn, read_options=None):
        return self.sql(
            sql_query, feature_store, online_conn, "default", read_options
        ).show(n)

    def read_vector_db(
        self,
        feature_group: fg_mod.FeatureGroup,
        n: int = None,
        dataframe_type: str = "default",
    ) -> Union[
        pd.DataFrame, np.ndarray, List[List[Any]], TypeVar("pyspark.sql.DataFrame")
    ]:
        results = VectorDbClient.read_feature_group(feature_group, n)
        feature_names = [f.name for f in feature_group.features]
        dataframe_type = dataframe_type.lower()
        if dataframe_type in ["default", "spark"]:
            if len(results) == 0:
                return self._spark_session.createDataFrame(
                    self._spark_session.sparkContext.emptyRDD(), StructType()
                )
            else:
                return self._spark_session.createDataFrame(results, feature_names)
        else:
            df = pd.DataFrame(results, columns=feature_names, index=None)
            return self._return_dataframe_type(df, dataframe_type)

    def set_job_group(self, group_id, description):
        self._spark_session.sparkContext.setJobGroup(group_id, description)

    def register_external_temporary_table(self, external_fg, alias):
        if not isinstance(external_fg, fg_mod.SpineGroup):
            external_dataset = external_fg.storage_connector.read(
                external_fg.data_source.query,
                external_fg.data_format,
                external_fg.options,
                external_fg.storage_connector._get_path(
                    external_fg.data_source.path
                ),  # cant rely on location since this method can be used before FG is saved
            )
        else:
            external_dataset = external_fg.dataframe

        external_dataset.createOrReplaceTempView(alias)
        return external_dataset

    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        hudi_engine_instance = hudi_engine.HudiEngine(
            feature_store_id,
            feature_store_name,
            hudi_fg_alias.feature_group,
            self._spark_context,
            self._spark_session,
        )

        hudi_engine_instance.register_temporary_table(
            hudi_fg_alias,
            read_options,
        )

    def register_delta_temporary_table(
        self,
        delta_fg_alias: hudi_feature_group_alias.HudiFeatureGroupAlias,
        feature_store_id: int,
        feature_store_name: str,
        read_options: Optional[Dict[str, Any]],
        is_cdc_query: bool = False,
    ):
        delta_engine_instance = delta_engine.DeltaEngine(
            feature_store_id=feature_store_id,
            feature_store_name=feature_store_name,
            feature_group=delta_fg_alias.feature_group,
            spark_session=self._spark_session,
            spark_context=self._spark_context,
        )

        delta_engine_instance.register_temporary_table(
            delta_fg_alias=delta_fg_alias,
            read_options=read_options,
            is_cdc_query=is_cdc_query,
        )

    def _return_dataframe_type(self, dataframe, dataframe_type):
        if dataframe_type.lower() in ["default", "spark"]:
            return dataframe

        # Converting to pandas dataframe if return type is not spark
        if isinstance(dataframe, DataFrame):
            dataframe = dataframe.toPandas()

        if dataframe_type.lower() == "pandas":
            return dataframe
        if dataframe_type.lower() == "numpy":
            return dataframe.values
        if dataframe_type.lower() == "python":
            return dataframe.values.tolist()

        raise TypeError(
            "Dataframe type `{}` not supported on this platform.".format(dataframe_type)
        )

    def convert_to_default_dataframe(self, dataframe, column_names=None):
        if isinstance(dataframe, list):
            dataframe = self.convert_list_to_spark_dataframe(dataframe, column_names)
        elif HAS_NUMPY and isinstance(dataframe, np.ndarray):
            dataframe = self.convert_numpy_to_spark_dataframe(dataframe, column_names)
        elif HAS_PANDAS and isinstance(dataframe, pd.DataFrame):
            dataframe = self.convert_pandas_to_spark_dataframe(dataframe)
        elif isinstance(dataframe, RDD):
            dataframe = dataframe.toDF()

        if isinstance(dataframe, DataFrame):
            upper_case_features = [
                c for c in dataframe.columns if util.contains_uppercase(c)
            ]
            space_features = [
                c for c in dataframe.columns if util.contains_whitespace(c)
            ]
            if len(upper_case_features) > 0:
                warnings.warn(
                    "The ingested dataframe contains upper case letters in feature names: `{}`. "
                    "Feature names are sanitized to lower case in the feature store.".format(
                        upper_case_features
                    ),
                    util.FeatureGroupWarning,
                    stacklevel=1,
                )
            if len(space_features) > 0:
                warnings.warn(
                    "The ingested dataframe contains feature names with spaces: `{}`. "
                    "Feature names are sanitized to use underscore '_' in the feature store.".format(
                        space_features
                    ),
                    util.FeatureGroupWarning,
                    stacklevel=1,
                )

            lowercase_dataframe = dataframe.select(
                [col(x).alias(util.autofix_feature_name(x)) for x in dataframe.columns]
            )
            # for streaming dataframes this will be handled in DeltaStreamerTransformer.java class
            if not lowercase_dataframe.isStreaming:
                nullable_schema = copy.deepcopy(lowercase_dataframe.schema)
                for struct_field in nullable_schema:
                    struct_field.nullable = True
                lowercase_dataframe = self._spark_session.createDataFrame(
                    lowercase_dataframe.rdd, nullable_schema
                )

            return lowercase_dataframe
        if dataframe == "spine":
            return None

        raise TypeError(
            "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, "
            "pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: {}".format(
                type(dataframe)
            )
        )

    @staticmethod
    def utc_disguised_as_local(dt):
        local_tz = tzlocal.get_localzone()
        utc = timezone.utc
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=utc)
        return dt.astimezone(utc).replace(tzinfo=local_tz)

    def convert_list_to_spark_dataframe(self, dataframe, column_names=None):
        if HAS_NUMPY:
            return self.convert_numpy_to_spark_dataframe(
                np.array(dataframe), column_names=column_names
            )
        try:
            dataframe[0][0]
        except TypeError:
            raise TypeError(
                "Cannot convert a list that has less than two dimensions to a dataframe."
            ) from None
        ok = False
        try:
            dataframe[0][0][0]
        except TypeError:
            ok = True
        if not ok:
            raise TypeError(
                "Cannot convert a list that has more than two dimensions to a dataframe."
            ) from None
        num_cols = len(dataframe[0])
        if column_names and num_cols != len(column_names):
            raise ValueError(
                "The number of column names provided does not match the number of columns in the dataframe. "
                "Number of columns in dataframe: {}. Number of column names provided: {}.".format(
                    num_cols, len(column_names)
                )
            )
        if HAS_PANDAS:
            dataframe_dict = {}
            for n_col in range(num_cols):
                c = "col_" + str(n_col) if not column_names else column_names[n_col]
                dataframe_dict[c] = [dataframe[i][n_col] for i in range(len(dataframe))]
            return self.convert_pandas_to_spark_dataframe(pd.DataFrame(dataframe_dict))
        for i in range(len(dataframe)):
            dataframe[i] = [
                self.utc_disguised_as_local(d) if isinstance(d, datetime) else d
                for d in dataframe[i]
            ]
        return self._spark_session.createDataFrame(
            dataframe,
            [
                "col_" + str(n) if column_names is None else column_names[n]
                for n in range(num_cols)
            ],
        )

    def convert_numpy_to_spark_dataframe(self, dataframe, column_names=None):
        is_list_of_dict = (
            all(isinstance(row, dict) for row in dataframe)
            if len(dataframe) > 0
            else False
        )
        if dataframe.ndim != 2 and not is_list_of_dict:
            raise TypeError(
                "Cannot convert numpy array that do not have two dimensions to a dataframe. "
                "The number of dimensions are: {}".format(dataframe.ndim)
            )
        num_cols = dataframe.shape[1] if not is_list_of_dict else len(dataframe[0])
        if HAS_PANDAS:
            dataframe_dict = {}
            if not is_list_of_dict:
                if column_names:
                    assert (
                        num_cols == len(column_names)
                    ), f"Expecting {len(column_names)} features/labels but {num_cols} provided."
                for n_col in range(num_cols):
                    c = (
                        "col_" + str(n_col)
                        if column_names is None
                        else column_names[n_col]
                    )
                    dataframe_dict[c] = dataframe[:, n_col]
            else:
                dataframe_dict = dataframe.tolist()
            return self.convert_pandas_to_spark_dataframe(pd.DataFrame(dataframe_dict))
        # convert timestamps to current timezone
        for n_col in range(num_cols):
            if dataframe[:, n_col].dtype == np.dtype("datetime64[ns]"):
                # set the timezone to the client's timezone because that is
                # what spark expects.
                dataframe[:, n_col] = np.array(
                    [self.utc_disguised_as_local(d.item()) for d in dataframe[:, n_col]]
                )
        return self._spark_session.createDataFrame(
            dataframe.tolist(),
            [
                "col_" + str(n) if column_names is None else column_names[n]
                for n in range(num_cols)
            ],
        )

    def convert_pandas_to_spark_dataframe(self, dataframe):
        # convert timestamps to current timezone
        local_tz = tzlocal.get_localzone()
        # make shallow copy so the original df does not get changed
        dataframe_copy = dataframe.copy(deep=False)
        for c in dataframe_copy.columns:
            if isinstance(
                dataframe_copy[c].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype
            ):
                # convert to utc timestamp
                dataframe_copy[c] = dataframe_copy[c].dt.tz_convert(None)
            if HAS_NUMPY and dataframe_copy[c].dtype == np.dtype("datetime64[ns]"):
                # set the timezone to the client's timezone because that is
                # what spark expects.
                dataframe_copy[c] = dataframe_copy[c].dt.tz_localize(
                    str(local_tz), ambiguous="infer", nonexistent="shift_forward"
                )
        return self._spark_session.createDataFrame(dataframe_copy)

    def _check_duplicate_records(self, dataframe, feature_group):
        """
        Check for duplicate records within primary_key, event_time and partition_key columns.

        Raises FeatureStoreException if duplicates are found.

        Parameters:
        -----------
        dataframe : pyspark.sql.DataFrame
            The Spark DataFrame to check for duplicates
        feature_group : FeatureGroup
            The feature group instance containing primary_key, event_time and partition_key
        """
        # Get the key columns to check (primary_key + partition_key)
        key_columns = list(feature_group.primary_key)

        if not key_columns:
            # No keys to check, skip validation
            return

        if feature_group.event_time:
            key_columns.append(feature_group.event_time)

        if feature_group.partition_key:
            key_columns.extend(feature_group.partition_key)

        # Verify all key columns exist in the dataset
        dataframe_columns = dataframe.columns
        missing_columns = [
            col_name for col_name in key_columns if col_name not in dataframe_columns
        ]
        if missing_columns:
            raise FeatureStoreException(
                f"Key columns {missing_columns} are missing from the dataset. "
                f"Available columns: {dataframe_columns}"
            )

        # Check for duplicates using Spark groupBy and count
        # Group by key columns and count occurrences
        grouped = dataframe.groupBy(*key_columns).agg(count("*").alias("count"))

        # Filter groups with count > 1 (duplicates)
        duplicate_groups = grouped.filter(col("count") > 1)

        # Count the number of duplicate groups
        duplicate_count = duplicate_groups.count()

        if duplicate_count > 0:
            # Get total number of duplicate rows (sum of counts - 1 for each duplicate group)
            # Since count includes the first occurrence, duplicates = count - 1 per group
            duplicate_rows_data = duplicate_groups.select(
                col("count").cast("long")
            ).collect()
            total_duplicate_rows = (
                sum(row["count"] for row in duplicate_rows_data) - duplicate_count
            )

            # Get sample duplicate records for error message
            # Take first 10 duplicate groups and get their key values
            sample_groups = duplicate_groups.limit(10).collect()

            # Build sample string showing the duplicate key combinations
            sample_rows = []
            for row in sample_groups:
                row_dict = {}
                for col_name in key_columns:
                    row_dict[col_name] = row[col_name]
                row_dict["count"] = row["count"]
                sample_rows.append(str(row_dict))

            sample_str = "\n".join(sample_rows)

            raise FeatureStoreException(
                FeatureStoreException.DUPLICATE_RECORD_ERROR_MESSAGE
                + f"\nDataset contains {total_duplicate_rows} duplicate record(s) within "
                f"primary_key ({feature_group.primary_key}) and "
                f"partition_key ({feature_group.partition_key}). "
                f"Found {duplicate_count} duplicate group(s). "
                f"Sample duplicate key combinations:\n{sample_str}"
            )

    def save_dataframe(
        self,
        feature_group,
        dataframe,
        operation,
        online_enabled,
        storage,
        offline_write_options,
        online_write_options,
        validation_id=None,
    ):
        try:
            if (
                # Only `FeatureGroup class has time_travel_format property
                isinstance(feature_group, fg_mod.FeatureGroup)
                and feature_group.time_travel_format == "DELTA"
            ):
                self._check_duplicate_records(dataframe, feature_group)
                _logger.debug(
                    "No duplicate records found. Proceeding with Delta write."
                )

            if (
                isinstance(feature_group, fg_mod.ExternalFeatureGroup)
                and feature_group.online_enabled
            ) or feature_group.stream:
                self._save_online_dataframe(
                    feature_group, dataframe, online_write_options
                )
            else:
                if storage == "offline" or not online_enabled:
                    self._save_offline_dataframe(
                        feature_group,
                        dataframe,
                        operation,
                        offline_write_options,
                        validation_id,
                    )
                elif storage == "online":
                    self._save_online_dataframe(
                        feature_group, dataframe, online_write_options
                    )
                elif online_enabled and storage is None:
                    self._save_offline_dataframe(
                        feature_group,
                        dataframe,
                        operation,
                        offline_write_options,
                    )
                    self._save_online_dataframe(
                        feature_group, dataframe, online_write_options
                    )
        except Exception as e:
            raise FeatureStoreException(e).with_traceback(e.__traceback__) from e

    def save_stream_dataframe(
        self,
        feature_group: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup],
        dataframe,
        query_name,
        output_mode,
        await_termination: bool,
        timeout,
        checkpoint_dir: Optional[str],
        write_options: Optional[Dict[str, Any]],
    ):
        write_options = kafka_engine.get_kafka_config(
            feature_group.feature_store_id, write_options, engine="spark"
        )
        serialized_df = self._serialize_to_avro(feature_group, dataframe)

        if query_name is None:
            query_name = (
                f"insert_stream_{feature_group.feature_store.project_id}_{feature_group._id}"
                f"_{feature_group.name}_{feature_group.version}_onlinefs"
            )

        query = (
            serialized_df.withColumn("headers", self._get_headers(feature_group))
            .writeStream.outputMode(output_mode)
            .format(self.KAFKA_FORMAT)
            .option(
                "checkpointLocation",
                "/Projects/"
                + client.get_instance()._project_name
                + "/Resources/"
                + query_name
                + "-checkpoint"
                if checkpoint_dir is None
                else checkpoint_dir,
            )
            .options(**write_options)
            .option("topic", feature_group._online_topic_name)
            .queryName(query_name)
            .start()
        )

        if await_termination:
            query.awaitTermination(timeout)

            # wait for online ingestion
            if feature_group.online_enabled and write_options.get(
                "wait_for_online_ingestion", False
            ):
                feature_group.get_latest_online_ingestion().wait_for_completion(
                    options=write_options.get("online_ingestion_options", {})
                )

        return query

    def _save_offline_dataframe(
        self,
        feature_group,
        dataframe,
        operation,
        write_options,
        validation_id=None,
    ):
        if feature_group.time_travel_format == "HUDI":
            hudi_engine_instance = hudi_engine.HudiEngine(
                feature_group.feature_store_id,
                feature_group.feature_store_name,
                feature_group,
                self._spark_session,
                self._spark_context,
            )

            hudi_engine_instance.save_hudi_fg(
                dataframe, self.APPEND, operation, write_options, validation_id
            )
        elif feature_group.time_travel_format == "DELTA":
            delta_engine_instance = delta_engine.DeltaEngine(
                feature_group.feature_store_id,
                feature_group.feature_store_name,
                feature_group,
                self._spark_session,
                self._spark_context,
            )
            delta_engine_instance.save_delta_fg(dataframe, write_options, validation_id)
        else:
            dataframe.write.format(self.HIVE_FORMAT).mode(self.APPEND).options(
                **write_options
            ).partitionBy(
                feature_group.partition_key if feature_group.partition_key else []
            ).saveAsTable(feature_group._get_table_name())

    def _save_online_dataframe(self, feature_group, dataframe, write_options):
        write_options = kafka_engine.get_kafka_config(
            feature_group.feature_store_id, write_options, engine="spark"
        )

        serialized_df = self._serialize_to_avro(feature_group, dataframe)

        (
            serialized_df.withColumn(
                "headers", self._get_headers(feature_group, dataframe.count())
            )
            .write.format(self.KAFKA_FORMAT)
            .options(**write_options)
            .option("topic", feature_group._online_topic_name)
            .save()
        )

        # wait for online ingestion
        if feature_group.online_enabled and write_options.get(
            "wait_for_online_ingestion", False
        ):
            feature_group.get_latest_online_ingestion().wait_for_completion(
                options=write_options.get("online_ingestion_options", {})
            )

    def _get_headers(
        self,
        feature_group: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup],
        num_entries: Optional[int] = None,
    ) -> array:
        return array(
            *[
                struct(lit(key).alias("key"), lit(value).alias("value"))
                for key, value in kafka_engine.get_headers(
                    feature_group, num_entries
                ).items()
            ]
        )

    def _serialize_to_avro(
        self,
        feature_group: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup],
        dataframe: Union[RDD, DataFrame],
        serialized_column: str = "value",
    ):
        """Encodes all complex type features to binary using their avro type as schema."""
        encoded_dataframe = dataframe.select(
            [
                field["name"]
                if field["name"] not in feature_group.get_complex_features()
                else to_avro(
                    field["name"], feature_group._get_feature_avro_schema(field["name"])
                ).alias(field["name"])
                for field in json.loads(feature_group.avro_schema)["fields"]
            ]
        )

        """Packs all features into named struct to be serialized to single avro/binary
        column. And packs primary key into arry to be serialized for partitioning.
        """
        return encoded_dataframe.select(
            [
                # be aware: primary_key array should always be sorted
                to_avro(
                    concat(
                        *[
                            col(f).cast("string")
                            for f in sorted(feature_group.primary_key)
                        ]
                    )
                ).alias("key"),
                to_avro(
                    struct(
                        [
                            field["name"]
                            for field in json.loads(feature_group.avro_schema)["fields"]
                        ]
                    ),
                    feature_group._get_encoded_avro_schema(),
                ).alias(serialized_column),
            ]
        )

    def _generate_wrapper_record_avro_schema(
        self,
        feature_name: str,
        feature_group: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup],
    ):
        """
        Function to generate a wrapper record avro schema for a struct feature.
        This is required to deserialize a union of null and a struct field in spark, since spark expects the top level avro schema to be a record in this case.

        # Arguments
            feature_name: `str`: The name of the feature to generate the wrapper record avro schema for.
            feature_group: `Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup]`: The feature group object.
        # Returns
            `str`: The wrapper record avro schema.
        """
        return (
            '{"type": "record", "name": "Wrapper", "fields": [{"name": "'
            + feature_name
            + '",  "type": '
            + feature_group._get_feature_avro_schema(feature_name)
            + "}]}"
        )

    def _deserialize_from_avro(
        self,
        feature_group: Union[fg_mod.FeatureGroup, fg_mod.ExternalFeatureGroup],
        dataframe: Union[RDD, DataFrame],
        serialized_column: str = "value",
    ):
        """
        Step 1: Deserializes 'value' column from binary using avro schema.
        """
        decoded_dataframe = dataframe.withColumn(
            serialized_column,
            from_avro(serialized_column, feature_group._get_encoded_avro_schema()),
        )

        """
        Step 2: Replace complex fields in the 'value' column
        """
        new_value_fields = []
        unwrapped_field_names = []
        needs_unwrapping = False
        for field in json.loads(feature_group.avro_schema)["fields"]:
            field_name = field["name"]
            field_path = f"{serialized_column}.{field_name}"

            if field_name in feature_group.get_complex_features():
                # If the feature is a struct, use a wrapper record avro schema to deserialize it.
                is_stuct = feature_group.get_feature(field_name).type.startswith(
                    "struct<"
                )

                if is_stuct:
                    avro_schema = self._generate_wrapper_record_avro_schema(
                        field_name, feature_group
                    )
                    unwrapped_field_name = col(f"{field_path}.{field_name}").alias(
                        field_name
                    )
                    # We need to unwrap the struct field after deserialization to get the actual struct values.
                    needs_unwrapping = True
                else:
                    avro_schema = feature_group._get_feature_avro_schema(field_name)
                    unwrapped_field_name = col(field_path).alias(field_name)

                # re-apply from_avro on the nested field
                decoded_field = from_avro(
                    col(field_path),
                    avro_schema,
                ).alias(field_name)
            else:
                decoded_field = col(field_path).alias(field_name)
                unwrapped_field_name = decoded_field
            new_value_fields.append(decoded_field)
            unwrapped_field_names.append(unwrapped_field_name)
        """
        Step 3: Rebuild the "value" struct
        """
        updated_value_col = struct(*new_value_fields).alias(serialized_column)

        decoded_dataframe = decoded_dataframe.select(
            *[col(c) for c in decoded_dataframe.columns if c != serialized_column],
            updated_value_col,
        )

        if needs_unwrapping:
            # Unwrap the struct field after deserialization to get the actual struct values.
            unwrapped_value_col = struct(*unwrapped_field_names).alias(
                serialized_column
            )
            decoded_dataframe = decoded_dataframe.select(
                *[col(c) for c in decoded_dataframe.columns if c != serialized_column],
                unwrapped_value_col,
            )

        return decoded_dataframe

    def get_training_data(
        self,
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        query_obj: query.Query,
        read_options: Dict[str, Any],
        dataframe_type: str,
        training_dataset_version: int = None,
        transformation_context: Dict[str, Any] = None,
    ):
        """
        Function that creates or retrieves already created the training dataset.

        # Arguments
            training_dataset_obj `TrainingDataset`: The training dataset metadata object.
            feature_view_obj `FeatureView`: The feature view object for the which the training data is being created.
            query_obj `Query`: The query object that contains the query used to create the feature view.
            read_options `Dict[str, Any]`: Dictionary that can be used to specify extra parameters for reading data.
            dataframe_type `str`: The type of dataframe returned.
            training_dataset_version `int`: Version of training data to be retrieved.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Raises
            `ValueError`: If the training dataset statistics could not be retrieved.
        """
        return self.write_training_dataset(
            training_dataset,
            query_obj,
            read_options,
            None,
            read_options=read_options,
            to_df=True,
            feature_view_obj=feature_view_obj,
            training_dataset_version=training_dataset_version,
            transformation_context=transformation_context,
        )

    def split_labels(self, df, labels, dataframe_type):
        if labels:
            if isinstance(df, pd.DataFrame):
                labels_df = df[labels]
                df_new = df.drop(columns=labels)
            else:
                labels_df = df.select(*labels)
                df_new = df.drop(*labels)
            return (
                self._return_dataframe_type(df_new, dataframe_type),
                self._return_dataframe_type(labels_df, dataframe_type),
            )
        else:
            return self._return_dataframe_type(df, dataframe_type), None

    def drop_columns(self, df, drop_cols):
        return df.drop(*drop_cols)

    def write_training_dataset(
        self,
        training_dataset: training_dataset.TrainingDataset,
        query_obj: query.Query,
        user_write_options: Dict[str, Any],
        save_mode: str,
        read_options: Dict[str, Any] = None,
        feature_view_obj: feature_view.FeatureView = None,
        to_df: bool = False,
        training_dataset_version: Optional[int] = None,
        transformation_context: Dict[str, Any] = None,
    ):
        """
        Function that creates or retrieves already created the training dataset.

        # Arguments
            training_dataset `TrainingDataset`: The training dataset metadata object.
            query_obj `Query`: The query object that contains the query used to create the feature view.
            user_write_options `Dict[str, Any]`: Dictionary that can be used to specify extra parameters for writing data using spark.
            save_mode `str`: Spark save mode to be used while writing data.
            read_options `Dict[str, Any]`: Dictionary that can be used to specify extra parameters for reading data.
            feature_view_obj `FeatureView`: The feature view object for the which the training data is being created.
            to_df `bool`: Return dataframe instead of writing the data.
            training_dataset_version `Optional[int]`: Version of training data to be retrieved.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Raises
            `ValueError`: If the training dataset statistics could not be retrieved.
        """
        write_options = self.write_options(
            training_dataset.data_format, user_write_options
        )
        if read_options is None:
            read_options = {}

        if len(training_dataset.splits) == 0:
            if isinstance(query_obj, query.Query):
                dataset = self.convert_to_default_dataframe(
                    query_obj.read(read_options=read_options)
                )
            else:
                raise ValueError("Dataset should be a query.")

            # if training_dataset_version is None:
            transformation_function_engine.TransformationFunctionEngine.compute_and_set_feature_statistics(
                training_dataset, feature_view_obj, dataset
            )
            # else:
            #    transformation_function_engine.TransformationFunctionEngine.get_and_set_feature_statistics(
            #        training_dataset, feature_view_obj, training_dataset_version
            #    )

            if training_dataset.coalesce:
                dataset = dataset.coalesce(1)
            path = training_dataset.location + "/" + training_dataset.name
            return self._write_training_dataset_single(
                feature_view_obj.transformation_functions,
                dataset,
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                path,
                to_df=to_df,
                transformation_context=transformation_context,
            )
        else:
            split_dataset = self._split_df(
                query_obj, training_dataset, read_options=read_options
            )
            for key in split_dataset:
                if training_dataset.coalesce:
                    split_dataset[key] = split_dataset[key].coalesce(1)

                split_dataset[key] = split_dataset[key].cache()

            if training_dataset_version is None:
                transformation_function_engine.TransformationFunctionEngine.compute_and_set_feature_statistics(
                    training_dataset, feature_view_obj, split_dataset
                )
            else:
                transformation_function_engine.TransformationFunctionEngine.get_and_set_feature_statistics(
                    training_dataset, feature_view_obj, training_dataset_version
                )

            return self._write_training_dataset_splits(
                training_dataset,
                split_dataset,
                write_options,
                save_mode,
                to_df=to_df,
                transformation_functions=feature_view_obj.transformation_functions,
                transformation_context=transformation_context,
            )

    def _split_df(self, query_obj, training_dataset, read_options=None):
        if read_options is None:
            read_options = {}
        if (
            training_dataset.splits[0].split_type
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
                event_time_feature = query_obj._left_feature_group.__getattr__(
                    event_time
                )
                event_time_feature.use_fully_qualified_name = True

                query_obj.append_feature(event_time_feature)
                event_time = event_time_feature._get_fully_qualified_feature_name(
                    feature_group=query_obj._left_feature_group
                )

                return self._time_series_split(
                    training_dataset,
                    query_obj.read(read_options=read_options),
                    event_time,
                    drop_event_time=True,
                )
            else:
                # Use the fully qualified name of the event time feature if required
                event_time = event_time_feature[0]._get_fully_qualified_feature_name(
                    feature_group=query_obj._left_feature_group
                )

                return self._time_series_split(
                    training_dataset,
                    query_obj.read(read_options=read_options),
                    event_time,
                )
        else:
            return self._random_split(
                query_obj.read(read_options=read_options), training_dataset
            )

    def _random_split(self, dataset, training_dataset):
        splits = [(split.name, split.percentage) for split in training_dataset.splits]
        split_weights = [split[1] for split in splits]
        split_dataset = dataset.randomSplit(split_weights, training_dataset.seed)
        return dict([(split[0], split_dataset[i]) for i, split in enumerate(splits)])

    def _time_series_split(
        self, training_dataset, dataset, event_time, drop_event_time=False
    ):
        # duplicate the code from util module to avoid udf errors on windows
        def check_timestamp_format_from_date_string(input_date):
            date_format_patterns = {
                r"^([0-9]{4})([0-9]{2})([0-9]{2})$": "%Y%m%d",
                r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H",
                r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M",
                r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})$": "%Y%m%d%H%M%S",
                r"^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{3})$": "%Y%m%d%H%M%S%f",
                r"^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{6})Z$": "ISO",
            }
            normalized_date = (
                input_date.replace("/", "")
                .replace("-", "")
                .replace(" ", "")
                .replace(":", "")
                .replace(".", "")
            )

            date_format = None
            for pattern in date_format_patterns:
                date_format_pattern = re.match(pattern, normalized_date)
                if date_format_pattern:
                    date_format = date_format_patterns[pattern]
                    break

            if date_format is None:
                raise ValueError(
                    "Unable to identify format of the provided date value : "
                    + input_date
                )

            return normalized_date, date_format

        def get_timestamp_from_date_string(input_date):
            norm_input_date, date_format = check_timestamp_format_from_date_string(
                input_date
            )
            try:
                if date_format != "ISO":
                    date_time = datetime.strptime(norm_input_date, date_format)
                else:
                    date_time = datetime.fromisoformat(input_date[:-1])
            except ValueError as err:
                raise ValueError(
                    "Unable to parse the normalized input date value : "
                    + norm_input_date
                    + " with format "
                    + date_format
                ) from err
            if date_time.tzinfo is None:
                date_time = date_time.replace(tzinfo=timezone.utc)
            return int(float(date_time.timestamp()) * 1000)

        def convert_event_time_to_timestamp(event_time):
            if not event_time:
                return None
            if isinstance(event_time, str):
                return get_timestamp_from_date_string(event_time)
            elif isinstance(event_time, pd._libs.tslibs.timestamps.Timestamp):
                # convert to unix epoch time in milliseconds.
                event_time = event_time.to_pydatetime()
                # convert to unix epoch time in milliseconds.
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                return int(event_time.timestamp() * 1000)
            elif isinstance(event_time, datetime):
                # convert to unix epoch time in milliseconds.
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                return int(event_time.timestamp() * 1000)
            elif isinstance(event_time, date):
                # convert to unix epoch time in milliseconds.
                event_time = datetime(*event_time.timetuple()[:7])
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                return int(event_time.timestamp() * 1000)
            elif isinstance(event_time, int):
                if event_time == 0:
                    raise ValueError("Event time should be greater than 0.")
                # jdbc supports timestamp precision up to second only.
                if len(str(event_time)) <= 10:
                    event_time = event_time * 1000
                return event_time
            else:
                raise ValueError(
                    "Given event time should be in `datetime`, `date`, `str` or `int` type"
                )

        # registering the UDF
        _convert_event_time_to_timestamp = udf(
            convert_event_time_to_timestamp, LongType()
        )

        result_dfs = {}
        ts_col = _convert_event_time_to_timestamp(col(event_time))
        for split in training_dataset.splits:
            result_df = dataset.filter(ts_col >= split.start_time).filter(
                ts_col < split.end_time
            )
            if drop_event_time:
                result_df = result_df.drop(event_time)
            result_dfs[split.name] = result_df
        return result_dfs

    def _write_training_dataset_splits(
        self,
        training_dataset,
        feature_dataframes,
        write_options,
        save_mode,
        to_df=False,
        transformation_functions: List[
            transformation_function.TransformationFunction
        ] = None,
        transformation_context: Dict[str, Any] = None,
    ):
        for split_name, feature_dataframe in feature_dataframes.items():
            split_path = training_dataset.location + "/" + str(split_name)
            feature_dataframes[split_name] = self._write_training_dataset_single(
                transformation_functions,
                feature_dataframe,
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                split_path,
                to_df=to_df,
                transformation_context=transformation_context,
            )

        if to_df:
            return feature_dataframes

    def _write_training_dataset_single(
        self,
        transformation_functions,
        feature_dataframe,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
        to_df=False,
        transformation_context: Dict[str, Any] = None,
    ):
        # apply transformation functions (they are applied separately to each split)
        feature_dataframe = self._apply_transformation_function(
            transformation_functions,
            dataset=feature_dataframe,
            transformation_context=transformation_context,
        )
        if to_df:
            return feature_dataframe
        # TODO: currently not supported petastorm, hdf5 and npy file formats
        if data_format.lower() == "tsv":
            data_format = "csv"

        path = self.setup_storage_connector(storage_connector, path)

        feature_dataframe.write.format(data_format).options(**write_options).mode(
            save_mode
        ).save(path)

        feature_dataframe.unpersist()

    def read(
        self, storage_connector, data_format, read_options, location, dataframe_type
    ):
        if not data_format:
            raise FeatureStoreException("data_format is not specified")

        if isinstance(location, str):
            if data_format.lower() in ["delta", "parquet", "hudi", "orc", "bigquery"]:
                # All the above data format readers can handle partitioning
                # by their own, they don't need /**
                # for bigquery, argument location can be a SQL query
                path = location
            else:
                path = location + "/**"

            if data_format.lower() == "tsv":
                data_format = "csv"

        else:
            path = None

        path = self.setup_storage_connector(storage_connector, path)

        return self._return_dataframe_type(
            self._spark_session.read.format(data_format)
            .options(**(read_options if read_options else {}))
            .load(path),
            dataframe_type=dataframe_type,
        )

    def read_stream(
        self,
        storage_connector,
        message_format,
        schema,
        options,
        include_metadata,
    ):
        # ideally all this logic should be in the storage connector in case we add more
        # streaming storage connectors...
        stream = self._spark_session.readStream.format(
            storage_connector.SPARK_FORMAT
        )  # todo SPARK_FORMAT available only for KAFKA connectors

        # set user options last so that they overwrite any default options
        stream = stream.options(**storage_connector.spark_options(), **options)

        if storage_connector.type == StorageConnector.KAFKA:
            return self._read_stream_kafka(
                stream, message_format, schema, include_metadata
            )

    def _read_stream_kafka(self, stream, message_format, schema, include_metadata):
        kafka_cols = [
            col("key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
            col("timestampType"),
        ]

        if message_format == "avro" and schema is not None:
            # check if vallid avro schema
            avro.schema.parse(schema)
            df = stream.load()
            if include_metadata is True:
                return df.select(
                    *kafka_cols, from_avro(df.value, schema).alias("value")
                ).select(*kafka_cols, col("value.*"))
            return df.select(from_avro(df.value, schema).alias("value")).select(
                col("value.*")
            )
        elif message_format == "json" and schema is not None:
            df = stream.load()
            if include_metadata is True:
                return df.select(
                    *kafka_cols,
                    from_json(df.value.cast("string"), schema).alias("value"),
                ).select(*kafka_cols, col("value.*"))
            return df.select(
                from_json(df.value.cast("string"), schema).alias("value")
            ).select(col("value.*"))

        if include_metadata is True:
            return stream.load()
        return stream.load().select("key", "value")

    def add_file(self, file, distribute=True):
        if not file:
            return file

        file_name = os.path.basename(file)

        # Another edge / workaround here. In k8s since the materialization doesn't work as in Yarn
        # If you want to use the Kafka connector for instance, you need to attach the files to the Spark job
        # from Hopsworks. This is done already in Hopsworks. When that happens, the files will be available
        # on both drivers and executors under `/srv/hops/artifacts`. In that case, the paths are also going
        # to be present in the APP_FILES environment variable.
        if "APP_FILES" in os.environ and file in os.environ["APP_FILES"]:
            return f"{os.environ['MATERIALISATION_DIR']}/{file_name}"

        # This is used for unit testing
        if not file.startswith("file://"):
            file = "hdfs://" + file

        # for external clients, download the file using the dataset API
        # also if the client is internal, but we only need the files on the driver
        if client._is_external() or not distribute:
            tmp_file = f"/tmp/{file_name}"
            print("Reading key file from storage connector.")
            response = self._dataset_api.read_content(file, util.get_dataset_type(file))

            with open(tmp_file, "wb") as f:
                f.write(response.content)

            file = f"file://{tmp_file}"

        # If we need the files on the executors, then we should call addFile
        if distribute:
            self._spark_context.addFile(file)
            return SparkFiles.get(file_name)
        else:
            # Remove the 'file://' prefix for local file paths
            return file[7:]

    def profile(
        self,
        dataframe,
        relevant_columns,
        correlations,
        histograms,
        exact_uniqueness=True,
    ):
        """Profile a dataframe with Deequ."""
        return self._jvm.com.logicalclocks.hsfs.spark.engine.SparkEngine.getInstance().profile(
            dataframe._jdf,
            relevant_columns,
            correlations,
            histograms,
            exact_uniqueness,
        )

    @uses_great_expectations
    def validate_with_great_expectations(
        self,
        dataframe: DataFrame,  # noqa: F821
        expectation_suite: great_expectations.core.ExpectationSuite,  # noqa: F821
        ge_validate_kwargs: Optional[dict],
    ):
        # NOTE: InMemoryStoreBackendDefaults SHOULD NOT BE USED in normal settings. You
        # may experience data loss as it persists nothing. It is used here for testing.
        # Please refer to docs to learn how to instantiate your DataContext.
        store_backend_defaults = (
            great_expectations.data_context.types.base.InMemoryStoreBackendDefaults()
        )
        data_context_config = (
            great_expectations.data_context.types.base.DataContextConfig(
                store_backend_defaults=store_backend_defaults,
                checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
            )
        )
        context = great_expectations.data_context.BaseDataContext(
            project_config=data_context_config
        )

        datasource = {
            "name": "my_spark_dataframe",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine",
                "force_reuse_spark_context": True,
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                }
            },
        }
        context.add_datasource(**datasource)

        # Here is a RuntimeBatchRequest using a dataframe
        batch_request = great_expectations.core.batch.RuntimeBatchRequest(
            datasource_name="my_spark_dataframe",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="<YOUR_MEANGINGFUL_NAME>",  # This can be anything that identifies this data_asset for you
            batch_identifiers={"batch_id": "default_identifier"},
            runtime_parameters={"batch_data": dataframe},  # Your dataframe goes here
        )
        context.save_expectation_suite(expectation_suite)
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=expectation_suite.expectation_suite_name,
        )
        report = validator.validate(**ge_validate_kwargs)

        return report

    def write_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "tfrecord":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true")
            options.update(provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true")
            options.update(provided_options)
        else:
            options = {}
            options.update(provided_options)
        return options

    def read_options(self, data_format, provided_options):
        if provided_options is None:
            provided_options = {}
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example", **provided_options)
            options.update(provided_options)
        elif data_format.lower() == "tfrecord":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true", inferSchema="true")
            options.update(provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true", inferSchema="true")
            options.update(provided_options)
        else:
            options = {}
            options.update(provided_options)
        return options

    def parse_schema_feature_group(
        self,
        dataframe,
        time_travel_format=None,
        **kwargs,
    ):
        features = []

        using_hudi = time_travel_format == "HUDI"
        for feat in dataframe.schema:
            name = util.autofix_feature_name(feat.name)
            try:
                converted_type = Engine._convert_spark_type_to_offline_type(
                    feat.dataType, using_hudi
                )
            except ValueError as e:
                raise FeatureStoreException(f"Feature '{feat.name}': {str(e)}") from e
            features.append(
                feature.Feature(
                    name, converted_type, feat.metadata.get("description", None)
                )
            )
        return features

    def parse_schema_training_dataset(self, dataframe):
        return [
            training_dataset_feature.TrainingDatasetFeature(
                util.autofix_feature_name(feat.name), feat.dataType.simpleString()
            )
            for feat in dataframe.schema
        ]

    def setup_storage_connector(self, storage_connector, path=None):
        if storage_connector.type == StorageConnector.S3:
            return self._setup_s3_hadoop_conf(storage_connector, path)
        elif storage_connector.type == StorageConnector.ADLS:
            return self._setup_adls_hadoop_conf(storage_connector, path)
        elif storage_connector.type == StorageConnector.GCS:
            return self._setup_gcp_hadoop_conf(storage_connector, path)
        else:
            return path

    def _setup_s3_hadoop_conf(self, storage_connector, path):
        FS_S3_GLOBAL_CONF = "fs.s3a.global-conf"

        # The argument arrive here as strings
        if storage_connector.arguments.get(FS_S3_GLOBAL_CONF, "True").lower() == "true":
            # For legacy behaviour set the S3 values at global level
            self._set_s3_hadoop_conf(storage_connector, "fs.s3a")

        # Set credentials at bucket level as well to allow users to use multiple
        # storage connector in the same application.
        self._set_s3_hadoop_conf(
            storage_connector, f"fs.s3a.bucket.{storage_connector.bucket}"
        )
        return path.replace("s3://", "s3a://", 1) if path is not None else None

    def _set_s3_hadoop_conf(self, storage_connector, prefix):
        if storage_connector.access_key:
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.access.key", storage_connector.access_key
            )
        if storage_connector.secret_key:
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.secret.key", storage_connector.secret_key
            )
        if storage_connector.server_encryption_algorithm:
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.server-side-encryption-algorithm",
                storage_connector.server_encryption_algorithm,
            )
        if storage_connector.server_encryption_key:
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.server-side-encryption-key",
                storage_connector.server_encryption_key,
            )
        if storage_connector.session_token:
            print(f"session token set for {prefix}")
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.session.token",
                storage_connector.session_token,
            )
        if storage_connector.region:
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.endpoint.region",
                storage_connector.region,
            )

        # Forward all user-specified fs.s3a.* options to Hadoop conf
        for key, value in storage_connector.spark_options().items():
            if not isinstance(key, str):
                continue
            if not key.startswith("fs.s3a."):
                continue
            if value is None:
                continue
            # Strip the leading 'fs.s3a.' so we can prefix with the connector specific prefix
            suffix = key.split("fs.s3a.", 1)[1]
            self._spark_context._jsc.hadoopConfiguration().set(
                f"{prefix}.{suffix}",
                str(value),
            )

    def _setup_adls_hadoop_conf(self, storage_connector, path):
        for k, v in storage_connector.spark_options().items():
            self._spark_context._jsc.hadoopConfiguration().set(k, v)

        return path

    def is_spark_dataframe(self, dataframe):
        if isinstance(dataframe, DataFrame):
            return True
        return False

    def update_table_schema(self, feature_group):
        if feature_group.time_travel_format == "DELTA":
            self._add_cols_to_delta_table(feature_group)
        else:
            self._save_empty_dataframe(feature_group)

    def _save_empty_dataframe(self, feature_group):
        location = feature_group.prepare_spark_location()

        dataframe = self._spark_session.read.format("hudi").load(location)

        for _feature in feature_group.features:
            if _feature.name not in dataframe.columns:
                dataframe = dataframe.withColumn(
                    _feature.name, lit(None).cast(_feature.type)
                )

        self.save_dataframe(
            feature_group,
            dataframe.limit(0),
            "upsert",
            feature_group.online_enabled,
            "offline",
            {},
            {},
        )

    def _add_cols_to_delta_table(self, feature_group):
        self._spark_session.conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        location = feature_group.prepare_spark_location()

        dataframe = self._spark_session.read.format("delta").load(location)

        for _feature in feature_group.features:
            if _feature.name not in dataframe.columns:
                dataframe = dataframe.withColumn(
                    _feature.name, lit(None).cast(_feature.type)
                )

        dataframe.limit(0).write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).option("spark.databricks.delta.schema.autoMerge.enabled", "true").save(
            location
        )

    def _apply_transformation_function(
        self,
        transformation_functions: List[transformation_function.TransformationFunction],
        dataset: DataFrame,
        transformation_context: Dict[str, Any] = None,
    ):
        """
        Apply transformation function to the dataframe.

        # Arguments
            transformation_functions `List[TransformationFunction]` : List of transformation functions.
            dataset `Union[DataFrame]`: A spark dataframe.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            `DataFrame`: A spark dataframe with the transformed data.
        # Raises
            `hopsworks.client.exceptions.FeatureStoreException`: If any of the features mentioned in the transformation function is not present in the Feature View.
        """
        dropped_features = set()
        transformations = []
        transformation_features = []
        output_col_names = []
        explode_name = []
        for tf in transformation_functions:
            hopsworks_udf = tf.hopsworks_udf

            # Setting transformation function context variables.
            hopsworks_udf.transformation_context = transformation_context

            missing_features = set(hopsworks_udf.transformation_features) - set(
                dataset.columns
            )

            if missing_features:
                if (
                    tf.transformation_type
                    == transformation_function.TransformationType.ON_DEMAND
                ):
                    # On-demand transformation are applied using the python/spark engine during insertion, the transformation while retrieving feature vectors are performed in the vector_server.
                    raise FeatureStoreException(
                        f"The following feature(s): `{'`, '.join(missing_features)}`, specified in the on-demand transformation function '{hopsworks_udf.function_name}' are not present in the dataframe being inserted into the feature group. "
                        + "Please verify that the correct feature names are used in the transformation function and that these features exist in the dataframe being inserted."
                    )
                else:
                    raise FeatureStoreException(
                        f"The following feature(s): `{'`, '.join(missing_features)}`, specified in the model-dependent transformation function '{hopsworks_udf.function_name}' are not present in the feature view. Please verify that the correct features are specified in the transformation function."
                    )
            if tf.hopsworks_udf.dropped_features:
                dropped_features.update(hopsworks_udf.dropped_features)

            # Add to dropped features if the feature need to overwritten to avoid ambiguous columns.
            if len(hopsworks_udf.return_types) == 1 and (
                hopsworks_udf.function_name == hopsworks_udf.output_column_names[0]
            ):
                dropped_features.update(hopsworks_udf.output_column_names)

            pandas_udf = hopsworks_udf.get_udf()
            output_col_name = hopsworks_udf.output_column_names[0]

            transformations.append(pandas_udf)
            output_col_names.append(output_col_name)
            transformation_features.append(hopsworks_udf.transformation_features)

            if len(hopsworks_udf.return_types) > 1:
                explode_name.append(f"{output_col_name}.*")
            else:
                explode_name.append(output_col_name)

        untransformed_columns = []  # Untransformed column maintained as a list since order is imported while selecting features.
        for column in dataset.columns:
            if column not in dropped_features:
                untransformed_columns.append(column)
        # Applying transformations
        transformed_dataset = dataset.select(
            *untransformed_columns,
            *[
                fun(*feature).alias(output_col_name)
                for fun, feature, output_col_name in zip(
                    transformations, transformation_features, output_col_names
                )
            ],
        ).select(*untransformed_columns, *explode_name)

        return transformed_dataset

    def extract_logging_metadata(
        self,
        untransformed_features: DataFrame,
        transformed_features: DataFrame,
        feature_view: feature_view.FeatureView,
        transformed: bool,
        inference_helpers: bool,
        event_time: bool,
        primary_key: bool,
        request_parameters: DataFrame = None,
    ):
        """
        Extracts the logging data from the passed dataframes and returns the expected batch dataframe with the logging metadata attached.
        The logging metadata is created as a hidden attribute named `hopsworks_logging_metadata` of the returned dataframe.

        The return dataframe will only contain the features that the user requested (i.e. if the user requested to not include primary keys, event time or inference helpers, those features will be excluded).
        """
        # Extract primary keys and event time from fully qualified names
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

        # Create an expression to select the primary key, event time and alias it to original name if needed.
        severing_key_select_expr = []
        for col in feature_view._fully_qualified_primary_keys:
            if col in fully_qualified_serving_key_mapper.keys():
                severing_key_select_expr.append(
                    f"{col} as {fully_qualified_serving_key_mapper[col]}"
                )
            elif col in fully_qualified_serving_key_mapper.values():
                severing_key_select_expr.append(col)

        event_time_select_expr = []
        if (
            fully_qualified_root_fg_event_time
            in feature_view._fully_qualified_event_time
        ):
            event_time_select_expr.append(
                f"{fully_qualified_root_fg_event_time} as {feature_view._root_feature_group_event_time_column_name}"
            )
        else:
            event_time_select_expr.append(
                feature_view._root_feature_group_event_time_column_name
            )

        serving_keys_df = untransformed_features.selectExpr(*severing_key_select_expr)
        event_time_df = untransformed_features.selectExpr(*event_time_select_expr)

        # Extract inference_helpers
        inference_helpers_df = untransformed_features.selectExpr(
            *feature_view.inference_helper_columns
        )

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

        # Setting data directly in spark dataframe since spark dataframe cannot be created directly using a constructor
        batch_dataframe.hopsworks_logging_metadata = logging_meta_data

        return batch_dataframe

    def _setup_gcp_hadoop_conf(self, storage_connector, path):
        PROPERTY_ENCRYPTION_KEY = "fs.gs.encryption.key"
        PROPERTY_ENCRYPTION_HASH = "fs.gs.encryption.key.hash"
        PROPERTY_ALGORITHM = "fs.gs.encryption.algorithm"
        PROPERTY_GCS_FS_KEY = "fs.AbstractFileSystem.gs.impl"
        PROPERTY_GCS_FS_VALUE = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        PROPERTY_GCS_ACCOUNT_ENABLE = "google.cloud.auth.service.account.enable"
        PROPERTY_ACCT_EMAIL = "fs.gs.auth.service.account.email"
        PROPERTY_ACCT_KEY_ID = "fs.gs.auth.service.account.private.key.id"
        PROPERTY_ACCT_KEY = "fs.gs.auth.service.account.private.key"
        # The AbstractFileSystem for 'gs:' URIs
        self._spark_context._jsc.hadoopConfiguration().setIfUnset(
            PROPERTY_GCS_FS_KEY, PROPERTY_GCS_FS_VALUE
        )
        # Whether to use a service account for GCS authorization. Setting this
        # property to `false` will disable use of service accounts for authentication.
        self._spark_context._jsc.hadoopConfiguration().setIfUnset(
            PROPERTY_GCS_ACCOUNT_ENABLE, "true"
        )

        # The JSON key file of the service account used for GCS
        # access when google.cloud.auth.service.account.enable is true.
        local_path = self.add_file(storage_connector.key_path)
        with open(local_path, "r") as f_in:
            jsondata = json.load(f_in)
        self._spark_context._jsc.hadoopConfiguration().set(
            PROPERTY_ACCT_EMAIL, jsondata["client_email"]
        )
        self._spark_context._jsc.hadoopConfiguration().set(
            PROPERTY_ACCT_KEY_ID, jsondata["private_key_id"]
        )
        self._spark_context._jsc.hadoopConfiguration().set(
            PROPERTY_ACCT_KEY, jsondata["private_key"]
        )

        if storage_connector.algorithm:
            # if encryption fields present
            self._spark_context._jsc.hadoopConfiguration().set(
                PROPERTY_ALGORITHM, storage_connector.algorithm
            )
            self._spark_context._jsc.hadoopConfiguration().set(
                PROPERTY_ENCRYPTION_KEY, storage_connector.encryption_key
            )
            self._spark_context._jsc.hadoopConfiguration().set(
                PROPERTY_ENCRYPTION_HASH, storage_connector.encryption_key_hash
            )
        else:
            # unset if already set
            self._spark_context._jsc.hadoopConfiguration().unset(PROPERTY_ALGORITHM)
            self._spark_context._jsc.hadoopConfiguration().unset(
                PROPERTY_ENCRYPTION_HASH
            )
            self._spark_context._jsc.hadoopConfiguration().unset(
                PROPERTY_ENCRYPTION_KEY
            )

        return path

    def create_empty_df(self, streaming_df):
        return SQLContext(self._spark_context).createDataFrame(
            self._spark_context.emptyRDD(), streaming_df.schema
        )

    @staticmethod
    def get_unique_values(feature_dataframe, feature_name):
        unique_values = feature_dataframe.select(feature_name).distinct().collect()
        return [field[feature_name] for field in unique_values]

    @staticmethod
    def _convert_spark_type_to_offline_type(spark_type, using_hudi):
        # The HiveSyncTool is strict and does not support schema evolution from tinyint/short to
        # int. Avro, on the other hand, does not support tinyint/short and delivers them as int
        # to Hive. Therefore, we need to force Hive to create int-typed columns in the first place.

        if not using_hudi:
            return spark_type.simpleString()
        elif type(spark_type) is ByteType:
            return "int"
        elif type(spark_type) is ShortType:
            return "int"
        elif type(spark_type) in [
            BooleanType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType,
            TimestampType,
            DateType,
            StringType,
            ArrayType,
            StructType,
            BinaryType,
        ]:
            return spark_type.simpleString()

        raise ValueError(f"spark type {str(type(spark_type))} not supported")

    @staticmethod
    def _convert_offline_type_to_spark_type(offline_type):
        if "array<" == offline_type[:6]:
            return ArrayType(
                Engine._convert_offline_type_to_spark_type(offline_type[6:-1])
            )
        elif "struct<label:string,index:int>" in offline_type:
            return StructType(
                [
                    StructField("label", StringType(), True),
                    StructField("index", IntegerType(), True),
                ]
            )
        elif offline_type.startswith("decimal"):
            return DecimalType()
        else:
            offline_type_spark_type_mapping = {
                "string": StringType(),
                "bigint": LongType(),
                "int": IntegerType(),
                "smallint": ShortType(),
                "tinyint": ByteType(),
                "float": FloatType(),
                "double": DoubleType(),
                "timestamp": TimestampType(),
                "boolean": BooleanType(),
                "date": DateType(),
                "binary": BinaryType(),
            }
            if offline_type in offline_type_spark_type_mapping:
                return offline_type_spark_type_mapping[offline_type]
            else:
                raise FeatureStoreException(
                    f"Offline type {offline_type} cannot be converted to a spark type."
                )

    @staticmethod
    def cast_columns(df, schema, online=False):
        pyspark_schema = dict(
            [
                (_feat.name, Engine._convert_offline_type_to_spark_type(_feat.type))
                for _feat in schema
            ]
        )
        for _feat in pyspark_schema:
            df = df.withColumn(_feat, col(_feat).cast(pyspark_schema[_feat]))
        return df

    @staticmethod
    def is_connector_type_supported(type):
        return True

    @staticmethod
    def _validate_logging_list(
        feature_log: Union[List[List[Any]], List[Any]], cols: List[str]
    ) -> None:
        """
        Function to check if the provided list has the same number of features/labels as expected.

        If the feature_log provided is a list then it is considered as a single feature (column).

        # Arguments:
            feature_log `Union[List[List[Any]], List[Any]]`: List of features/labels provided for logging.
            cols `List[str]`: List of expected features in the logging dataframe.
        """
        if isinstance(feature_log[0], list) or (
            HAS_NUMPY and isinstance(feature_log[0], np.ndarray)
        ):
            provided_len = len(feature_log[0])
        else:
            provided_len = 1
        assert provided_len == len(
            cols
        ), f"Expecting {len(cols)} features/labels but {provided_len} provided."

    def get_feature_logging_df(
        self,
        logging_data: Union[
            pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray
        ] = None,
        logging_feature_group_features: List[feature.Feature] = None,
        logging_feature_group_feature_names: List[str] = None,
        logging_features: List[str] = None,
        transformed_features: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        untransformed_features: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        predictions: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        serving_keys: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        helper_columns: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        request_parameters: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        event_time: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        extra_logging_features: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        request_id: Optional[
            Tuple[
                Union[pd.DataFrame, "pyspark.sql.DataFrame", List[List], np.ndarray],
                List[str],
                str,
            ]
        ] = None,
        td_col_name: Optional[str] = None,
        time_col_name: Optional[str] = None,
        model_col_name: Optional[str] = None,
        training_dataset_version: Optional[int] = None,
        model_name: Optional[str] = None,
        model_version: Optional[int] = None,
    ) -> "pyspark.sql.DataFrame":
        """
        Function that combines all the logging data into a single dataframe that can be written to the logging feature group.
        The function takes care of renaming the prediction columns, creating a json column for the request parameters and adding the meta data columns.

        # Arguments
            logging_data: `Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray]` : The data to be logged.
            logging_feature_group_features: `List[feature.Feature]` : The features of the logging feature group.
            logging_feature_group_feature_names: `List[str]`. The names of the logging feature group features.
            logging_features: `List[str]`: The names of the logging features, this excludes the names of all metadata columns.
            transformed_features: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the transformed features and their feature names and a log component name (a constant named "transformed_features").
            untransformed_features: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the untransformed features and their feature names and a log component name (a constant named "untransformed_features").
            predictions: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the predictions and their feature names and a log component name (a constant named "predictions").
            serving_keys: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the serving keys and    their feature names and a log component name (a constant named "serving_keys").
            helper_columns: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the helper columns and their feature names and a log component name (a constant named "helper_columns").
            request_parameters: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the request parameters and their feature names and a log component name (a constant named "request_parameters").
            event_time: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the event time and their feature names and a log component name (a constant named "event_time").
            extra_logging_features: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing extra logging features and their feature names and a log component name (a constant named "extra_logging_features").
            request_id: `Optional[Tuple[Union[pd.DataFrame, pyspark.sql.DataFrame, List[List], np.ndarray], List[str]]]` : A tuple containing the request id and their feature names and a log component name (a constant named "request_id").
            td_col_name: `Optional[str]` : The name of the training dataset version column.
            time_col_name: `Optional[str]` : The name of the time column.
            model_col_name: `Optional[str]` : The name of the model column.
            training_dataset_version: `Optional[int]` : The version of the training dataset.
            hsml_model: `str` : The name of the model.

        # Returns
            `DataFrame`: A spark dataframe with all the logging components.
            `List[str]`: Names of additional logging features passed in the Logging Dataframe.
            `List[str]`: Names of missing logging features passed in the Logging Dataframe.
        """
        TEMP_JOIN_KEY = "row_id"

        if logging_data is not None:
            try:
                logging_df = (
                    self.convert_to_default_dataframe(logging_data, logging_features)
                    if logging_data is not None
                    else None
                )
            except AssertionError as e:
                raise FeatureStoreException(
                    f"Error logging data `{constants.FEATURE_LOGGING.LOGGING_DATA}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.LOGGING_DATA}` to ensure that it has the following features : {logging_features}."
                ) from e

            logging_df = logging_df.withColumn(
                TEMP_JOIN_KEY,
                row_number().over(Window.orderBy(monotonically_increasing_id())),
            )
        else:
            logging_df = None
        user_passed_request_parameters = []
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
            if data is None:
                continue
            try:
                df = (
                    self.convert_to_default_dataframe(data, feature_names)
                    if data is not None or feature_names
                    else None
                )
            except AssertionError as e:
                raise FeatureStoreException(
                    f"Error logging data `{log_component_name}` do not have all required features. Please check the `{log_component_name}` to ensure that it has the following features : {feature_names}."
                ) from e
            if df.count() == 0:
                continue
            if logging_df is None or logging_df.count() == 0:
                logging_df = df
                logging_df = logging_df.withColumn(
                    TEMP_JOIN_KEY,
                    row_number().over(Window.orderBy(monotonically_increasing_id())),
                )
            elif df.count() == 1 and logging_df.count() > 1:
                df = df.withColumn(
                    TEMP_JOIN_KEY,
                    row_number().over(Window.orderBy(monotonically_increasing_id())),
                )

                missing_columns = [
                    column_name
                    for column_name in df.columns
                    if (
                        column_name not in logging_df.columns
                        or column_name == TEMP_JOIN_KEY
                    )
                ]

                logging_df = logging_df.join(
                    df.select(*missing_columns), TEMP_JOIN_KEY, "left"
                )
            elif df.count() != logging_df.count():
                raise FeatureStoreException(
                    f"Length of `{log_component_name}` provided do not match other arguments. Please check the logging data to make sure that all arguments have the same length."
                )
            else:
                df = df.withColumn(
                    TEMP_JOIN_KEY,
                    row_number().over(Window.orderBy(monotonically_increasing_id())),
                )
                common_columns = [
                    column_name
                    for column_name in df.columns
                    if column_name in logging_df.columns
                    and column_name != TEMP_JOIN_KEY
                ]
                # Dropping common columns so that it gets overwritten
                if common_columns:
                    logging_df = logging_df.drop(*common_columns)

                logging_df = logging_df.join(df.select("*"), TEMP_JOIN_KEY, "left")

            if log_component_name == constants.FEATURE_LOGGING.REQUEST_PARAMETERS:
                user_passed_request_parameters = [
                    col for col in df.columns if col != TEMP_JOIN_KEY
                ]

        # Renaming prediction columns
        _, predictions_feature_names, _ = predictions
        for prediction_feature_name in predictions_feature_names:
            logging_df = logging_df.withColumnRenamed(
                prediction_feature_name,
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + prediction_feature_name,
            )

        # Creating a json column for request parameters
        missing_request_parameters = []
        _, request_parameter_columns, _ = request_parameters
        if request_parameter_columns:
            if (
                constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
                not in logging_df.columns
            ):
                from pyspark.sql.functions import struct, to_json

                avaiable_request_parameters = [
                    column_name
                    for column_name in logging_df.columns
                    if column_name in request_parameter_columns
                ]
                missing_request_parameters = [
                    column_name
                    for column_name in request_parameter_columns
                    if column_name not in avaiable_request_parameters
                    and column_name not in user_passed_request_parameters
                ]

                for column_name in missing_request_parameters:
                    logging_df = logging_df.withColumn(column_name, lit(None))

                request_parameter_columns += [
                    col
                    for col in user_passed_request_parameters
                    if col not in request_parameter_columns
                ]

                logging_df = logging_df.withColumn(
                    constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME,
                    regexp_replace(
                        to_json(struct(*request_parameter_columns)), r"([,:])", r"$1 "
                    ),
                )

                # Drop individual request parameter columns that are not part of the logging feature group.
                logging_df = logging_df.drop(
                    *[
                        feature
                        for feature in request_parameter_columns
                        if feature not in logging_feature_group_feature_names
                    ]
                )
        # Adding meta data columns
        uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
        logging_df = logging_df.withColumn(
            td_col_name, lit(training_dataset_version).cast(LongType())
        )
        logging_df = logging_df.withColumn(
            model_col_name, lit(model_name).cast(StringType())
        )
        logging_df = logging_df.withColumn(
            constants.FEATURE_LOGGING.MODEL_VERSION_COLUMN_NAME,
            lit(model_version).cast(StringType()),
        )
        now = datetime.now()
        logging_df = logging_df.withColumn(
            time_col_name, lit(now).cast(TimestampType())
        )
        logging_df = logging_df.withColumn("log_id", uuid_udf())

        # Removing the temporary row_id column
        logging_df = logging_df.drop(TEMP_JOIN_KEY)

        # Find all missing logging features and add them as null columns.
        # Find all additional logging features that are not part of the logging feature group and drop them.
        missing_logging_features = (
            set(logging_feature_group_feature_names) - set(logging_df.columns)
        ).union(missing_request_parameters)
        additional_logging_features = (
            set(logging_df.columns)
            - set(logging_feature_group_feature_names)
            - set(request_parameter_columns)
            - set(user_passed_request_parameters)
        )

        # Cast all features to their respective offline type before writing.
        for f in logging_feature_group_features:
            if f.name in missing_logging_features:
                try:
                    offline_type = Engine._convert_offline_type_to_spark_type(f.type)
                except FeatureStoreException:
                    offline_type = f.type
                try:
                    logging_df = logging_df.withColumn(
                        f.name,
                        lit(None).cast(offline_type),
                    )
                except Exception as e:
                    raise FeatureStoreException(
                        f"Feature '{f.name}' cannot be set as Null. Cast into type '{f.type}' failed."
                    ) from e
            else:
                try:
                    offline_type = Engine._convert_offline_type_to_spark_type(f.type)
                except FeatureStoreException:
                    offline_type = f.type
                try:
                    logging_df = logging_df.withColumn(
                        f.name,
                        col(f.name).cast(offline_type),
                    )
                except Exception as e:
                    raise FeatureStoreException(
                        f"Feature '{f.name}' cannot be cast into type '{f.type}'."
                    ) from e

        # Select the required columns
        return (
            logging_df.select(*logging_feature_group_feature_names),
            additional_logging_features,
            missing_logging_features,
        )

    @staticmethod
    def read_feature_log(query, time_col):
        df = query.read()
        return df.drop("log_id", time_col)

    def get_spark_version(self):
        return self._spark_session.version

    def check_supported_dataframe(self, dataframe: Any) -> bool:
        """
        Check if a dataframe is supported by the engine.

        Both Pandas and Spark dataframes are supported in the Spark Engine.

        # Arguments:
            dataframe `Any`: A dataframe to check.

        # Returns:
            `bool`: True if the dataframe is supported, False otherwise.
        """
        if isinstance(dataframe, DataFrame) or isinstance(dataframe, pd.DataFrame):
            return True


class SchemaError(Exception):
    """Thrown when schemas don't match"""

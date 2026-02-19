#
#   Copyright 2022 Hopsworks AB
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

import datetime
import json
import logging
import sys
from unittest.mock import MagicMock, PropertyMock, call

import hopsworks_common
import numpy
import pandas as pd
import pyspark.sql
import pytest
from hopsworks_common import constants
from hsfs import (
    expectation_suite,
    feature,
    feature_group,
    feature_view,
    storage_connector,
    training_dataset,
    training_dataset_feature,
    transformation_function,
    util,
)
from hsfs.client import exceptions
from hsfs.constructor import hudi_feature_group_alias, query
from hsfs.core import data_source as ds
from hsfs.core import online_ingestion, training_dataset_engine
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS
from hsfs.engine import spark
from hsfs.hopsworks_udf import udf
from hsfs.serving_key import ServingKey
from hsfs.training_dataset_feature import TrainingDatasetFeature
from hsfs.transformation_function import TransformationType
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    lit,
    monotonically_increasing_id,
    regexp_replace,
    row_number,
    struct,
    to_json,
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
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


hopsworks_common.connection._hsfs_engine_type = "spark"


class TestSpark:
    # Helper Functions
    @staticmethod
    def get_expected_logging_df(
        logging_data=None,
        untransformed_feature_df=None,
        transformed_feature_df=None,
        predictions_df=None,
        serving_keys_df=None,
        inference_helper_df=None,
        extra_log_columns_df=None,
        event_time_df=None,
        request_id_df=None,
        request_parameters_df=None,
        logging_feature_group_features=None,
        spark_engine=None,
        metadata_logging_columns=None,
        column_names=None,
    ):
        logging_df = None
        missing_columns = []
        additional_columns = []

        # Aggregate all logging dataframes
        for df in [
            logging_data,
            untransformed_feature_df,
            transformed_feature_df,
            predictions_df,
            serving_keys_df,
            inference_helper_df,
            extra_log_columns_df,
            event_time_df,
            request_id_df,
            request_parameters_df,
        ]:
            if df is None:
                continue
            if logging_df is None:
                logging_df = df
                logging_df = logging_df.withColumn(
                    "row_id_temp",
                    row_number().over(Window.orderBy(monotonically_increasing_id())),
                )
                continue
            duplicate_columns = [
                col
                for col in df.columns
                if col in logging_df.columns and col != "row_id_temp"
            ]
            df = df.withColumn(
                "row_id_temp",
                row_number().over(Window.orderBy(monotonically_increasing_id())),
            )
            logging_df = logging_df.drop(*duplicate_columns)
            logging_df = logging_df.join(df, on="row_id_temp", how="left")

        expected_columns = [feature.name for feature in logging_feature_group_features]
        metadata_logging_columns_names = [col.name for col in metadata_logging_columns]

        # Rename Prediction columns
        for col in column_names["predictions"]:
            if col in logging_df.columns:
                logging_df = logging_df.withColumnRenamed(col, f"predicted_{col}")

        # Handle request parameters
        user_passed_request_params = (
            request_parameters_df.columns if request_parameters_df is not None else []
        )
        avaiable_request_params = [
            col
            for col in column_names["request_parameters"]
            if col not in user_passed_request_params and col in logging_df.columns
        ]
        missing_request_params = [
            col
            for col in column_names["request_parameters"]
            if col not in user_passed_request_params and col not in logging_df.columns
        ]

        if not user_passed_request_params and avaiable_request_params:
            request_parameters_df = logging_df.select(
                *(avaiable_request_params + ["row_id_temp"])
            )
        elif avaiable_request_params:
            request_parameters_df = request_parameters_df.withColumn(
                "row_id_temp",
                row_number().over(Window.orderBy(monotonically_increasing_id())),
            )
            request_parameters_df = request_parameters_df.join(
                logging_df.select(*avaiable_request_params + ["row_id_temp"]),
                on="row_id_temp",
                how="left",
            )

        if request_parameters_df is not None:
            for col in missing_request_params:
                if col in logging_df.columns:
                    request_parameters_df = request_parameters_df.withColumn(
                        col, lit(None)
                    )
            logging_df = logging_df.drop(
                *[
                    col
                    for col in request_parameters_df.columns
                    if col in logging_df.columns and col != "row_id_temp"
                ]
            )
            if "row_id_temp" not in request_parameters_df.columns:
                request_parameters_df = request_parameters_df.withColumn(
                    "row_id_temp",
                    row_number().over(Window.orderBy(monotonically_increasing_id())),
                )
            logging_df = logging_df.join(
                request_parameters_df, on="row_id_temp", how="left"
            )
            logging_df = logging_df.withColumn(
                constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME,
                regexp_replace(
                    to_json(
                        struct(
                            *[
                                col
                                for col in request_parameters_df.columns
                                if col != "row_id_temp"
                            ]
                        )
                    ),
                    r"([,:])",
                    r"$1 ",
                ),
            )
        else:
            logging_df = logging_df.withColumn(
                constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME,
                lit(constants.FEATURE_LOGGING.EMPTY_REQUEST_PARAMETER_COLUMN_VALUE),
            )

        missing_columns = [
            col
            for col in expected_columns
            if col not in logging_df.columns
            and col not in metadata_logging_columns_names
        ]
        for col in missing_columns:
            logging_df = logging_df.withColumn(col, lit(None))
        missing_columns += [
            col for col in missing_request_params if col not in missing_columns
        ]
        additional_columns = [
            col
            for col in logging_df.columns
            if col not in expected_columns and col != "row_id_temp"
        ]

        return (
            logging_df.select(
                *[
                    col
                    for col in expected_columns
                    if col not in metadata_logging_columns_names
                ]
            ).cache(),
            expected_columns,
            additional_columns,
            missing_columns,
        )

    @staticmethod
    def get_logging_arguments(
        logging_data=None,
        transformed_features=None,
        untransformed_features=None,
        predictions=None,
        serving_keys=None,
        helper_columns=None,
        request_parameters=None,
        event_time=None,
        request_id=None,
        extra_logging_features=None,
        column_names=None,
        logging_feature_group_features=None,
    ):
        return {
            "logging_data": logging_data,
            "logging_feature_group_features": logging_feature_group_features,
            "logging_feature_group_feature_names": [
                feat.name for feat in logging_feature_group_features
            ],
            "logging_features": [
                feat.name
                for feat in logging_feature_group_features
                if feat.name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
            ],
            "transformed_features": (
                transformed_features,
                column_names["transformed_features"],
                constants.FEATURE_LOGGING.TRANSFORMED_FEATURES,
            ),
            "untransformed_features": (
                untransformed_features,
                column_names["untransformed_features"],
                constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES,
            ),
            "predictions": (
                predictions,
                column_names["predictions"],
                constants.FEATURE_LOGGING.PREDICTIONS,
            ),
            "serving_keys": (
                serving_keys,
                column_names["serving_keys"],
                constants.FEATURE_LOGGING.SERVING_KEYS,
            ),
            "helper_columns": (
                helper_columns,
                column_names["helper_columns"],
                constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS,
            ),
            "request_parameters": (
                request_parameters,
                column_names["request_parameters"],
                constants.FEATURE_LOGGING.REQUEST_PARAMETERS,
            ),
            "event_time": (
                event_time,
                column_names["event_time"],
                constants.FEATURE_LOGGING.EVENT_TIME,
            ),
            "request_id": (
                request_id,
                column_names["request_id"],
                constants.FEATURE_LOGGING.REQUEST_ID,
            ),
            "extra_logging_features": (
                extra_logging_features,
                column_names["extra_logging_features"],
                constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES,
            ),
            "td_col_name": constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME,
            "time_col_name": constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME,
            "model_col_name": constants.FEATURE_LOGGING.MODEL_COLUMN_NAME,
            "training_dataset_version": 1,
            "model_name": "test_model",
        }

    # Pytest Fixtures
    @pytest.fixture(scope="class")
    def spark_engine(self):
        spark_engine = spark.Engine()
        # Set shuffle partitions to 1 for testing purposes
        spark_engine._spark_session.conf.set("spark.sql.shuffle.partitions", "1")
        yield spark_engine

    @pytest.fixture
    def logging_features(self):
        meta_data_logging_columns = [
            feature.Feature(
                constants.FEATURE_LOGGING.LOG_ID_COLUMN_NAME,
                primary=True,
                type="string",
            ),
            feature.Feature(
                constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME,
                type="int",
            ),
            feature.Feature(
                constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME, type="timestamp"
            ),
            feature.Feature(constants.FEATURE_LOGGING.MODEL_COLUMN_NAME, type="string"),
            feature.Feature(
                constants.FEATURE_LOGGING.MODEL_VERSION_COLUMN_NAME, type="int"
            ),
        ]

        logging_features = [
            # Metdata Columns
            feature.Feature("primary_key", primary=True, type="bigint"),
            feature.Feature("event_time", type="timestamp"),
            feature.Feature("feature_1", type="double"),
            feature.Feature("feature_2", type="double"),
            feature.Feature("feature_3", type="int"),
            feature.Feature(
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label", type="string"
            ),
            feature.Feature("min_max_scaler_feature_3", type="double"),
            feature.Feature("extra_1", type="string"),
            feature.Feature("extra_2", type="int"),
            feature.Feature("inference_helper_1", type="double"),
            feature.Feature("rp_1", type="int"),
            feature.Feature("rp_2", type="int"),
            feature.Feature(
                constants.FEATURE_LOGGING.REQUEST_ID_COLUMN_NAME, type="string"
            ),
            feature.Feature(
                constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME, type="string"
            ),
        ]

        column_names = {
            "untransformed_features": ["feature_1", "feature_2", "feature_3"],
            "transformed_features": [
                "feature_1",
                "feature_2",
                "min_max_scaler_feature_3",
            ],
            "predictions": ["label"],
            "serving_keys": ["primary_key"],
            "helper_columns": ["inference_helper_1"],
            "request_parameters": ["rp_1", "rp_2"],
            "event_time": ["event_time"],
            "request_id": ["request_id"],
            "extra_logging_features": ["extra_1", "extra_2"],
        }
        yield logging_features, meta_data_logging_columns, column_names

    @pytest.fixture
    def logging_test_dataframe(self, spark_engine):
        # Create Spark DataFrame directly using schema
        schema = StructType(
            [
                StructField("primary_key", LongType(), True),
                StructField("event_time", TimestampType(), True),
                StructField("feature_1", DoubleType(), True),
                StructField("feature_2", DoubleType(), True),
                StructField("feature_3", IntegerType(), True),
                StructField("label", StringType(), True),
                StructField("min_max_scaler_feature_3", DoubleType(), True),
                StructField("rp_1", IntegerType(), True),
                StructField("rp_2", IntegerType(), True),
                StructField("extra_1", StringType(), True),
                StructField("extra_2", IntegerType(), True),
                StructField("request_id", StringType(), True),
                StructField("inference_helper_1", DoubleType(), True),
            ]
        )

        log_data_list = [
            (
                1,
                datetime.datetime(2025, 1, 1, 12, 0, 0),
                0.25,
                5.0,
                100,
                "A",
                0.25,
                1,
                4,
                "extra_a",
                10,
                "req_1",
                0.95,
            ),
            (
                2,
                datetime.datetime(2025, 1, 2, 13, 30, 0),
                0.75,
                10.2,
                200,
                "B",
                0.75,
                2,
                5,
                "extra_b",
                20,
                "req_2",
                0.85,
            ),
            (
                3,
                datetime.datetime(2025, 1, 3, 15, 45, 0),
                1.1,
                7.7,
                300,
                "A",
                1.1,
                3,
                6,
                "extra_c",
                30,
                "req_3",
                0.76,
            ),
        ]

        return spark_engine._spark_session.createDataFrame(
            log_data_list, schema
        ).cache()

    def test_sql(self, mocker):
        # Arrange
        mock_spark_engine_sql_offline = mocker.patch(
            "hsfs.engine.spark.Engine._sql_offline"
        )
        mocker.patch("hsfs.engine.spark.Engine.set_job_group")
        mock_spark_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.spark.Engine._return_dataframe_type"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.sql(
            sql_query=None,
            feature_store=None,
            connector=None,
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_spark_engine_sql_offline.call_count == 1
        assert mock_spark_engine_return_dataframe_type.call_count == 1

    def test_sql_connector(self, mocker):
        # Arrange
        mock_spark_engine_sql_offline = mocker.patch(
            "hsfs.engine.spark.Engine._sql_offline"
        )
        mocker.patch("hsfs.engine.spark.Engine.set_job_group")
        mock_spark_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.spark.Engine._return_dataframe_type"
        )

        spark_engine = spark.Engine()

        connector = mocker.Mock()

        # Act
        spark_engine.sql(
            sql_query=None,
            feature_store=None,
            connector=connector,
            dataframe_type=None,
            read_options={"numPartitions": 5},
        )

        # Assert
        assert mock_spark_engine_sql_offline.call_count == 0
        assert connector.read.call_count == 1
        assert connector.read.call_args[0][2] == {"numPartitions": 5}
        assert mock_spark_engine_return_dataframe_type.call_count == 1

    def test_sql_offline(self, mocker):
        # Arrange
        mock_sql = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sql",
            new_callable=PropertyMock,
            return_value=mock_sql,
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._sql_offline(
            sql_query=None,
            feature_store=None,
        )

        # Assert
        assert mock_sql.call_count == 2

    def test_show(self, mocker):
        # Arrange
        mock_spark_engine_sql = mocker.patch("hsfs.engine.spark.Engine.sql")

        spark_engine = spark.Engine()

        # Act
        spark_engine.show(
            sql_query=None,
            feature_store=None,
            n=None,
            online_conn=None,
        )

        # Assert
        assert mock_spark_engine_sql.call_count == 1

    def test_set_job_group(self, mocker):
        # Arrange
        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.set_job_group(
            group_id=None,
            description=None,
        )

        # Assert
        mock_spark_context.setJobGroup.assert_called_once()

    def test_register_external_temporary_table(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_sc_read = mocker.patch("hsfs.storage_connector.JdbcConnector.read")

        spark_engine = spark.Engine()

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            connection_string="",
            arguments="",
        )

        external_fg = feature_group.ExternalFeatureGroup(
            id=10,
            location="test_location",
            data_source=ds.DataSource(storage_connector=jdbc_connector),
        )

        # Act
        spark_engine.register_external_temporary_table(
            external_fg=external_fg,
            alias=None,
        )

        # Assert
        assert mock_sc_read.return_value.createOrReplaceTempView.call_count == 1

    def test_register_hudi_temporary_table(self, mocker):
        # Arrange
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")
        mocker.patch("hsfs.feature_group.FeatureGroup.from_response_json")

        spark_engine = spark.Engine()

        hudi_fg_alias = hudi_feature_group_alias.HudiFeatureGroupAlias(
            feature_group=None, alias=None
        )

        # Act
        spark_engine.register_hudi_temporary_table(
            hudi_fg_alias=hudi_fg_alias,
            feature_store_id=None,
            feature_store_name=None,
            read_options=None,
        )

        # Assert
        assert mock_hudi_engine.return_value.register_temporary_table.call_count == 1

    def test_register_delta_temporary_table(self, mocker):
        # Arrange
        mock_delta_engine = mocker.patch("hsfs.core.delta_engine.DeltaEngine")
        mocker.patch("hsfs.feature_group.FeatureGroup.from_response_json")

        spark_engine = spark.Engine()

        hudi_fg_alias = hudi_feature_group_alias.HudiFeatureGroupAlias(
            feature_group=None, alias=None
        )

        # Act
        spark_engine.register_delta_temporary_table(
            delta_fg_alias=hudi_fg_alias,
            feature_store_id=None,
            feature_store_name=None,
            read_options=None,
        )

        # Assert
        assert mock_delta_engine.return_value.register_temporary_table.call_count == 1

    def test_return_dataframe_type_default(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock()

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df

    def test_return_dataframe_type_spark(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock()

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="spark",
        )

        # Assert
        assert result == mock_df

    def test_return_dataframe_type_pandas(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock(spec=DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="pandas",
        )
        # Assert
        assert result == mock_df.toPandas()

        mock_df = mocker.Mock(spec=pd.DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="pandas",
        )
        # Assert
        assert result == mock_df

    def test_return_dataframe_type_numpy(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock(spec=DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="numpy",
        )

        # Assert
        assert result == mock_df.toPandas().values

        mock_df = mocker.Mock(spec=pd.DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="numpy",
        )
        # Assert
        assert result == mock_df.values

    def test_return_dataframe_type_python(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock(spec=DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="python",
        )

        # Assert
        assert result == mock_df.toPandas().values.tolist()

        mock_df = mocker.Mock(spec=pd.DataFrame)

        # Act
        result = spark_engine._return_dataframe_type(
            dataframe=mock_df,
            dataframe_type="python",
        )
        # Assert
        assert result == mock_df.values.tolist()

    def test_return_dataframe_type_other(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_df = mocker.Mock()

        # Act
        with pytest.raises(TypeError) as e_info:
            spark_engine._return_dataframe_type(
                dataframe=mock_df,
                dataframe_type="other",
            )

        # Assert
        assert (
            str(e_info.value)
            == "Dataframe type `other` not supported on this platform."
        )

    def test_convert_to_default_dataframe_list_1dimension(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            spark_engine.convert_to_default_dataframe(
                dataframe=[],
            )

        # Assert
        assert (
            str(e_info.value)
            == "Cannot convert numpy array that do not have two dimensions to a dataframe. The number of dimensions are: 1"
        )

    def test_convert_to_default_dataframe_list(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=[[1, "test_1"], [2, "test_2"]],
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_numpy_array(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=numpy.array([[1, "test_1"], [2, "test_2"]]),
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pandas_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=expected,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pyspark_rdd(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"_1": [1, 2], "_2": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        rdd = spark_engine._spark_session.sparkContext.parallelize(
            [[1, "test_1"], [2, "test_2"]]
        )

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=rdd,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pyspark_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        df = spark_engine._spark_session.createDataFrame(expected)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=df,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) == list(expected)
        for column in list(result_df):
            assert result_df[column].equals(result_df[column])

    def test_convert_to_default_dataframe_pyspark_dataframe_capitalized_columns(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "Col_1": ["test_1", "test_2"]}
        expected = pd.DataFrame(data=d)

        df = spark_engine._spark_session.createDataFrame(expected)

        # Act
        result = spark_engine.convert_to_default_dataframe(
            dataframe=df,
        )

        # Assert
        result_df = result.toPandas()
        assert list(result_df) != list(expected)
        for column in list(result_df):
            assert result_df[util.autofix_feature_name(column)].equals(
                result_df[column]
            )

    def test_convert_to_default_dataframe_wrong_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            spark_engine.convert_to_default_dataframe(
                dataframe=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "The provided dataframe type is not recognized. Supported types are: spark rdds, spark dataframes, pandas dataframes, python 2D lists, and numpy 2D arrays. The provided dataframe has type: <class 'NoneType'>"
        )

    def test_convert_to_default_dataframe_nullable(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [None, "test_2"]}
        data = pd.DataFrame(data=d)

        schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=False),
                StructField("col_1", StringType(), nullable=False),
                StructField("col_2", StringType(), nullable=True),
            ]
        )
        original_df = spark_engine._spark_session.createDataFrame(data, schema=schema)

        # Act
        result_df = spark_engine.convert_to_default_dataframe(dataframe=original_df)

        # Assert
        original_schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=False),
                StructField("col_1", StringType(), nullable=False),
                StructField("col_2", StringType(), nullable=True),
            ]
        )
        result_schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=True),
                StructField("col_1", StringType(), nullable=True),
                StructField("col_2", StringType(), nullable=True),
            ]
        )

        assert original_schema == original_df.schema
        assert result_schema == result_df.schema

    def test_convert_to_default_dataframe_nullable_uppercase_and_spaced(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"COL_0": [1, 2], "COL_1": ["test_1", "test_2"], "COL 2": [None, "test_2"]}
        data = pd.DataFrame(data=d)

        schema = StructType(
            [
                StructField("COL_0", IntegerType(), nullable=False),
                StructField("COL_1", StringType(), nullable=False),
                StructField("COL 2", StringType(), nullable=True),
            ]
        )
        original_df = spark_engine._spark_session.createDataFrame(data, schema=schema)

        # Act
        result_df = spark_engine.convert_to_default_dataframe(dataframe=original_df)

        # Assert
        original_schema = StructType(
            [
                StructField("COL_0", IntegerType(), nullable=False),
                StructField("COL_1", StringType(), nullable=False),
                StructField("COL 2", StringType(), nullable=True),
            ]
        )
        result_schema = StructType(
            [
                StructField("col_0", IntegerType(), nullable=True),
                StructField("col_1", StringType(), nullable=True),
                StructField("col_2", StringType(), nullable=True),
            ]
        )

        assert original_schema == original_df.schema
        assert result_schema == result_df.schema

    def test_save_dataframe(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_offline(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage="offline",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_offline_online_enabled(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=True,
            storage="offline",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_online(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage="online",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_storage_online_online_enabled(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=True,
            storage="online",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 1
        assert mock_spark_engine_save_offline_dataframe.call_count == 0

    def test_save_dataframe_online_enabled(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=True,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 1
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_fg_stream(self, mocker):
        # Arrange
        mock_spark_engine_save_online_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_online_dataframe"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=True,
        )

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_spark_engine_save_online_dataframe.call_count == 1
        assert mock_spark_engine_save_offline_dataframe.call_count == 0

    def test_save_dataframe_delta_calls_check_duplicate_records(self, mocker):
        # Arrange
        mock_check_duplicate_records = mocker.patch(
            "hsfs.engine.spark.Engine._check_duplicate_records"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["pk1"],
            partition_key=[],
            id=10,
            time_travel_format="DELTA",
        )

        mock_dataframe = mocker.Mock(spec=DataFrame)

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=mock_dataframe,
            operation="insert",
            online_enabled=False,
            storage="offline",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_check_duplicate_records.call_count == 1
        mock_check_duplicate_records.assert_called_once_with(mock_dataframe, fg)
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_dataframe_non_delta_does_not_call_check_duplicate_records(
        self, mocker
    ):
        # Arrange
        mock_check_duplicate_records = mocker.patch(
            "hsfs.engine.spark.Engine._check_duplicate_records"
        )
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["pk1"],
            partition_key=[],
            id=10,
            time_travel_format="HUDI",
        )

        mock_dataframe = mocker.Mock(spec=DataFrame)

        # Act
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=mock_dataframe,
            operation="insert",
            online_enabled=False,
            storage="offline",
            offline_write_options=None,
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_check_duplicate_records.call_count == 0
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    @pytest.mark.parametrize(
        "test_name,primary_key,partition_key,event_time,data",
        [
            (
                "duplicate_primary_key",
                ["id"],
                [],
                None,
                [
                    {"id": 1, "text": "a"},
                    {"id": 1, "text": "a_dup"},
                    {"id": 2, "text": "b"},
                ],
            ),
            (
                "duplicate_primary_key_partition",
                ["id"],
                ["p"],
                None,
                [
                    {"id": 1, "p": 0, "text": "a_p0"},
                    {"id": 1, "p": 0, "text": "a_p0_dup"},
                    {"id": 2, "p": 0, "text": "b_p0"},
                ],
            ),
            (
                "duplicate_primary_key_event_time",
                ["id"],
                [],
                "event_time",
                [
                    {"id": 1, "event_time": "2024-01-01", "text": "a_t1"},
                    {"id": 1, "event_time": "2024-01-01", "text": "a_t1_dup"},
                    {"id": 2, "event_time": "2024-01-02", "text": "b_t2"},
                ],
            ),
        ],
    )
    def test_save_dataframe_delta_duplicate_should_fail(
        self, mocker, test_name, primary_key, partition_key, event_time, data
    ):
        # Arrange
        from datetime import datetime

        mocker.patch("hsfs.engine.get_type", return_value="spark")

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name=f"dl_dup_{test_name}",
            version=1,
            featurestore_id=99,
            primary_key=primary_key,
            partition_key=partition_key,
            event_time=event_time,
            time_travel_format="DELTA",
        )

        # Convert event_time strings to datetime if needed
        if event_time and any(isinstance(row.get(event_time), str) for row in data):
            for row in data:
                if event_time in row and isinstance(row[event_time], str):
                    row[event_time] = datetime.fromisoformat(row[event_time])

        df = spark_engine._spark_session.createDataFrame(data)

        # Act & Assert
        with pytest.raises(exceptions.FeatureStoreException) as exc_info:
            spark_engine.save_dataframe(
                feature_group=fg,
                dataframe=df,
                operation="insert",
                online_enabled=True,
                storage="offline",
                offline_write_options={},
                online_write_options={},
                validation_id=None,
            )

        assert exceptions.FeatureStoreException.DUPLICATE_RECORD_ERROR_MESSAGE in str(
            exc_info.value
        )

    @pytest.mark.parametrize(
        "test_name,primary_key,partition_key,event_time,data_factory",
        [
            (
                "pk_partition_across",
                ["id"],
                ["p"],
                None,
                lambda dt: [
                    {"id": 1, "p": 0, "text": "a_p0"},
                    {"id": 1, "p": 1, "text": "a_p1"},
                    {"id": 2, "p": 0, "text": "b_p0"},
                ],
            ),
            (
                "pk_event_time_across",
                ["id"],
                [],
                "event_time",
                lambda dt: [
                    {"id": 1, "event_time": dt.datetime(2024, 1, 1), "text": "a_t1"},
                    {"id": 1, "event_time": dt.datetime(2024, 1, 2), "text": "a_t2"},
                    {"id": 2, "event_time": dt.datetime(2024, 1, 1), "text": "b_t1"},
                ],
            ),
            (
                "pk_with_no_duplicate",
                ["id"],
                [],
                None,
                lambda dt: [
                    {"id": 1, "text": "a"},
                    {"id": 2, "text": "b"},
                    {"id": 3, "text": "c"},
                ],
            ),
            (
                "no_pk_partition_only",
                [],
                ["p"],
                None,
                lambda dt: [
                    {"id": 1, "p": 0, "text": "a_p0"},
                    {"id": 1, "p": 1, "text": "a_p1"},
                    {"id": 2, "p": 0, "text": "b_p0"},
                ],
            ),
            (
                "no_pk_event_time_only",
                [],
                [],
                "event_time",
                lambda dt: [
                    {"id": 1, "event_time": dt.datetime(2024, 1, 1), "text": "a_t1"},
                    {"id": 1, "event_time": dt.datetime(2024, 1, 2), "text": "a_t2"},
                    {"id": 2, "event_time": dt.datetime(2024, 1, 1), "text": "b_t1"},
                ],
            ),
            (
                "no_pk",
                [],
                [],
                None,
                lambda dt: [
                    {"id": 1, "text": "a"},
                    {"id": 1, "text": "a_dup"},
                    {"id": 2, "text": "b"},
                ],
            ),
        ],
    )
    def test_save_dataframe_delta_duplicate_should_succeed(
        self, mocker, test_name, primary_key, partition_key, event_time, data_factory
    ):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        mock_spark_engine_save_offline_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine._save_offline_dataframe"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name=f"dl_dup_{test_name}",
            version=1,
            featurestore_id=99,
            primary_key=primary_key,
            partition_key=partition_key,
            event_time=event_time,
            time_travel_format="DELTA",
        )

        data = data_factory(datetime)
        df = spark_engine._spark_session.createDataFrame(data)

        # Act - should not raise exception
        spark_engine.save_dataframe(
            feature_group=fg,
            dataframe=df,
            operation="insert",
            online_enabled=True,
            storage="offline",
            offline_write_options={},
            online_write_options={},
            validation_id=None,
        )

        # Assert - no exception should be raised, and save should be called
        assert mock_spark_engine_save_offline_dataframe.call_count == 1

    def test_save_stream_dataframe(self, mocker, backend_fixtures):
        # Arrange
        mock_common_client_get_instance = mocker.patch(
            "hopsworks_common.client.get_instance"
        )
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mock_spark_engine_serialize_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._serialize_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()
        project_id = 1
        fg.feature_store.project_id = project_id

        mock_common_client_get_instance.return_value._project_name = "test_project_name"

        df = pd.DataFrame(data={"col_0": [1, 2], "col_1": ["test_1", "test_2"]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=spark_df,
            query_name=None,
            output_mode="test_mode",
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == f"/Projects/test_project_name/Resources/{self._get_spark_query_name(project_id, fg)}-checkpoint"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == self._get_spark_query_name(project_id, fg)
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 0
        )

    def test_save_stream_dataframe_query_name(self, mocker, backend_fixtures):
        # Arrange
        mock_common_client_get_instance = mocker.patch(
            "hopsworks_common.client.get_instance"
        )
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mock_spark_engine_serialize_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._serialize_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()

        mock_common_client_get_instance.return_value._project_name = "test_project_name"

        df = pd.DataFrame(data={"col_0": [1, 2], "col_1": ["test_1", "test_2"]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=spark_df,
            query_name="test_query_name",
            output_mode="test_mode",
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == "/Projects/test_project_name/Resources/test_query_name-checkpoint"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == "test_query_name"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 0
        )

    def _get_spark_query_name(self, project_id, feature_group):
        return (
            f"insert_stream_{project_id}_{feature_group.id}"
            f"_{feature_group.name}_{feature_group.version}_onlinefs"
        )

    def test_save_stream_dataframe_checkpoint_dir(self, mocker, backend_fixtures):
        # Arrange
        mock_common_client_get_instance = mocker.patch(
            "hopsworks_common.client.get_instance"
        )
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mock_spark_engine_serialize_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._serialize_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()
        project_id = 1
        fg.feature_store.project_id = project_id

        mock_common_client_get_instance.return_value._project_name = "test_project_name"

        df = pd.DataFrame(data={"col_0": [1, 2], "col_1": ["test_1", "test_2"]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=spark_df,
            query_name=None,
            output_mode="test_mode",
            await_termination=None,
            timeout=None,
            checkpoint_dir="test_checkpoint_dir",
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == "test_checkpoint_dir"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == self._get_spark_query_name(project_id, fg)
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 0
        )

    def test_save_stream_dataframe_await_termination(self, mocker, backend_fixtures):
        # Arrange
        mock_common_client_get_instance = mocker.patch(
            "hopsworks_common.client.get_instance"
        )
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mock_spark_engine_serialize_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._serialize_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()
        project_id = 1
        fg.feature_store.project_id = project_id

        mock_common_client_get_instance.return_value._project_name = "test_project_name"

        df = pd.DataFrame(data={"col_0": [1, 2], "col_1": ["test_1", "test_2"]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        spark_engine.save_stream_dataframe(
            feature_group=fg,
            dataframe=spark_df,
            query_name=None,
            output_mode="test_mode",
            await_termination=True,
            timeout=123,
            checkpoint_dir=None,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.call_args[
                0
            ][0]
            == "test_mode"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][0]
            == "checkpointLocation"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.call_args[
                0
            ][1]
            == f"/Projects/test_project_name/Resources/{self._get_spark_query_name(project_id, fg)}-checkpoint"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.call_args[
                0
            ][0]
            == self._get_spark_query_name(project_id, fg)
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_count
            == 1
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.writeStream.outputMode.return_value.format.return_value.option.return_value.options.return_value.option.return_value.queryName.return_value.start.return_value.awaitTermination.call_args[
                0
            ][0]
            == 123
        )

    def test_save_offline_dataframe(self, mocker):
        # Arrange
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._get_table_name",
            return_value="test_get_table_name",
        )
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test", version=1, featurestore_id=99, primary_key=[], id=10
        )
        fg.feature_store = mocker.Mock()

        mock_df = mocker.Mock()

        # Act
        spark_engine._save_offline_dataframe(
            feature_group=fg,
            dataframe=mock_df,
            operation=None,
            write_options={"test_name": "test_value"},
            validation_id=None,
        )

        # Assert
        assert mock_df.write.format.call_count == 1
        assert mock_hudi_engine.return_value.save_hudi_fg.call_count == 0
        assert mock_df.write.format.call_args[0][0] == "hive"
        assert mock_df.write.format.return_value.mode.call_args[0][0] == "append"
        assert mock_df.write.format.return_value.mode.return_value.options.call_args[
            1
        ] == {"test_name": "test_value"}
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.call_args[
                0
            ][0]
            == []
        )
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.return_value.saveAsTable.call_args[
                0
            ][0]
            == "test_get_table_name"
        )

    def test_save_offline_dataframe_partition_by(self, mocker):
        # Arrange
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._get_table_name",
            return_value="test_get_table_name",
        )
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        spark_engine = spark.Engine()

        f = feature.Feature(name="f", type="str", partition=True)
        f1 = feature.Feature(name="f1", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=[f, f1],
        )

        mock_df = mocker.Mock()

        # Act
        spark_engine._save_offline_dataframe(
            feature_group=fg,
            dataframe=mock_df,
            operation=None,
            write_options={"test_name": "test_value"},
            validation_id=None,
        )

        # Assert
        assert mock_df.write.format.call_count == 1
        assert mock_hudi_engine.return_value.save_hudi_fg.call_count == 0
        assert mock_df.write.format.call_args[0][0] == "hive"
        assert mock_df.write.format.return_value.mode.call_args[0][0] == "append"
        assert mock_df.write.format.return_value.mode.return_value.options.call_args[
            1
        ] == {"test_name": "test_value"}
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.call_args[
                0
            ][0]
            == ["f"]
        )
        assert (
            mock_df.write.format.return_value.mode.return_value.options.return_value.partitionBy.return_value.saveAsTable.call_args[
                0
            ][0]
            == "test_get_table_name"
        )

    def test_save_offline_dataframe_hudi_time_travel_format(self, mocker):
        # Arrange
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            time_travel_format="HUDI",
        )

        mock_df = mocker.Mock()

        # Act
        spark_engine._save_offline_dataframe(
            feature_group=fg,
            dataframe=mock_df,
            operation=None,
            write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_df.write.format.call_count == 0
        assert mock_hudi_engine.return_value.save_hudi_fg.call_count == 1

    def test_save_online_dataframe(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mock_spark_engine_serialize_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._serialize_to_avro"
        )

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )
        json = backend_fixtures["storage_connector"]["get_kafka_external"]["response"]
        sc = storage_connector.StorageConnector.from_response_json(json)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()

        df = pd.DataFrame(data={"col_0": [1, 2], "col_1": ["test_1", "test_2"]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        spark_engine._save_online_dataframe(
            feature_group=fg,
            dataframe=spark_df,
            write_options={"test_name": "test_value"},
        )

        # Assert
        assert mock_spark_engine_serialize_to_avro.call_count == 1
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.call_args[0][0]
            == "headers"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.write.format.call_args[
                0
            ][0]
            == "kafka"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.write.format.return_value.options.call_args[
                1
            ]
            == {
                "kafka.bootstrap.servers": "test_bootstrap_servers",
                "kafka.security.protocol": "test_security_protocol",
                "kafka.ssl.endpoint.identification.algorithm": "test_ssl_endpoint_identification_algorithm",
                "kafka.ssl.key.password": "test_ssl_key_password",
                "kafka.ssl.keystore.location": "result_from_add_file",
                "kafka.ssl.keystore.password": "test_ssl_keystore_password",
                "kafka.ssl.truststore.location": "result_from_add_file",
                "kafka.ssl.truststore.password": "test_ssl_truststore_password",
                "kafka.test_option_name": "test_option_value",
                "test_name": "test_value",
            }
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.write.format.return_value.options.return_value.option.call_args[
                0
            ][0]
            == "topic"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.write.format.return_value.options.return_value.option.call_args[
                0
            ][1]
            == "test_online_topic_name"
        )
        assert (
            mock_spark_engine_serialize_to_avro.return_value.withColumn.return_value.write.format.return_value.options.return_value.option.return_value.save.call_count
            == 1
        )

    def test_save_online_dataframe_sends_num_entries_by_default(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mock_spark_engine_serialize_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._serialize_to_avro"
        )
        mock_get_headers = mocker.patch("hsfs.engine.spark.Engine._get_headers")

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )
        json_data = backend_fixtures["storage_connector"]["get_kafka_external"][
            "response"
        ]
        sc = storage_connector.StorageConnector.from_response_json(json_data)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()

        df = pd.DataFrame(data={"col_0": [1, 2], "col_1": ["test_1", "test_2"]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        spark_engine._save_online_dataframe(
            feature_group=fg,
            dataframe=spark_df,
            write_options={},
        )

        # Assert - num_entries should be dataframe row count (2) when flag is not set
        mock_get_headers.assert_called_once_with(fg, 2)
        mock_spark_engine_serialize_to_avro.assert_called_once()

    def test_save_online_dataframe_disable_online_ingestion_count(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mock_spark_engine_serialize_to_avro = mocker.patch(
            "hsfs.engine.spark.Engine._serialize_to_avro"
        )
        mock_get_headers = mocker.patch("hsfs.engine.spark.Engine._get_headers")

        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"
        mock_engine_get_instance.return_value.add_file.return_value = (
            "result_from_add_file"
        )

        mock_storage_connector_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )
        json_data = backend_fixtures["storage_connector"]["get_kafka_external"][
            "response"
        ]
        sc = storage_connector.StorageConnector.from_response_json(json_data)
        mock_storage_connector_api.return_value.get_kafka_connector.return_value = sc

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            online_topic_name="test_online_topic_name",
        )
        fg.feature_store = mocker.Mock()

        df = pd.DataFrame(data={"col_0": [1, 2], "col_1": ["test_1", "test_2"]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        spark_engine._save_online_dataframe(
            feature_group=fg,
            dataframe=spark_df,
            write_options={"disable_online_ingestion_count": True},
        )

        # Assert - num_entries should be None when disable_online_ingestion_count is True
        mock_get_headers.assert_called_once_with(fg, None)
        mock_spark_engine_serialize_to_avro.assert_called_once()

    def test_serialize_to_avro(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_to_avro = mocker.patch("hsfs.engine.spark.to_avro")
        mock_to_avro.return_value = lit(b"111")

        fg_data = []
        fg_data.append(("ekarson", ["GRAVITY RUSH 2", "KING'S QUEST"]))
        fg_data.append(("ratmilkdrinker", ["NBA 2K", "CALL OF DUTY"]))
        pandas_df = pd.DataFrame(fg_data, columns=["account_id", "last_played_games"])

        df = spark_engine._spark_session.createDataFrame(pandas_df)

        features = [
            feature.Feature(name="account_id", type="str"),
            feature.Feature(name="last_played_games", type="array"),
        ]

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=features,
        )
        fg._subject = {
            "id": 1025,
            "subject": "fg_1",
            "version": 1,
            "schema": '{"type":"record","name":"fg_1","namespace":"test_featurestore.db","fields":[{"name":"account_id","type":["null","string"]},{"name":"last_played_games","type":["null",{"type":"array","items":["null","string"]}]}]}',
        }

        # Act
        serialized_df = spark_engine._serialize_to_avro(
            feature_group=fg,
            dataframe=df,
        )

        # Assert
        assert (
            serialized_df.schema.json()
            == '{"fields":[{"metadata":{},"name":"key","nullable":false,"type":"binary"},{"metadata":{},"name":"value","nullable":false,"type":"binary"}],"type":"struct"}'
        )

    def test_deserialize_from_avro(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        data = []
        data.append((b"2121", b"21212121"))
        data.append((b"1212", b"12121212"))
        pandas_df = pd.DataFrame(data, columns=["key", "value"])

        df = spark_engine._spark_session.createDataFrame(pandas_df)

        features = [
            feature.Feature(name="account_id", type="str"),
            feature.Feature(name="last_played_games", type="array"),
            feature.Feature(name="event_time", type="timestamp"),
        ]

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=features,
        )
        fg._subject = {
            "id": 1025,
            "subject": "fg_1",
            "version": 1,
            "schema": '{"type":"record","name":"fg_1","namespace":"test_featurestore.db","fields":[{"name":"account_id","type":["null","string"]},{"name":"last_played_games","type":["null",{"type":"array","items":["null","string"]}]},{"name":"event_time","type":["null",{"type":"long","logicalType":"timestamp-micros"}]}]}',
        }

        # Act
        deserialized_df = spark_engine._deserialize_from_avro(
            feature_group=fg,
            dataframe=df,
        )

        # Assert
        expected_schema = json.loads("""{
            "fields": [
                {"metadata": {}, "name": "key", "nullable": true, "type": "binary"},
                {"metadata": {}, "name": "value", "nullable": false, "type": {
                    "fields": [
                        {"metadata": {}, "name": "account_id", "nullable": true, "type": "string"},
                        {"metadata": {}, "name": "last_played_games", "nullable": true, "type": {
                            "containsNull": true, "elementType": "string", "type": "array"}},
                        {"metadata": {}, "name": "event_time", "nullable": true, "type": "timestamp"}
                    ],
                    "type": "struct"
                }}
            ],
            "type": "struct"
        }""")

        actual_schema = json.loads(deserialized_df.schema.json())
        assert actual_schema == expected_schema

    def test_serialize_deserialize_avro(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        now = datetime.datetime.now()

        fg_data = []
        fg_data.append(
            ("ekarson", ["GRAVITY RUSH 2", "KING'S QUEST"], pd.Timestamp(now))
        )
        fg_data.append(
            ("ratmilkdrinker", ["NBA 2K", "CALL OF DUTY"], pd.Timestamp(now))
        )
        pandas_df = pd.DataFrame(
            fg_data, columns=["account_id", "last_played_games", "event_time"]
        )

        df = spark_engine._spark_session.createDataFrame(pandas_df)

        features = [
            feature.Feature(name="account_id", type="str"),
            feature.Feature(name="last_played_games", type="array"),
            feature.Feature(name="event_time", type="timestamp"),
        ]

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=features,
        )
        fg._subject = {
            "id": 1025,
            "subject": "fg_1",
            "version": 1,
            "schema": '{"type":"record","name":"fg_1","namespace":"test_featurestore.db","fields":[{"name":"account_id","type":["null","string"]},{"name":"last_played_games","type":["null",{"type":"array","items":["null","string"]}]},{"name":"event_time","type":["null",{"type":"long","logicalType":"timestamp-micros"}]}]}',
        }

        # Act
        serialized_df = spark_engine._serialize_to_avro(
            feature_group=fg,
            dataframe=df,
        )

        deserialized_df = spark_engine._deserialize_from_avro(
            feature_group=fg,
            dataframe=serialized_df,
        )

        # Assert
        assert (
            serialized_df.schema.json()
            == '{"fields":[{"metadata":{},"name":"key","nullable":false,"type":"binary"},{"metadata":{},"name":"value","nullable":false,"type":"binary"}],"type":"struct"}'
        )
        assert df.schema == deserialized_df.schema["value"].dataType
        assert df.collect() == deserialized_df.select("value.*").collect()

    def test_serialize_deserialize_avro_with_struct_feature(self, mocker, spark_engine):
        # Arrange
        now = datetime.datetime.now()

        fg_data = []
        fg_data.append(
            ("ekarson", ["GRAVITY RUSH 2", "KING'S QUEST"], now, {"value": "test"})
        )
        fg_data.append(
            ("ratmilkdrinker", ["NBA 2K", "CALL OF DUTY"], now, {"value": "test"})
        )

        schema = StructType(
            [
                StructField("account_id", StringType(), True),
                StructField("last_played_games", ArrayType(StringType()), True),
                StructField("event_time", TimestampType(), True),
                StructField(
                    "struct_feature",
                    StructType([StructField("value", StringType(), True)]),
                ),
            ]
        )

        df = spark_engine._spark_session.createDataFrame(data=fg_data, schema=schema)

        features = [
            feature.Feature(name="account_id", type="str"),
            feature.Feature(name="last_played_games", type="array"),
            feature.Feature(name="event_time", type="timestamp"),
            feature.Feature(name="struct_feature", type="struct<value:string>"),
        ]

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            features=features,
        )
        fg._subject = {
            "id": 1025,
            "subject": "fg_1",
            "version": 1,
            "schema": '{"type":"record","name":"fg_1","namespace":"test_featurestore.db","fields":[{"name":"account_id","type":["null","string"]},{"name":"last_played_games","type":["null",{"type":"array","items":["null","string"]}]},{"name":"event_time","type":["null",{"type":"long","logicalType":"timestamp-micros"}]},{"name":"struct_feature","type":["null",{"type":"record","name":"S_struct_feature","fields":[{"name":"value","type":["null","string"]}]}]}]}',
        }

        # Act
        serialized_df = spark_engine._serialize_to_avro(
            feature_group=fg,
            dataframe=df,
        )

        deserialized_df = spark_engine._deserialize_from_avro(
            feature_group=fg,
            dataframe=serialized_df,
        )

        # Assert
        assert (
            serialized_df.schema.json()
            == '{"fields":[{"metadata":{},"name":"key","nullable":false,"type":"binary"},{"metadata":{},"name":"value","nullable":false,"type":"binary"}],"type":"struct"}'
        )
        assert df.schema == deserialized_df.schema["value"].dataType
        assert df.collect() == deserialized_df.select("value.*").collect()

    def test_get_training_data(self, mocker):
        # Arrange
        mock_spark_engine_write_training_dataset = mocker.patch(
            "hsfs.engine.spark.Engine.write_training_dataset"
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.get_training_data(
            training_dataset=None,
            feature_view_obj=None,
            query_obj=None,
            read_options=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_spark_engine_write_training_dataset.call_count == 1

    def test_split_labels(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": ["test_1", "test_2"], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        # Act
        result = spark_engine.split_labels(df=df, labels=None, dataframe_type="default")

        # Assert
        assert result == (df, None)

    def test_split_labels_labels(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df_new = pd.DataFrame(data={"col_1": ["test_1", "test_2"]})
        expected_labels_df = pd.DataFrame(data={"col_0": [1, 2]})

        # Act
        df_new, labels_df = spark_engine.split_labels(
            df=spark_df, labels=["col_0"], dataframe_type="default"
        )

        # Assert
        result_df_new = df_new.toPandas()
        result_labels_df = labels_df.toPandas()
        assert result_labels_df.equals(expected_labels_df)
        assert result_df_new.equals(expected_df_new)

    def test_write_training_dataset(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            spark_engine.write_training_dataset(
                training_dataset=td,
                query_obj=None,
                user_write_options=None,
                save_mode=None,
                read_options=None,
                feature_view_obj=None,
                to_df=None,
            )

        # Assert
        assert str(e_info.value) == "Dataset should be a query."
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 0
        assert mock_spark_engine_write_training_dataset_splits.call_count == 0

    def test_write_training_dataset_to_df(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        jsonq = backend_fixtures["query"]["get"]["response"]
        q = query.Query.from_response_json(jsonq)

        mock_query_read = mocker.patch("hsfs.constructor.query.Query.read")
        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "col_2": [3, 4],
            "event_time": [1, 2],
        }
        df = pd.DataFrame(data=d)
        query_df = spark_engine._spark_session.createDataFrame(df)
        mock_query_read.side_effect = [query_df]

        td = training_dataset.TrainingDataset(
            name="test",
            version=None,
            splits={},
            event_start_time=None,
            event_end_time=None,
            description="test",
            storage_connector=None,
            featurestore_id=10,
            data_format="tsv",
            location="",
            statistics_config=None,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            extra_filter=None,
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        # Act
        df_returned = spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options={},
            save_mode=training_dataset_engine.TrainingDatasetEngine.OVERWRITE,
            read_options={},
            feature_view_obj=fv,
            to_df=True,
        )

        # Assert
        assert set(df_returned.columns) == {"col_0", "col_1", "col_2", "event_time"}
        assert df_returned.count() == 2
        assert df_returned.exceptAll(query_df).rdd.isEmpty()

    def test_write_training_dataset_split_to_df(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        jsonq = backend_fixtures["query"]["get"]["response"]
        q = query.Query.from_response_json(jsonq)

        mock_query_read = mocker.patch("hsfs.constructor.query.Query.read")
        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "col_2": [3, 4],
            "event_time": [1, 2],
        }
        df = pd.DataFrame(data=d)
        query_df = spark_engine._spark_session.createDataFrame(df)
        mock_query_read.side_effect = [query_df]

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=None,
            splits={},
            test_size=0.5,
            train_start=None,
            train_end=None,
            test_start=None,
            test_end=None,
            time_split_size=2,
            description="test",
            storage_connector=None,
            featurestore_id=12,
            data_format="tsv",
            location="",
            statistics_config=None,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            extra_filter=None,
            seed=1,
        )

        # Act
        split_dfs_returned = spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options={},
            save_mode=training_dataset_engine.TrainingDatasetEngine.OVERWRITE,
            read_options={},
            feature_view_obj=fv,
            to_df=True,
        )

        # Assert
        sum_rows = 0
        for key in split_dfs_returned:
            df_returned = split_dfs_returned[key]
            assert set(df_returned.columns) == {"col_0", "col_1", "col_2", "event_time"}
            sum_rows += df_returned.count()

        assert sum_rows == 2

    def test_write_training_dataset_query(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        q = query.Query(left_feature_group=None, left_features=None)

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 1
        assert mock_spark_engine_write_training_dataset_splits.call_count == 0

    def test_write_training_dataset_query_coalesce(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            coalesce=True,
        )

        q = query.Query(left_feature_group=None, left_features=None)

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 1
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 1
        assert mock_spark_engine_write_training_dataset_splits.call_count == 0

    def test_write_training_dataset_td_splits(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mock_spark_engine_split_df = mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"name": "value"},
        )

        q = query.Query(left_feature_group=None, left_features=None)

        m = mocker.Mock()

        mock_spark_engine_split_df.return_value = {"temp": m}

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 0
        assert m.coalesce.call_count == 0
        assert mock_spark_engine_write_training_dataset_splits.call_count == 1

    def test_write_training_dataset_td_splits_coalesce(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mocker.patch("hsfs.engine.spark.Engine.write_options")
        mock_spark_engine_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.convert_to_default_dataframe"
        )
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )
        mock_spark_engine_split_df = mocker.patch("hsfs.engine.spark.Engine._split_df")
        mock_spark_engine_write_training_dataset_splits = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_splits"
        )

        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[],
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"name": "value"},
            coalesce=True,
        )

        q = query.Query(left_feature_group=None, left_features=None)

        m = mocker.Mock()

        mock_spark_engine_split_df.return_value = {"temp": m}

        # Act
        spark_engine.write_training_dataset(
            training_dataset=td,
            query_obj=q,
            user_write_options=None,
            save_mode=None,
            read_options=None,
            feature_view_obj=fv,
            to_df=None,
        )

        # Assert
        assert (
            mock_spark_engine_convert_to_default_dataframe.return_value.coalesce.call_count
            == 0
        )
        assert mock_spark_engine_write_training_dataset_single.call_count == 0
        assert m.coalesce.call_count == 1
        assert mock_spark_engine_write_training_dataset_splits.call_count == 1

    def test_split_df(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_spark_engine_time_series_split = mocker.patch(
            "hsfs.engine.spark.Engine._time_series_split"
        )
        mock_spark_engine_random_split = mocker.patch(
            "hsfs.engine.spark.Engine._random_split"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": 1},
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
        )

        q = query.Query(left_feature_group=fg, left_features=None)

        # Act
        spark_engine._split_df(
            query_obj=q,
            training_dataset=td,
            read_options={},
        )

        # Assert
        assert mock_spark_engine_time_series_split.call_count == 0
        assert mock_spark_engine_random_split.call_count == 1

    def test_split_df_time_split_td_features(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_spark_engine_time_series_split = mocker.patch(
            "hsfs.engine.spark.Engine._time_series_split"
        )
        mock_spark_engine_random_split = mocker.patch(
            "hsfs.engine.spark.Engine._random_split"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": 1},
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            event_time="event_time",
            features=[f, f1, f2],
            featurestore_name="test_fs",
        )

        q = query.Query(left_feature_group=fg, left_features=None)

        # Act
        spark_engine._split_df(
            query_obj=q,
            training_dataset=td,
            read_options={},
        )

        # Assert
        assert mock_spark_engine_time_series_split.call_count == 1
        assert mock_spark_engine_random_split.call_count == 0

    def test_split_df_time_split_query_features(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_spark_engine_time_series_split = mocker.patch(
            "hsfs.engine.spark.Engine._time_series_split"
        )
        mock_spark_engine_random_split = mocker.patch(
            "hsfs.engine.spark.Engine._random_split"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": 1},
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(name="event_time", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[f, f1, f2],
            id=10,
            event_time="event_time",
            featurestore_name="test_fs",
        )

        q = query.Query(left_feature_group=fg, left_features=[f, f1, f2])

        # Act
        spark_engine._split_df(
            query_obj=q,
            training_dataset=td,
            read_options={},
        )

        # Assert
        assert mock_spark_engine_time_series_split.call_count == 1
        assert mock_spark_engine_random_split.call_count == 0

    def test_split_df_time_split_query_features_fully_qualified_name(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_spark_engine_time_series_split = mocker.patch(
            "hsfs.engine.spark.Engine._time_series_split"
        )
        mock_spark_engine_random_split = mocker.patch(
            "hsfs.engine.spark.Engine._random_split"
        )

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": 1},
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")
        f2 = feature.Feature(
            name="event_time", type="str", use_fully_qualified_name=True
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[f, f1, f2],
            id=10,
            event_time="event_time",
            featurestore_name="test_fs",
        )

        q = query.Query(left_feature_group=fg, left_features=[f, f1, f2])

        # Act
        spark_engine._split_df(
            query_obj=q,
            training_dataset=td,
            read_options={},
        )

        # Assert
        assert mock_spark_engine_time_series_split.call_count == 1
        assert mock_spark_engine_random_split.call_count == 0

    def test_random_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            seed=1,
        )

        d = {
            "col_0": [1, 2, 3, 4, 5, 6],
            "col_1": ["test_1", "test_2", "test_3", "test_4", "test_5", "test_6"],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine._random_split(
            dataset=spark_df,
            training_dataset=td,
        )

        # Assert
        assert list(result) == ["test_split1", "test_split2"]
        sum_rows = 0
        for column in list(result):
            assert result[column].schema == spark_df.schema
            sum_rows += result[column].count()
        assert sum_rows == 6

    def test_time_series_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": [1000000000, 2000000000],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_date(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=1488600000,
            test_end=1488718800,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": ["2017-03-04", "2017-03-05"],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        spark_df = spark_df.withColumn(
            "event_time", spark_df["event_time"].cast(DateType())
        )

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )
        train_spark_df = train_spark_df.withColumn(
            "event_time", train_spark_df["event_time"].cast(DateType())
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )
        test_spark_df = test_spark_df.withColumn(
            "event_time", test_spark_df["event_time"].cast(DateType())
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_timestamp(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=1488600000,
            test_end=1488718800,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": ["2017-03-04", "2017-03-05"],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        spark_df = spark_df.withColumn(
            "event_time", spark_df["event_time"].cast(TimestampType())
        )

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )
        train_spark_df = train_spark_df.withColumn(
            "event_time", train_spark_df["event_time"].cast(TimestampType())
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )
        test_spark_df = test_spark_df.withColumn(
            "event_time", test_spark_df["event_time"].cast(TimestampType())
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_epoch_sec(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=1488600001,
            test_end=1488718801,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": [1488600000, 1488718800],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )

        expected = {"train": train_spark_df, "test": test_spark_df}

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_time_series_split_drop_event_time(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        spark_engine = spark.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        d = {
            "col_0": [1, 2],
            "col_1": ["test_1", "test_2"],
            "event_time": [1000000000, 2000000000],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        train_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 1]
        )

        test_spark_df = spark_engine._spark_session.createDataFrame(
            df.loc[df["col_0"] == 2]
        )

        expected = {"train": train_spark_df, "test": test_spark_df}
        expected["train"] = expected["train"].drop("event_time")
        expected["test"] = expected["test"].drop("event_time")

        # Act
        result = spark_engine._time_series_split(
            training_dataset=td,
            dataset=spark_df,
            event_time="event_time",
            drop_event_time=True,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].schema == expected[column].schema
            assert result[column].collect() == expected[column].collect()

    def test_write_training_dataset_splits(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )

        spark_engine = spark.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=plus_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = training_dataset_feature.TrainingDatasetFeature(
            name="col_0", type=IntegerType(), index=0
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="col_1", type=StringType(), index=1
        )
        features = [f, f1]

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            features=features,
        )

        # Act
        result = spark_engine._write_training_dataset_splits(
            training_dataset=td,
            feature_dataframes={"col_0": None, "col_1": None},
            write_options=None,
            save_mode=None,
            to_df=False,
            transformation_functions=[tf("col_0")],
        )

        # Assert
        assert result is None
        assert mock_spark_engine_write_training_dataset_single.call_count == 2

    def test_write_training_dataset_splits_to_df(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_spark_engine_write_training_dataset_single = mocker.patch(
            "hsfs.engine.spark.Engine._write_training_dataset_single"
        )

        spark_engine = spark.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=plus_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        transformation_fn_dict = {}

        transformation_fn_dict["col_0"] = tf

        f = training_dataset_feature.TrainingDatasetFeature(
            name="col_0", type=IntegerType(), index=0
        )
        f1 = training_dataset_feature.TrainingDatasetFeature(
            name="col_1", type=StringType(), index=1
        )
        features = [f, f1]

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            transformation_functions=transformation_fn_dict,
            features=features,
        )

        # Act
        result = spark_engine._write_training_dataset_splits(
            training_dataset=td,
            feature_dataframes={"col_0": None, "col_1": None},
            write_options=None,
            save_mode=None,
            to_df=True,
            transformation_functions=[tf("col_0")],
        )

        # Assert
        assert result is not None
        assert mock_spark_engine_write_training_dataset_single.call_count == 2

    def test_write_training_dataset_single(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_transformation_function_engine_apply_transformation_functions = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        @udf(int)
        def add_one(feature):
            return feature + 1

        tf = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._write_training_dataset_single(
            transformation_functions=[tf],
            feature_dataframe=pd.DataFrame({"feature": [1]}),
            storage_connector=None,
            data_format="csv",
            write_options={},
            save_mode=None,
            path=None,
            to_df=False,
        )

        # Assert
        assert (
            mock_transformation_function_engine_apply_transformation_functions.call_count
            == 1
        )
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_transformation_function_engine_apply_transformation_functions.return_value.write.format.call_args[
                0
            ][0]
            == "csv"
        )

    def test_write_training_dataset_single_tsv(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_transformation_function_engine_apply_transformation_functions = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        @udf(int)
        def add_one(feature):
            return feature + 1

        tf = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._write_training_dataset_single(
            transformation_functions=[tf],
            feature_dataframe=pd.DataFrame({"feature": [1]}),
            storage_connector=None,
            data_format="tsv",
            write_options={},
            save_mode=None,
            path=None,
            to_df=False,
        )

        # Assert
        assert (
            mock_transformation_function_engine_apply_transformation_functions.call_count
            == 1
        )
        assert mock_spark_engine_setup_storage_connector.call_count == 1
        assert (
            mock_transformation_function_engine_apply_transformation_functions.return_value.write.format.call_args[
                0
            ][0]
            == "csv"
        )

    def test_write_training_dataset_single_to_df(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_transformation_function_engine_apply_transformation_functions = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions"
        )
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        @udf(int)
        def add_one(feature):
            return feature + 1

        tf = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine._write_training_dataset_single(
            transformation_functions=[tf],
            feature_dataframe=pd.DataFrame({"feature": [1]}),
            storage_connector=None,
            data_format=None,
            write_options={},
            save_mode=None,
            path=None,
            to_df=True,
        )

        # Assert
        assert (
            mock_transformation_function_engine_apply_transformation_functions.call_count
            == 1
        )
        assert mock_spark_engine_setup_storage_connector.call_count == 0

    def test_read_none_data_format(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            spark_engine.read(
                storage_connector=None,
                data_format=None,
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_empty_data_format(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            spark_engine.read(
                storage_connector=None,
                data_format="",
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_read_options(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="csv",
            read_options={"name": "value"},
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("csv")
        mock_read.format.return_value.options.assert_called_once_with(name="value")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(None, None)

    def test_read_location_format_delta(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="delta",
            read_options={"header": "true"},
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("delta")
        mock_read.format.return_value.options.assert_called_once_with(header="true")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(
            None, "test_location"
        )

    def test_read_location_format_parquet(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="parquet",
            read_options={"header": "true"},
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("parquet")
        mock_read.format.return_value.options.assert_called_once_with(header="true")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(
            None, "test_location"
        )

    def test_read_location_format_hudi(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="hudi",
            read_options={"header": "true"},
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("hudi")
        mock_read.format.return_value.options.assert_called_once_with(header="true")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(
            None, "test_location"
        )

    def test_read_location_format_orc(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="orc",
            read_options={"header": "true"},
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("orc")
        mock_read.format.return_value.options.assert_called_once_with(header="true")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(
            None, "test_location"
        )

    def test_read_location_format_bigquery(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="bigquery",
            read_options={"header": "true"},
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("bigquery")
        mock_read.format.return_value.options.assert_called_once_with(header="true")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(
            None, "test_location"
        )

    def test_read_location_format_csv(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="csv",
            read_options={"header": "true"},
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("csv")
        mock_read.format.return_value.options.assert_called_once_with(header="true")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(
            None, "test_location/**"
        )

    def test_read_location_format_tsv(self, mocker):
        # Arrange
        mock_df = MagicMock(name="DataFrame")

        mock_read = MagicMock()
        mock_read.format.return_value.options.return_value.load.return_value = mock_df
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "read",
            new_callable=PropertyMock,
            return_value=mock_read,
        )

        # Patch Engine.setup_storage_connector as before
        mock_spark_engine_setup_storage_connector = mocker.patch(
            "hsfs.engine.spark.Engine.setup_storage_connector"
        )

        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read(
            storage_connector=None,
            data_format="csv",
            read_options={"header": "true"},
            location="test_location",
            dataframe_type="default",
        )

        # Assert
        assert result == mock_df
        mock_read.format.assert_called_once_with("csv")
        mock_read.format.return_value.options.assert_called_once_with(header="true")
        mock_read.format.return_value.options.return_value.load.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once()
        mock_spark_engine_setup_storage_connector.assert_called_once_with(
            None, "test_location/**"
        )

    def test_read_stream(self, mocker):
        # Arrange
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mock_engine_get_instance.return_value.get_spark_version.return_value = "3.1.0"

        mock_read_stream = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "readStream",
            new_callable=PropertyMock,
            return_value=mock_read_stream,
        )

        mock_spark_engine_read_stream_kafka = mocker.patch(
            "hsfs.engine.spark.Engine._read_stream_kafka"
        )

        spark_engine = spark.Engine()

        kafka_connector = storage_connector.KafkaConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        result = spark_engine.read_stream(
            storage_connector=kafka_connector,
            message_format=None,
            schema=None,
            options={},
            include_metadata=None,
        )

        # Assert
        assert result is not None
        mock_spark_engine_read_stream_kafka.assert_called_once()
        mock_read_stream.format.assert_called_once_with("kafka")
        mock_read_stream.format.return_value.options.assert_called_once()

    def test_read_stream_kafka(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_spark_df = spark_df.select("key", "value")

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format=None,
            schema=None,
            include_metadata=None,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_include_metadata(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_spark_df = spark_df

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format=None,
            schema=None,
            include_metadata=True,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_message_format_json(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_df = pd.DataFrame(data={"name": ["value1", "value2"]})

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format="json",
            schema=StructType([StructField("name", StringType())]),
            include_metadata=None,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_message_format_json_include_metadata(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()
        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": ['{"name": "value1"}', '{"name": "value2"}'],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        expected_df = pd.DataFrame(
            data={
                "key": [1, 2],
                "topic": ["test_topic", "test_topic"],
                "partition": ["test_partition", "test_partition"],
                "offset": ["test_offset", "test_offset"],
                "timestamp": ["test_timestamp", "test_timestamp"],
                "timestampType": ["test_timestampType", "test_timestampType"],
                "name": ["value1", "value2"],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format="json",
            schema=StructType([StructField("name", StringType())]),
            include_metadata=True,
        )

        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_read_stream_kafka_message_format_avro(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()

        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": [b"21212121", b"12121212"],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        schema_string = """{
                                "namespace": "example.avro",
                                "type": "record",
                                "name": "KeyValue",
                                "fields": [
                                    {"name": "name", "type": "string"}
                                ]
                            }"""

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format="avro",
            schema=schema_string,
            include_metadata=False,
        )

        # Assert
        assert len(result.columns) == 1
        assert "name" in result.columns
        assert result.schema["name"].dataType == StringType()

    def test_read_stream_kafka_message_format_avro_include_metadata(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        mock_stream = mocker.Mock()
        d = {
            "key": [1, 2],
            "topic": ["test_topic", "test_topic"],
            "partition": ["test_partition", "test_partition"],
            "offset": ["test_offset", "test_offset"],
            "timestamp": ["test_timestamp", "test_timestamp"],
            "timestampType": ["test_timestampType", "test_timestampType"],
            "value": [b"21212121", b"12121212"],
            "x": [True, False],
        }
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)
        mock_stream.load.return_value = spark_df

        schema_string = """{
                                "namespace": "example.avro",
                                "type": "record",
                                "name": "KeyValue",
                                "fields": [
                                    {"name": "name", "type": "string"}
                                ]
                            }"""

        # Act
        result = spark_engine._read_stream_kafka(
            stream=mock_stream,
            message_format="avro",
            schema=schema_string,
            include_metadata=True,
        )

        # Assert
        assert len(result.columns) == 7
        assert "name" in result.columns
        assert result.schema["name"].dataType == StringType()

    @pytest.mark.parametrize(
        "distribute_arg",
        [
            None,  # Test without providing distribute argument (uses default)
            True,  # Test with distribute=True
            False,  # Test with distribute=False
        ],
    )
    def test_add_file(self, mocker, distribute_arg):
        # Arrange
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")
        mock_pyspark_files_get = mocker.patch("pyspark.files.SparkFiles.get")
        mocker.patch("hopsworks_common.client._is_external", return_value=False)
        mocker.patch("shutil.copy")

        mock_add_file = mocker.patch("pyspark.SparkContext.addFile")

        spark_engine = spark.Engine()

        # Mock dataset API and file I/O for distribute=False case
        if distribute_arg is False:
            mock_dataset_api.return_value.read_content.return_value.content = b""
            mocker.patch("builtins.open", mocker.mock_open())

        # Act
        if distribute_arg is None:
            # Call without distribute argument
            spark_engine.add_file(file="test_file")
        else:
            # Call with distribute argument
            spark_engine.add_file(file="test_file", distribute=distribute_arg)

        # Assert
        if distribute_arg is False:
            # When distribute=False, read_content should be called once
            mock_dataset_api.return_value.read_content.assert_called_once()
            # addFile and SparkFiles.get should NOT be called
            mock_add_file.assert_not_called()
            mock_pyspark_files_get.assert_not_called()
        else:
            # When distribute is True or None (default), addFile should be called
            mock_dataset_api.return_value.read_content.assert_not_called()
            mock_add_file.assert_called_once_with("hdfs://test_file")
            mock_pyspark_files_get.assert_called_once_with("test_file")

    def test_add_file_if_present_in_job_configuration(self, mocker):
        # Arrange
        mocker.patch(
            "os.environ",
            {
                "APP_FILES": "/Projects/test_file",
                "MATERIALISATION_DIR": "/tmp/materialisation_dir",
            },
        )

        mock_add_file = mocker.patch("pyspark.SparkContext.addFile")

        spark_engine = spark.Engine()

        # Act
        path = spark_engine.add_file(
            file="/Projects/test_file",
        )

        # Assert
        assert path == "/tmp/materialisation_dir/test_file"
        mock_add_file.assert_not_called()

    def test_profile(self, mocker):
        # Arrange
        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        # Act
        spark_engine.profile(
            dataframe=mocker.Mock(),
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            mock_spark_context._jvm.com.logicalclocks.hsfs.spark.engine.SparkEngine.getInstance.return_value.profile.call_count
            == 1
        )

    @pytest.mark.skipif(
        HAS_GREAT_EXPECTATIONS is False,
        reason="Great Expectations is not installed",
    )
    def test_validate_with_great_expectations(self, mocker):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "event_time": [1, 2]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        es = expectation_suite.ExpectationSuite(
            expectation_suite_name="es_name", expectations=None, meta={}
        )

        # Act
        result = spark_engine.validate_with_great_expectations(
            dataframe=spark_df,
            expectation_suite=es.to_ge_type(),
            ge_validate_kwargs={"run_name": "test_run_id"},
        )

        # Assert
        assert result.to_json_dict() == {
            "evaluation_parameters": {},
            "meta": {
                "active_batch_definition": {
                    "batch_identifiers": {"batch_id": "default_identifier"},
                    "data_asset_name": "<YOUR_MEANGINGFUL_NAME>",
                    "data_connector_name": "default_runtime_data_connector_name",
                    "datasource_name": "my_spark_dataframe",
                },
                "batch_markers": {"ge_load_time": mocker.ANY},
                "batch_spec": {
                    "batch_data": "SparkDataFrame",
                    "data_asset_name": "<YOUR_MEANGINGFUL_NAME>",
                },
                "checkpoint_name": None,
                "expectation_suite_name": "es_name",
                "great_expectations_version": "0.18.12",
                "run_id": {"run_name": "test_run_id", "run_time": mocker.ANY},
                "validation_time": mocker.ANY,
            },
            "results": [],
            "statistics": {
                "evaluated_expectations": 0,
                "success_percent": None,
                "successful_expectations": 0,
                "unsuccessful_expectations": 0,
            },
            "success": True,
        }

    def test_write_options(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"test_key": "test_value"}

    def test_write_options_tfrecords(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="tfrecords",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_write_options_tfrecord(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="tfrecord",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_write_options_csv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="csv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"delimiter": ",", "header": "true", "test_key": "test_value"}

    def test_write_options_tsv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.write_options(
            data_format="tsv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"delimiter": "\t", "header": "true", "test_key": "test_value"}

    def test_read_options(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="",
            provided_options=None,
        )

        # Assert
        assert result == {}

    def test_read_options_provided_options(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"test_key": "test_value"}

    def test_read_options_provided_options_tfrecords(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="tfrecords",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_read_options_provided_options_tfrecord(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="tfrecord",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {"recordType": "Example", "test_key": "test_value"}

    def test_read_options_provided_options_csv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="csv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {
            "delimiter": ",",
            "header": "true",
            "inferSchema": "true",
            "test_key": "test_value",
        }

    def test_read_options_provided_options_tsv(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.read_options(
            data_format="tsv",
            provided_options={"test_key": "test_value"},
        )

        # Assert
        assert result == {
            "delimiter": "\t",
            "header": "true",
            "inferSchema": "true",
            "test_key": "test_value",
        }

    def test_parse_schema_feature_group(self, mocker):
        # Arrange
        mock_spark_engine_convert_spark_type = mocker.patch(
            "hsfs.engine.spark.Engine._convert_spark_type_to_offline_type"
        )

        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.parse_schema_feature_group(
            dataframe=spark_df,
            time_travel_format=None,
        )

        # Assert
        assert result[0].name == "col_0"
        assert result[1].name == "col_1"
        assert mock_spark_engine_convert_spark_type.call_count == 2
        assert mock_spark_engine_convert_spark_type.call_args[0][1] is False

    def test_parse_schema_feature_group_hudi(self, mocker):
        # Arrange
        mock_spark_engine_convert_spark_type = mocker.patch(
            "hsfs.engine.spark.Engine._convert_spark_type_to_offline_type"
        )

        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.parse_schema_feature_group(
            dataframe=spark_df,
            time_travel_format="HUDI",
        )

        # Assert
        assert result[0].name == "col_0"
        assert result[1].name == "col_1"
        assert mock_spark_engine_convert_spark_type.call_count == 2
        assert mock_spark_engine_convert_spark_type.call_args[0][1] is True

    def test_parse_schema_feature_group_value_error(self, mocker):
        # Arrange
        mock_spark_engine_convert_spark_type = mocker.patch(
            "hsfs.engine.spark.Engine._convert_spark_type_to_offline_type"
        )

        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        mock_spark_engine_convert_spark_type.side_effect = ValueError(
            "test error response"
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            spark_engine.parse_schema_feature_group(
                dataframe=spark_df,
                time_travel_format=None,
            )

        # Assert
        assert str(e_info.value) == "Feature 'col_0': test error response"

    def test_parse_schema_training_dataset(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.parse_schema_training_dataset(
            dataframe=spark_df,
        )

        # Assert
        assert result[0].name == "col_0"
        assert result[1].name == "col_1"

    def test_convert_spark_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=IntegerType(),
            using_hudi=False,
        )

        # Assert
        assert result == "int"

    def test_cast_columns(self):
        class LabelIndex:
            def __init__(self, label, index):
                self.label = label
                self.index = index

        spark_engine = spark.Engine()
        d = {
            "string": ["s"],
            "bigint": ["1"],
            "int": ["1"],
            "smallint": ["1"],
            "tinyint": ["1"],
            "float": ["1"],
            "double": ["1"],
            "timestamp": [1641340800000],
            "boolean": ["False"],
            "date": ["2022-01-27"],
            "binary": ["1"],
            "array<string>": [["123"]],
            "struc": [LabelIndex("0", "1")],
            "decimal": ["1.1"],
        }
        df = pd.DataFrame(data=d)
        spark_df = spark_engine._spark_session.createDataFrame(df)
        schema = [
            TrainingDatasetFeature("string", type="string"),
            TrainingDatasetFeature("bigint", type="bigint"),
            TrainingDatasetFeature("int", type="int"),
            TrainingDatasetFeature("smallint", type="smallint"),
            TrainingDatasetFeature("tinyint", type="tinyint"),
            TrainingDatasetFeature("float", type="float"),
            TrainingDatasetFeature("double", type="double"),
            TrainingDatasetFeature("timestamp", type="timestamp"),
            TrainingDatasetFeature("boolean", type="boolean"),
            TrainingDatasetFeature("date", type="date"),
            TrainingDatasetFeature("binary", type="binary"),
            TrainingDatasetFeature("array<string>", type="array<string>"),
            TrainingDatasetFeature("struc", type="struct<label:string,index:int>"),
            TrainingDatasetFeature("decimal", type="decimal"),
        ]
        cast_df = spark_engine.cast_columns(spark_df, schema)
        expected = {
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
            "array<string>": ArrayType(StringType()),
            "struc": StructType(
                [
                    StructField("label", StringType(), True),
                    StructField("index", IntegerType(), True),
                ]
            ),
            "decimal": DecimalType(),
        }
        for col in cast_df.dtypes:
            assert col[1] == expected[col[0]].simpleString()

    def test_convert_spark_type_using_hudi_byte_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=ByteType(),
            using_hudi=True,
        )

        # Assert
        assert result == "int"

    def test_convert_spark_type_using_hudi_short_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=ShortType(),
            using_hudi=True,
        )

        # Assert
        assert result == "int"

    def test_convert_spark_type_using_hudi_bool_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=BooleanType(),
            using_hudi=True,
        )

        # Assert
        assert result == "boolean"

    def test_convert_spark_type_using_hudi_int_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=IntegerType(),
            using_hudi=True,
        )

        # Assert
        assert result == "int"

    def test_convert_spark_type_using_hudi_long_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=LongType(),
            using_hudi=True,
        )

        # Assert
        assert result == "bigint"

    def test_convert_spark_type_using_hudi_float_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=FloatType(),
            using_hudi=True,
        )

        # Assert
        assert result == "float"

    def test_convert_spark_type_using_hudi_double_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=DoubleType(),
            using_hudi=True,
        )

        # Assert
        assert result == "double"

    def test_convert_spark_type_using_hudi_decimal_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=DecimalType(),
            using_hudi=True,
        )

        # Assert
        assert result == "decimal(10,0)"

    def test_convert_spark_type_using_hudi_timestamp_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=TimestampType(),
            using_hudi=True,
        )

        # Assert
        assert result == "timestamp"

    def test_convert_spark_type_using_hudi_date_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=DateType(),
            using_hudi=True,
        )

        # Assert
        assert result == "date"

    def test_convert_spark_type_using_hudi_string_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=StringType(),
            using_hudi=True,
        )

        # Assert
        assert result == "string"

    def test_convert_spark_type_using_hudi_struct_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=StructType(),
            using_hudi=True,
        )

        # Assert
        assert result == "struct<>"

    def test_convert_spark_type_using_hudi_binary_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine._convert_spark_type_to_offline_type(
            spark_type=BinaryType(),
            using_hudi=True,
        )

        # Assert
        assert result == "binary"

    def test_convert_spark_type_using_hudi_map_type(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        with pytest.raises(ValueError) as e_info:
            spark_engine._convert_spark_type_to_offline_type(
                spark_type=MapType(StringType(), StringType()),
                using_hudi=True,
            )

        # Assert
        assert (
            str(e_info.value)
            == "spark type <class 'pyspark.sql.types.MapType'> not supported"
        )

    def test_setup_storage_connector_s3(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.S3Connector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        s3_connector = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        spark_engine.setup_storage_connector(
            storage_connector=s3_connector,
            path="test_path",
        )

        # Assert
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 1
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 0

    def test_setup_storage_connector_adls(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.AdlsConnector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        adls_connector = storage_connector.AdlsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        spark_engine.setup_storage_connector(
            storage_connector=adls_connector,
            path="test_path",
        )

        # Assert
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 1
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 0

    def test_setup_storage_connector_gcs(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.GcsConnector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        gcs_connector = storage_connector.GcsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        spark_engine.setup_storage_connector(
            storage_connector=gcs_connector,
            path="test_path",
        )

        # Assert
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 1

    def test_setup_storage_connector_jdbc(self, mocker):
        # Arrange
        mocker.patch("hsfs.storage_connector.JdbcConnector.refetch")
        mock_spark_engine_setup_s3_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_s3_hadoop_conf"
        )
        mock_spark_engine_setup_adls_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_adls_hadoop_conf"
        )
        mock_spark_engine_setup_gcp_hadoop_conf = mocker.patch(
            "hsfs.engine.spark.Engine._setup_gcp_hadoop_conf"
        )

        spark_engine = spark.Engine()

        jdbc_connector = storage_connector.JdbcConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
        )

        # Act
        result = spark_engine.setup_storage_connector(
            storage_connector=jdbc_connector,
            path="test_path",
        )

        # Assert
        assert result == "test_path"
        assert mock_spark_engine_setup_s3_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_adls_hadoop_conf.call_count == 0
        assert mock_spark_engine_setup_gcp_hadoop_conf.call_count == 0

    def test_setup_s3_hadoop_conf_legacy(self, mocker):
        # Arrange
        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        s3_connector = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            bucket="bucket-name",
            access_key="1",
            secret_key="2",
            server_encryption_algorithm="3",
            server_encryption_key="4",
            session_token="5",
            arguments=[{"name": "fs.s3a.endpoint", "value": "testEndpoint"}],
        )

        # Act
        result = spark_engine._setup_s3_hadoop_conf(
            storage_connector=s3_connector,
            path="s3://_test_path",
        )

        # Assert
        assert result == "s3a://_test_path"
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.set.call_count
            == 14
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.access.key", s3_connector.access_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.secret.key", s3_connector.secret_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.server-side-encryption-algorithm",
            s3_connector.server_encryption_algorithm,
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.server-side-encryption-key", s3_connector.server_encryption_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.session.token", s3_connector.session_token
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.endpoint", s3_connector.arguments.get("fs.s3a.endpoint")
        )

    def test_setup_s3_hadoop_conf_disable_legacy(self, mocker):
        # Arrange
        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        s3_connector = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            bucket="bucket-name",
            access_key="1",
            secret_key="2",
            server_encryption_algorithm="3",
            server_encryption_key="4",
            region="eu-west-1",
            session_token="5",
            arguments=[
                {"name": "fs.s3a.endpoint", "value": "testEndpoint"},
                {"name": "fs.s3a.global-conf", "value": "False"},
                {"name": "fs.s3a.path.style.access", "value": "True"},
                {"name": "fs.s3a.connection.timeout", "value": "200000"},
            ],
        )

        # Act
        result = spark_engine._setup_s3_hadoop_conf(
            storage_connector=s3_connector,
            path="s3://_test_path",
        )

        # Assert
        assert result == "s3a://_test_path"
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.set.call_count
            == 11  # Options should only be set at bucket level
        )
        assert (
            call("fs.s3a.access.key", s3_connector.access_key)
            not in mock_spark_context._jsc.hadoopConfiguration.return_value.set.mock_calls
        )
        assert (
            call("fs.s3a.secret.key", s3_connector.secret_key)
            not in mock_spark_context._jsc.hadoopConfiguration.return_value.set.mock_calls
        )
        assert (
            call(
                "fs.s3a.server-side-encryption-algorithm",
                s3_connector.server_encryption_algorithm,
            )
            not in mock_spark_context._jsc.hadoopConfiguration.return_value.set.mock_calls
        )

        assert (
            call(
                "fs.s3a.server-side-encryption-key", s3_connector.server_encryption_key
            )
            not in mock_spark_context._jsc.hadoopConfiguration.return_value.set.mock_calls
        )

        assert (
            call(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            )
            not in mock_spark_context._jsc.hadoopConfiguration.return_value.set.mock_calls
        )

        assert (
            call("fs.s3a.session.token", s3_connector.session_token)
            not in mock_spark_context._jsc.hadoopConfiguration.return_value.set.mock_calls
        )

        assert (
            call("fs.s3a.endpoint", s3_connector.arguments.get("fs.s3a.endpoint"))
            not in mock_spark_context._jsc.hadoopConfiguration.return_value.set.mock_calls
        )

        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.access.key", s3_connector.access_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.secret.key", s3_connector.secret_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.server-side-encryption-algorithm",
            s3_connector.server_encryption_algorithm,
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.server-side-encryption-key",
            s3_connector.server_encryption_key,
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.session.token", s3_connector.session_token
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.endpoint",
            s3_connector.arguments.get("fs.s3a.endpoint"),
        )

    def test_setup_s3_hadoop_conf_bucket_scope(self, mocker):
        # Arrange
        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        s3_connector = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            bucket="bucket-name",
            access_key="1",
            secret_key="2",
            server_encryption_algorithm="3",
            server_encryption_key="4",
            session_token="5",
            arguments=[{"name": "fs.s3a.endpoint", "value": "testEndpoint"}],
        )

        # Act
        result = spark_engine._setup_s3_hadoop_conf(
            storage_connector=s3_connector,
            path="s3://_test_path",
        )

        # Assert
        assert result == "s3a://_test_path"
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.set.call_count
            == 14
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.access.key", s3_connector.access_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.secret.key", s3_connector.secret_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.server-side-encryption-algorithm",
            s3_connector.server_encryption_algorithm,
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.server-side-encryption-key",
            s3_connector.server_encryption_key,
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.session.token", s3_connector.session_token
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.s3a.bucket.bucket-name.endpoint",
            s3_connector.arguments.get("fs.s3a.endpoint"),
        )

    def test_setup_adls_hadoop_conf(self, mocker):
        # Arrange
        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        adls_connector = storage_connector.AdlsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            spark_options=[
                {"name": "name_1", "value": "value_1"},
                {"name": "name_2", "value": "value_2"},
            ],
        )

        # Act
        result = spark_engine._setup_adls_hadoop_conf(
            storage_connector=adls_connector,
            path="adls_test_path",
        )

        # Assert
        assert result == "adls_test_path"
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.set.call_count == 2
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "name_1", "value_1"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "name_2", "value_2"
        )

    def test_is_spark_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        # Act
        result = spark_engine.is_spark_dataframe(
            dataframe=None,
        )

        # Assert
        assert result is False

    def test_is_spark_dataframe_spark_dataframe(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.is_spark_dataframe(
            dataframe=spark_df,
        )

        # Assert
        assert result is True

    def test_update_table_schema_hudi(self, mocker):
        # Arrange
        mock_spark_engine_save_dataframe = mocker.patch(
            "hsfs.engine.spark.Engine.save_dataframe"
        )
        mock_spark_read = mocker.patch("pyspark.sql.SparkSession.read")
        mock_format = mocker.Mock()
        mock_spark_read.format.return_value = mock_format

        # Arrange
        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            featurestore_name="test_featurestore",
            time_travel_format="HUDI",
        )

        # Act
        spark_engine.update_table_schema(feature_group=fg)

        # Assert
        assert mock_spark_engine_save_dataframe.call_count == 1
        assert mock_spark_read.format.call_count == 1

    def test_update_table_schema_delta(self, mocker):
        # Arrange
        mock_spark_read = mocker.patch("pyspark.sql.SparkSession.read")
        mock_format = mocker.Mock()
        mock_spark_read.format.return_value = mock_format

        # Mock the conf property and its set method
        mock_conf = mocker.Mock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "conf",
            new_callable=PropertyMock,
            return_value=mock_conf,
        )

        # Arrange
        spark_engine = spark.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            featurestore_name="test_featurestore",
            time_travel_format="DELTA",
        )

        # Act
        spark_engine.update_table_schema(feature_group=fg)

        # Assert
        assert mock_spark_read.format.call_count == 1
        # Verify that the Delta catalog configuration is set
        mock_conf.set.assert_called_with(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    def test_apply_transformation_function_single_output_udf_default_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf(int, drop=["col1"])
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_one_col_0_": [2, 3],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_single_output_udf_python_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf(int, drop=["col1"], mode="python")
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_one_col_0_": [2, 3],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_single_output_udf_pandas_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf(int, drop=["col1"], mode="pandas")
        def plus_one(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_one_col_0_": [2, 3],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    @pytest.mark.parametrize("execution_mode", ["default", "pandas", "python"])
    def test_apply_transformation_function_single_output_udf_transformation_context(
        self, mocker, execution_mode
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf(int, drop=["col1"], mode=execution_mode)
        def plus_one(col1, context):
            return col1 + context["test"]

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_one_col_0_": [21, 22],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
            transformation_context={"test": 20},
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_output_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1"])
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_two_col_0_0": [2, 3],
                "plus_two_col_0_1": [3, 4],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_output_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1"], mode="pandas")
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_two_col_0_0": [2, 3],
                "plus_two_col_0_1": [3, 4],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_output_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1"], mode="python")
        def plus_two(col1):
            return col1 + 1, col1 + 2

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=plus_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=BooleanType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [True, False]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [True, False],
                "plus_two_col_0_0": [2, 3],
                "plus_two_col_0_1": [3, 4],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int])
        def test(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [10, 11],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], mode="python")
        def test(col1, col2):
            return col1 + 1, col2 + 2

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [10, 11],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], mode="pandas")
        def test(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_0": [1, 2],
                "col_1": ["test_1", "test_2"],
                "col_2": [10, 11],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1"])
        def test(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [10, 11],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1"], mode="pandas")
        def test(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [10, 11],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1"], mode="python")
        def test(col1, col2):
            return col1 + 1, col2 + 2

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "col_2": [10, 11],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1", "col2"])
        def test(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1", "col2"], mode="pandas")
        def test(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "spark"
        spark_engine = spark.Engine()

        @udf([int, int], drop=["col1", "col2"], mode="python")
        def test(col1, col2):
            return col1 + 1, col2 + 2

        tf = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        f = feature.Feature(name="col_0", type=IntegerType(), index=0)
        f1 = feature.Feature(name="col_1", type=StringType(), index=1)
        f2 = feature.Feature(name="col_2", type=IntegerType(), index=1)
        features = [f, f1, f2]
        fg1 = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=features,
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=99,
            query=fg1.select_all(),
            transformation_functions=[tf("col_0", "col_2")],
        )

        d = {"col_0": [1, 2], "col_1": ["test_1", "test_2"], "col_2": [10, 11]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        expected_df = pd.DataFrame(
            data={
                "col_1": ["test_1", "test_2"],
                "test_col_0_col_2_0": [2, 3],
                "test_col_0_col_2_1": [12, 13],
            }
        )

        expected_spark_df = spark_engine._spark_session.createDataFrame(expected_df)

        # Act
        result = spark_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=spark_df,
        )
        # Assert
        assert result.schema == expected_spark_df.schema
        assert result.collect() == expected_spark_df.collect()

    def test_setup_gcp_hadoop_conf(self, mocker):
        # Arrange
        mock_spark_engine_add_file = mocker.patch("hsfs.engine.spark.Engine.add_file")

        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        content = (
            '{"type": "service_account", "project_id": "test", "private_key_id": "123456", '
            '"private_key": "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----", '
            '"client_email": "test@project.iam.gserviceaccount.com"}'
        )
        credentialsFile = "keyFile.json"
        with open(credentialsFile, "w") as f:
            f.write(content)

        gcs_connector = storage_connector.GcsConnector(
            id=1, name="test_connector", featurestore_id=99, key_path=credentialsFile
        )

        mock_spark_engine_add_file.return_value = "keyFile.json"

        # Act
        result = spark_engine._setup_gcp_hadoop_conf(
            storage_connector=gcs_connector,
            path="test_path",
        )

        # Assert
        assert result == "test_path"
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.setIfUnset.call_count
            == 2
        )
        assert mock_spark_engine_add_file.call_count == 1
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.set.call_count == 3
        )
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.unset.call_count
            == 3
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "google.cloud.auth.service.account.enable", "true"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.email", "test@project.iam.gserviceaccount.com"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key.id", "123456"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key",
            "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.unset.assert_any_call(
            "fs.gs.encryption.algorithm"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.unset.assert_any_call(
            "fs.gs.encryption.key"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.unset.assert_any_call(
            "fs.gs.encryption.key.hash"
        )

    def test_setup_gcp_hadoop_conf_algorithm(self, mocker):
        # Arrange
        mock_spark_engine_add_file = mocker.patch("hsfs.engine.spark.Engine.add_file")

        mock_spark_context = MagicMock()
        mocker.patch.object(
            pyspark.sql.SparkSession,
            "sparkContext",
            new_callable=PropertyMock,
            return_value=mock_spark_context,
        )

        spark_engine = spark.Engine()

        content = (
            '{"type": "service_account", "project_id": "test", "private_key_id": "123456", '
            '"private_key": "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----", '
            '"client_email": "test@project.iam.gserviceaccount.com"}'
        )
        credentialsFile = "keyFile.json"
        with open(credentialsFile, "w") as f:
            f.write(content)

        gcs_connector = storage_connector.GcsConnector(
            id=1,
            name="test_connector",
            featurestore_id=99,
            key_path=credentialsFile,
            algorithm="temp_algorithm",
            encryption_key="1",
            encryption_key_hash="2",
        )

        mock_spark_engine_add_file.return_value = "keyFile.json"

        # Act
        result = spark_engine._setup_gcp_hadoop_conf(
            storage_connector=gcs_connector,
            path="test_path",
        )

        # Assert
        assert result == "test_path"
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.setIfUnset.call_count
            == 2
        )
        assert mock_spark_engine_add_file.call_count == 1
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.set.call_count == 6
        )
        assert (
            mock_spark_context._jsc.hadoopConfiguration.return_value.unset.call_count
            == 0
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.setIfUnset.assert_any_call(
            "google.cloud.auth.service.account.enable", "true"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.encryption.algorithm", gcs_connector.algorithm
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.encryption.key", gcs_connector.encryption_key
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.encryption.key.hash", gcs_connector.encryption_key_hash
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.email", "test@project.iam.gserviceaccount.com"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key.id", "123456"
        )
        mock_spark_context._jsc.hadoopConfiguration.return_value.set.assert_any_call(
            "fs.gs.auth.service.account.private.key",
            "-----BEGIN PRIVATE KEY-----test-----END PRIVATE KEY-----",
        )

    def test_get_unique_values(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2, 2], "col_1": ["test_1", "test_2", "test_3"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.get_unique_values(
            feature_dataframe=spark_df,
            feature_name="col_0",
        )

        # Assert
        assert result == [1, 2]

    def test_create_empty_df(self):
        # Arrange
        spark_engine = spark.Engine()

        d = {"col_0": [1, 2, 2], "col_1": ["test_1", "test_2", "test_3"]}
        df = pd.DataFrame(data=d)

        spark_df = spark_engine._spark_session.createDataFrame(df)

        # Act
        result = spark_engine.create_empty_df(
            streaming_df=spark_df,
        )

        # Assert
        assert result.schema == spark_df.schema
        assert result.collect() == []

    def test_get_feature_logging_df_logging_data_no_missing_no_additional_dataframe(
        self, mocker, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        args = TestSpark.get_logging_arguments(
            logging_data=logging_test_dataframe,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, _ = TestSpark.get_expected_logging_df(
            logging_data=logging_test_dataframe,
            logging_feature_group_features=logging_feature_group_features,
            spark_engine=spark_engine,
            column_names=column_names,
            metadata_logging_columns=meta_data_logging_columns,
        )

        # Assert
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_logging_data_no_missing_no_additional_list(
        self, mocker, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_features_names = [
            feature.name
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]
        logging_features_names = [
            name if name != "predicted_label" else "label"
            for name in logging_features_names
        ]
        log_data_df_list = [
            list(row)
            for row in logging_test_dataframe.select(*logging_features_names).collect()
        ]
        args = TestSpark.get_logging_arguments(
            logging_data=log_data_df_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, _ = TestSpark.get_expected_logging_df(
            logging_data=logging_test_dataframe,
            logging_feature_group_features=logging_feature_group_features,
            spark_engine=spark_engine,
            column_names=column_names,
            metadata_logging_columns=meta_data_logging_columns,
        )

        # Assert
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_logging_data_no_missing_no_additional_dict(
        self, mocker, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_features_names = [
            feature.name
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]
        logging_features_names = [
            name if name != "predicted_label" else "label"
            for name in logging_features_names
        ]
        # Select column in the correct order
        log_data_df_dict = [
            row.asDict()
            for row in logging_test_dataframe.select(*logging_features_names).collect()
        ]
        args = TestSpark.get_logging_arguments(
            logging_data=log_data_df_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, _ = spark_engine.get_feature_logging_df(**args)

        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, _ = TestSpark.get_expected_logging_df(
            logging_data=logging_test_dataframe,
            logging_feature_group_features=logging_feature_group_features,
            spark_engine=spark_engine,
            column_names=column_names,
            metadata_logging_columns=meta_data_logging_columns,
        )

        # Assert
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_logging_data_missing_columns_and_additional_dataframe(
        self,
        mocker,
        caplog,
        logging_features,
        logging_test_dataframe,
        spark_engine,
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.drop("feature_3", "extra_2").withColumn(
            "additional_col", lit(0)
        )
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            logging_data=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                logging_data=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)

        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_logging_data_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.drop("feature_3", "extra_2").withColumn(
            "additional_col", lit(0)
        )
        caplog.set_level(logging.INFO)
        log_data_df_dict = [row.asDict() for row in spark_df.select("*").collect()]
        args = TestSpark.get_logging_arguments(
            logging_data=log_data_df_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                logging_data=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )

        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_logging_data_missing_columns_and_additional_list(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.drop("feature_3", "extra_2").withColumn(
            "additional_col", lit(0)
        )
        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                logging_data=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.LOGGING_DATA}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.LOGGING_DATA}` to ensure that it has the following features : ['primary_key', 'event_time', 'feature_1', 'feature_2', 'feature_3', 'predicted_label', 'min_max_scaler_feature_3', 'extra_1', 'extra_2', 'inference_helper_1', 'rp_1', 'rp_2', 'request_id']."
        )

    def test_get_feature_logging_df_untransformed_features_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(
            *column_names["untransformed_features"]
        )
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            untransformed_features=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                untransformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )

        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_untransformed_features_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(
            *column_names["untransformed_features"]
        )
        untransformed_features_data = [
            list(row) for row in spark_df.select("*").collect()
        ]
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            untransformed_features=untransformed_features_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                untransformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_untransformed_features_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select(
            *column_names["untransformed_features"]
        )
        untransformed_features_dict = [
            row.asDict() for row in spark_df.select("*").collect()
        ]
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            untransformed_features=untransformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                untransformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_untransformed_features_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("feature_1").withColumn(
            "additional_col", lit(None).cast(IntegerType())
        )
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            untransformed_features=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                untransformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_untransformed_features_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("feature_1").withColumn(
            "additional_col", lit(1).cast(DoubleType())
        )
        caplog.set_level(logging.INFO)

        untransformed_data_df_dict = [
            row.asDict() for row in spark_df.select("*").collect()
        ]
        args = TestSpark.get_logging_arguments(
            untransformed_features=untransformed_data_df_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                untransformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_untransformed_features_missing_columns_and_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("feature_1").withColumn(
            "additional_col", lit(1).cast(DoubleType())
        )
        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                untransformed_features=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES}` to ensure that it has the following features : {column_names['untransformed_features']}."
        )

    def test_get_feature_logging_df_transformed_features_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["transformed_features"])
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            logging_data=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                transformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_transformed_features_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select(*column_names["transformed_features"])
        transformed_features_data = [
            list(row) for row in spark_df.select("*").collect()
        ]
        caplog.set_level(logging.INFO)
        arg = TestSpark.get_logging_arguments(
            transformed_features=transformed_features_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**arg)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                transformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_transformed_features_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["transformed_features"])
        transformed_features_dict = [
            row.asDict() for row in spark_df.select("*").collect()
        ]
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            transformed_features=transformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                transformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_transformed_features_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("feature_1").withColumn(
            "additional_col", lit(1).cast(DoubleType())
        )
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            transformed_features=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                transformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_transformed_features_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        caplog.set_level(logging.INFO)

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("feature_1").withColumn(
            "additional_col", lit(1).cast(DoubleType())
        )
        transformed_data_df_dict = [
            row.asDict() for row in spark_df.select("*").collect()
        ]

        args = TestSpark.get_logging_arguments(
            transformed_features=transformed_data_df_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                transformed_feature_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_transformed_features_missing_columns_and_additional_list(
        self, mocker, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select("feature_1").withColumn(
            "additional_col", lit(1).cast(DoubleType())
        )
        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                transformed_features=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.TRANSFORMED_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.TRANSFORMED_FEATURES}` to ensure that it has the following features : {column_names['transformed_features']}."
        )

    def test_get_feature_logging_df_predictions_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        caplog.set_level(logging.INFO)
        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select(*column_names["predictions"])
        args = TestSpark.get_logging_arguments(
            logging_data=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                predictions_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_predictions_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        # Select column in the correct order
        spark_df = logging_test_dataframe.select(*column_names["predictions"])
        predictions_data = [list(row) for row in spark_df.select("*").collect()]
        print(predictions_data)
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            predictions=predictions_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                predictions_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        print(logging_dataframe.select("predicted_label").collect())
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.select(*logging_feature_names).collect()
            == expected_dataframe.select(*logging_feature_names).collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_predictions_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        # Select column in the correct order
        spark_df = logging_test_dataframe.select(*column_names["predictions"])
        prediction_dict = [row.asDict() for row in spark_df.select("*").collect()]
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            predictions=prediction_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                predictions_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_predictions_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        caplog.set_level(logging.INFO)

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = [
            feature
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label"
        ]
        logging_features.append(
            feature.Feature(
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label1", type="string"
            )
        )
        logging_features.append(
            feature.Feature(
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label2", type="string"
            )
        )
        logging_feature_group_features = meta_data_logging_columns + logging_features
        column_names["predictions"] = ["label1"]
        spark_df = (
            logging_test_dataframe.select("label")
            .withColumn("label1", lit("1").cast(StringType()))
            .withColumnRenamed("label", "label_additional")
        )

        # Specify column names for each type of data
        args = TestSpark.get_logging_arguments(
            predictions=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                predictions_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_predictions_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = [
            feature
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label"
        ]
        logging_features.append(
            feature.Feature(
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label1", type="string"
            )
        )
        logging_features.append(
            feature.Feature(
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label2", type="string"
            )
        )
        logging_feature_group_features = meta_data_logging_columns + logging_features
        column_names["predictions"] = ["label1"]
        spark_df = (
            logging_test_dataframe.select("label")
            .withColumn("label1", lit("1").cast(StringType()))
            .withColumnRenamed("label", "label_additional")
        )
        caplog.set_level(logging.INFO)

        predictions_dict = [row.asDict() for row in spark_df.select("*").collect()]

        args = TestSpark.get_logging_arguments(
            predictions=predictions_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                predictions_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_predictions_missing_columns_and_additional_list(
        self, mocker, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = [
            feature
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label"
        ]
        logging_features.append(
            feature.Feature(
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label1", type="string"
            )
        )
        logging_features.append(
            feature.Feature(
                constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + "label2", type="string"
            )
        )

        logging_feature_group_features = meta_data_logging_columns + logging_features
        column_names["predictions"] = ["label1"]
        spark_df = (
            logging_test_dataframe.select("label")
            .withColumn("label_additional", lit("1").cast(StringType()))
            .withColumnRenamed("label", "label1")
        )

        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        # Specify column names for each type of data
        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                predictions=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.PREDICTIONS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.PREDICTIONS}` to ensure that it has the following features : {column_names['predictions']}."
        )

    def test_get_feature_logging_df_serving_keys_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        caplog.set_level(logging.INFO)

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["serving_keys"])
        args = TestSpark.get_logging_arguments(
            serving_keys=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                serving_keys_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_serving_keys_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["serving_keys"])
        serving_key_data = [list(row) for row in spark_df.select("*").collect()]
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            serving_keys=serving_key_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                serving_keys_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_serving_keys_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["serving_keys"])
        serving_key_data = [row.asDict() for row in spark_df.select("*").collect()]

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            serving_keys=serving_key_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                serving_keys_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_serving_keys_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = [
            feature for feature in logging_features if feature.name != "primary_key"
        ]
        logging_features.append(feature.Feature("primary_key1", type="bigint"))
        logging_features.append(feature.Feature("primary_key2", type="bigint"))
        logging_feature_group_features = meta_data_logging_columns + logging_features
        column_names["serving_keys"] = ["primary_key1", "primary_key2"]

        spark_df = (
            logging_test_dataframe.select("primary_key")
            .withColumn("primary_key1", lit(1).cast("bigint"))
            .withColumnRenamed("primary_key", "primary_key_additional")
        )

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            serving_keys=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                serving_keys_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key1")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key1")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_serving_keys_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = [
            feature for feature in logging_features if feature.name != "primary_key"
        ]
        logging_features.append(feature.Feature("primary_key1", type="bigint"))
        logging_features.append(feature.Feature("primary_key2", type="bigint"))

        logging_feature_group_features = meta_data_logging_columns + logging_features

        column_names["serving_keys"] = ["primary_key1", "primary_key2"]
        spark_df = (
            logging_test_dataframe.select("primary_key")
            .withColumn("primary_key1", lit(1).cast("bigint"))
            .withColumnRenamed("primary_key", "primary_key_additional")
        )
        caplog.set_level(logging.INFO)
        serving_key_dict = [row.asDict() for row in spark_df.select("*").collect()]
        args = TestSpark.get_logging_arguments(
            serving_keys=serving_key_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                serving_keys_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key1")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key1")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_serving_key_missing_columns_and_additional_list(
        self, mocker, logging_features, logging_test_dataframe, spark_engine
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = [
            feature for feature in logging_features if feature.name != "primary_key"
        ]
        logging_features.append(feature.Feature("primary_key1", type="bigint"))
        logging_features.append(feature.Feature("primary_key2", type="bigint"))

        logging_feature_group_features = meta_data_logging_columns + logging_features

        column_names["serving_keys"] = ["primary_key1", "primary_key2"]
        spark_df = logging_test_dataframe.select("primary_key").withColumnRenamed(
            "primary_key", "primary_key1"
        )

        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                serving_keys=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.SERVING_KEYS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.SERVING_KEYS}` to ensure that it has the following features : {column_names['serving_keys']}."
        )

    def test_get_feature_logging_df_inference_helpers_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["helper_columns"])
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            helper_columns=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                inference_helper_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_inference_helper_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["helper_columns"])
        inference_helper_list = [list(row) for row in spark_df.select("*").collect()]
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            helper_columns=inference_helper_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                inference_helper_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_inference_helper_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["helper_columns"])
        inference_helper_data = [row.asDict() for row in spark_df.select("*").collect()]
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            helper_columns=inference_helper_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                inference_helper_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_inference_helpers_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = list(logging_features)
        logging_features.append(feature.Feature("inference_helper_2", type="double"))
        logging_feature_group_features = meta_data_logging_columns + logging_features
        column_names["helper_columns"] = ["inference_helper_1", "inference_helper_2"]
        spark_df = logging_test_dataframe.select("inference_helper_1").withColumn(
            "inference_helper_additional", lit(1.1).cast(DoubleType())
        )
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            helper_columns=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                inference_helper_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_inference_helpers_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = list(logging_features)

        logging_features.append(feature.Feature("inference_helper_2", type="double"))
        logging_feature_group_features = meta_data_logging_columns + logging_features
        column_names["helper_columns"] = ["inference_helper_1", "inference_helper_2"]
        spark_df = logging_test_dataframe.select("inference_helper_1").withColumn(
            "inference_helper_additional", lit(1.1).cast(DoubleType())
        )
        caplog.set_level(logging.INFO)
        inference_helpers_dict = [
            row.asDict() for row in spark_df.select("*").collect()
        ]
        args = TestSpark.get_logging_arguments(
            helper_columns=inference_helpers_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                inference_helper_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_inference_helpers_missing_columns_and_additional_list(
        self, mocker, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = list(logging_features)
        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helper_features = ["inference_helper_1"]
        spark_df = logging_test_dataframe.select("inference_helper_1").withColumn(
            "inference_helper_additional", lit(1.1).cast(DoubleType())
        )
        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                helper_columns=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS}` to ensure that it has the following features : {inference_helper_features}."
        )

    def test_get_feature_logging_df_extra_log_columns_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(
            *column_names["extra_logging_features"]
        )
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            extra_logging_features=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                extra_log_columns_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_extra_log_columns_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(
            *column_names["extra_logging_features"]
        )
        extra_logging_data = [list(row) for row in spark_df.select("*").collect()]
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            extra_logging_features=extra_logging_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                extra_log_columns_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)

        # Assert expected columns using Spark DataFrame methods
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3

        # Verify specific columns exist and have expected data types
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_extra_log_columns_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select(
            *column_names["extra_logging_features"]
        )
        extra_logging_data = [row.asDict() for row in spark_df.select("*").collect()]

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            extra_logging_features=extra_logging_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                extra_log_columns_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )

        assert sorted(missing_features) == sorted(missing_logging_features)

        # Assert expected columns using Spark DataFrame methods
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3

        # Verify specific columns exist and have expected data types
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_extra_log_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select(
            "extra_1", "extra_2"
        ).withColumnRenamed("extra_2", "extra_additional")

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            extra_logging_features=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                extra_log_columns_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_extra_log_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(
            "extra_1", "extra_2"
        ).withColumnRenamed("extra_2", "extra_additional")
        caplog.set_level(logging.INFO)
        extra_log_columns_dict = [
            row.asDict() for row in spark_df.select("*").collect()
        ]
        args = TestSpark.get_logging_arguments(
            extra_logging_features=extra_log_columns_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                extra_log_columns_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_extra_log_missing_columns_and_additional_list(
        self, mocker, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("extra_1").withColumnRenamed(
            "extra_2", "extra_additional"
        )

        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                extra_logging_features=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES}` to ensure that it has the following features : {column_names['extra_logging_features']}."
        )

    def test_get_feature_logging_df_event_time_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["event_time"])
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            event_time=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                event_time_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_event_time_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select(*column_names["event_time"])
        event_time_data = [list(row) for row in spark_df.select("*").collect()]
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            event_time=event_time_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                event_time_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_event_time_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["event_time"])
        event_time_data = [row.asDict() for row in spark_df.select("*").collect()]

        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            event_time=event_time_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                event_time_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_request_id_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["request_id"])

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            request_id=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_id_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_request_id_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["request_id"])

        request_id_data = [list(row) for row in spark_df.select("*").collect()]
        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            request_id=request_id_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_id_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )

        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_request_id_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["request_id"])
        event_time_data = [row.asDict() for row in spark_df.select("*").collect()]

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            request_id=event_time_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_id_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_request_parameters_no_missing_no_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["request_parameters"])
        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            request_parameters=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_parameters_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_request_parameters_no_missing_no_additional_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        #  Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["request_parameters"])
        request_parameter_data = [list(row) for row in spark_df.select("*").collect()]

        caplog.set_level(logging.INFO)

        args = TestSpark.get_logging_arguments(
            request_parameters=request_parameter_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_parameters_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_request_parameters_no_missing_no_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select(*column_names["request_parameters"])
        inference_helper_data = [row.asDict() for row in spark_df.select("*").collect()]

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            request_parameters=inference_helper_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_parameters_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)

        # Assert expected columns using Spark DataFrame methods
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3

        # Verify specific columns exist and have expected data types
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    def test_get_feature_logging_df_request_parameters_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = list(logging_features)

        logging_feature_group_features = meta_data_logging_columns + logging_features
        spark_df = logging_test_dataframe.select("rp_1", "rp_2").withColumnRenamed(
            "rp_2", "rp_3"
        )

        caplog.set_level(logging.INFO)
        args = TestSpark.get_logging_arguments(
            request_parameters=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_parameters_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )

        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_request_parameters_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("rp_1", "rp_2").withColumnRenamed(
            "rp_2", "rp_3"
        )

        caplog.set_level(logging.INFO)
        request_parameters_data = [
            row.asDict() for row in spark_df.select("*").collect()
        ]

        args = TestSpark.get_logging_arguments(
            request_parameters=request_parameters_data,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, missing_features = (
            TestSpark.get_expected_logging_df(
                request_parameters_df=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_request_parameters_missing_columns_and_additional_list(
        self, mocker, logging_features, spark_engine, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("rp_1").withColumnRenamed(
            "rp_1", "rp_3"
        )
        log_data_df_list = [list(row) for row in spark_df.select("*").collect()]

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestSpark.get_logging_arguments(
                request_parameters=log_data_df_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = spark_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME}` to ensure that it has the following features : {column_names['request_parameters']}."
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_logging_data_override_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("*")

        untransformed_feature_data = [
            (
                100.25,
                105.0,
                10100,
            ),
            (
                100.75,
                102.2,
                10200,
            ),
            (
                101.1,
                107.7,
                10300,
            ),
        ]
        untransformed_feature_schema = StructType(
            [
                StructField("feature_1", DoubleType(), True),
                StructField("feature_2", DoubleType(), True),
                StructField("feature_3", IntegerType(), True),
            ]
        )
        untransformed_feature_df = spark_engine._spark_session.createDataFrame(
            untransformed_feature_data, untransformed_feature_schema
        )

        transformed_features_schema = StructType(
            [
                StructField("feature_1", DoubleType(), True),
                StructField("feature_2", DoubleType(), True),
                StructField("min_max_scaler_feature_3", DoubleType(), True),
            ]
        )
        transformed_features_data = [
            (
                100.25,
                105.0,
                10100.0,
            ),
            (
                100.75,
                102.2,
                100.75,
            ),
            (
                101.1,
                107.7,
                100.25,
            ),
        ]
        transformed_feature_df = spark_engine._spark_session.createDataFrame(
            transformed_features_data, transformed_features_schema
        )

        predictions_schema = StructType(
            [
                StructField("label", StringType(), True),
            ]
        )
        predictions_data = [
            ("X",),
            ("Y",),
            ("Z",),
        ]
        predictions_df = spark_engine._spark_session.createDataFrame(
            predictions_data, predictions_schema
        )

        serving_keys_schema = StructType(
            [
                StructField("primary_key", IntegerType(), True),
            ]
        )
        serving_keys_data = [
            (100,),
            (200,),
            (300,),
        ]
        serving_keys_df = spark_engine._spark_session.createDataFrame(
            serving_keys_data, serving_keys_schema
        )

        inference_helper_schema = StructType(
            [
                StructField("inference_helper_1", DoubleType(), True),
            ]
        )
        inference_helper_data = [
            (10.1,),
            (20.0,),
            (30.0,),
        ]
        inference_helper_df = spark_engine._spark_session.createDataFrame(
            inference_helper_data, inference_helper_schema
        )

        extra_log_columns_schema = StructType(
            [
                StructField("extra_1", StringType(), True),
                StructField("extra_2", IntegerType(), True),
            ]
        )
        extra_log_columns_data = [
            ("extra_1_1_value", 11),
            ("extra_2_2_value", 22),
            ("extra_3_3_value", 33),
        ]
        extra_log_columns_df = spark_engine._spark_session.createDataFrame(
            extra_log_columns_data, extra_log_columns_schema
        )

        event_time_schema = StructType(
            [
                StructField("event_time", TimestampType(), True),
            ]
        )
        event_time_data = [
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
        ]
        event_time_df = spark_engine._spark_session.createDataFrame(
            event_time_data, event_time_schema
        )

        request_id_schema = StructType(
            [
                StructField("request_id", StringType(), True),
            ]
        )
        request_id_data = [
            ("request_1_1",),
            ("request_2_2",),
            ("request_3_3",),
        ]
        request_id_df = spark_engine._spark_session.createDataFrame(
            request_id_data, request_id_schema
        )

        request_parameters_schema = StructType(
            [
                StructField("rp_1", IntegerType(), True),
                StructField("rp_2", IntegerType(), True),
            ]
        )
        request_parameters_data = [
            (100, 200),
            (200, 300),
            (300, 400),
        ]

        request_parameters_df = spark_engine._spark_session.createDataFrame(
            request_parameters_data, request_parameters_schema
        )

        args = TestSpark.get_logging_arguments(
            logging_data=spark_df,
            untransformed_features=untransformed_feature_df,
            transformed_features=transformed_feature_df,
            predictions=predictions_df,
            serving_keys=serving_keys_df,
            helper_columns=inference_helper_df,
            extra_logging_features=extra_log_columns_df,
            event_time=event_time_df,
            request_id=request_id_df,
            request_parameters=request_parameters_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, _, _ = TestSpark.get_expected_logging_df(
            extra_log_columns_df=extra_log_columns_df,
            event_time_df=event_time_df,
            request_id_df=request_id_df,
            request_parameters_df=request_parameters_df,
            untransformed_feature_df=untransformed_feature_df,
            transformed_feature_df=transformed_feature_df,
            predictions_df=predictions_df,
            serving_keys_df=serving_keys_df,
            inference_helper_df=inference_helper_df,
            logging_data=spark_df,
            logging_feature_group_features=logging_feature_group_features,
            spark_engine=spark_engine,
            column_names=column_names,
            metadata_logging_columns=meta_data_logging_columns,
        )
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_logging_data_override_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("*")
        spark_df_dict = [row.asDict() for row in spark_df.select("*").collect()]

        untransformed_feature_data = [
            (
                100.25,
                105.0,
                10100,
            ),
            (
                100.75,
                102.2,
                10200,
            ),
            (
                101.1,
                107.7,
                10300,
            ),
        ]
        untransformed_feature_schema = StructType(
            [
                StructField("feature_1", DoubleType(), True),
                StructField("feature_2", DoubleType(), True),
                StructField("feature_3", IntegerType(), True),
            ]
        )
        untransformed_feature_df = spark_engine._spark_session.createDataFrame(
            untransformed_feature_data, untransformed_feature_schema
        )
        untransformed_feature_dict = [
            row.asDict() for row in untransformed_feature_df.select("*").collect()
        ]

        transformed_features_schema = StructType(
            [
                StructField("feature_1", DoubleType(), True),
                StructField("feature_2", DoubleType(), True),
                StructField("min_max_scaler_feature_3", DoubleType(), True),
            ]
        )
        transformed_features_data = [
            (
                100.25,
                105.0,
                10100.0,
            ),
            (
                100.75,
                102.2,
                100.75,
            ),
            (
                101.1,
                107.7,
                100.25,
            ),
        ]
        transformed_feature_df = spark_engine._spark_session.createDataFrame(
            transformed_features_data, transformed_features_schema
        )
        transformed_feature_dict = [
            row.asDict() for row in transformed_feature_df.select("*").collect()
        ]

        predictions_schema = StructType(
            [
                StructField("label", StringType(), True),
            ]
        )
        predictions_data = [
            ("X",),
            ("Y",),
            ("Z",),
        ]
        predictions_df = spark_engine._spark_session.createDataFrame(
            predictions_data, predictions_schema
        )
        predictions_dict = [
            row.asDict() for row in predictions_df.select("*").collect()
        ]

        serving_keys_schema = StructType(
            [
                StructField("primary_key", IntegerType(), True),
            ]
        )
        serving_keys_data = [
            (100,),
            (200,),
            (300,),
        ]
        serving_keys_df = spark_engine._spark_session.createDataFrame(
            serving_keys_data, serving_keys_schema
        )
        serving_keys_dict = [
            row.asDict() for row in serving_keys_df.select("*").collect()
        ]

        inference_helper_schema = StructType(
            [
                StructField("inference_helper_1", DoubleType(), True),
            ]
        )
        inference_helper_data = [
            (10.1,),
            (20.0,),
            (30.0,),
        ]
        inference_helper_df = spark_engine._spark_session.createDataFrame(
            inference_helper_data, inference_helper_schema
        )
        inference_helper_dict = [
            row.asDict() for row in inference_helper_df.select("*").collect()
        ]

        extra_log_columns_schema = StructType(
            [
                StructField("extra_1", StringType(), True),
                StructField("extra_2", IntegerType(), True),
            ]
        )
        extra_log_columns_data = [
            ("extra_1_1_value", 11),
            ("extra_2_2_value", 22),
            ("extra_3_3_value", 33),
        ]
        extra_log_columns_df = spark_engine._spark_session.createDataFrame(
            extra_log_columns_data, extra_log_columns_schema
        )
        extra_log_columns_dict = [
            row.asDict() for row in extra_log_columns_df.select("*").collect()
        ]

        event_time_schema = StructType(
            [
                StructField("event_time", TimestampType(), True),
            ]
        )
        event_time_data = [
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
        ]
        event_time_df = spark_engine._spark_session.createDataFrame(
            event_time_data, event_time_schema
        )
        event_time_dict = [row.asDict() for row in event_time_df.select("*").collect()]

        request_id_schema = StructType(
            [
                StructField("request_id", StringType(), True),
            ]
        )
        request_id_data = [
            ("request_1_1",),
            ("request_2_2",),
            ("request_3_3",),
        ]
        request_id_df = spark_engine._spark_session.createDataFrame(
            request_id_data, request_id_schema
        )
        request_id_dict = [row.asDict() for row in request_id_df.select("*").collect()]

        request_parameters_schema = StructType(
            [
                StructField("rp_1", IntegerType(), True),
                StructField("rp_2", IntegerType(), True),
            ]
        )
        request_parameters_data = [
            (100, 200),
            (200, 300),
            (300, 400),
        ]

        request_parameters_df = spark_engine._spark_session.createDataFrame(
            request_parameters_data, request_parameters_schema
        )
        request_parameters_dict = [
            row.asDict() for row in request_parameters_df.select("*").collect()
        ]

        # Specify column names for each type of data
        args = TestSpark.get_logging_arguments(
            logging_data=spark_df_dict,
            untransformed_features=untransformed_feature_dict,
            transformed_features=transformed_feature_dict,
            predictions=predictions_dict,
            serving_keys=serving_keys_dict,
            helper_columns=inference_helper_dict,
            extra_logging_features=extra_log_columns_dict,
            event_time=event_time_dict,
            request_id=request_id_dict,
            request_parameters=request_parameters_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                extra_log_columns_df=extra_log_columns_df,
                event_time_df=event_time_df,
                request_id_df=request_id_df,
                request_parameters_df=request_parameters_df,
                untransformed_feature_df=untransformed_feature_df,
                transformed_feature_df=transformed_feature_df,
                predictions_df=predictions_df,
                serving_keys_df=serving_keys_df,
                inference_helper_df=inference_helper_df,
                logging_data=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_get_feature_logging_df_logging_data_override_list(
        self, mocker, caplog, logging_features, logging_test_dataframe, spark_engine
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        spark_df = logging_test_dataframe.select("*")

        untransformed_feature_data = [
            (
                100.25,
                105.0,
                10100,
            ),
            (
                100.75,
                102.2,
                10200,
            ),
            (
                101.1,
                107.7,
                10300,
            ),
        ]
        untransformed_feature_schema = StructType(
            [
                StructField("feature_1", DoubleType(), True),
                StructField("feature_2", DoubleType(), True),
                StructField("feature_3", IntegerType(), True),
            ]
        )
        untransformed_feature_df = spark_engine._spark_session.createDataFrame(
            untransformed_feature_data, untransformed_feature_schema
        )
        untransformed_features_list = [
            list(row) for row in untransformed_feature_df.select("*").collect()
        ]

        transformed_features_schema = StructType(
            [
                StructField("feature_1", DoubleType(), True),
                StructField("feature_2", DoubleType(), True),
                StructField("min_max_scaler_feature_3", DoubleType(), True),
            ]
        )
        transformed_features_data = [
            (
                100.25,
                105.0,
                10100.0,
            ),
            (
                100.75,
                102.2,
                100.75,
            ),
            (
                101.1,
                107.7,
                100.25,
            ),
        ]
        transformed_feature_df = spark_engine._spark_session.createDataFrame(
            transformed_features_data, transformed_features_schema
        )
        transformed_features_list = [
            list(row) for row in transformed_feature_df.select("*").collect()
        ]

        predictions_schema = StructType(
            [
                StructField("label", StringType(), True),
            ]
        )
        predictions_data = [
            ("X",),
            ("Y",),
            ("Z",),
        ]
        predictions_df = spark_engine._spark_session.createDataFrame(
            predictions_data, predictions_schema
        )
        predictions_list = [list(row) for row in predictions_df.select("*").collect()]

        serving_keys_schema = StructType(
            [
                StructField("primary_key", IntegerType(), True),
            ]
        )
        serving_keys_data = [
            (100,),
            (200,),
            (300,),
        ]
        serving_keys_df = spark_engine._spark_session.createDataFrame(
            serving_keys_data, serving_keys_schema
        )
        serving_keys_list = [list(row) for row in serving_keys_df.select("*").collect()]

        inference_helper_schema = StructType(
            [
                StructField("inference_helper_1", DoubleType(), True),
            ]
        )
        inference_helper_data = [
            (10.1,),
            (20.0,),
            (30.0,),
        ]
        inference_helper_df = spark_engine._spark_session.createDataFrame(
            inference_helper_data, inference_helper_schema
        )
        inference_helper_list = [
            list(row) for row in inference_helper_df.select("*").collect()
        ]

        extra_log_columns_schema = StructType(
            [
                StructField("extra_1", StringType(), True),
                StructField("extra_2", IntegerType(), True),
            ]
        )
        extra_log_columns_data = [
            ("extra_1_1_value", 11),
            ("extra_2_2_value", 22),
            ("extra_3_3_value", 33),
        ]
        extra_log_columns_df = spark_engine._spark_session.createDataFrame(
            extra_log_columns_data, extra_log_columns_schema
        )
        extra_log_columns_list = [
            list(row) for row in extra_log_columns_df.select("*").collect()
        ]

        event_time_schema = StructType(
            [
                StructField("event_time", TimestampType(), True),
            ]
        )
        event_time_data = [
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
            (datetime.datetime(2021, 1, 1, 12, 0, 0),),
        ]
        event_time_df = spark_engine._spark_session.createDataFrame(
            event_time_data, event_time_schema
        )
        event_time_list = [list(row) for row in event_time_df.select("*").collect()]

        request_id_schema = StructType(
            [
                StructField("request_id", StringType(), True),
            ]
        )
        request_id_data = [
            ("request_1_1",),
            ("request_2_2",),
            ("request_3_3",),
        ]
        request_id_df = spark_engine._spark_session.createDataFrame(
            request_id_data, request_id_schema
        )
        request_id_list = [list(row) for row in request_id_df.select("*").collect()]

        request_parameters_schema = StructType(
            [
                StructField("rp_1", IntegerType(), True),
                StructField("rp_2", IntegerType(), True),
            ]
        )
        request_parameters_data = [
            (100, 200),
            (200, 300),
            (300, 400),
        ]

        request_parameters_df = spark_engine._spark_session.createDataFrame(
            request_parameters_data, request_parameters_schema
        )
        request_parameters_list = [
            list(row) for row in request_parameters_df.select("*").collect()
        ]

        # Specify column names for each type of data
        args = TestSpark.get_logging_arguments(
            logging_data=spark_df,
            untransformed_features=untransformed_features_list,
            transformed_features=transformed_features_list,
            predictions=predictions_list,
            serving_keys=serving_keys_list,
            helper_columns=inference_helper_list,
            extra_logging_features=extra_log_columns_list,
            event_time=event_time_list,
            request_id=request_id_list,
            request_parameters=request_parameters_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )
        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            spark_engine.get_feature_logging_df(**args)
        )

        # Assert
        logging_feature_names = [feature.name for feature in logging_features]
        expected_dataframe, expected_columns, additional_features, missing_features = (
            TestSpark.get_expected_logging_df(
                extra_log_columns_df=extra_log_columns_df,
                event_time_df=event_time_df,
                request_id_df=request_id_df,
                request_parameters_df=request_parameters_df,
                untransformed_feature_df=untransformed_feature_df,
                transformed_feature_df=transformed_feature_df,
                predictions_df=predictions_df,
                serving_keys_df=serving_keys_df,
                inference_helper_df=inference_helper_df,
                logging_data=spark_df,
                logging_feature_group_features=logging_feature_group_features,
                spark_engine=spark_engine,
                column_names=column_names,
                metadata_logging_columns=meta_data_logging_columns,
            )
        )
        assert logging_dataframe.columns == expected_columns
        assert logging_dataframe.count() == 3
        assert logging_dataframe.columns == expected_columns
        assert (
            logging_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
            == expected_dataframe.sort("primary_key")
            .select(*logging_feature_names)
            .collect()
        )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_extract_logging_metadata_all_columns_and_drop_none(
        self, mocker, spark_engine, logging_test_dataframe
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["inference_helper_1"],
            labels=["label"],
        )

        fv.schema = [
            TrainingDatasetFeature(name="primary_key", type="bigint", label=False),
            TrainingDatasetFeature(name="event_time", type="timestamp", label=False),
            TrainingDatasetFeature(name="feature_1", type="float", label=False),
            TrainingDatasetFeature(name="feature_2", type="float", label=False),
            TrainingDatasetFeature(name="label", type="string", label=False),
            TrainingDatasetFeature(
                name="inference_helper_1", type="float", label=False
            ),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        untransformed_spark_df = logging_test_dataframe.select(
            "primary_key", "event_time", "feature_1", "feature_2", "inference_helper_1"
        )
        transformed_spark_df = logging_test_dataframe.select(
            "primary_key",
            "event_time",
            "min_max_scaler_feature_3",
            "inference_helper_1",
        )

        request_parameters_spark_df = logging_test_dataframe.select("rp_1", "rp_2")

        # Act
        untransformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters_spark_df,
        )

        transformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters_spark_df,
        )

        # Check the column in the returned dataframe are as expected.
        assert untransformed_result.columns == [
            "primary_key",
            "event_time",
            "feature_1",
            "feature_2",
            "inference_helper_1",
        ]
        assert transformed_result.columns == [
            "primary_key",
            "event_time",
            "min_max_scaler_feature_3",
            "inference_helper_1",
        ]

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert (
                result.hopsworks_logging_metadata.untransformed_features.columns
                == untransformed_spark_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.untransformed_features.collect()
                == untransformed_spark_df.collect()
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.columns
                == transformed_spark_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.collect()
                == transformed_spark_df.collect()
            )
            assert result.hopsworks_logging_metadata.event_time.columns == [
                "event_time"
            ]
            assert (
                result.hopsworks_logging_metadata.event_time.collect()
                == untransformed_spark_df.select("event_time").collect()
            )
            assert result.hopsworks_logging_metadata.serving_keys.columns == [
                "primary_key"
            ]
            assert (
                result.hopsworks_logging_metadata.serving_keys.collect()
                == untransformed_spark_df.select("primary_key").collect()
            )
            assert result.hopsworks_logging_metadata.inference_helper.columns == [
                "inference_helper_1"
            ]
            assert (
                result.hopsworks_logging_metadata.inference_helper.collect()
                == untransformed_spark_df.select("inference_helper_1").collect()
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters_spark_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.collect()
                == request_parameters_spark_df.collect()
            )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_extract_logging_metadata_all_columns_and_drop_all(
        self, mocker, spark_engine, logging_test_dataframe
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["inference_helper_1"],
            labels=["label"],
        )

        fv.schema = [
            TrainingDatasetFeature(name="primary_key", type="bigint", label=False),
            TrainingDatasetFeature(name="event_time", type="timestamp", label=False),
            TrainingDatasetFeature(name="feature_1", type="float", label=False),
            TrainingDatasetFeature(name="feature_2", type="float", label=False),
            TrainingDatasetFeature(name="label", type="string", label=False),
            TrainingDatasetFeature(
                name="inference_helper_1", type="float", label=False
            ),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        untransformed_spark_df = logging_test_dataframe.select(
            "primary_key", "event_time", "feature_1", "feature_2", "inference_helper_1"
        )
        transformed_spark_df = logging_test_dataframe.select(
            "primary_key",
            "event_time",
            "min_max_scaler_feature_3",
            "inference_helper_1",
        )
        request_parameters_spark_df = logging_test_dataframe.select("rp_1", "rp_2")

        # Act
        untransformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters_spark_df,
        )

        transformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters_spark_df,
        )

        # Check if the column in the returned dataframe are as expected.
        assert untransformed_result.columns == ["feature_1", "feature_2"]
        assert transformed_result.columns == [
            "min_max_scaler_feature_3",
        ]

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert result.hopsworks_logging_metadata.untransformed_features.columns == [
                "feature_1",
                "feature_2",
            ]
            assert (
                result.hopsworks_logging_metadata.untransformed_features.collect()
                == untransformed_spark_df.select("feature_1", "feature_2").collect()
            )
            assert result.hopsworks_logging_metadata.transformed_features.columns == [
                "min_max_scaler_feature_3",
            ]
            assert (
                result.hopsworks_logging_metadata.transformed_features.collect()
                == transformed_spark_df.select("min_max_scaler_feature_3").collect()
            )
            assert result.hopsworks_logging_metadata.event_time.columns == [
                "event_time"
            ]
            assert (
                result.hopsworks_logging_metadata.event_time.collect()
                == untransformed_spark_df.select("event_time").collect()
            )
            assert result.hopsworks_logging_metadata.serving_keys.columns == [
                "primary_key"
            ]
            assert (
                result.hopsworks_logging_metadata.serving_keys.collect()
                == untransformed_spark_df.select("primary_key").collect()
            )
            assert result.hopsworks_logging_metadata.inference_helper.columns == [
                "inference_helper_1"
            ]
            assert (
                result.hopsworks_logging_metadata.inference_helper.collect()
                == untransformed_spark_df.select("inference_helper_1").collect()
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters_spark_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.collect()
                == request_parameters_spark_df.collect()
            )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_extract_logging_metadata_all_columns_and_drop_none_fully_qualified_names(
        self, mocker, spark_engine, logging_test_dataframe
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_features()

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["inference_helper_1"],
            labels=["label"],
        )

        # Since only features are selected, the primary key and event time are not included in the schema.
        # They would be read as fully qualified names from the feature view if required.
        fv.schema = [
            TrainingDatasetFeature(name="feature_1", type="float", label=False),
            TrainingDatasetFeature(name="feature_2", type="float", label=False),
            TrainingDatasetFeature(name="label", type="string", label=False),
            TrainingDatasetFeature(
                name="inference_helper_1", type="float", label=False
            ),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        # Dataframes read has the fully qualified names for the primary key and event time.
        # The fully qualified name is constructed as <feature_store_name>_<feature_group_name>_<feature_group_version>_<feature_name>
        untransformed_spark_df = (
            logging_test_dataframe.select(
                "primary_key",
                "event_time",
                "feature_1",
                "feature_2",
                "inference_helper_1",
            )
            .withColumnRenamed("primary_key", "test_fs_test1_1_primary_key")
            .withColumnRenamed("event_time", "test_fs_test1_1_event_time")
        )
        transformed_spark_df = (
            logging_test_dataframe.select(
                "primary_key",
                "event_time",
                "min_max_scaler_feature_3",
                "inference_helper_1",
            )
            .withColumnRenamed("primary_key", "test_fs_test1_1_primary_key")
            .withColumnRenamed("event_time", "test_fs_test1_1_event_time")
        )
        request_parameters_spark_df = logging_test_dataframe.select("rp_1", "rp_2")

        # Act
        untransformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters_spark_df,
        )

        transformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters_spark_df,
        )

        # Assert

        # Check the column in the returned dataframe are as expected.
        assert untransformed_result.columns == [
            "test_fs_test1_1_primary_key",
            "test_fs_test1_1_event_time",
            "feature_1",
            "feature_2",
            "inference_helper_1",
        ]
        assert transformed_result.columns == [
            "test_fs_test1_1_primary_key",
            "test_fs_test1_1_event_time",
            "min_max_scaler_feature_3",
            "inference_helper_1",
        ]

        batch_data_df = untransformed_spark_df.withColumnRenamed(
            "test_fs_test1_1_primary_key", "primary_key"
        ).withColumnRenamed("test_fs_test1_1_event_time", "event_time")

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert (
                result.hopsworks_logging_metadata.untransformed_features.columns
                == untransformed_result.columns
            )
            assert (
                result.hopsworks_logging_metadata.untransformed_features.collect()
                == untransformed_result.collect()
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.columns
                == transformed_spark_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.collect()
                == transformed_spark_df.collect()
            )
            assert result.hopsworks_logging_metadata.event_time.columns == [
                "event_time"
            ]
            assert (
                result.hopsworks_logging_metadata.event_time.collect()
                == batch_data_df.select("event_time").collect()
            )
            assert result.hopsworks_logging_metadata.serving_keys.columns == [
                "primary_key"
            ]
            assert (
                result.hopsworks_logging_metadata.serving_keys.collect()
                == batch_data_df.select("primary_key").collect()
            )
            assert result.hopsworks_logging_metadata.inference_helper.columns == [
                "inference_helper_1"
            ]
            assert (
                result.hopsworks_logging_metadata.inference_helper.collect()
                == batch_data_df.select("inference_helper_1").collect()
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters_spark_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.collect()
                == request_parameters_spark_df.collect()
            )

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Skip on Windows since test is really slow due due to multiple collects used.",
    )
    def test_extract_logging_metadata_all_columns_and_drop_all_fully_qualified_names(
        self, mocker, spark_engine, logging_test_dataframe
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_features()

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["inference_helper_1"],
            labels=["label"],
        )

        # Since only features are selected, the primary key and event time are not included in the schema.
        # They would be read as fully qualified names from the feature view if required.
        fv.schema = [
            TrainingDatasetFeature(name="feature_1", type="float", label=False),
            TrainingDatasetFeature(name="feature_2", type="float", label=False),
            TrainingDatasetFeature(name="label", type="string", label=False),
            TrainingDatasetFeature(
                name="inference_helper_1", type="float", label=False
            ),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        # Dataframes read has the fully qualified names for the primary key and event time.
        # The fully qualified name is constructed as <feature_store_name>_<feature_group_name>_<feature_group_version>_<feature_name>
        untransformed_spark_df = (
            logging_test_dataframe.select(
                "primary_key",
                "event_time",
                "feature_1",
                "feature_2",
                "inference_helper_1",
            )
            .withColumnRenamed("primary_key", "test_fs_test1_1_primary_key")
            .withColumnRenamed("event_time", "test_fs_test1_1_event_time")
        )
        transformed_spark_df = (
            logging_test_dataframe.select(
                "primary_key",
                "event_time",
                "min_max_scaler_feature_3",
                "inference_helper_1",
            )
            .withColumnRenamed("primary_key", "test_fs_test1_1_primary_key")
            .withColumnRenamed("event_time", "test_fs_test1_1_event_time")
        )
        request_parameters_spark_df = logging_test_dataframe.select("rp_1", "rp_2")

        # Act
        untransformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters_spark_df,
        )

        transformed_result = spark_engine.extract_logging_metadata(
            untransformed_features=untransformed_spark_df,
            transformed_features=transformed_spark_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters_spark_df,
        )

        # Assert
        assert untransformed_result.columns == ["feature_1", "feature_2"]
        assert transformed_result.columns == [
            "min_max_scaler_feature_3",
        ]

        expected_untransformed_df = untransformed_spark_df.drop(
            *[
                "test_fs_test1_1_primary_key",
                "test_fs_test1_1_event_time",
                "inference_helper_1",
            ]
        )

        expected_transformed_df = transformed_spark_df.drop(
            *[
                "test_fs_test1_1_primary_key",
                "test_fs_test1_1_event_time",
                "inference_helper_1",
            ]
        )

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert (
                result.hopsworks_logging_metadata.untransformed_features.columns
                == expected_untransformed_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.untransformed_features.collect()
                == expected_untransformed_df.collect()
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.columns
                == expected_transformed_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.collect()
                == expected_transformed_df.collect()
            )
            assert result.hopsworks_logging_metadata.event_time.columns == [
                "event_time"
            ]
            assert (
                result.hopsworks_logging_metadata.event_time.collect()
                == untransformed_spark_df.select("test_fs_test1_1_event_time").collect()
            )
            assert result.hopsworks_logging_metadata.serving_keys.columns == [
                "primary_key"
            ]
            assert (
                result.hopsworks_logging_metadata.serving_keys.collect()
                == untransformed_spark_df.select(
                    "test_fs_test1_1_primary_key"
                ).collect()
            )
            assert result.hopsworks_logging_metadata.inference_helper.columns == [
                "inference_helper_1"
            ]
            assert (
                result.hopsworks_logging_metadata.inference_helper.collect()
                == untransformed_spark_df.select("inference_helper_1").collect()
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters_spark_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.collect()
                == request_parameters_spark_df.collect()
            )

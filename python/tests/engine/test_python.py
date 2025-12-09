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
import decimal
import json
import logging
from datetime import date, datetime

import hopsworks_common
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from hopsworks_common import constants
from hopsworks_common.core.constants import HAS_POLARS
from hsfs import (
    feature,
    feature_group,
    feature_view,
    storage_connector,
    training_dataset,
    util,
)
from hsfs.client import exceptions
from hsfs.constructor import query
from hsfs.constructor.hudi_feature_group_alias import HudiFeatureGroupAlias
from hsfs.core import inode, job, online_ingestion
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS
from hsfs.engine import python
from hsfs.expectation_suite import ExpectationSuite
from hsfs.hopsworks_udf import udf
from hsfs.serving_key import ServingKey
from hsfs.training_dataset_feature import TrainingDatasetFeature


if HAS_POLARS:
    import polars as pl
    from polars.testing import assert_frame_equal as polars_assert_frame_equal


hopsworks_common.connection._hsfs_engine_type = "python"


class TestPython:
    # Helper Functions

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

    @staticmethod
    def create_expected_logging_dataframe(
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
        meta_data_logging_columns=None,
    ):
        expected_df = pd.DataFrame()
        missing_features, additional_features = [], []
        meta_data_column_names = [feature.name for feature in meta_data_logging_columns]

        expected_columns = [feature.name for feature in logging_feature_group_features]
        for df in [
            logging_data,
            transformed_features,
            untransformed_features,
            predictions,
            serving_keys,
            helper_columns,
            request_parameters,
            event_time,
            request_id,
            extra_logging_features,
        ]:
            if df is not None:
                for col in df.columns:
                    expected_df[col] = df[col]

        for col in column_names["predictions"]:
            if col in expected_df.columns:
                expected_df[constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + col] = (
                    expected_df.pop(col)
                )

        for feature_name in expected_columns:
            if feature_name not in expected_df.columns:
                expected_df[feature_name] = None
                if (
                    feature_name not in meta_data_column_names
                    and feature_name
                    != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
                ):
                    missing_features.append(feature_name)

        if request_parameters is None:
            request_parameters = pd.DataFrame()
        for col in column_names["request_parameters"]:
            if col not in request_parameters.columns and col in expected_df.columns:
                request_parameters[col] = expected_df[col]

            if col not in request_parameters.columns:
                request_parameters[col] = None
                if col not in missing_features:
                    missing_features.append(col)
        if request_parameters.empty or all(pd.isna(request_parameters).all()):
            expected_df[constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME] = (
                constants.FEATURE_LOGGING.EMPTY_REQUEST_PARAMETER_COLUMN_VALUE
            )
        else:
            expected_df[constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME] = (
                request_parameters.apply(lambda row: json.dumps(row.to_dict()), axis=1)
            )

        for col in expected_df.columns:
            if col not in expected_columns:
                additional_features.append(col)

        return (
            expected_df[expected_columns],
            expected_columns,
            missing_features,
            additional_features,
        )

    # Pytest Fixtures
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
                constants.FEATURE_LOGGING.MODEL_VERSION_COLUMN_NAME, type="string"
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
    def logging_test_dataframe(self):
        return pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "feature_1": [0.25, 0.75, 1.1],
                "feature_2": [5.0, 10.2, 7.7],
                "feature_3": [100, 200, 300],
                "label": ["A", "B", "A"],
                "min_max_scaler_feature_3": [0.25, 0.75, 1.1],
                "rp_1": [1, 2, 3],
                "rp_2": [4, 5, 6],
                "extra_1": ["extra_a", "extra_b", "extra_c"],
                "extra_2": [10, 20, 30],
                "request_id": ["req_1", "req_2", "req_3"],
                "inference_helper_1": [0.95, 0.85, 0.76],
            }
        )

    def test_sql(self, mocker):
        # Arrange
        mock_python_engine_sql_offline = mocker.patch(
            "hsfs.engine.python.Engine._sql_offline"
        )
        mock_python_engine_jdbc = mocker.patch("hsfs.engine.python.Engine._jdbc")

        python_engine = python.Engine()

        # Act
        python_engine.sql(
            sql_query=None,
            feature_store=None,
            online_conn=None,
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_python_engine_sql_offline.call_count == 1
        assert mock_python_engine_jdbc.call_count == 0

    def test_sql_online_conn(self, mocker):
        # Arrange
        mock_python_engine_sql_offline = mocker.patch(
            "hsfs.engine.python.Engine._sql_offline"
        )
        mock_python_engine_jdbc = mocker.patch("hsfs.engine.python.Engine._jdbc")

        python_engine = python.Engine()

        # Act
        python_engine.sql(
            sql_query=None,
            feature_store=None,
            online_conn=mocker.Mock(),
            dataframe_type=None,
            read_options=None,
        )

        # Assert
        assert mock_python_engine_sql_offline.call_count == 0
        assert mock_python_engine_jdbc.call_count == 1

    def test_jdbc(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch(
            "hsfs.core.util_sql.create_mysql_engine"
        )
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client._is_external")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=query, connector=None, dataframe_type="default", read_options={}
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_jdbc_dataframe_type_none(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.util_sql.create_mysql_engine")
        mocker.patch("hopsworks_common.client.get_instance")
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as fstore_except:
            python_engine._jdbc(
                sql_query=query, connector=None, dataframe_type=None, read_options={}
            )

        # Assert
        assert (
            str(fstore_except.value)
            == 'dataframe_type : None not supported. Possible values are "default", "pandas", "polars", "numpy" or "python"'
        )

    def test_jdbc_read_options(self, mocker):
        # Arrange
        mock_util_create_mysql_engine = mocker.patch(
            "hsfs.core.util_sql.create_mysql_engine"
        )
        mocker.patch("hopsworks_common.client.get_instance")
        mock_python_engine_return_dataframe_type = mocker.patch(
            "hsfs.engine.python.Engine._return_dataframe_type"
        )
        query = "SELECT * FROM TABLE"

        python_engine = python.Engine()

        # Act
        python_engine._jdbc(
            sql_query=query,
            connector=None,
            dataframe_type="default",
            read_options={"external": ""},
        )

        # Assert
        assert mock_util_create_mysql_engine.call_count == 1
        assert mock_python_engine_return_dataframe_type.call_count == 1

    def test_read_none_data_format(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.read(
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
        mocker.patch("pandas.concat")

        python_engine = python.Engine()

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.read(
                storage_connector=None,
                data_format="",
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert str(e_info.value) == "data_format is not specified"

    def test_read_hopsfs_connector(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert mock_python_engine_read_s3.call_count == 0

    def test_read_s3_connector_endpoint_url(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")
        mock_boto_3 = mocker.patch("boto3.client")
        mock_boto_3.return_value.list_objects_v2.return_value = {"Contents": []}

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1,
            name="test_connector",
            featurestore_id=1,
            arguments=[{"name": "fs.s3a.endpoint", "value": "http://localhost:9000"}],
            access_key="test_access_key",
            secret_key="test_secret_key",
            region="eu-north-1",
        )

        # Act
        python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location="s3://test_bucket/test_file.csv",
            dataframe_type="default",
        )

        # Assert
        assert mock_boto_3.call_count == 1
        assert "endpoint_url" in mock_boto_3.call_args[1]
        assert mock_boto_3.call_args[1]["endpoint_url"] == "http://localhost:9000"
        assert "region_name" in mock_boto_3.call_args[1]
        assert mock_boto_3.call_args[1]["region_name"] == "eu-north-1"

    def test_read_hopsfs_connector_empty_dataframe(self, mocker):
        # Arrange

        # Setting list of empty dataframes as return value from _read_hopsfs
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs",
            return_value=[pd.DataFrame(), pd.DataFrame()],
        )

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        dataframe = python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert isinstance(dataframe, pd.DataFrame)
        assert len(dataframe) == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_hopsfs_connector_empty_dataframe_polars(self, mocker):
        # Arrange

        # Setting empty list as return value from _read_hopsfs
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs",
            return_value=[pl.DataFrame(), pl.DataFrame()],
        )

        python_engine = python.Engine()

        connector = storage_connector.HopsFSConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        dataframe = python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="polars",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 1
        assert isinstance(dataframe, pl.DataFrame)
        assert len(dataframe) == 0

    def test_read_s3_connector(self, mocker):
        # Arrange
        mocker.patch("pandas.concat")
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        python_engine.read(
            storage_connector=connector,
            data_format="csv",
            read_options=None,
            location=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_read_hopsfs.call_count == 0
        assert mock_python_engine_read_s3.call_count == 1

    def test_read_other_connector(self, mocker):
        # Arrange
        mock_python_engine_read_hopsfs = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs"
        )
        mock_python_engine_read_s3 = mocker.patch("hsfs.engine.python.Engine._read_s3")

        python_engine = python.Engine()

        connector = storage_connector.JdbcConnector(
            id=1, name="test_connector", featurestore_id=1
        )

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.read(
                storage_connector=connector,
                data_format="csv",
                read_options=None,
                location=None,
                dataframe_type="default",
            )

        # Assert
        assert (
            str(e_info.value)
            == "JDBC Storage Connectors for training datasets are not supported yet for external environments."
        )
        assert mock_python_engine_read_hopsfs.call_count == 0
        assert mock_python_engine_read_s3.call_count == 0

    def test_read_pandas_csv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_pandas(data_format="csv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_pandas_tsv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_pandas(data_format="tsv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_pandas_parquet(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        mock_obj = mocker.Mock()
        mock_obj.read.return_value = bytes()

        # Act
        python_engine._read_pandas(data_format="parquet", obj=mock_obj)

        # Assert
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 1

    def test_read_pandas_other(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("pandas.read_csv")
        mock_pandas_read_parquet = mocker.patch("pandas.read_parquet")

        python_engine = python.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._read_pandas(data_format="ocr", obj=None)

        # Assert
        assert (
            str(e_info.value)
            == "ocr training dataset format is not supported to read as pandas dataframe."
        )
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_csv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_polars(data_format="csv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_tsv(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        python_engine._read_polars(data_format="tsv", obj=None)

        # Assert
        assert mock_pandas_read_csv.call_count == 1
        assert mock_pandas_read_parquet.call_count == 0

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_parquet(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        mock_obj = mocker.Mock()
        mock_obj.read.return_value = bytes()

        # Act
        python_engine._read_polars(data_format="parquet", obj=mock_obj)

        # Assert
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 1

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_read_polars_other(self, mocker):
        # Arrange
        mock_pandas_read_csv = mocker.patch("polars.read_csv")
        mock_pandas_read_parquet = mocker.patch("polars.read_parquet")

        python_engine = python.Engine()

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._read_polars(data_format="ocr", obj=None)

        # Assert
        assert (
            str(e_info.value)
            == "ocr training dataset format is not supported to read as polars dataframe."
        )
        assert mock_pandas_read_csv.call_count == 0
        assert mock_pandas_read_parquet.call_count == 0

    def test_read_hopsfs(self, mocker):
        # Arrange
        mock_python_engine_read_hopsfs_remote = mocker.patch(
            "hsfs.engine.python.Engine._read_hopsfs_remote"
        )

        python_engine = python.Engine()

        # Act
        python_engine._read_hopsfs(location=None, data_format=None)

        # Assert
        assert mock_python_engine_read_hopsfs_remote.call_count == 1

    def test_read_hopsfs_remote(self, mocker):
        # Arrange
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        i = inode.Inode(attributes={"path": "test_path"})

        mock_dataset_api.return_value._list_dataset_path.return_value = (0, [i, i, i])
        mock_dataset_api.return_value.read_content.return_value.content = bytes()

        # Act
        python_engine._read_hopsfs_remote(location=None, data_format=None)

        # Assert
        assert mock_dataset_api.return_value._list_dataset_path.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 3

    def test_read_s3(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        mock_boto3_client.return_value.list_objects_v2.return_value = {
            "is_truncated": False,
            "Contents": [
                {"Key": "test", "Size": 1, "Body": ""},
                {"Key": "test1", "Size": 1, "Body": ""},
            ],
        }

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" not in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_s3_session_token(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1, session_token="test_token"
        )

        mock_boto3_client.return_value.list_objects_v2.return_value = {
            "is_truncated": False,
            "Contents": [
                {"Key": "test", "Size": 1, "Body": ""},
                {"Key": "test1", "Size": 1, "Body": ""},
            ],
        }

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 2

    def test_read_s3_next_continuation_token(self, mocker):
        # Arrange
        mock_boto3_client = mocker.patch("boto3.client")
        mock_python_engine_read_pandas = mocker.patch(
            "hsfs.engine.python.Engine._read_pandas"
        )

        python_engine = python.Engine()

        connector = storage_connector.S3Connector(
            id=1, name="test_connector", featurestore_id=1
        )

        mock_boto3_client.return_value.list_objects_v2.side_effect = [
            {
                "is_truncated": True,
                "NextContinuationToken": "test_token",
                "Contents": [
                    {"Key": "test", "Size": 1, "Body": ""},
                    {"Key": "test1", "Size": 1, "Body": ""},
                ],
            },
            {
                "is_truncated": False,
                "Contents": [
                    {"Key": "test2", "Size": 1, "Body": ""},
                    {"Key": "test3", "Size": 1, "Body": ""},
                ],
            },
        ]

        # Act
        python_engine._read_s3(
            storage_connector=connector, location="", data_format=None
        )

        # Assert
        assert "aws_access_key_id" in mock_boto3_client.call_args[1]
        assert "aws_secret_access_key" in mock_boto3_client.call_args[1]
        assert "aws_session_token" not in mock_boto3_client.call_args[1]
        assert mock_boto3_client.call_count == 1
        assert mock_python_engine_read_pandas.call_count == 4

    def test_read_options(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.read_options(data_format=None, provided_options=None)

        # Assert
        assert result == {}

    def test_read_options_stream_source(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.read_stream(
                storage_connector=None,
                message_format=None,
                schema=None,
                options=None,
                include_metadata=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Streaming Sources are not supported for pure Python Environments."
        )

    def test_show(self, mocker):
        # Arrange
        mock_python_engine_sql = mocker.patch("hsfs.engine.python.Engine.sql")

        python_engine = python.Engine()

        # Act
        python_engine.show(sql_query=None, feature_store=None, n=None, online_conn=None)

        # Assert
        assert mock_python_engine_sql.call_count == 1

    def test_cast_columns(self, mocker):
        python_engine = python.Engine()
        d = {
            "string": ["s", "s"],
            "bigint": [1, 2],
            "int": [1, 2],
            "smallint": [1, 2],
            "tinyint": [1, 2],
            "float": [1.0, 2.2],
            "double": [1.0, 2.2],
            "timestamp": [1641340800000, 1641340800000],
            "boolean": ["False", None],
            "date": ["2022-01-27", "2022-01-28"],
            "binary": [b"1", b"2"],
            "array<string>": ["['123']", "['1234']"],
            "struc": ["{'label':'blue','index':45}", "{'label':'blue','index':46}"],
            "decimal": ["1.1", "1.2"],
        }
        df = pd.DataFrame(data=d)
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
        cast_df = python_engine.cast_columns(df, schema)
        arrow_schema = pa.Schema.from_pandas(cast_df)
        expected = {
            "string": object,
            "bigint": pd.Int64Dtype(),
            "int": pd.Int32Dtype(),
            "smallint": pd.Int16Dtype(),
            "tinyint": pd.Int8Dtype(),
            "float": pd.Float32Dtype(),
            "double": pd.Float64Dtype(),
            "timestamp": np.dtype("datetime64[ns]"),
            "boolean": object,
            "date": np.dtype(date),
            "binary": object,
            "array<string>": object,
            "struc": object,
            "decimal": np.dtype(decimal.Decimal),
        }
        assert pa.types.is_string(arrow_schema.field("string").type)
        assert pa.types.is_boolean(arrow_schema.field("boolean").type)
        assert pa.types.is_binary(arrow_schema.field("binary").type)
        assert pa.types.is_list(arrow_schema.field("array<string>").type)
        assert pa.types.is_struct(arrow_schema.field("struc").type)
        for col in cast_df.columns:
            assert cast_df[col].dtype == expected[col]

    def test_register_external_temporary_table(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.register_external_temporary_table(
            external_fg=None, alias=None
        )

        # Assert
        assert result is None

    def test_register_hudi_temporary_table(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.register_hudi_temporary_table(
            hudi_fg_alias=None,
            feature_store_id=None,
            feature_store_name=None,
            read_options=None,
        )

        # Assert
        assert result is None

    def test_register_hudi_temporary_table_time_travel(self):
        # Arrange
        python_engine = python.Engine()
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )
        q = HudiFeatureGroupAlias(
            fg.to_dict(),
            "fg",
            left_feature_group_end_timestamp="20220101",
            left_feature_group_start_timestamp="20220101",
        )
        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.register_hudi_temporary_table(
                hudi_fg_alias=q,
                feature_store_id=None,
                feature_store_name=None,
                read_options=None,
            )

        # Assert
        assert str(e_info.value) == (
            "Incremental queries are not supported in the python client."
            + " Read feature group without timestamp to retrieve latest snapshot or switch to "
            + "environment with Spark Engine."
        )

    def test_register_hudi_temporary_table_time_travel_sub_query(self):
        # Arrange
        python_engine = python.Engine()
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )
        q = HudiFeatureGroupAlias(
            fg.to_dict(),
            "fg",
            left_feature_group_end_timestamp="20220101",
            left_feature_group_start_timestamp="20220101",
        )
        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            python_engine.register_hudi_temporary_table(
                hudi_fg_alias=q,
                feature_store_id=None,
                feature_store_name=None,
                read_options=None,
            )

        # Assert
        assert str(e_info.value) == (
            "Incremental queries are not supported in the python client."
            + " Read feature group without timestamp to retrieve latest snapshot or switch to "
            + "environment with Spark Engine."
        )

    def test_profile_pandas(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_pandas_with_null_column(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, None], "col3": [None, None]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_profile_polars(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pl.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_profile_polars_with_null_column(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "Fractional", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, None], "col3": [None, None]}
        df = pl.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=None,
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "Fractional", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col2", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 3

    def test_profile_relevant_columns(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.return_value = {
            "dataType": "Integral",
            "test_key": "test_value",
        }

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=["col1"],
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 1

    def test_profile_relevant_columns_diff_dtypes(self, mocker):
        # Arrange
        mock_python_engine_convert_pandas_statistics = mocker.patch(
            "hsfs.engine.python.Engine._convert_pandas_statistics"
        )

        python_engine = python.Engine()

        mock_python_engine_convert_pandas_statistics.side_effect = [
            {"dataType": "Integral", "test_key": "test_value"},
            {"dataType": "String", "test_key": "test_value"},
        ]

        d = {"col1": [1, 2], "col2": [0.1, 0.2], "col3": ["a", "b"]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.profile(
            df=df,
            relevant_columns=["col1", "col3"],
            correlations=None,
            histograms=None,
            exact_uniqueness=True,
        )

        # Assert
        assert (
            result
            == '{"columns": [{"dataType": "Integral", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col1", "completeness": 1}, '
            '{"dataType": "String", "test_key": "test_value", "isDataTypeInferred": "false", '
            '"column": "col3", "completeness": 1}]}'
        )
        assert mock_python_engine_convert_pandas_statistics.call_count == 2

    def test_convert_pandas_statistics(self):
        # Arrange
        python_engine = python.Engine()

        stat = {
            "25%": 25,
            "50%": 50,
            "75%": 75,
            "mean": 50,
            "count": 100,
            "max": 10,
            "std": 33,
            "min": 1,
        }

        # Act
        result = python_engine._convert_pandas_statistics(stat=stat, dataType="Integer")

        # Assert
        assert result == {
            "dataType": "Integer",
            "approxPercentiles": [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                25,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                50,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                75,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            "maximum": 10,
            "mean": 50,
            "minimum": 1,
            "stdDev": 33,
            "sum": 5000,
            "count": 100,
        }

    def test_validate(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.validate(dataframe=None, expectations=None, log_activity=True)

        # Assert
        assert (
            str(e_info.value)
            == "Deequ data validation is only available with Spark Engine. Use validate_with_great_expectations"
        )

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is not installed.",
    )
    def test_validate_with_great_expectations(self, mocker):
        # Arrange
        mock_ge_from_pandas = mocker.patch("great_expectations.from_pandas")

        python_engine = python.Engine()

        # Act
        python_engine.validate_with_great_expectations(
            dataframe=None, expectation_suite=None, ge_validate_kwargs={}
        )

        # Assert
        assert mock_ge_from_pandas.call_count == 1

    @pytest.mark.skipif(
        HAS_GREAT_EXPECTATIONS,
        reason="Great Expectations is installed.",
    )
    def test_validate_with_great_expectations_raise_module_not_found(self):
        # Arrange
        python_engine = python.Engine()
        suite = ExpectationSuite(
            expectation_suite_name="test_suite",
            expectations=[],
            meta={},
            run_validation=True,
        )

        # Act
        with pytest.raises(ModuleNotFoundError):
            python_engine.validate_with_great_expectations(
                dataframe=None, expectation_suite=suite, ge_validate_kwargs={}
            )

    def test_set_job_group(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.set_job_group(group_id=None, description=None)

        # Assert
        assert result is None

    def test_convert_to_default_dataframe_pandas(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains upper case letters in feature names: `['Col1']`. Feature names are sanitized to lower case in the feature store."
        )

    def test_convert_to_default_dataframe_pandas_with_spaces(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        d = {"col 1": [1, 2], "co 2 co": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        assert result.columns.values.tolist() == ["col_1", "co_2_co"]
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains feature names with spaces: `['col 1', 'co 2 co']`. "
            "Feature names are sanitized to use underscore '_' in the feature store."
        )

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_convert_to_default_dataframe_polars(self, mocker):
        # Arrange
        mock_warnings = mocker.patch("warnings.warn")

        python_engine = python.Engine()

        df = pl.DataFrame(
            [
                pl.Series("Col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "Date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone="Africa/Abidjan"),
                ),
            ]
        )

        # Resulting dataframe
        expected_converted_df = pl.DataFrame(
            [
                pl.Series("col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone=None),
                ),
            ]
        )

        # Act
        result = python_engine.convert_to_default_dataframe(dataframe=df)

        # Assert
        polars_assert_frame_equal(result, expected_converted_df)
        assert (
            mock_warnings.call_args[0][0]
            == "The ingested dataframe contains upper case letters in feature names: `['Col1', 'Date']`. Feature names are sanitized to lower case in the feature store."
        )

    def test_parse_schema_feature_group_pandas(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.type_systems.convert_pandas_dtype_to_offline_type")

        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine.parse_schema_feature_group(
            dataframe=df, time_travel_format=None
        )

        # Assert
        assert len(result) == 2
        assert result[0].name == "col1"
        assert result[1].name == "col2"

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_parse_schema_feature_group_polars(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.type_systems.convert_pandas_dtype_to_offline_type")

        python_engine = python.Engine()

        df = pl.DataFrame(
            [
                pl.Series("col1", [1, 2], dtype=pl.Float32),
                pl.Series("col2", [1, 2], dtype=pl.Int64),
                pl.Series(
                    "date",
                    [
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                        datetime.strptime("09/19/18 13:55:26", "%m/%d/%y %H:%M:%S"),
                    ],
                    pl.Datetime(time_zone="Africa/Abidjan"),
                ),
            ]
        )

        # Act
        result = python_engine.parse_schema_feature_group(
            dataframe=df, time_travel_format=None
        )

        # Assert
        assert len(result) == 3
        assert result[0].name == "col1"
        assert result[1].name == "col2"
        assert result[2].name == "date"

    def test_parse_schema_training_dataset(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.parse_schema_training_dataset(dataframe=None)

        # Assert
        assert (
            str(e_info.value)
            == "Training dataset creation from Dataframes is not supported in Python environment. Use HSFS Query object instead."
        )

    def test_save_dataframe(self, mocker):
        # Arrange
        mock_python_engine_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.legacy_save_dataframe"
        )
        mocker.patch("hsfs.engine.get_type")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
        )

        # Act
        python_engine.save_dataframe(
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
        assert mock_python_engine_write_dataframe_kafka.call_count == 0
        assert mock_python_engine_legacy_save_dataframe.call_count == 1

    def test_save_dataframe_stream(self, mocker):
        # Arrange
        mock_python_engine_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.legacy_save_dataframe"
        )

        python_engine = python.Engine()

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
        python_engine.save_dataframe(
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
        assert mock_python_engine_write_dataframe_kafka.call_count == 1
        assert mock_python_engine_legacy_save_dataframe.call_count == 0

    def test_save_dataframe_delta_time_travel_format(self, mocker):
        # Arrange
        mock_python_engine_write_dataframe_kafka = mocker.patch(
            "hsfs.engine.python.Engine._write_dataframe_kafka"
        )
        mock_python_engine_legacy_save_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.legacy_save_dataframe"
        )
        mock_delta_engine = mocker.patch("hsfs.core.delta_engine.DeltaEngine")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="DELTA",
        )

        test_dataframe = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})

        # Act
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=test_dataframe,
            operation="insert",
            online_enabled=False,
            storage="offline",
            offline_write_options={},
            online_write_options={},
            validation_id=None,
        )

        # Assert
        assert mock_python_engine_write_dataframe_kafka.call_count == 0
        assert mock_python_engine_legacy_save_dataframe.call_count == 0
        assert mock_delta_engine.call_count == 1

        # Verify DeltaEngine was called with correct parameters
        mock_delta_engine.assert_called_once_with(
            feature_store_id=fg.feature_store_id,
            feature_store_name=fg.feature_store_name,
            feature_group=fg,
            spark_session=None,
            spark_context=None,
        )

        # Verify save_delta_fg was called with correct parameters
        mock_delta_engine.return_value.save_delta_fg.assert_called_once_with(
            test_dataframe, write_options={}, validation_id=None
        )

    def test_save_dataframe_delta_calls_check_duplicate_records(self, mocker):
        # Arrange
        mock_check_duplicate_records = mocker.patch(
            "hsfs.engine.python.Engine._check_duplicate_records"
        )
        mock_delta_engine = mocker.patch("hsfs.core.delta_engine.DeltaEngine")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["pk1"],
            partition_key=[],
            id=99,
            stream=False,
            time_travel_format="DELTA",
        )

        test_dataframe = pd.DataFrame({"pk1": [1, 2, 3], "col2": [4, 5, 6]})

        # Act
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=test_dataframe,
            operation="insert",
            online_enabled=False,
            storage="offline",
            offline_write_options={},
            online_write_options={},
            validation_id=None,
        )

        # Assert
        assert mock_check_duplicate_records.call_count == 1
        mock_check_duplicate_records.assert_called_once_with(test_dataframe, fg)
        assert mock_delta_engine.call_count == 1

    def test_save_dataframe_non_delta_does_not_call_check_duplicate_records(
        self, mocker
    ):
        # Arrange
        mock_check_duplicate_records = mocker.patch(
            "hsfs.engine.python.Engine._check_duplicate_records"
        )
        mocker.patch("hsfs.engine.get_type", return_value="python")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["pk1"],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )

        test_dataframe = pd.DataFrame({"pk1": [1, 2, 3], "col2": [4, 5, 6]})

        # Act
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=test_dataframe,
            operation="insert",
            online_enabled=False,
            storage="offline",
            offline_write_options={},
            online_write_options={},
            validation_id=None,
        )

        # Assert
        assert mock_check_duplicate_records.call_count == 0

    @pytest.mark.parametrize(
        "test_name,primary_key,partition_key,event_time,data_dict",
        [
            (
                "duplicate_primary_key",
                ["id"],
                [],
                None,
                {"id": [1, 1, 2], "text": ["a", "a_dup", "b"]},
            ),
            (
                "duplicate_primary_key_partition",
                ["id"],
                ["p"],
                None,
                {"id": [1, 1, 2], "p": [0, 0, 0], "text": ["a_p0", "a_p0_dup", "b_p0"]},
            ),
            (
                "duplicate_primary_key_event_time",
                ["id"],
                [],
                "event_time",
                {
                    "id": [1, 1, 2],
                    "event_time": [
                        pd.Timestamp("2024-01-01"),
                        pd.Timestamp("2024-01-01"),
                        pd.Timestamp("2024-01-02"),
                    ],
                    "text": ["a_t1", "a_t1_dup", "b_t2"],
                },
            ),
        ],
    )
    def test_save_dataframe_delta_duplicate_should_fail(
        self, mocker, test_name, primary_key, partition_key, event_time, data_dict
    ):
        # Arrange
        mocker.patch("hsfs.core.delta_engine.DeltaEngine")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe",
            side_effect=lambda x: x,
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mocker.patch("hsfs.engine.python.Engine._write_dataframe_kafka")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name=f"dl_dup_{test_name}",
            version=1,
            featurestore_id=99,
            primary_key=primary_key,
            partition_key=partition_key,
            event_time=event_time,
            stream=False,
            time_travel_format="DELTA",
        )

        df = pd.DataFrame(data_dict)

        # Act & Assert
        with pytest.raises(exceptions.FeatureStoreException) as exc_info:
            python_engine.save_dataframe(
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
        "test_name,primary_key,partition_key,event_time,data_dict",
        [
            (
                "pk_partition_across",
                ["id"],
                ["p"],
                None,
                {"id": [1, 1, 2], "p": [0, 1, 0], "text": ["a_p0", "a_p1", "b_p0"]},
            ),
            (
                "pk_event_time_across",
                ["id"],
                [],
                "event_time",
                {
                    "id": [1, 1, 2],
                    "event_time": [
                        pd.Timestamp("2024-01-01"),
                        pd.Timestamp("2024-01-02"),
                        pd.Timestamp("2024-01-01"),
                    ],
                    "text": ["a_t1", "a_t2", "b_t1"],
                },
            ),
            (
                "pk_with_no_duplicate",
                ["id"],
                [],
                None,
                {"id": [1, 2, 3], "text": ["a", "b", "c"]},
            ),
            (
                "no_pk_partition_only",
                [],
                ["p"],
                None,
                {"id": [1, 1, 2], "p": [0, 1, 0], "text": ["a_p0", "a_p1", "b_p0"]},
            ),
            (
                "no_pk_event_time_only",
                [],
                [],
                "event_time",
                {
                    "id": [1, 1, 2],
                    "event_time": [
                        pd.Timestamp("2024-01-01"),
                        pd.Timestamp("2024-01-02"),
                        pd.Timestamp("2024-01-01"),
                    ],
                    "text": ["a_t1", "a_t2", "b_t1"],
                },
            ),
            (
                "no_pk",
                [],
                [],
                None,
                {"id": [1, 1, 2], "text": ["a", "a_dup", "b"]},
            ),
        ],
    )
    def test_save_dataframe_delta_duplicate_should_succeed(
        self, mocker, test_name, primary_key, partition_key, event_time, data_dict
    ):
        # Arrange
        mocker.patch("hsfs.core.delta_engine.DeltaEngine")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe",
            side_effect=lambda x: x,
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mocker.patch("hsfs.engine.python.Engine._write_dataframe_kafka")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name=f"dl_dup_{test_name}",
            version=1,
            featurestore_id=99,
            primary_key=primary_key,
            partition_key=partition_key,
            event_time=event_time,
            stream=False,
            time_travel_format="DELTA",
        )

        df = pd.DataFrame(data_dict)

        # Act - should not raise exception
        python_engine.save_dataframe(
            feature_group=fg,
            dataframe=df,
            operation="insert",
            online_enabled=True,
            storage="offline",
            offline_write_options={},
            online_write_options={},
            validation_id=None,
        )

        # Assert - no exception should be raised

    def test_legacy_save_dataframe(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_get_url = mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_api.return_value._start.return_value = (
            hopsworks_common.execution.Execution(job=mocker.Mock())
        )
        mocker.patch("hsfs.engine.python.Engine._get_app_options")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_dataset_api = mocker.patch("hsfs.core.dataset_api.DatasetApi")

        python_engine = python.Engine()

        mock_fg_api.return_value.ingestion.return_value.job = job.Job(
            1, "test_job", None, None, None, None
        )

        # Act
        python_engine.legacy_save_dataframe(
            feature_group=mocker.Mock(),
            dataframe=None,
            operation=None,
            online_enabled=None,
            storage=None,
            offline_write_options={},
            online_write_options=None,
            validation_id=None,
        )

        # Assert
        assert mock_fg_api.return_value.ingestion.call_count == 1
        assert mock_dataset_api.return_value.upload_feature_group.call_count == 1
        assert mock_execution_api.return_value._start.call_count == 1
        assert mock_get_url.call_count == 1

    def test_get_training_data(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_python_engine_prepare_transform_split_df = mocker.patch(
            "hsfs.engine.python.Engine._prepare_transform_split_df"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.get_training_data(
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            query_obj=mocker.Mock(),
            read_options=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_prepare_transform_split_df.call_count == 0

    def test_get_training_data_splits(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_python_engine_prepare_transform_split_df = mocker.patch(
            "hsfs.engine.python.Engine._prepare_transform_split_df"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        python_engine = python.Engine()

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"name": "value"},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.get_training_data(
            training_dataset_obj=td,
            feature_view_obj=None,
            query_obj=mocker.Mock(),
            read_options=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_prepare_transform_split_df.call_count == 1

    def test_split_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels=None
        )

        # Assert
        assert str(result_df) == "   Col1  col2\n0     1     3\n1     2     4"
        assert str(result_df_split) == "None"

    def test_split_labels_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels=None
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert result_df_split is None

    def test_split_labels_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="pandas", labels=None
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert result_df_split is None

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_split_labels_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pl.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="polars", labels=None
        )

        # Assert
        assert isinstance(result_df, pl.DataFrame) or isinstance(
            result_df, pl.dataframe.frame.DataFrame
        )
        assert result_df_split is None

    def test_split_labels_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pd.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="python", labels=None
        )

        # Assert
        assert isinstance(result_df, list)
        assert result_df_split is None

    def test_split_labels_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"Col1": [1, 2], "col2": [3, 4]}

        df = pd.DataFrame(data=d)
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="numpy", labels=None
        )

        # Assert
        assert isinstance(result_df, np.ndarray)
        assert result_df_split is None

    def test_split_labels_labels(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels="col1"
        )

        # Assert
        assert str(result_df) == "   col2\n0     3\n1     4"
        assert str(result_df_split) == "0    1\n1    2\nName: col1, dtype: int64"

    def test_split_labels_labels_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="default", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert isinstance(result_df_split, pd.Series)

    def test_split_labels_labels_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="pandas", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert isinstance(result_df_split, pd.Series)

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_split_labels_labels_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pl.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="polars", labels="col1"
        )

        # Assert
        assert isinstance(result_df, pl.DataFrame) or isinstance(
            result_df, pl.dataframe.frame.DataFrame
        )
        assert isinstance(result_df_split, pl.Series)

    def test_split_labels_labels_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="python", labels="col1"
        )

        # Assert
        assert isinstance(result_df, list)
        assert isinstance(result_df_split, list)

    def test_split_labels_labels_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result_df, result_df_split = python_engine.split_labels(
            df=df, dataframe_type="numpy", labels="col1"
        )

        # Assert
        assert isinstance(result_df, np.ndarray)
        assert isinstance(result_df_split, np.ndarray)

    def test_prepare_transform_split_df_random_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_random_split = mocker.patch(
            "hsfs.engine.python.Engine._random_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        q = query.Query(left_feature_group=None, left_features=None)

        mock_python_engine_random_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_random_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_td_features(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
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

        q = query.Query(left_feature_group=fg, left_features=[])

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_query_features(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
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
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_prepare_transform_split_df_time_split_query_features_fully_qualified_name(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.constructor.query.Query.read")
        mock_python_engine_time_series_split = mocker.patch(
            "hsfs.engine.python.Engine._time_series_split"
        )
        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_feature_view = mocker.patch("hsfs.feature_view.FeatureView")

        python_engine = python.Engine()

        d = {
            "col1": [1, 2],
            "col2": [3, 4],
            "test_fs_test_1_event_time": [1000000000, 2000000000],
        }
        df = pd.DataFrame(data=d)

        mock_python_engine_time_series_split.return_value = {
            "train": df.loc[df["col1"] == 1],
            "test": df.loc[df["col1"] == 2],
        }

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
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
            features=[f, f1, f2],
            partition_key=[],
            id=10,
            event_time="event_time",
            featurestore_name="test_fs",
        )

        q = query.Query(left_feature_group=fg, left_features=[f, f1, f2])

        # Act
        result = python_engine._prepare_transform_split_df(
            query_obj=q,
            training_dataset_obj=td,
            feature_view_obj=mock_feature_view,
            read_option=None,
            dataframe_type="default",
        )

        # Assert
        assert mock_python_engine_time_series_split.call_count == 1
        assert isinstance(result["train"], pd.DataFrame)
        assert isinstance(result["test"], pd.DataFrame)

    def test_random_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["test_split1", "test_split2"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_size_precision_1(self, mocker):
        # In python sum([0.6, 0.3, 0.1]) != 1.0 due to floating point precision.
        # This test checks if different split ratios can be handled.
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2] * 10, "col2": [3, 4] * 10}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.6, "validation": 0.3, "test": 0.1},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["train", "validation", "test"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_size_precision_2(self, mocker):
        # This test checks if the method can handle split ratio with high precision.
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2] * 10, "col2": [3, 4] * 10}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 1 / 3, "validation": 1 - 1 / 3 - 0.1, "test": 0.1},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        result = python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert list(result) == ["train", "validation", "test"]
        for column in list(result):
            assert not result[column].empty

    def test_random_split_bad_percentage(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.2},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            python_engine._random_split(df=df, training_dataset_obj=td)

        # Assert
        assert (
            str(e_info.value)
            == "Sum of split ratios should be 1 and each values should be in range (0, 1)"
        )

    def test_time_series_split(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [], "col2": [], "event_time": []}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_time_series_split_drop_event_time(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [], "col2": [], "event_time": []}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}
        expected["train"] = expected["train"].drop(["event_time"], axis=1)
        expected["test"] = expected["test"].drop(["event_time"], axis=1)

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=True,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_time_series_split_event_time(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4], "event_time": [1000000000, 2000000000]}
        df = pd.DataFrame(data=d)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"col1": None, "col2": None},
            label=["f", "f_wrong"],
            id=10,
            train_start=1000000000,
            train_end=2000000000,
            test_end=3000000000,
        )

        expected = {"train": df.loc[df["col1"] == 1], "test": df.loc[df["col1"] == 2]}

        # Act
        result = python_engine._time_series_split(
            df=df,
            training_dataset_obj=td,
            event_time="event_time",
            drop_event_time=False,
        )

        # Assert
        assert list(result) == list(expected)
        for column in list(result):
            assert result[column].equals(expected[column])

    def test_convert_to_unix_timestamp_pandas(self):
        # Act
        result = util.convert_event_time_to_timestamp(
            event_time=pd.Timestamp("2017-01-01")
        )

        # Assert
        assert result == 1483228800000.0

    def test_convert_to_unix_timestamp_str(self, mocker):
        # Arrange
        mock_util_get_timestamp_from_date_string = mocker.patch(
            "hopsworks_common.util.get_timestamp_from_date_string"
        )

        mock_util_get_timestamp_from_date_string.return_value = 1483225200000

        # Act
        result = util.convert_event_time_to_timestamp(
            event_time="2017-01-01 00-00-00-000"
        )

        # Assert
        assert result == 1483225200000

    def test_convert_to_unix_timestamp_int(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=1483225200)

        # Assert
        assert result == 1483225200000

    def test_convert_to_unix_timestamp_datetime(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=datetime(2022, 9, 18))

        # Assert
        assert result == 1663459200000

    def test_convert_to_unix_timestamp_date(self):
        # Act
        result = util.convert_event_time_to_timestamp(event_time=date(2022, 9, 18))

        # Assert
        assert result == 1663459200000

    def test_convert_to_unix_timestamp_pandas_datetime(self):
        # Act
        result = util.convert_event_time_to_timestamp(
            event_time=pd.Timestamp("2022-09-18")
        )

        # Assert
        assert result == 1663459200000

    def test_write_training_dataset(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.util.get_job_url")
        mock_python_engine_wait_for_job = mocker.patch(
            "hopsworks_common.engine.execution_engine.ExecutionEngine.wait_until_finished"
        )

        python_engine = python.Engine()

        # Act
        with pytest.raises(Exception) as e_info:
            python_engine.write_training_dataset(
                training_dataset=None,
                dataset=None,
                user_write_options={},
                save_mode=None,
                feature_view_obj=None,
                to_df=False,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Currently only query based training datasets are supported by the Python engine"
        )
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 0
        assert mock_td_api.return_value.compute.call_count == 0
        assert mock_python_engine_wait_for_job.call_count == 0

    def test_write_training_dataset_query_td(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_job = mocker.patch("hsfs.core.job.Job")

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mock_td_api.return_value.compute.return_value = mock_job
        mocker.patch("hsfs.util.get_job_url")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        q = query.Query(fg, fg.features)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        # Act
        python_engine.write_training_dataset(
            training_dataset=td,
            dataset=q,
            user_write_options={},
            save_mode=None,
            feature_view_obj=None,
            to_df=False,
        )

        # Assert
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 0
        assert mock_td_api.return_value.compute.call_count == 1
        assert mock_job._wait_for_job.call_count == 1

    def test_write_training_dataset_query_fv(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.training_dataset_job_conf.TrainingDatasetJobConf")
        mock_job = mocker.patch("hsfs.core.job.Job")
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_api.return_value.compute_training_dataset.return_value = mock_job

        mock_td_api = mocker.patch("hsfs.core.training_dataset_api.TrainingDatasetApi")
        mocker.patch("hsfs.util.get_job_url")

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup.from_response_json(
            backend_fixtures["feature_group"]["get"]["response"]
        )
        q = query.Query(fg, fg.features)

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"test_split1": 0.5, "test_split2": 0.5},
            label=["f", "f_wrong"],
            id=10,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=q,
            featurestore_id=99,
            labels=[],
        )

        # Act
        python_engine.write_training_dataset(
            training_dataset=td,
            dataset=q,
            user_write_options={},
            save_mode=None,
            feature_view_obj=fv,
            to_df=False,
        )

        # Assert
        assert mock_fv_api.return_value.compute_training_dataset.call_count == 1
        assert mock_td_api.return_value.compute.call_count == 0
        assert mock_job._wait_for_job.call_count == 1

    def test_return_dataframe_type_default(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="default"
        )

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"

    def test_return_dataframe_type_pandas(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="pandas"
        )

        # Assert
        assert str(result) == "   col1  col2\n0     1     3\n1     2     4"

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_return_dataframe_type_polars(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pl.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="pandas"
        )

        # Assert
        assert isinstance(result, pl.DataFrame) or isinstance(
            result, pl.dataframe.frame.DataFrame
        )
        assert df.equals(result)

    def test_return_dataframe_type_numpy(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="numpy"
        )

        # Assert
        assert str(result) == "[[1 3]\n [2 4]]"

    def test_return_dataframe_type_python(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        result = python_engine._return_dataframe_type(
            dataframe=df, dataframe_type="python"
        )

        # Assert
        assert result == [[1, 3], [2, 4]]

    def test_return_dataframe_type_other(self):
        # Arrange
        python_engine = python.Engine()

        d = {"col1": [1, 2], "col2": [3, 4]}
        df = pd.DataFrame(data=d)

        # Act
        with pytest.raises(TypeError) as e_info:
            python_engine._return_dataframe_type(dataframe=df, dataframe_type="other")

        # Assert
        assert (
            str(e_info.value)
            == "Dataframe type `other` not supported on this platform."
        )

    def test_is_spark_dataframe(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        result = python_engine.is_spark_dataframe(dataframe=None)

        # Assert
        assert result is False

    def test_save_stream_dataframe(self):
        # Arrange
        python_engine = python.Engine()

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            python_engine.save_stream_dataframe(
                feature_group=None,
                dataframe=None,
                query_name=None,
                output_mode=None,
                await_termination=None,
                timeout=None,
                write_options=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Stream ingestion is not available on Python environments, because it requires Spark as engine."
        )

    def test_update_table_schema(self, mocker):
        # Arrange
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        python_engine = python.Engine()

        mock_fg_api.return_value.update_table_schema.return_value.job = job.Job(
            1, "test_job", None, None, None, None
        )

        # Act
        result = python_engine.update_table_schema(feature_group=None)

        # Assert
        assert result is None
        assert mock_fg_api.return_value.update_table_schema.call_count == 1

    def test_get_app_options(self, mocker):
        # Arrange
        mock_ingestion_job_conf = mocker.patch(
            "hsfs.core.ingestion_job_conf.IngestionJobConf"
        )

        python_engine = python.Engine()

        # Act
        python_engine._get_app_options(user_write_options={"spark": 1, "test": 2})

        # Assert
        assert mock_ingestion_job_conf.call_count == 1
        assert mock_ingestion_job_conf.call_args[1]["write_options"] == {"test": 2}

    @pytest.mark.parametrize(
        "distribute_arg",
        [
            None,  # Test without providing distribute argument (uses default)
            True,  # Test with distribute=True
            False,  # Test with distribute=False
        ],
    )
    def test_add_file(self, distribute_arg):
        # Arrange
        python_engine = python.Engine()

        file = None

        # Act
        if distribute_arg is None:
            # Call without distribute argument
            result = python_engine.add_file(file=file)
        else:
            # Call with distribute argument
            result = python_engine.add_file(file=file, distribute=distribute_arg)

        # Assert
        assert result == file

    def test_apply_transformation_function_udf_default_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

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
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    def test_apply_transformation_function_udf_pandas_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="pandas")
        def plus_one(col1):
            return col1 + 1

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
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    def test_apply_transformation_function_udf_python_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="python")
        def plus_one(col1):
            return col1 + 1

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
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.parametrize("execution_mode", ["default", "pandas", "python"])
    def test_apply_transformation_function_udf_transformation_context(
        self, mocker, execution_mode
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode=execution_mode)
        def plus_one(col1, context):
            return col1 + context["test"]

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
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions,
            dataset=df,
            transformation_context={"test": 10},
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 11
        assert result["plus_one_tf_name_"][1] == 12

    def test_apply_transformation_function_multiple_output_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"])
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    def test_apply_transformation_function_multiple_output_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="python")
        def plus_two(col1):
            return col1 + 1, col1 + 2

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    def test_apply_transformation_function_multiple_output_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="pandas")
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    def test_apply_transformation_function_multiple_input_output_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int])
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], mode="pandas")
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], mode="python")
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1", "col2"])
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["plus_two_col1_col2_0", "plus_two_col1_col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1", "col2"], mode="python")
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["plus_two_col1_col2_0", "plus_two_col1_col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_all_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1", "col2"], mode="pandas")
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(result.columns == ["plus_two_col1_col2_0", "plus_two_col1_col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_default_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"])
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns == ["col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_python_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="python")
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns == ["col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    def test_apply_transformation_function_multiple_input_output_drop_some_udf_pandas_mode(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf([int, int], drop=["col1"], mode="pandas")
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

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
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert all(
            result.columns == ["col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_apply_transformation_function_polars_udf_default_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def plus_one(col1):
            return col1 + 1

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
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_apply_transformation_function_polars_udf_python_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="python")
        def plus_one(col1):
            return col1 + 1

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
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_apply_transformation_function_polars_udf_pandas_mode(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int, mode="pandas")
        def plus_one(col1):
            return col1 + 1

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
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = python_engine._apply_transformation_function(
            transformation_functions=fv.transformation_functions, dataset=df
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    def test_get_unique_values(self):
        # Arrange
        python_engine = python.Engine()

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        result = python_engine.get_unique_values(
            feature_dataframe=df, feature_name="col1"
        )

        # Assert
        assert len(result) == 3
        assert 1 in result
        assert 2 in result
        assert 3 in result

    def test_apply_transformation_function_missing_feature_on_demand_transformations(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            transformation_functions=[add_one("missing_col1")],
            id=11,
            stream=False,
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as exception:
            python_engine._apply_transformation_function(
                transformation_functions=fg.transformation_functions, dataset=df
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, specified in the on-demand transformation function 'add_one' are not present in the dataframe being inserted into the feature group. "
            "Please verify that the correct feature names are used in the transformation function and that these features exist in the dataframe being inserted."
        )

    def test_apply_transformation_function_missing_feature_model_dependent_transformations(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        python_engine = python.Engine()

        @udf(int)
        def add_one(col1):
            return col1 + 1

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
            transformation_functions=[add_one("missing_col1")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as exception:
            python_engine._apply_transformation_function(
                transformation_functions=fv.transformation_functions, dataset=df
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, specified in the model-dependent transformation function 'add_one' are not present in the feature view. "
            "Please verify that the correct features are specified in the transformation function."
        )

    def test_materialization_kafka(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            return_value=" tests_offsets",
        )
        mocker.patch(
            "hsfs.core.job_api.JobApi.last_execution",
            return_value=["", ""],
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults",
            await_termination=False,
        )

    def test_materialization_kafka_first_job_execution(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            return_value="tests_offsets",
        )
        mocker.patch(
            "hsfs.core.job_api.JobApi.last_execution",
            return_value=[],
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults -initialCheckPointString tests_offsets",
            await_termination=False,
        )

    def test_materialization_kafka_skip_offsets(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            return_value="tests_offsets",
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={
                "start_offline_materialization": True,
                "skip_offsets": True,
            },
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults -initialCheckPointString tests_offsets",
            await_termination=False,
        )

    def test_materialization_kafka_topic_doesnt_exist(self, mocker):
        # Arrange
        mocker.patch("hsfs.core.kafka_engine.get_kafka_config", return_value={})
        mocker.patch("hsfs.feature_group.FeatureGroup._get_encoded_avro_schema")
        mocker.patch("hsfs.core.kafka_engine.get_encoder_func")
        mocker.patch("hsfs.core.kafka_engine.encode_complex_features")
        mock_python_engine_kafka_produce = mocker.patch(
            "hsfs.core.kafka_engine.kafka_produce"
        )
        mocker.patch("hsfs.util.get_job_url")
        mocker.patch(
            "hsfs.core.kafka_engine.kafka_get_offsets",
            side_effect=["", "tests_offsets"],
        )

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch(
            "hsfs.core.online_ingestion_api.OnlineIngestionApi.create_online_ingestion",
            return_value=online_ingestion.OnlineIngestion(id=123),
        )

        python_engine = python.Engine()

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )
        fg.feature_store = mocker.Mock()
        fg.feature_store.project_id = 234

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "test_topic"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        df = pd.DataFrame(data={"col1": [1, 2, 2, 3]})

        # Act
        python_engine._write_dataframe_kafka(
            feature_group=fg,
            dataframe=df,
            offline_write_options={"start_offline_materialization": True},
        )

        # Assert
        assert mock_python_engine_kafka_produce.call_count == 4
        job_mock.run.assert_called_once_with(
            args="defaults -initialCheckPointString tests_offsets",
            await_termination=False,
        )

    def test_test(self, mocker):
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            id=10,
            stream=False,
            time_travel_format="HUDI",
        )

        mocker.patch.object(fg, "commit_details", return_value={"commit1": 1})

        fg._online_topic_name = "topic_name"
        job_mock = mocker.MagicMock()
        job_mock.config = {"defaultArgs": "defaults"}
        fg._materialization_job = job_mock

        assert fg.materialization_job.config == {"defaultArgs": "defaults"}

    def test_extract_logging_metadata_all_columns_and_drop_none(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

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

        untransformed_df = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "feature_1": [0.25, 0.75, 1.1],
                "feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        # Mocked transformed dataframe usually created with a transformation function (transformation functions are not added in the feature view for simplicity)
        transformed_df = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "min_max_scaler_feature_1": [0.25, 0.75, 1.1],
                "min_max_scaler_feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        request_parameters = pd.DataFrame({"rp_1": [1, 2, 3], "rp_2": [4, 5, 6]})

        # Act
        untransformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters,
        )

        transformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters,
        )

        # Assert

        # Check the column in the returned dataframe are as expected.
        assert all(
            untransformed_result.columns
            == [
                "primary_key",
                "event_time",
                "feature_1",
                "feature_2",
                "inference_helper_1",
            ]
        )
        assert all(
            transformed_result.columns
            == [
                "primary_key",
                "event_time",
                "min_max_scaler_feature_1",
                "min_max_scaler_feature_2",
                "inference_helper_1",
            ]
        )

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert all(
                result.hopsworks_logging_metadata.untransformed_features.columns
                == untransformed_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.untransformed_features.values.tolist()
                == untransformed_df.values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.transformed_features.columns
                == transformed_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.values.tolist()
                == transformed_df.values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.event_time.columns == ["event_time"]
            )
            assert (
                result.hopsworks_logging_metadata.event_time.values.tolist()
                == untransformed_df[["event_time"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.serving_keys.columns == "primary_key"
            )
            assert (
                result.hopsworks_logging_metadata.serving_keys.values.tolist()
                == untransformed_df[["primary_key"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.inference_helper.columns
                == ["inference_helper_1"]
            )
            assert (
                result.hopsworks_logging_metadata.inference_helper.values.tolist()
                == untransformed_df[["inference_helper_1"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.values.tolist()
                == request_parameters.values.tolist()
            )

        assert all(
            transformed_result.columns
            == [
                "primary_key",
                "event_time",
                "min_max_scaler_feature_1",
                "min_max_scaler_feature_2",
                "inference_helper_1",
            ]
        )

    def test_extract_logging_metadata_all_columns_and_drop_all(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

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

        untransformed_df = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "feature_1": [0.25, 0.75, 1.1],
                "feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        # Mocked transformed dataframe usually created with a transformation function (transformation functions are not added in the feature view for simplicity)
        transformed_df = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "min_max_scaler_feature_1": [0.25, 0.75, 1.1],
                "min_max_scaler_feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        request_parameters = pd.DataFrame({"rp_1": [1, 2, 3], "rp_2": [4, 5, 6]})

        # Act
        untransformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters,
        )

        transformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters,
        )

        # Assert

        # Check if the column in the returned dataframe are as expected.
        assert all(untransformed_result.columns == ["feature_1", "feature_2"])
        assert all(
            transformed_result.columns
            == ["min_max_scaler_feature_1", "min_max_scaler_feature_2"]
        )

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert all(
                result.hopsworks_logging_metadata.untransformed_features.columns
                == ["feature_1", "feature_2"]
            )
            assert (
                result.hopsworks_logging_metadata.untransformed_features.values.tolist()
                == untransformed_df[["feature_1", "feature_2"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.transformed_features.columns
                == ["min_max_scaler_feature_1", "min_max_scaler_feature_2"]
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.values.tolist()
                == transformed_df[
                    ["min_max_scaler_feature_1", "min_max_scaler_feature_2"]
                ].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.event_time.columns == ["event_time"]
            )
            assert (
                result.hopsworks_logging_metadata.event_time.values.tolist()
                == untransformed_df[["event_time"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.serving_keys.columns == "primary_key"
            )
            assert (
                result.hopsworks_logging_metadata.serving_keys.values.tolist()
                == untransformed_df[["primary_key"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.inference_helper.columns
                == ["inference_helper_1"]
            )
            assert (
                result.hopsworks_logging_metadata.inference_helper.values.tolist()
                == untransformed_df[["inference_helper_1"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.values.tolist()
                == request_parameters.values.tolist()
            )

    def test_extract_logging_metadata_all_columns_and_drop_none_fully_qualified_names(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

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
        untransformed_df = pd.DataFrame(
            {
                "test_fs_test1_1_primary_key": [1, 2, 3],
                "test_fs_test1_1_event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "feature_1": [0.25, 0.75, 1.1],
                "feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        # Mocked transformed dataframe usually created with a transformation function (transformation functions are not added in the feature view for simplicity)
        transformed_df = pd.DataFrame(
            {
                "test_fs_test1_1_primary_key": [1, 2, 3],
                "test_fs_test1_1_event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "min_max_scaler_feature_1": [0.25, 0.75, 1.1],
                "min_max_scaler_feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        request_parameters = pd.DataFrame({"rp_1": [1, 2, 3], "rp_2": [4, 5, 6]})

        # Act
        untransformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters,
        )

        transformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=True,
            event_time=True,
            primary_key=True,
            request_parameters=request_parameters,
        )

        # Assert

        # Check the column in the returned dataframe are as expected.
        assert all(
            untransformed_result.columns
            == [
                "test_fs_test1_1_primary_key",
                "test_fs_test1_1_event_time",
                "feature_1",
                "feature_2",
                "inference_helper_1",
            ]
        )
        assert all(
            transformed_result.columns
            == [
                "test_fs_test1_1_primary_key",
                "test_fs_test1_1_event_time",
                "min_max_scaler_feature_1",
                "min_max_scaler_feature_2",
                "inference_helper_1",
            ]
        )

        batch_data_df = untransformed_df.rename(
            columns={
                "test_fs_test1_1_primary_key": "primary_key",
                "test_fs_test1_1_event_time": "event_time",
            }
        )

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert all(
                result.hopsworks_logging_metadata.untransformed_features.columns
                == untransformed_result.columns
            )
            assert (
                result.hopsworks_logging_metadata.untransformed_features.values.tolist()
                == untransformed_result.values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.transformed_features.columns
                == transformed_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.values.tolist()
                == transformed_df.values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.event_time.columns == ["event_time"]
            )
            assert (
                result.hopsworks_logging_metadata.event_time.values.tolist()
                == batch_data_df[["event_time"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.serving_keys.columns
                == ["primary_key"]
            )
            assert (
                result.hopsworks_logging_metadata.serving_keys.values.tolist()
                == batch_data_df[["primary_key"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.inference_helper.columns
                == ["inference_helper_1"]
            )
            assert (
                result.hopsworks_logging_metadata.inference_helper.values.tolist()
                == batch_data_df[["inference_helper_1"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.values.tolist()
                == request_parameters.values.tolist()
            )

    def test_extract_logging_metadata_all_columns_and_drop_all_fully_qualified_names(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

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
        untransformed_df = pd.DataFrame(
            {
                "test_fs_test1_1_primary_key": [1, 2, 3],
                "test_fs_test1_1_event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "feature_1": [0.25, 0.75, 1.1],
                "feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        # Mocked transformed dataframe usually created with a transformation function (transformation functions are not added in the feature view for simplicity)
        transformed_df = pd.DataFrame(
            {
                "test_fs_test1_1_primary_key": [1, 2, 3],
                "test_fs_test1_1_event_time": pd.to_datetime(
                    [
                        "2025-01-01 12:00:00",
                        "2025-01-02 13:30:00",
                        "2025-01-03 15:45:00",
                    ]
                ),
                "min_max_scaler_feature_1": [0.25, 0.75, 1.1],
                "min_max_scaler_feature_2": [5.0, 10.2, 7.7],
                "inference_helper_1": [0.99, 0.85, 0.76],
            }
        )

        request_parameters = pd.DataFrame({"rp_1": [1, 2, 3], "rp_2": [4, 5, 6]})

        # Act
        untransformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=False,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters,
        )

        transformed_result = python_engine.extract_logging_metadata(
            untransformed_features=untransformed_df,
            transformed_features=transformed_df,
            feature_view=fv,
            transformed=True,
            inference_helpers=False,
            event_time=False,
            primary_key=False,
            request_parameters=request_parameters,
        )

        # Assert
        assert all(untransformed_result.columns == ["feature_1", "feature_2"])
        assert all(
            transformed_result.columns
            == ["min_max_scaler_feature_1", "min_max_scaler_feature_2"]
        )

        expected_untransformed_df = untransformed_df.drop(
            columns=[
                "test_fs_test1_1_primary_key",
                "test_fs_test1_1_event_time",
                "inference_helper_1",
            ]
        )

        expected_transformed_df = transformed_df.drop(
            columns=[
                "test_fs_test1_1_primary_key",
                "test_fs_test1_1_event_time",
                "inference_helper_1",
            ]
        )

        # Check if the metadata is correctly attached to the returned dataframe.
        for result in [untransformed_result, transformed_result]:
            assert hasattr(result, "hopsworks_logging_metadata")
            assert all(
                result.hopsworks_logging_metadata.untransformed_features.columns
                == expected_untransformed_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.untransformed_features.values.tolist()
                == expected_untransformed_df.values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.transformed_features.columns
                == expected_transformed_df.columns
            )
            assert (
                result.hopsworks_logging_metadata.transformed_features.values.tolist()
                == expected_transformed_df.values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.event_time.columns == ["event_time"]
            )
            assert (
                result.hopsworks_logging_metadata.event_time.values.tolist()
                == untransformed_df[["test_fs_test1_1_event_time"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.serving_keys.columns
                == ["primary_key"]
            )
            assert (
                result.hopsworks_logging_metadata.serving_keys.values.tolist()
                == untransformed_df[["test_fs_test1_1_primary_key"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.inference_helper.columns
                == ["inference_helper_1"]
            )
            assert (
                result.hopsworks_logging_metadata.inference_helper.values.tolist()
                == untransformed_df[["inference_helper_1"]].values.tolist()
            )
            assert all(
                result.hopsworks_logging_metadata.request_parameters.columns
                == request_parameters.columns
            )
            assert (
                result.hopsworks_logging_metadata.request_parameters.values.tolist()
                == request_parameters.values.tolist()
            )

    def test_get_feature_logging_df_logging_data_no_missing_no_additional_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        logging_test_dataframes = [logging_test_dataframe]
        if HAS_POLARS:
            logging_test_dataframes.append(pl.from_pandas(logging_test_dataframe))

        for log_data in logging_test_dataframes:
            args = TestPython.get_logging_arguments(
                logging_data=log_data,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

            # Assert expected columns and values
            expected_log_data, expected_columns, _, _ = (
                TestPython.create_expected_logging_dataframe(
                    logging_data=log_data,
                    logging_feature_group_features=logging_feature_group_features,
                    column_names=column_names,
                    meta_data_logging_columns=meta_data_logging_columns,
                )
            )
            logging_feature_names = [feature.name for feature in logging_features]
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )

    def test_get_feature_logging_df_logging_data_no_missing_no_additional_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_features_names = [
            feature.name if feature.name != "predicted_label" else "label"
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]

        logging_test_dataframe_list = logging_test_dataframe[
            logging_features_names
        ].values.tolist()

        args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        # Assert expected columns and values
        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                logging_data=logging_test_dataframe,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_logging_data_no_missing_no_additional_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_features_names = [
            feature.name if feature.name != "predicted_label" else "label"
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]

        logging_test_dataframe_dict = logging_test_dataframe[
            logging_features_names
        ].to_dict(orient="records")

        args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        # Assert expected columns and values
        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                logging_data=logging_test_dataframe,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_logging_data_missing_columns_and_additional_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_test_dataframe = logging_test_dataframe.drop(
            columns=["feature_1", "feature_3", "rp_2"]
        )
        logging_test_dataframe["additional_1"] = [10, 20, 30]
        logging_test_dataframe["additional_2"] = ["a", "b", "c"]

        logging_test_dataframes = [logging_test_dataframe]
        if HAS_POLARS:
            logging_test_dataframes.append(pl.from_pandas(logging_test_dataframe))

        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                logging_data=logging_test_dataframe,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        caplog.set_level(logging.INFO)

        for log_data in logging_test_dataframes:
            args = TestPython.get_logging_arguments(
                logging_data=log_data,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert expected columns and values
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert sorted(additional_features) == sorted(additional_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )

            caplog.clear()

    def test_get_feature_logging_df_logging_data_missing_columns_and_additional_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_test_dataframe = logging_test_dataframe.drop(
            columns=["feature_1", "feature_3", "rp_2"]
        )
        logging_test_dataframe["additional_1"] = [10, 20, 30]
        logging_test_dataframe["additional_2"] = ["a", "b", "c"]
        log_data_dict = logging_test_dataframe.to_dict(orient="records")

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            logging_data=log_data_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                logging_data=logging_test_dataframe,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_logging_data_missing_columns_and_additional_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_test_dataframe = logging_test_dataframe.drop(
            columns=["feature_1", "feature_3", "rp_2"]
        )
        logging_test_dataframe["additional_1"] = [10, 20, 30]
        logging_test_dataframe["additional_2"] = ["a", "b", "c"]
        log_data_list = logging_test_dataframe.values.tolist()

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            args = TestPython.get_logging_arguments(
                logging_data=log_data_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.LOGGING_DATA}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.LOGGING_DATA}` to ensure that it has the following features : ['primary_key', 'event_time', 'feature_1', 'feature_2', 'feature_3', 'predicted_label', 'min_max_scaler_feature_3', 'extra_1', 'extra_2', 'inference_helper_1', 'rp_1', 'rp_2', 'request_id']."
        )

    def test_get_feature_logging_df_only_untransformed_features_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_testing_dfs = [
            logging_test_dataframe[["feature_1", "feature_2", "feature_3"]]
        ]

        if HAS_POLARS:
            untransformed_testing_dfs.append(
                pl.from_pandas(untransformed_testing_dfs[0])
            )

        caplog.set_level(logging.INFO)

        for untransformed_features in untransformed_testing_dfs:
            args = TestPython.get_logging_arguments(
                untransformed_features=untransformed_features,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )
            logging_feature_names = [feature.name for feature in logging_features]

            missing_features = {
                col
                for col in logging_feature_names + column_names["request_parameters"]
                if col not in untransformed_testing_dfs[0].columns
                and col != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
            }

            expected_log_data, expected_columns, missing_features, _ = (
                TestPython.create_expected_logging_dataframe(
                    untransformed_features=untransformed_testing_dfs[0],
                    logging_feature_group_features=logging_feature_group_features,
                    column_names=column_names,
                    meta_data_logging_columns=meta_data_logging_columns,
                )
            )

            # Assert log message for missing columns
            assert sorted(missing_features) == sorted(missing_logging_features)

            # Assert expected columns and values
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )

            caplog.clear()

    def test_get_feature_logging_df_only_untransformed_features_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "feature_3"]
        ]
        untransformed_features_dict = untransformed_features_df.to_dict(
            orient="records"
        )

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            untransformed_features=untransformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                untransformed_features=untransformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_untransformed_features_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "feature_3"]
        ]
        untransformed_features_list = untransformed_features_df.values.tolist()

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            untransformed_features=untransformed_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                untransformed_features=untransformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_untransformed_features_missing_and_additional_column_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[["feature_1", "feature_2"]]
        untransformed_features_df["additional_1"] = ["add_a", "add_b", "add_c"]

        untransformed_testing_dfs = [untransformed_features_df]
        if HAS_POLARS:
            untransformed_testing_dfs.append(pl.from_pandas(untransformed_features_df))

        caplog.set_level(logging.INFO)

        for untransformed_features in untransformed_testing_dfs:
            args = TestPython.get_logging_arguments(
                untransformed_features=untransformed_features,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert log message for missing columns
            logging_feature_names = [feature.name for feature in logging_features]
            (
                expected_log_data,
                expected_columns,
                missing_features,
                additional_features,
            ) = TestPython.create_expected_logging_dataframe(
                untransformed_features=untransformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert sorted(additional_features) == sorted(additional_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_untransformed_features_missing_and_additional_column_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[["feature_1", "feature_2"]]
        untransformed_features_df["additional_1"] = ["add_a", "add_b", "add_c"]
        untransformed_features_dict = untransformed_features_df.to_dict(
            orient="records"
        )

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            untransformed_features=untransformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                untransformed_features=untransformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)

        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_transformed_features_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_transformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "min_max_scaler_feature_3"]
        ]
        transformed_features_dfs = [pandas_transformed_features_df]
        if HAS_POLARS:
            transformed_features_dfs.append(
                pl.from_pandas(pandas_transformed_features_df)
            )

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                transformed_features=pandas_transformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        for transformed_features_df in transformed_features_dfs:
            args = TestPython.get_logging_arguments(
                transformed_features=transformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert expected columns and values
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )

            caplog.clear()

    def test_get_feature_logging_df_only_transformed_features_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        transformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "min_max_scaler_feature_3"]
        ]

        transformed_features_dict = transformed_features_df.to_dict(orient="records")

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            transformed_features=transformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                transformed_features=transformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_transformed_features_missing_and_additional_columns_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_transformed_features_df = logging_test_dataframe[["feature_1"]]
        pandas_transformed_features_df["additional_1"] = ["add_a", "add_b", "add_c"]

        transformed_features_dfs = [pandas_transformed_features_df]
        if HAS_POLARS:
            transformed_features_dfs.append(
                pl.from_pandas(pandas_transformed_features_df)
            )

        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                transformed_features=pandas_transformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        for transformed_features_df in transformed_features_dfs:
            args = TestPython.get_logging_arguments(
                transformed_features=transformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert log message for missing columns
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert sorted(additional_features) == sorted(additional_logging_features)

            # Assert expected columns and values
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_transformed_features_missing_and_additional_columns_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_transformed_features_df = logging_test_dataframe[["feature_1"]]
        pandas_transformed_features_df["additional_1"] = ["add_a", "add_b", "add_c"]
        transformed_features_dict = pandas_transformed_features_df.to_dict(
            orient="records"
        )

        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                transformed_features=pandas_transformed_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            transformed_features=transformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert log message for missing columns
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_transformed_features_missing_and_additional_columns_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_transformed_features_df = logging_test_dataframe[["feature_1"]]
        pandas_transformed_features_df["additional_1"] = ["add_a", "add_b", "add_c"]
        transformed_features_list = pandas_transformed_features_df.values.tolist()
        caplog.set_level(logging.INFO)

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestPython.get_logging_arguments(
                transformed_features=transformed_features_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.TRANSFORMED_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.TRANSFORMED_FEATURES}` to ensure that it has the following features : {column_names['transformed_features']}."
        )

    def test_get_feature_logging_df_only_predictions_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_predictions_df = logging_test_dataframe[["label"]]
        predictions_dfs = [pandas_predictions_df]

        if HAS_POLARS:
            predictions_dfs.append(pl.from_pandas(pandas_predictions_df))

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                predictions=pandas_predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)
        for predictions_df in predictions_dfs:
            args = TestPython.get_logging_arguments(
                predictions=predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert log message for missing columns
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_predictions_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        predictions_df = logging_test_dataframe[["label"]]
        predictions_dict = predictions_df.to_dict(orient="records")

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            predictions=predictions_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                predictions=predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_predictions_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        predictions_df = logging_test_dataframe[["label"]]
        predictions_list = predictions_df.values.tolist()

        caplog.set_level(logging.INFO)
        args = TestPython.get_logging_arguments(
            predictions=predictions_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert log message for missing columns
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                predictions=predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_prediction_missing_and_additional_column_dataframe(
        self,
        mocker,
        caplog,
        logging_features,
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        # Adding extra prediction columns for testing
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
        column_names["predictions"] = ["label1", "label2"]

        pandas_predictions_df = pd.DataFrame(
            {
                "label1": ["A", "B", "A"],
                "additions_col": ["A", "B", "A"],
            }
        )
        predictions_dfs = [pandas_predictions_df]
        if HAS_POLARS:
            predictions_dfs.append(pl.from_pandas(pandas_predictions_df))

        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                predictions=pandas_predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        for predictions_df in predictions_dfs:
            args = TestPython.get_logging_arguments(
                predictions=predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert log message for missing columns
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert sorted(additional_features) == sorted(additional_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_prediction_missing_and_additional_column_dict(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        # Adding extra prediction columns for testing
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
        column_names["predictions"] = ["label1", "label2"]
        predictions_df = pd.DataFrame(
            {
                "label1": ["A", "B", "A"],
                "additions_col": ["A", "B", "A"],
            }
        )
        predictions_dict = predictions_df.to_dict(orient="records")
        caplog.set_level(logging.INFO)
        args = TestPython.get_logging_arguments(
            predictions=predictions_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert log message for missing columns
        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                predictions=predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)

        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_prediction_missing_and_additional_column_list(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        # Adding extra prediction columns for testing
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
        column_names["predictions"] = ["label1", "label2"]

        predictions_df = pd.DataFrame(
            {
                "label1": ["A", "B", "A"],
                "additions_col": ["A", "B", "A"],
                "label2": ["C", "D", "C"],
                "extra_col": ["E", "F", "G"],
            }
        )
        predictions_list = predictions_df.values.tolist()
        caplog.set_level(logging.INFO)

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestPython.get_logging_arguments(
                predictions=predictions_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.PREDICTIONS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.PREDICTIONS}` to ensure that it has the following features : {column_names['predictions']}."
        )

    def test_get_feature_logging_df_only_serving_keys_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_serving_keys_df = logging_test_dataframe[["primary_key"]]
        serving_keys_dfs = [pandas_serving_keys_df]
        if HAS_POLARS:
            serving_keys_dfs.append(pl.from_pandas(pandas_serving_keys_df))

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                serving_keys=pandas_serving_keys_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        for serving_keys_df in serving_keys_dfs:
            args = TestPython.get_logging_arguments(
                serving_keys=serving_keys_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert expected columns and values
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_serving_keys_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = logging_test_dataframe[["primary_key"]]
        serving_keys_dict = serving_keys_df.to_dict(orient="records")

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                serving_keys=serving_keys_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            serving_keys=serving_keys_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_serving_keys_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = logging_test_dataframe[["primary_key"]]
        serving_keys_list = serving_keys_df.values.tolist()

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                serving_keys=serving_keys_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            serving_keys=serving_keys_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_serving_keys_missing_and_additional_column_dataframe(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        # Adding extra prediction columns for testing
        logging_features = [
            feature for feature in logging_features if feature.name != "primary_key"
        ]
        logging_features.append(feature.Feature("primary_key_1", type="int"))
        logging_features.append(feature.Feature("primary_key_2", type="int"))
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_serving_keys_df = pd.DataFrame(
            {
                "primary_key_1": [1, 2, 3],
                "primary_key_2": [4, 5, 6],
                "additions_col": [7, 8, 9],
            }
        )
        serving_keys_dfs = [pandas_serving_keys_df]
        if HAS_POLARS:
            serving_keys_dfs.append(pl.from_pandas(pandas_serving_keys_df))
        caplog.set_level(logging.INFO)
        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                serving_keys=pandas_serving_keys_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        for serving_keys_df in serving_keys_dfs:
            args = TestPython.get_logging_arguments(
                serving_keys=serving_keys_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert sorted(additional_features) == sorted(additional_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_serving_keys_missing_and_additional_column_dict(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_features = [
            feature for feature in logging_features if feature.name != "primary_key"
        ]
        logging_features.append(feature.Feature("primary_key_1", type="int"))
        logging_features.append(feature.Feature("primary_key_2", type="int"))
        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = pd.DataFrame(
            {
                "primary_key_1": [1, 2, 3],
                "primary_key_2": [4, 5, 6],
                "additions_col": [7, 8, 9],
            }
        )
        serving_keys_dict = serving_keys_df.to_dict(orient="records")
        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            serving_keys=serving_keys_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                serving_keys=serving_keys_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_serving_keys_missing_and_additional_column_list(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "primary_key2": [4, 5, 6],
                "additions_col": [7, 8, 9],
            }
        )
        serving_keys_list = serving_keys_df.values.tolist()
        caplog.set_level(logging.INFO)

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestPython.get_logging_arguments(
                serving_keys=serving_keys_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.SERVING_KEYS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.SERVING_KEYS}` to ensure that it has the following features : {column_names['serving_keys']}."
        )

    def test_get_feature_logging_df_only_inference_helper_columns_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_inference_helper_df = logging_test_dataframe[["inference_helper_1"]]
        inference_helper_dfs = [pandas_inference_helper_df]
        if HAS_POLARS:
            inference_helper_dfs.append(pl.from_pandas(pandas_inference_helper_df))
        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                helper_columns=pandas_inference_helper_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        for inference_helper_df in inference_helper_dfs:
            args = TestPython.get_logging_arguments(
                helper_columns=inference_helper_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert expected columns and values
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_inference_helper_columns_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helper_df = logging_test_dataframe[["inference_helper_1"]]
        inference_helper_dict = inference_helper_df.to_dict(orient="records")
        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                helper_columns=inference_helper_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        args = TestPython.get_logging_arguments(
            helper_columns=inference_helper_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert log message for missing columns
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_inference_helper_columns_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helper_df = logging_test_dataframe[["inference_helper_1"]]
        inference_helper_list = inference_helper_df.values.tolist()
        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                helper_columns=inference_helper_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        args = TestPython.get_logging_arguments(
            helper_columns=inference_helper_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_inference_helper_missing_and_additional_column_dataframe(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_inference_helper_df = pd.DataFrame(
            {
                "inference_helper": [0.1, 0.2, 0.3],
                "additions_col": [7, 8, 9],
            }
        )
        inference_helper_dfs = [pandas_inference_helper_df]
        if HAS_POLARS:
            inference_helper_dfs.append(pl.from_pandas(pandas_inference_helper_df))

        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                helper_columns=pandas_inference_helper_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        caplog.set_level(logging.INFO)

        for inference_helper_df in inference_helper_dfs:
            args = TestPython.get_logging_arguments(
                helper_columns=inference_helper_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert log message for missing columns
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert sorted(additional_features) == sorted(additional_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_inference_helper_missing_and_additional_column_dict(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helper_df = pd.DataFrame(
            {
                "inference_helper": [0.1, 0.2, 0.3],
                "additions_col": [7, 8, 9],
            }
        )
        inference_helper_dict = inference_helper_df.to_dict(orient="records")
        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                helper_columns=inference_helper_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            helper_columns=inference_helper_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert log message for missing columns
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_inference_helper_missing_and_additional_column_list(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helper_df = pd.DataFrame(
            {
                "inference_helper": [0.1, 0.2, 0.3],
                "additions_col": [7, 8, 9],
            }
        )
        inference_helper_list = inference_helper_df.values.tolist()

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestPython.get_logging_arguments(
                helper_columns=inference_helper_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS}` to ensure that it has the following features : {column_names['helper_columns']}."
        )

    def test_get_feature_logging_df_only_request_parameters_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_request_parameters_df = logging_test_dataframe[["rp_1", "rp_2"]]
        request_parameters_dfs = [pandas_request_parameters_df]
        if HAS_POLARS:
            request_parameters_dfs.append(pl.from_pandas(pandas_request_parameters_df))

        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                request_parameters=pandas_request_parameters_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        for request_parameters_df in request_parameters_dfs:
            args = TestPython.get_logging_arguments(
                request_parameters=request_parameters_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert log message for missing columns
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_request_parameters_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_parameters_df = logging_test_dataframe[["rp_1", "rp_2"]]
        request_parameters_dict = request_parameters_df.to_dict(orient="records")

        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                request_parameters=request_parameters_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        args = TestPython.get_logging_arguments(
            request_parameters=request_parameters_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert log message for missing columns
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_request_parameters_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_parameters_df = logging_test_dataframe[["rp_1", "rp_2"]]
        request_parameters_list = request_parameters_df.values.tolist()

        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                request_parameters=request_parameters_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        args = TestPython.get_logging_arguments(
            request_parameters=request_parameters_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert log message for missing columns
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_request_parameters_missing_and_additional_column_dataframe(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_request_parameters_df = pd.DataFrame(
            {
                "rp_2": [7, 8, 9],
                "additional_col": [10, 11, 12],
            }
        )
        request_parameters_dfs = [pandas_request_parameters_df]
        if HAS_POLARS:
            request_parameters_dfs.append(pl.from_pandas(pandas_request_parameters_df))
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                request_parameters=pandas_request_parameters_df.copy(),
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)
        for request_parameters_df in request_parameters_dfs:
            args = TestPython.get_logging_arguments(
                request_parameters=request_parameters_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert expected columns and values
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_request_parameters_missing_and_additional_column_dict(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_request_parameters_df = pd.DataFrame(
            {
                "rp_2": [7, 8, 9],
                "additional_col": [10, 11, 12],
            }
        )
        request_parameter_dict = pandas_request_parameters_df.to_dict(orient="records")

        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                request_parameters=pandas_request_parameters_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            request_parameters=request_parameter_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_request_parameters_missing_and_additional_column_list(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_request_parameters_df = pd.DataFrame(
            {
                "rp_1": [4, 5, 6],
                "rp_2": [7, 8, 9],
                "additional_col": [10, 11, 12],
            }
        )
        request_parameter_list = pandas_request_parameters_df.values.tolist()

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestPython.get_logging_arguments(
                request_parameters=request_parameter_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.REQUEST_PARAMETERS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.REQUEST_PARAMETERS}` to ensure that it has the following features : {column_names['request_parameters']}."
        )

    def test_get_feature_logging_df_only_event_time_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_event_time_df = logging_test_dataframe[["event_time"]]
        event_time_dfs = [pandas_event_time_df]
        if HAS_POLARS:
            event_time_dfs.append(pl.from_pandas(pandas_event_time_df))

        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                event_time=pandas_event_time_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        for event_time_df in event_time_dfs:
            args = TestPython.get_logging_arguments(
                event_time=event_time_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

            # Assert expected columns and values
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )

    def test_get_feature_logging_df_only_event_time_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        event_time_df = logging_test_dataframe[["event_time"]]
        event_time_dict = event_time_df.to_dict(orient="records")

        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                event_time=event_time_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            event_time=event_time_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_event_time_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        event_time_df = logging_test_dataframe[["event_time"]]
        event_time_list = event_time_df.values.tolist()

        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                event_time=event_time_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            event_time=event_time_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_request_id_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_request_id_df = logging_test_dataframe[["request_id"]]
        request_id_dfs = [pandas_request_id_df]
        if HAS_POLARS:
            request_id_dfs.append(pl.from_pandas(pandas_request_id_df))

        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                request_id=pandas_request_id_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        for request_id_df in request_id_dfs:
            args = TestPython.get_logging_arguments(
                request_id=request_id_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

            # Assert
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )

    def test_get_feature_logging_df_only_request_id_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_id_df = logging_test_dataframe[["request_id"]]
        request_id_dict = request_id_df.to_dict(orient="records")

        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                request_id=request_id_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            request_id=request_id_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        # Assert expected columns and values
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_request_id_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_id_df = logging_test_dataframe[["request_id"]]
        request_id_list = request_id_df.values.tolist()

        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                request_id=request_id_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            request_id=request_id_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        # Assert expected columns and values
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_extra_logging_columns_dataframe(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_extra_logging_features_df = logging_test_dataframe[
            ["extra_1", "extra_2"]
        ]
        extra_logging_features_dfs = [pandas_extra_logging_features_df]
        if HAS_POLARS:
            extra_logging_features_dfs.append(
                pl.from_pandas(pandas_extra_logging_features_df)
            )

        caplog.set_level(logging.INFO)
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                extra_logging_features=pandas_extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        for extra_logging_features_df in extra_logging_features_dfs:
            args = TestPython.get_logging_arguments(
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, _, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert expected columns and values
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_extra_logging_columns_dict(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        extra_logging_features_df = logging_test_dataframe[["extra_1", "extra_2"]]
        extra_logging_features_dict = extra_logging_features_df.to_dict(
            orient="records"
        )

        caplog.set_level(logging.INFO)
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        args = TestPython.get_logging_arguments(
            extra_logging_features=extra_logging_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_extra_logging_columns_list(
        self, mocker, caplog, logging_features, logging_test_dataframe
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        extra_logging_features_df = logging_test_dataframe[["extra_1", "extra_2"]]
        extra_logging_features_list = extra_logging_features_df.values.tolist()

        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        args = TestPython.get_logging_arguments(
            extra_logging_features=extra_logging_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_extra_logging_columns_missing_and_additional_column_dataframe(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        pandas_extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["extra_a", "extra_b", "extra_c"],
                "additional_column_1": ["add_a", "add_b", "add_c"],
            }
        )
        extra_logging_features_dfs = [pandas_extra_logging_features_df]
        if HAS_POLARS:
            extra_logging_features_dfs.append(
                pl.from_pandas(pandas_extra_logging_features_df)
            )

        caplog.set_level(logging.INFO)
        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                extra_logging_features=pandas_extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        for extra_logging_features_df in extra_logging_features_dfs:
            args = TestPython.get_logging_arguments(
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )

            # Act
            logging_dataframe, additional_logging_features, missing_logging_features = (
                python_engine.get_feature_logging_df(**args)
            )

            # Assert
            assert sorted(missing_features) == sorted(missing_logging_features)
            assert sorted(additional_features) == sorted(additional_logging_features)
            assert all(logging_dataframe.columns == expected_columns)
            assert (
                logging_dataframe[logging_feature_names].values.tolist()
                == expected_log_data[logging_feature_names].values.tolist()
            )
            caplog.clear()

    def test_get_feature_logging_df_only_extra_logging_columns_missing_and_additional_column_dict(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["extra_a", "extra_b", "extra_c"],
                "additional_column_1": ["add_a", "add_b", "add_c"],
            }
        )
        extra_logging_features_dict = extra_logging_features_df.to_dict(
            orient="records"
        )
        caplog.set_level(logging.INFO)

        expected_log_data, expected_columns, missing_features, additional_features = (
            TestPython.create_expected_logging_dataframe(
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        args = TestPython.get_logging_arguments(
            extra_logging_features=extra_logging_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, additional_logging_features, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        assert sorted(additional_features) == sorted(additional_logging_features)
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_only_extra_logging_columns_missing_and_additional_column_list(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        extra_logging_features_df = pd.DataFrame(
            {
                "additional_column_1": ["add_a", "add_b", "add_c"],
            }
        )
        extra_logging_features_list = extra_logging_features_df.values.tolist()
        caplog.set_level(logging.INFO)

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            args = TestPython.get_logging_arguments(
                extra_logging_features=extra_logging_features_list,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
            )
            # Act
            _ = python_engine.get_feature_logging_df(**args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES}` to ensure that it has the following features : {column_names['extra_logging_features']}."
        )

    def test_get_feature_logging_df_model_name_string(
        self, mocker, caplog, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        predictions_df = pd.DataFrame({"label": ["A", "B", "A"]})

        caplog.set_level(logging.INFO)

        args = TestPython.get_logging_arguments(
            predictions=predictions_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert
        expected_log_data, expected_columns, missing_features, _ = (
            TestPython.create_expected_logging_dataframe(
                predictions=predictions_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        assert sorted(missing_features) == sorted(missing_logging_features)
        assert all(
            logging_dataframe[constants.FEATURE_LOGGING.MODEL_COLUMN_NAME]
            == "test_model"
        )
        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_logging_data_override_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        # Override dataframes
        untransformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "feature_3": [200, 300, 400],
            }
        )

        transformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "min_max_scaler_feature_3": [10.25, 10.75, 11.1],
            }
        )

        predictions_df = pd.DataFrame(
            {
                "label": ["X", "Y", "X"],
            }
        )

        serving_keys_df = pd.DataFrame(
            {
                "primary_key": [11, 21, 31],
            }
        )

        inference_helpers_df = pd.DataFrame(
            {
                "inference_helper_1": [112, 212, 33],
            }
        )

        request_parameters_df = pd.DataFrame(
            {
                "rp_1": [11, 22, 33],
                "rp_2": [44, 55, 66],
            }
        )

        event_time = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2027-01-01 12:00:00",
                        "2027-01-02 13:30:00",
                        "2027-01-03 15:45:00",
                    ]
                ),
            }
        )

        request_id_df = pd.DataFrame(
            {
                "request_id": ["new_req_1", "new_req_2", "new_req_3"],
            }
        )

        extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["new_extra_a", "new_extra_b", "new_extra_c"],
                "extra_2": [101, 201, 301],
            }
        )

        args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe,
            transformed_features=transformed_features_df,
            untransformed_features=untransformed_features_df,
            predictions=predictions_df,
            serving_keys=serving_keys_df,
            helper_columns=inference_helpers_df,
            request_parameters=request_parameters_df,
            event_time=event_time,
            request_id=request_id_df,
            extra_logging_features=extra_logging_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_dataframe, _, missing_logging_features = (
            python_engine.get_feature_logging_df(**args)
        )

        # Assert expected columns and values
        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                logging_data=logging_test_dataframe,
                untransformed_features=untransformed_features_df,
                transformed_features=transformed_features_df,
                predictions=predictions_df,
                serving_keys=serving_keys_df,
                helper_columns=inference_helpers_df,
                request_parameters=request_parameters_df,
                event_time=event_time,
                request_id=request_id_df,
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_logging_data_override_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        # Override dataframes
        untransformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "feature_3": [200, 300, 400],
            }
        )
        untransformed_features_list = untransformed_features_df.values.tolist()

        transformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "min_max_scaler_feature_3": [10.25, 10.75, 11.1],
            }
        )
        transformed_features_list = transformed_features_df.values.tolist()

        predictions_df = pd.DataFrame(
            {
                "label": ["X", "Y", "X"],
            }
        )
        predictions_list = predictions_df.values.tolist()

        serving_keys_df = pd.DataFrame(
            {
                "primary_key": [11, 21, 31],
            }
        )
        serving_keys_list = serving_keys_df.values.tolist()

        inference_helpers_df = pd.DataFrame(
            {
                "inference_helper_1": [112, 212, 33],
            }
        )
        inference_helpers_list = inference_helpers_df.values.tolist()

        request_parameters_df = pd.DataFrame(
            {
                "rp_1": [11, 22, 33],
                "rp_2": [44, 55, 66],
            }
        )
        request_parameters_list = request_parameters_df.values.tolist()

        event_time = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2027-01-01 12:00:00",
                        "2027-01-02 13:30:00",
                        "2027-01-03 15:45:00",
                    ]
                ),
            }
        )
        event_time_list = event_time["event_time"].tolist()

        request_id_df = pd.DataFrame(
            {
                "request_id": ["new_req_1", "new_req_2", "new_req_3"],
            }
        )
        request_id_list = request_id_df["request_id"].values.tolist()

        extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["new_extra_a", "new_extra_b", "new_extra_c"],
                "extra_2": [101, 201, 301],
            }
        )
        extra_logging_features_list = extra_logging_features_df.values.tolist()

        args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe,
            transformed_features=transformed_features_list,
            untransformed_features=untransformed_features_list,
            predictions=predictions_list,
            serving_keys=serving_keys_list,
            helper_columns=inference_helpers_list,
            request_parameters=request_parameters_list,
            event_time=event_time_list,
            request_id=request_id_list,
            extra_logging_features=extra_logging_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                logging_data=logging_test_dataframe,
                untransformed_features=untransformed_features_df,
                transformed_features=transformed_features_df,
                predictions=predictions_df,
                serving_keys=serving_keys_df,
                helper_columns=inference_helpers_df,
                request_parameters=request_parameters_df,
                event_time=event_time,
                request_id=request_id_df,
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]
        # Assert expected columns and values

        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_df_logging_data_override_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        # Override dataframes
        untransformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "feature_3": [200, 300, 400],
            }
        )
        untransformed_features_dict = untransformed_features_df.to_dict(
            orient="records"
        )

        transformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "min_max_scaler_feature_3": [10.25, 10.75, 11.1],
            }
        )
        transformed_features_dict = transformed_features_df.to_dict(orient="records")

        predictions_df = pd.DataFrame(
            {
                "label": ["X", "Y", "X"],
            }
        )
        predictions_dict = predictions_df.to_dict(orient="records")

        serving_keys_df = pd.DataFrame(
            {
                "primary_key": [11, 21, 31],
            }
        )
        serving_keys_dict = serving_keys_df.to_dict(orient="records")

        inference_helpers_df = pd.DataFrame(
            {
                "inference_helper_1": [112, 212, 33],
            }
        )
        inference_helpers_dict = inference_helpers_df.to_dict(orient="records")

        request_parameters_df = pd.DataFrame(
            {
                "rp_1": [11, 22, 33],
                "rp_2": [44, 55, 66],
            }
        )
        request_parameters_dict = request_parameters_df.to_dict(orient="records")

        event_time = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2027-01-01 12:00:00",
                        "2027-01-02 13:30:00",
                        "2027-01-03 15:45:00",
                    ]
                ),
            }
        )
        event_time_dict = event_time.to_dict(orient="records")

        request_id_df = pd.DataFrame(
            {
                "request_id": ["new_req_1", "new_req_2", "new_req_3"],
            }
        )
        request_id_dict = request_id_df.to_dict(orient="records")

        extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["new_extra_a", "new_extra_b", "new_extra_c"],
                "extra_2": [101, 201, 301],
            }
        )
        extra_logging_features_dict = extra_logging_features_df.to_dict(
            orient="records"
        )

        logging_features_names = [
            feature.name if feature.name != "predicted_label" else "label"
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]

        logging_test_dataframe_dict = logging_test_dataframe[
            logging_features_names
        ].to_dict(orient="records")

        args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_dict,
            transformed_features=transformed_features_dict,
            untransformed_features=untransformed_features_dict,
            predictions=predictions_dict,
            serving_keys=serving_keys_dict,
            helper_columns=inference_helpers_dict,
            request_parameters=request_parameters_dict,
            event_time=event_time_dict,
            request_id=request_id_dict,
            extra_logging_features=extra_logging_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )
        logging_dataframe, _, _ = python_engine.get_feature_logging_df(**args)

        # Assert expected columns and values
        expected_log_data, expected_columns, _, _ = (
            TestPython.create_expected_logging_dataframe(
                logging_data=logging_test_dataframe,
                untransformed_features=untransformed_features_df,
                transformed_features=transformed_features_df,
                predictions=predictions_df,
                serving_keys=serving_keys_df,
                helper_columns=inference_helpers_df,
                request_parameters=request_parameters_df,
                event_time=event_time,
                request_id=request_id_df,
                extra_logging_features=extra_logging_features_df,
                logging_feature_group_features=logging_feature_group_features,
                column_names=column_names,
                meta_data_logging_columns=meta_data_logging_columns,
            )
        )
        logging_feature_names = [feature.name for feature in logging_features]

        assert all(logging_dataframe.columns == expected_columns)
        assert (
            logging_dataframe[logging_feature_names].values.tolist()
            == expected_log_data[logging_feature_names].values.tolist()
        )

    def test_get_feature_logging_list_logging_data_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        log_data_args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_logging_data_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [col.name for col in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_features_names = [
            feature.name if feature.name != "predicted_label" else "label"
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]

        logging_test_dataframe_list = logging_test_dataframe[
            logging_features_names
        ].values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            logging_data=logging_test_dataframe,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )

        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_logging_data_missing_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_test_dataframe = logging_test_dataframe.drop(
            columns=["feature_3", "label", "rp_1", "rp_2", "request_id"]
        )

        logging_test_dataframe_list = logging_test_dataframe.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.LOGGING_DATA}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.LOGGING_DATA}` to ensure that it has the following features : ['primary_key', 'event_time', 'feature_1', 'feature_2', 'feature_3', 'predicted_label', 'min_max_scaler_feature_3', 'extra_1', 'extra_2', 'inference_helper_1', 'rp_1', 'rp_2', 'request_id']."
        )

    def test_get_feature_logging_list_logging_data_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [col.name for col in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        logging_test_dataframe_dict = logging_test_dataframe.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            logging_data=logging_test_dataframe,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_untransformed_features_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "feature_3"]
        ]

        log_data_args = TestPython.get_logging_arguments(
            untransformed_features=untransformed_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_untransformed_features_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [col.name for col in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "feature_3"]
        ]
        untransformed_features_list = untransformed_features_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            untransformed_features=untransformed_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            untransformed_features=untransformed_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_untransformed_features_list_missing(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[["feature_1", "feature_3"]]
        untransformed_features_list = untransformed_features_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            untransformed_features=untransformed_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES}` to ensure that it has the following features : {column_names['untransformed_features']}."
        )

    def test_get_feature_logging_list_untransformed_features_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [col.name for col in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        untransformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "feature_3"]
        ]
        untransformed_features_dict = untransformed_features_df.to_dict(
            orient="records"
        )

        log_data_args = TestPython.get_logging_arguments(
            untransformed_features=untransformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            untransformed_features=untransformed_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_transformed_features_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        transformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "min_max_scaler_feature_3"]
        ]

        log_data_args = TestPython.get_logging_arguments(
            transformed_features=transformed_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_transformed_features_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_columnn_names = [col for col in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        transformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "min_max_scaler_feature_3"]
        ]
        transformed_features_list = transformed_features_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            transformed_features=transformed_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            transformed_features=transformed_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )

        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columnn_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_transformed_features_list_missing(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features

        transformed_features_df = logging_test_dataframe[
            ["feature_1", "min_max_scaler_feature_3"]
        ]
        transformed_features_list = transformed_features_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            transformed_features=transformed_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.TRANSFORMED_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.TRANSFORMED_FEATURES}` to ensure that it has the following features : {column_names['transformed_features']}."
        )

    def test_get_feature_logging_list_transformed_features_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [col.name for col in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        transformed_features_df = logging_test_dataframe[
            ["feature_1", "feature_2", "min_max_scaler_feature_3"]
        ]
        transformed_features_dict = transformed_features_df.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            transformed_features=transformed_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            transformed_features=transformed_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_predictions_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        predictions_df = logging_test_dataframe[["label"]]

        # Specify column names for each type of data
        log_data_args = TestPython.get_logging_arguments(
            predictions=predictions_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_predictions_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [col.name for col in meta_data_logging_columns]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        predictions_df = logging_test_dataframe[["label"]]

        predictions_list = predictions_df.values.tolist()

        # Specify column names for each type of data
        log_data_args = TestPython.get_logging_arguments(
            predictions=predictions_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            predictions=predictions_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )

        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_predictions_list_additional(
        self, mocker, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        predictions_df = pd.DataFrame(
            {
                "label1": ["A", "B", "A"],
                "label2": ["C", "D", "C"],
            }
        )

        predictions_list = predictions_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            predictions=predictions_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.PREDICTIONS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.PREDICTIONS}` to ensure that it has the following features : {column_names['predictions']}."
        )

    def test_get_feature_logging_list_predictions_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_columns_names = [
            col.name for col in meta_data_logging_columns
        ]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        predictions_df = logging_test_dataframe[["label"]]

        predictions_dict = predictions_df.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            predictions=predictions_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            predictions=predictions_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_serving_keys_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = logging_test_dataframe[["primary_key"]]

        # Specify column names for each type of data
        log_data_args = TestPython.get_logging_arguments(
            serving_keys=serving_keys_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        _ = python_engine.get_feature_logging_list(**log_data_args)

        # If any of the inputs is a dataframe, `get_feature_logging_df` should be a dataframe
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_serving_keys_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_columns_names = [
            col.name for col in meta_data_logging_columns
        ]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = logging_test_dataframe[["primary_key"]]
        serving_keys_list = serving_keys_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            serving_keys=serving_keys_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            serving_keys=serving_keys_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_serving_keys_list_additional(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = pd.DataFrame(
            {
                "primary_key1": [1, 2, 3],
                "primary_key2": [1, 2, 3],
            }
        )
        serving_keys_list = serving_keys_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            serving_keys=serving_keys_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.SERVING_KEYS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.SERVING_KEYS}` to ensure that it has the following features : {column_names['serving_keys']}."
        )

    def test_get_feature_logging_list_serving_keys_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_columns_names = [
            col.name for col in meta_data_logging_columns
        ]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        serving_keys_df = logging_test_dataframe[["primary_key"]]
        serving_keys_dict = serving_keys_df.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            serving_keys=serving_keys_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            serving_keys=serving_keys_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_helper_columns_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helpers_df = logging_test_dataframe[["inference_helper_1"]]

        log_data_args = TestPython.get_logging_arguments(
            helper_columns=inference_helpers_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_helper_columns_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_columns_names = [col for col in meta_data_logging_columns]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helpers_df = logging_test_dataframe[["inference_helper_1"]]
        inference_helpers_list = inference_helpers_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            helper_columns=inference_helpers_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            helper_columns=inference_helpers_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_helper_columns_list_additional(
        self, mocker, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helpers_df = pd.DataFrame(
            {
                "inference_helper_1": [1, 2, 3],
                "inference_helper_2": [4, 5, 6],
            }
        )
        inference_helpers_list = inference_helpers_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            helper_columns=inference_helpers_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS}` to ensure that it has the following features : {column_names['helper_columns']}."
        )

    def test_get_feature_logging_list_helper_columns_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_columns_names = [col for col in meta_data_logging_columns]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        inference_helpers_df = logging_test_dataframe[["inference_helper_1"]]
        inference_helpers_dict = inference_helpers_df.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            helper_columns=inference_helpers_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            helper_columns=inference_helpers_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )

        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_request_parameters_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        request_parameters_df = logging_test_dataframe[["rp_1", "rp_2"]]

        log_data_args = TestPython.get_logging_arguments(
            request_parameters=request_parameters_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_request_parameters_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [
            feature.name for feature in meta_data_logging_columns
        ]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_parameters_df = logging_test_dataframe[["rp_1", "rp_2"]]
        request_parameters_list = request_parameters_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            request_parameters=request_parameters_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            request_parameters=request_parameters_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_request_parameters_list_missing(
        self, mocker, logging_features
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_parameters_df = pd.DataFrame(
            {
                "rp_1": [1, 2, 3],
            }
        )
        request_parameters_list = request_parameters_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            request_parameters=request_parameters_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.REQUEST_PARAMETERS}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.REQUEST_PARAMETERS}` to ensure that it has the following features : {column_names['request_parameters']}."
        )

    def test_get_feature_logging_list_request_parameters_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_column_names = [
            feature.name for feature in meta_data_logging_columns
        ]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_parameters_df = logging_test_dataframe[["rp_1", "rp_2"]]
        logging_test_dataframe["rp_3"] = [7, 8, 9]
        request_parameters_dict = request_parameters_df.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            request_parameters=request_parameters_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            request_parameters=request_parameters_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_event_time_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        event_time = logging_test_dataframe[["event_time"]]

        log_data_args = TestPython.get_logging_arguments(
            event_time=event_time,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_event_time_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_column_names = [feature.name for feature in meta_data_logging_columns]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        event_time = logging_test_dataframe[["event_time"]]
        event_time_list = event_time["event_time"].tolist()

        # Specify column names for each type of data
        log_data_args = TestPython.get_logging_arguments(
            event_time=event_time_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            event_time=event_time,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_event_time_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_column_names = [feature.name for feature in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        event_time = logging_test_dataframe[["event_time"]]
        event_time_dict = event_time.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            event_time=event_time_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            event_time=event_time,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_request_id_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        request_id_df = logging_test_dataframe[["request_id"]]

        log_data_args = TestPython.get_logging_arguments(
            request_id=request_id_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_request_id_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_column_names = [feature.name for feature in meta_data_logging_columns]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_id_df = logging_test_dataframe[["request_id"]]
        request_id_list = request_id_df["request_id"].values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            request_id=request_id_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            request_id=request_id_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_request_id_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_column_names = [feature.name for feature in meta_data_logging_columns]

        logging_feature_group_features = meta_data_logging_columns + logging_features

        request_id_df = logging_test_dataframe[["request_id"]]
        request_id_dict = request_id_df.to_dict(orient="records")

        log_data_args = TestPython.get_logging_arguments(
            request_id=request_id_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            request_id=request_id_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_column_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_extra_logging_column_dataframe(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        meta_data_logging_columns, logging_features, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        extra_logging_features_df = logging_test_dataframe[["extra_1", "extra_2"]]

        log_data_args = TestPython.get_logging_arguments(
            extra_logging_features=extra_logging_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert python_engine.get_feature_logging_df.call_count == 1
        for key in log_data_args:
            assert (
                python_engine.get_feature_logging_df.call_args[1][key]
                is log_data_args[key]
            )

    def test_get_feature_logging_list_extra_logging_column_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        meta_data_logging_columns_names = [
            feature.name for feature in meta_data_logging_columns
        ]

        extra_logging_features_df = logging_test_dataframe[["extra_1", "extra_2"]]
        extra_logging_features_list = extra_logging_features_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            extra_logging_features=extra_logging_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            extra_logging_features=extra_logging_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_extra_logging_column_list_missing(
        self,
        mocker,
        logging_features,
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features

        logging_feature_group_features = meta_data_logging_columns + logging_features

        extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["extra_a", "extra_b", "extra_c"],
            }
        )
        extra_logging_features_list = extra_logging_features_df.values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            extra_logging_features=extra_logging_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        with pytest.raises(exceptions.FeatureStoreException) as exp:
            # Act
            _ = python_engine.get_feature_logging_list(**log_data_args)

        # Assert
        assert (
            str(exp.value)
            == f"Error logging data `{constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES}` do not have all required features. Please check the `{constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES}` to ensure that it has the following features : {column_names['extra_logging_features']}."
        )

    def test_get_feature_logging_list_extra_logging_column_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        meta_data_logging_columns_names = [
            feature.name for feature in meta_data_logging_columns
        ]
        logging_feature_group_features = meta_data_logging_columns + logging_features

        extra_logging_features_df = logging_test_dataframe[["extra_1", "extra_2"]]
        extra_logging_features_dict = extra_logging_features_df.to_dict(
            orient="records"
        )

        log_data_args = TestPython.get_logging_arguments(
            extra_logging_features=extra_logging_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            extra_logging_features=extra_logging_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_names:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_logging_data_override_list(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        meta_data_logging_columns_name = [
            feature.name for feature in meta_data_logging_columns
        ]

        # Override dataframes
        untransformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "feature_3": [200, 300, 400],
            }
        )
        untransformed_features_list = untransformed_features_df.values.tolist()

        transformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "min_max_scaler_feature_3": [10.25, 10.75, 11.1],
            }
        )
        transformed_features_list = transformed_features_df.values.tolist()

        predictions_df = pd.DataFrame(
            {
                "label": ["X", "Y", "X"],
            }
        )
        predictions_list = predictions_df.values.tolist()

        serving_keys_df = pd.DataFrame(
            {
                "primary_key": [11, 21, 31],
            }
        )
        serving_keys_list = serving_keys_df.values.tolist()

        inference_helpers_df = pd.DataFrame(
            {
                "inference_helper_1": [112, 212, 33],
            }
        )
        inference_helpers_list = inference_helpers_df.values.tolist()

        request_parameters_df = pd.DataFrame(
            {
                "rp_1": [11, 22, 33],
                "rp_2": [44, 55, 66],
            }
        )
        request_parameters_list = request_parameters_df.values.tolist()

        event_time = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2027-01-01 12:00:00",
                        "2027-01-02 13:30:00",
                        "2027-01-03 15:45:00",
                    ]
                ),
            }
        )
        event_time_list = event_time["event_time"].tolist()

        request_id_df = pd.DataFrame(
            {
                "request_id": ["new_req_1", "new_req_2", "new_req_3"],
            }
        )
        request_id_list = request_id_df["request_id"].values.tolist()

        extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["new_extra_a", "new_extra_b", "new_extra_c"],
                "extra_2": [101, 201, 301],
            }
        )
        extra_logging_features_list = extra_logging_features_df.values.tolist()

        logging_features_names = [
            feature.name if feature.name != "predicted_label" else "label"
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]

        logging_test_dataframe_list = logging_test_dataframe[
            logging_features_names
        ].values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_list,
            untransformed_features=untransformed_features_list,
            transformed_features=transformed_features_list,
            predictions=predictions_list,
            serving_keys=serving_keys_list,
            helper_columns=inference_helpers_list,
            request_parameters=request_parameters_list,
            event_time=event_time_list,
            request_id=request_id_list,
            extra_logging_features=extra_logging_features_list,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            logging_data=logging_test_dataframe,
            untransformed_features=untransformed_features_df,
            transformed_features=transformed_features_df,
            predictions=predictions_df,
            serving_keys=serving_keys_df,
            helper_columns=inference_helpers_df,
            request_parameters=request_parameters_df,
            event_time=event_time,
            request_id=request_id_df,
            extra_logging_features=extra_logging_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        # Assert
        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_name:
                    assert row[key] == expected_row[key]

    def test_get_feature_logging_list_logging_data_override_dict(
        self, mocker, logging_features, logging_test_dataframe
    ):
        # Prepare
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        python_engine = python.Engine()
        mocker.patch.object(
            python_engine,
            "get_feature_logging_df",
            return_value=(pd.DataFrame(), None, None),
        )

        logging_features, meta_data_logging_columns, column_names = logging_features
        logging_feature_group_features = meta_data_logging_columns + logging_features
        meta_data_logging_columns_name = [
            feature.name for feature in meta_data_logging_columns
        ]

        # Override dataframes
        untransformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "feature_3": [200, 300, 400],
            }
        )
        untransformed_features_dict = untransformed_features_df.to_dict(
            orient="records"
        )

        transformed_features_df = pd.DataFrame(
            {
                "feature_1": [1.25, 2.75, 3.1],
                "feature_2": [15.0, 20.2, 37.7],
                "min_max_scaler_feature_3": [10.25, 10.75, 11.1],
            }
        )
        transformed_features_dict = transformed_features_df.to_dict(orient="records")

        predictions_df = pd.DataFrame(
            {
                "label": ["X", "Y", "X"],
            }
        )
        predictions_dict = predictions_df.to_dict(orient="records")

        serving_keys_df = pd.DataFrame(
            {
                "primary_key": [11, 21, 31],
            }
        )
        serving_keys_dict = serving_keys_df.to_dict(orient="records")

        inference_helpers_df = pd.DataFrame(
            {
                "inference_helper_1": [112, 212, 33],
            }
        )
        inference_helpers_dict = inference_helpers_df.to_dict(orient="records")

        request_parameters_df = pd.DataFrame(
            {
                "rp_1": [11, 22, 33],
                "rp_2": [44, 55, 66],
            }
        )
        request_parameters_dict = request_parameters_df.to_dict(orient="records")

        event_time = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2027-01-01 12:00:00",
                        "2027-01-02 13:30:00",
                        "2027-01-03 15:45:00",
                    ]
                ),
            }
        )
        event_time_dict = event_time.to_dict(orient="records")

        request_id_df = pd.DataFrame(
            {
                "request_id": ["new_req_1", "new_req_2", "new_req_3"],
            }
        )
        request_id_dict = request_id_df.to_dict(orient="records")

        extra_logging_features_df = pd.DataFrame(
            {
                "extra_1": ["new_extra_a", "new_extra_b", "new_extra_c"],
                "extra_2": [101, 201, 301],
            }
        )
        extra_logging_features_dict = extra_logging_features_df.to_dict(
            orient="records"
        )

        logging_features_names = [
            feature.name if feature.name != "predicted_label" else "label"
            for feature in logging_features
            if feature.name != constants.FEATURE_LOGGING.REQUEST_PARAMETERS_COLUMN_NAME
        ]

        logging_test_dataframe_list = logging_test_dataframe[
            logging_features_names
        ].values.tolist()

        log_data_args = TestPython.get_logging_arguments(
            logging_data=logging_test_dataframe_list,
            untransformed_features=untransformed_features_dict,
            transformed_features=transformed_features_dict,
            predictions=predictions_dict,
            serving_keys=serving_keys_dict,
            helper_columns=inference_helpers_dict,
            request_parameters=request_parameters_dict,
            event_time=event_time_dict,
            request_id=request_id_dict,
            extra_logging_features=extra_logging_features_dict,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
        )

        # Act
        logging_list, _, _ = python_engine.get_feature_logging_list(**log_data_args)

        expected_log_data, _, _, _ = TestPython.create_expected_logging_dataframe(
            logging_data=logging_test_dataframe,
            untransformed_features=untransformed_features_df,
            transformed_features=transformed_features_df,
            predictions=predictions_df,
            serving_keys=serving_keys_df,
            helper_columns=inference_helpers_df,
            request_parameters=request_parameters_df,
            event_time=event_time,
            request_id=request_id_df,
            extra_logging_features=extra_logging_features_df,
            logging_feature_group_features=logging_feature_group_features,
            column_names=column_names,
            meta_data_logging_columns=meta_data_logging_columns,
        )
        expected_logging_list = expected_log_data.to_dict(orient="records")

        for row, expected_row in zip(logging_list, expected_logging_list):
            for key in row:
                if key not in meta_data_logging_columns_name:
                    assert row[key] == expected_row[key]

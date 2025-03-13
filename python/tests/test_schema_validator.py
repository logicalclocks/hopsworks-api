import random
import string
from unittest import mock

import pandas as pd
import polars as pl
import pytest
from hsfs import engine, feature_group
from hsfs.core.schema_validation import (
    DataFrameValidator,
    PandasValidator,
    PolarsValidator,
    PySparkValidator,
)
from hsfs.feature import Feature


@pytest.fixture
def pandas_df():
    return pd.DataFrame(
        {
            "primary_key": [1, 2, 3],
            "event_time": ["2021-01-01", "2021-01-02", "2021-01-03"],
            "string_col": [
                (
                    "".join(
                        random.choice(string.ascii_letters)
                        for _ in range(random.randint(1, 100))
                    )
                )
                for i in range(3)
            ],
        }
    )


@pytest.fixture
def polars_df():
    return pl.DataFrame(
        {
            "primary_key": [1, 2, 3],
            "event_time": ["2021-01-01", "2021-01-02", "2021-01-03"],
            "string_col": [
                (
                    "".join(
                        random.choice(string.ascii_letters)
                        for _ in range(random.randint(1, 100))
                    )
                )
                for i in range(3)
            ],
        }
    )


@pytest.fixture
def spark_df(mocker):
    from pyspark.sql import DataFrame

    # mock spark DataFrame object
    return mocker.MagicMock(spec=DataFrame)


@pytest.fixture
def feature_group_data():
    with mock.patch("hopsworks_common.client.get_instance"):
        engine.init("python")
    fg = feature_group.FeatureGroup(
        name="test_fg",
        version=1,
        online_enabled=True,
        partition_key=[],
        featurestore_id=1,
        primary_key=["primary_key"],
        features=[
            Feature("primary_key", "int", online_type="varchar(10)"),
            Feature("event_time", "string", online_type="varchar(10)"),
            Feature("string_col", "string", online_type="varchar(200)"),
        ],
    )
    return fg


@pytest.fixture
def feature_group_created():
    with mock.patch("hopsworks_common.client.get_instance"):
        engine.init("python")
    fg_existing = feature_group.FeatureGroup(
        name="test_existing_fg",
        id=1,
        version=1,
        online_enabled=True,
        partition_key=[],
        featurestore_id=1,
        primary_key=["primary_key"],
        features=[
            Feature("primary_key", "int", online_type="varchar(10)"),
            Feature("event_time", "string", online_type="varchar(10)"),
            Feature("string_col", "string", online_type="varchar(100)"),
        ],
    )
    return fg_existing


class TestSchemaValidator:
    def test_get_validator_pandas(self, pandas_df, mocker):
        validator = DataFrameValidator.get_validator(pandas_df)
        assert isinstance(validator, PandasValidator)

    def test_get_validator_polars(self, polars_df):
        validator = DataFrameValidator.get_validator(polars_df)
        assert isinstance(validator, PolarsValidator)

    def test_get_validator_spark(self, spark_df):
        validator = DataFrameValidator.get_validator(spark_df)
        assert isinstance(validator, PySparkValidator)

    def test_validate_schema_primary_key_missing(self, pandas_df, feature_group_data):
        KEY = "missing_key"
        feature_group_data.primary_key = [KEY]
        with pytest.raises(
            ValueError, match=f"Primary key column {KEY} is missing in input dataframe"
        ):
            DataFrameValidator().validate_schema(
                feature_group_data, pandas_df, feature_group_data.features
            )

    def test_validate_schema_primary_key_null(self, pandas_df, feature_group_data):
        pandas_df.loc[0, "primary_key"] = None
        with pytest.raises(
            ValueError, match="Primary key column primary_key contains null values"
        ):
            DataFrameValidator().validate_schema(
                feature_group_data, pandas_df, feature_group_data.features
            )

    def test_validate_schema_string_length_exceeded(
        self, pandas_df, feature_group_created, mocker
    ):
        pandas_df.loc[0, "string_col"] = "a" * 101
        with pytest.raises(
            ValueError, match="Column string_col has string values longer than 100"
        ):
            DataFrameValidator().validate_schema(
                feature_group_created, pandas_df, feature_group_created.features
            )

    def test_validate_schema_feature_group_created(
        self, pandas_df, feature_group_created, mocker
    ):
        pandas_df.loc[0, "string_col"] = "a" * 101
        with pytest.raises(
            ValueError, match="Column string_col has string values longer"
        ):
            DataFrameValidator().validate_schema(
                feature_group_created, pandas_df, feature_group_created.features
            )

    def test_validate_schema_feature_group_not_created(
        self, pandas_df, feature_group_data
    ):
        pandas_df.loc[0, "string_col"] = "a" * 101
        df_features = DataFrameValidator().validate_schema(
            feature_group_data, pandas_df, feature_group_data.features
        )
        assert df_features[2].online_type == "varchar(101)"

    def test_pk_null_string_length_exceeded(self, pandas_df, feature_group_data):
        pandas_df.loc[0, "primary_key"] = None
        pandas_df.loc[0, "string_col"] = "a" * 101
        with pytest.raises(
            ValueError, match="One or more schema validation errors found"
        ):
            DataFrameValidator().validate_schema(
                feature_group_data, pandas_df, feature_group_data.features
            )

    def test_offline_fg(self, pandas_df, feature_group_data, caplog):
        feature_group_data.online_enabled = False
        df_features = PandasValidator().validate_schema(
            feature_group_data, pandas_df, feature_group_data.features
        )
        assert df_features == feature_group_data.features
        assert "Feature group is not online enabled. Skipping validation" in caplog.text

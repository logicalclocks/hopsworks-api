import random
import string
from unittest import mock

import pandas as pd
import pytest
from hopsworks_common.core.constants import HAS_POLARS
from hsfs import engine, feature_group
from hsfs.core.schema_validation import (
    DataFrameValidator,
    PandasValidator,
    PolarsValidator,
    PySparkValidator,
)
from hsfs.engine import spark
from hsfs.feature import Feature


if HAS_POLARS:
    import polars as pl


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
                for _ in range(3)
            ],
        },
        dtype=pd.StringDtype(),
    )


@pytest.fixture
@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
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
                for _ in range(3)
            ],
        }
    )


@pytest.fixture
def spark_df():
    import random
    import string

    from pyspark.sql import Row

    spark_engine = spark.Engine()
    string_values = [
        "".join(
            random.choice(string.ascii_letters) for _ in range(random.randint(1, 100))
        )
        for _ in range(3)
    ]

    data = [
        Row(primary_key=1, event_time="2021-01-01", string_col=string_values[0]),
        Row(primary_key=2, event_time="2021-01-02", string_col=string_values[1]),
        Row(primary_key=3, event_time="2021-01-03", string_col=string_values[2]),
    ]

    return spark_engine._spark_session.createDataFrame(data)


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


# Base class for common test behavior
class BaseDataFrameTest:
    def test_primary_key_missing(self, df, feature_group_data):
        KEY = "missing_key"
        feature_group_data.primary_key = [KEY]
        with pytest.raises(
            ValueError, match=f"Primary key column {KEY} is missing in input dataframe"
        ):
            DataFrameValidator().validate_schema(
                feature_group_data, df, feature_group_data.features
            )

    def test_feature_group_with_features_not_created(self, df, feature_group_data):
        # test with feature group with explicit features
        df_features = DataFrameValidator().validate_schema(
            feature_group_data, df, feature_group_data.features
        )
        # assert that the online type of the string_col feature is same as explcitly set in the feature group
        assert df_features[2].online_type == "varchar(200)"

    def test_primary_key_null(self, df, feature_group_data):
        modified_df = self._modify_row(df, 0, primary_key=None)
        with pytest.raises(
            ValueError, match="Primary key column primary_key contains null values"
        ):
            DataFrameValidator().validate_schema(
                feature_group_data, modified_df, feature_group_data.features
            )

    def test_string_length_exceeded(self, df, feature_group_created, mocker):
        modified_df = self._modify_row(df, 0, string_col="a" * 101)
        with pytest.raises(ValueError, match="String length exceeded"):
            DataFrameValidator().validate_schema(
                feature_group_created, modified_df, feature_group_created.features
            )

    def test_fg_features_string_length_exceeded(self, df, feature_group_data):
        modified_df = self._modify_row(df, 0, string_col="a" * 301)
        with pytest.raises(ValueError, match="String length exceeded"):
            DataFrameValidator().validate_schema(
                feature_group_data, modified_df, feature_group_data.features
            )

    def test_feature_group_created(self, df, feature_group_created, mocker):
        modified_df = self._modify_row(df, 0, string_col="a" * 101)
        with pytest.raises(ValueError, match="String length exceeded"):
            DataFrameValidator().validate_schema(
                feature_group_created, modified_df, feature_group_created.features
            )

    def test_feature_group_not_created(self, df, feature_group_data):
        # test with non existing feature group with no explicit features
        # arrange
        modified_df = self._modify_row(df, 0, string_col="a" * 101)
        initial_features = [
            Feature("primary_key", "int"),
            Feature("event_time", "string"),
            Feature("string_col", "string"),
        ]
        feature_group_data.features = []
        df_features = DataFrameValidator().validate_schema(
            feature_group_data, modified_df, initial_features
        )
        assert df_features[2].online_type == "varchar(200)"

    def test_pk_null_string_length_exceeded(self, df, feature_group_data):
        modified_df = self._modify_row(df, 0, primary_key=None, string_col="a" * 101)
        with pytest.raises(
            ValueError, match="One or more schema validation errors found"
        ):
            DataFrameValidator().validate_schema(
                feature_group_data, modified_df, feature_group_data.features
            )

    def test_should_not_update_nonvarchar(self, df, feature_group_data):
        # Test that the validator does not update the online type of a non-varchar column
        # arrange
        # set string_col feature online type to text
        feature_group_data.features[2].online_type = "text"
        modified_df = self._modify_row(df, 0, string_col="b" * 1001)
        # act
        df_features = DataFrameValidator().validate_schema(
            feature_group_data, modified_df, feature_group_data.features
        )
        assert df_features == feature_group_data.features

    def test_offline_fg(self, df, feature_group_data, caplog):
        # test that offline fg are skipped for validations
        # arrange
        feature_group_data.online_enabled = False
        modified_df = self._modify_row(df, 0, primary_key=None, string_col="a" * 101)
        # act
        df_features = DataFrameValidator().validate_schema(
            feature_group_data, modified_df, feature_group_data.features
        )
        # assert raises warning
        assert "Feature group is not online enabled" in caplog.text
        # assert no changes were made
        assert df_features == feature_group_data.features

    def test_embedding_feature_group(self, df, feature_group_data, caplog):
        # Test that the embedding fg is skipped validation
        # arrange
        modified_df = self._modify_row(df, 0, primary_key=None, string_col="a" * 101)
        feature_group_data.embedding_index = "string_col"
        # act
        df_features = DataFrameValidator().validate_schema(
            feature_group_data, modified_df, feature_group_data.features
        )
        # assert raises warning
        assert "Feature group is embedding" in caplog.text
        assert df_features == feature_group_data.features


class TestPandasDataframe(BaseDataFrameTest):
    @pytest.fixture
    def df(self, pandas_df):
        return pandas_df

    def _modify_row(self, df, row_idx, **kwargs):
        """Helper method to modify a specific row in a Pandas DataFrame with custom index."""
        # Get the index value at the given position
        idx_value = df.index[row_idx]
        # Apply modifications for each column
        for column, value in kwargs.items():
            df.loc[idx_value, column] = value

        return df

    def test_get_validator_pandas(self, df):
        validator = DataFrameValidator.get_validator(df)
        assert isinstance(validator, PandasValidator)

    def test_string_column_detection(self):
        df = pd.DataFrame(
            {
                "pk": [1, 2, 3, 4, 5],
                "obj": [[1, 2], ["a", "b"], {"key": "value"}, None, [5, 6]],
                "string1": pd.Series(
                    ["hello", "world", "python", None, "test"], dtype="object"
                ),
                "string2": pd.Series(
                    ["dasd", "dfsadsfa", "dsadwasd", None, "tedfdsfdgst"],
                    dtype="string",
                ),
                "mixed": ["hello", 2.0, pd.Timestamp.now(), None, pd.Timestamp.now()],
            }
        )
        # act
        string_cols = PandasValidator.get_string_columns(df)
        # validate that only 'val' is detected as a string column
        assert ["string1", "string2"] == string_cols


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestPolarsDataframe(BaseDataFrameTest):
    @pytest.fixture
    def df(self, polars_df):
        return polars_df

    def _modify_row(self, df, row_idx, **kwargs):
        """Helper method to modify a specific row in a Polars DataFrame."""
        expressions = []

        for column, value in kwargs.items():
            expressions.append(
                pl.when(pl.col("index") == row_idx)
                .then(pl.lit(value))
                .otherwise(pl.col(column))
                .alias(column)
            )

        if not expressions:
            return df

        return df.with_row_index().with_columns(expressions).drop("index")

    def test_get_validator_polars(self, df):
        validator = DataFrameValidator.get_validator(df)
        assert isinstance(validator, PolarsValidator)


class TestSparkDataframe(BaseDataFrameTest):
    @pytest.fixture
    def df(self, spark_df):
        return spark_df

    def _modify_row(self, df, row_idx, **kwargs):
        """Helper method to modify a specific row in a Spark DataFrame."""
        from pyspark.sql.functions import col, lit, row_number, when
        from pyspark.sql.window import Window

        # Create a window spec for row numbering
        window_spec = Window.orderBy(
            "primary_key"
        )  # Change this to an appropriate ordering column

        # Add row numbers
        df_with_row_nr = df.withColumn("row_nr", row_number().over(window_spec))

        # Apply modifications for each column
        result_df = df_with_row_nr
        for column, value in kwargs.items():
            result_df = result_df.withColumn(
                column,
                when(col("row_nr") == row_idx + 1, lit(value)).otherwise(col(column)),
            )

        # Remove the row_nr column
        return result_df.drop("row_nr")

    def test_get_validator_spark(self, spark_df):
        validator = DataFrameValidator.get_validator(spark_df)
        assert isinstance(validator, PySparkValidator)

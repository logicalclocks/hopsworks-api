#
#   Copyright 2024 Hopsworks AB
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

import pandas as pd
import pytest
from hsml.utils.schema import column, columnar_schema
from mock import call


class TestColumnarSchema:
    # constructor

    def test_constructor_default(self, mocker):
        # Arrange
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_list_to_schema",
            return_value="convert_list_to_schema",
        )
        mock_convert_pandas_df_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_df_to_schema",
            return_value="convert_pandas_df_to_schema",
        )
        mock_convert_pandas_series_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_series_to_schema",
            return_value="convert_pandas_series_to_schema",
        )
        mock_convert_spark_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_spark_to_schema",
            return_value="convert_spark_to_schema",
        )
        mock_convert_td_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_td_to_schema",
            return_value="convert_td_to_schema",
        )
        mock_find_spec = mocker.patch("importlib.util.find_spec", return_value=None)

        # Act
        with pytest.raises(TypeError) as e_info:
            _ = columnar_schema.ColumnarSchema()

        # Assert
        assert "is not supported in a columnar schema" in str(e_info.value)
        mock_convert_list_to_schema.assert_not_called()
        mock_convert_pandas_df_to_schema.assert_not_called()
        mock_convert_pandas_series_to_schema.assert_not_called()
        mock_convert_spark_to_schema.assert_not_called()
        mock_convert_td_to_schema.assert_not_called()
        assert mock_find_spec.call_count == 1

    def test_constructor_list(self, mocker):
        # Arrange
        columnar_obj = [1, 2, 3, 4]
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_list_to_schema",
            return_value="convert_list_to_schema",
        )
        mock_convert_pandas_df_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_df_to_schema",
            return_value="convert_pandas_df_to_schema",
        )
        mock_convert_pandas_series_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_series_to_schema",
            return_value="convert_pandas_series_to_schema",
        )
        mock_convert_spark_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_spark_to_schema",
            return_value="convert_spark_to_schema",
        )
        mock_convert_td_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_td_to_schema",
            return_value="convert_td_to_schema",
        )
        mock_find_spec = mocker.patch("importlib.util.find_spec", return_value=None)

        # Act
        cs = columnar_schema.ColumnarSchema(columnar_obj)

        # Assert
        assert cs.columns == "convert_list_to_schema"
        mock_convert_list_to_schema.assert_called_once_with(columnar_obj)
        mock_convert_pandas_df_to_schema.assert_not_called()
        mock_convert_pandas_series_to_schema.assert_not_called()
        mock_convert_spark_to_schema.assert_not_called()
        mock_convert_td_to_schema.assert_not_called()
        mock_find_spec.assert_not_called()

    def test_constructor_pd_dataframe(self, mocker):
        # Arrange
        columnar_obj = pd.DataFrame([1, 2, 3, 4])
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_list_to_schema",
            return_value="convert_list_to_schema",
        )
        mock_convert_pandas_df_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_df_to_schema",
            return_value="convert_pandas_df_to_schema",
        )
        mock_convert_pandas_series_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_series_to_schema",
            return_value="convert_pandas_series_to_schema",
        )
        mock_convert_spark_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_spark_to_schema",
            return_value="convert_spark_to_schema",
        )
        mock_convert_td_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_td_to_schema",
            return_value="convert_td_to_schema",
        )
        mock_find_spec = mocker.patch("importlib.util.find_spec", return_value=None)

        # Act
        cs = columnar_schema.ColumnarSchema(columnar_obj)

        # Assert
        assert cs.columns == "convert_pandas_df_to_schema"
        mock_convert_list_to_schema.assert_not_called()
        mock_convert_pandas_df_to_schema.assert_called_once_with(columnar_obj)
        mock_convert_pandas_series_to_schema.assert_not_called()
        mock_convert_spark_to_schema.assert_not_called()
        mock_convert_td_to_schema.assert_not_called()
        mock_find_spec.assert_not_called()

    def test_constructor_pd_series(self, mocker):
        # Arrange
        columnar_obj = pd.Series([1, 2, 3, 4])
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_list_to_schema",
            return_value="convert_list_to_schema",
        )
        mock_convert_pandas_df_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_df_to_schema",
            return_value="convert_pandas_df_to_schema",
        )
        mock_convert_pandas_series_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_series_to_schema",
            return_value="convert_pandas_series_to_schema",
        )
        mock_convert_spark_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_spark_to_schema",
            return_value="convert_spark_to_schema",
        )
        mock_convert_td_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_td_to_schema",
            return_value="convert_td_to_schema",
        )
        mock_find_spec = mocker.patch("importlib.util.find_spec", return_value=None)

        # Act
        cs = columnar_schema.ColumnarSchema(columnar_obj)

        # Assert
        assert cs.columns == "convert_pandas_series_to_schema"
        mock_convert_list_to_schema.assert_not_called()
        mock_convert_pandas_df_to_schema.assert_not_called()
        mock_convert_pandas_series_to_schema.assert_called_once_with(columnar_obj)
        mock_convert_spark_to_schema.assert_not_called()
        mock_convert_td_to_schema.assert_not_called()
        mock_find_spec.assert_not_called()

    def test_constructor_pyspark_dataframe(self, mocker):
        try:
            import pyspark
        except ImportError:
            pytest.skip("pyspark not available")

        # Arrange
        columnar_obj = mocker.MagicMock(spec=pyspark.sql.dataframe.DataFrame)
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_list_to_schema",
            return_value="convert_list_to_schema",
        )
        mock_convert_pandas_df_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_df_to_schema",
            return_value="convert_pandas_df_to_schema",
        )
        mock_convert_pandas_series_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_series_to_schema",
            return_value="convert_pandas_series_to_schema",
        )
        mock_convert_spark_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_spark_to_schema",
            return_value="convert_spark_to_schema",
        )
        mock_convert_td_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_td_to_schema",
            return_value="convert_td_to_schema",
        )
        mock_find_spec = mocker.patch(
            "importlib.util.find_spec", return_value="Not None"
        )

        # Act
        cs = columnar_schema.ColumnarSchema(columnar_obj)

        # Assert
        assert cs.columns == "convert_spark_to_schema"
        mock_convert_list_to_schema.assert_not_called()
        mock_convert_pandas_df_to_schema.assert_not_called()
        mock_convert_pandas_series_to_schema.assert_not_called()
        mock_convert_spark_to_schema.assert_called_once_with(columnar_obj)
        mock_convert_td_to_schema.assert_not_called()
        mock_find_spec.assert_called_once_with("pyspark")

    def test_constructor_hsfs_td(self, mocker):
        # Arrange
        try:
            import hsfs
        except ImportError:
            pytest.skip("hsfs not available")

        # Arrange
        columnar_obj = mocker.MagicMock(spec=hsfs.training_dataset.TrainingDataset)
        mock_convert_list_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_list_to_schema",
            return_value="convert_list_to_schema",
        )
        mock_convert_pandas_df_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_df_to_schema",
            return_value="convert_pandas_df_to_schema",
        )
        mock_convert_pandas_series_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_pandas_series_to_schema",
            return_value="convert_pandas_series_to_schema",
        )
        mock_convert_spark_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_spark_to_schema",
            return_value="convert_spark_to_schema",
        )
        mock_convert_td_to_schema = mocker.patch(
            "hsml.utils.schema.columnar_schema.ColumnarSchema._convert_td_to_schema",
            return_value="convert_td_to_schema",
        )
        mock_find_spec = mocker.patch(
            "importlib.util.find_spec", return_value="Not None"
        )

        # Act
        cs = columnar_schema.ColumnarSchema(columnar_obj)

        # Assert
        assert cs.columns == "convert_td_to_schema"
        mock_convert_list_to_schema.assert_not_called()
        mock_convert_pandas_df_to_schema.assert_not_called()
        mock_convert_pandas_series_to_schema.assert_not_called()
        mock_convert_spark_to_schema.assert_not_called()
        mock_convert_td_to_schema.assert_called_once_with(columnar_obj)
        assert mock_find_spec.call_count == 1

    # convert list to schema

    def test_convert_list_to_schema(self, mocker):
        # Arrange
        columnar_obj = [1, 2, 3, 4]
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._convert_list_to_schema = (
            columnar_schema.ColumnarSchema._convert_list_to_schema
        )
        mock_columnar_schema._build_column.side_effect = columnar_obj

        # Act
        c = mock_columnar_schema._convert_list_to_schema(
            mock_columnar_schema, columnar_obj
        )

        # Assert
        expected_calls = [call(cv) for cv in columnar_obj]
        mock_columnar_schema._build_column.assert_has_calls(expected_calls)
        assert mock_columnar_schema._build_column.call_count == len(columnar_obj)
        assert c == columnar_obj

    # convert pandas df to schema

    def test_convert_pd_dataframe_to_schema(self, mocker):
        # Arrange
        columnar_obj = pd.DataFrame([[1, 2], [3, 4], [1, 2], [3, 4]])
        mock_column_init = mocker.patch(
            "hsml.utils.schema.column.Column.__init__", return_value=None
        )
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._convert_pandas_df_to_schema = (
            columnar_schema.ColumnarSchema._convert_pandas_df_to_schema
        )

        # Act
        c = mock_columnar_schema._convert_pandas_df_to_schema(
            mock_columnar_schema, columnar_obj
        )

        # Assert
        cols = columnar_obj.columns
        dtypes = columnar_obj.dtypes
        expected_calls = [call(dtypes[col], name=col) for col in cols]
        mock_column_init.assert_has_calls(expected_calls)
        assert mock_column_init.call_count == 2
        assert len(c) == 2

    # convert pandas series to schema

    def test_convert_pd_series_to_schema(self, mocker):
        # Arrange
        columnar_obj = pd.Series([1, 2, 3, 4])
        mock_column_init = mocker.patch(
            "hsml.utils.schema.column.Column.__init__", return_value=None
        )
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._convert_pandas_series_to_schema = (
            columnar_schema.ColumnarSchema._convert_pandas_series_to_schema
        )

        # Act
        c = mock_columnar_schema._convert_pandas_series_to_schema(
            mock_columnar_schema, columnar_obj
        )

        # Assert
        expected_call = call(columnar_obj.dtype, name=columnar_obj.name)
        mock_column_init.assert_has_calls([expected_call])
        assert mock_column_init.call_count == 1
        assert len(c) == 1

    # convert spark to schema

    def test_convert_spark_to_schema(self, mocker):
        # Arrange
        try:
            import pyspark
        except ImportError:
            pytest.skip("pyspark not available")

        # Arrange
        columnar_obj = mocker.MagicMock(spec=pyspark.sql.dataframe.DataFrame)
        columnar_obj.dtypes = [("name_1", "type_1"), ("name_2", "type_2")]
        mock_column_init = mocker.patch(
            "hsml.utils.schema.column.Column.__init__", return_value=None
        )
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._convert_spark_to_schema = (
            columnar_schema.ColumnarSchema._convert_spark_to_schema
        )

        # Act
        c = mock_columnar_schema._convert_spark_to_schema(
            mock_columnar_schema, columnar_obj
        )

        # Assert
        expected_calls = [call(dtype, name=name) for name, dtype in columnar_obj.dtypes]
        mock_column_init.assert_has_calls(expected_calls)
        assert mock_column_init.call_count == len(columnar_obj.dtypes)
        assert len(c) == len(columnar_obj.dtypes)

    # convert td to schema

    def test_convert_td_to_schema(self, mocker):
        # Arrange
        class MockFeature:
            def __init__(self, fname, ftype):
                self.name = fname
                self.type = ftype

        columnar_obj = mocker.MagicMock()
        columnar_obj.schema = [
            MockFeature("name_1", "type_1"),
            MockFeature("name_2", "type_2"),
        ]
        mock_column_init = mocker.patch(
            "hsml.utils.schema.column.Column.__init__", return_value=None
        )
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._convert_td_to_schema = (
            columnar_schema.ColumnarSchema._convert_td_to_schema
        )

        # Act
        c = mock_columnar_schema._convert_td_to_schema(
            mock_columnar_schema, columnar_obj
        )

        # Assert
        expected_calls = [
            call(feat.type, name=feat.name) for feat in columnar_obj.schema
        ]
        mock_column_init.assert_has_calls(expected_calls)
        assert mock_column_init.call_count == len(columnar_obj.schema)
        assert len(c) == len(columnar_obj.schema)

    # build column

    def test_build_column_type_only(self, mocker):
        # Arrange
        columnar_obj = {"type": "tensor_type"}
        mock_column_init = mocker.patch(
            "hsml.utils.schema.column.Column.__init__", return_value=None
        )
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._build_column = (
            columnar_schema.ColumnarSchema._build_column
        )

        # Act
        c = mock_columnar_schema._build_column(mock_columnar_schema, columnar_obj)

        # Assert
        assert isinstance(c, column.Column)
        mock_column_init.assert_called_once_with(
            columnar_obj["type"], name=None, description=None
        )

    def test_build_tensor_invalid_missing_type(self, mocker):
        # Arrange
        columnar_obj = {}
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._build_column = (
            columnar_schema.ColumnarSchema._build_column
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            _ = mock_columnar_schema._build_column(mock_columnar_schema, columnar_obj)

        # Assert
        assert "Mandatory 'type' key missing from entry" in str(e_info.value)

    def test_build_tensor_type_name_and_description(self, mocker):
        # Arrange
        columnar_obj = {
            "type": "tensor_type",
            "name": "tensor_name",
            "description": "tensor_description",
        }
        mock_column_init = mocker.patch(
            "hsml.utils.schema.column.Column.__init__", return_value=None
        )
        mock_columnar_schema = mocker.MagicMock()
        mock_columnar_schema._build_column = (
            columnar_schema.ColumnarSchema._build_column
        )

        # Act
        c = mock_columnar_schema._build_column(mock_columnar_schema, columnar_obj)

        # Assert
        assert isinstance(c, column.Column)
        mock_column_init.assert_called_once_with(
            columnar_obj["type"],
            name=columnar_obj["name"],
            description=columnar_obj["description"],
        )

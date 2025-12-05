#
#   Copyright 2022 Logical Clocks AB
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

import contextlib
import importlib

import pandas
from hsml.utils.schema.column import Column


with contextlib.suppress(ImportError):
    import pyspark


class ColumnarSchema:
    """Metadata object representing a columnar schema for a model."""

    def __init__(self, columnar_obj=None):
        from hsfs.training_dataset import (
            TrainingDataset,  # import performed here to prevent circular dependencies when importing ModelSchema
        )

        if isinstance(columnar_obj, list):
            self.columns = self._convert_list_to_schema(columnar_obj)
        elif isinstance(columnar_obj, pandas.DataFrame):
            self.columns = self._convert_pandas_df_to_schema(columnar_obj)
        elif isinstance(columnar_obj, pandas.Series):
            self.columns = self._convert_pandas_series_to_schema(columnar_obj)
        elif importlib.util.find_spec("pyspark") is not None and isinstance(
            columnar_obj, pyspark.sql.dataframe.DataFrame
        ):
            self.columns = self._convert_spark_to_schema(columnar_obj)
        elif isinstance(columnar_obj, TrainingDataset):
            self.columns = self._convert_td_to_schema(columnar_obj)
        else:
            raise TypeError(
                f"{type(columnar_obj)} is not supported in a columnar schema."
            )

    def _convert_list_to_schema(self, columnar_obj):
        columns = []
        for column in columnar_obj:
            columns.append(self._build_column(column))
        return columns

    def _convert_pandas_df_to_schema(self, pandas_df):
        pandas_columns = pandas_df.columns
        pandas_data_types = pandas_df.dtypes
        columns = []
        for name in pandas_columns:
            columns.append(Column(pandas_data_types[name], name=name))
        return columns

    def _convert_pandas_series_to_schema(self, pandas_series):
        columns = []
        columns.append(Column(pandas_series.dtype, name=pandas_series.name))
        return columns

    def _convert_spark_to_schema(self, spark_df):
        columns = []
        types = spark_df.dtypes
        for dtype in types:
            name, dtype = dtype
            columns.append(Column(dtype, name=name))
        return columns

    def _convert_td_to_schema(self, td):
        columns = []
        features = td.schema
        for feature in features:
            columns.append(Column(feature.type, name=feature.name))
        return columns

    def _build_column(self, columnar_obj):
        type = None
        name = None
        description = None

        if "description" in columnar_obj:
            description = columnar_obj["description"]

        if "name" in columnar_obj:
            name = columnar_obj["name"]

        if "type" in columnar_obj:
            type = columnar_obj["type"]
        else:
            raise ValueError(f"Mandatory 'type' key missing from entry {columnar_obj}")

        return Column(type, name=name, description=description)

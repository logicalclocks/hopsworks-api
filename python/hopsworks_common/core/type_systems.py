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
#

from __future__ import annotations

import ast
import datetime
import decimal
from typing import TYPE_CHECKING, Literal, NewType

import pytz
from hopsworks_common.core.constants import (
    HAS_PANDAS,
    HAS_POLARS,
    HAS_PYARROW,
)
from hopsworks_common.decorators import uses_polars
from hsfs.client.exceptions import FeatureStoreException


if TYPE_CHECKING:
    import numpy as np
    import pandas as pd
    import polars as pl
    from hsfs.core.feature_logging import LoggingMetaData

if HAS_PYARROW:
    import pyarrow as pa

    # Decimal types are currently not supported
    _INT_TYPES = [pa.uint8(), pa.uint16(), pa.int8(), pa.int16(), pa.int32()]
    _BIG_INT_TYPES = [pa.uint32(), pa.int64()]
    _FLOAT_TYPES = [pa.float16(), pa.float32()]
    _DOUBLE_TYPES = [pa.float64()]
    _TIMESTAMP_UNIT = ["ns", "us", "ms", "s"]
    _BOOLEAN_TYPES = [pa.bool_()]
    _STRING_TYPES = [pa.string(), pa.large_string()]
    _DATE_TYPES = [pa.date32(), pa.date64()]
    _BINARY_TYPES = [pa.binary(), pa.large_binary()]

    PYARROW_HOPSWORKS_DTYPE_MAPPING = {
        **dict.fromkeys(_INT_TYPES, "int"),
        **dict.fromkeys(_BIG_INT_TYPES, "bigint"),
        **dict.fromkeys(_FLOAT_TYPES, "float"),
        **dict.fromkeys(_DOUBLE_TYPES, "double"),
        **dict.fromkeys(
            [
                *[pa.timestamp(unit) for unit in _TIMESTAMP_UNIT],
                *[
                    pa.timestamp(unit, tz=tz)
                    for unit in _TIMESTAMP_UNIT
                    for tz in pytz.all_timezones
                ],
            ],
            "timestamp",
        ),
        **dict.fromkeys(_BOOLEAN_TYPES, "boolean"),
        **dict.fromkeys(
            [
                *_STRING_TYPES,
                # Category type in pandas stored as dictinoary in pyarrow
                *[
                    pa.dictionary(
                        value_type=value_type, index_type=index_type, ordered=ordered
                    )
                    for value_type in _STRING_TYPES
                    for index_type in _INT_TYPES + _BIG_INT_TYPES
                    for ordered in [True, False]
                ],
            ],
            "string",
        ),
        **dict.fromkeys(_DATE_TYPES, "date"),
        **dict.fromkeys(_BINARY_TYPES, "binary"),
    }
else:
    PYARROW_HOPSWORKS_DTYPE_MAPPING = {}

if HAS_PYARROW:

    def convert_offline_type_to_pyarrow_type(offline_type: str) -> pa.DataType:
        """Convert an offline type string to a PyArrow type.

        Supports simple types (int, bigint, string, etc.), array types (array<type>),
        and struct types (struct<field1:type1,field2:type2>).

        Parameters:
            offline_type: The offline type string to convert.

        Returns:
            The corresponding PyArrow type.
        """
        offline_type = offline_type.strip().lower()

        # Handle array types: array<type>
        if offline_type.startswith("array<") and offline_type.endswith(">"):
            element_type_str = offline_type[
                6:-1
            ]  # Extract content between array< and >
            element_type = convert_offline_type_to_pyarrow_type(element_type_str)
            return pa.list_(element_type)

        # Handle struct types: struct<field1:type1,field2:type2>
        if offline_type.startswith("struct<") and offline_type.endswith(">"):
            struct_content = offline_type[7:-1]  # Extract content between struct< and >
            fields = []
            # Parse struct fields: field1:type1,field2:type2
            # Need to handle nested structs and arrays in field types
            i = 0
            while i < len(struct_content):
                # Find the field name (until colon)
                field_start = i
                while i < len(struct_content) and struct_content[i] != ":":
                    i += 1
                if i >= len(struct_content):
                    raise FeatureStoreException(
                        f"Invalid struct type format: {offline_type}. Missing colon after field name."
                    )
                field_name = struct_content[field_start:i].strip()
                i += 1  # Skip colon

                # Find the field type (handle nested structs/arrays)
                type_start = i
                bracket_count = 0
                angle_bracket_count = 0
                while i < len(struct_content):
                    char = struct_content[i]
                    if char == "," and bracket_count == 0 and angle_bracket_count == 0:
                        break
                    if char == "<":
                        angle_bracket_count += 1
                    elif char == ">":
                        angle_bracket_count -= 1
                    elif char == "(":
                        bracket_count += 1
                    elif char == ")":
                        bracket_count -= 1
                    i += 1

                field_type_str = struct_content[type_start:i].strip()
                field_type = convert_offline_type_to_pyarrow_type(field_type_str)
                fields.append(pa.field(field_name, field_type, nullable=True))
                if i < len(struct_content):
                    i += 1  # Skip comma

            return pa.struct(fields)

        # Handle simple types
        offline_type_lower = offline_type.lower()
        type_mapping = {
            "string": pa.string(),
            "bigint": pa.int64(),
            "int": pa.int32(),
            "smallint": pa.int16(),
            "tinyint": pa.int8(),
            "float": pa.float32(),
            "double": pa.float64(),
            "boolean": pa.bool_(),
            "timestamp": pa.timestamp("us"),
            "date": pa.date32(),
            "binary": pa.binary(),
        }

        # Handle decimal types: decimal(precision,scale) or just decimal
        if offline_type_lower.startswith("decimal"):
            # Try to parse decimal(precision,scale)
            if "(" in offline_type_lower:
                # Extract precision and scale
                start = offline_type_lower.index("(")
                end = offline_type_lower.index(")")
                params = offline_type_lower[start + 1 : end].split(",")
                if len(params) == 2:
                    precision = int(params[0].strip())
                    scale = int(params[1].strip())
                    return pa.decimal128(precision, scale)
            # Default decimal type
            return pa.decimal128(10, 0)

        if offline_type_lower in type_mapping:
            return type_mapping[offline_type_lower]

        raise FeatureStoreException(
            f"Unsupported offline type: {offline_type}. Cannot convert to PyArrow type."
        )
else:

    def convert_offline_type_to_pyarrow_type(offline_type: str):
        raise FeatureStoreException(
            "PyArrow is not installed. Cannot convert offline type to PyArrow type."
        )


# python cast column to offline type
if HAS_POLARS:
    import polars as pl

    polars_offline_dtype_mapping = {
        "bigint": pl.Int64,
        "int": pl.Int32,
        "smallint": pl.Int16,
        "tinyint": pl.Int8,
        "float": pl.Float32,
        "double": pl.Float64,
    }

    polars_online_dtype_mapping = {
        "bigint": pl.Int64,
        "int": pl.Int32,
        "smallint": pl.Int16,
        "tinyint": pl.Int8,
        "float": pl.Float32,
        "double": pl.Float64,
    }

if HAS_PANDAS:
    import numpy as np
    import pandas as pd

    pandas_offline_dtype_mapping = {
        "bigint": pd.Int64Dtype(),
        "int": pd.Int32Dtype(),
        "smallint": pd.Int16Dtype(),
        "tinyint": pd.Int8Dtype(),
        "float": pd.Float32Dtype(),
        "double": pd.Float64Dtype(),
    }

    pandas_online_dtype_mapping = {
        "bigint": pd.Int64Dtype(),
        "int": pd.Int32Dtype(),
        "smallint": pd.Int16Dtype(),
        "tinyint": pd.Int8Dtype(),
        "float": pd.Float32Dtype(),
        "double": pd.Float64Dtype(),
    }


def create_extended_type(base_type: type) -> HopsworksLoggingMetadataType:
    """This is wrapper function to create a new class that extends the base_type class with a new attribute that can be used to store metadata.

    Parameters:
        base_type : The base class to extend
    """

    class HopsworksLoggingMetadataType(base_type):
        """This is a class that extends the base_type class with a new attribute `hopsworks_logging_metadata` that can be used to store metadata."""

        _is_extended_type = True

        @property
        def hopsworks_logging_metadata(self) -> LoggingMetaData | None:
            if not hasattr(self, "_hopsworks_logging_metadata"):
                return None
            return self._hopsworks_logging_metadata

        @hopsworks_logging_metadata.setter
        def hopsworks_logging_metadata(self, meta_data: LoggingMetaData):
            self._hopsworks_logging_metadata = meta_data

    return HopsworksLoggingMetadataType


# TODO: Rework whatever is going on here
HopsworksLoggingMetadataType = NewType(
    "HopsworksLoggingMetadataType", create_extended_type(type)
)  # Adding new type for type hinting and static analysis.


def convert_pandas_dtype_to_offline_type(arrow_type: str) -> str:
    # This is a simple type conversion between pandas dtypes and pyspark (hive) types,
    # using pyarrow types obatined from pandas dataframe to convert pandas typed fields,
    # A recurisive function  "convert_pandas_object_type_to_offline_type" is used to convert complex types like lists and structures
    # "_onvert_simple_pandas_dtype_to_offline_type" is used to convert simple types
    # In the backend, the types specified here will also be used for mapping to Avro types.
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_struct(arrow_type)
    ):
        return convert_pandas_object_type_to_offline_type(arrow_type)

    return convert_simple_pandas_dtype_to_offline_type(arrow_type)


def convert_pandas_object_type_to_offline_type(arrow_type: str) -> str:
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        # figure out sub type
        sub_arrow_type = arrow_type.value_type
        subtype = convert_pandas_dtype_to_offline_type(sub_arrow_type)
        return f"array<{subtype}>"
    if pa.types.is_struct(arrow_type):
        struct_schema = {}
        for index in range(arrow_type.num_fields):
            struct_schema[arrow_type.field(index).name] = (
                convert_pandas_dtype_to_offline_type(arrow_type.field(index).type)
            )
        return (
            "struct<"
            + ",".join([f"{key}:{value}" for key, value in struct_schema.items()])
            + ">"
        )

    raise ValueError(f"dtype 'O' (arrow_type '{str(arrow_type)}') not supported")


def cast_pandas_column_to_offline_type(
    feature_column: pd.Series, offline_type: str
) -> pd.Series:
    offline_type = offline_type.lower()
    if offline_type == "timestamp":
        return pd.to_datetime(feature_column, utc=True).dt.tz_localize(None)
    if offline_type == "date":
        return pd.to_datetime(feature_column, utc=True).dt.date
    if offline_type.startswith(("array<", "struct<")) or offline_type == "boolean":
        return feature_column.apply(
            lambda x: (ast.literal_eval(x) if isinstance(x, str) else x)
            if (
                x is not None
                and (
                    isinstance(x, (list, dict, np.ndarray))
                    or (not pd.isnull(x) and x != "")
                )
            )
            else None
        )
    if offline_type == "string":
        return feature_column.apply(lambda x: str(x) if x is not None else None)
    if offline_type.startswith("decimal"):
        return feature_column.apply(
            lambda x: decimal.Decimal(x) if (x is not None) else None
        )
    if offline_type in pandas_offline_dtype_mapping:
        return feature_column.astype(pandas_offline_dtype_mapping[offline_type])
    return feature_column  # handle gracefully, just return the column as-is


@uses_polars
def cast_polars_column_to_offline_type(
    feature_column: pl.Series, offline_type: str
) -> pl.Series:
    offline_type = offline_type.lower()
    if offline_type == "timestamp":
        # convert (if tz!=UTC) to utc, then make timezone unaware
        return feature_column.cast(pl.Datetime(time_zone=None))
    if offline_type == "date":
        return feature_column.cast(pl.Date)
    if offline_type.startswith(("array<", "struct<")) or offline_type == "boolean":
        return feature_column.map_elements(
            lambda x: (ast.literal_eval(x) if isinstance(x, str) else x)
            if (x is not None and x != "")
            else None
        )
    if offline_type == "string":
        return feature_column.map_elements(lambda x: str(x) if x is not None else None)
    if offline_type.startswith("decimal"):
        return feature_column.map_elements(
            lambda x: decimal.Decimal(x) if (x is not None) else None
        )
    if offline_type in polars_offline_dtype_mapping:
        return feature_column.cast(polars_offline_dtype_mapping[offline_type])
    return feature_column  # handle gracefully, just return the column as-is


def cast_column_to_offline_type(
    feature_column: pd.Series | pl.Series, offline_type: str
) -> pd.Series:
    if isinstance(feature_column, pd.Series):
        return cast_pandas_column_to_offline_type(feature_column, offline_type.lower())
    if HAS_POLARS and isinstance(feature_column, pl.Series):
        return cast_polars_column_to_offline_type(feature_column, offline_type.lower())
    return None


def cast_column_to_online_type(
    feature_column: pd.Series, online_type: str
) -> pd.Series:
    online_type = online_type.lower()
    if online_type == "timestamp":
        # convert (if tz!=UTC) to utc, then make timezone unaware
        return pd.to_datetime(feature_column, utc=True).dt.tz_localize(None)
    if online_type == "date":
        return pd.to_datetime(feature_column, utc=True).dt.date
    if online_type.startswith("varchar") or online_type == "text":
        return feature_column.apply(lambda x: str(x) if x is not None else None)
    if online_type == "boolean":
        return feature_column.apply(
            lambda x: (ast.literal_eval(x) if isinstance(x, str) else x)
            if (x is not None and x != "")
            else None
        )
    if online_type.startswith("decimal"):
        return feature_column.apply(
            lambda x: decimal.Decimal(x) if (x is not None) else None
        )
    if online_type in pandas_online_dtype_mapping:
        return feature_column.astype(pandas_online_dtype_mapping[online_type])
    return feature_column  # handle gracefully, just return the column as-is


def convert_simple_pandas_dtype_to_offline_type(arrow_type: str) -> str:
    try:
        return PYARROW_HOPSWORKS_DTYPE_MAPPING[arrow_type]
    except KeyError as err:
        raise ValueError(f"dtype '{arrow_type}' not supported") from err


def translate_legacy_spark_type(
    output_type: str,
) -> Literal[
    "STRING",
    "BINARY",
    "BYTE",
    "SHORT",
    "INT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "TIMESTAMP",
    "DATE",
    "BOOLEAN",
]:
    if output_type == "StringType()":
        return "STRING"
    if output_type == "BinaryType()":
        return "BINARY"
    if output_type == "ByteType()":
        return "BYTE"
    if output_type == "ShortType()":
        return "SHORT"
    if output_type == "IntegerType()":
        return "INT"
    if output_type == "LongType()":
        return "LONG"
    if output_type == "FloatType()":
        return "FLOAT"
    if output_type == "DoubleType()":
        return "DOUBLE"
    if output_type == "TimestampType()":
        return "TIMESTAMP"
    if output_type == "DateType()":
        return "DATE"
    if output_type == "BooleanType()":
        return "BOOLEAN"
    return "STRING"  # handle gracefully, and return STRING type, the default for spark udfs


def convert_spark_type_to_offline_type(spark_type_string: str) -> str:
    if spark_type_string.endswith("Type()"):
        spark_type_string = translate_legacy_spark_type(spark_type_string)
    if spark_type_string == "STRING":
        return "STRING"
    if spark_type_string == "BINARY":
        return "BINARY"
    if (
        spark_type_string == "BYTE"
        or spark_type_string == "SHORT"
        or spark_type_string == "INT"
    ):
        return "INT"
    if spark_type_string == "LONG":
        return "BIGINT"
    if spark_type_string == "FLOAT":
        return "FLOAT"
    if spark_type_string == "DOUBLE":
        return "DOUBLE"
    if spark_type_string == "TIMESTAMP":
        return "TIMESTAMP"
    if spark_type_string == "DATE":
        return "DATE"
    if spark_type_string == "BOOLEAN":
        return "BOOLEAN"
    raise ValueError(
        f"Return type {spark_type_string} not supported for transformation functions."
    )


def infer_spark_type(output_type):
    if not output_type:
        return "STRING"  # STRING is default type for spark udfs

    if isinstance(output_type, str):
        if output_type.endswith("Type()"):
            return translate_legacy_spark_type(output_type)
        output_type = output_type.lower()

    if output_type in (str, "str", "string"):
        return "STRING"
    if output_type in (bytes, "binary"):
        return "BINARY"
    if output_type in (np.int8, "int8", "byte", "tinyint"):
        return "BYTE"
    if output_type in (np.int16, "int16", "short", "smallint"):
        return "SHORT"
    if output_type in (int, "int", "integer", np.int32):
        return "INT"
    if output_type in (np.int64, "int64", "long", "bigint"):
        return "LONG"
    if output_type in (float, "float"):
        return "FLOAT"
    if output_type in (np.float64, "float64", "double"):
        return "DOUBLE"
    if output_type in (
        datetime.datetime,
        np.datetime64,
        "datetime",
        "timestamp",
    ):
        return "TIMESTAMP"
    if output_type in (datetime.date, "date"):
        return "DATE"
    if output_type in (bool, "boolean", "bool"):
        return "BOOLEAN"
    raise TypeError(f"Not supported type {output_type}.")

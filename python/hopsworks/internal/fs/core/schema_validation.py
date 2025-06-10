import logging
import re

import pandas as pd
from hopsworks_common.core.constants import HAS_POLARS


logger = logging.getLogger(__name__)


class DataFrameValidator:
    # Base validator class

    @staticmethod
    def get_validator(df):
        """method to get the appropriate implementation of validator for the DataFrame type"""
        if isinstance(df, pd.DataFrame):
            return PandasValidator()

        if HAS_POLARS:
            import polars as pl

            if isinstance(df, pl.DataFrame):
                return PolarsValidator()

        try:
            from pyspark.sql import DataFrame as SparkDataFrame

            if isinstance(df, SparkDataFrame):
                return PySparkValidator()
        except ImportError:
            pass

        return None

    def _format_error_dict(self, errors):
        """Format error dictionary as a readable table string."""
        if not errors:
            return "{}"

        # Find maximum column name length for alignment
        col_width = max(len(col) for col in errors.keys()) + 2

        # Create table headers and separator
        result = ["", "Error details:"]
        result.append(f"{'Column':<{col_width}} | Error Description")
        result.append(f"{'-' * col_width}-+-{'-' * 50}")

        # Add each error row
        for col, desc in errors.items():
            result.append(f"{col:<{col_width}} | {desc}")

        return "\n".join(result)

    def _raise_validation_error(self, errors):
        """Raise a formatted validation error."""
        base_message = "One or more schema validation errors found for online ingestion. For details about the validation, refer to the documentation(https://docs.hopsworks.ai/latest/user_guides/fs/feature_group/data_types/#string-online-data-types)."
        formatted_errors = self._format_error_dict(errors)
        raise ValueError(f"{base_message}{formatted_errors}")

    def validate_schema(self, feature_group, df, df_features):
        """Common validation rules"""
        if feature_group.online_enabled is False:
            logger.warning("Feature group is not online enabled. Skipping validation")
            return df_features
        if feature_group._embedding_index is not None:
            logger.warning("Feature group is embedding type. Skipping validation")
            return df_features

        validator = self.get_validator(df)
        if validator is None:
            # If no validator is found for this type, skip validation and return df_features
            return df_features
        # If this is the base class and not a subclass instance being called directly,
        # delegate to the appropriate subclass
        if self.__class__ == DataFrameValidator:
            return validator.validate_schema(feature_group, df, df_features)
        errors = {}
        # Check if the primary key columns exist
        for pk in feature_group.primary_key:
            if pk not in df.columns:
                raise ValueError(
                    f"Primary key column {pk} is missing in input dataframe"
                )
        # Execute data type specific validation
        errors, column_lengths, is_pk_null, is_string_length_exceeded = (
            self._validate_df_specifics(feature_group, df)
        )

        # Handle errors
        if is_pk_null or (
            is_string_length_exceeded and (feature_group.id or feature_group.features)
        ):
            self._raise_validation_error(errors)
        elif is_string_length_exceeded:
            # If the feature group is not created and string lengths exceed default, adjust the string columns
            df_features = self.increase_string_columns(column_lengths, df_features)

        return df_features

    def _validate_df_specifics(self, feature_group, df):
        """To be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement this method")

    @staticmethod
    def get_feature_from_list(feature_name, features):
        for i_feature in features:
            if i_feature.name == feature_name:
                return i_feature

        return None

    @staticmethod
    def extract_numbers(input_string):
        # Define regular expression pattern for matching numbers
        pattern = r"\d+"
        # Use re.findall() to find all occurrences of the pattern in the input string
        return re.findall(pattern, input_string)

    def get_online_varchar_length(self, feature):
        # check of online_type is not null and starts with varchar
        if (
            feature
            and feature.online_type
            and feature.online_type.startswith("varchar")
        ):
            return int(self.extract_numbers(feature.online_type)[0])
        return None

    @staticmethod
    def increase_string_columns(column_lengths: dict, dataframe_features):
        def round_up_to_hundred(num):
            import math

            return math.ceil(num / 100) * 100

        # Adjusts the length of varchar columns
        for i_feature in dataframe_features:
            if i_feature.name in column_lengths:
                char_limit = round_up_to_hundred(column_lengths[i_feature.name])
                i_feature.online_type = f"varchar({char_limit})"
                logger.warning(
                    f"Maximum string length for column {i_feature.name} increased to {char_limit} in online table."
                )
        return dataframe_features


class PandasValidator(DataFrameValidator):
    @staticmethod
    def get_string_columns(df):
        if pd.__version__.startswith("2."):
            # For pandas 2+, use is_string_dtype api
            return [
                col
                for col in df.columns
                if pd.api.types.is_string_dtype(df[col].dropna())
            ]
        else:
            # For pandas 1.x,  is_string_dtype api is not compatible, so check each row if its a string
            string_cols = []
            for col in df.select_dtypes(include=["object", "string"]).columns:
                # Skip empty columns
                if df[col].count() == 0:
                    continue
                # Check if ALL non-null values are strings
                if df[col].dropna().map(lambda x: isinstance(x, str)).all():
                    string_cols.append(col)
            return string_cols

    # Pandas df specific validator
    def _validate_df_specifics(self, feature_group, df):
        errors = {}
        column_lengths = {}
        is_pk_null = False
        is_string_length_exceeded = False

        # Check for null values in primary key columns
        for pk in feature_group.primary_key:
            if df[pk].isnull().any():
                errors[pk] = f"Primary key column {pk} contains null values."
                is_pk_null = True

        # Check string lengths
        string_columns = self.get_string_columns(df)
        for col in string_columns:
            currentmax = df[col].str.len().max()
            col_max_len = (
                self.get_online_varchar_length(
                    self.get_feature_from_list(col, feature_group.features)
                )
                if feature_group.features
                else 100
            )

            if col_max_len is not None and currentmax > col_max_len:
                errors[col] = (
                    f"String length exceeded. Column {col} has string values longer than maximum column limit of {col_max_len} characters."
                )
                column_lengths[col] = currentmax
                is_string_length_exceeded = True

        return errors, column_lengths, is_pk_null, is_string_length_exceeded


class PolarsValidator(DataFrameValidator):
    # Polars df specific validator
    def _validate_df_specifics(self, feature_group, df):
        import polars as pl

        errors = {}
        column_lengths = {}
        is_pk_null = False
        is_string_length_exceeded = False

        # Check for null values in primary key columns
        for pk in feature_group.primary_key:
            if df[pk].is_null().any():
                errors[pk] = f"Primary key column {pk} contains null values."
                is_pk_null = True

        # Check string lengths
        for col in df.select(pl.col(pl.String)).columns:
            currentmax = df[col].str.len_chars().max()
            col_max_len = (
                self.get_online_varchar_length(
                    self.get_feature_from_list(col, feature_group.features)
                )
                if feature_group.features
                else 100
            )

            if col_max_len is not None and currentmax > col_max_len:
                errors[col] = (
                    f"String length exceeded. Column {col} has string values longer than maximum column limit of {col_max_len} characters."
                )
                column_lengths[col] = currentmax
                is_string_length_exceeded = True

        return errors, column_lengths, is_pk_null, is_string_length_exceeded


class PySparkValidator(DataFrameValidator):
    # PySpark-specific validator
    def _validate_df_specifics(self, feature_group, df):
        # Import PySpark SQL functions and types
        import pyspark.sql.functions as sf
        from pyspark.sql.types import StringType

        errors = {}
        column_lengths = {}
        is_pk_null = False
        is_string_length_exceeded = False

        # Check for null values in primary key columns
        for pk in feature_group.primary_key:
            if df.filter(df[pk].isNull()).count() > 0:
                errors[pk] = f"Primary key column {pk} contains null values."
                is_pk_null = True

        # Check string lengths for string columns
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                col = field.name
                # Compute max length - PySpark specific way
                currentmax_row = df.select(sf.max(sf.length(col))).collect()[0][0]
                currentmax = 0 if currentmax_row is None else currentmax_row

                col_max_len = (
                    self.get_online_varchar_length(
                        self.get_feature_from_list(col, feature_group.features)
                    )
                    if feature_group.features
                    else 100
                )

                if col_max_len is not None and currentmax > col_max_len:
                    errors[col] = (
                        f"String length exceeded. Column {col} has string values longer than maximum column limit of {col_max_len} characters."
                    )
                    column_lengths[col] = currentmax
                    is_string_length_exceeded = True

        return errors, column_lengths, is_pk_null, is_string_length_exceeded

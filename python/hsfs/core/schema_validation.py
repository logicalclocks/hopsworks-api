import logging
import re


class DataFrameValidator:
    # Base validator class

    @staticmethod
    def get_validator(df):
        """method to get the appropriate implementation of validator for the DataFrame type"""
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            return PandasValidator()

        try:
            import polars as pl

            if isinstance(df, pl.DataFrame):
                return PolarsValidator()
        except ImportError:
            pass

        try:
            from pyspark.sql import DataFrame as SparkDataFrame

            if isinstance(df, SparkDataFrame):
                return PySparkValidator()
        except ImportError:
            pass

        return None

    def validate_schema(self, feature_group, df, df_features):
        """Common validation rules"""
        if feature_group.online_enabled is False:
            logging.warning("Feature group is not online enabled. Skipping validation")
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
            self._validate_df_specifics(feature_group, df, bool(feature_group.id))
        )

        # Handle errors
        if is_pk_null or (is_string_length_exceeded and feature_group.id):
            raise ValueError(
                f"One or more schema validation errors found for online ingestion. For details about the validation, refer to the documentation(https://docs.hopsworks.ai/latest/user_guides/fs/feature_group/data_types/#string-online-data-types). Error details ('column_name':'error description'): {errors}"
            )
        elif is_string_length_exceeded:
            # If the feature group is not created and string lengths exceed default, adjust the string columns
            df_features = self.increase_string_columns(column_lengths, df_features)

        return df_features

    def _validate_df_specifics(self, feature_group, df, is_fg_created):
        """To be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement this method")

    @staticmethod
    def get_feature_from_list(feature_name, features):
        for i_feature in features:
            if i_feature.name == feature_name:
                return i_feature
        raise ValueError(f"Feature {feature_name} not found in feature list")

    @staticmethod
    def extract_numbers(input_string):
        # Define regular expression pattern for matching numbers
        pattern = r"\d+"
        # Use re.findall() to find all occurrences of the pattern in the input string
        return re.findall(pattern, input_string)

    def get_online_varchar_length(self, feature):
        # returns the column length of varchar columns
        if not feature.type == "string":
            raise ValueError("Feature not a string type")
        if not feature.online_type:
            raise ValueError("Feature is not online enabled")

        return int(self.extract_numbers(feature.online_type)[0])

    @staticmethod
    def increase_string_columns(column_lengths: dict, dataframe_features):
        # Adjusts the length of varchar columns
        for i_feature in dataframe_features:
            if i_feature.name in column_lengths:
                i_feature.online_type = f"varchar({column_lengths[i_feature.name]})"
                logging.warning(
                    f"Column {i_feature.name} maximum string length increased to {column_lengths[i_feature.name]}"
                )
        return dataframe_features


class PandasValidator(DataFrameValidator):
    # Pandas df specific validator
    def _validate_df_specifics(self, feature_group, df, is_fg_created):
        errors = {}
        column_lengths = {}
        is_pk_null = False
        is_string_length_exceeded = False

        # Check for null values in primary key columns
        for pk in feature_group.primary_key:
            if df[pk].isnull().any():
                errors[pk] = f"Primary key column {pk} contains null values"
                is_pk_null = True

        # Check string lengths
        for col in df.select_dtypes(include=["object"]).columns:
            currentmax = df[col].str.len().max()
            col_max_len = (
                self.get_online_varchar_length(
                    self.get_feature_from_list(col, feature_group.features)
                )
                if is_fg_created
                else 100
            )

            if currentmax > col_max_len:
                errors[col] = (
                    f"Column {col} has string values longer than {col_max_len} characters"
                )
                column_lengths[col] = currentmax
                is_string_length_exceeded = True

        return errors, column_lengths, is_pk_null, is_string_length_exceeded


class PolarsValidator(DataFrameValidator):
    # Polars df specific validator
    def _validate_df_specifics(self, feature_group, df, is_fg_created):
        import polars as pl

        errors = {}
        column_lengths = {}
        is_pk_null = False
        is_string_length_exceeded = False

        # Check for null values in primary key columns
        for pk in feature_group.primary_key:
            if df[pk].is_null().any():
                errors[pk] = f"Primary key column {pk} contains null values"
                is_pk_null = True

        # Check string lengths
        for col in df.select(pl.col(pl.Utf8)).columns:
            currentmax = df[col].str.len_chars().max()
            col_max_len = (
                self.get_online_varchar_length(
                    self.get_feature_from_list(col, feature_group.features)
                )
                if is_fg_created
                else 100
            )

            if currentmax > col_max_len:
                errors[col] = (
                    f"Column {col} has string values longer than {col_max_len} characters"
                )
                column_lengths[col] = currentmax
                is_string_length_exceeded = True

        return errors, column_lengths, is_pk_null, is_string_length_exceeded


class PySparkValidator(DataFrameValidator):
    # PySpark-specific validator
    def _validate_df_specifics(self, feature_group, df, is_fg_created):
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
                errors[pk] = f"Primary key column {pk} contains null values"
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
                    if is_fg_created
                    else 100
                )

                if currentmax > col_max_len:
                    errors[col] = (
                        f"Column {col} has string values longer than {col_max_len} characters"
                    )
                    column_lengths[col] = currentmax
                    is_string_length_exceeded = True

        return errors, column_lengths, is_pk_null, is_string_length_exceeded

#
#   Copyright 2025 Hopsworks AB
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

import hopsworks_common
from hsfs import engine as hsfs_engine
from hsfs import feature, feature_group
from hsfs.core.schema_validation import PySparkValidator
from hsfs.engine import spark


hopsworks_common.connection._hsfs_engine_type = "spark"


class TestPySparkValidator:
    def test_validate_pk_null_detection(self):
        # Arrange
        spark_engine = spark.Engine()
        hsfs_engine._engine = spark_engine

        data = [(None, "alice"), (2, "bob")]
        df = spark_engine._spark_session.createDataFrame(data, schema=["id", "name"])

        fg = feature_group.FeatureGroup(
            name="fg",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
        )

        validator = PySparkValidator()

        # Act
        errors, column_lengths, is_pk_null, is_string_length_exceeded = (
            validator._validate_df_specifics(fg, df)
        )

        # Assert
        assert is_pk_null is True
        assert "id" in errors
        assert column_lengths == {}
        assert is_string_length_exceeded is False

    def test_validate_string_length_exceeded(self):
        # Arrange
        spark_engine = spark.Engine()
        hsfs_engine._engine = spark_engine

        data = [(1, "short"), (2, "this_is_long")]
        df = spark_engine._spark_session.createDataFrame(data, schema=["id", "name"])

        # Feature definition with online_type varchar(5) to trigger exceed
        fg_feature = feature.Feature("name", online_type="varchar(5)")
        fg = feature_group.FeatureGroup(
            name="fg",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            features=[fg_feature],
            partition_key=[],
        )

        validator = PySparkValidator()

        # Act
        errors, column_lengths, is_pk_null, is_string_length_exceeded = (
            validator._validate_df_specifics(fg, df)
        )

        # Assert
        assert is_pk_null is False
        assert is_string_length_exceeded is True
        assert "name" in errors
        assert column_lengths["name"] >= len("this_is_long")

    def test_validate_no_issues(self):
        # Arrange
        spark_engine = spark.Engine()
        hsfs_engine._engine = spark_engine

        data = [(1, "ok"), (2, "fine")]
        df = spark_engine._spark_session.createDataFrame(data, schema=["id", "name"])

        fg_feature = feature.Feature("name", online_type="varchar(100)")
        fg = feature_group.FeatureGroup(
            name="fg",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            features=[fg_feature],
            partition_key=[],
        )

        validator = PySparkValidator()

        # Act
        errors, column_lengths, is_pk_null, is_string_length_exceeded = (
            validator._validate_df_specifics(fg, df)
        )

        # Assert
        assert errors == {}
        assert column_lengths == {}
        assert is_pk_null is False
        assert is_string_length_exceeded is False

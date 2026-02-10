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

import warnings
from unittest import mock

import pytest
from hsfs import feature, feature_group, feature_group_commit, util, validation_report
from hsfs.client import exceptions
from hsfs.core import feature_group_engine
from hsfs.core.feature_group_engine import FeatureGroupEngine
from hsfs.hopsworks_udf import udf


class TestFeatureGroupEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.save(
            feature_group=fg,
            feature_dataframe=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1

    def test_save_dataframe_transformation_functions(self, mocker):
        # Arrange
        feature_store_id = 99

        @udf(int)
        def add_one(col1):
            return col1 + 1

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        transformation_engine = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        @udf(int)
        def test(feature):
            return feature + 1

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            transformation_functions=[test],
            id=10,
        )

        # Act
        fg_engine.save(
            feature_group=fg,
            feature_dataframe=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1
        assert transformation_engine.apply_transformation_functions.call_count == 1

    def test_save_ge_report(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mock_ge_engine = mocker.patch(
            "hsfs.core.great_expectation_engine.GreatExpectationEngine"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        vr = validation_report.ValidationReport(
            success=None,
            results=[],
            meta=None,
            statistics=None,
            ingestion_result="REJECTED",
        )

        mock_ge_engine.return_value.validate.return_value = vr

        # Act
        fg_engine.save(
            feature_group=fg,
            feature_dataframe=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0

    def test_save_empty_table_creates_delta_table_for_delta_format(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        fg_engine = feature_group_engine.FeatureGroupEngine(feature_store_id=1)
        fg = feature_group.FeatureGroup(
            name="fg",
            version=1,
            featurestore_id=1,
            featurestore_name="fs",
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="DELTA",
        )
        mocker.patch.object(
            feature_group_engine.FeatureGroupEngine,
            "_get_spark_session_and_context",
            return_value=("spark", "context"),
        )
        delta_engine_mock = mocker.Mock()
        delta_engine_cls = mocker.patch(
            "hsfs.core.feature_group_engine.delta_engine.DeltaEngine",
            return_value=delta_engine_mock,
        )

        # Act
        fg_engine.save_empty_table(fg)

        # Assert
        delta_engine_cls.assert_called_once_with(
            fg.feature_store_id,
            fg.feature_store_name,
            fg,
            "spark",
            "context",
        )
        delta_engine_mock.save_empty_table.assert_called_once_with(write_options=None)

    def test_save_empty_table_noop_for_non_delta(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        fg_engine = feature_group_engine.FeatureGroupEngine(feature_store_id=1)
        fg = feature_group.FeatureGroup(
            name="fg",
            version=1,
            featurestore_id=1,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="HUDI",
        )
        delta_engine_cls = mocker.patch(
            "hsfs.core.feature_group_engine.delta_engine.DeltaEngine"
        )

        # Act
        result = fg_engine.save_empty_table(fg)

        # Assert
        assert result is None
        delta_engine_cls.assert_not_called()

    @pytest.mark.parametrize(
        "online_enabled,validation_options,should_validate_schema",
        [
            # Online enabled
            (True, None, True),
            (True, {}, True),
            (True, {"online_schema_validation": False}, False),
            (True, {"schema_validation": False}, False),
            # Not enabled
            (False, None, True),
            (False, {}, True),
            (False, {"online_schema_validation": False}, False),
            (False, {"schema_validation": False}, False),
        ],
    )
    def test_insert(
        self, online_enabled, validation_options, should_validate_schema, mocker
    ):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_validate_schema = mocker.patch(
            "hsfs.core.schema_validation.DataFrameValidator.validate_schema"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_enabled=online_enabled,
        )

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=None,
            operation=None,
            storage=None,
            write_options=None,
            validation_options=validation_options,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_validate_schema.called == should_validate_schema

    def test_insert_storage(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.insert(
                feature_group=fg,
                feature_dataframe=None,
                overwrite=None,
                operation=None,
                storage="online",
                write_options=None,
            )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            str(e_info.value) == "Online storage is not enabled for this feature group."
        )

    def test_insert_transformation_functions(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        tf_engine_patch = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        @udf(int)
        def test(feature):
            return feature + 1

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            transformation_functions=[test],
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=None,
            operation=None,
            storage=None,
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1
        assert tf_engine_patch.apply_transformation_functions.call_count == 1

    def test_insert_id(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=None,
            operation=None,
            storage=None,
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1

    def test_insert_ge_report(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_ge_engine = mocker.patch(
            "hsfs.core.great_expectation_engine.GreatExpectationEngine"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value="url",
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        vr = validation_report.ValidationReport(
            success=None,
            results=[],
            meta=None,
            statistics=None,
            ingestion_result="REJECTED",
        )

        mock_ge_engine.return_value.validate.return_value = vr

        # Act
        with pytest.raises(exceptions.DataValidationException):
            fg_engine.insert(
                feature_group=fg,
                feature_dataframe=None,
                overwrite=None,
                operation=None,
                storage=None,
                write_options=None,
            )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 0
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0

    def test_insert_overwrite(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        # Act
        fg_engine.insert(
            feature_group=fg,
            feature_dataframe=None,
            overwrite=True,
            operation=None,
            storage=None,
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.delete_content.call_count == 1
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1

    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.delete(feature_group=fg)

        # Assert
        assert mock_fg_api.return_value.delete.call_count == 1

    def test_commit_details(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.commit_details(feature_group=fg, wallclock_time=None, limit=None)

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 0
        assert (
            str(e_info.value)
            == "commit_details can only be used on time travel enabled feature groups"
        )

    def test_commit_details_time_travel_format(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="wrong",
            id=10,
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.commit_details(feature_group=fg, wallclock_time=None, limit=None)

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 0
        assert (
            str(e_info.value)
            == "commit_details can only be used on time travel enabled feature groups"
        )

    def test_commit_details_time_travel_format_hudi(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch("hsfs.util.get_hudi_datestr_from_timestamp")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="HUDI",
            id=10,
        )

        # Act
        fg_engine.commit_details(feature_group=fg, wallclock_time=None, limit=None)

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 1

    def test_commit_details_time_travel_format_hudi_fg_commit(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.util.get_timestamp_from_date_string")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_util_get_hudi_datestr_from_timestamp = mocker.patch(
            "hsfs.util.get_hudi_datestr_from_timestamp"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="HUDI",
            id=10,
        )

        fg_commit = feature_group_commit.FeatureGroupCommit(
            commitid=1, rows_inserted=2, rows_updated=3, rows_deleted=4
        )
        mock_fg_api.return_value.get_commit_details.return_value = [fg_commit]
        mock_util_get_hudi_datestr_from_timestamp.return_value = "123"

        # Act
        result = fg_engine.commit_details(
            feature_group=fg, wallclock_time=None, limit=None
        )

        # Assert
        assert mock_fg_api.return_value.get_commit_details.call_count == 1
        assert result == {
            1: {
                "committedOn": "123",
                "rowsUpdated": 3,
                "rowsInserted": 2,
                "rowsDeleted": 4,
            }
        }

    def test_commit_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mock_hudi_engine = mocker.patch("hsfs.core.hudi_engine.HudiEngine")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.commit_delete(feature_group=fg, delete_df=None, write_options=None)

        # Assert
        assert mock_hudi_engine.return_value.delete_record.call_count == 1

    def test_clean_delta(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_instance")
        mock_hudi_engine = mocker.patch("hsfs.core.delta_engine.DeltaEngine")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            time_travel_format="DELTA",
        )

        # Act
        fg_engine.delta_vacuum(feature_group=fg, retention_hours=200)

        # Assert
        assert mock_hudi_engine.return_value.vacuum.call_count == 1

    def test_clean_hudi(self, mocker):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            time_travel_format="HUDI",
        )

        # Act
        fg_engine.delta_vacuum(feature_group=fg, retention_hours=200)

    def test_sql(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_sc_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch("hsfs.engine.get_instance")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_engine.sql(
            query=None,
            feature_store_name=None,
            dataframe_type=None,
            online=None,
            read_options=None,
        )

        # Assert
        assert mock_sc_api.return_value.get_online_connector.call_count == 0

    def test_sql_online(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_sc_api = mocker.patch(
            "hsfs.core.storage_connector_api.StorageConnectorApi"
        )
        mocker.patch("hsfs.engine.get_instance")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_engine.sql(
            query=None,
            feature_store_name=None,
            dataframe_type=None,
            online=True,
            read_options=None,
        )

        # Assert
        assert mock_sc_api.return_value.get_online_connector.call_count == 1

    def test_update_features_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine._update_features_metadata(feature_group=fg, features=None)

        # Assert
        assert mock_fg_api.return_value.update_metadata.call_count == 1

    def test_update_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_engine_new_feature_list = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.new_feature_list"
        )
        mock_fg_engine_update_features_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._update_features_metadata"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fg_engine.update_features(feature_group=None, updated_features=None)

        # Assert
        assert mock_fg_engine_new_feature_list.call_count == 1
        assert mock_fg_engine_update_features_metadata.call_count == 1

    def test_append_features(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mock_fg_engine_update_features_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._update_features_metadata"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="str")
        f2 = feature.Feature(name="f2", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            features=[f, f1],
            id=10,
        )

        fg.read = mocker.Mock()

        # Act
        fg_engine.append_features(feature_group=fg, new_features=[f1, f2])

        # Assert
        assert mock_engine_get_instance.return_value.update_table_schema.call_count == 1
        assert len(mock_fg_engine_update_features_metadata.call_args[0][1]) == 4

    def test_update_description(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.update_description(feature_group=fg, description=None)

        # Assert
        assert mock_fg_api.return_value.update_metadata.call_count == 1

    def test_get_subject(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_kafka_api = mocker.patch("hsfs.core.kafka_api.KafkaApi")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )
        fg.feature_store = mocker.patch("hsfs.feature_store.FeatureStore")

        # Act
        fg_engine.get_subject(feature_group=fg)

        # Assert
        assert mock_kafka_api.return_value.get_subject.call_count == 1

    def test_insert_stream(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine.insert_stream(
                feature_group=fg,
                dataframe=None,
                query_name=None,
                output_mode=None,
                await_termination=None,
                timeout=None,
                checkpoint_dir=None,
                write_options=None,
            )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 0
        )
        assert (
            str(e_info.value)
            == "Online storage is not enabled for this feature group. It is currently only possible to stream to the online storage."
        )

    def test_insert_stream_online_enabled(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_warnings_warn = mocker.patch("warnings.warn")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_enabled=True,
        )

        # Act
        fg_engine.insert_stream(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode=None,
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 1
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 1
        )
        assert (
            mock_warnings_warn.call_args[0][0]
            == "`insert_stream` method in the next release will be available only for feature groups created with "
            "`stream=True`."
        )

    def test_insert_stream_stream(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            stream=True,
        )

        # Act
        fg_engine.insert_stream(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode=None,
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 1
        )

    def test_insert_stream_stream_transformation_functions(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        tf_engine_patch = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )

        @udf(int)
        def test(feature):
            return feature + 1

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            transformation_functions=[test],
            stream=True,
        )

        # Act
        fg_engine.insert_stream(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode=None,
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 1
        )
        assert tf_engine_patch.apply_transformation_functions.call_count == 1

    def test_insert_stream_online_enabled_id(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mock_engine_get_instance = mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_warnings_warn = mocker.patch("warnings.warn")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_enabled=True,
            id=10,
        )

        # Act
        fg_engine.insert_stream(
            feature_group=fg,
            dataframe=None,
            query_name=None,
            output_mode=None,
            await_termination=None,
            timeout=None,
            checkpoint_dir=None,
            write_options=None,
        )

        # Assert
        assert mock_engine_get_instance.return_value.save_dataframe.call_count == 0
        assert (
            mock_engine_get_instance.return_value.save_stream_dataframe.call_count == 1
        )
        assert (
            mock_warnings_warn.call_args[0][0]
            == "`insert_stream` method in the next release will be available only for feature groups created with "
            "`stream=True`."
        )

    def test_verify_schema_compatibility(self):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg_features = []
        df_features = []

        # Act
        fg_engine._verify_schema_compatibility(
            feature_group_features=fg_features, dataframe_features=df_features
        )

        # Assert
        assert len(fg_features) == 0
        assert len(df_features) == 0

    def test_verify_schema_compatibility_feature_group_features(self):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int")

        fg_features = [f, f1]
        df_features = []

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine._verify_schema_compatibility(
                feature_group_features=fg_features, dataframe_features=df_features
            )

        # Assert
        assert len(fg_features) == 2
        assert len(df_features) == 0
        assert (
            str(e_info.value)
            == "Features are not compatible with Feature Group schema: \n"
            " - f (type: 'str') is missing from input dataframe.\n"
            " - f1 (type: 'int') is missing from input dataframe."
            "\nNote that feature (or column) names are case insensitive and "
            "spaces are automatically replaced with underscores."
        )

    def test_verify_schema_compatibility_dataframe_features(self):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int")

        fg_features = []
        df_features = [f, f1]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine._verify_schema_compatibility(
                feature_group_features=fg_features, dataframe_features=df_features
            )

        # Assert
        assert len(fg_features) == 0
        assert len(df_features) == 2
        assert (
            str(e_info.value)
            == "Features are not compatible with Feature Group schema: \n"
            " - f (type: 'str') does not exist in feature group.\n"
            " - f1 (type: 'int') does not exist in feature group."
            "\nNote that feature (or column) names are case insensitive and "
            "spaces are automatically replaced with underscores."
        )

    def test_verify_schema_compatibility_feature_group_features_dataframe_features(
        self,
    ):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int  ")
        f1_2 = feature.Feature(name="f1", type="INT")

        fg_features = [f, f1]
        df_features = [f, f1_2]

        # Act
        fg_engine._verify_schema_compatibility(
            feature_group_features=fg_features, dataframe_features=df_features
        )

        # Assert
        assert len(fg_features) == 2
        assert len(df_features) == 2

    def test_verify_schema_compatibility_feature_group_features_dataframe_features_wrong_type(
        self,
    ):
        # Arrange
        feature_store_id = 99

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")
        f1 = feature.Feature(name="f1", type="int")
        f1_2 = feature.Feature(name="f1", type="bool")

        fg_features = [f, f1]
        df_features = [f, f1_2]

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            fg_engine._verify_schema_compatibility(
                feature_group_features=fg_features, dataframe_features=df_features
            )

        # Assert
        assert len(fg_features) == 2
        assert len(df_features) == 2
        assert (
            str(e_info.value)
            == "Features are not compatible with Feature Group schema: \n"
            " - f1 (expected type: 'int', derived from input: 'bool') has the wrong type."
            "\nNote that feature (or column) names are case insensitive and "
            "spaces are automatically replaced with underscores."
        )

    def test_save_feature_group_metadata(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_save_empty_table = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_empty_table"
        )
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        fg_engine.save_feature_group_metadata(
            feature_group=fg, dataframe_features=[f], write_options=None
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is False
        assert f.partition is False
        assert f.hudi_precombine_key is False
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature Group created successfully, explore it at \n{feature_group_url}"
        )
        mock_save_empty_table.assert_not_called()

    def test_save_feature_group_metadata_features(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_save_empty_table = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_empty_table"
        )
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            features=[f],
            id=10,
        )

        # Act
        fg_engine.save_feature_group_metadata(
            feature_group=fg, dataframe_features=None, write_options=None
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is False
        assert f.partition is False
        assert f.hudi_precombine_key is False
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature Group created successfully, explore it at \n{feature_group_url}"
        )
        mock_save_empty_table.assert_called_once_with(fg, write_options=None)

    def test_save_feature_group_metadata_primary_partition_precombine(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_save_empty_table = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_empty_table"
        )
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=["f"],
            foreign_key=[],
            partition_key=["f"],
            hudi_precombine_key="f",
            time_travel_format="HUDI",
        )

        # Act
        fg_engine.save_feature_group_metadata(
            feature_group=fg, dataframe_features=[f], write_options=None
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is True
        assert f.partition is True
        assert f.hudi_precombine_key is True
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature Group created successfully, explore it at \n{feature_group_url}"
        )
        mock_save_empty_table.assert_not_called()

    def test_save_feature_group_metadata_primary_partition_precombine_event_error(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )

        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_pk_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["feature_name"],
                foreign_key=[],
                partition_key=["f"],
                hudi_precombine_key="f",
                event_time="f",
                time_travel_format="HUDI",
            )

            fg_engine.save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        with pytest.raises(exceptions.FeatureStoreException) as e_partk_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["f"],
                foreign_key=[],
                partition_key=["feature_name"],
                hudi_precombine_key="f",
                event_time="f",
                time_travel_format="HUDI",
            )

            fg_engine.save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        with pytest.raises(exceptions.FeatureStoreException) as e_prekk_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["f"],
                foreign_key=[],
                partition_key=["f"],
                hudi_precombine_key="feature_name",
                event_time="f",
                time_travel_format="HUDI",
            )

            fg_engine.save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        with pytest.raises(exceptions.FeatureStoreException) as e_eventt_info:
            fg = feature_group.FeatureGroup(
                name="test",
                version=1,
                featurestore_id=feature_store_id,
                primary_key=["f"],
                foreign_key=[],
                partition_key=["f"],
                hudi_precombine_key="f",
                event_time="feature_name",
                time_travel_format="HUDI",
            )

            fg_engine.save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        assert (
            str(e_pk_info.value)
            == "Provided primary key(s) feature_name doesn't exist in feature dataframe"
        )
        assert (
            str(e_partk_info.value)
            == "Provided partition key(s) feature_name doesn't exist in feature dataframe"
        )
        assert (
            str(e_prekk_info.value)
            == "Provided hudi precombine key feature_name doesn't exist in feature "
            "dataframe"
        )
        assert (
            str(e_eventt_info.value)
            == "Provided event_time feature feature_name doesn't exist in feature dataframe"
        )

    def test_save_feature_group_metadata_no_schema_does_not_create_empty_table(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_save_empty_table = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_empty_table"
        )
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            features=[],
        )

        # Act
        fg_engine.save_feature_group_metadata(
            feature_group=fg,
            dataframe_features=[feature.Feature(name="f", type="str")],
            write_options=None,
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature Group created successfully, explore it at \n{feature_group_url}"
        )
        mock_save_empty_table.assert_not_called()

    def test_save_feature_group_metadata_write_options(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"
        write_options = {"spark": "test"}

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mock_save_empty_table = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_empty_table"
        )
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )
        mock_print = mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            stream=True,
            id=10,
        )

        # Act
        fg_engine.save_feature_group_metadata(
            feature_group=fg, dataframe_features=[f], write_options=write_options
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        assert f.primary is False
        assert f.partition is False
        assert f.hudi_precombine_key is False
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature Group created successfully, explore it at \n{feature_group_url}"
        )
        mock_save_empty_table.assert_not_called()

    def test_save_feature_group_metadata_client_only_options(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"
        write_options = {
            "kafka_producer_config": {"bootstrap.servers": "localhost:9092"},
            "online_ingestion_options": {"timeout": 120, "period": 2},
            "some_server_option": "value",
        }

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_empty_table"
        )
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )
        mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            stream=True,
            id=10,
        )

        # Act
        fg_engine.save_feature_group_metadata(
            feature_group=fg, dataframe_features=[f], write_options=write_options
        )

        # Assert
        assert mock_fg_api.return_value.save.call_count == 1
        job_conf = fg._deltastreamer_jobconf
        write_opts = job_conf._options
        option_names = [opt["name"] for opt in write_opts]
        assert "kafka_producer_config" not in option_names
        assert "online_ingestion_options" not in option_names
        assert "some_server_option" in option_names

    def test_update_feature_group_schema_on_demand_transformations(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")

        @udf(int)
        def test(feature):
            return feature + 1

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            transformation_functions=[test("col2")],
        )
        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")

        # Act
        result = fg_engine._update_feature_group_schema_on_demand_transformations(
            feature_group=fg, features=[f, f1]
        )

        # Assert
        assert len(result) == 3
        assert result[0].name == "col1"
        assert result[1].name == "col2"
        assert result[2].name == "test"

    def test_update_feature_group_schema_on_demand_transformations_drop(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")

        @udf(int, drop="feature")
        def test(feature):
            return feature + 1

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            transformation_functions=[test("col2")],
        )
        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="str")

        # Act
        result = fg_engine._update_feature_group_schema_on_demand_transformations(
            feature_group=fg, features=[f, f1]
        )

        # Assert
        assert len(result) == 2
        assert result[0].name == "col1"
        assert result[1].name == "test"

    def test_update_feature_group_schema_on_demand_transformations_duplicate_feature_name(
        self, mocker
    ):
        """Test that transformation output columns with the same name as existing features are not added."""
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")

        @udf(int)
        def col2(col1):
            return col1 + 1

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            transformation_functions=[col2("col1")],
        )
        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="col2", type="int")

        # Act
        result = fg_engine._update_feature_group_schema_on_demand_transformations(
            feature_group=fg, features=[f, f1]
        )

        # Assert
        assert len(result) == 2
        assert result[0].name == "col1"
        assert result[1].name == "col2"
        assert result[1].on_demand is False

    def test_update_feature_group_schema_on_demand_transformations_partial_duplicate(
        self, mocker
    ):
        """Test transformation with multiple outputs where some match existing feature names."""
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata"
        )
        mocker.patch("hsfs.core.great_expectation_engine.GreatExpectationEngine")

        @udf([int, int])
        def multi_output(col1):
            return col1 + 1, col1 + 2

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            transformation_functions=[multi_output("col1")],
        )
        f = feature.Feature(name="col1", type="str")
        f1 = feature.Feature(name="multi_output_0", type="int")

        # Act
        result = fg_engine._update_feature_group_schema_on_demand_transformations(
            feature_group=fg, features=[f, f1]
        )

        # Assert
        assert len(result) == 3
        assert result[0].name == "col1"
        assert result[1].name == "multi_output_0"
        assert result[1].on_demand is False
        assert result[2].name == "multi_output_1"
        assert result[2].on_demand is True

    def test_save_feature_group_metadata_corrects_topic_name(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_group_url = "test_url"

        mocker.patch("hsfs.engine.get_type")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._verify_schema_compatibility"
        )
        mock_fg_api = mocker.patch("hsfs.core.feature_group_api.FeatureGroupApi")
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_empty_table"
        )
        mocker.patch(
            "hsfs.util.get_feature_group_url",
            return_value=feature_group_url,
        )
        mocker.patch("builtins.print")

        fg_engine = feature_group_engine.FeatureGroupEngine(
            feature_store_id=feature_store_id
        )

        f = feature.Feature(name="f", type="str")

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            online_enabled=True,
            topic_name="my_topic",
        )

        # Act
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            fg_engine.save_feature_group_metadata(
                feature_group=fg, dataframe_features=[f], write_options=None
            )

        # Assert
        assert fg.topic_name == "my_topic_onlinefs"
        assert len(w) == 1
        assert issubclass(w[0].category, util.FeatureGroupWarning)
        assert mock_fg_api.return_value.save.call_count == 1

    def test_topic_name_auto_suffix_when_online_enabled(self):
        fg = mock.MagicMock()
        fg.online_enabled = True
        fg.topic_name = "my_topic"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            FeatureGroupEngine._validate_topic_name(fg)

        assert fg.topic_name == "my_topic_onlinefs"
        assert len(w) == 1
        assert issubclass(w[0].category, util.FeatureGroupWarning)
        assert "my_topic" in str(w[0].message)
        assert "_onlinefs" in str(w[0].message)

    def test_topic_name_no_warning_when_suffix_present(self):
        fg = mock.MagicMock()
        fg.online_enabled = True
        fg.topic_name = "my_topic_onlinefs"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            FeatureGroupEngine._validate_topic_name(fg)

        assert fg.topic_name == "my_topic_onlinefs"
        assert len(w) == 0

    def test_topic_name_none_no_validation(self):
        fg = mock.MagicMock()
        fg.online_enabled = True
        fg.topic_name = None

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            FeatureGroupEngine._validate_topic_name(fg)

        assert fg.topic_name is None
        assert len(w) == 0

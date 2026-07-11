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

import hsfs
import pytest
from hsfs import (
    engine,
    expectation_suite,
    feature,
    feature_group,
    feature_group_writer,
    feature_store,
    statistics_config,
    storage_connector,
    user,
    util,
)
from hsfs.client.exceptions import FeatureStoreException, RestAPIError
from hsfs.core.constants import GE_MAJOR, HAS_GREAT_EXPECTATIONS
from hsfs.engine import python, spark
from hsfs.transformation_function import TransformationType


with mock.patch("hopsworks_common.client._get_instance"):
    engine._init("python")


def get_test_feature_group():
    return feature_group.FeatureGroup(
        name="test",
        version=1,
        description="description",
        online_enabled=False,
        time_travel_format="HUDI",
        partition_key=[],
        primary_key=["pk"],
        foreign_key=["fk"],
        hudi_precombine_key="pk",
        featurestore_id=1,
        featurestore_name="fs",
        features=[
            feature.Feature("pk", primary=True),
            feature.Feature("fk", foreign=True),
            feature.Feature("ts", primary=False),
            feature.Feature("f1", primary=False),
            feature.Feature("f2", primary=False),
        ],
        statistics_config={},
        event_time="ts",
        stream=False,
        expectation_suite=None,
    )


test_feature_group = get_test_feature_group()


class TestFeatureGroup:
    @pytest.fixture(autouse=True)
    def mock_has_deltalake(self, mocker):
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.columns) == 2
        assert isinstance(fg.columns[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is False
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_list"]["response"]

        # Act
        fg_list = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.columns) == 2
        assert isinstance(fg.columns[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is False
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert len(fg.columns) == 2
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format == "DELTA"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name is None
        assert fg.event_time is None
        assert fg.stream is False
        assert fg.expectation_suite is None
        assert fg.deprecated is False

    def test_from_response_json_basic_info_deprecated(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_basic_info_deprecated"][
            "response"
        ]

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert len(fg.columns) == 2
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format == "DELTA"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name is None
        assert fg.event_time is None
        assert fg.stream is False
        assert fg.expectation_suite is None
        assert fg.deprecated is True
        assert len(warning_record) == 1
        assert str(warning_record[0].message) == (
            f"Feature Group `{fg.name}`, version `{fg.version}` is deprecated"
        )

    def test_from_response_json_stream(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_stream"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.columns) == 2
        assert isinstance(fg.columns[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is True
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_stream_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_stream_list"]["response"]

        # Act
        fg_list = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.columns) == 2
        assert isinstance(fg.columns[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is True
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_from_response_json_stream_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]

        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == ""
        assert fg.partition_key == []
        assert fg.primary_key == []
        assert fg.hudi_precombine_key is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert len(fg.columns) == 0
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format == "DELTA"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name is None
        assert fg.event_time is None
        assert fg.stream is True
        assert fg.expectation_suite is None

    def test_constructor_with_list_event_time_for_compatibility(
        self, mocker, backend_fixtures, dataframe_fixture_basic
    ):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type")
        json = backend_fixtures["feature_store"]["get"]["response"]

        features = [
            feature.Feature(name="pk", type="int"),
            feature.Feature(name="et", type="timestamp"),
            feature.Feature(name="feat", type="int"),
        ]

        # Act
        fs = feature_store.FeatureStore.from_response_json(json)
        with warnings.catch_warnings(record=True) as warning_record:
            new_fg = fs.create_feature_group(
                name="fg_name",
                version=1,
                description="fg_description",
                event_time=["event_date"],
                features=features,
            )
        with pytest.raises(FeatureStoreException):
            util._verify_attribute_key_names(new_fg, False)

        # Assert
        assert new_fg.event_time == "event_date"
        assert len(warning_record) == 1
        assert issubclass(warning_record[0].category, DeprecationWarning)
        assert str(warning_record[0].message) == (
            "Providing event_time as a single-element list is deprecated"
            " and will be dropped in future versions. Provide the feature_name string instead."
        )

    def test_select_all(self):
        query = test_feature_group.select_all()
        features = query.features
        assert len(features) == 5
        assert {f.name for f in features} == {"pk", "fk", "ts", "f1", "f2"}

    def test_select_all_exclude_pk(self):
        query = test_feature_group.select_all(include_primary_key=False)
        features = query.features
        assert len(features) == 4
        assert {f.name for f in features} == {"ts", "fk", "f1", "f2"}

    def test_select_all_exclude_fk(self):
        query = test_feature_group.select_all(include_foreign_key=False)
        features = query.features
        assert len(features) == 4
        assert {f.name for f in features} == {"f1", "f2", "pk", "ts"}

    def test_select_all_exclude_ts(self):
        query = test_feature_group.select_all(include_event_time=False)
        features = query.features
        assert len(features) == 4
        assert {f.name for f in features} == {"pk", "fk", "f1", "f2"}

    def test_select_all_exclude_pk_ts(self):
        query = test_feature_group.select_all(
            include_primary_key=False, include_event_time=False
        )
        features = query.features
        assert len(features) == 3
        assert {f.name for f in features} == {"f1", "f2", "fk"}

    def test_select_features(self):
        query = test_feature_group.select_features()
        features = query.features
        assert len(features) == 2
        assert {f.name for f in features} == {"f1", "f2"}

    def test_materialization_job(self, mocker):
        mock_job = mocker.Mock()
        mock_job_api = mocker.patch(
            "hsfs.core.job_api.JobApi.get", return_value=mock_job
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # call first time should populate cache
        fg.materialization_job  # noqa: B018

        mock_job_api.assert_called_once_with("test_fg_2_offline_fg_materialization")
        assert fg._materialization_job == mock_job

        # call second time
        fg.materialization_job  # noqa: B018

        # make sure it still was called only once
        mock_job_api.assert_called_once  # noqa: B018
        assert fg.materialization_job == mock_job

    def test_feature_group_online_disk_not_set(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_featurestore_online_tablespace",
            return_value="ts_1",  # Simulate no tablespace set
        )

        # Act
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        # Assert
        assert variable_api_mock.call_count == 0
        assert fg._online_config is None

    def test_feature_group_online_disk_not_set_online_config(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_featurestore_online_tablespace",
            return_value="ts_1",
        )

        # Act
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_config={
                "table_space": "tt",
                "online_comments": ["NDB_TABLE=READ_BACKUP=1"],
            },
        )

        # Assert
        assert variable_api_mock.call_count == 0
        assert fg._online_config.to_dict() == {
            "onlineComments": ["NDB_TABLE=READ_BACKUP=1"],
            "tableSpace": "tt",
            "primaryKeyIndexType": None,
        }

    def test_feature_group_online_disk_true(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_featurestore_online_tablespace",
            return_value="ts_1",  # Simulate no tablespace set
        )

        # Act
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_disk=True,
        )

        # Assert
        assert variable_api_mock.call_count == 1
        assert fg._online_config.to_dict() == {
            "onlineComments": None,
            "tableSpace": "ts_1",
            "primaryKeyIndexType": None,
        }

    def test_feature_group_online_disk_true_override_online_config(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_featurestore_online_tablespace",
            return_value="ts_1",
        )

        # Act
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_disk=True,
            online_config={
                "table_space": "",
                "online_comments": ["NDB_TABLE=READ_BACKUP=1"],
            },
        )

        # Assert
        assert variable_api_mock.call_count == 1
        assert fg._online_config.to_dict() == {
            "onlineComments": ["NDB_TABLE=READ_BACKUP=1"],
            "tableSpace": "ts_1",
            "primaryKeyIndexType": None,
        }

    def test_feature_group_online_disk_false(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_featurestore_online_tablespace",
            return_value="ts_1",
        )

        # Act
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_disk=False,
        )

        # Assert
        assert variable_api_mock.call_count == 0
        assert fg._online_config.to_dict() == {
            "onlineComments": None,
            "tableSpace": "",
            "primaryKeyIndexType": None,
        }

    def test_feature_group_online_disk_false_override_online_config(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_featurestore_online_tablespace",
            return_value="ts_1",
        )

        # Act
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            online_disk=False,
            online_config={
                "table_space": "ts_1",
                "online_comments": ["NDB_TABLE=READ_BACKUP=1"],
            },
        )

        # Assert
        assert variable_api_mock.call_count == 0
        assert fg._online_config.to_dict() == {
            "onlineComments": ["NDB_TABLE=READ_BACKUP=1"],
            "tableSpace": "",
            "primaryKeyIndexType": None,
        }

    def test_feature_group_data_source_update_storage_connector(self, mocker):
        # Arrange
        data_source = mocker.Mock()
        sc = storage_connector.S3Connector(id=1, name="s3_conn", featurestore_id=1)
        data_source.storage_connector = sc

        # Act
        feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            data_source=data_source,
        )

        # Assert
        assert data_source._update_storage_connector.call_count == 1
        data_source._update_storage_connector.assert_called_once_with(sc)

    def test_materialization_job_retry_success(self, mocker):
        # Arrange
        mocker.patch("time.sleep")

        mock_response_job_not_found = mocker.Mock()
        mock_response_job_not_found.status_code = 404
        mock_response_job_not_found.json.return_value = {"errorCode": 130009}

        mock_response_not_found = mocker.MagicMock()
        mock_response_not_found.status_code = 404

        mock_job = mocker.Mock()

        mock_job_api = mocker.patch(
            "hsfs.core.job_api.JobApi.get",
            side_effect=[
                RestAPIError("", mock_response_job_not_found),
                RestAPIError("", mock_response_not_found),
                RestAPIError("", mock_response_not_found),
                mock_job,
            ],
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        job_result = fg.materialization_job

        # Assert
        assert job_result is mock_job
        assert mock_job_api.call_count == 4
        assert mock_job_api.call_args_list[0][0] == (
            "test_fg_2_offline_fg_materialization",
        )
        assert mock_job_api.call_args_list[1][0] == ("test_fg_2_offline_fg_backfill",)
        assert mock_job_api.call_args_list[2][0] == ("test_fg_2_offline_fg_backfill",)
        assert mock_job_api.call_args_list[3][0] == ("test_fg_2_offline_fg_backfill",)

    def test_materialization_job_retry_fail(self, mocker):
        # Arrange
        mocker.patch("time.sleep")

        mock_response_not_found = mocker.MagicMock()
        mock_response_not_found.status_code = 404

        mock_job_api = mocker.patch(
            "hsfs.core.job_api.JobApi.get",
            side_effect=RestAPIError("", mock_response_not_found),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            fg.materialization_job  # noqa: B018

        # Assert
        assert mock_job_api.call_count == 6
        assert str(e_info.value) == "No materialization job was found"

    def test_multi_part_insert_return_writer(self, mocker):
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        result = fg.multi_part_insert()
        assert isinstance(result, feature_group_writer.FeatureGroupWriter)
        assert result._feature_group == fg

    def test_multi_part_insert_call_insert(self, mocker, dataframe_fixture_basic):
        mock_writer = mocker.Mock()
        mocker.patch(
            "hsfs.feature_group_writer.FeatureGroupWriter", return_value=mock_writer
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
        )

        fg.multi_part_insert(dataframe_fixture_basic)
        mock_writer.insert.assert_called_once()
        assert fg._multi_part_insert is True

    def test_save_feature_list(self, mocker):
        mock_save_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata",
            return_value=None,
        )

        features = [
            feature.Feature(name="pk", type="int"),
            feature.Feature(name="et", type="timestamp"),
            feature.Feature(name="feat", type="int"),
        ]

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            time_travel_format="DELTA",
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        fg.save(features)
        mock_save_metadata.assert_called_once_with(
            fg, None, {"delta.enableChangeDataFeed": "true"}
        )

    def test_save_feature_in_create(self, mocker):
        mock_save_metadata = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save_feature_group_metadata",
            return_value=None,
        )

        features = [
            feature.Feature(name="pk", type="int"),
            feature.Feature(name="et", type="timestamp"),
            feature.Feature(name="feat", type="int"),
        ]

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            time_travel_format="DELTA",
            features=features,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        fg.save()
        mock_save_metadata.assert_called_once_with(
            fg, None, {"delta.enableChangeDataFeed": "true"}
        )

    def test_save_exception_empty_input(self):
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
        )

        with pytest.raises(FeatureStoreException) as e:
            fg.save()

        assert "Feature list not provided" in str(e.value)

    def test_save_with_non_feature_list(self, mocker):
        engine = python.Engine()
        mocker.patch("hsfs.engine._get_instance", return_value=engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mock_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.python.Engine._convert_to_default_dataframe"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._save",
            return_value=(None, None),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            time_travel_format="DELTA",
        )

        data = [[1, "test_1"], [2, "test_2"]]
        fg.save(data)

        # Stats are computed by the backend ingestion-triggered FM job, not in the client.
        mock_convert_to_default_dataframe.assert_called_once_with(data)

    def test_save_report_true_default(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine._get_instance", return_value=engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine._convert_to_default_dataframe",
            return_value=dataframe_fixture_basic,
        )
        mock_insert = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._insert",
            return_value=(None, None),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            time_travel_format="DELTA",
        )

        fg.insert(dataframe_fixture_basic)
        mock_insert.assert_called_once_with(
            fg,
            feature_dataframe=dataframe_fixture_basic,
            overwrite=False,
            operation="upsert",
            storage=None,
            write_options={"wait_for_job": False, "wait_for_online_ingestion": False},
            validation_options={"save_report": True},
            transformation_context=None,
            transform=True,
            n_processes=None,
        )

    def test_save_report_default_overwritable(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine._get_instance", return_value=engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine._convert_to_default_dataframe",
            return_value=dataframe_fixture_basic,
        )
        mock_insert = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine._insert",
            return_value=(None, None),
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=2,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            id=10,
            time_travel_format="DELTA",
        )

        fg.insert(dataframe_fixture_basic, validation_options={"save_report": False})

        mock_insert.assert_called_once_with(
            fg,
            feature_dataframe=dataframe_fixture_basic,
            overwrite=False,
            operation="upsert",
            storage=None,
            write_options={"wait_for_job": False, "wait_for_online_ingestion": False},
            validation_options={"save_report": False},
            transformation_context=None,
            transform=True,
            n_processes=None,
        )

    @pytest.mark.parametrize(
        "sc,expected",
        [
            (None, True),  # default HopsFS
            (
                storage_connector.HopsFSConnector(
                    id=1, name="default", featurestore_id=1
                ),
                True,
            ),  # explicit HopsFS connector
            (
                storage_connector.S3Connector(id=1, name="s3", featurestore_id=1),
                False,
            ),  # non-HopsFS connector
        ],
    )
    def test_is_hopsfs_storage(self, sc, expected):
        fg = get_test_feature_group()
        fg.storage_connector = sc
        assert fg._is_hopsfs_storage() is expected

    def test_is_hopsfs_storage_uses_location_when_source_connector_is_external(
        self,
    ):
        fg = get_test_feature_group()
        fg._location = (
            "hopsfs://rpc.namenode.service.consul:8020/apps/hive/warehouse/fs.db/fg_1"
        )
        fg.storage_connector = storage_connector.RedshiftConnector(
            id=1,
            name="redshift",
            featurestore_id=1,
        )

        assert fg._is_hopsfs_storage() is True

    def test_is_hopsfs_storage_uses_single_slash_hopsfs_location(
        self,
    ):
        fg = get_test_feature_group()
        fg._location = "hopsfs:/apps/hive/warehouse/fs.db/fg_1"
        fg.storage_connector = storage_connector.RedshiftConnector(
            id=1,
            name="redshift",
            featurestore_id=1,
        )

        assert fg._is_hopsfs_storage() is True

    def test_init_time_travel_and_stream_uses_resolvers_python(
        self, mocker, monkeypatch
    ):
        """Verify that `_init_time_travel_and_stream` delegates to `_resolve_time_travel_format` and, for the Python engine, to `_resolve_stream_python` to derive `_time_travel_format` and `_stream`.

        This test does not validate resolver logic; it only checks that the outputs of the resolver functions are propagated to the FeatureGroup instance and that the resolvers are called with the expected arguments (notably that the stream resolver receives the already-resolved format).
        """
        # Arrange: ensure code path selects Python Engine class
        monkeypatch.setattr("hsfs.engine._get_type", lambda: "python")
        expected_fmt = "DELTA"
        expected_stream = False
        fmt_mock = mocker.Mock(return_value=expected_fmt)
        stream_mock = mocker.Mock(return_value=expected_stream)
        monkeypatch.setattr(
            "hsfs.feature_group.FeatureGroup._resolve_time_travel_format",
            fmt_mock,
        )
        monkeypatch.setattr(
            "hsfs.feature_group.FeatureGroup._resolve_stream_python",
            stream_mock,
        )

        fg = get_test_feature_group()

        # Act
        # Reset mocks to ignore any constructor-time calls
        fmt_mock.reset_mock()
        stream_mock.reset_mock()
        fg._init_time_travel_and_stream(
            stream=False,
            time_travel_format="None",
        )

        # Assert: _init uses resolvers' outputs
        assert fg._time_travel_format == expected_fmt
        assert fg._stream is expected_stream
        assert fmt_mock.call_count == 1
        assert fmt_mock.call_args.kwargs == {
            "time_travel_format": "None",
        }
        assert stream_mock.call_count == 1
        assert stream_mock.call_args.kwargs == {
            "stream": False,
            "time_travel_format": expected_fmt,
            "hopsfs_external": False,
        }

    def test_init_time_travel_and_stream_uses_resolver_spark(self, mocker, monkeypatch):
        """Verify that `_init_time_travel_and_stream` delegates to `_resolve_time_travel_format` for setting `_time_travel_format` on the Spark engine, and that it does not call `_resolve_stream_python` nor mutate `_stream` (stream is a Python-engine concern only).

        This test avoids checking the internal logic of the resolver and only validates the delegation and side-effects of `_init_time_travel_and_stream`.
        """
        # Arrange: ensure code path selects Spark Engine class
        monkeypatch.setattr("hsfs.engine._get_type", lambda: "spark")
        expected_fmt = "HUDI"
        fmt_mock = mocker.Mock(return_value=expected_fmt)
        stream_mock = mocker.Mock(return_value=True)
        monkeypatch.setattr(
            "hsfs.feature_group.FeatureGroup._resolve_time_travel_format",
            fmt_mock,
        )
        monkeypatch.setattr(
            "hsfs.feature_group.FeatureGroup._resolve_stream_python",
            stream_mock,
        )

        fg = get_test_feature_group()
        fg._stream = False  # Should remain unchanged in spark path

        # Act
        # Reset mocks to ignore any constructor-time calls
        fmt_mock.reset_mock()
        stream_mock.reset_mock()
        fg._init_time_travel_and_stream(
            stream=False,
            time_travel_format="HUDI",
        )

        # Assert: format set via resolver, stream resolver not used, _stream unchanged
        assert fg._time_travel_format == expected_fmt
        assert fmt_mock.call_count == 1
        stream_mock.assert_not_called()
        assert fg._stream is False

    @pytest.mark.parametrize(
        "time_travel_format,has_deltalake,expected,expected_exception",
        [
            (None, False, "NONE", None),
            (None, True, "NONE", None),
            ("NONE", False, "NONE", None),
            ("NONE", True, "NONE", None),
            ("HUDI", False, "HUDI", None),
            ("HUDI", True, "HUDI", None),
            ("DELTA", False, None, FeatureStoreException),
            ("DELTA", True, "DELTA", None),
        ],
    )
    def test_resolve_time_travel_format(
        self,
        monkeypatch,
        time_travel_format,
        has_deltalake,
        expected,
        expected_exception,
    ):
        monkeypatch.setattr(
            "hsfs.feature_group.FeatureGroup._has_deltalake", lambda: has_deltalake
        )
        if expected_exception:
            with pytest.raises(expected_exception):
                feature_group.FeatureGroup._resolve_time_travel_format(
                    time_travel_format=time_travel_format,
                )
        else:
            result = feature_group.FeatureGroup._resolve_time_travel_format(
                time_travel_format=time_travel_format,
            )
            assert result == expected

    @pytest.mark.parametrize(
        "time_travel_format,stream,expect_stream",
        [
            # DELTA not streams
            ("DELTA", False, False),
            # DELTA always streams when stream is True
            ("DELTA", True, True),
            # HUDI always streams
            ("HUDI", False, True),
            ("HUDI", True, True),
        ],
    )
    def test_resolve_stream_python(self, time_travel_format, stream, expect_stream):
        result = feature_group.FeatureGroup._resolve_stream_python(
            stream=stream,
            time_travel_format=time_travel_format,
        )
        assert result is expect_stream

    def test_embedding_index_forces_online_enabled(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")

        # Act
        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            embedding_index=hsfs.embedding.EmbeddingIndex(
                index_name="test_index",
                features=[hsfs.embedding.EmbeddingFeature("emb_feat", 128)],
            ),
            online_enabled=False,
            time_travel_format="DELTA",
        )

        # Assert
        assert fg.online_enabled is True
        assert fg.stream is False


class TestExternalFeatureGroup:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group"]["get"]["response"]

        # Act
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert isinstance(
            fg.data_source.storage_connector, storage_connector.StorageConnector
        )
        assert fg.data_source.query == "Select * from "
        assert fg.data_format == "HUDI"
        assert fg.data_source.path == "test_path"
        assert fg.options == {"test_name": "test_value"}
        assert fg.name == "external_fg_test"
        assert fg.version == 1
        assert fg.description == "test description"
        assert fg.primary_key == ["intt"]
        assert fg._feature_store_id == 67
        assert fg._feature_store_name == "test_project_featurestore"
        assert fg.created == "2022-08-16T07:19:12Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 14
        assert len(fg.columns) == 3
        assert isinstance(fg.columns[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://rpc.namenode.service.consul:8020/apps/hive/warehouse/test_project_featurestore.db/external_fg_test_1"
        )
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time == "datet"
        assert isinstance(fg.expectation_suite, expectation_suite.ExpectationSuite)

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group"]["get_list"]["response"]

        # Act
        fg_list = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert len(fg_list) == 1
        fg = fg_list[0]
        assert isinstance(
            fg.data_source.storage_connector, storage_connector.StorageConnector
        )
        assert fg.data_source.query == "Select * from "
        assert fg.data_format == "HUDI"
        assert fg.data_source.path == "test_path"
        assert fg.options == {"test_name": "test_value"}
        assert fg.name == "external_fg_test"
        assert fg.version == 1
        assert fg.description == "test description"
        assert fg.primary_key == ["intt"]
        assert fg._feature_store_id == 67
        assert fg._feature_store_name == "test_project_featurestore"
        assert fg.created == "2022-08-16T07:19:12Z"
        assert isinstance(fg.creator, user.User)
        assert fg.id == 14
        assert len(fg.columns) == 3
        assert isinstance(fg.columns[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://rpc.namenode.service.consul:8020/apps/hive/warehouse/test_project_featurestore.db/external_fg_test_1"
        )
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time == "datet"
        assert isinstance(fg.expectation_suite, expectation_suite.ExpectationSuite)

    def test_from_response_json_basic_info(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group"]["get_basic_info"]["response"]

        # Act
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert isinstance(
            fg.data_source.storage_connector, storage_connector.StorageConnector
        )
        assert fg.data_source.query is None
        assert fg.data_format is None
        assert fg.data_source.path is None
        assert fg.options is None
        assert fg.name is None
        assert fg.version is None
        assert fg.description is None
        assert fg.primary_key == []
        assert fg._feature_store_id is None
        assert fg._feature_store_name is None
        assert fg.created is None
        assert fg.creator is None
        assert fg.id == 15
        assert fg.columns == []
        assert fg.location is None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg.event_time is None
        assert fg.expectation_suite is None

    def test_feature_group_set_expectation_suite(
        self,
        mocker,
        backend_fixtures,
    ):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]
        es = expectation_suite.ExpectationSuite.from_response_json(json)
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)

        fg.expectation_suite = es

        assert fg.expectation_suite.id == es.id
        assert fg.expectation_suite._feature_group_id == fg.id
        assert fg.expectation_suite._feature_store_id == fg.feature_store_id

    @pytest.mark.skipif(
        not HAS_GREAT_EXPECTATIONS or GE_MAJOR != 0,
        reason=(
            "Fixture uses placeholder expectation_type='1' which GE 1.x rejects "
            "during ExpectationSuite construction. The save-from-Hopsworks-type "
            "variant covers the same SDK code path on both versions."
        ),
    )
    def test_feature_group_save_expectation_suite_from_ge_type(
        self, mocker, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]
        es = expectation_suite.ExpectationSuite.from_response_json(json)
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)

        # to mock delete we need get_expectation_suite to not return none
        mock_get_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi._get",
            return_value=es,
        )
        mock_create_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi._create",
            return_value=es,
        )
        mock_delete_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi._delete"
        )

        version_api = mocker.patch("hsfs.core.variable_api.VariableApi._get_version")
        version_api.return_value = hsfs.__version__

        mock_es_engine_get_expectation_suite_url = mocker.patch(
            "hsfs.core.expectation_suite_engine.ExpectationSuiteEngine._get_expectation_suite_url"
        )
        mock_print = mocker.patch("builtins.print")

        # Act
        fg.save_expectation_suite(es.to_ge_type(), overwrite=True)
        # Assert
        assert mock_delete_expectation_suite_api.call_count == 1
        assert mock_create_expectation_suite_api.call_count == 1
        assert mock_get_expectation_suite_api.call_count == 1
        assert mock_es_engine_get_expectation_suite_url.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0][:55]
            == "Attached expectation suite to Feature Group, edit it at"
        )

    def test_feature_group_save_expectation_suite_from_hopsworks_type(
        self, mocker, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["expectation_suite"]["get"]["response"]
        es = expectation_suite.ExpectationSuite.from_response_json(json)
        json = backend_fixtures["feature_group"]["get_stream_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)

        mock_update_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi._update"
        )

        version_api = mocker.patch("hsfs.core.variable_api.VariableApi._get_version")
        version_api.return_value = hsfs.__version__

        mock_es_engine_get_expectation_suite_url = mocker.patch(
            "hsfs.core.expectation_suite_engine.ExpectationSuiteEngine._get_expectation_suite_url"
        )
        mock_print = mocker.patch("builtins.print")

        # Act
        fg.save_expectation_suite(es)
        # Assert

        assert mock_update_expectation_suite_api.call_count == 1
        assert mock_es_engine_get_expectation_suite_url.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0][:63]
            == "Updated expectation suite attached to Feature Group, edit it at"
        )

    def test_from_response_json_transformation_functions(
        self, backend_fixtures, mocker
    ):
        # Arrange
        json = backend_fixtures["feature_group"]["get_transformations"]["response"]
        # Act
        fg = feature_group.FeatureGroup.from_response_json(json)

        # Assert
        assert fg.name == "fg_test"
        assert fg.version == 1
        assert fg._feature_store_id == 67
        assert fg.description == "test_description"
        assert fg.partition_key == []
        assert fg.primary_key == ["intt"]
        assert fg.hudi_precombine_key == "intt"
        assert fg._feature_store_name == "test_featurestore"
        assert fg.created == "2022-08-01T11:07:55Z"
        assert len(fg.transformation_functions) == 2
        assert (
            fg.transformation_functions[0].hopsworks_udf.function_name == "add_one_fs"
        )
        assert fg.transformation_functions[1].hopsworks_udf.function_name == "add_two"
        assert (
            fg.transformation_functions[0].hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )
        assert (
            fg.transformation_functions[1].hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_two(data1 : pd.Series):\n    return data1 + 2\n"
        )
        assert (
            fg.transformation_functions[0].transformation_type
            == TransformationType.ON_DEMAND
        )
        assert (
            fg.transformation_functions[1].transformation_type
            == TransformationType.ON_DEMAND
        )
        assert isinstance(fg.creator, user.User)
        assert fg.id == 15
        assert len(fg.columns) == 2
        assert isinstance(fg.columns[0], feature.Feature)
        assert (
            fg.location
            == "hopsfs://10.0.2.15:8020/apps/hive/warehouse/test_featurestore.db/fg_test_1"
        )
        assert fg.online_enabled is True
        assert fg.time_travel_format == "HUDI"
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name == "119_15_fg_test_1_onlinefs"
        assert fg.event_time is None
        assert fg.stream is False
        assert (
            fg.expectation_suite.expectation_suite_name == "test_expectation_suite_name"
        )

    def test_prepare_spark_location(self, mocker, backend_fixtures):
        # Arrange
        engine = spark.Engine()
        engine_instance = mocker.patch("hsfs.engine._get_instance", return_value=engine)
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)
        fg._location = f"{fg.name}_{fg.version}"

        # Act
        path = fg.prepare_spark_location()

        # Assert
        assert fg.location == path
        engine_instance.assert_not_called()

    def test_prepare_spark_location_with_s3_connector(self, mocker, backend_fixtures):
        # Arrange
        engine = spark.Engine()
        engine_instance = mocker.patch("hsfs.engine._get_instance", return_value=engine)
        refetch_api = mocker.patch("hsfs.storage_connector.S3Connector._refetch")
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)
        fg._location = f"{fg.name}_{fg.version}"
        fg._data_source._storage_connector = storage_connector.S3Connector(
            id=1, name="s3_conn", featurestore_id=fg.feature_store_id
        )

        # Act
        path = fg.prepare_spark_location()

        # Assert
        assert fg.location == path
        engine_instance.assert_called_once()
        refetch_api.assert_called_once()

    def test_prepare_spark_location_with_s3_connector_python(
        self, mocker, backend_fixtures
    ):
        # Arrange
        engine = python.Engine()
        engine_instance = mocker.patch("hsfs.engine._get_instance", return_value=engine)
        mocker.patch("hsfs.storage_connector.S3Connector._refetch")
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)
        fg._location = f"{fg.name}_{fg.version}"
        fg._data_source._storage_connector = storage_connector.S3Connector(
            id=1, name="s3_conn", featurestore_id=fg.feature_store_id
        )

        # Act
        with pytest.raises(AttributeError):
            fg.prepare_spark_location()

        # Assert
        engine_instance.assert_called_once()

    def test_upper_case_primary_key_event_time(self, mocker, backend_fixtures, caplog):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type")
        json = backend_fixtures["feature_store"]["get"]["response"]

        features = [
            feature.Feature(name="PrimaryKey", type="int"),
            feature.Feature(name="Event_Time", type="timestamp"),
            feature.Feature(name="feat", type="int"),
        ]

        # Act
        fs = feature_store.FeatureStore.from_response_json(json)
        with warnings.catch_warnings(record=True) as warning_record:
            new_fg = fs.create_feature_group(
                name="fg_name",
                version=1,
                description="fg_description",
                event_time="Event_Time",
                primary_key=["PrimaryKey"],
                features=features,
            )

        assert len(warning_record) == 2
        assert (
            str(warning_record[0].message)
            == "The feature name `Event_Time` contains upper case letters. Feature names are sanitized to lower case in the feature store."
        )
        assert (
            str(warning_record[1].message)
            == "The feature name `PrimaryKey` contains upper case letters. Feature names are sanitized to lower case in the feature store."
        )

        # Assert
        assert new_fg.event_time == "event_time"
        assert new_fg.primary_key == ["primarykey"]
        assert new_fg.columns[0].name == "primarykey"
        assert new_fg.columns[1].name == "event_time"
        assert new_fg.columns[2].name == "feat"


class TestFeatureGroupExecuteOdts:
    @pytest.fixture(autouse=True)
    def mock_has_deltalake(self, mocker):
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )

    def test_execute_odts_with_transformations(self, mocker):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=["pk"],
            foreign_key=[],
            partition_key=[],
            transformation_functions=[odt("col1")],
        )

        mock_apply = mocker.patch.object(
            fg._feature_group_engine,
            "_apply_on_demand_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline)
        df_test_data = pd.DataFrame({"col1": [1, 2, 3]})
        result_df = fg.execute_odts(data=df_test_data, online=False)

        mock_apply.assert_called_with(
            execution_graph=fg._transformation_function_execution_dag,
            data=df_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online)
        dict_test_data = {"col1": 1}
        result_dict = fg.execute_odts(data=dict_test_data, online=True)

        mock_apply.assert_called_with(
            execution_graph=fg._transformation_function_execution_dag,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )
        assert result_dict == dict_test_data

    def test_chained_odt_kept_intermediate_does_not_warn_ambiguous(self, mocker):
        # Kept chained-ODT outputs land in the feature group schema as
        # on-demand features. They are not raw features, so consuming one in a
        # downstream transformation must NOT trip the raw-name-ambiguity
        # warning. The ambiguity check only considers raw feature names.
        import warnings

        from hsfs.feature import Feature
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(float)
        def monthly_to_quarterly(mrr_usd):
            return mrr_usd * 3

        @udf(float)
        def quarterly_to_annual(qrr_usd):
            return qrr_usd * 4

        def odt(hopsworks_udf):
            return TransformationFunction(
                featurestore_id=99,
                hopsworks_udf=hopsworks_udf,
                transformation_type=TransformationType.ON_DEMAND,
            )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            fg = feature_group.FeatureGroup(
                name="subscription_metrics",
                version=1,
                featurestore_id=99,
                primary_key=["account_id"],
                foreign_key=[],
                partition_key=[],
                transformation_functions=[
                    odt(monthly_to_quarterly("mrr_usd").alias("qrr_usd")),
                    odt(quarterly_to_annual("qrr_usd").alias("arr_usd")),
                ],
                features=[
                    Feature("account_id"),
                    Feature("mrr_usd"),
                    Feature("qrr_usd", on_demand=True),
                    Feature("arr_usd", on_demand=True),
                ],
            )

        ambiguity_warnings = [
            str(w.message)
            for w in caught
            if "both a feature of this entity" in str(w.message)
        ]
        assert not ambiguity_warnings, (
            f"kept chained on-demand outputs must not warn: {ambiguity_warnings}"
        )
        # The chain is still wired: quarterly_to_annual depends on the
        # monthly_to_quarterly output.
        dag = fg._transformation_function_execution_dag
        output_order = [tf.hopsworks_udf.output_column_names for tf in dag.nodes]
        assert output_order.index(["qrr_usd"]) < output_order.index(["arr_usd"])

    def test_chained_odt_raw_feature_name_collision_still_warns(self, mocker):
        # The complement of the case above: when a *raw* (non-on-demand)
        # feature genuinely shares a name with another transformation's output,
        # the ambiguity warning must still fire. Narrowing the schema set to
        # raw features does not silence a real collision.
        import warnings

        from hsfs.feature import Feature
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def normalize(amount):
            return amount + 1

        @udf(int)
        def scale(currency):
            return currency + 1

        def odt(hopsworks_udf):
            return TransformationFunction(
                featurestore_id=99,
                hopsworks_udf=hopsworks_udf,
                transformation_type=TransformationType.ON_DEMAND,
            )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            feature_group.FeatureGroup(
                name="collision_fg",
                version=1,
                featurestore_id=99,
                primary_key=["pk"],
                foreign_key=[],
                partition_key=[],
                transformation_functions=[
                    odt(normalize("amount").alias("currency")),
                    odt(scale("currency")),
                ],
                # `currency` is a genuine raw feature here (on_demand=False),
                # so consuming it after another TF overwrote the name is the
                # real migration hazard the warning exists for.
                features=[
                    Feature("pk"),
                    Feature("amount"),
                    Feature("currency"),
                ],
            )

        assert any("both a feature of this entity" in str(w.message) for w in caught), (
            "a raw feature colliding with a transformation output must still warn"
        )

    def test_execute_odts_with_transformation_context_and_request_parameters(
        self, mocker
    ):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def add_context_value(feature, context):
            return feature + context["value"]

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_context_value,
            transformation_type=TransformationType.ON_DEMAND,
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=["pk"],
            foreign_key=[],
            partition_key=[],
            transformation_functions=[odt("col1")],
        )

        context = {"value": 10}
        request_params = {"param1": "value1"}

        mock_apply = mocker.patch.object(
            fg._feature_group_engine,
            "_apply_on_demand_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline) and transformation_context
        df_test_data = pd.DataFrame({"col1": [1, 2, 3]})
        result_df = fg.execute_odts(
            data=df_test_data, online=False, transformation_context=context
        )

        mock_apply.assert_called_with(
            execution_graph=fg._transformation_function_execution_dag,
            data=df_test_data,
            online=False,
            transformation_context=context,
            request_parameters=None,
            n_processes=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online) and request_parameters
        dict_test_data = {"col1": 1}
        result_dict = fg.execute_odts(
            data=dict_test_data, online=True, request_parameters=request_params
        )

        mock_apply.assert_called_with(
            execution_graph=fg._transformation_function_execution_dag,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=request_params,
            n_processes=None,
        )
        assert result_dict == dict_test_data

    def test_execute_odts_no_transformations(self, mocker, caplog):
        import logging

        import pandas as pd

        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=["pk"],
            foreign_key=[],
            partition_key=[],
            transformation_functions=[],
        )

        test_data = pd.DataFrame({"col1": [1, 2, 3]})

        mock_apply = mocker.patch.object(
            fg._feature_group_engine,
            "_apply_on_demand_transformations",
        )

        # Act
        with caplog.at_level(logging.INFO):
            result = fg.execute_odts(data=test_data, online=False)

        # Assert
        mock_apply.assert_not_called()
        assert (
            "No on-demand transformation functions attached to the feature group"
            in caplog.text
        )
        pd.testing.assert_frame_equal(result, test_data)

    @pytest.mark.parametrize("execution_mode", ["python", "pandas", "default"])
    def test_execute_odts_execution_modes(self, mocker, execution_mode):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int, mode=execution_mode)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=["pk"],
            foreign_key=[],
            partition_key=[],
            transformation_functions=[odt("col1")],
        )

        if execution_mode == "default":
            online_test_data = {"col1": 1}
            offline_test_data = pd.DataFrame({"col1": [1, 2, 3]})
        elif execution_mode == "python":
            online_test_data = offline_test_data = {"col1": 1}
        elif execution_mode == "pandas":
            online_test_data = offline_test_data = pd.DataFrame({"col1": [1, 2, 3]})

        mock_apply = mocker.patch.object(
            fg._feature_group_engine,
            "_apply_on_demand_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Act - online
        fg.execute_odts(data=online_test_data, online=True)

        # Assert - online
        mock_apply.assert_called_with(
            execution_graph=fg._transformation_function_execution_dag,
            data=online_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )

        # Act - offline
        fg.execute_odts(data=offline_test_data, online=False)

        # Assert - offline
        mock_apply.assert_called_with(
            execution_graph=fg._transformation_function_execution_dag,
            data=offline_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )


class TestFeatureGroupRead:
    @pytest.fixture(autouse=True)
    def mock_has_deltalake(self, mocker):
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )

    def test_read_with_start_time_no_event_time_raises(self):
        # Arrange
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=1,
            featurestore_name="fs",
            features=[feature.Feature("pk", primary=True)],
            primary_key=["pk"],
            partition_key=[],
            event_time=None,
        )

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="no event_time column"):
            fg.read(start_time="2024-01-01")

    def test_read_with_end_time_no_event_time_raises(self):
        # Arrange
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=1,
            featurestore_name="fs",
            features=[feature.Feature("pk", primary=True)],
            primary_key=["pk"],
            partition_key=[],
            event_time=None,
        )

        # Act & Assert
        with pytest.raises(FeatureStoreException, match="no event_time column"):
            fg.read(end_time="2024-01-31")

    def test_read_wallclock_time_with_start_time_raises(self):
        # Arrange
        fg = get_test_feature_group()

        # Act & Assert
        with (
            mock.patch("hsfs.engine._get_type", return_value="spark"),
            pytest.raises(
                FeatureStoreException, match="Cannot use wallclock_time together"
            ),
        ):
            fg.read(wallclock_time="2024-01-01", start_time="2024-01-01")

    def test_read_wallclock_time_with_end_time_raises(self):
        # Arrange
        fg = get_test_feature_group()

        # Act & Assert
        with (
            mock.patch("hsfs.engine._get_type", return_value="spark"),
            pytest.raises(
                FeatureStoreException, match="Cannot use wallclock_time together"
            ),
        ):
            fg.read(wallclock_time="2024-01-01", end_time="2024-01-31")

    def test_read_no_event_time_ignores_scheduler_env_vars(self, mocker):
        # The scheduler always injects HOPS_START_TIME / HOPS_END_TIME, including for jobs
        # whose feature groups have no event_time column.
        # Promoting those env vars into start/end args would then trip the no-event_time
        # guard on a read() the caller invoked with no time args at all — which is what
        # broke the fraud_online tutorial pipeline under the scheduler.
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=1,
            featurestore_name="fs",
            features=[feature.Feature("pk", primary=True)],
            primary_key=["pk"],
            partition_key=[],
            event_time=None,
        )
        fake_query = mock.MagicMock()
        mocker.patch.object(fg, "select_all", return_value=fake_query)
        mocker.patch("hsfs.engine._get_instance")
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            fg.read()

        # No filter() should have been applied — the env-injected window must not bleed in.
        fake_query.filter.assert_not_called()
        fake_query.read.assert_called_once()

    def test_read_explicit_start_time_no_event_time_still_raises_under_scheduler(self):
        # Even when scheduler env vars are set, an *explicit* time arg from the caller on
        # a no-event_time FG is genuine user error and must still raise. Only the env-var
        # fallback is silenced for no-event_time FGs.
        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=1,
            featurestore_name="fs",
            features=[feature.Feature("pk", primary=True)],
            primary_key=["pk"],
            partition_key=[],
            event_time=None,
        )
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }
        with (
            mock.patch.dict("os.environ", env, clear=False),
            pytest.raises(FeatureStoreException, match="no event_time column"),
        ):
            fg.read(start_time="2024-01-01")


class TestExternalFeatureGroupRead:
    def test_read_with_start_time_no_event_time_raises(self, backend_fixtures):
        # Arrange
        with mock.patch("hsfs.engine._get_type", return_value="spark"):
            json = backend_fixtures["external_feature_group"]["get"]["response"]
            fg = feature_group.ExternalFeatureGroup.from_response_json(json)
            fg._event_time = None

            # Act & Assert
            with pytest.raises(FeatureStoreException, match="no event_time column"):
                fg.read(start_time="2024-01-01")

    def test_read_with_end_time_no_event_time_raises(self, backend_fixtures):
        # Arrange
        with mock.patch("hsfs.engine._get_type", return_value="spark"):
            json = backend_fixtures["external_feature_group"]["get"]["response"]
            fg = feature_group.ExternalFeatureGroup.from_response_json(json)
            fg._event_time = None

            # Act & Assert
            with pytest.raises(FeatureStoreException, match="no event_time column"):
                fg.read(end_time="2024-01-31")

    def test_read_no_event_time_ignores_scheduler_env_vars(
        self, mocker, backend_fixtures
    ):
        # Same scheduler-injection issue as for FeatureGroup: env-injected HOPS_* vars
        # must not be promoted into args on no-event_time FGs.
        json = backend_fixtures["external_feature_group"]["get"]["response"]
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)
        fg._event_time = None
        fake_query = mock.MagicMock()
        mocker.patch.object(fg, "select_all", return_value=fake_query)
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        mocker.patch("hsfs.engine._get_instance")
        env = {
            "HOPS_START_TIME": "2026-01-01T00:00:00Z",
            "HOPS_END_TIME": "2026-02-01T00:00:00Z",
        }

        with mock.patch.dict("os.environ", env, clear=False):
            fg.read()

        fake_query.filter.assert_not_called()
        fake_query.read.assert_called_once()


def _fg_with_partitioned_by(
    partitioned_by=None,
    partition_key=None,
    event_time="ts",
    online_partition_columns=False,
    time_travel_format="DELTA",
    primary_key=None,
    zorder_by=None,
    clustered_by=None,
    bucket_index=None,
    sort_order=None,
    stream=False,
):
    return feature_group.FeatureGroup(
        name="test",
        version=1,
        description="d",
        online_enabled=False,
        time_travel_format=time_travel_format,
        partition_key=partition_key or [],
        primary_key=primary_key if primary_key is not None else ["pk"],
        foreign_key=[],
        hudi_precombine_key=None,
        featurestore_id=1,
        featurestore_name="fs",
        features=[],
        statistics_config={},
        event_time=event_time,
        stream=stream,
        expectation_suite=None,
        partitioned_by=partitioned_by,
        online_partition_columns=online_partition_columns,
        zorder_by=zorder_by,
        clustered_by=clustered_by,
        bucket_index=bucket_index,
        sort_order=sort_order,
    )


class TestFeatureGroupPartitionedBy:
    @pytest.fixture(autouse=True)
    def mock_offline_format_libraries(self, mocker):
        # The helper builds DELTA/ICEBERG feature groups; stub the library
        # probes so these metadata/property tests run where deltalake or
        # pyiceberg is not installed (e.g. the Windows CI runner).
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_pyiceberg", return_value=True
        )

    def test_partitioned_by_default_none(self):
        fg = _fg_with_partitioned_by()
        assert fg.partitioned_by is None
        assert fg.online_partition_columns is False

    def test_partitioned_by_round_trips_canonicalized(self):
        fg = _fg_with_partitioned_by(
            partitioned_by=["DAY( ts )", "bucket( 16 , pk )"],
            time_travel_format="ICEBERG",
        )
        assert fg.partitioned_by == ["day(ts)", "bucket(16,pk)"]

    def test_online_partition_columns_round_trips(self):
        fg = _fg_with_partitioned_by(
            partitioned_by=["day(ts)"],
            online_partition_columns=True,
            time_travel_format="HUDI",
        )
        assert fg.online_partition_columns is True

    def test_partitioned_by_in_to_dict(self):
        fg = _fg_with_partitioned_by(
            partitioned_by=["year(ts)", "month(ts)"], time_travel_format="HUDI"
        )
        d = fg.to_dict()
        assert d["partitionedBy"] == ["year(ts)", "month(ts)"]
        assert d["onlinePartitionColumns"] is False

    def test_partitioned_by_and_partition_key_raises(self):
        with pytest.raises(FeatureStoreException, match="Set either partition_key"):
            _fg_with_partitioned_by(
                partitioned_by=["year(ts)"], partition_key=["other_col"]
            )

    def test_partitioned_by_rejects_old_grain_form(self):
        with pytest.raises(FeatureStoreException, match="removed grain form") as e:
            _fg_with_partitioned_by(partitioned_by=["year", "month"])
        # the error points at the transform form on the event_time column
        assert "year(event_time_col)" in str(e.value)

    def test_partitioned_by_rejects_unknown_transform(self):
        with pytest.raises(FeatureStoreException, match="Unknown partition transform"):
            _fg_with_partitioned_by(partitioned_by=["minute(ts)"])

    def test_partitioned_by_rejects_duplicates(self):
        with pytest.raises(FeatureStoreException, match="duplicate transform"):
            _fg_with_partitioned_by(partitioned_by=["day(ts)", "day(ts)"])

    def test_validate_partitioned_by_rejects_empty_list(self):
        # The validator rejects []; the constructor normalizes [] to None
        # before validating, so the check is unit-tested directly.
        with pytest.raises(FeatureStoreException, match="non-empty list"):
            feature_group._validate_partitioned_by([], None, "ts", "DELTA", ["pk"])

    def test_partitioned_by_requires_supported_format(self):
        with pytest.raises(FeatureStoreException, match="requires time_travel_format"):
            _fg_with_partitioned_by(partitioned_by=["day(ts)"], time_travel_format=None)

    def test_partitioned_by_iceberg_needs_no_event_time(self):
        # transforms name their source column explicitly; only HUDI temporal
        # grains still require event_time
        fg = _fg_with_partitioned_by(
            partitioned_by=["day(ts)"], event_time=None, time_travel_format="ICEBERG"
        )
        assert fg.partitioned_by == ["day(ts)"]

    def test_partitioned_by_event_time_named_like_grain_allowed(self):
        # the old grain-name/event_time collision check is gone: an event_time
        # column named "year" is a valid transform source
        fg = _fg_with_partitioned_by(
            partitioned_by=["year(year)"],
            event_time="year",
            time_travel_format="HUDI",
        )
        assert fg.partitioned_by == ["year(year)"]

    def test_partitioned_by_iceberg_rejects_week(self):
        with pytest.raises(FeatureStoreException, match="not supported for"):
            _fg_with_partitioned_by(
                partitioned_by=["week(ts)"], time_travel_format="ICEBERG"
            )

    def test_partitioned_by_iceberg_rejects_redundant_temporal(self):
        with pytest.raises(FeatureStoreException, match="at most one temporal"):
            _fg_with_partitioned_by(
                partitioned_by=["year(ts)", "day(ts)"], time_travel_format="ICEBERG"
            )

    def test_partitioned_by_rejected_on_delta(self):
        # Delta has no partition transforms; liquid clustering goes through
        # clustered_by and identity partitions through partition_key
        with pytest.raises(FeatureStoreException, match="not supported on DELTA") as e:
            _fg_with_partitioned_by(partitioned_by=["day(ts)"])
        assert "clustered_by" in str(e.value)

    def test_partitioned_by_hudi_temporal_requires_event_time(self):
        with pytest.raises(FeatureStoreException, match="requires event_time"):
            _fg_with_partitioned_by(
                partitioned_by=["day(ts)"],
                event_time=None,
                time_travel_format="HUDI",
            )

    def test_partitioned_by_hudi_temporal_source_must_be_event_time(self):
        with pytest.raises(FeatureStoreException, match="event_time column"):
            _fg_with_partitioned_by(
                partitioned_by=["day(created)"], time_travel_format="HUDI"
            )

    def test_partitioned_by_hudi_rejects_bucket_transform(self):
        # the Hudi bucket index goes through bucket_index, not a transform
        with pytest.raises(
            FeatureStoreException, match="not supported for time_travel_format=HUDI"
        ):
            _fg_with_partitioned_by(
                partitioned_by=["bucket(4, pk)"], time_travel_format="HUDI"
            )


class TestFeatureGroupZorderBy:
    @pytest.fixture(autouse=True)
    def mock_offline_format_libraries(self, mocker):
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_pyiceberg", return_value=True
        )

    def test_zorder_by_default_none(self):
        fg = _fg_with_partitioned_by(time_travel_format="ICEBERG")
        assert fg.zorder_by is None
        assert "zorderBy" not in fg.to_dict()

    def test_zorder_by_round_trips_iceberg(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="ICEBERG", zorder_by=["merchant_id", "amount"]
        )
        assert fg.zorder_by == ["merchant_id", "amount"]
        assert fg.to_dict()["zorderBy"] == ["merchant_id", "amount"]

    def test_zorder_by_valid_on_hudi(self):
        fg = _fg_with_partitioned_by(time_travel_format="HUDI", zorder_by=["amount"])
        assert fg.zorder_by == ["amount"]

    def test_zorder_by_composes_with_partitioned_by(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="ICEBERG",
            partitioned_by=["day(ts)"],
            zorder_by=["amount"],
        )
        assert fg.partitioned_by == ["day(ts)"]
        assert fg.zorder_by == ["amount"]

    def test_zorder_by_rejected_on_delta(self):
        with pytest.raises(FeatureStoreException, match="liquid clustering"):
            _fg_with_partitioned_by(time_travel_format="DELTA", zorder_by=["amount"])

    def test_zorder_by_rejected_without_time_travel_format(self):
        with pytest.raises(FeatureStoreException, match="ICEBERG or HUDI"):
            _fg_with_partitioned_by(time_travel_format=None, zorder_by=["amount"])

    def test_zorder_by_rejects_duplicates(self):
        with pytest.raises(FeatureStoreException, match="duplicate"):
            _fg_with_partitioned_by(
                time_travel_format="ICEBERG", zorder_by=["amount", "amount"]
            )

    def test_zorder_by_caps_columns(self):
        with pytest.raises(FeatureStoreException, match="at most"):
            _fg_with_partitioned_by(
                time_travel_format="ICEBERG", zorder_by=["a", "b", "c", "d", "e"]
            )

    def test_validate_zorder_by_rejects_empty_list(self):
        # The validator rejects []; the constructor normalizes [] to None
        # before validating, so the check is unit-tested directly.
        with pytest.raises(FeatureStoreException, match="non-empty list"):
            feature_group._validate_zorder_by([], "ICEBERG")

    def test_sort_order_round_trips_canonical(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="ICEBERG", sort_order=["amount desc"]
        )
        assert fg.sort_order == ["amount desc nulls last"]
        assert fg.to_dict()["sortOrder"] == ["amount desc nulls last"]

    def test_sort_order_and_zorder_by_mutually_exclusive(self):
        # The two prescribe conflicting file layouts: writes would sort
        # linearly while optimize() rewrites on the z-curve.
        with pytest.raises(FeatureStoreException, match="not both"):
            _fg_with_partitioned_by(
                time_travel_format="ICEBERG",
                sort_order=["amount desc"],
                zorder_by=["merchant_id"],
            )

    def test_zorder_by_canonicalized_to_lower_case(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="ICEBERG", zorder_by=["Merchant_ID", "Amount"]
        )
        assert fg.zorder_by == ["merchant_id", "amount"]

    def test_zorder_by_rejects_case_insensitive_duplicates(self):
        with pytest.raises(FeatureStoreException, match="duplicate"):
            _fg_with_partitioned_by(
                time_travel_format="ICEBERG", zorder_by=["id", "ID"]
            )

    def test_optimize_delegates_to_engine(self, mocker):
        fg = _fg_with_partitioned_by(time_travel_format="ICEBERG", zorder_by=["amount"])
        fg._feature_group_engine = mocker.Mock()
        fg.optimize()
        fg._feature_group_engine._optimize.assert_called_once_with(
            fg,
            full=False,
            strategy=None,
            columns=None,
            rewrite_all=None,
            target_file_size_mb=None,
            where=None,
        )

    def test_optimize_full_passes_through(self, mocker):
        fg = _fg_with_partitioned_by(time_travel_format="ICEBERG")
        fg._feature_group_engine = mocker.Mock()
        fg.optimize(full=True)
        fg._feature_group_engine._optimize.assert_called_once_with(
            fg,
            full=True,
            strategy=None,
            columns=None,
            rewrite_all=None,
            target_file_size_mb=None,
            where=None,
        )

    def test_update_partition_spec_delegates_to_engine(self, mocker):
        fg = _fg_with_partitioned_by(
            time_travel_format="ICEBERG", partitioned_by=["day(ts)"]
        )
        fg._feature_group_engine = mocker.Mock()
        result = fg.update_partition_spec(add=["hour(ts)"], remove=["day(ts)"])
        fg._feature_group_engine._update_partition_spec.assert_called_once_with(
            fg, ["hour(ts)"], ["day(ts)"]
        )
        assert result is fg

    def test_update_partition_spec_no_args_delegates_reconcile(self, mocker):
        # a no-arg call is reconcile mode: the engine re-syncs the stored
        # metadata from the table without changing the spec
        fg = _fg_with_partitioned_by(
            time_travel_format="ICEBERG", partitioned_by=["day(ts)"]
        )
        fg._feature_group_engine = mocker.Mock()
        result = fg.update_partition_spec()
        fg._feature_group_engine._update_partition_spec.assert_called_once_with(
            fg, None, None
        )
        assert result is fg

    @pytest.mark.parametrize(
        "getter", ["get_partition_spec", "get_partition_specs", "get_sort_order"]
    )
    def test_iceberg_layout_getters_reject_other_formats(self, mocker, getter):
        fg = _fg_with_partitioned_by(time_travel_format="HUDI")
        fg._feature_group_engine = mocker.Mock()
        with pytest.raises(
            FeatureStoreException, match="requires a ICEBERG feature group"
        ) as e:
            getattr(fg, getter)()
        # the format-independent alternative is pointed out
        assert "describe_layout()" in str(e.value)
        fg._feature_group_engine._describe_layout.assert_not_called()

    def test_get_partition_spec_reads_layout(self, mocker):
        fg = _fg_with_partitioned_by(time_travel_format="ICEBERG")
        fg._feature_group_engine = mocker.Mock()
        fg._feature_group_engine._describe_layout.return_value = {
            "partition_spec": ["ts_day: day(ts)"]
        }
        assert fg.get_partition_spec() == ["ts_day: day(ts)"]

    def test_get_clustering_columns_rejects_non_delta(self, mocker):
        fg = _fg_with_partitioned_by(time_travel_format="ICEBERG")
        fg._feature_group_engine = mocker.Mock()
        with pytest.raises(
            FeatureStoreException, match="requires a DELTA feature group"
        ) as e:
            fg.get_clustering_columns()
        assert "describe_layout()" in str(e.value)
        fg._feature_group_engine._describe_layout.assert_not_called()

    def test_get_clustering_columns_reads_layout(self, mocker):
        fg = _fg_with_partitioned_by(time_travel_format="DELTA")
        fg._feature_group_engine = mocker.Mock()
        fg._feature_group_engine._describe_layout.return_value = {
            "clustering_columns": ["ts", "id"]
        }
        assert fg.get_clustering_columns() == ["ts", "id"]


class TestFeatureGroupClusteredBy:
    @pytest.fixture(autouse=True)
    def mock_offline_format_libraries(self, mocker):
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_pyiceberg", return_value=True
        )

    def test_clustered_by_default_none(self):
        fg = _fg_with_partitioned_by()
        assert fg.clustered_by is None
        assert "clusteredBy" not in fg.to_dict()

    def test_clustered_by_round_trips_delta(self):
        # stream=True routes python-engine writes through the Spark
        # materialization job, satisfying the Spark-writers requirement
        fg = _fg_with_partitioned_by(
            clustered_by=["merchant_id", "amount"], stream=True
        )
        assert fg.clustered_by == ["merchant_id", "amount"]
        assert fg.to_dict()["clusteredBy"] == ["merchant_id", "amount"]

    def test_clustered_by_property_returns_copy(self):
        fg = _fg_with_partitioned_by(clustered_by=["amount"], stream=True)
        fg.clustered_by.append("mutated")
        assert fg.clustered_by == ["amount"]

    def test_clustered_by_empty_list_sentinel_serializes(self):
        # [] is the clear sentinel written by the layout evolution APIs; it
        # must serialize (unlike None) so the backend clears the stored spec
        fg = _fg_with_partitioned_by(clustered_by=["amount"], stream=True)
        fg._clustered_by = []
        assert fg.to_dict()["clusteredBy"] == []
        assert fg.clustered_by is None

    @pytest.mark.parametrize("fmt", ["ICEBERG", "HUDI", None])
    def test_clustered_by_requires_delta(self, fmt):
        with pytest.raises(
            FeatureStoreException, match="requires\\s+time_travel_format DELTA"
        ):
            _fg_with_partitioned_by(
                clustered_by=["amount"], time_travel_format=fmt, stream=True
            )

    def test_clustered_by_rejects_duplicates(self):
        with pytest.raises(FeatureStoreException, match="duplicate"):
            _fg_with_partitioned_by(clustered_by=["amount", "amount"], stream=True)

    def test_clustered_by_caps_columns(self):
        with pytest.raises(FeatureStoreException, match="at most"):
            _fg_with_partitioned_by(clustered_by=["a", "b", "c", "d", "e"], stream=True)

    def test_validate_clustered_by_rejects_empty_list(self):
        # The validator rejects []; the constructor normalizes [] to None
        # before validating, so the check is unit-tested directly.
        with pytest.raises(FeatureStoreException, match="non-empty list"):
            feature_group._validate_clustered_by([], "DELTA", None)

    def test_clustered_by_and_partition_key_mutually_exclusive(self):
        with pytest.raises(
            FeatureStoreException, match="Set either partition_key or clustered_by"
        ):
            _fg_with_partitioned_by(
                clustered_by=["amount"], partition_key=["region"], stream=True
            )

    def test_clustered_by_python_engine_requires_stream(self, mocker):
        # delta-rs cannot write the Clustering/DomainMetadata table features,
        # so a python-engine clustered feature group must use stream=True
        mocker.patch("hsfs.engine._get_type", return_value="python")
        with pytest.raises(
            FeatureStoreException, match="clustered_by requires Spark writers"
        ):
            _fg_with_partitioned_by(clustered_by=["amount"], stream=False)

    def test_clustered_by_python_engine_stream_true_allowed(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="python")
        fg = _fg_with_partitioned_by(clustered_by=["amount"], stream=True)
        assert fg.clustered_by == ["amount"]

    def test_clustered_by_spark_engine_allowed_without_stream(self, mocker):
        mocker.patch("hsfs.engine._get_type", return_value="spark")
        fg = _fg_with_partitioned_by(clustered_by=["amount"], stream=False)
        assert fg.clustered_by == ["amount"]

    def test_zorder_by_rejected_on_delta_points_at_clustered_by(self):
        with pytest.raises(FeatureStoreException, match="Use clustered_by"):
            _fg_with_partitioned_by(zorder_by=["amount"])

    def test_update_clustering_delegates_to_engine(self, mocker):
        fg = _fg_with_partitioned_by(clustered_by=["amount"], stream=True)
        fg._feature_group_engine = mocker.Mock()
        result = fg.update_clustering(["ts", "amount"])
        fg._feature_group_engine._update_clustering.assert_called_once_with(
            fg, ["ts", "amount"]
        )
        assert result is fg

    def test_disable_clustering_delegates_to_engine(self, mocker):
        fg = _fg_with_partitioned_by(clustered_by=["amount"], stream=True)
        fg._feature_group_engine = mocker.Mock()
        result = fg.disable_clustering()
        fg._feature_group_engine._update_clustering.assert_called_once_with(fg, None)
        assert result is fg


class TestFeatureGroupBucketIndex:
    @pytest.fixture(autouse=True)
    def mock_offline_format_libraries(self, mocker):
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_deltalake", return_value=True
        )
        mocker.patch(
            "hsfs.feature_group.FeatureGroup._has_pyiceberg", return_value=True
        )

    def test_bucket_index_default_none(self):
        fg = _fg_with_partitioned_by(time_travel_format="HUDI")
        assert fg.bucket_index is None
        assert "bucketIndex" not in fg.to_dict()

    def test_bucket_index_round_trips_hudi(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="HUDI",
            bucket_index={"field": "pk", "num_buckets": 16},
        )
        assert fg.bucket_index == {"field": "pk", "num_buckets": 16}
        assert fg.to_dict()["bucketIndex"] == {"field": "pk", "numBuckets": 16}

    def test_bucket_index_property_returns_copy(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="HUDI",
            bucket_index={"field": "pk", "num_buckets": 16},
        )
        fg.bucket_index["num_buckets"] = 99
        assert fg.bucket_index == {"field": "pk", "num_buckets": 16}

    def test_bucket_index_field_canonicalized_to_lower_case(self):
        # Feature names are sanitized to lower case; the field must match the
        # stored primary key ("pk") even when the caller spells it "PK".
        fg = _fg_with_partitioned_by(
            time_travel_format="HUDI",
            bucket_index={"field": "PK", "num_buckets": 16},
        )
        assert fg.bucket_index["field"] == "pk"

    @pytest.mark.parametrize(
        "bucket_index",
        [
            {"field": "pk"},
            {"num_buckets": 16},
            {"field": "pk", "num_buckets": 16, "extra": 1},
            {},
        ],
    )
    def test_bucket_index_requires_exact_keys(self, bucket_index):
        with pytest.raises(FeatureStoreException, match="must be a dict with"):
            _fg_with_partitioned_by(
                time_travel_format="HUDI", bucket_index=bucket_index
            )

    def test_validate_bucket_index_rejects_non_dict(self):
        with pytest.raises(FeatureStoreException, match="must be a dict with"):
            feature_group._validate_bucket_index(["pk", 16], "HUDI", ["pk"])

    def test_bucket_index_simple_engine_allowed(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="HUDI",
            bucket_index={"field": "pk", "num_buckets": 16, "engine": "simple"},
        )
        assert fg.bucket_index == {
            "field": "pk",
            "num_buckets": 16,
            "engine": "simple",
        }

    def test_bucket_index_rejects_consistent_hashing_engine(self):
        # consistent hashing needs a merge-on-read table with a clustering
        # lifecycle, which copy-on-write Hudi feature groups do not have
        with pytest.raises(FeatureStoreException, match="merge-on-read") as e:
            _fg_with_partitioned_by(
                time_travel_format="HUDI",
                bucket_index={
                    "field": "pk",
                    "num_buckets": 16,
                    "engine": "consistent_hashing",
                },
            )
        assert "'engine' must be 'simple'" in str(e.value)

    @pytest.mark.parametrize("num_buckets", [0, -1, "16", 1.5])
    def test_bucket_index_num_buckets_must_be_positive_int(self, num_buckets):
        with pytest.raises(FeatureStoreException, match="positive integer"):
            _fg_with_partitioned_by(
                time_travel_format="HUDI",
                bucket_index={"field": "pk", "num_buckets": num_buckets},
            )

    @pytest.mark.parametrize("fmt", ["ICEBERG", "DELTA", None])
    def test_bucket_index_requires_hudi(self, fmt):
        with pytest.raises(
            FeatureStoreException, match="requires\\s+time_travel_format HUDI"
        ):
            _fg_with_partitioned_by(
                time_travel_format=fmt,
                bucket_index={"field": "pk", "num_buckets": 16},
            )

    def test_bucket_index_field_must_be_in_primary_key(self):
        with pytest.raises(FeatureStoreException, match="primary key"):
            _fg_with_partitioned_by(
                time_travel_format="HUDI",
                bucket_index={"field": "amount", "num_buckets": 16},
            )

    def test_bucket_index_field_check_skipped_without_primary_key(self):
        # the backend validates against the final schema; without a client-side
        # primary key there is nothing to check against
        fg = _fg_with_partitioned_by(
            time_travel_format="HUDI",
            primary_key=[],
            bucket_index={"field": "amount", "num_buckets": 16},
        )
        assert fg.bucket_index == {"field": "amount", "num_buckets": 16}

    def test_bucket_index_composes_with_partitioned_by(self):
        fg = _fg_with_partitioned_by(
            time_travel_format="HUDI",
            partitioned_by=["year(ts)"],
            bucket_index={"field": "pk", "num_buckets": 8},
        )
        assert fg.partitioned_by == ["year(ts)"]
        assert fg.bucket_index == {"field": "pk", "num_buckets": 8}


class TestFeatureGroupVisualize:
    def test_visualize_transformations(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.engine._get_instance")
        mocker.patch("hsfs.core.feature_group_engine.FeatureGroupEngine")

        from hsfs import transformation_function
        from hsfs.hopsworks_udf import udf

        @udf(int)
        def add_one(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[],
            id=10,
            stream=False,
            transformation_functions=[tf1],
        )

        # visualize() returns None, so verify via _to_mermaid on the DAG
        src = fg._transformation_function_execution_dag._to_mermaid()
        assert "add_one" in src
        assert "Input Features" in src
        assert "Output Features" in src
        # Also verify visualize_transformations doesn't raise
        fg.visualize_transformations(mode="text")

    def test_visualize_transformations_no_tfs(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.engine._get_instance")
        mocker.patch("hsfs.core.feature_group_engine.FeatureGroupEngine")

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[],
            id=10,
            stream=False,
        )

        with pytest.raises(FeatureStoreException, match="No transformation functions"):
            fg.visualize_transformations()

    def test_visualize_transformations_external_feature_group(
        self, mocker, backend_fixtures
    ):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.engine._get_instance")

        json = backend_fixtures["external_feature_group"]["get"]["response"]
        external_fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # ExternalFeatureGroup never builds an execution DAG; the base-class
        # default must produce the clean error, not an AttributeError.
        with pytest.raises(FeatureStoreException, match="No transformation functions"):
            external_fg.visualize_transformations()

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
from hsfs.core.constants import HAS_GREAT_EXPECTATIONS
from hsfs.engine import python, spark
from hsfs.transformation_function import TransformationType


with mock.patch("hopsworks_common.client.get_instance"):
    engine.init("python")


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
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
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
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
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
        assert len(fg.features) == 2
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format is None
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
        assert len(fg.features) == 2
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format is None
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
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
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
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
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
        assert len(fg.features) == 0
        assert fg.location is None
        assert fg.online_enabled is False
        assert fg.time_travel_format is None
        assert isinstance(fg.statistics_config, statistics_config.StatisticsConfig)
        assert fg._online_topic_name is None
        assert fg.event_time is None
        assert fg.stream is True
        assert fg.expectation_suite is None

    def test_constructor_with_list_event_time_for_compatibility(
        self, mocker, backend_fixtures, dataframe_fixture_basic
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
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
            util.verify_attribute_key_names(new_fg, False)

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
            "hopsworks_common.core.variable_api.VariableApi.get_featurestore_online_tablespace",
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
            "hopsworks_common.core.variable_api.VariableApi.get_featurestore_online_tablespace",
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
        }

    def test_feature_group_online_disk_true(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_featurestore_online_tablespace",
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
        }

    def test_feature_group_online_disk_true_override_online_config(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_featurestore_online_tablespace",
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
        }

    def test_feature_group_online_disk_false(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_featurestore_online_tablespace",
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
        assert fg._online_config.to_dict() == {"onlineComments": None, "tableSpace": ""}

    def test_feature_group_online_disk_false_override_online_config(self, mocker):
        # Arrange
        variable_api_mock = mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi.get_featurestore_online_tablespace",
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
        }

    def test_feature_group_data_source_update_storage_connector(self, mocker):
        # Arrange
        data_source = mocker.Mock()
        sc = storage_connector.S3Connector(id=1, name="s3_conn", featurestore_id=1)

        # Act
        feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=[],
            foreign_key=[],
            partition_key=[],
            data_source=data_source,
            storage_connector=sc,
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
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata",
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
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save_feature_group_metadata",
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
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mock_convert_to_default_dataframe = mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe"
        )
        mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.save",
            return_value=(None, None),
        )
        mock_commit_details = mocker.patch(
            "hsfs.feature_group.FeatureGroup.commit_details",
            return_value={
                "21378543924": {}
            },  # non-empty dict to simulate successful commit
        )
        mock_stats_engine = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_and_save_statistics",
            return_value=None,
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

        mock_convert_to_default_dataframe.assert_called_once_with(data)
        mock_commit_details.assert_called_once()
        mock_stats_engine.assert_called_once()

    def test_save_report_true_default(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe",
            return_value=dataframe_fixture_basic,
        )
        mock_insert = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.insert",
            return_value=(None, None),
        )
        mock_commit_details = mocker.patch(
            "hsfs.feature_group.FeatureGroup.commit_details",
            return_value={
                "21378543924": {}
            },  # non-empty dict to simulate successful commit
        )
        mock_stats_engine = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_and_save_statistics",
            return_value=None,
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
        mock_commit_details.assert_called_once()
        mock_stats_engine.assert_called_once()

    def test_save_report_default_overwritable(self, mocker, dataframe_fixture_basic):
        engine = python.Engine()
        mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch(
            "hsfs.engine.python.Engine.convert_to_default_dataframe",
            return_value=dataframe_fixture_basic,
        )
        mock_insert = mocker.patch(
            "hsfs.core.feature_group_engine.FeatureGroupEngine.insert",
            return_value=(None, None),
        )
        mock_commit_details = mocker.patch(
            "hsfs.feature_group.FeatureGroup.commit_details",
            return_value={
                "21378543924": {}
            },  # non-empty dict to simulate successful commit
        )
        mock_stats_engine = mocker.patch(
            "hsfs.core.statistics_engine.StatisticsEngine.compute_and_save_statistics",
            return_value=None,
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
        mock_commit_details.assert_called_once()
        mock_stats_engine.assert_called_once()

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
        fg._storage_connector = sc
        assert fg._is_hopsfs_storage() is expected

    def test_init_time_travel_and_stream_uses_resolvers_python(
        self, mocker, monkeypatch
    ):
        """Verify that `_init_time_travel_and_stream` delegates to `_resolve_time_travel_format` and, for the Python engine, to `_resolve_stream_python` to derive `_time_travel_format` and `_stream`.

        This test does not validate resolver logic; it only checks that the outputs of the resolver functions are propagated to the FeatureGroup instance and that the resolvers are called with the expected arguments (notably that the stream resolver receives the already-resolved format).
        """
        # Arrange: ensure code path selects Python Engine class
        monkeypatch.setattr("hsfs.engine.get_type", lambda: "python")
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
            online_enabled=True,
            is_hopsfs=False,
        )

        # Assert: _init uses resolvers' outputs
        assert fg._time_travel_format == expected_fmt
        assert fg._stream is expected_stream
        assert fmt_mock.call_count == 1
        assert fmt_mock.call_args.kwargs == {
            "time_travel_format": "None",
            "online_enabled": True,
            "is_hopsfs": False,
        }
        assert stream_mock.call_count == 1
        assert stream_mock.call_args.kwargs == {
            "stream": False,
            "time_travel_format": expected_fmt,
            "is_hopsfs": False,
            "online_enabled": True,
        }

    def test_init_time_travel_and_stream_uses_resolver_spark(self, mocker, monkeypatch):
        """Verify that `_init_time_travel_and_stream` delegates to `_resolve_time_travel_format` for setting `_time_travel_format` on the Spark engine, and that it does not call `_resolve_stream_python` nor mutate `_stream` (stream is a Python-engine concern only).

        This test avoids checking the internal logic of the resolver and only validates the delegation and side-effects of `_init_time_travel_and_stream`.
        """
        # Arrange: ensure code path selects Spark Engine class
        monkeypatch.setattr("hsfs.engine.get_type", lambda: "spark")
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
            online_enabled=False,
            is_hopsfs=False,
        )

        # Assert: format set via resolver, stream resolver not used, _stream unchanged
        assert fg._time_travel_format == expected_fmt
        assert fmt_mock.call_count == 1
        stream_mock.assert_not_called()
        assert fg._stream is False

    @pytest.mark.parametrize(
        "time_travel_format,is_hopsfs,has_deltalake,online_enabled,expected",
        [
            # time_travel_format=None cases (resolved by flags)
            (None, False, False, True, "HUDI"),  # Non-HopsFS & Online -> HUDI
            (None, False, False, False, "HUDI"),  # Non-HopsFS & Offline -> HUDI
            (None, False, True, True, "DELTA"),  # Non-HopsFS & Online -> HUDI
            (None, False, True, False, "DELTA"),  # Non-HopsFS & Offline -> HUDI
            (None, True, False, True, "HUDI"),  # HopsFS & Online -> HUDI
            (None, True, True, True, "DELTA"),  # HopsFS & Online -> HUDI
            (
                None,
                True,
                True,
                False,
                "DELTA",
            ),  # HopsFS & Offline -> DELTA when available
            (
                None,
                True,
                False,
                False,
                "HUDI",
            ),  # HopsFS & Offline -> HUDI when not available
            # time_travel_format="HUDI" cases (passthrough)
            ("HUDI", False, False, True, "HUDI"),
            ("HUDI", False, True, False, "HUDI"),
            ("HUDI", True, False, True, "HUDI"),
            ("HUDI", True, True, False, "HUDI"),
            # time_travel_format="DELTA" cases (passthrough)
            ("DELTA", False, False, True, "DELTA"),
            ("DELTA", False, True, False, "DELTA"),
            ("DELTA", True, False, True, "DELTA"),
            ("DELTA", True, True, False, "DELTA"),
        ],
    )
    def test_resolve_time_travel_format(
        self,
        monkeypatch,
        time_travel_format,
        online_enabled,
        is_hopsfs,
        has_deltalake,
        expected,
    ):
        monkeypatch.setattr(
            "hsfs.feature_group.FeatureGroup._has_deltalake", lambda: has_deltalake
        )
        result = feature_group.FeatureGroup._resolve_time_travel_format(
            time_travel_format=time_travel_format,
            online_enabled=online_enabled,
            is_hopsfs=is_hopsfs,
        )
        assert result == expected

    @pytest.mark.parametrize(
        "time_travel_format,stream,is_hopsfs,online_enabled,expected",
        [
            # DELTA not streams when not HopsFS and online enabled
            ("DELTA", False, True, False, False),
            ("DELTA", False, True, True, True),
            ("DELTA", False, False, False, True),
            ("DELTA", False, False, True, True),
            # DELTA always streams when stream is True
            ("DELTA", True, True, False, True),
            ("DELTA", True, True, True, True),
            ("DELTA", True, False, False, True),
            ("DELTA", True, False, True, True),
            # HUDI always streams
            ("HUDI", False, True, False, True),
            ("HUDI", False, True, True, True),
            ("HUDI", False, False, False, True),
            ("HUDI", False, False, True, True),
            ("HUDI", True, True, False, True),
            ("HUDI", True, True, True, True),
            ("HUDI", True, False, False, True),
            ("HUDI", True, False, True, True),
        ],
    )
    def test_resolve_stream_python(
        self, time_travel_format, stream, is_hopsfs, online_enabled, expected
    ):
        result = feature_group.FeatureGroup._resolve_stream_python(
            stream=stream,
            time_travel_format=time_travel_format,
            is_hopsfs=is_hopsfs,
            online_enabled=online_enabled,
        )
        assert result is expected

    def test_embedding_index_forces_online_enabled(self, mocker):
        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")

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
        )

        # Assert
        assert fg.online_enabled is True
        assert fg.stream is True


class TestExternalFeatureGroup:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["external_feature_group"]["get"]["response"]

        # Act
        fg = feature_group.ExternalFeatureGroup.from_response_json(json)

        # Assert
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
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
        assert len(fg.features) == 3
        assert isinstance(fg.features[0], feature.Feature)
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
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
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
        assert len(fg.features) == 3
        assert isinstance(fg.features[0], feature.Feature)
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
        assert isinstance(fg.storage_connector, storage_connector.StorageConnector)
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
        assert fg.features == []
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
        not HAS_GREAT_EXPECTATIONS,
        reason="great_expectations not installed",
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
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.get",
            return_value=es,
        )
        mock_create_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.create",
            return_value=es,
        )
        mock_delete_expectation_suite_api = mocker.patch(
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.delete"
        )

        version_api = mocker.patch("hsfs.core.variable_api.VariableApi.get_version")
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
            "hsfs.core.expectation_suite_api.ExpectationSuiteApi.update"
        )

        version_api = mocker.patch("hsfs.core.variable_api.VariableApi.get_version")
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
        assert len(fg.features) == 2
        assert isinstance(fg.features[0], feature.Feature)
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
        engine_instance = mocker.patch("hsfs.engine.get_instance", return_value=engine)
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
        engine_instance = mocker.patch("hsfs.engine.get_instance", return_value=engine)
        refetch_api = mocker.patch("hsfs.storage_connector.S3Connector.refetch")
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)
        fg._location = f"{fg.name}_{fg.version}"
        fg._storage_connector = storage_connector.S3Connector(
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
        engine_instance = mocker.patch("hsfs.engine.get_instance", return_value=engine)
        mocker.patch("hsfs.storage_connector.S3Connector.refetch")
        json = backend_fixtures["feature_group"]["get_basic_info"]["response"]
        fg = feature_group.FeatureGroup.from_response_json(json)
        fg._location = f"{fg.name}_{fg.version}"
        fg._storage_connector = storage_connector.S3Connector(
            id=1, name="s3_conn", featurestore_id=fg.feature_store_id
        )

        # Act
        with pytest.raises(AttributeError):
            fg.prepare_spark_location()

        # Assert
        engine_instance.assert_called_once()

    def test_upper_case_primary_key_event_time(self, mocker, backend_fixtures, caplog):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
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
        assert new_fg.features[0].name == "primarykey"
        assert new_fg.features[1].name == "event_time"
        assert new_fg.features[2].name == "feat"


class TestFeatureGroupExecuteOdts:
    def test_execute_odts_with_transformations(self, mocker):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")

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
            "apply_on_demand_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline)
        df_test_data = pd.DataFrame({"col1": [1, 2, 3]})
        result_df = fg.execute_odts(data=df_test_data, online=False)

        mock_apply.assert_called_with(
            transformation_functions=fg.transformation_functions,
            data=df_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online)
        dict_test_data = {"col1": 1}
        result_dict = fg.execute_odts(data=dict_test_data, online=True)

        mock_apply.assert_called_with(
            transformation_functions=fg.transformation_functions,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
        )
        assert result_dict == dict_test_data

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
        mocker.patch("hsfs.engine.get_type", return_value="python")

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
            "apply_on_demand_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline) and transformation_context
        df_test_data = pd.DataFrame({"col1": [1, 2, 3]})
        result_df = fg.execute_odts(
            data=df_test_data, online=False, transformation_context=context
        )

        mock_apply.assert_called_with(
            transformation_functions=fg.transformation_functions,
            data=df_test_data,
            online=False,
            transformation_context=context,
            request_parameters=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online) and request_parameters
        dict_test_data = {"col1": 1}
        result_dict = fg.execute_odts(
            data=dict_test_data, online=True, request_parameters=request_params
        )

        mock_apply.assert_called_with(
            transformation_functions=fg.transformation_functions,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=request_params,
        )
        assert result_dict == dict_test_data

    def test_execute_odts_no_transformations(self, mocker, caplog):
        import logging

        import pandas as pd

        # Arrange
        mocker.patch("hsfs.engine.get_type", return_value="python")

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
            "apply_on_demand_transformations",
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
        mocker.patch("hsfs.engine.get_type", return_value="python")

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
            "apply_on_demand_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Act - online
        fg.execute_odts(data=online_test_data, online=True)

        # Assert - online
        mock_apply.assert_called_with(
            transformation_functions=fg.transformation_functions,
            data=online_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
        )

        # Act - offline
        fg.execute_odts(data=offline_test_data, online=False)

        # Assert - offline
        mock_apply.assert_called_with(
            transformation_functions=fg.transformation_functions,
            data=offline_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
        )

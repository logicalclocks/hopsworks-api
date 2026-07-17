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

import pytest
from hopsworks_common.core import sink_job_configuration
from hopsworks_common.job_schedule import JobSchedule


class TestSinkJobConfiguration:
    def test_to_dict_defaults(self):
        config = sink_job_configuration.SinkJobConfiguration()

        assert config.to_dict() == {
            "type": sink_job_configuration.SinkJobConfiguration.DTO_TYPE,
            "name": None,
            "environmentName": (
                sink_job_configuration.SinkJobConfiguration.DEFAULT_ENVIRONMENT_NAME
            ),
            "transformScriptPath": None,
            "writeMode": sink_job_configuration.WriteMode.APPEND.value,
            "batchSize": 100000,
            "sqlSourceFetchChunkSize": 50000,
            "sourceReadWorkers": 1,
            "dataProcessingWorkers": 1,
            "maxUploadBatchSizeMB": 128,
            "sqlTableNumPartitions": 2,
            "loadingConfig": {
                "loadingStrategy": sink_job_configuration.LoadingStrategy.FULL_LOAD.value,
                "incrementalLoadingConfig": None,
                "fullLoadConfig": None,
            },
            "columnMappings": [],
            "featuregroupId": None,
            "featurestoreId": None,
            "storageConnectorId": None,
            "endpointConfig": None,
            "jobSchedule": None,
        }

    def test_to_dict_with_objects(self):
        loading_config = sink_job_configuration.LoadingConfig(
            loading_strategy=sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE,
            source_cursor_field="updated_at",
            initial_value="2023-01-01",
        )
        column_mappings = [
            sink_job_configuration.FeatureColumnMapping(
                source_column="source_1", feature_name="feature_1"
            )
        ]
        schedule = JobSchedule(
            start_date_time=1000,
            end_date_time=2000,
            enabled=True,
            cron_expression="0 0 * * *",
        )

        config = sink_job_configuration.SinkJobConfiguration(
            name="sink_job",
            environment_name="custom-dlt-env",
            transform_script_path="/Projects/demo/Resources/transform.py",
            batch_size=500,
            sql_source_fetch_chunk_size=600,
            source_read_workers=2,
            data_processing_workers=3,
            max_upload_batch_size_mb=256,
            sql_table_num_partitions=8,
            loading_config=loading_config,
            column_mappings=column_mappings,
            schedule_config=schedule,
        )

        assert config.to_dict() == {
            "type": sink_job_configuration.SinkJobConfiguration.DTO_TYPE,
            "name": "sink_job",
            "environmentName": "custom-dlt-env",
            "transformScriptPath": "/Projects/demo/Resources/transform.py",
            "writeMode": sink_job_configuration.WriteMode.APPEND.value,
            "batchSize": 500,
            "sqlSourceFetchChunkSize": 600,
            "sourceReadWorkers": 2,
            "dataProcessingWorkers": 3,
            "maxUploadBatchSizeMB": 256,
            "sqlTableNumPartitions": 8,
            "loadingConfig": {
                "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE.value,
                "incrementalLoadingConfig": {
                    "sourceCursorField": "updated_at",
                    "initialIngestionDate": 1672531200000,
                },
                "fullLoadConfig": None,
            },
            "columnMappings": [
                {"sourceColumn": "source_1", "featureName": "feature_1"}
            ],
            "featuregroupId": None,
            "featurestoreId": None,
            "storageConnectorId": None,
            "endpointConfig": None,
            "jobSchedule": {
                "id": None,
                "startDateTime": 1000,
                "endDateTime": 2000,
                "cronExpression": "0 0 * * *",
                "enabled": True,
                "catchup": False,
                "maxActiveRuns": 1,
                "startTimeOffsetSeconds": None,
                "endTimeOffsetSeconds": None,
                "skipToDate": None,
                "maxCatchupRuns": None,
            },
        }

    def test_from_response_json(self):
        json_dict = {
            "name": "sink_job",
            "environmentName": "custom-dlt-env",
            "transformScriptPath": "/Projects/demo/Resources/transform.py",
            "writeMode": "merge",
            "batchSize": 123,
            "sqlSourceFetchChunkSize": 456,
            "sourceReadWorkers": 7,
            "dataProcessingWorkers": 8,
            "maxUploadBatchSizeMB": 512,
            "sqlTableNumPartitions": 9,
            "loadingConfig": {
                "loadingStrategy": "INCREMENTAL_ID",
                "incrementalLoadingConfig": {
                    "sourceCursorField": "id",
                    "initialValue": "10",
                },
            },
            "columnMappings": [
                {"sourceColumn": "source_a", "featureName": "feature_a"}
            ],
            "jobSchedule": {
                "startDateTime": 3000,
                "endDateTime": 4000,
                "cronExpression": "0 * * * *",
                "enabled": False,
            },
        }

        config = sink_job_configuration.SinkJobConfiguration.from_response_json(
            json_dict
        )

        assert config.name == "sink_job"
        assert config.environment_name == "custom-dlt-env"
        assert config.transform_script_path == "/Projects/demo/Resources/transform.py"
        assert config.write_mode == sink_job_configuration.WriteMode.MERGE
        assert config.batch_size == 123
        assert config.sql_source_fetch_chunk_size == 456
        assert config.source_read_workers == 7
        assert config.data_processing_workers == 8
        assert config.max_upload_batch_size_mb == 512
        assert config.sql_table_num_partitions == 9
        assert isinstance(config.loading_config, sink_job_configuration.LoadingConfig)
        assert config.loading_config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_ID.value,
            "incrementalLoadingConfig": {
                "sourceCursorField": "id",
                "initialValue": "10",
            },
            "fullLoadConfig": None,
        }
        assert len(config.column_mappings) == 1
        assert config.column_mappings[0].source_column == "source_a"
        assert config.column_mappings[0].feature_name == "feature_a"
        assert isinstance(config.schedule_config, JobSchedule)
        assert config.schedule_config.to_dict() == {
            "id": None,
            "startDateTime": 3000,
            "endDateTime": 4000,
            "cronExpression": "0 * * * *",
            "enabled": False,
            "catchup": False,
            "maxActiveRuns": 1,
            "startTimeOffsetSeconds": None,
            "endTimeOffsetSeconds": None,
            "skipToDate": None,
            "maxCatchupRuns": None,
        }

    def test_from_response_json_uses_default_environment_name(self):
        config = sink_job_configuration.SinkJobConfiguration.from_response_json({})

        assert (
            config.environment_name
            == sink_job_configuration.SinkJobConfiguration.DEFAULT_ENVIRONMENT_NAME
        )

    def test_column_mappings_are_sanitized_from_dicts(self):
        config = sink_job_configuration.SinkJobConfiguration(
            column_mappings=[{"source_column": "source_a", "feature_name": "Feature A"}]
        )

        assert len(config.column_mappings) == 1
        assert isinstance(
            config.column_mappings[0], sink_job_configuration.FeatureColumnMapping
        )
        assert config.column_mappings[0].source_column == "source_a"
        assert config.column_mappings[0].feature_name == "feature_a"
        assert config.to_dict()["columnMappings"] == [
            {"sourceColumn": "source_a", "featureName": "feature_a"}
        ]

    def test_set_extra_params(self):
        config = sink_job_configuration.SinkJobConfiguration(name="old_name")

        config._set_extra_params(
            featuregroup_id=1,
            featurestore_id=2,
            storage_connector_id=3,
            endpoint_config={"relativeUrl": "/example"},
            name="new_name",
            environment_name="custom-dlt-env",
            transform_script_path="/Projects/demo/Resources/transform.py",
        )

        assert config.to_dict()["featuregroupId"] == 1
        assert config.to_dict()["featurestoreId"] == 2
        assert config.to_dict()["storageConnectorId"] == 3
        assert config.to_dict()["endpointConfig"] == {"relativeUrl": "/example"}
        assert config.to_dict()["name"] == "new_name"
        assert config.to_dict()["environmentName"] == "custom-dlt-env"
        assert (
            config.to_dict()["transformScriptPath"]
            == "/Projects/demo/Resources/transform.py"
        )

    def test_write_mode_validation(self):
        config = sink_job_configuration.SinkJobConfiguration(write_mode="merge")

        assert config.write_mode == sink_job_configuration.WriteMode.MERGE
        assert (
            config.to_dict()["writeMode"]
            == sink_job_configuration.WriteMode.MERGE.value
        )

        with pytest.raises(ValueError, match="Invalid write_mode"):
            sink_job_configuration.SinkJobConfiguration(write_mode="invalid")


class _FakeFeatureGroup:
    def __init__(self, fg_id):
        self.id = fg_id


class TestTableIngestionTarget:
    def test_requires_a_feature_group_id(self):
        with pytest.raises(ValueError, match="feature_group"):
            sink_job_configuration.TableIngestionTarget()

    def test_accepts_feature_group_object(self):
        target = sink_job_configuration.TableIngestionTarget(_FakeFeatureGroup(42))
        assert target.to_dict() == {"featuregroupId": 42, "enabled": True}

    def test_accepts_explicit_id(self):
        target = sink_job_configuration.TableIngestionTarget(feature_group_id=7)
        assert target.to_dict()["featuregroupId"] == 7

    def test_only_set_overrides_are_emitted(self):
        target = sink_job_configuration.TableIngestionTarget(
            feature_group_id=1,
            enabled=False,
            write_mode="MERGE",
            batch_size=5000,
            resource_config={"cores": 2, "memory": 4096},
        )
        d = target.to_dict()
        assert d == {
            "featuregroupId": 1,
            "enabled": False,
            "writeMode": "MERGE",
            "batchSize": 5000,
            "resourceConfig": {"cores": 2, "memory": 4096},
        }
        # untouched tuning fields are not sent, so the backend applies job defaults
        assert "sqlSourceFetchChunkSize" not in d

    def test_per_target_loading_and_column_mappings(self):
        target = sink_job_configuration.TableIngestionTarget(
            feature_group_id=1,
            loading_config=sink_job_configuration.LoadingConfig(
                "INCREMENTAL_ID", source_cursor_field="id"
            ),
            column_mappings=[
                sink_job_configuration.FeatureColumnMapping("src", "feat"),
            ],
        )
        d = target.to_dict()
        assert d["loadingConfig"]["loadingStrategy"] == "INCREMENTAL_ID"
        assert d["columnMappings"] == [{"sourceColumn": "src", "featureName": "feat"}]


class TestSinkJobConfigurationMultiTable:
    def test_multi_table_payload(self):
        config = sink_job_configuration.SinkJobConfiguration(
            name="crm_ingestion",
            table_parallelism=3,
            targets=[
                sink_job_configuration.TableIngestionTarget(feature_group_id=1),
                sink_job_configuration.TableIngestionTarget(
                    feature_group_id=2, write_mode="MERGE"
                ),
            ],
        )
        config._set_extra_params(featurestore_id=7, storage_connector_id=9)
        d = config.to_dict()

        assert d["type"] == sink_job_configuration.SinkJobConfiguration.DTO_TYPE
        assert d["tableParallelism"] == 3
        assert d["featurestoreId"] == 7
        assert d["storageConnectorId"] == 9
        assert [t["featuregroupId"] for t in d["targets"]] == [1, 2]
        assert d["targets"][1]["writeMode"] == "MERGE"
        # multi-table jobs carry no single feature group at the top level
        assert "featuregroupId" not in d
        # job-level scalars stay as defaults for targets to inherit
        assert d["batchSize"] == 100000

    def test_no_targets_keeps_single_table_payload(self):
        config = sink_job_configuration.SinkJobConfiguration(name="single")
        d = config.to_dict()
        assert "targets" not in d
        assert "featuregroupId" in d


class TestTableIngestionTargetRoundTrip:
    def test_from_response_json_keeps_endpoint_config(self):
        from hopsworks_common.core.rest_endpoint import RestEndpointConfig

        target = sink_job_configuration.TableIngestionTarget(
            feature_group_id=1,
            endpoint_config=RestEndpointConfig(relative_url="v1/events"),
        )
        restored = sink_job_configuration.TableIngestionTarget.from_response_json(
            target.to_dict()
        )
        assert restored.to_dict()["endpointConfig"] == {"relativeUrl": "v1/events"}

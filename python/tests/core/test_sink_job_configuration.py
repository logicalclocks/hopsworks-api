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

from hopsworks_common.core import sink_job_configuration
from hopsworks_common.job_schedule import JobSchedule


class TestSinkJobConfiguration:
    def test_to_dict_defaults(self):
        config = sink_job_configuration.SinkJobConfiguration()

        assert config.to_dict() == {
            "type": sink_job_configuration.SinkJobConfiguration.DTO_TYPE,
            "name": None,
            "batchSize": 100000,
            "loadingConfig": {
                "loadingStrategy": sink_job_configuration.LoadingStrategy.FULL_LOAD.value,
                "sourceCursorField": None,
                "initialValue": None,
                "initialValueDate": None,
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
            batch_size=500,
            loading_config=loading_config,
            column_mappings=column_mappings,
            schedule_config=schedule,
        )

        assert config.to_dict() == {
            "type": sink_job_configuration.SinkJobConfiguration.DTO_TYPE,
            "name": "sink_job",
            "batchSize": 500,
            "loadingConfig": {
                "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_DATE.value,
                "sourceCursorField": "updated_at",
                "initialValue": None,
                "initialValueDate": "2023-01-01",
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
            },
        }

    def test_from_response_json(self):
        json_dict = {
            "name": "sink_job",
            "batchSize": 123,
            "loadingConfig": {
                "loadingStrategy": "INCREMENTAL_ID",
                "sourceCursorField": "id",
                "initialValue": "10",
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
        assert config.batch_size == 123
        assert isinstance(config.loading_config, sink_job_configuration.LoadingConfig)
        assert config.loading_config.to_dict() == {
            "loadingStrategy": sink_job_configuration.LoadingStrategy.INCREMENTAL_ID.value,
            "sourceCursorField": "id",
            "initialValue": "10",
            "initialValueDate": None,
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
        }

    def test_set_extra_params(self):
        config = sink_job_configuration.SinkJobConfiguration(name="old_name")

        config.set_extra_params(
            featuregroup_id=1,
            featurestore_id=2,
            storage_connector_id=3,
            endpoint_config={"url": "http://example"},
            name="new_name",
        )

        assert config.to_dict()["featuregroupId"] == 1
        assert config.to_dict()["featurestoreId"] == 2
        assert config.to_dict()["storageConnectorId"] == 3
        assert config.to_dict()["endpointConfig"] == {"url": "http://example"}
        assert config.to_dict()["name"] == "new_name"

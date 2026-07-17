#
#   Copyright 2026 Hopsworks AB
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

from unittest.mock import MagicMock, patch

import pytest
from hopsworks_common.core.sink_job_configuration import SinkJobConfiguration
from hsfs import storage_connector
from hsfs.core import data_source as data_source_mod
from hsfs.core import multi_table_ingestion


def _feature_group(fg_id):
    fg = MagicMock()
    fg.id = fg_id
    return fg


def _builder():
    sc = storage_connector.RedshiftConnector(9, "sc", 7)
    ds = data_source_mod.DataSource(storage_connector=sc)
    return ds.new_ingestion_job(
        "crm_ingestion", table_parallelism=3, write_mode="APPEND"
    )


class TestMultiTableIngestionJob:
    def test_new_ingestion_job_returns_empty_builder(self):
        job = _builder()
        assert isinstance(job, multi_table_ingestion.MultiTableIngestionJob)
        assert job.name == "crm_ingestion"
        assert job.targets == []
        assert job.job is None

    def test_add_target_collects_and_dedupes(self):
        job = _builder()
        job.add_target(feature_group_id=1)
        job.add_target(feature_group_id=2, write_mode="MERGE")
        # same feature group again replaces rather than duplicates
        job.add_target(feature_group_id=1, batch_size=5000)

        assert [t._feature_group_id for t in job.targets] == [1, 2]
        assert job.targets[0].to_dict()["batchSize"] == 5000

    def test_save_without_targets_raises(self):
        with pytest.raises(ValueError, match="no targets"):
            _builder().save()

    def test_save_builds_multi_table_payload_atomically(self):
        job = _builder()
        job.add_target(feature_group_id=1)
        job.add_target(feature_group_id=2, write_mode="MERGE")

        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            created = MagicMock()
            api.create.return_value = created

            result = job.save()

            posted_name, conf = api.create.call_args[0]
            payload = conf.to_dict()

        assert result is created
        assert job.job is created
        # one create call carries every target
        api.create.assert_called_once()
        assert posted_name == "crm_ingestion"
        assert payload["tableParallelism"] == 3
        assert payload["featurestoreId"] == 7
        assert payload["storageConnectorId"] == 9
        assert payload["writeMode"] == "APPEND"
        assert [t["featuregroupId"] for t in payload["targets"]] == [1, 2]
        assert payload["targets"][1]["writeMode"] == "MERGE"

    def test_run_saves_first_when_not_saved(self):
        job = _builder()
        job.add_target(feature_group_id=1)

        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            created = MagicMock()
            api.create.return_value = created

            job.run()

            api.create.assert_called_once()
            created.run.assert_called_once_with(await_termination=False)

    def test_attach_feature_group_extracts_only_changed_overrides(self):
        job = _builder()
        # a bare config must not pin defaults as per-target overrides
        job._attach_feature_group(_feature_group(1), SinkJobConfiguration())
        # a changed field becomes an override
        job._attach_feature_group(
            _feature_group(2), SinkJobConfiguration(batch_size=999)
        )

        assert "batchSize" not in job.targets[0].to_dict()
        assert job.targets[1].to_dict()["batchSize"] == 999

    def test_attach_feature_group_without_config(self):
        job = _builder()
        job._attach_feature_group(_feature_group(5), None)
        assert job.targets[0].to_dict() == {"featuregroupId": 5, "enabled": True}

    def test_set_table_enabled_toggles_target(self):
        job = _builder()
        job.add_target(feature_group_id=1)
        job.set_table_enabled(feature_group_id=1, enabled=False)
        assert job.targets[0].to_dict()["enabled"] is False

    def test_set_table_enabled_resaves_when_already_saved(self):
        job = _builder()
        job.add_target(feature_group_id=1)

        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            created = MagicMock()
            created.config = {"targets": [{"featuregroupId": 1, "enabled": True}]}
            current = MagicMock()
            current.config = {"targets": [{"featuregroupId": 1, "enabled": True}]}
            updated = MagicMock()
            updated.config = {"targets": [{"featuregroupId": 1, "enabled": False}]}
            current.save.return_value = updated
            api.create.return_value = created
            api.get.return_value = current
            job.save()
            api.create.reset_mock()

            job.set_table_enabled(feature_group_id=1, enabled=False)

            api.get.assert_called_once_with("crm_ingestion")
            current.save.assert_called_once()
            api.create.assert_not_called()
            assert current.config["targets"][0]["enabled"] is False
            assert job.job is updated

    def test_set_table_enabled_resolves_recreated_feature_group_ids(self):
        job = _builder()
        old_fg = _feature_group(1)
        job.add_target(feature_group_id=1)
        job.add_target(feature_group_id=2)

        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            created = MagicMock()
            created.config = {
                "targets": [
                    {"featuregroupId": 1, "enabled": True},
                    {"featuregroupId": 2, "enabled": True},
                ]
            }
            current = MagicMock()
            current.config = {
                "targets": [
                    {"featuregroupId": 11, "enabled": True},
                    {"featuregroupId": 12, "enabled": True},
                ]
            }
            updated = MagicMock()
            updated.config = {
                "targets": [
                    {"featuregroupId": 11, "enabled": False},
                    {"featuregroupId": 12, "enabled": True},
                ]
            }
            current.save.return_value = updated
            api.create.return_value = created
            api.get.return_value = current

            job.save()
            job.set_table_enabled(feature_group=old_fg, enabled=False)

        assert current.config["targets"][0]["enabled"] is False
        assert [target._feature_group_id for target in job.targets] == [11, 12]
        assert old_fg.id == 11

    def test_set_table_enabled_preserves_column_mappings(self):
        job = _builder()
        # attach with generated column mappings, as the engine does
        conf = SinkJobConfiguration(
            column_mappings=[{"sourceColumn": "Total Amount", "featureName": "total"}]
        )
        job._attach_feature_group(_feature_group(1), conf)
        job.set_table_enabled(feature_group_id=1, enabled=False)
        target = job.targets[0].to_dict()
        assert target["enabled"] is False
        assert target["columnMappings"] == [
            {"sourceColumn": "Total Amount", "featureName": "total"}
        ]

    def test_set_table_enabled_unknown_feature_group_raises(self):
        job = _builder()
        job.add_target(feature_group_id=1)
        with pytest.raises(ValueError, match="No target"):
            job.set_table_enabled(feature_group_id=99)


class TestScheduleConfigNormalization:
    def test_create_or_update_schedule_accepts_job_schedule_object(self, mocker):
        import datetime

        from hopsworks_common.core.job_api import JobApi
        from hopsworks_common.job_schedule import JobSchedule

        client = mocker.patch(
            "hopsworks_common.core.job_api.client._get_instance"
        ).return_value
        client._project_id = 1
        client._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.job_api.job_schedule.JobSchedule.from_response_json"
        )

        # SinkJobConfiguration normalizes dict schedules to JobSchedule objects,
        # which used to break this endpoint (it indexed schedule_config["id"]).
        schedule = JobSchedule(
            start_date_time=datetime.datetime(2026, 1, 1),
            enabled=True,
            cron_expression="0 0 * * *",
            id=99,
        )

        JobApi().create_or_update_schedule_job("crm_ingestion", schedule)

        method, _path = client._send_request.call_args[0][:2]
        body = client._send_request.call_args.kwargs["data"]
        assert method == "PUT"  # id is set
        assert '"id": 99' in body

    def test_create_or_update_schedule_dict_without_id_posts(self, mocker):
        from hopsworks_common.core.job_api import JobApi

        client = mocker.patch(
            "hopsworks_common.core.job_api.client._get_instance"
        ).return_value
        client._project_id = 1
        client._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.job_api.job_schedule.JobSchedule.from_response_json"
        )

        # A "create" schedule dict has no "id" yet; it must not raise KeyError.
        JobApi().create_or_update_schedule_job(
            "crm_ingestion", {"cronExpression": "0 0 * * *", "enabled": True}
        )

        method = client._send_request.call_args[0][0]
        assert method == "POST"

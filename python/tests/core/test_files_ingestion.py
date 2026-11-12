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
from __future__ import annotations

import json
from unittest.mock import MagicMock, Mock, patch

import pytest
from hopsworks_common import job as job_mod
from hopsworks_common.execution import Execution
from hsfs import storage_connector
from hsfs.core import files_ingestion


# The exact body of the cross-repo contract (section 1), for the settings below.
_CONTRACT_BODY = {
    "type": "ingestionJobConfiguration",
    "appName": "handbook_files",
    "jobType": "INGESTION",
    "featurestoreId": 67,
    "mode": "FILES",
    "filesConfig": {
        "storageConnectorId": 12,
        "prefix": "handbook/2026/",
        "include": ["**/*.pdf", "**/*.docx"],
        "exclude": ["**/~$*"],
        "datasetName": "acme_docs_handbook",
        "mirrorDeletions": False,
        "indexFiles": False,
        "indexFeatureGroupName": None,
        "maxFileSizeMB": None,
        "filesPerPage": 100,
        "failOnFirstError": False,
        "transformScriptPath": None,
        "keepOriginal": False,
    },
    "resourceConfig": {"cores": 1, "memory": 2048, "gpus": 0},
    "environmentName": "dlthub-ingestion-pipeline",
}

_STATS = {
    "executionId": 4711,
    "status": "partial",
    "listed": 120,
    "copied": 10,
    "failed": 1,
    "bytesCopied": 123456,
    "failedFiles": [{"relativePath": "a/b.pdf", "error": "boom"}],
    "auditPath": "/Projects/p/DataSources/my_s3/files/acme_docs_handbook/audit/date=2026-11-07/101500Z_4711.csv",
}


def _connector(connector_type="S3"):
    classes = {
        "S3": storage_connector.S3Connector,
        "GCS": storage_connector.GcsConnector,
        "ADLS": storage_connector.AdlsConnector,
        "SFTP": storage_connector.SftpConnector,
        "GOOGLE_DRIVE": storage_connector.GoogleDriveConnector,
        "REDSHIFT": storage_connector.RedshiftConnector,
    }
    return classes[connector_type](12, "my_s3", 67)


def _job_builder(**overrides):
    settings = {
        "prefix": "handbook/2026/",
        "include": ["**/*.pdf", "**/*.docx"],
        "exclude": ["**/~$*"],
        "resources": {"cores": 1, "memory": 2048, "gpus": 0},
        "name": "handbook_files",
        "environment_name": "dlthub-ingestion-pipeline",
    }
    settings.update(overrides)
    return files_ingestion.FilesIngestionJob(
        _connector(), "acme_docs_handbook", **settings
    )


def _server_job(config=None, schedule=None, name="handbook_files"):
    return job_mod.Job(
        id=1,
        name=name,
        creation_time=None,
        config=config if config is not None else json.loads(json.dumps(_CONTRACT_BODY)),
        job_type="INGESTION",
        creator=None,
        job_schedule=schedule,
    )


def _create_capturing(api, created):
    """Make the mocked JobApi.create record the body as it is sent, before save() refreshes the server config from the response."""
    bodies = []

    def create(name, conf):
        bodies.append(json.loads(conf.json()))
        return created

    api.create.side_effect = create
    return bodies


def _client(mocker, response=None):
    client_mock = Mock()
    client_mock._project_id = 119
    client_mock._send_request.return_value = response
    mocker.patch("hopsworks_common.client._get_instance", return_value=client_mock)
    return client_mock


class TestConfiguration:
    def test_to_dict_matches_the_contract(self):
        assert _job_builder().to_dict() == _CONTRACT_BODY
        assert json.loads(_job_builder().json()) == _CONTRACT_BODY

    def test_defaults(self):
        job = files_ingestion.FilesIngestionJob(_connector(), "Acme Docs")

        body = job.to_dict()
        assert job.name == "my_s3_Acme_Docs_files"
        assert body["appName"] == "my_s3_Acme_Docs_files"
        assert body["filesConfig"] == {
            "storageConnectorId": 12,
            "prefix": "",
            "include": [],
            "exclude": [],
            "datasetName": "Acme Docs",
            "mirrorDeletions": False,
            "indexFiles": False,
            "indexFeatureGroupName": None,
            "maxFileSizeMB": None,
            "filesPerPage": 100,
            "failOnFirstError": False,
            "transformScriptPath": None,
            "keepOriginal": False,
        }
        assert "resourceConfig" not in body
        assert "environmentName" not in body
        assert job.index_feature_group is None
        assert job.schedule is None
        assert job.job is None

    def test_default_job_name_is_sanitized(self):
        sc = storage_connector.S3Connector(12, "My-S3 Bucket", 67)
        job = files_ingestion.FilesIngestionJob(sc, "Acme.Docs/2026")
        # one rule shared with the UI: no lowercasing, [^A-Za-z0-9_] -> "_"
        assert job.name == "My_S3_Bucket_Acme_Docs_2026_files"

    def test_index_true_uses_the_default_name(self):
        job = _job_builder(index=True)

        files = job.to_dict()["filesConfig"]
        assert files["indexFiles"] is True
        assert files["indexFeatureGroupName"] is None
        assert job.index_feature_group == "files_acme_docs_handbook"

    def test_index_name_is_sent(self):
        job = _job_builder(index="handbook_index")

        files = job.to_dict()["filesConfig"]
        assert files["indexFiles"] is True
        assert files["indexFeatureGroupName"] == "handbook_index"
        assert job.index_feature_group == "handbook_index"

    def test_transform_settings_are_sent(self):
        job = _job_builder(
            transform_script=" Resources/transform.py ", keep_original=True
        )

        files = job.to_dict()["filesConfig"]
        assert files["transformScriptPath"] == "Resources/transform.py"
        assert files["keepOriginal"] is True
        assert job.transform_script == "Resources/transform.py"
        assert job.keep_original is True

    def test_keep_original_says_nothing_without_a_script(self):
        job = _job_builder(keep_original=True)

        assert job.to_dict()["filesConfig"]["keepOriginal"] is False
        assert job.transform_script is None and job.keep_original is False

    def test_other_settings_are_sent(self):
        job = _job_builder(
            mirror_deletions=True,
            max_file_size_mb=512,
            fail_on_first_error=True,
            files_per_page=50,
        )

        files = job.to_dict()["filesConfig"]
        assert files["mirrorDeletions"] is True
        assert files["maxFileSizeMB"] == 512
        assert files["failOnFirstError"] is True
        assert files["filesPerPage"] == 50
        assert job.mirror_deletions is True
        assert job.max_file_size_mb == 512

    @pytest.mark.parametrize(
        ("settings", "message"),
        [
            ({"dataset": ""}, "dataset"),
            ({"dataset": "   "}, "dataset"),
            ({"prefix": "/handbook/"}, "must not start with '/'"),
            ({"prefix": "handbook/../secret/"}, "'..'"),
            ({"include": "**/*.pdf"}, "include must be a list"),
            ({"exclude": ["ok", 3]}, "exclude must be a list"),
            ({"index": ""}, "index must be"),
            ({"index": 1}, "index must be"),
            ({"max_file_size_mb": -1}, "max_file_size_mb"),
            ({"max_file_size_mb": "big"}, "max_file_size_mb"),
            ({"files_per_page": 0}, "files_per_page"),
            ({"transform_script": "Resources/t.txt"}, "transform_script"),
            ({"transform_script": 3}, "transform_script"),
            ({"resources": [1]}, "resources must be a dict"),
            ({"schedule": ""}, "schedule must be"),
            ({"name": " "}, "name must be"),
        ],
    )
    def test_validation_errors(self, settings, message):
        dataset = settings.pop("dataset", "acme_docs_handbook")
        with pytest.raises(ValueError, match=message):
            files_ingestion.FilesIngestionJob(_connector(), dataset, **settings)

    def test_name_required_without_a_connector(self):
        with pytest.raises(ValueError, match="name is required"):
            files_ingestion.FilesIngestionJob(
                None, "ds", storage_connector_id=12, featurestore_id=67
            )


class TestFromJob:
    def test_round_trip(self):
        config = json.loads(json.dumps(_CONTRACT_BODY))
        config["filesConfig"]["indexFiles"] = True
        config["filesConfig"]["indexFeatureGroupName"] = "handbook_index"
        config["filesConfig"]["maxFileSizeMB"] = 256
        server_job = _server_job(
            config,
            schedule={
                "id": 3,
                "startDateTime": 1700000000000,
                "enabled": True,
                "cronExpression": "0 0 2 ? * *",
            },
        )

        job = files_ingestion.FilesIngestionJob.from_job(server_job)

        assert job.job is server_job
        assert job.name == "handbook_files"
        assert job.dataset_name == "acme_docs_handbook"
        assert job.prefix == "handbook/2026/"
        assert job.include == ["**/*.pdf", "**/*.docx"]
        assert job.exclude == ["**/~$*"]
        assert job.mirror_deletions is False
        assert job.index_feature_group == "handbook_index"
        assert job.max_file_size_mb == 256
        assert job.storage_connector_id == 12
        assert job.schedule == "0 0 2 ? * *"
        # what it sends back is what it was built from
        assert job.to_dict() == config

    def test_index_without_a_name_is_the_default(self):
        config = json.loads(json.dumps(_CONTRACT_BODY))
        config["filesConfig"]["indexFiles"] = True

        job = files_ingestion.FilesIngestionJob.from_job(_server_job(config))

        assert job.index_feature_group == "files_acme_docs_handbook"
        assert job.to_dict()["filesConfig"]["indexFeatureGroupName"] is None

    @pytest.mark.parametrize(
        "config",
        [
            {"type": "ingestionJobConfiguration", "mode": "TABLES", "targets": []},
            {"type": "pythonJobConfiguration", "appPath": "x.py"},
            {"type": "ingestionJobConfiguration", "mode": "FILES"},
        ],
    )
    def test_refuses_other_jobs(self, config):
        with pytest.raises(ValueError, match="not a files ingestion job"):
            files_ingestion.FilesIngestionJob.from_job(_server_job(config))

    def test_refuses_a_files_job_without_a_dataset(self):
        config = json.loads(json.dumps(_CONTRACT_BODY))
        del config["filesConfig"]["datasetName"]
        with pytest.raises(ValueError, match="without a datasetName"):
            files_ingestion.FilesIngestionJob.from_job(_server_job(config))

    def test_save_keeps_the_fields_the_sdk_does_not_model(self):
        config = json.loads(json.dumps(_CONTRACT_BODY))
        config["description"] = "handbook copy"
        config["defaultArgs"] = "-v"
        config["envVars"] = {"LOG_LEVEL": "DEBUG"}
        config["filesConfig"]["listingTimeoutSeconds"] = 3600
        job = files_ingestion.FilesIngestionJob.from_job(_server_job(config))

        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            api.create.return_value = _server_job(config)
            job.update(prefix="handbook/2027/", mirror_deletions=True)
            body = json.loads(api.create.call_args[0][1].json())

        assert body["description"] == "handbook copy"
        assert body["defaultArgs"] == "-v"
        assert body["envVars"] == {"LOG_LEVEL": "DEBUG"}
        assert body["resourceConfig"] == {"cores": 1, "memory": 2048, "gpus": 0}
        assert body["environmentName"] == "dlthub-ingestion-pipeline"
        assert body["filesConfig"]["listingTimeoutSeconds"] == 3600
        assert body["filesConfig"]["prefix"] == "handbook/2027/"
        assert body["filesConfig"]["mirrorDeletions"] is True

    def test_save_refreshes_the_server_config(self):
        job = _job_builder()
        server = json.loads(json.dumps(_CONTRACT_BODY))
        server["description"] = "set by the server"
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            api.create.return_value = _server_job(server)
            job.save()
            job.update(include=["**/*.md"])
            body = json.loads(api.create.call_args[0][1].json())

        assert body["description"] == "set by the server"
        assert body["filesConfig"]["include"] == ["**/*.md"]


class TestSave:
    def test_save_puts_the_contract_body_and_no_schedule(self):
        job = _job_builder()
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            created = _server_job()
            api.create.return_value = created

            result = job.save()

            posted_name, conf = api.create.call_args[0]
        assert result is created
        assert job.job is created
        assert posted_name == "handbook_files"
        assert json.loads(conf.json()) == _CONTRACT_BODY
        api.create_or_update_schedule_job.assert_not_called()
        api.delete_schedule_job.assert_not_called()

    def test_save_creates_the_schedule(self):
        job = _job_builder(schedule="0 0 2 ? * *")
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            api.create.return_value = _server_job()

            job.save()

            name, schedule = api.create_or_update_schedule_job.call_args[0]
        assert name == "handbook_files"
        assert schedule.cron_expression == "0 0 2 ? * *"
        assert schedule.id is None
        assert schedule.enabled is True
        assert schedule.start_date_time is not None

    def test_update_keeps_the_schedule_id_and_removes_on_none(self):
        existing = {
            "id": 3,
            "startDateTime": 1700000000000,
            "enabled": True,
            "cronExpression": "0 0 2 ? * *",
        }
        job = files_ingestion.FilesIngestionJob.from_job(_server_job(schedule=existing))
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            api.create.return_value = _server_job(schedule=existing)

            job.update(prefix="handbook/2027/", schedule="0 0 3 ? * *")
            _, schedule = api.create_or_update_schedule_job.call_args[0]
            body = json.loads(api.create.call_args[0][1].json())

            job.update(schedule=None)

        assert body["filesConfig"]["prefix"] == "handbook/2027/"
        assert schedule.id == 3
        assert schedule.cron_expression == "0 0 3 ? * *"
        api.delete_schedule_job.assert_called_once_with("handbook_files")
        assert job.schedule is None

    def test_update_keeps_a_paused_schedule_paused(self):
        existing = {
            "id": 3,
            "startDateTime": 1700000000000,
            "enabled": False,
            "cronExpression": "0 0 2 ? * *",
        }
        job = files_ingestion.FilesIngestionJob.from_job(_server_job(schedule=existing))
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            api.create.return_value = _server_job(schedule=existing)
            job.update(schedule="0 0 4 ? * *")
            _, schedule = api.create_or_update_schedule_job.call_args[0]

        assert schedule.enabled is False
        assert schedule.cron_expression == "0 0 4 ? * *"

    def test_update_skips_the_schedule_when_the_cron_is_unchanged(self):
        existing = {
            "id": 3,
            "startDateTime": 1700000000000,
            "enabled": True,
            "cronExpression": "0 0 2 ? * *",
        }
        job = files_ingestion.FilesIngestionJob.from_job(_server_job(schedule=existing))
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            api.create.return_value = _server_job(schedule=existing)
            job.update(prefix="handbook/2027/")

        api.create_or_update_schedule_job.assert_not_called()
        api.delete_schedule_job.assert_not_called()

    def test_update_refuses_fixed_fields(self):
        job = _job_builder()
        with pytest.raises(ValueError, match="Cannot update dataset, name"):
            job.update(dataset="other", name="x")

    def test_update_changes_the_editable_fields(self):
        job = _job_builder()
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            bodies = _create_capturing(api, _server_job())

            result = job.update(
                include=["**/*.md"],
                exclude=[],
                mirror_deletions=True,
                index="idx",
                max_file_size_mb=0,
                fail_on_first_error=True,
                files_per_page=10,
                resources={"cores": 2, "memory": 4096, "gpus": 0},
                environment_name=None,
            )
            body = bodies[0]

        assert result is job
        assert body["filesConfig"]["include"] == ["**/*.md"]
        assert body["filesConfig"]["exclude"] == []
        assert body["filesConfig"]["mirrorDeletions"] is True
        assert body["filesConfig"]["indexFiles"] is True
        assert body["filesConfig"]["indexFeatureGroupName"] == "idx"
        assert body["filesConfig"]["maxFileSizeMB"] == 0
        assert body["filesConfig"]["failOnFirstError"] is True
        assert body["filesConfig"]["filesPerPage"] == 10
        assert body["resourceConfig"] == {"cores": 2, "memory": 4096, "gpus": 0}
        assert "environmentName" not in body

    def test_run_saves_first_then_runs_the_job(self):
        job = _job_builder()
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            created = MagicMock()
            api.create.return_value = created

            execution = job.run()

            api.create.assert_called_once()
        created.run.assert_called_once_with(await_termination=False)
        assert execution is created.run.return_value

    def test_run_does_not_resave_a_saved_job(self):
        created = MagicMock()
        job = _job_builder(job=created)
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            job.run()
            MockJobApi.return_value.create.assert_not_called()
        created.run.assert_called_once_with(await_termination=False)

    def test_run_awaits_and_returns_a_finished_execution(self):
        created = MagicMock()
        started = MagicMock(id=4711)
        created.run.return_value = started
        job = _job_builder(job=created)
        finished = MagicMock()
        with patch(
            "hopsworks_common.engine.execution_engine.ExecutionEngine"
        ) as MockEngine:
            MockEngine.return_value._wait_until_finished.return_value = finished

            result = job.run(await_termination=True)

            MockEngine.return_value._wait_until_finished.assert_called_once_with(
                created, started
            )
        created.run.assert_called_once_with(await_termination=False)
        assert result is finished

    def test_run_awaits_and_returns_a_partial_run_instead_of_raising(self):
        from hopsworks_common.client.exceptions import JobExecutionException

        created = MagicMock()
        started = MagicMock(id=4711)
        created.run.return_value = started
        job = _job_builder(job=created)
        failed = MagicMock(files_progress={"status": "partial", "failed": 1})
        with (
            patch(
                "hopsworks_common.engine.execution_engine.ExecutionEngine"
            ) as MockEngine,
            patch("hopsworks_common.core.execution_api.ExecutionApi") as MockApi,
        ):
            MockEngine.return_value._wait_until_finished.side_effect = (
                JobExecutionException("The Hopsworks Job failed")
            )
            MockApi.return_value._get.return_value = failed

            result = job.run(await_termination=True)

            MockApi.return_value._get.assert_called_once_with(created, 4711)
        assert result is failed
        assert result.files_progress["status"] == "partial"


class TestDelete:
    def test_delete_sends_the_query_params(self, mocker):
        client = _client(mocker)
        job = _job_builder(job=_server_job())

        job.delete(delete_files=True, delete_index=True)

        assert client._send_request.call_args.args == (
            "DELETE",
            ["project", 119, "jobs", "handbook_files"],
        )
        assert client._send_request.call_args.kwargs["query_params"] == {
            "deleteFiles": True,
            "deleteIndex": True,
        }
        assert job.job is None

    def test_delete_defaults_keep_files_and_index(self, mocker):
        client = _client(mocker)

        _job_builder(job=_server_job()).delete()

        assert client._send_request.call_args.kwargs["query_params"] == {
            "deleteFiles": False,
            "deleteIndex": False,
        }


class TestLastStats:
    def test_none_before_save(self):
        assert _job_builder().last_stats is None

    def test_reads_the_latest_execution(self, mocker):
        server_job = _server_job()
        client = _client(
            mocker,
            {
                "count": 1,
                "items": [{"id": 4711, "state": "FINISHED", "filesProgress": _STATS}],
            },
        )
        job = _job_builder(job=server_job)

        stats = job.last_stats

        assert stats == _STATS
        assert stats["failedFiles"][0]["relativePath"] == "a/b.pdf"
        assert client._send_request.call_args.args[:2] == (
            "GET",
            ["project", 119, "jobs", "handbook_files", "executions"],
        )

    def test_none_without_executions(self, mocker):
        _client(mocker, {"count": 0, "items": []})
        assert _job_builder(job=_server_job()).last_stats is None

    def test_none_when_the_execution_has_no_stats(self, mocker):
        _client(mocker, {"id": 4711, "state": "RUNNING"})
        assert _job_builder(job=_server_job()).last_stats is None

    def test_none_when_the_backend_sends_only_the_stub(self, mocker):
        # the backend always attaches the job's dataset, prefix and connector; the
        # result keys appear only once the run has written its stats file
        _client(
            mocker,
            {
                "id": 4711,
                "state": "RUNNING",
                "filesProgress": {
                    "datasetName": "acme_docs_handbook",
                    "prefix": "handbook/2026/",
                    "connectorName": "my_s3",
                },
            },
        )
        assert _job_builder(job=_server_job()).last_stats is None


class TestStorageConnector:
    def test_ingest_files_saves_and_returns_the_job(self):
        sc = _connector("GCS")
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            api = MockJobApi.return_value
            bodies = _create_capturing(api, _server_job())

            job = sc.ingest_files(
                dataset="acme_docs_handbook",
                prefix="handbook/2026/",
                include=["**/*.pdf", "**/*.docx"],
                exclude=["**/~$*"],
                resources={"cores": 1, "memory": 2048, "gpus": 0},
                name="handbook_files",
            )
            posted_name = api.create.call_args[0][0]

        assert isinstance(job, files_ingestion.FilesIngestionJob)
        assert posted_name == "handbook_files"
        expected = {k: v for k, v in _CONTRACT_BODY.items() if k != "environmentName"}
        assert bodies == [expected]
        assert job.job is not None

    @pytest.mark.parametrize(
        "connector_type", ["S3", "GCS", "ADLS", "SFTP", "GOOGLE_DRIVE"]
    )
    def test_ingest_files_accepts_object_stores(self, connector_type):
        with patch("hopsworks_common.core.job_api.JobApi") as MockJobApi:
            MockJobApi.return_value.create.return_value = _server_job()
            job = _connector(connector_type).ingest_files(dataset="ds")
        assert job.name == "my_s3_ds_files"

    def test_ingest_files_refuses_other_types(self):
        with pytest.raises(ValueError, match="REDSHIFT"):
            _connector("REDSHIFT").ingest_files(dataset="ds")

    def test_get_files_ingestion_jobs_filters_by_mode_and_connector(self, mocker):
        other = json.loads(json.dumps(_CONTRACT_BODY))
        other["filesConfig"]["storageConnectorId"] = 99
        _client(
            mocker,
            {
                "items": [
                    {
                        "id": 1,
                        "name": "handbook_files",
                        "creationTime": None,
                        "config": json.loads(json.dumps(_CONTRACT_BODY)),
                        "jobType": "INGESTION",
                        "creator": None,
                    },
                    {
                        "id": 2,
                        "name": "other_source",
                        "creationTime": None,
                        "config": other,
                        "jobType": "INGESTION",
                        "creator": None,
                    },
                    {
                        "id": 3,
                        "name": "tables",
                        "creationTime": None,
                        "config": {
                            "type": "ingestionJobConfiguration",
                            "mode": "TABLES",
                            "storageConnectorId": 12,
                        },
                        "jobType": "INGESTION",
                        "creator": None,
                    },
                    {
                        "id": 4,
                        "name": "py",
                        "creationTime": None,
                        "config": {"type": "pythonJobConfiguration"},
                        "jobType": "PYTHON",
                        "creator": None,
                    },
                ]
            },
        )

        jobs = _connector().get_files_ingestion_jobs()

        assert [j.name for j in jobs] == ["handbook_files"]
        assert jobs[0].dataset_name == "acme_docs_handbook"
        assert jobs[0].job.id == 1

    def test_get_files_ingestion_jobs_without_jobs(self, mocker):
        _client(mocker, {"count": 0})
        assert _connector().get_files_ingestion_jobs() == []


class TestExecutionFilesProgress:
    def test_keeps_the_camel_case_keys(self):
        execution = Execution.from_response_json(
            {"id": 4711, "state": "FINISHED", "filesProgress": _STATS},
            job=Mock(),
        )
        assert execution.files_progress == _STATS
        assert execution.files_progress["bytesCopied"] == 123456

    def test_none_for_other_jobs(self):
        execution = Execution.from_response_json(
            {"id": 1, "state": "FINISHED", "ingestionTargets": []}, job=Mock()
        )
        assert execution.files_progress is None

    def test_none_for_the_stub_without_a_result(self):
        execution = Execution.from_response_json(
            {
                "id": 1,
                "state": "RUNNING",
                "filesProgress": {
                    "datasetName": "d",
                    "prefix": "",
                    "connectorName": "c",
                },
            },
            job=Mock(),
        )
        assert execution.files_progress is None

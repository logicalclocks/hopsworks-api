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

from unittest import mock

from hopsworks_common.execution import Execution
from hsfs.core import execution


class TestExecution:
    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["execution"]["get"]["response"]

        # Act
        ex_list = execution.Execution.from_response_json(json, job=mock.Mock())

        # Assert
        assert len(ex_list) == 1
        ex = ex_list[0]
        assert ex.id == "test_id"
        assert ex.final_status == "test_final_status"
        assert ex.state == "test_state"

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["execution"]["get_empty"]["response"]

        # Act
        ex_list = execution.Execution.from_response_json(json, job=mock.Mock())

        # Assert
        assert len(ex_list) == 0

    def test_app_url_when_running_with_monitoring(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client._get_instance")
        mock_client.return_value._base_url = "https://myhost:443"

        ex = Execution(
            state="RUNNING",
            monitoring={"appUrl": "pythonapp/proj/my_app/"},
            job=mock.Mock(),
        )

        assert ex.app_url == "https://myhost:443/hopsworks-api/pythonapp/proj/my_app/"

    def test_app_url_when_running_with_prefixed_monitoring_path(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client._get_instance")
        mock_client.return_value._base_url = "https://myhost:443"

        ex = Execution(
            state="RUNNING",
            monitoring={"appUrl": "/hopsworks-api/pythonapp/proj/my_app/"},
            job=mock.Mock(),
        )

        assert ex.app_url == "https://myhost:443/hopsworks-api/pythonapp/proj/my_app/"

    def test_app_url_none_when_not_running(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")

        ex = Execution(
            state="FINISHED",
            monitoring={"appUrl": "pythonapp/proj/my_app/"},
            job=mock.Mock(),
        )

        assert ex.app_url is None

    def test_app_url_none_when_no_monitoring(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")

        ex = Execution(state="RUNNING", monitoring=None, job=mock.Mock())

        assert ex.app_url is None

    def test_app_url_none_when_monitoring_has_no_app_url(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")

        ex = Execution(
            state="RUNNING", monitoring={"otherKey": "value"}, job=mock.Mock()
        )

        assert ex.app_url is None

    def test_app_url_strips_trailing_slash_from_base_url(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client._get_instance")
        mock_client.return_value._base_url = "https://myhost:443/"

        ex = Execution(
            state="RUNNING",
            monitoring={"appUrl": "pythonapp/proj/my_app/"},
            job=mock.Mock(),
        )

        assert ex.app_url == "https://myhost:443/hopsworks-api/pythonapp/proj/my_app/"


class TestExecutionPerTable:
    def test_stop_table_delegates_to_api(self, mocker):
        job = mock.Mock()
        job.name = "crm_ingestion"
        ex = Execution(id=5, job=job)
        ex._execution_api = mock.Mock()

        ex.stop_table(2)

        ex._execution_api._stop_table.assert_called_once_with("crm_ingestion", 5, 2)

    def test_get_pod_logs_delegates_to_api(self, mocker):
        job = mock.Mock()
        job.name = "crm_ingestion"
        ex = Execution(id=5, job=job)
        ex._execution_api = mock.Mock()

        ex.get_pod_logs(table_index=2, lines=50)

        ex._execution_api._get_pod_logs.assert_called_once_with(
            "crm_ingestion", 5, 2, 50, None
        )

    def test_rerun_relaunches_in_place_and_refreshes_the_execution(self, mocker):
        mock_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi._rerun",
            return_value={
                "id": 33,
                "state": "INITIALIZING",
                "finalStatus": "UNDEFINED",
                "submissionTime": "2026-09-09T00:00:00Z",
                "args": "-start_time 2026-09-09T00:00:00Z",
                "rerunCount": 1,
            },
        )
        job = mock.Mock()
        job.name = "daily"
        ex = Execution(
            id=33,
            state="FAILED",
            final_status="FAILED",
            submission_time="2026-09-09T00:00:00Z",
            args="-start_time 2026-09-09T00:00:00Z",
            job=job,
        )

        ex.rerun()

        mock_api.assert_called_once_with("daily", 33)
        assert ex.id == 33
        assert ex.state == "INITIALIZING"
        assert ex.final_status == "UNDEFINED"
        assert ex.submission_time == "2026-09-09T00:00:00Z"
        assert ex.rerun_count == 1
        # The refresh must not detach the execution from its job.
        assert ex.job_name == "daily"

    def test_rerun_count_defaults_to_zero_for_older_backends(self):
        ex = Execution(id=1, state="FAILED", job=mock.Mock())

        assert ex.rerun_count == 0

    def test_files_progress_keeps_the_backend_keys(self):
        stats = {"status": "ok", "bytesCopied": 10, "failedFiles": []}

        ex_list = execution.Execution.from_response_json(
            {
                "count": 1,
                "items": [{"id": 4711, "state": "FINISHED", "filesProgress": stats}],
            },
            job=mock.Mock(),
        )

        assert ex_list[0].files_progress == stats
        assert ex_list[0].files_progress["bytesCopied"] == 10

    def test_files_progress_none_for_other_executions(self):
        ex = execution.Execution.from_response_json(
            {"id": 1, "state": "FINISHED"}, job=mock.Mock()
        )

        assert ex.files_progress is None

    def test_update_from_response_json_keeps_files_progress(self):
        ex = Execution(id=1, state="RUNNING", job=mock.Mock())

        ex.update_from_response_json(
            {"id": 1, "state": "FINISHED", "filesProgress": {"copied": 2}}
        )

        assert ex.files_progress == {"copied": 2}

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

import pytest
from hopsworks_common.client.exceptions import JobExecutionException
from hopsworks_common.engine.execution_engine import ExecutionEngine
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
            {
                "id": 1,
                "state": "FINISHED",
                "filesProgress": {"status": "ok", "copied": 2},
            }
        )

        assert ex.files_progress == {"status": "ok", "copied": 2}

    def test_files_progress_none_for_the_stub_without_a_result(self):
        ex = execution.Execution.from_response_json(
            {
                "id": 1,
                "state": "RUNNING",
                "filesProgress": {
                    "datasetName": "d",
                    "prefix": "p/",
                    "connectorName": "c",
                },
            },
            job=mock.Mock(),
        )

        assert ex.files_progress is None


class TestExecutionAttemptLogs:
    RERUN_RESPONSE = {
        "id": 7,
        "rerunCount": 2,
        "stdoutPath": "/Projects/p/Logs/Python/j/7/stdout.log",
        "stderrPath": "/Projects/p/Logs/Python/j/7/stderr.log",
        "attempts": [
            {
                "attempt": 1,
                "stdoutPath": "/Projects/p/Logs/Python/j/7/attempt-1/stdout.log",
                "stderrPath": "/Projects/p/Logs/Python/j/7/attempt-1/stderr.log",
            },
            # a Spark-style attempt: no stdout file
            {
                "attempt": 2,
                "stderrPath": "/Projects/p/Logs/Python/j/7/attempt-2/stderr.log",
            },
        ],
    }

    def _execution(self):
        return Execution.from_response_json(self.RERUN_RESPONSE, job=mock.Mock())

    def test_attempts_are_parsed_from_the_response(self):
        ex = self._execution()

        assert [a["attempt"] for a in ex.attempts] == [1, 2]
        assert (
            ex.attempts[0]["stdout_path"]
            == "/Projects/p/Logs/Python/j/7/attempt-1/stdout.log"
        )

    def test_attempts_carry_how_each_ended_and_when_it_ran(self):
        ex = Execution.from_response_json(
            {
                "id": 7,
                "rerunCount": 2,
                "state": "FINISHED",
                "finalStatus": "SUCCEEDED",
                "attempts": [
                    {
                        "attempt": 1,
                        "state": "FAILED",
                        "finalStatus": "FAILED",
                        "startTime": 1000,
                        "stopTime": 9000,
                    },
                    # set aside before outcomes were recorded
                    {"attempt": 2, "stopTime": 12000},
                ],
            },
            job=mock.Mock(),
        )

        first, second = ex.attempts
        assert first["state"] == "FAILED"
        assert first["final_status"] == "FAILED"
        assert (first["start_time"], first["stop_time"]) == (1000, 9000)
        assert "final_status" not in second
        assert second["stop_time"] == 12000
        # the current attempt is the execution itself
        assert ex.final_status == "SUCCEEDED"

    def test_attempts_empty_when_never_rerun(self):
        assert Execution(id=1, job=mock.Mock()).attempts == []

    def test_latest_attempt_uses_the_execution_paths(self):
        ex = self._execution()

        expected = (ex.stdout_path, ex.stderr_path)
        assert ExecutionEngine._attempt_log_paths(ex, None) == expected
        assert ExecutionEngine._attempt_log_paths(ex, 3) == expected

    def test_earlier_attempt_uses_its_own_paths(self):
        ex = self._execution()

        assert ExecutionEngine._attempt_log_paths(ex, 1) == (
            "/Projects/p/Logs/Python/j/7/attempt-1/stdout.log",
            "/Projects/p/Logs/Python/j/7/attempt-1/stderr.log",
        )
        assert ExecutionEngine._attempt_log_paths(ex, 2) == (
            None,
            "/Projects/p/Logs/Python/j/7/attempt-2/stderr.log",
        )

    @pytest.mark.parametrize("attempt", [0, 4])
    def test_unknown_attempt_is_refused(self, attempt):
        with pytest.raises(JobExecutionException, match="attempts 1 to 3"):
            ExecutionEngine._attempt_log_paths(self._execution(), attempt)

    def _engine(self, fresh, existing=None):
        """An engine whose backend returns `fresh` and holds the `existing` files (all by default)."""
        engine = ExecutionEngine()
        engine._execution_api = mock.Mock()
        engine._execution_api._get.return_value = fresh
        engine._dataset_api = mock.Mock()
        engine._dataset_api.exists.side_effect = lambda path: (
            existing is None or path in existing
        )
        engine._dataset_api.download.side_effect = lambda path, *_, **__: path
        return engine

    def test_download_logs_fetches_the_chosen_attempt(self, tmp_path):
        ex = self._execution()
        ex._job.name = "j"
        engine = self._engine(fresh=ex)

        out, err = engine._download_logs(ex, str(tmp_path), attempt=1)

        assert out == "/Projects/p/Logs/Python/j/7/attempt-1/stdout.log"
        assert err == "/Projects/p/Logs/Python/j/7/attempt-1/stderr.log"
        assert [d.name.split("_")[0] for d in tmp_path.iterdir()] == [
            "logs-job-j-exec-7-attempt-1"
        ]

    def test_explicit_attempt_is_resolved_against_the_backend_not_a_stale_object(
        self, tmp_path
    ):
        # fetched before anyone re-ran it: still believes it is on its first attempt
        stale = Execution(
            id=7,
            stdout_path="/Projects/p/Logs/Python/j/7/stdout.log",
            stderr_path="/Projects/p/Logs/Python/j/7/stderr.log",
            job=mock.Mock(),
        )
        stale._job.name = "j"
        fresh = self._execution()
        fresh._job.name = "j"
        engine = self._engine(fresh=fresh)

        out, _ = engine._download_logs(stale, str(tmp_path), attempt=1)

        engine._execution_api._get.assert_called_once_with(stale._job, 7)
        assert out == "/Projects/p/Logs/Python/j/7/attempt-1/stdout.log"
        # attempt 3 exists by now although the stale object has never heard of it
        out, _ = engine._download_logs(stale, str(tmp_path), attempt=3)
        assert out == "/Projects/p/Logs/Python/j/7/stdout.log"

    def test_default_download_does_not_refetch(self, tmp_path):
        ex = self._execution()
        ex._job.name = "j"
        engine = self._engine(fresh=ex)

        engine._download_logs(ex, str(tmp_path))

        engine._execution_api._get.assert_not_called()

    def test_earlier_attempt_with_no_files_is_an_error(self, tmp_path):
        ex = self._execution()
        ex._job.name = "j"
        engine = self._engine(fresh=ex, existing=set())

        with pytest.raises(JobExecutionException, match="No log files were kept"):
            engine._download_logs(ex, str(tmp_path), attempt=1)

    def test_earlier_attempt_with_one_stream_returns_none_for_the_other(self, tmp_path):
        ex = self._execution()
        ex._job.name = "j"
        engine = self._engine(fresh=ex)

        out, err = engine._download_logs(ex, str(tmp_path), attempt=2)

        assert out is None
        assert err == "/Projects/p/Logs/Python/j/7/attempt-2/stderr.log"

    def test_latest_attempt_without_files_still_returns_nones(self, tmp_path):
        # the latest attempt's logs may simply not be archived yet
        ex = self._execution()
        ex._job.name = "j"
        engine = self._engine(fresh=ex, existing=set())

        assert engine._download_logs(ex, str(tmp_path), attempt=3) == (None, None)

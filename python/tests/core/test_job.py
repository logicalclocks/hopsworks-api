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


import pytest
from hsfs.client import exceptions
from hsfs.core import execution, job


class TestJob:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        json = backend_fixtures["job"]["get"]["response"]

        # Act
        j = job.Job.from_response_json(json)

        # Assert
        assert j.id == "test_id"
        assert j.name == "test_name"
        assert j.executions == "test_executions"
        assert j.href == "test_href"

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        json = backend_fixtures["job"]["get_empty"]["response"]

        # Act
        j = job.Job.from_response_json(json)

        # Assert
        assert j.id == "test_id"
        assert j.name == "test_name"
        assert j.executions is None
        assert j.href is None

    def test_wait_for_job(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=mocker.Mock()
        )

        json = backend_fixtures["job"]["get"]["response"]
        x = job.Job.from_response_json(json).run(await_termination=False)

        # Act
        x.await_termination()

        # Assert
        assert mock_execution_api.return_value._get.call_count == 1

    def test_wait_for_job_wait_for_job_false(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_job_api = mocker.patch("hopsworks_common.core.execution_api.ExecutionApi")

        json = backend_fixtures["job"]["get"]["response"]
        job.Job.from_response_json(json).run(await_termination=False)

        # Assert
        assert mock_job_api.return_value._get.call_count == 0

    def test_wait_for_job_final_status_succeeded(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=mocker.Mock()
        )

        json = backend_fixtures["job"]["get"]["response"]
        x = job.Job.from_response_json(json).run(await_termination=False)

        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state=None, final_status="SUCCEEDED", job=mocker.Mock()
        )

        # Act
        x.await_termination()

        # Assert
        assert mock_execution_api.return_value._get.call_count == 1

    def test_wait_for_job_final_status_failed(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        pyspark_job_mock = mocker.Mock()
        pyspark_job_mock.job_type = "PYSPARK"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=pyspark_job_mock
        )

        json = backend_fixtures["job"]["get"]["response"]
        x = job.Job.from_response_json(json).run(await_termination=False)

        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state="FINISHED", final_status="FAILED", job=pyspark_job_mock
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            x.await_termination()

        # Assert
        assert mock_execution_api.return_value._get.call_count == 1
        assert (
            str(e_info.value)
            == "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
        )

    def test_wait_for_job_final_status_killed(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        pyspark_job_mock = mocker.Mock()
        pyspark_job_mock.job_type = "PYSPARK"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=pyspark_job_mock
        )

        json = backend_fixtures["job"]["get"]["response"]
        x = job.Job.from_response_json(json).run(await_termination=False)

        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state="FINISHED", final_status="KILLED", job=pyspark_job_mock
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            x.await_termination()

        # Assert
        assert mock_execution_api.return_value._get.call_count == 1
        assert str(e_info.value) == "The Hopsworks Job was stopped"

    def test_run_await_termination_succeeds_on_success(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=mocker.Mock()
        )
        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state=None, final_status="SUCCEEDED", job=mocker.Mock()
        )

        json = backend_fixtures["job"]["get"]["response"]

        # Act — should not raise
        result = job.Job.from_response_json(json).run(await_termination=True)

        # Assert
        assert result is not None
        assert result.success is True

    def test_wait_for_job_internal_raises_on_failed(self, mocker, backend_fixtures):
        # Arrange — _wait_for_job is used by feature store ingestion jobs
        mocker.patch("hopsworks_common.client.get_instance")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_job_api = mocker.patch("hopsworks_common.core.job_api.JobApi")
        pyspark_job_mock = mocker.Mock()
        pyspark_job_mock.job_type = "PYSPARK"
        failed_execution = execution.Execution(
            id=1, state="FINISHED", final_status="FAILED", job=pyspark_job_mock
        )
        mock_job_api.return_value.last_execution.return_value = [failed_execution]
        mock_execution_api.return_value._get.return_value = failed_execution

        j = job.Job(
            id="test_id",
            name="test_name",
            creation_time=None,
            config={},
            job_type="PYSPARK",
            creator=None,
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            j._wait_for_job(await_termination=True)

        # Assert
        assert (
            str(e_info.value)
            == "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
        )

    def test_wait_for_job_internal_no_raise_when_await_false(
        self, mocker, backend_fixtures
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )

        json = backend_fixtures["job"]["get"]["response"]
        j = job.Job.from_response_json(json)

        # Act — should not raise and should not poll
        j._wait_for_job(await_termination=False)

        # Assert
        assert mock_execution_api.return_value._get.call_count == 0

    def test_run_await_termination_pyspark_raises_on_failed(
        self, mocker, backend_fixtures
    ):
        # Arrange — PYSPARK (YARN) jobs report failure via final_status, state stays FINISHED
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        pyspark_job_mock = mocker.Mock()
        pyspark_job_mock.job_type = "PYSPARK"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=pyspark_job_mock
        )
        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state="FINISHED", final_status="FAILED", job=pyspark_job_mock
        )

        j = job.Job(
            id="test_id",
            name="test_name",
            creation_time=None,
            config={},
            job_type="PYSPARK",
            creator=None,
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            j.run(await_termination=True)

        # Assert
        assert (
            str(e_info.value)
            == "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
        )

    def test_run_await_termination_pyspark_raises_on_framework_failure(
        self, mocker, backend_fixtures
    ):
        # Arrange — PYSPARK (YARN) jobs report framework failure via final_status
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        pyspark_job_mock = mocker.Mock()
        pyspark_job_mock.job_type = "PYSPARK"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=pyspark_job_mock
        )
        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1,
            state="FINISHED",
            final_status="FRAMEWORK_FAILURE",
            job=pyspark_job_mock,
        )

        j = job.Job(
            id="test_id",
            name="test_name",
            creation_time=None,
            config={},
            job_type="PYSPARK",
            creator=None,
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            j.run(await_termination=True)

        # Assert
        assert (
            str(e_info.value)
            == "The Hopsworks Job monitoring failed, could not determine the final status"
        )

    def test_run_await_termination_pyspark_raises_on_killed(
        self, mocker, backend_fixtures
    ):
        # Arrange — PYSPARK (YARN) jobs report killed via final_status
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        pyspark_job_mock = mocker.Mock()
        pyspark_job_mock.job_type = "PYSPARK"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=pyspark_job_mock
        )
        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state="FINISHED", final_status="KILLED", job=pyspark_job_mock
        )

        j = job.Job(
            id="test_id",
            name="test_name",
            creation_time=None,
            config={},
            job_type="PYSPARK",
            creator=None,
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            j.run(await_termination=True)

        # Assert
        assert str(e_info.value) == "The Hopsworks Job was stopped"

    def test_run_await_termination_python_raises_on_failed(
        self, mocker, backend_fixtures
    ):
        # Arrange — PYTHON (non-YARN) jobs report failure via state
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        python_job_mock = mocker.Mock()
        python_job_mock.job_type = "PYTHON"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=python_job_mock
        )
        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state="FAILED", final_status="UNDEFINED", job=python_job_mock
        )

        j = job.Job(
            id="test_id",
            name="test_name",
            creation_time=None,
            config={},
            job_type="PYTHON",
            creator=None,
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            j.run(await_termination=True)

        # Assert
        assert (
            str(e_info.value)
            == "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
        )

    def test_run_await_termination_python_raises_on_killed(
        self, mocker, backend_fixtures
    ):
        # Arrange — PYTHON (non-YARN) jobs report killed via state
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        python_job_mock = mocker.Mock()
        python_job_mock.job_type = "PYTHON"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            job=python_job_mock
        )
        mock_execution_api.return_value._get.return_value = execution.Execution(
            id=1, state="KILLED", final_status="UNDEFINED", job=python_job_mock
        )

        j = job.Job(
            id="test_id",
            name="test_name",
            creation_time=None,
            config={},
            job_type="PYTHON",
            creator=None,
        )

        # Act
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            j.run(await_termination=True)

        # Assert
        assert str(e_info.value) == "The Hopsworks Job was stopped"

    # --- PYTHON_APP tests ---

    def test_run_python_app_waits_for_running(self, mocker, backend_fixtures):
        # Arrange — PYTHON_APP calls wait_for_running, not wait_until_finished
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_engine = mocker.patch(
            "hopsworks_common.engine.execution_engine.ExecutionEngine",
        )

        python_app_job_mock = mocker.Mock()
        python_app_job_mock.job_type = "PYTHON_APP"
        started_execution = execution.Execution(
            id=1,
            state="INITIALIZING",
            job=python_app_job_mock,
            monitoring={"appUrl": "pythonapp/proj/myapp/"},
        )
        running_execution = execution.Execution(
            id=1,
            state="RUNNING",
            job=python_app_job_mock,
            monitoring={"appUrl": "pythonapp/proj/myapp/"},
        )
        mock_execution_api.return_value._start.return_value = started_execution
        mock_execution_engine.return_value.wait_for_running.return_value = (
            running_execution
        )

        j = job.Job(
            id="test_id",
            name="myapp",
            creation_time=None,
            config={},
            job_type="PYTHON_APP",
            creator=None,
        )

        # Act
        result = j.run()

        # Assert — wait_for_running called, wait_until_finished NOT called
        assert mock_execution_engine.return_value.wait_for_running.call_count == 1
        assert mock_execution_engine.return_value.wait_until_finished.call_count == 0
        assert result.state == "RUNNING"

    def test_run_python_app_does_not_await_termination(self, mocker, backend_fixtures):
        # Arrange — PYTHON_APP ignores await_termination flag
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_engine = mocker.patch(
            "hopsworks_common.engine.execution_engine.ExecutionEngine",
        )

        python_app_job_mock = mocker.Mock()
        python_app_job_mock.job_type = "PYTHON_APP"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            id=1, state="INITIALIZING", job=python_app_job_mock
        )
        mock_execution_engine.return_value.wait_for_running.return_value = (
            execution.Execution(id=1, state="RUNNING", job=python_app_job_mock)
        )

        j = job.Job(
            id="test_id",
            name="myapp",
            creation_time=None,
            config={},
            job_type="PYTHON_APP",
            creator=None,
        )

        # Act — even with await_termination=True, should not call wait_until_finished
        j.run(await_termination=True)

        # Assert
        assert mock_execution_engine.return_value.wait_until_finished.call_count == 0

    def test_run_python_app_raises_on_failure(self, mocker, backend_fixtures):
        # Arrange — PYTHON_APP that fails during startup
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.execution.Execution.get_url")
        mock_execution_api = mocker.patch(
            "hopsworks_common.core.execution_api.ExecutionApi",
        )
        mock_execution_engine = mocker.patch(
            "hopsworks_common.engine.execution_engine.ExecutionEngine",
        )

        python_app_job_mock = mocker.Mock()
        python_app_job_mock.job_type = "PYTHON_APP"
        mock_execution_api.return_value._start.return_value = execution.Execution(
            id=1, state="INITIALIZING", job=python_app_job_mock
        )
        mock_execution_engine.return_value.wait_for_running.side_effect = (
            exceptions.JobExecutionException(
                "Python App failed to start. State: FAILED"
            )
        )

        j = job.Job(
            id="test_id",
            name="myapp",
            creation_time=None,
            config={},
            job_type="PYTHON_APP",
            creator=None,
        )

        # Act + Assert
        with pytest.raises(exceptions.JobExecutionException) as e_info:
            j.run()

        assert "Python App failed to start" in str(e_info.value)


class TestExecution:
    def test_app_url_with_monitoring(self, mocker):
        # Arrange
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        mock_client.return_value._base_url = "https://myhost:443"

        ex = execution.Execution(
            id=1,
            state="RUNNING",
            monitoring={"appUrl": "pythonapp/proj/myapp/"},
            job=mocker.Mock(),
        )

        # Act + Assert
        assert ex.app_url == "https://myhost:443/hopsworks-api/pythonapp/proj/myapp/"

    def test_app_url_without_monitoring(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        ex = execution.Execution(
            id=1, state="RUNNING", monitoring=None, job=mocker.Mock()
        )

        # Act + Assert
        assert ex.app_url is None

    def test_app_url_with_empty_monitoring(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        ex = execution.Execution(
            id=1, state="RUNNING", monitoring={}, job=mocker.Mock()
        )

        # Act + Assert
        assert ex.app_url is None

    def test_app_url_monitoring_without_app_url_key(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        ex = execution.Execution(
            id=1,
            state="RUNNING",
            monitoring={"sparkUrl": "some/spark/url"},
            job=mocker.Mock(),
        )

        # Act + Assert
        assert ex.app_url is None

    def test_app_url_not_running(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        ex = execution.Execution(
            id=1,
            state="KILLED",
            monitoring={"appUrl": "pythonapp/proj/myapp/"},
            job=mocker.Mock(),
        )

        # Act + Assert
        assert ex.app_url is None

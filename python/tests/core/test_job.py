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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
        mock_job_api = mocker.patch("hopsworks_common.core.execution_api.ExecutionApi")

        json = backend_fixtures["job"]["get"]["response"]
        job.Job.from_response_json(json).run(await_termination=False)

        # Assert
        assert mock_job_api.return_value._get.call_count == 0

    def test_wait_for_job_final_status_succeeded(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
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
        # Arrange — PYTHON_APP calls _wait_for_running, not _wait_until_finished
        mocker.patch("hopsworks_common.client._get_instance")
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
        mock_execution_engine.return_value._wait_for_running.return_value = (
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

        # Assert — _wait_for_running called, _wait_until_finished NOT called
        assert mock_execution_engine.return_value._wait_for_running.call_count == 1
        assert mock_execution_engine.return_value._wait_until_finished.call_count == 0
        assert result.state == "RUNNING"

    def test_run_python_app_does_not_await_termination(self, mocker, backend_fixtures):
        # Arrange — PYTHON_APP ignores await_termination flag
        mocker.patch("hopsworks_common.client._get_instance")
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
        mock_execution_engine.return_value._wait_for_running.return_value = (
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

        # Act — even with await_termination=True, should not call _wait_until_finished
        j.run(await_termination=True)

        # Assert
        assert mock_execution_engine.return_value._wait_until_finished.call_count == 0

    def test_run_python_app_raises_on_failure(self, mocker, backend_fixtures):
        # Arrange — PYTHON_APP that fails during startup
        mocker.patch("hopsworks_common.client._get_instance")
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
        mock_execution_engine.return_value._wait_for_running.side_effect = (
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
        mock_client = mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")

        ex = execution.Execution(
            id=1, state="RUNNING", monitoring=None, job=mocker.Mock()
        )

        # Act + Assert
        assert ex.app_url is None

    def test_app_url_with_empty_monitoring(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")

        ex = execution.Execution(
            id=1, state="RUNNING", monitoring={}, job=mocker.Mock()
        )

        # Act + Assert
        assert ex.app_url is None

    def test_app_url_monitoring_without_app_url_key(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")

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
        mocker.patch("hopsworks_common.client._get_instance")

        ex = execution.Execution(
            id=1,
            state="KILLED",
            monitoring={"appUrl": "pythonapp/proj/myapp/"},
            job=mocker.Mock(),
        )

        # Act + Assert
        assert ex.app_url is None


class TestJobDescription:
    def _job(self, mocker, **kwargs):
        mocker.patch("hopsworks_common.client._get_instance")
        return job.Job(
            id=1,
            name="test",
            creation_time=None,
            config={"appPath": "app.py"},
            job_type="PYTHON",
            creator=None,
            **kwargs,
        )

    def test_description_from_constructor(self, mocker):
        j = self._job(mocker, description="scalar description")

        assert j.description == "scalar description"

    def test_description_falls_back_to_config(self, mocker):
        # An old server does not send the scalar field; the config carries it.
        j = self._job(mocker)
        j._config["description"] = "config description"

        assert j.description == "config description"

    def test_description_none_when_absent_everywhere(self, mocker):
        j = self._job(mocker)

        assert j.description is None

    def test_description_setter_writes_config_too(self, mocker):
        # save() persists the config, so the setter must write it there.
        j = self._job(mocker)

        j.description = "new description"

        assert j._description == "new description"
        assert j._config["description"] == "new description"

    def test_config_setter_survives(self, mocker):
        # A duplicate `config` property definition used to shadow this setter.
        j = self._job(mocker)

        j.config = {"appPath": "other.py"}

        assert j.config == {"appPath": "other.py"}


class TestJobTags:
    def _job(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mock_job_api = mocker.patch("hopsworks_common.core.job_api.JobApi")
        j = job.Job(
            id=1,
            name="test",
            creation_time=None,
            config={},
            job_type="PYTHON",
            creator=None,
        )
        return j, mock_job_api.return_value

    def test_add_tag_delegates(self, mocker):
        j, api = self._job(mocker)

        j.add_tag("meta", {"k": "v"})

        api._add_tag.assert_called_once_with(j, "meta", {"k": "v"})

    def test_delete_tag_delegates(self, mocker):
        j, api = self._job(mocker)

        j.delete_tag("meta")

        api._delete_tag.assert_called_once_with(j, "meta")

    def test_get_tag_delegates(self, mocker):
        j, api = self._job(mocker)
        api._get_tag.return_value = "v"

        assert j.get_tag("meta") == "v"
        api._get_tag.assert_called_once_with(j, "meta")

    def test_get_tags_delegates(self, mocker):
        j, api = self._job(mocker)
        api._get_tags.return_value = {"meta": "v"}

        assert j.get_tags() == {"meta": "v"}

    def test_get_tag_metadata_delegates(self, mocker):
        j, api = self._job(mocker)
        tag_obj = mocker.Mock()
        api._get_tags_metadata.return_value = {"meta": tag_obj}

        assert j.get_tag_metadata("meta") is tag_obj
        api._get_tags_metadata.assert_called_once_with(j, "meta")

    def test_get_tag_metadata_missing_is_none(self, mocker):
        j, api = self._job(mocker)
        api._get_tags_metadata.return_value = {}

        assert j.get_tag_metadata("meta") is None

    def test_get_tags_metadata_delegates(self, mocker):
        j, api = self._job(mocker)
        tag_obj = mocker.Mock()
        api._get_tags_metadata.return_value = {"meta": tag_obj}

        assert j.get_tags_metadata() == {"meta": tag_obj}
        api._get_tags_metadata.assert_called_once_with(j)


class TestJobProjectBinding:
    """A job addresses the project it came from, not the connection's."""

    def _job(self):
        from hopsworks_common.job import Job

        return Job(
            id=7,
            name="nightly",
            creation_time="2026-08-03T00:00:00Z",
            config={"type": "sparkJobConfiguration"},
            job_type="SPARK",
            creator=None,
        )

    def test_unbound_job_uses_the_connection(self):
        job = self._job()
        assert job._project_id is None
        assert job._job_api._project_id is None

    def test_binding_rebuilds_every_handle(self):
        job = self._job()
        job._bind_project(42, "other_project")
        # All of them, not only the job handle: tags used to be bound while
        # executions, alerts and the URL still addressed the login project.
        assert job._job_api._project_id == 42
        assert job._job_api._project_name == "other_project"
        assert job._execution_api._project_id == 42
        assert job._alerts_api._project_id == 42
        assert job._alerts_api._project_name == "other_project"
        # The engine owns the handles that awaiting and log download go through.
        assert job._execution_engine._execution_api._project_id == 42
        assert job._execution_engine._dataset_api._project_id == 42
        assert job._execution_engine._dataset_api._project_name == "other_project"

    def test_every_project_handle_addresses_that_project(self, mocker):
        """Every getter on a foreign Project, not only the ones a review happened to name.

        The defect is always the same shape: a handle built with no project reads the connection's
        when it is called, and answers successfully for the wrong project.
        """
        from hopsworks_common.project import Project

        mocker.patch(
            "hopsworks_common.client._get_instance",
            return_value=mocker.Mock(_project_id=5, _project_name="login_project"),
        )
        b = Project(project_id=42, project_name="project_b")

        for handle in (
            b.get_job_api(),
            b.get_dataset_api(),
            b.get_alerts_api(),
            b.get_app_api(),
            b.get_git_api(),
            b.get_environment_api(),
            b.get_kafka_api(),
            b.get_opensearch_api(),
            b.get_search_api(),
        ):
            assert handle._pid() == 42, (
                f"{type(handle).__name__} addresses the wrong project"
            )
            assert handle._pname() == "project_b", (
                f"{type(handle).__name__} carries the wrong project name"
            )

    def test_executions_of_a_bound_job_stay_bound(self, mocker):
        job = self._job()
        job._bind_project(42, "other_project")
        instance = mocker.Mock(_project_id=5, _project_name="login_project")
        mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
        mocker.patch(
            "hopsworks_common.util._get_hostname_replaced_url", side_effect=lambda p: p
        )
        instance._send_request.return_value = {
            "count": 1,
            "items": [{"id": 3, "state": "RUNNING"}],
        }

        execution = job.get_executions()[0]

        assert execution._project_id == 42
        assert execution._execution_api._project_id == 42
        assert execution._execution_engine._execution_api._project_id == 42
        assert execution.get_url() == "/p/42/jobs/named/nightly/executions"

    def test_refreshing_an_execution_keeps_its_project(self, mocker):
        job = self._job()
        job._bind_project(42, "other_project")
        from hopsworks_common.execution import Execution

        execution = Execution.from_response_json({"id": 3, "state": "RUNNING"}, job)

        execution.update_from_response_json({"id": 3, "state": "FINISHED"})

        # Re-initialising used to drop the job entirely, which rebound every handle to the login
        # project and left job_name raising.
        assert execution.job_name == "nightly"
        assert execution._project_id == 42
        assert execution._execution_api._project_id == 42

    def test_executions_address_the_bound_project(self, mocker):
        job = self._job()
        job._bind_project(42, "other_project")
        instance = mocker.Mock(_project_id=5, _project_name="login_project")
        mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
        instance._send_request.return_value = {"count": 0, "items": []}

        job._execution_api._get_all(job)

        path_params = instance._send_request.call_args[0][1]
        assert path_params[:2] == ["project", 42], path_params

    def test_alerts_address_the_bound_project(self, mocker):
        job = self._job()
        job._bind_project(42, "other_project")
        instance = mocker.Mock(_project_id=5, _project_name="login_project")
        mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
        instance._send_request.return_value = {"count": 0, "items": []}

        job._alerts_api.get_job_alerts(job.name)

        path_params = instance._send_request.call_args[0][1]
        assert path_params[:2] == ["project", 42], path_params

    def test_url_points_at_the_bound_project(self, mocker):
        job = self._job()
        job._bind_project(42, "other_project")
        mocker.patch(
            "hopsworks_common.client._get_instance",
            return_value=mocker.Mock(_project_id=5, _project_name="login_project"),
        )
        mocker.patch(
            "hopsworks_common.util._get_hostname_replaced_url", side_effect=lambda p: p
        )

        assert job.get_url() == "/p/42/jobs/named/nightly"

    def test_an_unbound_job_still_uses_the_connection(self, mocker):
        job = self._job()
        instance = mocker.Mock(_project_id=5, _project_name="login_project")
        mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
        instance._send_request.return_value = {"count": 0, "items": []}

        job._execution_api._get_all(job)

        path_params = instance._send_request.call_args[0][1]
        assert path_params[:2] == ["project", 5], path_params

    def test_job_api_stamps_the_jobs_it_returns(self, mocker):
        from hopsworks_common.core import job_api

        api = job_api.JobApi(project_id=42, project_name="other_project")
        job = self._job()
        assert api._bind(job) is job
        assert job._job_api._project_id == 42

    def test_bind_tolerates_a_list_and_none(self):
        from hopsworks_common.core import job_api

        api = job_api.JobApi(project_id=9, project_name="p")
        jobs = [self._job(), None]
        api._bind(jobs)
        assert jobs[0]._job_api._project_id == 9

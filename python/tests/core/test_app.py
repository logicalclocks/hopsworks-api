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

import pytest
from hopsworks_common.app import App
from hopsworks_common.core.app_api import AppApi
from hsfs.client import exceptions


class TestApp:
    def test_from_response_json(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        json_data = {
            "jobId": 42,
            "name": "my_app",
            "state": "RUNNING",
            "finalStatus": "UNDEFINED",
            "serving": True,
            "appUrl": "pythonapp/proj/my_app/",
            "appPath": "hdfs:///Projects/proj/Resources/app.py",
            "executionId": 10,
            "executionStart": 1000000,
            "creator": "user@test.com",
            "creatorFirstname": "Test",
            "creatorLastname": "User",
            "environmentName": "python-feature-pipeline",
            "cpuRequested": "1.0",
            "memoryRequested": "2048Mi",
        }

        app = App.from_response_json(json_data)

        assert app.name == "my_app"
        assert app.state == "RUNNING"
        assert app.serving is True
        assert app.execution_id == 10
        assert app.environment_name == "python-feature-pipeline"
        assert app.cpu_requested == "1.0"
        assert app.memory_requested == "2048Mi"
        assert app.app_path == "hdfs:///Projects/proj/Resources/app.py"

    def test_from_response_json_list(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        json_list = [
            {"jobId": 1, "name": "app1", "state": "RUNNING", "serving": True},
            {"jobId": 2, "name": "app2", "state": "KILLED", "serving": False},
        ]

        apps = App.from_response_json_list(json_list)

        assert len(apps) == 2
        assert apps[0].name == "app1"
        assert apps[1].name == "app2"

    def test_from_response_json_list_with_collection_wrapper(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        json_data = {
            "count": 2,
            "items": [
                {"jobId": 1, "name": "app1", "state": "RUNNING", "serving": True},
                {"jobId": 2, "name": "app2", "state": "KILLED", "serving": False},
            ],
        }

        apps = App.from_response_json_list(json_data)

        assert len(apps) == 2
        assert apps[0].name == "app1"
        assert apps[1].name == "app2"

    def test_app_url_when_serving(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        mock_client.return_value._base_url = "https://myhost:443"

        app = App(
            name="my_app",
            state="RUNNING",
            serving=True,
            app_url="pythonapp/proj/my_app/",
        )

        assert app.app_url == "https://myhost:443/hopsworks-api/pythonapp/proj/my_app/"

    def test_app_url_when_not_serving(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        app = App(
            name="my_app",
            state="RUNNING",
            serving=False,
            app_url="pythonapp/proj/my_app/",
        )

        assert app.app_url is None

    def test_app_url_when_no_url(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        app = App(name="my_app", state="KILLED", serving=False)

        assert app.app_url is None

    def test_run_waits_for_serving(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.app.time.sleep")
        mock_api = mocker.patch(
            "hopsworks_common.core.app_api.AppApi",
        )

        # First poll: not serving, second poll: serving
        not_serving = App(name="my_app", state="RUNNING", serving=False)
        serving = App(
            name="my_app",
            state="RUNNING",
            serving=True,
            app_url="pythonapp/proj/my_app/",
        )
        mock_api.return_value.get_app.side_effect = [not_serving, serving]

        app = App(name="my_app", state="STOPPED")
        app._app_api = mock_api.return_value

        result = app.run(await_serving=True)

        assert mock_api.return_value._start.call_count == 1
        assert result._serving is True

    def test_run_raises_on_failure(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch(
            "hopsworks_common.core.app_api.AppApi",
        )

        failed = App(name="my_app", state="FAILED", serving=False)
        mock_api.return_value.get_app.return_value = failed

        app = App(name="my_app", state="STOPPED")
        app._app_api = mock_api.return_value

        with pytest.raises(exceptions.JobExecutionException) as e_info:
            app.run(await_serving=True)

        assert "App failed to start" in str(e_info.value)

    def test_stop(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch(
            "hopsworks_common.core.app_api.AppApi",
        )

        stopped = App(name="my_app", state="KILLED", serving=False)
        mock_api.return_value.get_app.return_value = stopped

        app = App(name="my_app", state="RUNNING", execution_id=10)
        app._app_api = mock_api.return_value

        result = app.stop()

        mock_api.return_value._stop.assert_called_once_with("my_app", 10)
        assert result._state == "KILLED"

    def test_delete(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch(
            "hopsworks_common.core.app_api.AppApi",
        )

        app = App(name="my_app", state="STOPPED")
        app._app_api = mock_api.return_value

        app.delete()

        mock_api.return_value._delete.assert_called_once_with("my_app")

    def test_get_logs(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")
        mock_api.return_value._get_log.side_effect = [
            {"type": "out", "log": "stdout content"},
            {"type": "err", "log": "stderr content"},
        ]

        app = App(name="my_app", state="KILLED", execution_id=10)
        app._app_api = mock_api.return_value

        logs = app.get_logs()

        assert logs == {"stdout": "stdout content", "stderr": "stderr content"}
        mock_api.return_value._get_log.assert_has_calls(
            [
                mocker.call("my_app", 10, "out"),
                mocker.call("my_app", 10, "err"),
            ]
        )

    def test_get_logs_normalizes_missing_logs(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")
        mock_api.return_value._get_log.side_effect = [
            {"type": "out"},
            {"type": "err", "log": None},
        ]

        app = App(name="my_app", state="KILLED", execution_id=10)
        app._app_api = mock_api.return_value

        assert app.get_logs() == {"stdout": "", "stderr": ""}

    def test_get_logs_normalizes_empty_log_responses(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")
        mock_api.return_value._get_log.side_effect = [{}, {}]

        app = App(name="my_app", state="KILLED", execution_id=10)
        app._app_api = mock_api.return_value

        assert app.get_logs() == {"stdout": "", "stderr": ""}

    def test_get_logs_normalizes_none_log_responses(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")
        mock_api.return_value._get_log.side_effect = [None, None]

        app = App(name="my_app", state="KILLED", execution_id=10)
        app._app_api = mock_api.return_value

        assert app.get_logs() == {"stdout": "", "stderr": ""}

    def test_get_log_normalizes_empty_backend_response(self, mocker):
        mock_client = mocker.Mock()
        mock_client._project_id = 99
        mock_client._send_request.return_value = None
        mocker.patch("hopsworks_common.client.get_instance", return_value=mock_client)

        logs = AppApi()._get_log("my_app", 10, "out")

        assert logs == {}
        mock_client._send_request.assert_called_once_with(
            "GET",
            ["project", 99, "jobs", "my_app", "executions", 10, "log", "out"],
            headers={"content-type": "application/json"},
        )

    def test_get_logs_requires_execution(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        app = App(name="my_app", state="STOPPED", execution_id=None)

        with pytest.raises(exceptions.JobExecutionException) as e_info:
            app.get_logs()

        assert "no execution is available" in str(e_info.value)

    def test_run_without_await_serving(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")

        refreshed = App(name="my_app", state="RUNNING", serving=False)
        mock_api.return_value.get_app.return_value = refreshed

        app = App(name="my_app", state="STOPPED")
        app._app_api = mock_api.return_value

        result = app.run(await_serving=False)

        mock_api.return_value._start.assert_called_once_with("my_app")
        assert result._state == "RUNNING"

    def test_run_passes_env_vars(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")

        refreshed = App(name="my_app", state="RUNNING", serving=False)
        mock_api.return_value.get_app.return_value = refreshed

        app = App(name="my_app", state="STOPPED")
        app._app_api = mock_api.return_value
        app._env_vars = {"FOO": "bar"}

        app.run(await_serving=False)

        mock_api.return_value._start.assert_called_once_with(
            "my_app", env_vars={"FOO": "bar"}
        )

    def test_stop_when_not_running(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")

        app = App(name="my_app", state="STOPPED", execution_id=None)
        app._app_api = mock_api.return_value

        result = app.stop()

        mock_api.return_value._stop.assert_not_called()
        assert result is app

    def test_wait_for_serving_timeout(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hopsworks_common.app.time.sleep")
        mocker.patch("hopsworks_common.app.SERVING_TIMEOUT", 6.0)
        mocker.patch("hopsworks_common.app.SERVING_POLL_INTERVAL", 3.0)
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")

        not_serving = App(name="my_app", state="RUNNING", serving=False)
        mock_api.return_value.get_app.return_value = not_serving

        app = App(name="my_app", state="RUNNING")
        app._app_api = mock_api.return_value

        with pytest.raises(exceptions.JobExecutionException) as e_info:
            app._wait_for_serving()

        assert "Timed out" in str(e_info.value)

    @pytest.mark.parametrize(
        "failed_state",
        [
            "FAILED",
            "KILLED",
            "FRAMEWORK_FAILURE",
            "APP_MASTER_START_FAILED",
            "INITIALIZATION_FAILED",
            "SUBMISSION_FAILED",
        ],
    )
    def test_wait_for_serving_failed_state(self, mocker, failed_state):
        mocker.patch("hopsworks_common.client.get_instance")
        mock_api = mocker.patch("hopsworks_common.core.app_api.AppApi")

        failed = App(name="my_app", state=failed_state, serving=False)
        mock_api.return_value.get_app.return_value = failed

        app = App(name="my_app", state="RUNNING")
        app._app_api = mock_api.return_value

        with pytest.raises(exceptions.JobExecutionException) as e_info:
            app._wait_for_serving()

        assert "App failed to start" in str(e_info.value)
        assert failed_state in str(e_info.value)

    def test_str_repr(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        app = App(name="my_app", state="RUNNING", serving=True)

        assert "my_app" in str(app)
        assert "RUNNING" in str(app)
        assert "True" in str(app)

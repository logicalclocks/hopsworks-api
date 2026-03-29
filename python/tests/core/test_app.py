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

    def test_str_repr(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        app = App(name="my_app", state="RUNNING", serving=True)

        assert "my_app" in str(app)
        assert "RUNNING" in str(app)
        assert "True" in str(app)

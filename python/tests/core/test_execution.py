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
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        mock_client.return_value._base_url = "https://myhost:443"

        ex = Execution(
            state="RUNNING",
            monitoring={"appUrl": "pythonapp/proj/my_app/"},
            job=mock.Mock(),
        )

        assert ex.app_url == "https://myhost:443/hopsworks-api/pythonapp/proj/my_app/"

    def test_app_url_none_when_not_running(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        ex = Execution(
            state="FINISHED",
            monitoring={"appUrl": "pythonapp/proj/my_app/"},
            job=mock.Mock(),
        )

        assert ex.app_url is None

    def test_app_url_none_when_no_monitoring(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        ex = Execution(state="RUNNING", monitoring=None, job=mock.Mock())

        assert ex.app_url is None

    def test_app_url_none_when_monitoring_has_no_app_url(self, mocker):
        mocker.patch("hopsworks_common.client.get_instance")

        ex = Execution(
            state="RUNNING", monitoring={"otherKey": "value"}, job=mock.Mock()
        )

        assert ex.app_url is None

    def test_app_url_strips_trailing_slash_from_base_url(self, mocker):
        mock_client = mocker.patch("hopsworks_common.client.get_instance")
        mock_client.return_value._base_url = "https://myhost:443/"

        ex = Execution(
            state="RUNNING",
            monitoring={"appUrl": "pythonapp/proj/my_app/"},
            job=mock.Mock(),
        )

        assert ex.app_url == "https://myhost:443/hopsworks-api/pythonapp/proj/my_app/"

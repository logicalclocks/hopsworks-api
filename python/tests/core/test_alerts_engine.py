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

from unittest.mock import Mock

import pytest
from hopsworks_common.core.alerts_api import _API_MATCH
from hopsworks_common.engine import alerts_engine


class TestAlertsEngine:
    def test_await_receiver(self, mocker, backend_fixtures):
        # Arrange
        receiver_name = "slack_receiver"
        timeout = 10
        engine = alerts_engine.AlertsEngine()
        client_mock = Mock()
        client_mock.configure_mock(
            **{
                "_send_request.return_value": backend_fixtures["alert_receiver"]["get"][
                    "response"
                ]
            }
        )
        client_mock._project_name = "project1"
        mocker.patch("hopsworks_common.client.get_instance", return_value=client_mock)

        # Act
        receiver = engine.await_receiver(receiver_name, timeout)
        # Assert
        assert receiver.name == f"project1__{receiver_name}"

    def test_await_receiver_timeout(self, mocker, backend_fixtures):
        # Arrange
        receiver_name = "non_existent_receiver"
        timeout = 1
        engine = alerts_engine.AlertsEngine()
        client_mock = Mock()
        client_mock.configure_mock(
            **{
                "_send_request.return_value": backend_fixtures["alert_receiver"][
                    "get_empty"
                ]["response"]
            }
        )
        client_mock._project_name = "project1"
        mocker.patch("hopsworks_common.client.get_instance", return_value=client_mock)

        # Act & Assert
        with pytest.raises(TimeoutError):
            engine.await_receiver(receiver_name, timeout)

    def test_await_route(self, mocker, backend_fixtures):
        # Arrange
        receiver_name = "slack_receiver"
        match = [
            {"key": "status", "value": "script_finished"},
            {"key": "severity", "value": "info"},
            _API_MATCH,
        ]
        timeout = 10
        engine = alerts_engine.AlertsEngine()
        client_mock = Mock()
        client_mock.configure_mock(
            **{
                "_send_request.return_value": backend_fixtures["alert_route"]["get"][
                    "response"
                ]
            }
        )
        client_mock._project_name = "project1"
        mocker.patch("hopsworks_common.client.get_instance", return_value=client_mock)

        # Act
        route = engine.await_route(receiver_name, match, timeout)
        # Assert
        assert route.match == {
            "api": "alerts-api",
            "project": "project1",
            "severity": "info",
            "status": "script_finished",
            "type": "project-alert",
        }

    def test_await_route_timeout(self, mocker, backend_fixtures):
        # Arrange
        receiver_name = "non_existent_receiver"
        match = [
            {"key": "status", "value": "script_finished"},
            {"key": "severity", "value": "Warning"},
            _API_MATCH,
        ]
        timeout = 1
        engine = alerts_engine.AlertsEngine()
        client_mock = Mock()
        client_mock.configure_mock(
            **{
                "_send_request.return_value": backend_fixtures["alert_route"]["get"][
                    "response"
                ]
            }
        )
        client_mock._project_name = "project1"
        mocker.patch("hopsworks_common.client.get_instance", return_value=client_mock)

        # Act & Assert
        with pytest.raises(TimeoutError):
            engine.await_route(receiver_name, match, timeout)

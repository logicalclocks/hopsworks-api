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

from hopsworks_common.core import environment_api


class TestEnvironmentApi:
    def _api(self, mocker):
        api = environment_api.EnvironmentApi()
        mock_client = mocker.MagicMock()
        mock_client._project_id = 1
        mock_client._send_request.return_value = {"name": "myenv"}
        mocker.patch("hopsworks_common.client._get_instance", return_value=mock_client)
        return api

    def test_create_environment_forwards_an_explicit_timeout(self, mocker):
        """The parameter is only worth exposing if it reaches the wait it is meant to bound."""
        # Arrange
        api = self._api(mocker)
        mock_await = mocker.patch.object(
            api._environment_engine, "_await_environment_command"
        )

        # Act
        api.create_environment("myenv", timeout=42)

        # Assert
        mock_await.assert_called_once_with("myenv", 42)

    def test_create_environment_defaults_the_timeout_to_none(self, mocker):
        """Unset has to arrive as `None`, which is what tells the engine to use its own default."""
        # Arrange
        api = self._api(mocker)
        mock_await = mocker.patch.object(
            api._environment_engine, "_await_environment_command"
        )

        # Act
        api.create_environment("myenv")

        # Assert
        mock_await.assert_called_once_with("myenv", None)

    def test_create_environment_does_not_wait_when_not_awaiting(self, mocker):
        # Arrange
        api = self._api(mocker)
        mock_await = mocker.patch.object(
            api._environment_engine, "_await_environment_command"
        )

        # Act
        api.create_environment("myenv", await_creation=False, timeout=42)

        # Assert
        mock_await.assert_not_called()

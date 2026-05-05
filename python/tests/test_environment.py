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

from hopsworks_common import environment


class TestEnvironment:
    def test_uninstall_default_awaits_before_and_after(self, mocker):
        # Arrange
        env = environment.Environment(name="myenv")
        mock_await_env = mocker.patch.object(
            env._environment_engine, "await_environment_command"
        )
        mock_uninstall = mocker.patch.object(env._library_api, "_uninstall")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "await_library_command"
        )

        # Act
        env.uninstall("matplotlib")

        # Assert: in-flight env operations are awaited first, then DELETE, then await uninstall.
        mock_await_env.assert_called_once_with("myenv")
        mock_uninstall.assert_called_once_with("matplotlib", "myenv")
        mock_await_lib.assert_called_once_with("myenv", "matplotlib")

    def test_uninstall_no_await(self, mocker):
        # Arrange
        env = environment.Environment(name="myenv")
        mocker.patch.object(env._environment_engine, "await_environment_command")
        mocker.patch.object(env._library_api, "_uninstall")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "await_library_command"
        )

        # Act
        env.uninstall("matplotlib", await_uninstallation=False)

        # Assert
        mock_await_lib.assert_not_called()

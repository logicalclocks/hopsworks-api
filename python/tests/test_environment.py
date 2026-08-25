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
from hopsworks_common import environment


class TestEnvironment:
    def test_uninstall_default_awaits_before_and_after(self, mocker):
        # Arrange
        env = environment.Environment(name="myenv")
        mock_await_env = mocker.patch.object(
            env._environment_engine, "_await_environment_command"
        )
        mock_uninstall = mocker.patch.object(env._library_api, "_uninstall")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "_await_library_command"
        )

        # Act
        env.uninstall("matplotlib")

        # Assert: in-flight env operations are awaited first, then DELETE, then await uninstall.
        mock_await_env.assert_called_once_with("myenv", None)
        mock_uninstall.assert_called_once_with("matplotlib", "myenv")
        mock_await_lib.assert_called_once_with("myenv", "matplotlib", None)

    def test_uninstall_no_await(self, mocker):
        # Arrange
        env = environment.Environment(name="myenv")
        mocker.patch.object(env._environment_engine, "_await_environment_command")
        mocker.patch.object(env._library_api, "_uninstall")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "_await_library_command"
        )

        # Act
        env.uninstall("matplotlib", await_uninstallation=False)

        # Assert
        mock_await_lib.assert_not_called()

    def test_uninstall_forwards_an_explicit_timeout(self, mocker):
        """A timeout that is accepted but dropped on the way through is worse than one that was never offered."""
        # Arrange
        env = environment.Environment(name="myenv")
        mock_await_env = mocker.patch.object(
            env._environment_engine, "_await_environment_command"
        )
        mocker.patch.object(env._library_api, "_uninstall")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "_await_library_command"
        )

        # Act
        env.uninstall("matplotlib", timeout=42)

        # Assert: both the preflight wait and the uninstall wait are bounded by what the caller asked for.
        mock_await_env.assert_called_once_with("myenv", 42)
        mock_await_lib.assert_called_once_with("myenv", "matplotlib", 42)

    @pytest.mark.parametrize(
        "method, library_name",
        [
            ("install_wheel", "matplotlib-3.1.3-cp38-cp38-manylinux1_x86_64.whl"),
            ("install_requirements", "requirements.txt"),
        ],
    )
    def test_install_forwards_an_explicit_timeout(self, mocker, method, library_name):
        """Both install entry points expose `timeout`, and both have to route it to both waits."""
        # Arrange
        env = environment.Environment(name="myenv")
        mock_await_env = mocker.patch.object(
            env._environment_engine, "_await_environment_command"
        )
        mocker.patch.object(env._library_api, "_install")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "_await_library_command"
        )
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch(
            "hopsworks_common.util._convert_to_abs",
            side_effect=lambda path, _project: path,
        )

        # Act
        getattr(env, method)(f"/Projects/myproj/Resources/{library_name}", timeout=42)

        # Assert
        mock_await_env.assert_called_once_with("myenv", 42)
        mock_await_lib.assert_called_once_with("myenv", library_name, 42)

    def test_install_defaults_the_timeout_to_none(self, mocker):
        """Unset means "use the engine default", which only holds if `None` is what reaches the engine."""
        # Arrange
        env = environment.Environment(name="myenv")
        mock_await_env = mocker.patch.object(
            env._environment_engine, "_await_environment_command"
        )
        mocker.patch.object(env._library_api, "_install")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "_await_library_command"
        )
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch(
            "hopsworks_common.util._convert_to_abs",
            side_effect=lambda path, _project: path,
        )

        # Act
        env.install_requirements("/Projects/myproj/Resources/requirements.txt")

        # Assert
        mock_await_env.assert_called_once_with("myenv", None)
        mock_await_lib.assert_called_once_with("myenv", "requirements.txt", None)

    def test_the_preflight_wait_is_bounded_even_without_awaiting(self, mocker):
        """`timeout` is not only about the wait the caller opted into.

        The environment command already in flight is awaited before anything is submitted, so an install that does not await still spends this timeout.
        """
        # Arrange
        env = environment.Environment(name="myenv")
        mock_await_env = mocker.patch.object(
            env._environment_engine, "_await_environment_command"
        )
        mocker.patch.object(env._library_api, "_install")
        mock_await_lib = mocker.patch.object(
            env._environment_engine, "_await_library_command"
        )
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch(
            "hopsworks_common.util._convert_to_abs",
            side_effect=lambda path, _project: path,
        )

        # Act
        env.install_requirements(
            "/Projects/myproj/Resources/requirements.txt",
            await_installation=False,
            timeout=42,
        )

        # Assert
        mock_await_env.assert_called_once_with("myenv", 42)
        mock_await_lib.assert_not_called()

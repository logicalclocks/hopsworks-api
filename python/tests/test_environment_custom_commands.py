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

import json

import pytest
import requests
from hopsworks_common import environment
from hopsworks_common.client.exceptions import EnvironmentException
from hopsworks_common.engine import environment_engine


@pytest.fixture
def custom_env(mocker):
    env = environment.Environment(name="custom_env")
    connection = mocker.MagicMock(_project_id=1, _project_name="my_project")
    mocker.patch("hopsworks_common.client._get_instance", return_value=connection)
    mocker.patch.object(env._environment_engine, "_await_environment_command")
    mocker.patch.object(environment_engine.time, "sleep")
    return env, connection


def test_custom_commands_send_paths_and_variables_and_wait_for_the_submitted_id(
    custom_env,
):
    env, connection = custom_env
    connection._send_request.side_effect = [
        {"id": 42, "status": "NEW"},
        {"id": 42, "status": "NEW"},
        {"id": 42, "status": "ONGOING"},
        {"id": 42, "status": "SUCCESS"},
    ]
    artifacts = ["Resources/data.txt", "/Projects/my_project/Resources/config.json"]
    variables = {"BUILD_MODE": "release build", "TOKEN": "secret:build-token"}

    env.run_custom_commands(
        "Resources/setup.sh", artifacts=artifacts, env_vars=variables, timeout=42
    )

    env._environment_engine._await_environment_command.assert_called_once_with(
        "custom_env", 42
    )
    submit, *polls = connection._send_request.call_args_list
    assert submit.args == (
        "POST",
        ["project", 1, "python", "environments", "custom_env", "commands", "custom"],
    )
    assert json.loads(submit.kwargs["data"]) == {
        "commandsFile": "/Projects/my_project/Resources/setup.sh",
        "artifacts": "/Projects/my_project/Resources/data.txt,/Projects/my_project/Resources/config.json",
        "envVars": [
            {"name": "BUILD_MODE", "value": "release build"},
            {"name": "TOKEN", "value": "secret:build-token"},
        ],
    }
    assert submit.kwargs["headers"] == {"content-type": "application/json"}
    assert len(polls) == 3
    for poll in polls:
        assert poll.args == (
            "GET",
            ["project", 1, "python", "environments", "custom_env", "commands", 42],
        )
        assert (
            poll.kwargs["timeout"]
            == environment_engine.EnvironmentEngine.POLL_REQUEST_TIMEOUT
        )
    assert artifacts[0] == "Resources/data.txt"
    assert variables["TOKEN"] == "secret:build-token"


def test_custom_commands_can_submit_without_waiting(custom_env):
    env, connection = custom_env
    connection._send_request.return_value = {"id": 42, "status": "NEW"}

    env.run_custom_commands(
        "/Projects/my_project/Resources/setup.sh", await_completion=False, timeout=7
    )

    env._environment_engine._await_environment_command.assert_called_once_with(
        "custom_env", 7
    )
    connection._send_request.assert_called_once()
    assert json.loads(connection._send_request.call_args.kwargs["data"]) == {
        "commandsFile": "/Projects/my_project/Resources/setup.sh",
        "artifacts": "",
        "envVars": [],
    }


def test_custom_commands_default_wait_uses_the_engine_timeout(custom_env):
    env, connection = custom_env
    connection._send_request.side_effect = [
        {"id": 42, "status": "NEW"},
        {"id": 42, "status": "SUCCESS"},
    ]

    env.run_custom_commands("Resources/setup.sh")

    env._environment_engine._await_environment_command.assert_called_once_with(
        "custom_env", None
    )
    assert connection._send_request.call_count == 2


def test_custom_commands_raise_the_build_failure(custom_env):
    env, connection = custom_env
    connection._send_request.side_effect = [
        {"id": 42, "status": "NEW"},
        {"id": 42, "status": "FAILED", "errorMessage": "script exited with status 17"},
    ]

    with pytest.raises(EnvironmentException, match="script exited with status 17"):
        env.run_custom_commands("Resources/setup.sh")


def test_custom_commands_wait_is_bounded(custom_env, mocker):
    env, connection = custom_env
    connection._send_request.side_effect = [
        {"id": 42, "status": "NEW"},
        {"id": 42, "status": "ONGOING"},
    ]
    mocker.patch.object(environment_engine.time, "monotonic", side_effect=[0, 0, 10])

    with pytest.raises(EnvironmentException, match="custom command 42.*custom_env"):
        env.run_custom_commands("Resources/setup.sh", timeout=7)


def test_custom_commands_poll_retries_request_timeouts(custom_env):
    env, connection = custom_env
    connection._send_request.side_effect = [
        {"id": 42, "status": "NEW"},
        requests.exceptions.Timeout(),
        {"id": 42, "status": "SUCCESS"},
    ]

    env.run_custom_commands("Resources/setup.sh")

    assert connection._send_request.call_count == 3


def test_custom_commands_do_not_submit_when_preflight_fails(custom_env):
    env, connection = custom_env
    env._environment_engine._await_environment_command.side_effect = (
        EnvironmentException("busy")
    )

    with pytest.raises(EnvironmentException, match="busy"):
        env.run_custom_commands("Resources/setup.sh", await_completion=False)

    connection._send_request.assert_not_called()


@pytest.mark.parametrize(
    "arguments",
    [
        {"path": ""},
        {"path": None},
        {"artifacts": "Resources/data.txt"},
        {"artifacts": [""]},
        {"artifacts": [None]},
        {"artifacts": ["Resources/a,b.txt"]},
        {"env_vars": ["KEY=value"]},
        {"env_vars": {"KEY": 123}},
        {"env_vars": {123: "value"}},
    ],
)
def test_custom_commands_reject_invalid_arguments_before_waiting(custom_env, arguments):
    env, connection = custom_env
    arguments = {"path": "Resources/setup.sh", **arguments}

    with pytest.raises(ValueError):
        env.run_custom_commands(**arguments)

    env._environment_engine._await_environment_command.assert_not_called()
    connection._send_request.assert_not_called()

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
from hopsworks_common.client.exceptions import EnvironmentException, RestAPIError
from hopsworks_common.core.environment_api import _build_custom_commands_request
from hopsworks_common.engine import environment_engine


@pytest.fixture
def custom_env(mocker):
    env = environment.Environment(name="custom_env")
    connection = mocker.MagicMock(_project_id=1, _project_name="my_project")
    mocker.patch("hopsworks_common.client._get_instance", return_value=connection)
    mocker.patch.object(env._environment_engine, "_await_environment_command")
    mocker.patch.object(environment_engine.time, "sleep")
    mocker.patch.object(environment_engine.time, "monotonic", return_value=0)
    return env, connection


def _rest_error(status=400, code=300008):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(
        {"errorCode": code, "errorMsg": "Command not found"}
    ).encode()
    return RestAPIError("commands/42", response)


def test_custom_command_request_builder_is_pure(mocker):
    connection = mocker.patch(
        "hopsworks_common.client._get_instance",
        side_effect=AssertionError("HTTP client accessed"),
    )
    artifacts = ["Resources/local.txt", "/Projects/owner/Shared/data.txt"]
    variables = {"MODE": "a=b # $HOME"}

    request = _build_custom_commands_request(
        "consumer", "Resources/setup.sh", artifacts, variables
    )

    assert request == {
        "commandsFile": "/Projects/consumer/Resources/setup.sh",
        "artifacts": "/Projects/consumer/Resources/local.txt,/Projects/owner/Shared/data.txt",
        "envVars": [{"name": "MODE", "value": "a=b # $HOME"}],
    }
    assert artifacts == ["Resources/local.txt", "/Projects/owner/Shared/data.txt"]
    assert variables == {"MODE": "a=b # $HOME"}
    connection.assert_not_called()


def test_custom_commands_send_paths_and_variables_and_wait_for_the_submitted_id(
    custom_env,
):
    env, connection = custom_env
    connection._send_request.side_effect = [
        {"id": 42, "status": "NEW"},
        {"id": 42, "status": "NEW"},
        {"id": 42, "status": "ONGOING"},
        _rest_error(),
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
        assert poll.kwargs["timeout"] == 42
        assert poll.kwargs["deadline"] == 42
    assert submit.kwargs["timeout"] == 42
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
        _rest_error(),
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
    clock = [0]
    mocker.patch.object(
        environment_engine.time, "monotonic", side_effect=lambda: clock[0]
    )
    connection._send_request.return_value = {"id": 42, "status": "NEW"}
    mocker.patch.object(
        environment_engine.time,
        "sleep",
        side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )

    with pytest.raises(EnvironmentException, match="custom command 42.*custom_env"):
        env.run_custom_commands("Resources/setup.sh", timeout=7)

    assert clock[0] == 7
    assert connection._send_request.call_count == 2


def test_submission_time_is_deducted_from_polling_budget(custom_env, mocker):
    env, connection = custom_env
    clock = [0]
    mocker.patch.object(
        environment_engine.time, "monotonic", side_effect=lambda: clock[0]
    )

    def send(method, *args, **kwargs):
        if method == "POST":
            clock[0] += 6
            return {"id": 42}
        assert kwargs["timeout"] == 4
        return {"id": 42, "status": "SUCCESS"}

    connection._send_request.side_effect = send
    env.run_custom_commands("Resources/setup.sh", timeout=10)


def test_submission_timeout_is_not_retried(custom_env):
    env, connection = custom_env
    connection._send_request.side_effect = requests.exceptions.ReadTimeout()

    with pytest.raises(EnvironmentException, match="may have been submitted"):
        env.run_custom_commands("Resources/setup.sh", timeout=1, await_completion=False)

    connection._send_request.assert_called_once()
    assert connection._send_request.call_args.kwargs["timeout"] == 1


def test_submission_preparation_does_not_restart_the_deadline(custom_env, mocker):
    env, connection = custom_env
    now = [0]
    mocker.patch.object(
        environment_engine.time, "monotonic", side_effect=lambda: now[0]
    )
    calls = []

    def get_client():
        calls.append(None)
        if len(calls) == 2:
            now[0] = 2
        return connection

    mocker.patch("hopsworks_common.client._get_instance", side_effect=get_client)
    connection._send_request.return_value = {"id": 42}
    env.run_custom_commands("Resources/setup.sh", timeout=7, await_completion=False)

    assert connection._send_request.call_args.kwargs["timeout"] == 5
    assert connection._send_request.call_args.kwargs["deadline"] == 7


def test_custom_commands_reuse_the_existing_api(custom_env, mocker):
    env, connection = custom_env
    constructor = mocker.patch.object(
        environment.environment_api,
        "EnvironmentApi",
        side_effect=AssertionError("extra API"),
    )
    connection._send_request.side_effect = [{"id": 42}, _rest_error()]

    env.run_custom_commands("Resources/setup.sh")

    constructor.assert_not_called()


def test_custom_commands_poll_retries_request_timeouts(custom_env):
    env, connection = custom_env
    connection._send_request.side_effect = [
        {"id": 42, "status": "NEW"},
        requests.exceptions.Timeout(),
        _rest_error(),
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
    "status,code",
    [
        (400, 300000),
        (400, None),
        (401, 300008),
        (403, 300008),
        (404, 300008),
        (404, 300000),
        (500, 300008),
    ],
)
def test_custom_commands_do_not_hide_other_poll_errors(custom_env, status, code):
    env, connection = custom_env
    error = _rest_error(status, code)
    connection._send_request.side_effect = [{"id": 42, "status": "NEW"}, error]

    with pytest.raises(RestAPIError) as raised:
        env.run_custom_commands("Resources/setup.sh")

    assert raised.value is error


def test_custom_commands_do_not_hide_submission_errors(custom_env):
    env, connection = custom_env
    error = _rest_error()
    connection._send_request.side_effect = error

    with pytest.raises(RestAPIError) as raised:
        env.run_custom_commands("Resources/setup.sh")

    assert raised.value is error


def test_custom_commands_do_not_hide_errors_without_a_code(custom_env):
    env, connection = custom_env
    response = requests.Response()
    response.status_code = 400
    response._content = b"{}"
    error = RestAPIError("commands/42", response)
    connection._send_request.side_effect = [{"id": 42, "status": "NEW"}, error]

    with pytest.raises(RestAPIError) as raised:
        env.run_custom_commands("Resources/setup.sh")

    assert raised.value is error


@pytest.mark.parametrize(
    "script",
    [
        "Resources/setup.sh",
        "/Projects/my_project/Resources/setup.sh",
        "/Projects/shared/Resources/setup.sh",
    ],
)
def test_custom_commands_preserve_absolute_paths(custom_env, script):
    env, connection = custom_env
    connection._send_request.return_value = {"id": 42, "status": "NEW"}
    artifacts = [
        "Resources/local.txt",
        "/Projects/my_project/Resources/local.txt",
        "/Projects/shared/Resources/remote.txt",
    ]

    env.run_custom_commands(script, artifacts=artifacts, await_completion=False)

    request = json.loads(connection._send_request.call_args.kwargs["data"])
    assert request["commandsFile"] == (
        script if script.startswith("/") else "/Projects/my_project/" + script
    )
    assert (
        request["artifacts"]
        == "/Projects/my_project/Resources/local.txt,/Projects/my_project/Resources/local.txt,/Projects/shared/Resources/remote.txt"
    )


def test_custom_commands_preserve_server_path_permission_errors(custom_env):
    env, connection = custom_env
    error = _rest_error(403, 150001)
    connection._send_request.side_effect = error

    with pytest.raises(RestAPIError) as raised:
        env.run_custom_commands("/Projects/private_project/Resources/setup.sh")

    assert raised.value is error


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
        {"env_vars": {"KEY": "one\ntwo"}},
        {"env_vars": {"KEY": "one\rtwo"}},
        {"env_vars": {"KEY": "one\0two"}},
        {"env_vars": {"KEY\nOTHER": "value"}},
    ],
)
def test_custom_commands_reject_invalid_arguments_before_waiting(custom_env, arguments):
    env, connection = custom_env
    arguments = {"path": "Resources/setup.sh", **arguments}

    with pytest.raises(ValueError):
        env.run_custom_commands(**arguments)

    env._environment_engine._await_environment_command.assert_not_called()
    connection._send_request.assert_not_called()

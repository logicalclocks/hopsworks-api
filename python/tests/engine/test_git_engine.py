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

from types import SimpleNamespace
from unittest import mock

import pytest
import requests
from hopsworks_common.client.exceptions import GitException
from hopsworks_common.engine.git_engine import GitEngine


def _op(success, state="RUNNING"):
    return SimpleNamespace(
        success=success,
        state=state,
        id=5,
        repository=SimpleNamespace(id=3),
        command_result_message="boom",
    )


@pytest.fixture
def engine(mocker):
    mocker.patch("hopsworks_common.engine.git_engine.time.sleep")
    engine = GitEngine()
    engine._git_op_execution_api = mock.MagicMock()
    return engine


def test_an_operation_is_polled_until_it_ends(engine):
    engine._git_op_execution_api._get_execution.side_effect = [_op(None), _op(True)]

    assert engine._execute_op_blocking(_op(None), "CLONE").success is True
    assert engine._git_op_execution_api._get_execution.call_count == 2


def test_a_timeout_ends_the_wait_on_an_operation_still_running(engine, mocker):
    engine._git_op_execution_api._get_execution.return_value = _op(None)
    mocker.patch(
        "hopsworks_common.engine.git_engine.time.monotonic",
        side_effect=[0.0, 1.0, 11.0],
    )

    with pytest.raises(GitException, match="CLONE has not finished after 10 seconds"):
        engine._execute_op_blocking(_op(None), "CLONE", timeout=10)

    assert engine._git_op_execution_api._get_execution.call_count == 1


def test_each_status_request_gets_the_remaining_budget(engine, mocker):
    engine._git_op_execution_api._get_execution.side_effect = (
        requests.exceptions.Timeout()
    )
    mocker.patch(
        "hopsworks_common.engine.git_engine.time.monotonic",
        side_effect=[0.0, 4.0],
    )

    with pytest.raises(GitException, match="CLONE has not finished after 10 seconds"):
        engine._execute_op_blocking(_op(None), "CLONE", timeout=10)

    assert engine._git_op_execution_api._get_execution.call_args.kwargs == {
        "timeout": 6.0
    }


def test_a_failed_operation_raises_its_message(engine):
    with pytest.raises(GitException, match="boom"):
        engine._execute_op_blocking(_op(False), "CLONE")

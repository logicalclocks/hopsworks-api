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
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests
from hopsworks_common.client.exceptions import GitException
from hopsworks_common.core.git_api import GitApi


@pytest.fixture
def api(mocker):
    instance = MagicMock()
    instance._project_id = 119
    instance._project_name = "demo"
    mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
    mocker.patch(
        "hopsworks_common.core.git_api.util._get_hostname_replaced_url",
        return_value="https://h/p/119/settings/git",
    )
    git = GitApi()
    git._git_engine = MagicMock()
    git._git_engine._execute_op_blocking.return_value = SimpleNamespace(
        repository=SimpleNamespace(name="repo", path="/Projects/demo/Resources/repo")
    )
    return git, instance


def test_a_stalled_clone_request_ends_with_the_clone_budget(api, mocker):
    git, instance = api
    instance._send_request.side_effect = requests.exceptions.Timeout()

    with pytest.raises(GitException, match="has not finished after 30 seconds"):
        git._clone("https://github.com/o/r", "Resources", provider="GitHub", timeout=30)

    assert instance._send_request.call_args.kwargs["timeout"] == pytest.approx(
        30, abs=1
    )


def test_a_clone_timeout_names_only_the_budget(api):
    git, instance = api
    instance._send_request.side_effect = requests.exceptions.Timeout()

    with pytest.raises(GitException) as info:
        git._clone(
            "https://user:token@github.com/o/r",
            "Resources",
            provider="GitHub",
            timeout=30,
        )

    assert str(info.value) == "Git clone has not finished after 30 seconds."


def test_every_step_of_a_clone_shares_one_budget(api, mocker):
    git, instance = api
    mocker.patch(
        "hopsworks_common.core.git_api.time.monotonic",
        side_effect=[0.0, 1.0, 5.0, 12.0],
    )
    instance._send_request.side_effect = [
        {"id": 1, "repository": {"id": 3, "name": "repo"}},
        {"items": [{"id": 3, "name": "repo", "path": "/Projects/demo/Resources/repo"}]},
    ]
    mocker.patch(
        "hopsworks_common.core.git_api.git_op_execution.GitOpExecution.from_response_json",
        return_value=SimpleNamespace(),
    )
    mocker.patch(
        "hopsworks_common.core.git_api.git_repo.GitRepo.from_response_json",
        return_value=[
            SimpleNamespace(name="repo", path="/Projects/demo/Resources/repo")
        ],
    )

    repo = git._clone(
        "https://github.com/o/r", "Resources", provider="GitHub", timeout=30
    )

    assert repo.name == "repo"
    post, get = instance._send_request.call_args_list
    assert post.kwargs["timeout"] == 29.0
    assert git._git_engine._execute_op_blocking.call_args.kwargs["timeout"] == 25.0
    assert get.kwargs["timeout"] == 18.0


def test_a_clone_without_a_budget_sends_requests_without_a_timeout(api, mocker):
    git, instance = api
    mocker.patch(
        "hopsworks_common.core.git_api.git_op_execution.GitOpExecution.from_response_json",
        return_value=SimpleNamespace(),
    )
    mocker.patch(
        "hopsworks_common.core.git_api.git_repo.GitRepo.from_response_json",
        return_value=[
            SimpleNamespace(name="repo", path="/Projects/demo/Resources/repo")
        ],
    )

    git._clone("https://github.com/o/r", "Resources", provider="GitHub")

    assert all(
        c.kwargs["timeout"] is None for c in instance._send_request.call_args_list
    )
    assert git._git_engine._execute_op_blocking.call_args.kwargs["timeout"] is None

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

import asyncio
import threading
from types import SimpleNamespace
from unittest import mock

import pytest


pytest.importorskip("fastmcp")

from hopsworks.mcp.tools.project import ProjectTools  # noqa: E402
from hopsworks.mcp.utils import sdk  # noqa: E402


def test_a_project_switch_waits_for_running_sdk_work():
    started, release = threading.Event(), threading.Event()
    order = []

    def compile_build():
        started.set()
        release.wait(5)
        order.append("compile")

    def login(**_):
        order.append("login")
        return SimpleNamespace(
            name="p2", id=2, owner="o", description="", created="2026-11-06"
        )

    tools = ProjectTools(mock.MagicMock())
    conn = SimpleNamespace(host="h", port=443, api_key_value="k", _engine="python")

    async def scenario():
        work = asyncio.ensure_future(sdk.run_sdk(compile_build))
        await asyncio.to_thread(started.wait, 5)
        switch = asyncio.ensure_future(
            asyncio.to_thread(asyncio.run, tools.use_project("p2"))
        )
        await asyncio.sleep(0.2)
        assert order == []
        release.set()
        await work
        await switch

    with (
        mock.patch(
            "hopsworks.mcp.tools.project.client._get_connection", return_value=conn
        ),
        mock.patch("hopsworks.mcp.tools.project.hopsworks.login", side_effect=login),
    ):
        asyncio.run(scenario())

    assert order == ["compile", "login"]
    assert not sdk.connection_lock.locked()

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


def _tools_and_patches(order):
    def login(**_):
        order.append("login")
        return SimpleNamespace(
            name="p2", id=2, owner="o", description="", created="2026-11-06"
        )

    conn = SimpleNamespace(host="h", port=443, api_key_value="k", _engine="python")
    patches = (
        mock.patch(
            "hopsworks.mcp.tools.project.client._get_connection", return_value=conn
        ),
        mock.patch("hopsworks.mcp.tools.project.hopsworks.login", side_effect=login),
    )
    return ProjectTools(mock.MagicMock()), patches


def test_a_project_switch_waits_for_sdk_work_while_the_event_loop_keeps_serving():
    started, release = threading.Event(), threading.Event()
    order = []
    tools, patches = _tools_and_patches(order)

    def compile_build():
        started.set()
        release.wait(5)
        order.append("compile")

    async def scenario():
        work = asyncio.ensure_future(sdk.run_sdk(compile_build))
        await asyncio.to_thread(started.wait, 5)
        switch = asyncio.ensure_future(tools.use_project("p2"))
        ticks = 0
        for _ in range(5):
            await asyncio.sleep(0.02)
            ticks += 1
        assert ticks == 5
        assert order == []
        release.set()
        await work
        await switch

    with patches[0], patches[1]:
        asyncio.run(scenario())

    assert order == ["compile", "login"]
    assert not sdk.connection_lock.locked()


def test_a_cancelled_switch_gives_the_lock_back_once_the_sdk_work_ends():
    started, release = threading.Event(), threading.Event()
    order = []
    tools, patches = _tools_and_patches(order)

    def compile_build():
        started.set()
        release.wait(5)

    async def scenario():
        work = asyncio.ensure_future(sdk.run_sdk(compile_build))
        await asyncio.to_thread(started.wait, 5)
        switch = asyncio.ensure_future(tools.use_project("p2"))
        await asyncio.sleep(0.05)
        switch.cancel()
        release.set()
        await work
        for _ in range(100):
            if not sdk.connection_lock.locked():
                break
            await asyncio.sleep(0.01)

    with patches[0], patches[1]:
        asyncio.run(scenario())

    assert order == []
    assert not sdk.connection_lock.locked()


def test_a_project_switch_reads_the_connection_under_the_lock():
    order = []
    tools, patches = _tools_and_patches(order)
    conn = SimpleNamespace(host="h", port=443, api_key_value="k", _engine="python")

    def current_connection():
        # A login queued ahead of the switch would have replaced the connection by now.
        assert sdk.connection_lock.locked()
        return conn

    with (
        patches[1],
        mock.patch(
            "hopsworks.mcp.tools.project.client._get_connection",
            side_effect=current_connection,
        ),
    ):
        asyncio.run(tools.use_project("p2"))

    assert order == ["login"]

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
"""Smoke test for the Hopsworks MCP server.

The MCP package is excluded from the ``pep8_public`` check and has no other
tests, yet its tools consume internal SDK symbols. Importing the server module
registers every ``@mcp.tool``; if any tool module has a broken import or a
stale call at registration time, this fails. It runs only when the optional
``fastmcp`` dependency is installed.
"""

from __future__ import annotations

import asyncio

import pytest


pytest.importorskip("fastmcp")


def test_mcp_server_registers_tools():
    """The server imports and registers its tool surface."""
    from hopsworks.mcp.server import mcp

    tools = asyncio.run(mcp.get_tools())
    assert len(tools) >= 20, f"expected the MCP tools to register, got {len(tools)}"
    for expected in ("login", "use_project", "create_project"):
        assert expected in tools, f"MCP tool '{expected}' is missing"

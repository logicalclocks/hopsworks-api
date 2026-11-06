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
tests, yet its tools consume internal SDK symbols. Building the server
registers every ``@mcp.tool``; if any tool module has a broken import or a
stale call at registration time, this fails. It runs only when the optional
``fastmcp`` dependency is installed.
"""

from __future__ import annotations

import asyncio

import pytest


pytest.importorskip("fastmcp")

# The command-executing tools, gated behind enable_shell_tools.
_SHELL_TOOLS = {"start_session", "add_input", "get_output"}
_SEMANTIC_LAYER_TOOLS = (
    "list_semantic_projects",
    "list_metrics",
    "get_dimensions",
    "get_entities",
    "get_dimension_values",
    "query_metrics",
    "get_metrics_compiled_sql",
    "get_semantic_project_tags",
    "validate_semantic_project",
    "compile_semantic_project",
    "create_dashboard_from_metrics",
)


def test_mcp_server_registers_tools():
    """The default (safe) server registers its core tools and no shell tools."""
    from hopsworks.mcp.server import mcp

    tools = asyncio.run(mcp.get_tools())
    assert len(tools) >= 15, f"expected the MCP tools to register, got {len(tools)}"
    for expected in ("login", "use_project", "create_project", *_SEMANTIC_LAYER_TOOLS):
        assert expected in tools, f"MCP tool '{expected}' is missing"
    # Security: shell tools must not be on the default surface.
    assert not (_SHELL_TOOLS & set(tools)), (
        "shell tools must be gated behind enable_shell_tools, "
        f"but the default server exposes {_SHELL_TOOLS & set(tools)}"
    )


def test_shell_tools_register_when_enabled():
    """enable_shell_tools=True adds the terminal command-execution tools."""
    from hopsworks.mcp.server import build_mcp

    tools = asyncio.run(build_mcp(enable_shell_tools=True).get_tools())
    assert set(tools) >= _SHELL_TOOLS, (
        f"expected shell tools to register, missing {_SHELL_TOOLS - set(tools)}"
    )


def test_network_bind_without_auth_is_refused():
    """run_server fails closed on a non-loopback HTTP bind with no auth token."""
    from click.testing import CliRunner
    from hopsworks.mcp.run_server import run_server_command

    result = CliRunner().invoke(
        run_server_command,
        ["--host", "0.0.0.0", "--transport", "http", "--create_session", "False"],
    )
    assert result.exit_code != 0
    assert "Refusing to bind" in result.output


def test_a_duplicate_tool_name_fails_the_build():
    """A second registration under a taken name would silently replace the first tool."""
    from hopsworks.mcp import server
    from hopsworks.mcp.tools import FeatureGroupTools

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(server, "SemanticLayerTools", FeatureGroupTools)
        with pytest.raises(ValueError, match="already exists"):
            server.build_mcp()


def test_login_opens_the_named_project(mocker):
    """The login tool's project argument reaches hopsworks.login instead of the first project."""
    from hopsworks.mcp.utils import auth

    login = mocker.patch("hopsworks.login")

    auth.login(host="h", project="sales", api_key_value="k")

    assert login.call_args.kwargs["project"] == "sales"

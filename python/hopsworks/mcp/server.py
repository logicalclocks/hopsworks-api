#
#   Copyright 2025 Hopsworks AB
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

"""MCP server for Hopsworks.

`build_mcp()` is the factory. Shell-capable tools (`TerminalTools`,
`BrewerTools`) execute arbitrary commands as the server process, so they are
**opt-in** (`enable_shell_tools=True`) rather than always registered. Transport
authentication is also opt-in via an `auth` provider; `run_server` refuses to
expose a non-loopback HTTP transport without one.

The module-level `mcp`/`app` are a safe default for ASGI deployments that import
`hopsworks.mcp.server:app`: shell tools are off, and a bearer token verifier is
attached when `HOPSWORKS_MCP_AUTH_TOKEN` is set in the environment.
"""

from __future__ import annotations

import os

from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier
from starlette import status
from starlette.responses import Response

from .prompts import ProjectPrompts, SystemPrompts
from .resources.project import ProjectResources
from .tools import (
    AuthTools,
    BrewerTools,
    DatasetTools,
    FeatureGroupTools,
    JobTools,
    ProjectTools,
    TerminalTools,
)


# Env var an operator can set to require a bearer token on the HTTP transport.
# `run_server` also accepts it via --auth-token.
AUTH_TOKEN_ENV = "HOPSWORKS_MCP_AUTH_TOKEN"


def static_bearer_auth(token: str) -> StaticTokenVerifier:
    """Build a single-token bearer verifier for the HTTP transport.

    Clients must send ``Authorization: Bearer <token>``. This is a shared-secret
    gate, not per-user identity; pair it with a loopback bind or a fronting proxy
    for anything beyond a single trusted operator.

    Parameters:
        token: The bearer token clients must present.

    Returns:
        A verifier suitable for ``FastMCP(auth=...)``.
    """
    return StaticTokenVerifier(
        tokens={token: {"client_id": "hopsworks-mcp", "scopes": []}}
    )


def build_mcp(
    *,
    enable_shell_tools: bool = False,
    auth: StaticTokenVerifier | None = None,
) -> FastMCP:
    """Construct a Hopsworks MCP server.

    Parameters:
        enable_shell_tools: Register the command-executing tools
            (``TerminalTools``, ``BrewerTools``). Off by default — these grant
            arbitrary code execution as the server process and must be turned on
            deliberately.
        auth: Optional transport auth provider. When set, the HTTP transport
            rejects unauthenticated requests.

    Returns:
        The configured ``FastMCP`` instance.
    """
    server = FastMCP(name="Hopsworks MCP", auth=auth)

    AuthTools(server)
    ProjectTools(server)
    ProjectResources(server)
    ProjectPrompts(server)
    SystemPrompts(server)
    JobTools(server)
    DatasetTools(server)
    FeatureGroupTools(server)

    if enable_shell_tools:
        TerminalTools(server)
        BrewerTools(server)

    @server.custom_route("/health", methods=["GET"])
    async def health(_):
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return server


# Safe default for `hopsworks.mcp.server:app` importers: no shell tools, and a
# bearer gate when HOPSWORKS_MCP_AUTH_TOKEN is set. Operators who need the shell
# tools or a custom auth provider should run via `hopsworks-mcp` (run_server.py).
_env_token = os.environ.get(AUTH_TOKEN_ENV)
mcp = build_mcp(
    enable_shell_tools=False,
    auth=static_bearer_auth(_env_token) if _env_token else None,
)

app = mcp.http_app()

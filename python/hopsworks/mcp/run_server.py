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
from __future__ import annotations

import logging
import os
import signal
import sys
from typing import Literal

import click
import hopsworks
import uvicorn

from .server import AUTH_TOKEN_ENV, build_mcp, static_bearer_auth
from .utils.auth import login


# Hosts that keep the server off the network. Binding anything else over an
# HTTP transport without an auth token is refused (fail closed).
_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
_NETWORK_TRANSPORTS = {"http", "sse", "streamable-http"}


# Configure logging to handle closed streams gracefully
class SafeStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            if self.stream and not getattr(self.stream, "closed", False):
                super().emit(record)
        except (ValueError, BrokenPipeError, OSError):
            print("Error writing log message to stream, stream may be closed.")
            # Ignore errors from closed streams


log = logging.getLogger("hopsworks-mcp")


def handle_shutdown(signum, frame):
    log.info(f"Received signal {signum}, will shut down...")
    hopsworks.logout()  # Checks if a session is active and logs out
    sys.exit(0)


@click.option(
    "--host",
    default="127.0.0.1",
    help="Host to bind the server to. (default: 127.0.0.1, loopback only). "
    "Binding a routable address exposes the server to the network — see "
    "--auth-token and --enable-shell-tools.",
)
@click.option("--port", default=8000, help="Port to run the server on. (default: 8000)")
@click.option(
    "--transport",
    default="http",
    help="Transport method to use. (default: http). Options: 'stdio', 'http', 'sse', 'streamable-http'",
)
@click.option(
    "--auth-token",
    "auth_token",
    default=None,
    help="Bearer token required on the HTTP transport (clients send "
    f"'Authorization: Bearer <token>'). Falls back to ${AUTH_TOKEN_ENV}. "
    "Required to bind a non-loopback host unless --insecure-no-auth is set.",
)
@click.option(
    "--insecure-no-auth",
    "insecure_no_auth",
    is_flag=True,
    default=False,
    help="Allow binding a non-loopback host with no --auth-token. "
    "Unauthenticated network access — only for an isolated network you trust.",
)
@click.option(
    "--enable-shell-tools",
    "enable_shell_tools",
    is_flag=True,
    default=False,
    help="Register the terminal/brewer tools. These execute arbitrary commands "
    "as the server process; off by default. Never combine with an "
    "unauthenticated network bind.",
)
@click.option(
    "--create_session",
    default=True,
    help="Create a Hopsworks session for the MCP server. (default: True)",
)
@click.option("--hopsworks_host", default=None, help="Hopsworks host URL")
@click.option("--hopsworks_port", default=443, help="Hopsworks port (default 443)")
@click.option("--project", default=None, help="Project name to access")
@click.option(
    "--api_key_value",
    default=None,
    help="API key value for Hopsworks authentication",
)
@click.option(
    "--api_key_file",
    default=None,
    help="Path to a file containing the API key for Hopsworks authentication",
)
@click.option(
    "--hostname_verification/--no-hostname-verification",
    default=True,
    help="Verify the Hopsworks TLS hostname (default: on). Disable only for a "
    "trusted private cluster with a self-signed certificate.",
)
@click.option(
    "--trust_store_path",
    default=None,
    help="Path to the trust store for Hopsworks authentication",
)
@click.option(
    "--engine",
    default="python",
    help="Engine to use (python, spark, training, spark-no-metastore, spark-delta) (default: python)",
)
def run_server(
    host: str = "127.0.0.1",
    port: int | None = None,
    transport: Literal["stdio", "http", "sse", "streamable-http"] = "http",
    auth_token: str | None = None,
    insecure_no_auth: bool = False,
    enable_shell_tools: bool = False,
    create_session: bool = True,
    hopsworks_host: str | None = None,
    hopsworks_port: int = 443,
    project: str | None = None,
    api_key_value: str | None = None,
    api_key_file: str | None = None,
    hostname_verification: bool = True,
    trust_store_path: str | None = None,
    engine: Literal[
        "spark", "python", "training", "spark-no-metastore", "spark-delta"
    ] = "python",
):
    """Run the Hopsworks MCP server.

    Parameters:
        host: Address to bind to. Defaults to loopback; a routable address
            exposes the server to the network.
        port:
            Port to run the server on.
            If not provided, it will default to the value of the UVICORN_PORT environment variable or 8000 if that is not set.
        transport: Transport method to use.
        auth_token: Bearer token required on the HTTP transport (or via the
            environment variable). Required for a non-loopback bind.
        insecure_no_auth: Permit a non-loopback bind with no auth token.
        enable_shell_tools: Register the command-executing terminal/brewer tools
            (off by default).
        create_session: Whether to create a Hopsworks session for the MCP server.
        hopsworks_host: Hopsworks host URL.
        hopsworks_port: Hopsworks port.
        project: Project name to use as default.
        api_key_value: API key value for Hopsworks authentication.
        api_key_file: Path to a file containing the API key for Hopsworks authentication.
        hostname_verification: Verify the Hopsworks TLS hostname (default on).
        trust_store_path: Path to the trust store for Hopsworks authentication.
        engine: Hopsworks engine to use.
    """
    if transport not in {"stdio", "http", "sse", "streamable-http"}:
        raise ValueError(
            "Invalid transport type. Choose from 'stdio', 'http', 'sse', or 'streamable-http'."
        )

    if port is None:
        port = int(os.getenv("UVICORN_PORT", "8000"))

    auth_token = auth_token or os.getenv(AUTH_TOKEN_ENV)

    # Fail closed: a network transport on a routable host with no auth token is
    # the exact shape that turns this server into an open endpoint. Refuse it
    # unless the operator explicitly accepts the risk.
    on_network = transport in _NETWORK_TRANSPORTS and host not in _LOOPBACK_HOSTS
    if on_network and not auth_token and not insecure_no_auth:
        raise click.ClickException(
            f"Refusing to bind {host!r} over '{transport}' without authentication. "
            f"Pass --auth-token (or set ${AUTH_TOKEN_ENV}), bind 127.0.0.1, or "
            "pass --insecure-no-auth if this is an isolated, trusted network."
        )
    if enable_shell_tools and on_network and not auth_token:
        raise click.ClickException(
            "--enable-shell-tools exposes arbitrary command execution; it cannot "
            "be combined with an unauthenticated network bind. Add --auth-token "
            "or bind 127.0.0.1."
        )

    auth = static_bearer_auth(auth_token) if auth_token else None
    mcp = build_mcp(enable_shell_tools=enable_shell_tools, auth=auth)

    if create_session:
        # Set the API key for the Hopsworks client
        login(
            host=hopsworks_host,
            port=hopsworks_port,
            project=project,
            api_key_value=api_key_value,
            api_key_file=api_key_file,
            hostname_verification=hostname_verification,
            trust_store_path=trust_store_path,
            engine=engine,
        )

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    if transport == "stdio":
        # For stdio transport, suppress all logging from imported libraries
        # that might try to write to closed streams
        logging.getLogger().handlers.clear()
        logging.getLogger().addHandler(SafeStreamHandler(sys.stdout))

        mcp.run(transport=transport, show_banner=False)
    else:
        app = mcp.http_app(transport=transport)
        try:
            import uvloop as uvloop

            has_uvloop = True
        except ImportError:
            has_uvloop = False
        uvicorn.run(
            app,
            host=host,
            port=port,
            loop="uvloop" if has_uvloop else "auto",
            http="httptools",
        )


run_server_command = click.command(
    "hopsworks-mcp",
    short_help="Run the Hopsworks MCP server.",
    help="In addition to setting arguments directly, you can set environment variables: \
        HOPSWORKS_HOST, HOPSWORKS_PORT, HOPSWORKS_PROJECT, HOPSWORKS_API_KEY, \
            HOPSWORKS_HOSTNAME_VERIFICATION, HOPSWORKS_TRUST_STORE_PATH and HOPSWORKS_ENGINE.",
)(run_server)

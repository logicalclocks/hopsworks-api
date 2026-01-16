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

from .server import mcp
from .utils.auth import login


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
    "--host", default="0.0.0.0", help="Host to run the server on. (default: 0.0.0.0)"
)
@click.option("--port", default=8000, help="Port to run the server on. (default: 8000)")
@click.option(
    "--transport",
    default="http",
    help="Transport method to use. (default: http). Options: 'stdio', 'http', 'sse', 'streamable-http'",
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
    "--hostname_verification",
    default=False,
    help="Enable hostname verification for Hopsworks authentication",
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
    host: str = "0.0.0.0",
    port: int | None = None,
    transport: Literal["stdio", "http", "sse", "streamable-http"] = "http",
    create_session: bool = True,
    hopsworks_host: str | None = None,
    hopsworks_port: int = 443,
    project: str | None = None,
    api_key_value: str | None = None,
    api_key_file: str | None = None,
    hostname_verification: bool = False,
    trust_store_path: str | None = None,
    engine: Literal[
        "spark", "python", "training", "spark-no-metastore", "spark-delta"
    ] = "python",
):
    """Run the Hopsworks MCP server."""
    if transport not in {"stdio", "http", "sse", "streamable-http"}:
        raise ValueError(
            "Invalid transport type. Choose from 'stdio', 'http', 'sse', or 'streamable-http'."
        )

    if port is None:
        port = int(os.getenv("UVICORN_PORT", "8000"))

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

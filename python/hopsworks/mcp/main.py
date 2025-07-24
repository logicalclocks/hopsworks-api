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


import logging

import click
import hopsworks

from .server import mcp


log = logging.getLogger("mcp.main")


@click.command()
@click.option(
    "--host", default="0.0.0.0", help="Host to run the server on. (default: 0.0.0.0)"
)
@click.option("--port", default=8001, help="Port to run the server on. (default: 8001)")
@click.option(
    "--transport",
    default="sse",
    help="Transport method to use. (default: sse). Options: 'stdio', 'http', 'sse', 'streamable-http'",
)
@click.option("--api_key", default=None, help="API key for authentication")
@click.option("--hopsworks_host", default=None, help="Hopsworks host URL")
@click.option("--hopsworks_port", default=443, help="Hopsworks port (default 443)")
@click.option("--project", default=None, help="Project name to access")
@click.option(
    "--engine",
    default="python",
    help="Engine to use (python, spark, hive) (default: python)",
)
def main(
    host, port, transport, api_key, hopsworks_host, hopsworks_port, project, engine
):
    """Run the Hopsworks MCP server."""
    if transport not in ["stdio", "http", "sse", "streamable-http"]:
        raise ValueError(
            "Invalid transport type. Choose from 'stdio', 'http', 'sse', or 'streamable-http'."
        )

    if api_key:
        if not hopsworks_host:
            raise ValueError("Hopsworks host must be provided when using API key.")

        # Set the API key for the Hopsworks client
        project = hopsworks.login(
            host=hopsworks_host,
            port=hopsworks_port,
            project=project,
            api_key_value=api_key,
            engine=engine,
        )
        log.info(
            f"Connected to Hopsworks project '{project.name}' at {hopsworks_host}:{hopsworks_port} using API key."
        )

    if transport == "stdio":
        print(f"Starting Hopsworks MCP server using {transport} transport.")
        mcp.run(transport=transport)
    else:
        mcp.run(transport=transport, host=host, port=port)

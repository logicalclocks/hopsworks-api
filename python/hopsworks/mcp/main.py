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


import click

from .server import mcp


@click.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8001)
@click.option("--transport", default="sse")
def main(host, port, transport):
    """Run the Hopsworks MCP server."""
    if transport not in ["stdio", "http", "sse", "streamable-http"]:
        raise ValueError(
            "Invalid transport type. Choose from 'stdio', 'http', 'sse', or 'streamable-http'."
        )

    if transport == "stdio":
        print(f"Starting Hopsworks MCP server using {transport} transport.")
        mcp.run(transport=transport)
    else:
        mcp.run(transport=transport, host=host, port=port)

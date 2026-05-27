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

"""MCP server for Hopsworks."""

from fastmcp import FastMCP
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


# Create a FastMCP server instance
mcp = FastMCP(name="Hopsworks MCP")

# Initialize tools and resources
AuthTools(mcp)
ProjectTools(mcp)
ProjectResources(mcp)
ProjectPrompts(mcp)
SystemPrompts(mcp)
JobTools(mcp)
DatasetTools(mcp)
FeatureGroupTools(mcp)
TerminalTools(mcp)
BrewerTools(mcp)


@mcp.custom_route("/health", methods=["GET"])
async def health(_):
    return Response(status_code=status.HTTP_204_NO_CONTENT)


app = mcp.http_app()

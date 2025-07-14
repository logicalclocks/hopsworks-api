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
"""Authentication tools for Hopsworks."""

from typing import Literal

import hopsworks
from fastmcp import Context


class AuthTools:
    """Tools for authenticating with Hopsworks."""

    def __init__(self, mcp):
        """Initialize auth tools.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp

        # Register tools
        self.mcp.tool()(self.login)

    async def login(
        self,
        host: str = None,
        port: int = 443,
        project: str = None,
        api_key_value: str = None,
        engine: Literal["python", "spark", "hive"] = "python",
        ctx: Context = None,
    ) -> dict:
        """Connect to a Hopsworks instance.

        Args:
            host: The hostname of the Hopsworks instance
            port: The port on which the Hopsworks instance can be reached
            project: Name of the project to access
            api_key_value: Value of the API Key (should have scopes: featurestore, project, job, kafka)
            engine: The engine to use for data processing (python, spark, or hive)

        Returns:
            Connection information
        """
        if ctx:
            await ctx.info(
                f"Connecting to Hopsworks at {host or 'hopsworks.ai'} using {engine} engine..."
            )

        # Perform actual login with the Hopsworks API
        project = hopsworks.login(
            host=host,
            port=port,
            project=project,
            api_key_value=api_key_value,
            engine=engine,
        )

        return {
            "name": project.name,
            "id": project.id,
            "owner": project.owner,
            "description": project.description,
            "created": project.created,
        }

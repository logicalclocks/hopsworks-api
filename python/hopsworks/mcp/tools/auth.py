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
from __future__ import annotations

from fastmcp import Context
from hopsworks.mcp.models.project import Project
from hopsworks.mcp.utils.auth import login as hw_login
from hopsworks.mcp.utils.tags import TAGS


class AuthTools:
    """Tools for authenticating with Hopsworks."""

    def __init__(self, mcp):
        """Initialize auth tools.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp

        # Register tools
        self.mcp.tool(tags=[TAGS.AUTH, TAGS.STATEFUL])(self.login)

    async def login(
        self,
        host: str = None,
        port: int = 443,
        project: str = None,
        api_key_value: str = None,
        api_key_file: str = None,
        hostname_verification: bool = False,
        trust_store_path: str = None,
        engine: str = "python",
        ctx: Context | None = None,
    ) -> Project:
        """Connect to a Hopsworks instance.

        Args:
            host (str): Hopsworks host URL.
            port (int): Hopsworks port (default 443).
            project (str): Project name to access.
            api_key_value (str): API key value for Hopsworks authentication.
            api_key_file (str): Path to a file containing the API key for Hopsworks authentication.
            hostname_verification (bool): Enable hostname verification for Hopsworks authentication.
            trust_store_path (str): Path to the trust store for Hopsworks authentication.
            engine (str): Engine to use (default: python).
            ctx (Context): The MCP context, provided automatically.

        Returns:
            Project: The project details or an error message.
        """
        if ctx:
            await ctx.info(
                f"Connecting to Hopsworks at {host or 'hopsworks.ai'} using {engine} engine..."
            )

        # Perform actual login with the Hopsworks API
        project = hw_login(
            host=host,
            port=port,
            project=project,
            api_key_value=api_key_value,
            api_key_file=api_key_file,
            hostname_verification=hostname_verification,
            trust_store_path=trust_store_path,
            engine=engine,
        )

        return Project(
            name=project.name,
            id=project.id,
            owner=project.owner,
            description=project.description,
            created=project.created,
        )

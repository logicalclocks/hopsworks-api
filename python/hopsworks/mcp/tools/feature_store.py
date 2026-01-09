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

import hopsworks
from fastmcp import Context  # noqa: TC002
from hopsworks.mcp.models.feature_store import FeatureStore
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client


class FeatureStoreTools:
    """Tools for Hopsworks feature store."""

    def __init__(self, mcp):
        """Initialize feature store tools.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.FEATURE_STORE, TAGS.READ, TAGS.STATEFUL])(
            self.get_feature_store_in_current_project
        )
        self.mcp.tool(tags=[TAGS.FEATURE_STORE, TAGS.READ, TAGS.STATELESS])(
            self.get_feature_store
        )

    async def get_feature_store_in_current_project(
        self, name: str | None = None, ctx: Context | None = None
    ) -> FeatureStore:
        """Get the feature store for the current project.

        Parameters:
            name: The name of the feature store to retrieve. If None, retrieves the default feature store.
            ctx: The MCP context, provided automatically.

        Returns:
            FeatureStore: The feature store information for the current project or an error message.
        """
        if ctx:
            await ctx.info("Retrieving feature store for the current project...")

        # Get the current project and its feature store
        project = hopsworks.get_current_project()
        feature_store = project.get_feature_store(name=name)

        # Return the feature store details
        return FeatureStore(
            name=feature_store.name,
            id=feature_store.id,
            project_name=feature_store.project_name,
            project_id=feature_store.project_id,
            online_enabled=feature_store.online_enabled,
        )

    async def get_feature_store(
        self, project_name: str, name: str | None = None, ctx: Context | None = None
    ) -> FeatureStore:
        """Get the feature store for a specific project.

        Parameters:
            project_name: The name of the project to get the feature store for.
            name: The name of the feature store to retrieve. If None, retrieves the default feature store.
            ctx: The MCP context, provided automatically.

        Returns:
            FeatureStore: The feature store information for the specified project or an error message.
        """
        if ctx:
            await ctx.info(f"Retrieving feature store for project {project_name}...")

        # Get the project and its feature store
        conn = client.get_connection()
        project = conn.get_project(project_name)
        feature_store = project.get_feature_store(name=name)

        # Return the feature store details
        return FeatureStore(
            name=feature_store.name,
            id=feature_store.id,
            project_name=feature_store.project_name,
            project_id=feature_store.project_id,
            online_enabled=feature_store.online_enabled,
        )

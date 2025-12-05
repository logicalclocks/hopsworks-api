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

import hopsworks
from fastmcp import Context
from hopsworks.mcp.models.feature_store import FeatureStore
from hopsworks.mcp.utils.tags import TAGS


class FeatureStoreResources:
    """Resources for Hopsworks feature store."""

    def __init__(self, mcp):
        """Initialize feature store resources.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.resource(
            uri="featurestore://feature-store",
            mime_type="application/json",
            tags=[TAGS.FEATURE_STORE, TAGS.STATEFUL],
        )(self.get_feature_store)
        self.mcp.resource(
            uri="featurestore://feature-store/{name}",
            mime_type="application/json",
            tags=[TAGS.FEATURE_STORE, TAGS.STATELESS],
        )(self.get_feature_store_by_name)

    async def get_feature_store(self, ctx: Context = None) -> FeatureStore:
        """Get the feature store for the current project.

        Returns:
            FeatureStore: The feature store information for the current project or an error message.
        """
        if ctx:
            await ctx.info("Retrieving feature store for the current project...")
        # Get the current project and its feature store
        project = hopsworks.get_current_project()
        feature_store = project.get_feature_store()

        # Return the feature store details
        return FeatureStore(
            name=feature_store.name,
            id=feature_store.id,
            project_name=feature_store.project_name,
            project_id=feature_store.project_id,
            online_enabled=feature_store.online_enabled,
        )

    async def get_feature_store_by_name(
        self, name: str, ctx: Context = None
    ) -> FeatureStore:
        """Get the feature store by its name.

        Args:
            name (str): The name of the feature store.
            ctx: The MCP context, provided automatically.

        Returns:
            FeatureStore: The feature store information or an error message.
        """
        if ctx:
            await ctx.info(f"Retrieving feature store with name {name}...")
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

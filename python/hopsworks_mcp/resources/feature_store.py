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
            tags=["Feature Store"],
        )(self.get_feature_store)
        self.mcp.resource(
            uri="featurestore://feature-store/{name}",
            mime_type="application/json",
            tags=["Feature Store"],
        )(self.get_feature_store_by_name)

    async def get_feature_store(self, ctx: Context = None) -> dict:
        """
        Get the feature store for the current project.

        Returns:
            dict: A dictionary containing the feature store information for the current project or an error message.
        """
        if ctx:
            await ctx.info("Retrieving feature store for the current project...")
        # Get the current project and its feature store
        try:
            project = hopsworks.get_current_project()
            feature_store = project.get_feature_store()
            if feature_store is None:
                return {"error": "No feature store found in the current project."}
            # Return the feature store details
            return {
                "name": feature_store.name,
                "id": feature_store.id,
                "project_name": feature_store.project_name,
                "project_id": feature_store.project_id,
                "online_feature_store_name": feature_store.online_featurestore_name,
                "online_enabled": feature_store.online_enabled,
                "offline_feature_store_name": feature_store.offline_featurestore_name,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to get feature store: {str(e)}",
            }

    async def get_feature_store_by_name(self, name: str, ctx: Context = None) -> dict:
        """
        Get the feature store by its name.

        Args:
            name (str): The name of the feature store.

        Returns:
            dict: A dictionary containing the feature store information or an error message.
        """
        if ctx:
            await ctx.info(f"Retrieving feature store with name {name}...")
        try:
            project = hopsworks.get_current_project()
            feature_store = project.get_feature_store(name=name)
            if not feature_store:
                return {
                    "status": "error",
                    "message": f"Feature store with name '{name}' not found.",
                }
            # Return the feature store details
            return {
                "name": feature_store.name,
                "id": feature_store.id,
                "project_name": feature_store.project_name,
                "project_id": feature_store.project_id,
                "online_feature_store_name": feature_store.online_featurestore_name,
                "online_enabled": feature_store.online_enabled,
                "offline_feature_store_name": feature_store.offline_featurestore_name,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to get feature store: {str(e)}",
            }

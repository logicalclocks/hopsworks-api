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

from typing import TYPE_CHECKING

import hopsworks
from hopsworks.mcp.models.feature_group import FeatureGroup, FeatureGroups
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client


if TYPE_CHECKING:
    from fastmcp import Context


class FeatureGroupTools:
    """Tools for managing feature groups in Hopsworks MCP."""

    def __init__(self, mcp):
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.FEATURE_GROUP, TAGS.READ, TAGS.STATEFUL])(
            self.get_feature_groups_in_current_project
        )
        self.mcp.tool(tags=[TAGS.FEATURE_GROUP, TAGS.READ, TAGS.STATELESS])(
            self.get_feature_groups
        )

    def _get_feature_groups(self, feature_store, name: str = None) -> FeatureGroups:
        """Helper method to retrieve feature groups from a feature store.

        Args:
            feature_store: The feature store instance.
            name: The name of the feature group to retrieve. If None, retrieves all feature groups.

        Returns:
            str: A string representation of the feature groups.
        """
        try:
            feature_groups = feature_store.get_feature_groups(name=name)
        except TypeError:
            # Handle case where feature_store.get_feature_groups fails due to missing parameters
            # Type error FeatureGroup.__init__() missing 3 required positional arguments: 'name', 'version', and 'featurestore_id'
            # When from_response_json is called without a response
            feature_groups = []

        # If no feature groups are found, return an empty FeatureGroups object
        if not feature_groups:
            return FeatureGroups(feature_groups=[], total=0, offset=0, limit=0)

        if "items" not in feature_groups:
            return FeatureGroup(
                id=feature_groups.id,
                name=feature_groups.name,
                version=feature_groups.version,
                featurestore_id=feature_groups.featurestore_id,
                location=feature_groups.location,
                event_time=feature_groups.event_time,
                online_enabled=feature_groups.online_enabled,
                topic_name=feature_groups.topic_name,
                notification_topic_name=feature_groups.notification_topic_name,
                deprecated=feature_groups.deprecated,
            )

        return FeatureGroups(
            feature_groups=[
                FeatureGroup(
                    id=fg.id,
                    name=fg.name,
                    version=fg.version,
                    featurestore_id=fg.featurestore_id,
                    location=fg.location,
                    event_time=fg.event_time,
                    online_enabled=fg.online_enabled,
                    topic_name=fg.topic_name,
                    notification_topic_name=fg.notification_topic_name,
                    deprecated=fg.deprecated,
                )
                for fg in feature_groups["items"]
            ],
            total=feature_groups["count"],
            offset=0,
            limit=len(feature_groups["items"]),
        )

    async def get_feature_groups_in_current_project(
        self,
        feature_store_project_name: str | None = None,
        name: str | None = None,
        ctx: Context | None = None,
    ):
        """Get the feature groups for the current project.

        Parameters:
            feature_store_project_name: Project name of the feature store. If None, uses the default project feature store.
            name: The name of the feature group to retrieve. If None, retrieves all feature groups
            ctx: The MCP context, provided automatically.

        Returns:
            FeatureGroups: List of feature groups in the current project.
        """
        if ctx:
            await ctx.info("Retrieving feature groups for the current project...")

        # Get the current project and its feature groups
        project = hopsworks.get_current_project()
        feature_store = project.get_feature_store(name=feature_store_project_name)
        return self._get_feature_groups(feature_store, name=name)

    async def get_feature_groups(
        self,
        project_name: str,
        feature_store_project_name: str | None = None,
        name: str | None = None,
        ctx: Context | None = None,
    ):
        """Get the feature groups for a specific project.

        Parameters:
            project_name: The name of the project.
            feature_store_project_name: Project name of the feature store. If None, uses the default project feature store.
            name: The name of the feature group to retrieve. If None, retrieves all feature groups.
            ctx: The MCP context, provided automatically.

        Returns:
            FeatureGroups: List of feature groups in the specified project.
        """
        if ctx:
            await ctx.info(f"Retrieving feature groups for project {project_name}...")

        # Get the specified project and its feature groups
        conn = client.get_connection()
        project = conn.get_project(project_name)
        feature_store = project.get_feature_store(name=feature_store_project_name)
        return self._get_feature_groups(feature_store, name=name)

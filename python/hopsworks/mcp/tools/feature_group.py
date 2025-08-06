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
from hopsworks.mcp.models.feature_group import FeatureGroup, FeatureGroups
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client


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

    async def get_feature_groups_in_current_project(
        self,
        feature_store_project_name: str = None,
        name: str = None,
        ctx: Context = None,
    ):
        """Get the feature groups for the current project.

        Args:
            feature_store_project_name: Project name of the feature store. If None, uses the default project feature store.
            name: The name of the feature group to retrieve. If None, retrieves all feature groups

        Returns:
            FeatureGroups: List of feature groups in the current project.
        """
        if ctx:
            await ctx.info("Retrieving feature groups for the current project...")

        # Get the current project and its feature groups
        project = hopsworks.get_current_project()
        feature_store = project.get_feature_store(name=feature_store_project_name)
        feature_groups = feature_store.get_feature_groups(name=name)

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

    async def get_feature_groups(
        self,
        project_name: str,
        feature_store_project_name: str = None,
        name: str = None,
        ctx: Context = None,
    ):
        """Get the feature groups for a specific project.

        Args:
            project_name: The name of the project.
            feature_store_project_name: Project name of the feature store. If None, uses the default project feature store.
            name: The name of the feature group to retrieve. If None, retrieves all feature groups.

        Returns:
            FeatureGroups: List of feature groups in the specified project.
        """
        if ctx:
            await ctx.info(f"Retrieving feature groups for project {project_name}...")

        # Get the specified project and its feature groups
        conn = client.get_connection()
        project = conn.get_project(project_name)
        feature_store = project.get_feature_store(name=feature_store_project_name)
        feature_groups = feature_store.get_feature_groups(name=name)

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

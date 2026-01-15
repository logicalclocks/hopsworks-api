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
from hopsworks.mcp.models.feature_group import Feature, FeatureGroup
from hopsworks.mcp.utils.tags import TAGS


class FeatureGroupTools:
    """Tools for managing feature groups in Hopsworks MCP."""

    def __init__(self, mcp):
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.FEATURE_GROUP, TAGS.READ, TAGS.STATEFUL])(
            self.get_feature_groups
        )
        self.mcp.tool(tags=[TAGS.FEATURE_GROUP, TAGS.READ, TAGS.STATEFUL])(
            self.get_feature_group_versions
        )
        self.mcp.tool(tags=[TAGS.FEATURE_GROUP, TAGS.READ, TAGS.STATEFUL])(
            self.get_feature_group_details
        )
        self.mcp.tool(tags=[TAGS.FEATURE_GROUP, TAGS.READ, TAGS.STATEFUL])(
            self.get_features
        )
        self.mcp.tool(tags=[TAGS.FEATURE_GROUP, TAGS.READ, TAGS.STATEFUL])(
            self.get_feature_details
        )

    def _get_feature_group_versions(self, name: str | None = None):
        # Get the current project and its feature groups
        try:
            project = hopsworks.get_current_project()
        except hopsworks.ProjectException:
            raise RuntimeError(
                "No active Hopsworks project found, use login tool."
            ) from None

        return project.get_feature_store().get_feature_groups(name=name)

    async def get_feature_groups(self, ctx: Context) -> list[FeatureGroup]:
        """Get the latest versions of all feature groups in the project."""
        await ctx.info("Retrieving feature groups...")

        fgs = self._get_feature_group_versions()
        fg_names = {fg.name for fg in fgs}
        fg_latest_version = {
            name: sorted([fg.version for fg in fgs if fg.name == name], reverse=True)[0]
            for name in fg_names
        }
        return sorted(
            [
                FeatureGroup(id=fg.id, name=fg.name, version=fg.version)
                for fg in fgs
                if fg.version == fg_latest_version[fg.name]
            ],
            key=lambda fg: (fg.name, fg.version),
        )

    async def get_feature_group_versions(self, ctx: Context, name: str) -> list[int]:
        """Get all versions of a feature group with the specified name."""
        await ctx.info("Retrieving feature groups...")

        fgs = self._get_feature_group_versions(name)
        return sorted([fg.version for fg in fgs])

    async def get_feature_group_details(
        self,
        ctx: Context,
        name: str,
        version: int | None = None,
    ) -> FeatureGroup:
        """Get the detailed description of a feature group with the specified name and version (latest by default)."""
        await ctx.info(f"Retrieving details of {name} feature group...")

        fgs = self._get_feature_group_versions(name)
        try:
            fg = sorted(fgs, key=lambda fg: fg.version, reverse=True)[0]
        except IndexError:
            raise RuntimeError(
                f"Feature group {name} not found."
            ) from None
        return FeatureGroup(
            id=fg.id,
            name=fg.name,
            version=fg.version,
            description=fg.description,
            location=fg.location,
            event_time=fg.event_time,
            online_enabled=fg.online_enabled,
            topic_name=fg.topic_name,
            notification_topic_name=fg.notification_topic_name,
            deprecated=fg.deprecated,
        )

    async def get_features(
        self,
        ctx: Context,
        feature_group_name: str,
        version: int | None = None,
    ) -> list[Feature]:
        """Get the names of the features of a feature group with the specified name and version (latest by default)."""
        await ctx.info(f"Retrieving features of {feature_group_name} feature group...")

        fgs = self._get_feature_group_versions(feature_group_name)
        try:
            fg = sorted(fgs, key=lambda fg: fg.version, reverse=True)[0]
        except IndexError:
            raise RuntimeError(
                f"Feature group {feature_group_name} not found."
            ) from None
        return sorted(
            [Feature(name=f.name) for f in fg.features],
            key=lambda feature: feature.name,
        )

    async def get_feature_details(
        self,
        ctx: Context,
        feature_group_name: str,
        name: str,
        version: int | None = None,
    ) -> Feature:
        """Get the names of the features of a feature group with the specified name and version (latest by default)."""
        await ctx.info(f"Retrieving details of {feature_group_name}.{name} feature...")

        fgs = self._get_feature_group_versions(feature_group_name)
        try:
            fg = sorted(fgs, key=lambda fg: fg.version, reverse=True)[0]
        except IndexError:
            raise RuntimeError(
                f"Feature group {feature_group_name} not found."
            ) from None
        try:
            f = [f for f in fg.features if f.name == name][0]
        except IndexError:
            raise RuntimeError(
                f"Feature {name} not found in feature group {feature_group_name}."
            ) from None
        return Feature(
            name=f.name,
            type=f.type,
            description=f.description,
            primary=f.primary,
            event_time=fg.event_time == f.name,
        )

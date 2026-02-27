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
from hopsworks.mcp.models.project import Project, Projects
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client


class ProjectResources:
    """Resources for Hopsworks projects."""

    def __init__(self, mcp):
        """Initialize project resources.

        Parameters:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.resource(
            uri="project://projects",
            mime_type="application/json",
            tags=[TAGS.PROJECT, TAGS.STATELESS],
        )(self.list_projects)
        self.mcp.resource(
            uri="project://projects/{name}",
            mime_type="application/json",
            tags=[TAGS.PROJECT, TAGS.STATELESS],
        )(self.get_project_details)
        self.mcp.resource(
            uri="project://projects/current",
            mime_type="application/json",
            tags=[TAGS.PROJECT, TAGS.STATEFUL],
        )(self.get_current_project_details)

    async def list_projects(self, ctx: Context = None) -> Projects:
        """List all projects.

        Returns:
            Projects: A list of projects accessible by the user or an error message.
        """
        if ctx:
            await ctx.info("Listing all projects...")

        conn = client.get_connection()
        projects = conn.get_projects()
        return Projects(
            projects=[
                Project(
                    name=project.name,
                    id=project.id,
                    owner=project.owner,
                    description=project.description,
                    created=project.created,
                )
                for project in projects
            ]
        )

    async def get_current_project_details(self, ctx: Context = None) -> Project:
        """Get details of the current project.

        Returns:
            Project: The current project details or an error message.
        """
        if ctx:
            await ctx.info("Retrieving current project details...")

        project = hopsworks.get_current_project()
        return Project(
            name=project.name,
            id=project.id,
            owner=project.owner,
            description=project.description,
            created=project.created,
        )

    async def get_project_details(self, name: str, ctx: Context = None) -> Project:
        """Get project details.

        Parameters:
            name: The name of the project.
            ctx: The MCP context, provided automatically.

        Returns:
            Project: The project details or an error message.
        """
        if ctx:
            await ctx.info(f"Retrieving project details for {name}...")

        conn = client.get_connection()
        project = conn.get_project(name)
        return Project(
            name=project.name,
            id=project.id,
            owner=project.owner,
            description=project.description,
            created=project.created,
        )

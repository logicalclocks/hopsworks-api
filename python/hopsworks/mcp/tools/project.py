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

import asyncio

import hopsworks
from fastmcp import Context  # noqa: TC002
from hopsworks.mcp.models.project import Project, Projects
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client


class ProjectTools:
    """Tools for Hopsworks projects."""

    def __init__(self, mcp):
        """Initialize project tools.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.PROJECT, TAGS.READ, TAGS.STATEFUL])(self.use_project)
        self.mcp.tool(tags=[TAGS.PROJECT, TAGS.WRITE, TAGS.STATELESS])(
            self.create_project
        )
        self.mcp.tool(tags=[TAGS.PROJECT, TAGS.READ, TAGS.STATELESS])(
            self.list_projects
        )
        self.mcp.tool(tags=[TAGS.PROJECT, TAGS.READ, TAGS.STATEFUL])(
            self.get_current_project_details
        )
        self.mcp.tool(tags=[TAGS.PROJECT, TAGS.READ, TAGS.STATELESS])(
            self.get_project_details
        )

    async def _create_project(
        self, conn, name: str = None, description: str = None
    ) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, conn.create_project, name, description)

    async def use_project(self, name: str, ctx: Context | None = None) -> Project:
        """Use a specific project.

        Parameters:
            name: The name of the project to use.
            context: The MCP context, provided automatically.

        Returns:
            Project: The project details or an error message.
        """
        if ctx:
            await ctx.info(f"Changing to project {name}...")

        conn = client.get_connection()
        if conn is None:
            raise ConnectionError("Not connected to Hopsworks.")

        project = hopsworks.login(
            host=conn.host,
            port=conn.port,
            project=name,
            api_key_value=conn.api_key_value,
            engine=conn._engine,
        )
        return Project(
            name=project.name,
            id=project.id,
            owner=project.owner,
            description=project.description,
            created=project.created,
        )

    async def create_project(
        self,
        name: str | None = None,
        description: str | None = None,
        ctx: Context = None,
    ) -> Project:
        """Create a new project.

        Parameters:
            name: The name of the project.
            description: A description of the project.

        Returns:
            Project: The newly created project details or an error message.
        """
        if ctx:
            await ctx.info(f"Creating project {name}...")

        progress = 0
        conn = client.get_connection()
        task = asyncio.create_task(
            self._create_project(conn, name=name, description=description)
        )
        if ctx:
            while not task.done():
                await ctx.report_progress(
                    progress=progress, message=f"Creating project: {name}..."
                )
                progress += 1
                await asyncio.sleep(1)
            await ctx.report_progress(
                progress=progress, message=f"Project '{name}' created successfully."
            )

        project = await task
        return Project(
            name=project.name,
            id=project.id,
            owner=project.owner,
            description=project.description,
            created=project.created,
        )

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

    async def get_current_project_details(self, ctx: Context | None = None) -> Project:
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

    async def get_project_details(
        self, name: str, ctx: Context | None = None
    ) -> Project:
        """Get project details.

        Parameters:
            name: The name of the project.
            context: The MCP context, provided automatically.

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

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

import asyncio

import hopsworks
from fastmcp import Context
from hopsworks_common import client


class ProjectTools:
    """Tools for Hopsworks projects."""

    def __init__(self, mcp):
        """Initialize project tools.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=["Project"])(self.use_project)
        self.mcp.tool(tags=["Project"])(self.create_project)
        self.mcp.tool(tags=["Project"])(self.list_projects)
        self.mcp.tool(tags=["Project"])(self.get_current_project_details)
        self.mcp.tool(tags=["Project"])(self.get_project_details)

    async def _create_project(
        self, conn, name: str = None, description: str = None
    ) -> dict:
        loop = asyncio.get_event_loop()
        project = await loop.run_in_executor(
            None, conn.create_project, name, description
        )
        return project

    async def use_project(self, name: str, ctx: Context = None) -> dict:
        """
        Use a specific project.

        Args:
            name (str): The name of the project to use.
        Returns:
            dict: A dictionary containing the project details or an error message.
        """
        if ctx:
            await ctx.info(f"Changing to project {name}...")

        try:
            conn = client.get_connection()
            if not conn or not conn._connected:
                return {"status": "error", "message": "No connection available."}

            project = hopsworks.login(
                host=conn.host,
                port=conn.port,
                project=name,
                api_key_value=conn.api_key_value,
                engine=conn._engine,
            )
            if not project:
                return {"status": "error", "message": f"Project {name} not found."}
            return {
                "name": project.name,
                "id": project.id,
                "owner": project.owner,
                "description": project.description,
                "created": project.created,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to use project {name}. {str(e)}",
            }

    async def create_project(
        self, name: str = None, description: str = None, ctx: Context = None
    ) -> dict:
        """
        Create a new project.
        Args:
            name (str): The name of the project.
            description (str): A description of the project.
        Returns:
            dict: A dictionary containing the newly created project details or an error message.
        """
        if ctx is not None:
            await ctx.info(f"Creating project {name}...")

        total_progress = 20
        progress = 0
        try:
            conn = client.get_connection()
            task = asyncio.create_task(
                self._create_project(conn, name=name, description=description)
            )
            if ctx is not None:
                while not task.done():
                    await ctx.report_progress(
                        progress=progress,
                        total=total_progress,
                        message=f"Creating project: {name}...",
                    )
                    progress += 1
                    if progress > total_progress - 2:
                        progress = total_progress - 2
                    await asyncio.sleep(1)
                await ctx.report_progress(
                    progress=total_progress,
                    total=total_progress,
                    message=f"Project '{name}' created successfully.",
                )

            project = await task

            return {
                "name": project.name,
                "id": project.id,
                "owner": project.owner,
                "description": project.description,
                "created": project.created,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to create project. {str(e)}",
            }

    async def list_projects(self, ctx: Context = None) -> dict:
        """
        List all projects.

        Returns:
            dict: A dictionary containing the list of projects accessible by the user or an error message.
        """
        if ctx:
            await ctx.info("Listing all projects...")

        try:
            conn = client.get_connection()
            projects = conn.get_projects()
            return {
                "projects": [
                    {
                        "name": project.name,
                        "id": project.id,
                        "owner": project.owner,
                        "description": project.description,
                        "created": project.created,
                    }
                    for project in projects
                ]
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to list projects. {str(e)}",
            }

    async def get_current_project_details(self, ctx: Context = None) -> dict:
        """Get details of the current project.

        Returns:
            dict: A dictionary containing the current project details or an error message.
        """
        if ctx:
            await ctx.info("Retrieving current project details...")

        try:
            project = hopsworks.get_current_project()
            if not project:
                return {"status": "error", "message": "No current project found."}
            return {
                "name": project.name,
                "id": project.id,
                "owner": project.owner,
                "description": project.description,
                "created": project.created,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to get current project: {str(e)}",
            }

    async def get_project_details(self, name: str, ctx: Context = None) -> dict:
        """Get project details.

        Args:
            name (str): The name of the project.

        Returns:
            dict: A dictionary containing the project details or an error message.
        """
        if ctx:
            await ctx.info(f"Retrieving project details for {name}...")

        try:
            conn = client.get_connection()
            project = conn.get_project(name)
            if not project:
                return {
                    "status": "error",
                    "message": f"Project '{name}' not found.",
                }
            return {
                "name": project.name,
                "id": project.id,
                "owner": project.owner,
                "description": project.description,
                "created": project.created,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to get project details. {str(e)}",
            }

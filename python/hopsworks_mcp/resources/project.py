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
from hopsworks_common import client


class ProjectResources:
    """Resources for Hopsworks projects."""

    def __init__(self, mcp):
        """Initialize project resources.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.resource(
            uri="project://projects",
            name="list_projects",
            description="List all projects",
            mime_type="application/json",
            tags=["Project"],
        )(self.list_projects)
        self.mcp.resource(
            uri="project://projects/{name}",
            name="get_project_details",
            description="Get project details",
            mime_type="application/json",
            tags=["Project"],
        )(self.get_project_details)
        self.mcp.resource(
            uri="project://projects/current",
            name="get_current_project_details",
            description="Get details of the current project",
            mime_type="application/json",
            tags=["Project"],
        )(self.get_current_project_details)

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
            return [
                {
                    "name": project.name,
                    "id": project.id,
                    "owner": project.owner,
                    "description": project.description,
                    "created": project.created,
                }
                for project in projects
            ]
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
            conn = hopsworks.connection()
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

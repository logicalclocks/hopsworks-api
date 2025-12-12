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
from hopsworks.mcp.models.job import Jobs, to_base_model_job
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client


if TYPE_CHECKING:
    from fastmcp import Context


class JobTools:
    """Tools for managing jobs in Hopsworks."""

    def __init__(self, mcp):
        """Initialize the JobTools with the MCP server instance.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.JOB, TAGS.READ, TAGS.STATEFUL])(
            self.get_jobs_in_current_project
        )
        self.mcp.tool(tags=[TAGS.JOB, TAGS.READ, TAGS.STATELESS])(self.get_jobs)

    async def get_jobs_in_current_project(self, ctx: Context = None):
        """Get the jobs for the current project.

        Returns:
            Jobs: List of jobs in the current project.
        """
        if ctx:
            await ctx.info("Retrieving jobs for the current project...")

        # Get the current project and its jobs
        project = hopsworks.get_current_project()
        jobs = project.get_job_api().get_jobs()

        return Jobs(
            jobs=[to_base_model_job(job) for job in jobs],
            total=jobs["count"] if "count" in jobs else len(jobs),
        )

    async def get_jobs(self, project_name: str, ctx: Context | None = None):
        """Get the jobs for a specific project.

        Parameters:
            project_name: The name of the project to retrieve jobs from.
            ctx: The MCP context, provided automatically.

        Returns:
            Jobs: List of jobs in the specified project.
        """
        if ctx:
            await ctx.info(f"Retrieving jobs for project '{project_name}'...")

        # Get the specified project and its jobs
        conn = client.get_connection()
        project = conn.get_project(project_name)
        jobs = project.get_job_api().get_jobs()

        return Jobs(
            jobs=[to_base_model_job(job) for job in jobs],
            total=jobs["count"] if "count" in jobs else len(jobs),
        )

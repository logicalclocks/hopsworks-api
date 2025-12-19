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
from hopsworks.mcp.models.dataset import Dataset, Datasets, File, Files
from hopsworks.mcp.utils.tags import TAGS
from hopsworks_common import client
from hopsworks_common.core import dataset, inode


if TYPE_CHECKING:
    from fastmcp import Context


class DatasetTools:
    """Tools for managing datasets in Hopsworks."""

    def __init__(self, mcp):
        """Initialize the DatasetTools with the MCP server instance.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.tool(tags=[TAGS.DATASET, TAGS.READ, TAGS.STATEFUL])(
            self.get_datasets_in_current_project
        )
        self.mcp.tool(tags=[TAGS.DATASET, TAGS.READ, TAGS.STATELESS])(self.get_datasets)
        self.mcp.tool(tags=[TAGS.DATASET, TAGS.READ, TAGS.STATEFUL])(
            self.list_files_in_current_project
        )
        self.mcp.tool(tags=[TAGS.DATASET, TAGS.READ, TAGS.STATELESS])(self.list_files)
        self.mcp.tool(tags=[TAGS.DATASET, TAGS.WRITE, TAGS.STATEFUL])(
            self.mkdir_in_current_project
        )
        self.mcp.tool(tags=[TAGS.DATASET, TAGS.WRITE, TAGS.STATELESS])(self.mkdir)

    async def get_datasets_in_current_project(
        self,
        offset: int | str = 0,
        limit: int | str = 100,
        ctx: Context = None,
    ) -> Datasets:
        """Get the dataset for the current project.

        Args:
            offset (int | str): The offset for pagination. Defaults to 0.
            limit (int | str): The limit for pagination. Defaults to 100.
            ctx: The MCP context, provided automatically.

        Returns:
            Datasets: The dataset information for the current project or an error message.
        """
        project = hopsworks.get_current_project()
        if ctx:
            await ctx.info(f"Listing files in project {project.name} ...")

        if isinstance(offset, str):
            offset = int(offset)
        if isinstance(limit, str):
            limit = int(limit)

        count, datasets = project.get_dataset_api()._list_dataset_path(
            "", dataset.Dataset, offset=offset, limit=limit
        )

        # Return the dataset details
        return Datasets(
            datasets=[
                Dataset(
                    name=ds.name,
                    id=ds.id,
                    description=ds.description,
                    datasetType=ds.dataset_type,
                )
                for ds in datasets
            ],
            total=count,
            offset=offset,
            limit=limit,
        )

    async def get_datasets(
        self,
        project_name: str,
        offset: int | str = 0,
        limit: int | str = 100,
        ctx: Context = None,
    ) -> Datasets:
        """Get the dataset for a specific project.

        Args:
            project_name (str): The name of the project to get the dataset for.
            offset (int | str): The offset for pagination. Defaults to 0.
            limit (int | str): The limit for pagination. Defaults to 100.
            ctx: The MCP context, provided automatically.

        Returns:
            Datasets: The dataset information for the specified project or an error message.
        """
        if ctx:
            await ctx.info(f"Listing files in project {project_name} ...")

        if isinstance(offset, str):
            offset = int(offset)
        if isinstance(limit, str):
            limit = int(limit)

        conn = client.get_connection()
        project = conn.get_project(project_name)
        count, datasets = project.get_dataset_api()._list_dataset_path(
            "", dataset.Dataset, offset=offset, limit=limit
        )

        # Return the dataset details
        return Datasets(
            datasets=[
                Dataset(
                    name=ds.name,
                    id=ds.id,
                    description=ds.description,
                    datasetType=ds.dataset_type,
                )
                for ds in datasets
            ],
            total=count,
            offset=offset,
            limit=limit,
        )

    async def list_files_in_current_project(
        self,
        path: str,
        offset: int | str = 0,
        limit: int | str = 100,
        ctx: Context = None,
    ) -> Files:
        """List files in a specific path.

        Args:
            path (str): The path to list files in.
            offset (int | str): The offset for pagination. Defaults to 0.
            limit (int | str): The limit for pagination. Defaults to 100.
            ctx: The MCP context, provided automatically.

        Returns:
            Files: List of files in the specified path or an error message.
        """
        if ctx:
            await ctx.info(f"Listing files in path {path} ...")

        if isinstance(offset, str):
            offset = int(offset)
        if isinstance(limit, str):
            limit = int(limit)

        project = hopsworks.get_current_project()
        count, files = project.get_dataset_api()._list_dataset_path(
            path, inode.Inode, offset=offset, limit=limit
        )

        # Return the file details
        return Files(
            files=[
                File(
                    name=f.name,
                    is_directory=f.dir,
                    owner=f.owner,
                    path=f.path,
                    permission=f.permission,
                    last_modified=f.modification_time,
                    under_construction=f.under_construction,
                )
                for f in files
            ],
            total=count,
            offset=offset,
            limit=limit,
        )

    async def list_files(
        self,
        project_name: str,
        path: str,
        offset: int | str = 0,
        limit: int | str = 100,
        ctx: Context = None,
    ) -> Files:
        """List files in a specific path of a project.

        Args:
            project_name (str): The name of the project to list files for.
            path (str): The path to list files in.
            offset (int | str): The offset for pagination. Defaults to 0.
            limit (int | str): The limit for pagination. Defaults to 100.
            ctx: The MCP context, provided automatically.

        Returns:
            Files: List of files in the specified path or an error message.
        """
        if ctx:
            await ctx.info(
                f"Listing files in project {project_name} at path {path} ..."
            )

        if isinstance(offset, str):
            offset = int(offset)
        if isinstance(limit, str):
            limit = int(limit)

        conn = client.get_connection()
        project = conn.get_project(project_name)
        count, files = project.get_dataset_api()._list_dataset_path(
            path, inode.Inode, offset=offset, limit=limit
        )

        # Return the file details
        return Files(
            files=[
                File(
                    name=f.name,
                    is_directory=f.dir,
                    owner=f.owner,
                    path=f.path,
                    permission=f.permission,
                    last_modified=f.modification_time,
                    under_construction=f.under_construction,
                )
                for f in files
            ],
            total=count,
            offset=offset,
            limit=limit,
        )

    async def mkdir_in_current_project(self, path: str, ctx: Context = None) -> str:
        """Create a directory in the current project.

        Args:
            path (str): The path to create the directory in.
            ctx: The MCP context, provided automatically.

        Returns:
            str: Success message or an error message.
        """
        if ctx:
            await ctx.info(f"Creating directory at path {path} ...")

        project = hopsworks.get_current_project()
        project.get_dataset_api().mkdir(path=path)

        return f"Directory created at {path} in the current project."

    async def mkdir(
        self, project_name: str, path: str, ctx: Context | None = None
    ) -> str:
        """Create a directory in a specific project.

        Args:
            project_name (str): The name of the project to create the directory in.
            path (str): The path to create the directory in.
            ctx: The MCP context, provided automatically.

        Returns:
            str: Success message or an error message.
        """
        if ctx:
            await ctx.info(
                f"Creating directory at path {path} in project {project_name} ..."
            )

        conn = client.get_connection()
        project = conn.get_project(project_name)
        project.get_dataset_api().mkdir(path=path)

        return f"Directory created at {path} in project {project_name}."

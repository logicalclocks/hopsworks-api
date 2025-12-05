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


from hopsworks.mcp.utils.tags import TAGS


class ProjectPrompts:
    """Prompts for Hopsworks projects."""

    def __init__(self, mcp):
        """Initialize project prompts.

        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.prompt(tags=[TAGS.PROJECT])(self.create_project_request)
        self.mcp.prompt(tags=[TAGS.PROJECT])(self.change_project_request)
        self.mcp.prompt(tags=[TAGS.PROJECT])(self.get_project_details_request)
        self.mcp.prompt(tags=[TAGS.PROJECT])(self.get_current_project_details_request)
        self.mcp.prompt(tags=[TAGS.PROJECT])(self.get_all_projects_request)

    def create_project_request(self, name: str = None, description: str = None) -> str:
        """Generates a user message for creating a new project.

        Args:
            name: The name of the project
            description: A brief description of the project

        Returns:
            str: A user message for creating a new project.
        """
        if not name:
            return "Please provide a project name."

        return f"Create a new project with name: {name} and description: {description}"

    def change_project_request(self, name: str = None) -> str:
        """Generates a user message for changing the current project.

        Args:
            name: The name of the project to switch to

        Returns:
            str: A user message for changing the current project.
        """
        if not name:
            return "Please provide the name of the project you want to switch to."

        return f"Use project: {name}"

    def get_project_details_request(self, name: str = None) -> str:
        """Generates a user message for getting project details.

        Args:
            name: The name of the project

        Returns:
            str: A user message for getting project details.
        """
        if not name:
            return "Please provide the name of the project to get details."

        return f"Get details for project: {name}"

    def get_current_project_details_request(self) -> str:
        """Generates a user message for getting details of the current project.

        Returns:
            str: A user message for getting details of the current project.
        """
        return "Get details of the current project."

    def get_all_projects_request(self) -> str:
        """Generates a user message for getting all projects.

        Returns:
            str: A user message for getting all projects.
        """
        return "List all projects."

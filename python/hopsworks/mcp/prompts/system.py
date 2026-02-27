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


class SystemPrompts:
    """Prompts for Hopsworks system operations."""

    def __init__(self, mcp):
        """Initialize system prompts.

        Parameters:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        self.mcp.prompt(tags=[TAGS.SYSTEM])(self.get_system_prompt_stateful)
        self.mcp.prompt(tags=[TAGS.SYSTEM])(self.get_system_prompt_stateless)

        self.prompt = "You are a helpful assistant that can answer questions about Hopsworks and use various tools to assist users."

    def get_system_prompt_stateful(self) -> str:
        """Generates a system prompt for stateful requests.

        Returns:
            str: A system prompt for stateful requests.
        """
        return f"{self.prompt} \n Always assume current project and use tools with names that end with in_current_project, if the user does not supply a project name."

    def get_system_prompt_stateless(self) -> str:
        """Generates a system prompt for stateless requests.

        Returns:
            str: A system prompt for stateless requests.
        """
        return f"{self.prompt} \n Always ask which project the user wants to interact with."

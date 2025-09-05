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

"""MCP server for Hopsworks."""

from fastmcp import FastMCP

from .prompts import ProjectPrompts, SystemPrompts
from .resources import FeatureStoreResources, ProjectResources
from .tools import (
    AuthTools,
    DatasetTools,
    FeatureGroupTools,
    FeatureStoreTools,
    JobTools,
    ProjectTools,
)


# Create a FastMCP server instance
mcp = FastMCP(name="Hopsworks MCP")

# Initialize tools and resources
_auth_tools = AuthTools(mcp)
_project_tools = ProjectTools(mcp)
_feature_store_tools = FeatureStoreTools(mcp)
_feature_store_resources = FeatureStoreResources(mcp)
_project_resource = ProjectResources(mcp)
_project_prompts = ProjectPrompts(mcp)
_system_prompts = SystemPrompts(mcp)
_job_tools = JobTools(mcp)
_dataset_tools = DatasetTools(mcp)
_feature_group_tools = FeatureGroupTools(mcp)

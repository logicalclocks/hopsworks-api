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

"""Main entry point for the Hopsworks MCP server."""

import os

from resources import FeatureStoreResources, ProjectResources
from server import mcp
from tools import AuthTools, ProjectTools


HOST = os.getenv("HOPSWORKS_MCP_HOST", "0.0.0.0")
PORT = int(os.getenv("HOPSWORKS_MCP_PORT", 8001))
TRANSPORT = os.getenv("HOPSWORKS_MCP_TRANSPORT", "sse")

# Initialize tools and resources
auth_tools = AuthTools(mcp)
project_tools = ProjectTools(mcp)
feature_store_resource = FeatureStoreResources(mcp)
project_resource = ProjectResources(mcp)

if __name__ == "__main__":
    mcp.run(transport=TRANSPORT, host=HOST, port=PORT)

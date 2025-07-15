# Hopsworks MCP
The Hopsworks Model Context Protocol (MCP) Server

## Available tools
Hopsworks MCP Server provides the following tools for hopsworks-related functionalities.

|Tool | Description | Args | Response |
| -------- | ------- | -------- | ------- |
|login | Connect to a Hopsworks instance. | host: The hostname of the Hopsworks instance port: The port on which the Hopsworks instance can be reached project: Name of the project to access api_key_value: Value of the API Key (should have scopes: featurestore, project, job, kafka) engine: The engine to use for data processing (python, spark, or hive) | A dictionary containing the project details or an error message. |
| use_project | Use a specific project by its name. | name (str): The name of the project to use. | A dictionary containing the project details or an error message. |
| create_project | Create a new project. | name (str): The name of the project. description (str): A description of the project. |  A dictionary containing the newly created project details or an error message. | 
| list_projects | List all projects. |  | A dictionary containing the list of projects accessible by the user or an error message. | 
|  get_current_project_details | Get details of the current project. | | A dictionary containing the current project details or an error message. | 
| get_project_details | Get project details. | name (str): The name of the project. |  A dictionary containing the project details or an error message. | 
| get_feature_store | Get the feature store for the current project. | |  A dictionary containing the feature store information for the current project or an error message.
| get_feature_store_by_name | Get the feature store by its name. | name (str): The name of the feature store. |  A dictionary containing the feature store information or an error message. | 

## Usage

``` pip install hopsworks[python,mcp] ```
 
Then use it to start a MCP server 

```python
import os
from hopsworks_mcp.server import mcp

HOST = os.getenv("HOPSWORKS_MCP_HOST", "0.0.0.0")
PORT = int(os.getenv("HOPSWORKS_MCP_PORT", 8001))
TRANSPORT = os.getenv("HOPSWORKS_MCP_TRANSPORT", "sse")

if __name__ == "__main__":
    mcp.run(transport=TRANSPORT, host=HOST, port=PORT)
```

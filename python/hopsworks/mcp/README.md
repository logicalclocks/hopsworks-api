# Hopsworks MCP

The Hopsworks Model Context Protocol (MCP) Server

## Available tools

Hopsworks MCP Server provides the following tools for hopsworks-related functionalities.

| Tool | Description | Args | Response |
| --- | --- | --- | --- |
| login | Connect to a Hopsworks instance. | host: The hostname of the Hopsworks instance port: The port on which the Hopsworks instance can be reached project: Name of the project to access api_key_value: Value of the API Key (should have scopes: featurestore, project, job, kafka) engine: The engine to use for data processing (python, spark, or hive) | The project details or an error message. |
| use_project | Use a specific project by its name. | name (str): The name of the project to use. | The project details or an error message. |
| create_project | Create a new project. | name (str): The name of the project. description (str): A description of the project. | The newly created project details or an error message. |
| list_projects | List all projects. | | The list of projects accessible by the user or an error message. |
| get_current_project_details | Get details of the current project. | | The current project details or an error message. |
| get_project_details | Get project details. | name (str): The name of the project. | The project details or an error message. |
| get_feature_store | Get the feature store for the current project. | | The feature store information for the current project or an error message. |
| get_feature_store_by_name | Get the feature store by its name. | name (str): The name of the feature store. | The feature store information or an error message. |

## Usage

``` pip install hopsworks[python,mcp] ```

Then start the MCP server

```hopsworks-mcp --host 127.0.0.1 --port 8000 --transport sse```

or create the server yourself

```python
import os
from hopsworks.mcp.server import mcp

HOST = os.getenv("HOPSWORKS_MCP_HOST", "0.0.0.0")
PORT = int(os.getenv("HOPSWORKS_MCP_PORT", 8001))
TRANSPORT = os.getenv("HOPSWORKS_MCP_TRANSPORT", "sse")

if __name__ == "__main__":
    if transport == "stdio":
        mcp.run(transport=TRANSPORT)
    else:
        mcp.run(transport=TRANSPORT, host=HOST, port=PORT)
```

```plaintext
[07/25/25 17:36:59] INFO     Starting MCP server 'Hopsworks MCP' with transport 'stdio' server.py:1371
User: What is the current project?
================================ Human Message =================================

What is the current project?
================================== Ai Message ==================================

", "parameters": {}}
Tool Calls:
  get_current_project_details (d43fb478-c927-48bc-b2fd-f40249b7829c)
 Call ID: d43fb478-c927-48bc-b2fd-f40249b7829c
  Args:
[07/25/25 17:37:15] INFO     Starting MCP server 'Hopsworks MCP' with transport 'stdio' server.py:1371
================================= Tool Message =================================
Name: get_current_project_details

{"name":"project1","id":120,"owner":"admin@hopsworks.ai","description":"project one","created":"2025-07-22T07:02:04.000Z"}
================================== Ai Message ==================================

The current project is project1, with an ID of 120, owned by admin@hopsworks.ai, and created on July 22, 2025.
User: change to project2
================================ Human Message =================================

change to project2
================================== Ai Message ==================================
Tool Calls:
  use_project (f5c3b87c-c48f-421d-888e-431416eb5135)
 Call ID: f5c3b87c-c48f-421d-888e-431416eb5135
  Args:
    name: project2
[07/25/25 17:37:25] INFO     Starting MCP server 'Hopsworks MCP' with transport 'stdio' server.py:1371
================================= Tool Message =================================
Name: use_project

{"name":"project2","id":121,"owner":"admin@hopsworks.ai","description":"Test project2","created":"2025-07-25T14:52:32.000Z"}
================================== Ai Message ==================================

You have switched to project2, which is a test project with an ID of 121, owned by admin@hopsworks.ai, and created on July 25, 2025.
```

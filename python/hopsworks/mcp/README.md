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
| list_semantic_projects | List the semantic projects of the current project. | | Name, root path, mode, serving build and whether it is stale, per semantic project. |
| list_metrics | List a semantic project's metrics. | semantic_project (str), tag (list[str], optional): only metrics carrying every tag | CSV: name, type, label, description, tags. |
| list_saved_queries | List a semantic project's saved queries. | semantic_project (str) | CSV: name, label, description, metrics, group_by. |
| get_dimensions | List the dimensions the metrics can be grouped by. | semantic_project (str), metrics (list[str]) | CSV: name, type, granularities, entity path, description. |
| get_entities | List the entities the metrics can be grouped by. | semantic_project (str), metrics (list[str], optional) | CSV: name, entity path, description. |
| get_dimension_values | List the values of a dimension. | semantic_project (str), dimension (str), metrics (list[str], optional), limit (int, 100) | CSV with one column. |
| query_metrics | Run a metric query on Trino as the logged-in user. | semantic_project (str), metrics (list[str]), group_by, where, order_by (list[str], optional), limit (int, 100) | CSV rows, cut at 50,000 characters. |
| query_saved_query | Run a saved query on Trino as the logged-in user. | semantic_project (str), name (str), group_by (list[str], optional): replaces its own, where (list[str], optional): added to its own, limit (int, 100) | CSV rows, cut at 50,000 characters. |
| get_metrics_compiled_sql | The SQL of a metric query, without running it. | the query_metrics arguments | The SQL text. |
| get_semantic_project_tags | The schematised tags of a semantic project. | semantic_project (str) | Name and value per tag. |
| validate_semantic_project | Compile and report a semantic project's errors and warnings. | semantic_project (str) | Build status and issues. |
| compile_semantic_project | Compile a semantic project into a build. | semantic_project (str) | Build id, status and issues. |
| create_dashboard_from_metrics | Build a Superset 7.0 dashboard of metrics. | semantic_project (str), metrics (list[str]), group_by (list[str], optional), title (str, optional) | Dashboard id, title and URL. |

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

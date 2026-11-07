# Semantic layer agent tools

The tools the Hopsworks MCP server (`hopsworks-mcp`) serves for the semantic layer, with dbt-mcp's names, and what each returns.
They act on the logged-in project; the parent skill's "Agent tools" section says when to use them.

| Tool | Does |
|---|---|
| `list_semantic_projects` | name, root path, mode, serving build, stale |
| `list_metrics` | CSV of name, type, label, description, tags; `tag` filters |
| `list_saved_queries` | CSV of name, label, description, metrics, group_by |
| `list_exports` | CSV of each export's feature group, schedule, lookback and last run (status, rows, message) |
| `get_dimensions`, `get_entities` | CSV of what the metrics can be grouped by |
| `get_dimension_values` | CSV of one dimension's values |
| `query_metrics` | CSV rows, `limit` 100 by default, cut at 50,000 characters |
| `query_saved_query` | CSV rows of a saved query by name; `group_by` and `limit` replace its own (`group_by=[]` for its totals), `where` adds to its filters |
| `get_metrics_compiled_sql` | the SQL, for a dashboard dataset or an explanation |
| `get_semantic_project_tags` | the project's tags |
| `validate_semantic_project`, `compile_semantic_project` | compile, and the build's status and issues (tagged `write`) |
| `create_dashboard_from_metrics` | a Superset 7.0 dashboard of the metrics: its id, title and URL (tagged `write`) |

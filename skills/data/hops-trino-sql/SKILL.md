---
name: hops-trino-sql
description: Query Hopsworks feature store tables via Trino SQL using the hops CLI.
---

Prefer to use the hops CLI for interactive queries to Trino. Prefer to use the 'delta' catalog, although there could be tables in the 'hudi' catalog as well.
The schema name is <project_name>_featurestore. The table name is <feature_group_name>_<version>.
For DELTA and Hudi feature groups, the structure of table references is:
delta.<project_name>_featurestore.<fg_name>_<fg_version>
hudi.<project_name>_featurestore.<fg_name>_<fg_version>

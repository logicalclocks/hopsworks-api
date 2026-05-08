---
name: hops-trino-sql
description: Query Hopsworks feature store tables via Trino SQL using the hops CLI.
---

Prefer to use the hops CLI for data discovery.
Prefer to use the 'delta' catalog, although there could be tables in the 'hudi' catalog as well.
The schema is <project_name>_featurestore.

hops trino tables delta.<project_name>_featurestore

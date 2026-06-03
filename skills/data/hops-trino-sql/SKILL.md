---
name: hops-trino-sql
description: Query Hopsworks feature store tables via Trino SQL using the hops CLI.
---

Prefer the `hops` CLI for interactive Trino queries over writing Python.

## Table naming

Offline feature groups are queried through Trino with this reference pattern:

```
delta.<project_name>_featurestore.<fg_name>_<fg_version>
hudi.<project_name>_featurestore.<fg_name>_<fg_version>
```

- Catalog is `delta` for Delta feature groups, `hudi` for Hudi ones.
- Schema is `<project_name>_featurestore` (project name lowercase).
- Table is `<fg_name>_<version>` — the version suffix is required.

## Commands

```bash
hops trino query "SELECT * FROM delta.<project>_featurestore.<fg>_1 LIMIT 5"
hops trino catalogs                       # delta, hive, hudi, iceberg, ...
hops trino schemas delta                  # schemas in a catalog
hops trino tables delta.<project>_featurestore
hops trino describe delta.<project>_featurestore.<fg>_1
```

- The CLI's default catalog is `iceberg`, so always **fully-qualify** with
  `delta.`/`hudi.` (or pass `--catalog delta`) when reading feature groups.
- `--limit` caps rows (default soft cap; `--limit 0` = unlimited). `-f file.sql`
  runs a query from a file; `-i` reads from stdin.

## See also

- **hops-data-discovery** — find FG/FV/data-source/file names to query.
- **hops-fg** / **hops-fv** — create and read these tables via the Python SDK.

---
name: hops-trino-sql
description: Query Hopsworks feature store tables via Trino SQL using the hops CLI.
---

# Trino SQL on Hopsworks

## Concept

The offline store is lakehouse tables (Hudi, Delta, or Iceberg), and these are queryable directly as Trino tables. Trino SQL is a direct-table read path for interactive exploration and EDA; training and inference pipelines should read through a feature view instead, so that model-dependent transformations and point-in-time joins stay consistent. The `hops` CLI is the preferred path for interactive Trino queries over writing Python.

## Key facts / rules

### Table naming

Offline feature groups are queried through Trino with this reference pattern:

```
delta.<project_name>_featurestore.<fg_name>_<fg_version>
hudi.<project_name>_featurestore.<fg_name>_<fg_version>
```

- Catalog is `delta` for Delta feature groups, `hudi` for Hudi ones.
- Schema is `<project_name>_featurestore` (project name lowercase).
- Table is `<fg_name>_<version>` — the version suffix is required.

### Caveats

- The CLI's default catalog is `iceberg`, so always **fully-qualify** with
  `delta.`/`hudi.` (or pass `--catalog delta`) when reading feature groups.
- `--limit` caps rows (default soft cap; `--limit 0` = unlimited). `-f file.sql`
  runs a query from a file; `-i` reads from stdin.

### Faster queries

- Select only the columns you need (projection pushdown) and filter on the
  feature group's partition key to prune files (data skipping). Date partition
  keys are stored in ISO 8601 (`YYYY-MM-DD`), so range filters like
  `date >= '2025-01-31' AND date <= '2025-02-28'` get partition-pruned.
- Multipart partition keys prune left-to-right: filtering only the second key
  (not the first) skips no files.

## Commands / API

```bash
hops trino query "SELECT * FROM delta.<project>_featurestore.<fg>_1 LIMIT 5"
hops trino catalogs                       # delta, hive, hudi, iceberg, ...
hops trino schemas delta                  # schemas in a catalog
hops trino tables delta.<project>_featurestore
hops trino describe delta.<project>_featurestore.<fg>_1
```

## Related skills

- **hops-data-discovery** — find FG/FV/data-source/file names to query.
- **hops-fg** / **hops-fv** — create and read these tables via the Python SDK.

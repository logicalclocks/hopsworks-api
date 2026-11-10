---
name: hops-data-discovery
description: Find available data in Hopsworks from feature groups, data sources, free-text search, and files.
---

# Data Discovery on Hopsworks

## Concept

Find available data in a Hopsworks project across four surfaces: feature groups (and views / training data), free-text search, data sources, and files. Prefer the `hops` CLI for data discovery over writing Python.

Discovery enables **feature reuse**: a model that reuses an existing feature does not need a new feature pipeline, so search the feature registry first before engineering features from scratch. The commands below browse that registry (definitions, schemas, tags, statistics).

## Commands / API

### Feature groups (and views / training data)

```bash
hops fg list                              # all feature groups in the project
hops fg info <name> --version 1           # metadata (online flag, PK, event time)
hops fg features <name> --version 1       # schema with PK / partition flags
hops fg preview <name> --version 1 --n 10 # first rows (note: preview <name>, not <name> preview)
hops fv list                              # feature views
```

### Free-text search

```bash
hops search <term>                     # match FGs, FVs, training datasets, features
hops search <term> --type feature_group   # restrict by type
hops search <term> --global            # search across shared/other projects
```

### Data sources (external connectors)

```bash
hops datasource list                      # configured connectors (Snowflake, S3, JDBC, ...)
hops datasource info <name>
hops datasource databases <name>
hops datasource tables <name> --database <db>
hops datasource preview <name> --table <t> [--database <db>]
```

Read a source only through Hopsworks (preview, or mount a LIMIT query as an external feature group; see **hops-data-sources**), never with a database driver and a credential copied from a secret. A failed read is reported, not routed around.

### Files (HopsFS)

```bash
hops files list [path]                    # browse the project filesystem
```

## Related skills

- **hops-trino-sql** — run SQL against the offline tables you discover.
- **hops-fg** / **hops-fv** — create and read these via the Python SDK.
- **hops-data-sources** — mount a new external source as a feature group, or ingest it via DLTHub.

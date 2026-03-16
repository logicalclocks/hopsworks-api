# Architecture

Four co-versioned Python packages under `python/`, published as one `hopsworks` PyPI distribution.

## Packages

- `hopsworks` — user-facing entry point: `login()`, `Project`, jobs, datasets, git, secrets
- `hopsworks_common` — shared base: HTTP client, auth, connection, exceptions, JSON encoding
- `hsfs` — Feature Store: `FeatureGroup`, `FeatureView`, `Feature`, `Query`, `StorageConnector`
- `hsml` — ML Registry and Serving: `Model`, `Deployment`, `Predictor`, `Transformer`

Often, domain entities follow a three-layer split: domain object itself, HTTP calls in API class, and there can also be an engine.
HTTP calls do not appear directly in domain objects.

## Details

- @.agent/architecture/packages.md — per-package structure, key files, internal patterns
- @.agent/architecture/domain.md — domain concepts (FeatureGroup, FeatureView, Model, …) and how they relate
- @.agent/architecture/dependencies.md — dependency groups, version policy, how to add deps

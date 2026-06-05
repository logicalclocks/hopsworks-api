# Hopsworks skills

Markdown playbooks (`SKILL.md`) that drive an agent through Hopsworks workflows.
The **live, canonical list is `hops skills list`** (read one with
`hops skills show <name>`); this file is a static index for browsing.

Skills are routed by their **directory name** (e.g. `hops-fg`). They are grouped
into bucket folders:

## data/ — discovery, sources, SQL, Spark
- [hops-data-discovery](data/hops-data-discovery/SKILL.md) — find FGs, data sources, search, files.
- [hops-data-sources](data/hops-data-sources/SKILL.md) — mount external tables / DLTHub ingestion.
- [hops-spark](data/hops-spark/SKILL.md) — PySpark on Hopsworks (Spark Connect + Delta).
- [hops-trino-sql](data/hops-trino-sql/SKILL.md) — Trino SQL via the `hops` CLI.
- [unstructured-data](data/unstructured-data/SKILL.md) — parse files into a feature group.

## ml/ — FTI pipeline architecture
- [hops-reqs](ml/hops-reqs/SKILL.md) — specify an ML system into `reqs/`.
- [hops-features](ml/hops-features/SKILL.md) — specify a feature pipeline.
- [hops-eda](ml/hops-eda/SKILL.md) — EDA before training.
- [hops-eda-checklist](ml/hops-eda-checklist/SKILL.md) — reference: EDA dimensions (profiling, target, leakage).

## hops/ — feature store, training, inference, jobs, apps, agents
- [hops-fg](hops/hops-fg/SKILL.md) — feature groups.
- [hops-fv](hops/hops-fv/SKILL.md) — feature views, training data, online vectors.
- [hops-transformations](hops/hops-transformations/SKILL.md) — built-in/custom/on-demand transforms + transformation store.
- [hops-train](hops/hops-train/SKILL.md) — train + register a model.
- [hops-batch-inference](hops/hops-batch-inference/SKILL.md) — batch scoring + prediction logging.
- [hops-online-inference](hops/hops-online-inference/SKILL.md) — KServe model deployment.
- [hops-monitoring](hops/hops-monitoring/SKILL.md) — statistics, drift monitoring, validation, alerts.
- [hops-job](hops/hops-job/SKILL.md) — jobs and Airflow DAGs.
- [hops-environments](hops/hops-environments/SKILL.md) — clone a Python env, install custom libs.
- [hops-app](hops/hops-app/SKILL.md) — Streamlit app deployments.
- [hops-superset](hops/hops-superset/SKILL.md) — Superset datasets / charts / dashboards.
- [hops-agent-deployment](hops/hops-agent-deployment/SKILL.md) — served interactive agent.
- [hops-agent-job](hops/hops-agent-job/SKILL.md) — scheduled coding-agent job.

> Note: `hops/` is the legacy flat bucket. New skills go in the categorized
> buckets (`data/`, `ml/`, and future `agents/`, `dashboards/`). Migrating the
> `hops/` skills into those buckets is tracked separately.

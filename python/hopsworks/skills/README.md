# Hopsworks skills

Markdown playbooks (`SKILL.md`) that drive an agent through Hopsworks workflows.
The **live, canonical list is `hops skills list`** (read one with
`hops skills show <name>`); this file is a static index for browsing.

Skills are routed by their **directory name** (e.g. `hops-fg`). They are grouped
into bucket folders:

## data/ — discovery, sources, SQL, Spark, table maintenance
- [hops-data-discovery](data/hops-data-discovery/SKILL.md) — find FGs, data sources, search, files.
- [hops-data-sources](data/hops-data-sources/SKILL.md) — mount external tables / DLTHub ingestion.
- [hops-dbt](data/hops-dbt/SKILL.md) — dbt models over feature groups in Trino, with tests and an execution graph.
- [hops-spark](data/hops-spark/SKILL.md) — PySpark on Hopsworks (Spark Connect + Delta).
- [hops-table-maintenance](data/hops-table-maintenance/SKILL.md) — Table layout maintenance for offline feature groups.
- [hops-trino-sql](data/hops-trino-sql/SKILL.md) — Trino SQL via the `hops` CLI.
- [hops-unstructured-data](data/hops-unstructured-data/SKILL.md) — parse files into a feature group.

## ml/ — FTI pipeline architecture: feature store, training, inference
- [hops-reqs](ml/hops-reqs/SKILL.md) — specify an ML system into `reqs/`.
- [hops-features](ml/hops-features/SKILL.md) — specify a feature pipeline.
- [hops-eda](ml/hops-eda/SKILL.md) — EDA before training.
- [hops-eda-checklist](ml/hops-eda-checklist/SKILL.md) — reference: EDA dimensions (profiling, target, leakage).
- [hops-fg](ml/hops-fg/SKILL.md) — feature groups.
- [hops-fv](ml/hops-fv/SKILL.md) — feature views, training data, online vectors.
- [hops-transformations](ml/hops-transformations/SKILL.md) — built-in/custom/on-demand transforms + transformation store.
- [hops-train](ml/hops-train/SKILL.md) — train + register a model.
- [hops-batch-inference](ml/hops-batch-inference/SKILL.md) — batch scoring + prediction logging.
- [hops-online-inference](ml/hops-online-inference/SKILL.md) — KServe model deployment.
- [hops-monitoring](ml/hops-monitoring/SKILL.md) — statistics, drift monitoring, validation, alerts.

## agents/ — served agents and scheduled agent tasks
- [hops-agent-deployment](agents/hops-agent-deployment/SKILL.md) — served interactive agent.
- [hops-agent-task](agents/hops-agent-task/SKILL.md) — scheduled coding-agent task (claude-code / codex job).

## dashboards/ — apps and BI
- [hops-app](dashboards/hops-app/SKILL.md) — Python app deployments (Streamlit + custom apps).
- [hops-superset](dashboards/hops-superset/SKILL.md) — Superset datasets / charts / dashboards.

## platform/ — cross-cutting platform knowledge and compute
- [hops-job](platform/hops-job/SKILL.md) — jobs and Airflow DAGs.
- [hops-environments](platform/hops-environments/SKILL.md) — clone a Python env, install custom libs.
- [hops-ui-navigation](platform/hops-ui-navigation/SKILL.md) — where things live in the Hopsworks UI (project sidebar map).
- [hops-collaboration](platform/hops-collaboration/SKILL.md) — project members, platform user admin, and feature store/dataset sharing.
- [hops-kubectl-debug](platform/hops-kubectl-debug/SKILL.md) — diagnose failed workloads from namespace-scoped pod state, events, and logs.

A `SKILL.md` carries what every task using the skill needs; deep dives,
copy-paste variants and long tables live in the skill's `references/` folder
(see `TEMPLATE_action.md` / `TEMPLATE_knowledge.md` for the shape).

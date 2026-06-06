---
name: hops-ui-navigation
description: Use when the user asks where something lives in the Hopsworks UI or how to reach a page (Data Sources, Feature Groups, Feature Views, Model Registry, Deployments, Apps, Agents, Jobs, Jupyter, Project Settings). Knowledge skill: the project-scoped sidebar layout and what sits under each section, so you can point users to the right place.
---

# Hopsworks UI — Navigation Map

A knowledge skill: it describes where things live in the Hopsworks web UI so you can guide a user to the right page.
It is not a workflow. For creating or operating resources, use the matching action skill (links at the bottom).

The UI is **project-scoped**: pick a project, then everything below is a left sidebar inside that project.
The sidebar is grouped into section headers (AI/ML, Agents, Compute, Analytics, Configuration) with pages under each.

## Source of truth

This map is derived by hand from the frontend, not maintained independently.
When routes or sidebar entries change, re-derive it from:

- `hopsworks-front/src/layouts/app/navigation/useAppNavigation.tsx` — the sidebar tree, order, and section headers.
- `hopsworks-front/src/routes/routeNames.ts` — the route paths behind each entry.
- `hopsworks-front/src/modules/feature-group/hooks/useTabsNavigation.tsx` and `src/modules/deployments/index.tsx` — titles for the dynamic Feature Groups / Deployments entries.

## Sidebar layout (top to bottom)

- **Home** — project landing page.
- **Files** — HopsFS file browser (datasets, `Resources/`, `Models/`, `Jupyter/`). Opens at the user's own folder.
- **Data Sources** — storage connectors (JDBC, S3, Snowflake, BigQuery, Redshift, GCS, ADLS, …). Create / edit / preview connectors here.
- **Feature Groups** — list and inspect feature groups; drill into a group for its schema, data preview, statistics, and provenance.

- **AI/ML** *(section)*
  - **Feature Views** — list feature views; a selected view exposes its Feature List, **Training Data** (Statistics, Correlations, New Training Data), Feature Logging, Provenance, Tags, API, and Query.
  - **Model Registry** — registered models and versions; a version shows Metrics, Evaluation, Model Card, Schema, Tags, Code Snippets (VLLM Config for LLM models).
  - **Model Deployments** — KServe model-serving endpoints; status, logs, predict, metrics.
  - **Apps** — Python / Streamlit app deployments and their URLs.

- **Agents** *(section)*
  - **Agent Deployments** — served interactive agents (server-only KServe deployments).
  - **Agent Jobs** — fire-and-forget agent executions; Overview and Executions.

- **Compute** *(section)*
  - **Environment** — Python environments (clone a base, install requirements/wheels).
  - **Jobs** — jobs and executions (Job Details, Configuration, Schedule, Scheduler, Alerts). Shown as **Ingestions** for service users.
  - **Airflow** — Airflow UI (only when `airflow_enabled`).
  - **Jupyter** — JupyterLab servers (hidden/disabled when Jupyter is off).

- **Analytics** *(section, only when Trino or Superset is enabled)*
  - **Queries** — Trino SQL query engine (history, runner).
  - **Superset** — opens the external Superset dashboard in a new tab.

- **Configuration** *(section)*
  - **Project Settings** — General, Integrations, Compute Configuration, Git, Kubernetes Scheduler, Governance Policies, OnlineFS Metrics, IAM Role Chaining, Kafka, Alerts.

## Conditional visibility

Some entries depend on edition, role, or service flags, so a user may not see all of them:

- **Enterprise-only** — Integrations, Compute Configuration, Kubernetes Scheduler, Governance Policies, OnlineFS Metrics, IAM Role Chaining (and Provenance / Tags on a feature view). A non-enterprise user sees these greyed out or hidden.
- **Service users** see **Ingestions** instead of **Jobs**, and have several settings disabled.
- **Feature flags** — Airflow (`airflow_enabled`), Jupyter (`enable_terminal` / enablement), Feature Monitoring, Trino, Superset gate their entries.

## Where do I find…?

| User asks | Sidebar path |
|---|---|
| A storage connector / external source | Data Sources |
| A feature group's schema or data | Feature Groups → (group) |
| Training data, statistics, correlations | AI/ML → Feature Views → (view) → Training Data |
| A registered model | AI/ML → Model Registry |
| A serving endpoint / predict | AI/ML → Model Deployments |
| A Streamlit app URL | AI/ML → Apps |
| A served agent / agent job | Agents → Agent Deployments / Agent Jobs |
| Python env / install a package | Compute → Environment |
| Run / schedule a job, see executions | Compute → Jobs |
| Notebooks | Compute → Jupyter |
| Trino SQL / Superset dashboard | Analytics → Queries / Superset |
| Git, Kafka, alerts, project config | Configuration → Project Settings |

Full docs: https://docs.hopsworks.ai

## Related skills and commands

- Discover resources without the UI: **hops-data-discovery** (`hops fg list`, `hops fv list`, `hops search ls`).
- Operate the resource behind a page: **hops-fg**, **hops-fv**, **hops-train**, **hops-online-inference**, **hops-app**, **hops-agent-deployment**, **hops-job**, **hops-environments**, **hops-superset**, **hops-data-sources**.
- A quick UI-equivalent snapshot from the CLI: `hops context`.

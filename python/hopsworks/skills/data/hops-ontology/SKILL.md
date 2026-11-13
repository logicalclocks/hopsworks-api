---
name: hops-ontology
description: Use when building an ontology system with the `ontology` factory or using one, an auto-generated business glossary and text-to-SQL chat over the project's SQL data sources (Auto-Ontology on Hopsworks). Auto-invoke on "ontology", "text-to-SQL", "ask my database", "business glossary", "hops ontology", "/hops-factory-ontology". Input data sources with SHARED credentials and a question; output a Python app, jobs, feature groups with vector indexes and vLLM deployments, and answers with their SQL.
---

# Ontology systems

An ontology system reads the metadata of the project's SQL data sources, compiles it with an LLM into business terms mapped to tables, columns and reviewed SQL, and answers questions about the data by writing and running SQL.
Each system of the `ontology` factory is one ontology, named by its slug:

| Part | Name |
| --- | --- |
| Python environment, shared by the project's ontology systems | `ontology-<commit>`, a clone of `python-feature-pipeline` with Auto-Ontology installed from the pinned fork commit |
| Python app (API, UI, chat) | `<slug>`, a custom app in that environment |
| App tables (catalog, ontology, chats) | `<app table_prefix><table>` in the project's online database |
| Jobs | `<slug>-ingest`, `<slug>-compile` (both scheduled), `<slug>-embed`, `<slug>-reset`, `<slug>-import` |
| Feature groups with vector indexes | `<slug_>_data_objects`, `<slug_>_semantic` (`<slug_>` is the slug with underscores) |
| Models and vLLM deployments, shared by the project's ontology systems | `Nemotron_3_5_Embed_1B_BF16` as `nemotronembed`, `NVIDIA_Nemotron_3_5_Lightning_30B_A3B_FP8` as `nemotronllm` |

In external mode the LLM or the embedding model is an OpenAI-compatible endpoint from the account variables (`LLM_URL`, `LLM_MODEL`, `LLM_API_KEY`, `EMBEDDING_URL`, `EMBEDDING_MODEL`, `EMBEDDING_API_KEY`) instead of a deployment.

## Contract
- **Input:** data sources of type MySQL, PostgreSQL, ClickHouse, Snowflake or Databricks with SHARED credentials; for local models, a GPU node with time-slicing.
- **Output:** a running app that answers questions with their SQL, and its catalog and ontology.
- **Pre-condition:** a Data owner builds it (the example creates tables, and delete needs a Data owner); `hopsable_proxy_enabled` on the cluster for the CLI and MCP tools.

## Build one

```bash
hops factory run ontology                          # asks the form's questions here
hops factory run ontology --preset wwi-example     # the sales example: its own schema and data source
```

Then `/hops-factory-ontology <slug>` in the system's directory builds it, phase by phase (reqs, models, app, ingest, compile, verify), from [references/auto_ontology/README.md](references/auto_ontology/README.md).
The build copies the app and job launchers in [references/auto_ontology/](references/auto_ontology/) verbatim and writes only `ontology.yaml` from the answers: the code is the `auto_ontology` package the environment ships.

## Use one

```bash
hops ontology status <slug>                                    # can it answer, and what blocks it; exits 1 when not
hops ontology ask <slug> "Which state has the most customers, and how many?"  # the answer, its SQL and up to 100 rows
hops ontology ask <slug> "And in Texas?" --conversation-id <uuid>  # a follow-up in a saved conversation
hops app url <slug>                                            # the app: catalog, terms, graph, chat
```

The app is also the App view of the system on its Factory page, at `/hopsworks-api/pythonapp/<project>/<slug>/`.
The MCP server (`hopsworks-mcp`) has the same reads as tools, each taking the slug as `system`: `check_readiness`, `search_terms`, `get_term`, `get_term_columns`, `get_term_sql_attributes`, `get_sql_attribute`, `describe_table`, `check_answerable` (a cheap pre-flight) and `ask_question` (slow: tens of seconds, steps reported as progress).
Prefer a term's SQL attributes to inventing arithmetic, and check readiness when an answer comes back empty.

The CLI and the tools reach the app through the Hopsworks app proxy with a proxy token minted for the caller, so the app sees the caller's name and applies the caller's project role.

## Roles

| Project role | In the app |
| --- | --- |
| Data owner | everything: sources, compilation and PII settings, reset, import and export, tags and rules, prompts, analytics, chat |
| Data scientist | read, catalog edits, saved analyses, chat |
| Observer, Feature store restricted | the catalog and tags, read-only, without sample values; no chat, since chat runs SQL on the data sources |
| Not a member | refused |

Only Data owners and Data scientists can mint the proxy token, so only they can use `hops ontology` and the MCP tools.

## When it does not answer

- **409 not compiled:** run `hops job run <slug>-compile --wait`, then `hops job run <slug>-embed --wait`.
- **No database ingested:** `hops job run <slug>-ingest --wait`; its log says which source failed.
- **App not serving:** `hops app logs <slug>`, then `hops app start <slug>`.
- **An answer from stale terms:** an edit in the app reaches the vectors after `<slug>-embed` runs.

## Delete

`hops factory system delete <slug> --assets` deletes the app, the jobs, the feature groups and the system's directory.
The shared models and deployments stay while another system names them, and go with the last one only when their description names it.

## Next steps
- [hops-factory](../../platform/hops-factory/SKILL.md): the factory definitions and `hops factory system`.
- [hops-data-sources](../hops-data-sources/SKILL.md): creating the SQL data sources an ontology reads.
- [hops-app](../../dashboards/hops-app/SKILL.md): the custom app the system runs.

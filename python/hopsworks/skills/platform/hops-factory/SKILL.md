---
name: hops-factory
description: Use when creating, editing, cloning, importing, exporting or deleting a software factory in Hopsworks (the six built-in ML system and medallion layer factories, or a project's own), or writing a factory definition YAML. Auto-invoke on "factory", "new factory", "factory definition", "hops factory". Input what the factory should build; output a validated definition imported into the project.
---

# Hopsworks software factories

A factory is a YAML definition: the questions of a creation form, the phases of the build, and the instructions Claude Code follows to build what the answers describe.
The Hopsworks UI generates the form, the Factory page section and the progress bar from it.
Six factories are built in and read-only: `ml-batch`, `ml-realtime` and `ml-agent` (ML systems) and `medallion-bronze`, `medallion-silver` and `medallion-gold` (medallion layers); `medallion-bronze` builds only its examples, generated data from a generator in the hops-medallion references.
A project's data owners add its own: written from scratch, cloned from any factory, or imported from a YAML file.

## Contract
- **Input:** what the factory should build and what to ask the user.
- **Output:** a definition that passes `hops factory validate`, imported with `hops factory import`.
- **Pre-condition:** the caller is a data owner of the project to create, edit or delete one.

## Commands
```bash
hops factory list                                  # built-in and project factories, version, systems
hops factory get <name> [--version N]              # the definition
hops factory export <name> [-o file.yaml]
hops factory validate file.yaml                    # offline check, no cluster needed
hops factory import file.yaml [--name new] [--yes] # create; --update saves a new version
hops factory clone <source> <new-name> [--title T]
hops factory enable|disable <name>
hops factory delete <name>                         # refused while systems built with it exist
hops factory run <name> [--answers answers.json] [--preset id] [--no-launch]
                                                   # a new system; without --answers the questions are asked here
hops factory run <name> <slug>                     # resume a system from its <slug>/system.yaml, no answers needed
hops factory system list [--factory <name>]        # the systems built, with the factory and version of each
hops factory system status|register|remove|delete <system>
hops factory run <name> <slug> --change <id> [--answers F]  # request one of the factory's changes; the build carries it out
hops factory system dir <slug>                     # the system's directory
hops factory system delete-assets <system> --job J --table T:V  # what a build deletes with; refuses what the system reads
```

## Writing a definition
Start from the smallest one that works and grow it; validate after every change.
```yaml
apiVersion: hopsworks.ai/factory/v1
kind: Factory
name: churn-review            # lowercase, digits, hyphens; the CLI name
title: Churn review
description: A weekly review of churn drivers, as a Superset dashboard.
form:
  sections:
    - id: basics
      title: Basics
      fields:
        - {id: name, type: slug, label: Name, required: true}
        - {id: question, type: textarea, label: "What should it answer?", required: true}
    - id: refresh
      title: Refresh
      collapsed: true          # a one-line summary of its answers, with Edit
      fields:
        - {id: cadence, key: refresh.cadence, type: choice, label: Cadence, default: weekly,
           options: [{value: daily, label: Every day}, weekly]}
        - id: tables
          type: list
          label: Tables to read
          item_label: Table
          fields:
            - {id: table, type: feature_group, label: Feature group, required: true, filter: {layer: [silver]}}
phases:
  - {key: build, label: Build, minutes: 20}
  - {key: verify, label: Verify, minutes: 5}
build:
  skills: [hops-superset]
  instructions: |
    Read requirements in system.yaml and build a dashboard that answers requirements.question.
```
- Every form needs a `slug` field: it names the system's directory.
- Quote a label holding `?`, `:` or `#`: it is YAML.
- A form has no conditions: every question of a section is shown. Put optional questions in a `collapsed: true` section, which shows a summary of its answers with Edit.
- `key` puts an answer at a dotted path of the answers the build gets; the id when absent.
- `account_env` fields (`env: LLM_API_KEY`, `secret: true`) are saved as the user's account variables, never in `system.yaml`.
- `presets` are named starting answers, listed under the factory's New button: `{id, label, answers: {<field id>: ...}, fixed: {...}}`.
- A phase key cannot be `system`, `factory` or `schema_version`.
- To extend a built-in instead of replacing it, clone it: the clone keeps `build.builtin` (`mlsystem`, `medallion-bronze`, `medallion-silver` or `medallion-gold`) and its questions; answers the built-in build does not read land in `requirements.extra` and the clone's instructions in `factory.instructions` of each system's `system.yaml`.

Field types, list columns and every rule: [references/definition.md](references/definition.md).

## Rules
- Validate before importing; fix every problem `hops factory validate` prints.
- Never put a secret, a token or a shell command meant to run unreviewed into a definition: the instructions run in the Terminal of whoever builds with it.
- Importing someone else's definition: read its `build.instructions` in full first, as `hops factory import` prints them.
- A factory with systems cannot be deleted; delete its systems first with `hops factory system delete <system> --assets`.

## Next steps
- [hops-job](../hops-job/SKILL.md) — the jobs a factory's build creates and schedules.
- [hops-ui-navigation](../hops-ui-navigation/SKILL.md) — the Factory page and Manage factories.

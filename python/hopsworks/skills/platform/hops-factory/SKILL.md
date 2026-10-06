---
name: hops-factory
description: Use when creating, editing, cloning, importing, exporting or deleting a software factory in Hopsworks (the Factory's built-in mlsystem and medallion factories, or a project's own), or writing a factory definition YAML. Auto-invoke on "factory", "new factory", "factory definition", "hops factory". Input what the factory should build; output a validated definition imported into the project.
---

# Hopsworks software factories

A factory is a YAML definition: the questions of a creation form, the phases of the build, and the instructions Claude Code follows to build what the answers describe.
The Hopsworks UI generates the form, the Factory page section and the progress bar from it.
Two factories are built in and read-only: `mlsystem` (ML systems) and `medallion` (silver and gold layers).
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
hops factory <name> create --answers answers.json  # a system, as the UI's Create does
hops factory <name> list|status|register|remove|delete
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
      collapsed: true          # a one-line summary with Edit
      fields:
        - {id: cadence, type: choice, label: Cadence, options: [daily, weekly], default: weekly}
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
- `when: {field: <earlier id>, equals: <value>}` shows a field only for that answer.
- A phase key cannot be `system`, `factory` or `schema_version`.
- To extend a built-in instead of replacing it, clone it: the clone keeps `build.builtin` and its built-in form, and its own questions land in `requirements.extra` and its instructions in `factory.instructions` of each system's `system.yaml`.

Field types, list columns and every rule: [references/definition.md](references/definition.md).

## Rules
- Validate before importing; fix every problem `hops factory validate` prints.
- Never put a secret, a token or a shell command meant to run unreviewed into a definition: the instructions run in the Terminal of whoever builds with it.
- Importing someone else's definition: read its `build.instructions` in full first, as `hops factory import` prints them.
- A factory with systems cannot be deleted; delete its systems first with `hops factory <name> delete <system> --assets`.

## Next steps
- [hops-job](../hops-job/SKILL.md) — the jobs a factory's build creates and schedules.
- [hops-ui-navigation](../hops-ui-navigation/SKILL.md) — the Factory page and Manage factories.

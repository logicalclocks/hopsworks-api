# Factory definition reference

`apiVersion: hopsworks.ai/factory/v1`, `kind: Factory`.
The cluster refuses a definition that breaks a rule below, and `hops factory validate` checks the same rules offline.

## Top level

| key | required | rule |
| --- | --- | --- |
| `name` | yes | `[a-z][a-z0-9-]*`, at most 63; not a built-in's name (`ml-batch`, `ml-realtime`, `ml-agent`, `analytics-bronze`, `analytics-silver`, `analytics-gold`, `analytics-pipeline`) |
| `title` | yes | text, at most 255 |
| `description` | no | text |
| `form.sections` | yes | a non-empty list |
| `phases` | yes | a non-empty list |
| `build` | yes | `builtin` or `instructions` |
| `list.columns` | no | extra columns of the Factory's list |
| `presets` | no | named starting answers |

The whole definition is at most 29000 bytes of UTF-8.

## Sections

| key | rule |
| --- | --- |
| `id` | unique, `[a-z][a-z0-9_]*` |
| `title` | text |
| `collapsed` | true shows the section as a one-line summary of its answers, with Edit |

A form has no conditions: every question of a section is shown, and `when` is refused.
| `fields` | a non-empty list |

## Fields

Every field has a unique `id`, a `type` and a `label`, and may have `help`, `required`, `default` and `key`.
`key` is the dotted path where the answer goes in the answers the build gets (`sla.batch.cadence`); the id when absent.

| type | renders as | answer |
| --- | --- | --- |
| `slug` | text checked against `[a-z][a-z0-9-]*`, unique among the project's systems; every form has one | string |
| `text`, `textarea` | input, text area | string |
| `number` | number input; `min`, `max` | number |
| `boolean` | checkbox | true or false |
| `choice` | one of `options` | string |
| `multichoice` | some of `options` | list of strings |
| `feature_group` | one of the project's feature groups; `filter: {layer: [bronze, silver, gold], hide_logging: true}` | `{name, version}` |
| `feature_groups` | several of them, with the same `filter` | list of `{name, version}` |
| `list` | entries added and removed by the user, each asking its own `fields`; `item_label`, `min_items` | list of objects |
| `account_env` | an input (`secret: true` hides it) saved as the user's account variable `env`; not inside a list | nothing: it never reaches the answers |
| `entry` | in a change's form only: one item of the list at `from` in `system.yaml` (a dotted path; a list met on the way is walked through, so `marts.jobs` is every mart's jobs), named by its `value` key (default `slug`) and shown by `show` (default `name`); with `fill: true` the chosen item's values prefill the change's other fields, read at their keys | the item's `value` |

An option is a value, or `{value, label}` to show a label.

## Phases

`{key, label, minutes}`: `key` unique, `[a-z][a-z0-9_]*`, not `system`, `factory` or `schema_version`; `minutes` the estimate the progress bar shows while it is not done.
The build writes each phase's `status` (`pending`, `running`, `met`, `unmet`), `started` and `finished` to the block of `system.yaml` named by its key.

## Build

- `instructions`: what Claude Code does, in prose. It runs as `/hops-factory-<name> <slug>` in the system's directory, wrapped in the rules every factory shares: resume from `system.yaml`, record phases, record `outputs`, keep the code in a `hops-<slug>` GitHub repository.
- `skills`: skills to load before the instructions.
- `builtin: mlsystem | analytics-bronze | analytics-silver | analytics-gold`: build with that built-in instead. `answers` are constants every system gets (`{system_type: batch}`); the answers that built-in reads go to it, the others to `requirements.extra`, and `instructions` to `factory.instructions`, which the built-in build follows too.

## List columns

`{label, from}`: `from` is a dotted path into the system's `system.yaml`, such as `requirements.cadence`.

## What a system gets

`hops factory run <name>` takes the answers from `--answers` (a JSON file nested by each field's key, as the UI writes it) or asks each question in the terminal, then writes `<slug>/system.yaml` with `factory: {name, version, phases}`, `system`, `requirements` (the answers shown) and a `pending` block per phase; `<slug>/.claude/commands/hops-factory-<name>.md`; and `<slug>/AGENTS.md`; then registers the system with the factory and the definition version it was built with.
Editing a factory saves a new version; a system keeps the version it was built with.
`hops factory run <name> <slug>` resumes the system from its `system.yaml`, which records the factory, its version and the answers, so it takes no answers file.

## Presets

`presets: [{id, label, description, answers, fixed}]`: `answers` are starting answers by field id, `fixed` answers sent as they are and never shown (the built-in ML factories' examples set `example` there).
The Factory lists them under the factory's New button; `hops factory run <name> --preset <id>` starts from one.

## Changes

`changes: [{id, label, description, form, instructions}]`: what can be asked of a system after it is built, each with its own form (the same field types, no slug field needed, `entry` fields allowed) and the `instructions` the build follows.
`hops factory run <name> <slug> --change <id>` takes the answers from `--answers` or asks them, checks them against the form and `system.yaml`, appends `{id, label, at, answers, instructions, status: pending}` to the system's `changes` in `system.yaml`, and resumes the build, which carries out each pending request first and marks it `done` or `failed`.
The Factory lists a system's changes under **Change** on its page. They are the factory's current version's, also for a system built with an earlier one.
A change that deletes part of a system says so in its instructions, and the build deletes only with `hops factory system delete-assets <system>`, which refuses a feature group the system reads or one of a lower analytics layer.

```yaml
changes:
  - id: drop-week
    label: Drop a week
    instructions: Remove the week in answers.week from requirements.weeks and rebuild the dashboard without it.
    form:
      sections:
        - id: pick
          title: Week
          fields:
            - {id: week, type: entry, label: Week, from: outputs.weeks, value: name}
```

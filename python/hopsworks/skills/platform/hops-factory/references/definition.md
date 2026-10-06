# Factory definition reference

`apiVersion: hopsworks.ai/factory/v1`, `kind: Factory`.
The cluster refuses a definition that breaks a rule below, and `hops factory validate` checks the same rules offline.

## Top level

| key | required | rule |
| --- | --- | --- |
| `name` | yes | `[a-z][a-z0-9-]*`, at most 63; not `mlsystem` or `medallion` |
| `title` | yes | text, at most 255 |
| `description` | no | text |
| `form.sections` | yes | a non-empty list |
| `phases` | yes | a non-empty list |
| `build` | yes | `builtin` or `instructions` |
| `list.columns` | no | extra columns of the Factory's list |

The whole definition is at most 29000 bytes of UTF-8.

## Sections

| key | rule |
| --- | --- |
| `id` | unique, `[a-z][a-z0-9_]*` |
| `title` | text |
| `collapsed` | true shows the section as a one-line summary of its answers, with Edit |
| `fields` | a non-empty list |

## Fields

Every field has a unique `id`, a `type` and a `label`, and may have `help`, `required`, `default` and `when`.

| type | renders as | answer |
| --- | --- | --- |
| `slug` | text checked against `[a-z][a-z0-9-]*`, unique among the project's systems | string |
| `text`, `textarea` | input, text area | string |
| `number` | number input; `min`, `max` | number |
| `boolean` | checkbox | true or false |
| `choice` | one of `options` | string |
| `multichoice` | some of `options` | list of strings |
| `feature_groups` | the project's feature groups; `filter: {layer: [bronze, silver, gold], hide_logging: true}` | list of `{name, version}` |
| `component` | a form the UI ships: `mlsystem.requirements`, `medallion.silver`, `medallion.gold` | that form's answers |

`when: {field: <id of an earlier field>, equals: <value>}` shows the field only while that answer equals the value; a hidden field is neither checked nor recorded.

## Phases

`{key, label, minutes}`: `key` unique, `[a-z][a-z0-9_]*`, not `system`, `factory` or `schema_version`; `minutes` the estimate the progress bar shows while it is not done.
The build writes each phase's `status` (`pending`, `running`, `met`, `unmet`), `started` and `finished` to the block of `system.yaml` named by its key.

## Build

- `instructions`: what Claude Code does, in prose. It runs as `/hops-factory-<name> <slug>` in the system's directory, wrapped in the rules every factory shares: resume from `system.yaml`, record phases, record `outputs`, keep the code in a `hops-<slug>` GitHub repository.
- `skills`: skills to load before the instructions.
- `builtin: mlsystem | medallion`: build with that built-in instead; its `component` form provides the answers it needs, the factory's other answers go to `requirements.extra` and `instructions` to `factory.instructions`, which the built-in's build follows too.

## List columns

`{label, from}`: `from` is a dotted path into the system's `system.yaml`, such as `requirements.cadence`.

## What a system gets

`hops factory <name> create --answers` writes `<slug>/system.yaml` with `factory: {name, version, phases}`, `system`, `requirements` (the answers shown) and a `pending` block per phase; `<slug>/.claude/commands/hops-factory-<name>.md`; and `<slug>/AGENTS.md`; then registers the system with the factory and the definition version it was built with.
Editing a factory saves a new version; a system keeps the version it was built with.

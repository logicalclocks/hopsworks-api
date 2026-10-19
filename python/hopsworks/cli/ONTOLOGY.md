# hops CLI ontology

The rules every `hops` command follows.
`tests/cli/test_ontology.py` reads the vocabulary below and fails on any command outside it, so a new name is added here first.

## Shape

```
hops <noun> [<owned noun>] <verb> [ARGS] [OPTIONS]
```

- A group is a noun: `fg`, `job`, `deployment`.
- A thing owned by an entity nests under it: `hops fg tag add`, `hops job schedule set`, `hops fv alert create`.
- A leaf is a verb, or a read-only view of its parent named by a noun (`hops fg lineage`, `hops job logs`).
- A view that gains a write action becomes a group with verbs: `hops fg stats info` and `hops fg stats compute`.
- `hops datasource create <type>` takes the data source type as its leaf.
- The first argument is the entity name; versions of other entities are options (`--fv-version`).

## Vocabulary

Leaves:

- Core: `list`, `info`, `create`, `update`, `delete`.
- Membership: `add`, `remove`, `set`.
- Lifecycle: `start`, `stop`, `run`, `deploy`, `redeploy`, `extend`, `reset`, `enable`, `disable`.
- Data: `insert`, `read`, `preview`, `lookup`, `query`, `predict`, `search`, `derive`, `compute`, `backfill`.
- Transfer: `upload`, `download`, `import`, `export`, `clone`, `push`, `pull`, `mirror`, `share`, `unshare`, `install`, `register`.
- Domain: `create-external`, `infer-metadata`, `test-access`, `validate`, `trigger`, `use`, `mkdir`, `acquire`, `release`, `renew`, `write-doc`, `delete-assets`.
- Views: `lineage`, `logs`, `history`, `schema`, `columns`, `tables`, `databases`, `catalogs`, `schemas`, `triggered`, `url`, `dir`, `status`.
- Top level: `setup`, `login`, `logout`, `context`, `sql`, `update`.

## Not used

These names are not used; the name after the colon replaces them:

- `get`, `show`, `describe`: `info`, or `update` when it writes.
- `new`: `start` or `create`.

## Help

- `hops -h` lists the groups, `hops <group> -h` lists its commands with one line each, `hops <group> <command> -h` shows the description and options.
- `-h` and `--help` work on every command.
- The summary line fits the group listing without truncation; details go in the description below it.
- Docstring sections after `\f` (`Args:`, `Returns:`) never show in help.

## Renames

Old names keep working for one release through `deprecations.py` and print the new name on stderr.

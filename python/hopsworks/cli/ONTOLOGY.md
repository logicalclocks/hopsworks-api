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
- Lifecycle: `start`, `stop`, `run`, `deploy`, `redeploy`, `extend`, `reset`, `enable`, `disable`, `wait`.
- Data: `insert`, `read`, `preview`, `lookup`, `query`, `predict`, `search`, `derive`, `compute`, `backfill`.
- Transfer: `upload`, `download`, `import`, `export`, `clone`, `push`, `pull`, `mirror`, `share`, `unshare`, `install`, `register`.
- Domain: `create-external`, `infer-metadata`, `test-access`, `validate`, `trigger`, `use`, `mkdir`, `acquire`, `release`, `renew`, `write-doc`, `delete-assets`, `plan`.
- Views: `lineage`, `logs`, `history`, `schema`, `columns`, `tables`, `databases`, `catalogs`, `schemas`, `triggered`, `url`, `dir`, `status`.
- Top level: `setup`, `login`, `logout`, `context`, `sql`, `update`.

## Not used

These names are not used; the name after the colon replaces them:

- `get`, `show`, `describe`: `info`, or `update` when it writes.
- `new`: `start` or `create`.

## Flags

One name per meaning:

- `--limit`: how many rows to show.
- `--env`: the Python environment a job, app, agent or deployment runs in.
- `--output`: a file to write; `--dir`: a directory to write into.
- `--wait/--no-wait`: whether to block until the operation finishes; `--timeout`: the seconds to wait, where the SDK takes a limit.
- `--yes`: skip the confirmation of a command that destroys data.
  `factory system delete-assets` does not ask: builds run it without a terminal, it names every asset it deletes, and it checks them before deleting any.
- `--version`: the version of the entity the first argument names.
  Versions of other entities name their entity: `--fv-version`, `--td-version`.
  `td` commands take a feature view first, so they use only `--fv-version` and `--td-version`.
- `--online`: the online store; it enables it on create commands and reads from it on `fg preview`.
  `fg insert --online-only` writes to it and skips the offline backfill.

## Retired flags

Each first name is gone; the name after the colon replaces it:

- `--n`: `--limit`.
- `--environment`: `--env`.
- `--out`: `--output`.
- `--training-dataset-version`: `--td-version`.
- `--wait SECONDS`: `--timeout SECONDS`.

## Errors

- A failure reads `Could not <verb> <thing> '<name>': <reason>`.
- A lookup that raised reads `Could not find <thing> '<name>': <reason>`; the cause may be auth or network, so it does not claim "not found".
- A lookup that returned nothing reads `<Thing> '<name>' not found.`; ids and versions go unquoted (`Alert 5 not found.`, `Training dataset v3 not found.`).
- Every other message is a sentence that starts with a capital letter and names the next step when there is one.
- A REST failure shows the server's message, and the scope to add when an API key lacks one.
- Exit codes: 0 success, 1 failure, 2 usage error or invalid input, 3 the system.yaml changed since it was read, 4 another invocation holds the build lease.

## Help

- `hops -h` lists the groups, `hops <group> -h` lists its commands with one line each, `hops <group> <command> -h` shows the description and options.
- `-h` and `--help` work on every command.
- The summary line fits the group listing without truncation; details go in the description below it.
- Docstring sections after `\f` (`Args:`, `Returns:`) never show in help.

## Renames

Old command and flag names keep working for one release through `deprecations.py` and print the new name on stderr.

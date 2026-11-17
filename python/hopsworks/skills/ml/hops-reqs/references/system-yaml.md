# `system.yaml`: rules, status vocabulary, writing and progress

The spec of record for one ML system, at the root of the system's repository
(`<repo>/<slug>/system.yaml` in a repository the user named that holds more). The
orchestrator reads state from this file only; each phase writes its own block.
A complete worked example, a batch churn system that finished every phase, is
[example-system.yaml](example-system.yaml); `tests/unit/test_system_yaml.py`
from the [system template](system_template/) validates a file against the rules
below and passes on that example.

The schema, every block and field of `system.yaml` with its meaning, is in `system-yaml-schema.md` (`hops skills info hops-reqs references/system-yaml-schema.md`).

## Rules

1. **One phase, one section.** A phase writes its own section, appends to `decisions`, and may
   update `system.status`. Every other line is preserved byte for byte, and the file must stay
   valid YAML. That is what makes the file trustworthy across runs.
   Every phase has a section, in the order the phases run: `requirements`, `data`,
   `features`, `training`, `inference`, `app`, `verify`, each with a `status`, so the
   file alone says how far the system got and what each step decided and measured. Each block
   also records `started` and `finished`, and `system.progress` the current position and the
   remaining estimate (Progress and estimates, below).
   Write a field with `python <slug>/set.py 'dotted.key=<yaml value>'` (`key+=value` appends to a
   list): it validates and renames into place, so an invalid write never replaces the file. While
   `requirements.status` is `pending` the file is a draft, and `task`, `system_type` and `sla` may
   still be missing, since the `/hops-ml` interview records them one answer at a time.
2. **Only as wordy as needed.** A field is one line unless it genuinely needs a `>` block. A
   `note` is one sentence. A decision is one line. The reader has the pipeline code next to the
   file; the YAML says what and why, the code says how.
3. **Numbers, not adjectives.** `targets` and `sla` hold a number, a direction, and how it is
   measured. "Good accuracy" is rejected at the requirements phase.
4. **Nothing under `measured` or `created` that was not verified with the `hops` CLI in the
   same run.** A claim without a check is not recorded.
5. **No secrets.** An LLM key is referenced by the name of the Hopsworks project secret that
   holds it.
6. **Naming.** Three identifier flavours from one slug, because Hopsworks object types have
   different character rules: `ident` (snake_case) for feature groups, feature views and
   models; `slug` (hyphens) for jobs and apps; `alnum` for deployments and agents.
7. **Validated on every write.** `tests/unit/test_system_yaml.py`, shipped with the system
   from the template, checks the file against this schema: `schema_version`, the status vocabulary, one `sla`
   block, one route per data source, the shape of every `runs` and `measured` row, and the
   identity fields on every piece of evidence. It runs with the unit tests, and a phase writes
   the YAML to a temporary file, runs that test against it, and renames it into place, so a
   half-written or invalid file never replaces a valid one.
8. **One invocation at a time.** The build holds the lease `<slug>/.hops.lock` (owner, token,
   expiry) for the whole invocation, renewing it as it goes, and another refuses to start
   while it holds, so two sessions cannot interleave writes. A lease past its expiry is taken
   over.
9. **Intent before effect, evidence after.** A row for a job run or a model registration is
   written with `state: submitted` before the remote call, updated to `running` with the
   execution id, and to `finished` or `failed` with the result, so a crash between the effect
   and its record is found by reconciliation on resume instead of producing a duplicate. Code
   commits and evidence commits are separate: the program is committed before it runs
   (`[<slug>] train run <n>: <what changed>`), its result after (`[<slug>] train run <n>:
   result`), and a revert targets the recorded code commit, never `HEAD`.

## Status vocabulary and transitions

Every phase block has a `status`. Satisfied: `met`; `skipped` (the phase does not apply, such
as `training` for an agent system without a model, `data` and `features` for a system whose
pretrained model reads only what a request sends, or `app` declined); `accepted` (the user
waived the target or SLA; the block records what was accepted and a `decisions` line says so).
Not satisfied: `pending`, `running`, `unmet`, `stale`. `verify` has `pass | fail`, is rerun by
every resume, and is never a gate.

| Change, or failed claim | Marked `stale` |
|---|---|
| `requirements` edited (any field but `status`) | every later phase |
| a data source's status or a generator's parameters changed | `data` onwards |
| an entrypoint, transformation or `evaluate.py` commit differs from the one its evidence names | the phase owning the file, and every later phase |
| feature view or training dataset version changed | `training` onwards |
| `training.model` changed (a new accepted or promoted version) | `inference` onwards |
| a deployment or job the YAML names is missing, stopped, unscheduled or failing | the phase that owns it |
| evidence past its expiry (benchmark and freshness claims: seven days by default) | the phase that owns it |

## Writing the file

A phase never edits `system.yaml` in place. `set.py` changes fields: under the
system's write lock it reads the file and its sha256, applies the change,
validates the result, writes a temporary file in the same directory, and
replaces the old one only when its sha256 is still the one read; otherwise it
redoes the change on the newer file. A whole new file goes through the same path:

```bash
python <slug>/tests/unit/test_system_yaml.py <slug>/.system.yaml.edit-1 \
  && hops factory system write-doc <slug> --expected-sha256 <sha256 of the file read> --from <slug>/.system.yaml.edit-1
```

`write-doc` exits 3 when the file changed since it was read (read it again and
redo the edit) and 2 when the new file is not valid YAML. The Hopsworks UI saves
an edited `system.yaml` the same way.

The lock is the build lease `<slug>/.hops.lock`, JSON written by
`hops factory run` before it starts Claude Code, which gets its token as
`$HOPS_LEASE_TOKEN`:

```json
{"owner": "meb10000@terminal-0:4121", "token": "9f0c...", "acquired": "2026-09-22T09:02:00Z", "expires": "2026-09-22T13:02:00Z"}
```

A second `hops factory run` refuses to start while it holds and prints the
owner. The build renews it (`hops factory system lease renew <slug>`) as each
phase starts and while it waits on a job, and releases it when it ends; a
lease past `expires` is taken over by the next run. A renewal that fails means
another invocation took over: the build stops writing at once.

## Progress and estimates

`system.progress` is written by the orchestrator at every event: `phase` (the
current phase), `done` (the satisfied phases), `now` (what runs, with its
execution id and elapsed time against `per_run`), `remaining_estimate` and
`estimate_basis`. `status.py` from the template renders the table from those
fields and each block's `started` and `finished`, without a session or the lock:

```
phase      status    started  took   remaining   now
reqs       met       09:02    11m
data       met       09:13    6m
features   met       09:19    14m
train      running   09:33    38m    ~22m        run 4 of 5, execution 1110 at 6m of 10m
infer      pending                   ~15m        5 attempts x ~3m
app        pending                   ~10m
verify     pending                   ~3m
3 of 7 phases done; about 50m left of ~1h59m; ends about 11:01 unless something escalates
```

Every phase block may carry `estimate`, the build's own expectation for that phase (`5m`,
`1h30m`): written for every phase when the requirements are met, from what they say (a
pretrained model is only downloaded; a training budget is its wall clock), and corrected
when the phase starts if the build knows better by then. The Factory page and `status.py`
show it in place of the defaults below.

Estimates, in order of preference:

- A phase that ran before (a rerun or a resume) is estimated from its own `started` and `finished`.
- A training round in progress: remaining runs (`max_runs` less the finished and failed rows) times the average run so far, capped by what is left of `wall_clock`.
- An inference round in progress: remaining attempts times the average attempt so far.
- The phase's own `estimate`, less the time spent since it started.
- Defaults before anything has run: reqs 10 min; data 5 min per existing feature group and 20 min per new connector, file or synthetic source; features 15 min per pipeline; train the budget's `wall_clock`, or 5 min for a pretrained model (`training.required: false`), which is only downloaded and registered; infer 3 min per attempt; app 10 min (none when `app.wanted` is false); verify 3 min.

An estimate is always labelled as one and excludes escalations, review rounds and a raised budget.

Every `started`, `finished` and `since` is UTC from `date -u` (`2026-09-22T09:02Z`). A
terminal may run in a local zone; a local time written as UTC makes a block finish before it
started, which the validator rejects.

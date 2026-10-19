# /hops-build: data, features, train, infer, app

Part of `/hops-build` (`hops-build.md`), read at the step that names it; the Rules and the lock there still hold.

## data

Make every `requirements.data_sources` entry `present` and record it under `data`. Load
**hops-data-sources** for new connectors, **hops-synthetic-data** for synthetic sources,
**hops-fg**, **hops-job**, **hops-environments**.

- Existing feature group: `hops fg info <name> --version <v>`, `hops fg preview <name> --limit 5`;
  key and event time must match the declared grain. A mismatch goes back to `reqs` as an
  `open_question`. Check that it stays fresh at the system's cadence: its newest event time
  (`hops sql "SELECT max(<event_time>) FROM <name>_<v>"`) and the job that writes it, with a
  schedule (`hops job list`, `hops job schedule info <job>`). With neither, scheduled inference
  would score the last snapshot forever: the features phase builds the refresh, or `reqs`
  records an `open_question` saying the source is static.
- New connector: follow the secret procedure in `references/data-sources.md` (print the exact
  command, which reads each secret with `read -rs` inside a subshell, for the user to paste into a
  separate shell, or redirect a file the user wrote), confirm with `hops datasource info`, find and inspect the
  table, then mount (offline reads) or ingest (online, vector index, or an API) by the
  **hops-data-sources** rule. A connector is created once and never deleted.
- File or URL: land it under `Resources/<slug>/data/`.
- Synthetic: copy `hops-synthetic-data/references/generator.py` to
  `src/<slug_pkg>/synthetic_data.py` and write the story as code in Polars; it runs in
  `python-feature-pipeline`, which ships Polars, so no environment is cloned. Run the backfill
  in this shell when the frame fits (rows x columns x 8 bytes under 1 GB), else as the backfill
  job, count the offline rows with `hops sql` after materialization, and for events deploy
  `<slug>-events` in `--mode live`.
  Recompute the online and offline sizes from the declared columns and put them in the
  `decisions` line. A volume above the tier's cap needs the user's one-line confirmation;
  an example's `requirements.sizing.confirmed_above_cap` is that confirmation, so an example
  asks nothing here either.
- `met` when every source is `present`, the generator's tests pass, and for events the live job
  runs and the online store has an event younger than two ticks.

## features

Load **hops-features**, **hops-fg**, **hops-transformations**, **hops-job**,
**hops-environments**. Create the phase's stubs first: `python <hops-reqs>/references/new_system.py <slug> --phase features` (`<hops-reqs>`
is the directory of the path `hops skills info hops-reqs` prints; a file the system has is kept). For each `requirements.features` entry `computed_in: feature_pipeline` or
`streaming`, write a pipeline from `src/<slug_pkg>/feature_pipeline.py` (one per pipeline) that
runs as `requirements.operations.features` says: scheduled (the window from `HOPS_START_TIME` and
`HOPS_END_TIME`) or continuous (Structured Streaming). Polars in a Python job by default (hops-features says
when Spark instead), and every feature group created with `statistics_config=False`, which from Python
would run a PySpark job per insert. Sink
feature groups with a description on every feature, created with `parents=` naming every feature
group the pipeline reads; validation before write; idempotent over the window. Features are written
at their natural grain with an `event_time`, and labels in their own group at the prediction time:
never a snapshot or pre-joined group for point-in-time correctness, which the feature view's join
provides (**hops-fv**). Write the unit and integration tests. Commit, build the bundle, run the backfill
in this shell when its sources fit (the Rules), else with `hops job backfill`, verify with bounded reads (`hops sql` count and newest event time,
`hops fg preview`), attach the schedule with its offsets and a failure alert
(`hops job alert create ... --status failed`), or start the continuous execution; for a batch
system run `benchmarks/benchmark_features.py` over one full window. Record each pipeline with its
environment, job, tests and benchmark. `met` when every pipeline has verified rows, a verified
schedule or running execution with its alert, and passing tests.

## train

Before spawning, create the phase's stubs (`python <hops-reqs>/references/new_system.py <slug> --phase train`), ask anything the round
will need (the split kind when it is unclear), print the estimate (the budget's `wall_clock`), write `training.status: running` and `started`, then spawn
the **hops-train-agent** sub-agent with the Agent tool and this prompt:

```
system.yaml: <absolute path>
goal: <requirements.targets, verbatim>
budget: <requirements.budget.training, verbatim>
protocol: <training.autoresearch when set, else the path of hops-train/references/autoresearch.md>
round: <n> of <budget.training.rounds>; this is <the first round | a new round after feature X>
instruction: <the phase instruction, or "none">
```

While it runs, a second terminal's `python <slug>/status.py` shows its rows. It returns
`{status: met | unmet | interrupted, best: {run_id, model, metric}, runs, recommendation:
{kind: feature | data | unreachable, detail}, partial}`. Check `len(training.runs)` against
`max_runs`. `interrupted`: ask its question with `AskUserQuestion`, record the answer, resume the
round. `unmet`: apply the escalation policy in `hops-reqs/SKILL.md` (build one recommended
feature between rounds, at most three rounds per invocation; ask the user for data or an
unreachable target, offering to accept the best model, which runs acceptance and sets
`training.status: accepted`).

## infer

Create the phase's stubs (`python <hops-reqs>/references/new_system.py <slug> --phase infer`), print the estimate (attempts x 3
min), write `inference.status: running`, then spawn the
**hops-infer-agent** sub-agent with:

```
system.yaml: <absolute path>
goal: <requirements.sla.<system_type>, verbatim>
budget: <requirements.budget.inference, verbatim>
mode: <batch | realtime>
instruction: <the phase instruction, or "none">
```

On `unmet`, ask with the measured table: change the SLA, change the design (realtime to batch),
or accept as is (`inference.status: accepted` and a `decisions` line).
When the agent's `recommendation.detail` reports monitoring it could not build (no alert receiver, feature monitoring off on the cluster), print it with what `inference.monitoring.unavailable` records and ask once whether to create the receiver (`hops alert receiver create`) and finish it.

## app

When `inference` is satisfied and the interview did not record `app.wanted`, ask once whether the
user wants an app, proposing what fits: a dashboard over the prediction feature group for batch, a
query UI against the deployment for realtime. When `app.wanted` is recorded, never ask: build it,
with `kind: dashboard` going to the dashboard builder and every other kind to the app builder. On
yes, take `app.description` when recorded, add what `system.yaml` says it reads (the prediction
feature group for batch, which has no deployment, so a what-if score embeds the registered model;
the deployment for realtime; the entity, the consumers), and spawn **hops-dashboard-builder** (`action: create`,
`program: <slug>/dashboards/<slug>.py`) or **hops-app-builder** (`action: create`, the source under
`<slug>/app/`) with the prompt `/hops` gives them, so the program or source is committed with the
system. Record `app`. On no, `app: {wanted: false, status: skipped}`.

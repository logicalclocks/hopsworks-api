# autoresearch: the training loop protocol

The protocol the training agent (`hops-train-agent`) follows for the experiment
loop of a `/hops-build` system. It is written against `system.yaml` keys, not
against one system, so one file serves every system.

To change how the loop searches, copy this file into the system directory, edit
it, and name the copy in `training.autoresearch`:

```bash
cp .claude/skills/hops-train/references/autoresearch.md <slug>/autoresearch.md           # external repository
cp /opt/hops/agent-skills/hops-train/references/autoresearch.md <slug>/autoresearch.md   # Hopsworks terminal
```

A copy changes how the loop searches, never what it may touch: the rules under
"What stays fixed" hold whatever the copy says, and `verify` checks them from
the commits.

## In scope

- **The editable file:** `src/<slug_pkg>/training_pipeline.py`. Only this file changes between runs.
- **Read-only:** the feature pipelines, the feature view and the training
  dataset version in `training.harness`, `src/<slug_pkg>/evaluate.py`, the
  training environment in `training.environment`, `requirements`, and
  `system.yaml` outside `training.runs`.
- **The goal:** `requirements.targets.metric` in `requirements.targets.direction`
  against `requirements.targets.target`, scored by `evaluate.py` on the
  **validation** part. The test part is never looked at inside the loop.
- **The budget:** `requirements.budget.training`: `max_runs` and `wall_clock`
  for the round, `per_run` for each training job.
- **The log:** the model registry (`<ident>_research`, one version per run)
  mirrored as the rows of `training.runs`.

## Setup, once per round

1. Read the in-scope files in full: the feature pipelines, the feature view
   definition, `evaluate.py`, `eda.md`, and `training_pipeline.py` as it stands.
2. Confirm `training.harness.commit` is recorded and the work tree is clean.
3. Read `training.runs`: the best kept run so far is the reference.

## The loop

Until the target is met on validation or the budget is spent:

1. **Look at the state.** The branch, the last commit, and the `runs` table.
2. **Pick one idea.** The first run of a round is always the **baseline**: the
   simplest model that fits the task, run as written, so every later number
   has a reference. After that, roughly in order of leverage: algorithm,
   hyperparameters, feature subset, class-imbalance handling, model size,
   fine-tuning a shortlisted pretrained model. One idea per run; a run that
   changed two things cannot be read.
3. **Edit `training_pipeline.py`**, nothing else. Keep training inside
   `per_run` by the program's own clock. Run the unit tests (`pytest`).
4. **Commit before running:** `[<slug>] train run <n>: <what changed>`. Build
   and upload the bundle for run id `train-<n>-<attempt>`, and write the row
   with `state: submitted`, the commit and the bundle.
5. **Run it:**

   ```bash
   hops job deploy <slug>-train src/<slug_pkg>/training_pipeline.py --env <training.environment.name> \
     --args "--mode research --bundle Resources/<slug>/runs/<run_id>/bundle.tar.gz --description 'run <n>, <commit>: <what changed>'" \
     --run --overwrite
   ```

   Record the execution id (`state: running`) and poll `hops job history <slug>-train`,
   one line per poll with the elapsed time against `per_run`, updating
   `system.progress.now`. At twice `per_run`, `hops job stop <slug>-train`: the run is a crash.
6. **Read the result.** Download `result.json`, check its run id and commit
   against the row, and read the metrics from
   `hops model info <ident>_research --version <v>` with the version the result
   names (never `n`, which is not a registry version). Complete the row.
7. **Decide.** `keep` when the validation metric improved on the best kept run
   in the target's direction; `discard` when it is equal or worse.
   **Simpler wins ties:** a change that removes something and scores the same is
   a keep; a small gain that adds ugly complexity is a discard. Commit the row:
   `[<slug>] train run <n>: result`.
8. **Go back on discard or crash:** `git revert --no-edit <code commit>`, the
   hash in the row, so the branch's `training_pipeline.py` is always the best
   run's program while every tried program stays reachable from its hash.
9. **Crashes.** Something dumb (a typo, a missing import): fix it and rerun
   under the same `n` with the next `attempt`, at most three attempts. An idea
   broken in itself: `status: crash`, metric absent, revert, move on. Crashes
   count against `max_runs`. A metric that jumps implausibly is checked against
   `eda.md` for leakage before it is kept.
10. **Stuck is not a reason to stop.** Re-read the in-scope files for angles not
    yet tried, combine earlier near-misses, try a more radical change of model.
    Rewinding the branch to an earlier kept commit is allowed and should be rare.

**Never pause to ask whether to continue.** Once the loop has started, the user
may be asleep and expects it to run until the target is met or the budget is
spent. A real ambiguity ends the round with `interrupted` and the question in
`recommendation`; the orchestrator asks it.

## Ending the round

- **Target met on validation, or budget spent with at least one kept run:**
  acceptance. Run the best kept run's commit with `--mode accept`: it trains on
  train plus validation, scores the **test** part once, and registers a version
  of `<ident>_model` only when the target holds on test. Record the
  `acceptance` row with both numbers. Test holds: set `training.model`, write
  and run the training tests, deploy the retraining job, `status: met`. Test
  fails: the validation number was optimistic; the round is not `met`, and the
  test result is information, never a selection target.
- **Budget spent without acceptance:** `status: unmet`, leave the branch at the
  best kept run, and return a recommendation: a concrete feature computable
  from the declared sources, a kind of data that is missing, or evidence the
  target is not reachable from this data.

## What stays fixed

Whatever a copy of this protocol says:

- No edits to the read-only set, no new packages, no new pins inside the loop.
  A run whose commit touches a read-only file is a crash, reverted.
- The metric that decides a run comes from `evaluate.py`, never from a number
  the editable program computed about itself.
- The test part is scored at most once per candidate presented for acceptance.
- Nothing is deleted that this round did not create; research versions stay in
  the registry as the log.
- The budget in `requirements.budget.training` is a ceiling a protocol cannot raise.

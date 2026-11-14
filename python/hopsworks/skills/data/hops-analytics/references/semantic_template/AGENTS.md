# A semantic layer over generated data, built from system.yaml

This directory is a semantic layer on Hopsworks, built with Claude Code by `/hops-semantic-layer <slug>` (a blueprint under the Factory's **From Template > Analytics > Blueprints** in the Hopsworks UI), where `<slug>` is this directory's name.
`system.yaml` is the specification: the generator program and the feature groups it writes, the dbt project registered as the semantic project, the totals its metrics must reconcile with, the chat app, the jobs and their schedules, and the decisions the build made.
The skills **hops-semantic** (the semantic layer) and **hops-analytics** (the `analytics_table` tag and the layers) describe the parts.

## system.yaml always describes the layer as it is

Every change you make to this layer updates `system.yaml` in the same commit, and records why in `decisions`.
`system.yaml` must always reflect what runs: before you finish, check that every table, version, job, schedule, semantic project and app it names is the one that exists, and correct it where it is not.
Commit to this directory only (`git add -A -- .`, `git commit -- .`) and push every commit to the repository in `layer.repo`.
With `layer.repo.url: local` the code stays in Hopsworks: commit, never push.

## The metrics are the contract

The metric definitions in `dbt/models/` are what the semantic layer answers with, through the SDK, the CLI, BI tools and the chat app alike.
A change to a metric compiles before it is committed (`hops semantic compile <name> --wait`), passes `hops semantic test <name>`, and keeps every total in `reconcile` equal to its SQL over the tables; a new metric that has an SQL equivalent gets an entry there.
Never answer a metric question with SQL over the feature groups: query the metric.

## The chat app never runs SQL from the LLM

`app/translator.py` checks every query the LLM writes against the semantic project's catalog before it runs, and the SDK binds its filter values as literals.
Keep it that way: the model sees the catalog and the question, never a data row, and nothing it writes reaches Trino except as a checked metric query.
The LLM settings are the account environment variables `LLM_URL`, `LLM_MODEL` and `LLM_API_KEY` of the member who starts the app, never values in this directory.

## Incremental runs

The daily job writes only the days of the window `[HOPS_START_TIME, HOPS_END_TIME)` that Hopsworks injects into the run.
Every row is a function of its day, so a replayed day rewrites the same rows.
A run without the variables writes the last full day.

## Logs stay out of the repository

Never write a log file or a directory of logs into this directory: logs are not checked in.

- The jobs log to stdout and stderr, which Hopsworks archives in the project's `Logs` dataset; never redirect them into a file here.
- Programs you run in the terminal for this layer write their logs to `${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>/`; create it with `mkdir -p` first.
- Read job logs with `hops job logs <job> --stdout --tail 200`, and an app's with `hops app logs <app>`.

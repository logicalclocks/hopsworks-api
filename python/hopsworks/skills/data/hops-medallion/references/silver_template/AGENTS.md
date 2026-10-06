# A silver medallion layer built from system.yaml

This directory is a silver layer on Hopsworks, built with Claude Code by `/hops-silver <slug>` (the Factory's **New Medallion Layer** in the Hopsworks UI), where `<slug>` is this directory's name.
`system.yaml` is the specification: the bronze sources, the silver tasks, the engine, the schedule, and the silver tables, job and decisions the build made.
The skill **hops-medallion** describes how a silver layer is built.

## system.yaml always describes the layer as it is

Every change you make to this layer updates `system.yaml` in the same commit, whatever it is for: a fix, maintenance, a new silver table or column, a changed task, schedule, engine or source.
Record why in `decisions`.
`system.yaml` must always reflect what runs: before you finish, check that every table, version, job and schedule it names is the one that exists, and correct it where it is not.
Commit to this directory only (`git add -A -- .`, `git commit -- .`) and push every commit to the medallion's GitHub repository (`layer.repo`), which its gold layers share.

## When system.yaml has changed

Before anything else in a session, compare the spec in `system.yaml` (`sources`, `tasks`, `extra_tasks`, `engine`, `schedule`, `layer.lifecycle`) with `outputs.applied_spec`, the spec the tables were last built from, and read `git log -p -- system.yaml` since the last `[<slug>] apply` or build commit.
A difference is a change to apply, with `/hops-silver <slug> apply`: retag for a lifecycle, reschedule for a schedule, and for a changed task, engine or source a new version of each silver table whose content changes, backfilled and switched to by the job (hops-medallion, Changing a layer).
Keep the superseded versions, and end by setting `outputs.applied_spec` to the spec applied.
An `additions` entry with `status: pending` is a request for new tables from its sources, in the user's words: design and build them like the first tables, and set it to `applied`.

## Silver tables are materialized and in third normal form

Every silver table is a feature group written by this layer's job, never a view or an external feature group over bronze.
The silver tables are in third normal form: one table per entity or event, atomic columns, every non-key column depending on the whole key and nothing but the key, lookups in their own tables, no aggregates or denormalized copies, which belong in gold.
A change that would break the normal form is redesigned, not built.
Bronze feature groups are never modified.
Every silver feature group carries the `medallion_table` tag with `layer: silver` and the lifecycle in `system.yaml`.

## Incremental runs

The job runs on a schedule and processes only the bronze rows whose arrival column is in `[HOPS_START_TIME, HOPS_END_TIME)`, the window Hopsworks injects into each scheduled run.
A run without the variables processes the whole bronze history; that is how the first backfill runs.
A replayed window must leave the silver tables unchanged.

## Lineage, schedule, status and backfill

Every silver feature group is created with the bronze feature groups it reads as `parents`.
There is one job per refresh cadence (`<slug>-silver-<cadence>`), each writing the silver tables of that cadence and scheduled with `--catchup`, so missed windows are replayed.
`hops factory system status <slug>` writes the layer's health report to `status/report.html`; `hops job run <job> --start-time 1970-01-01 --end-time <now> --wait` reprocesses the whole bronze history for a job (a plain run of the scheduled job gets only the last cron interval).

## Logs stay out of the repository

Never write a log file or a directory of logs into this directory: logs are not checked in.

- The job logs to stdout and stderr, which Hopsworks archives in the project's `Logs` dataset; never redirect them into a file here.
- Programs you run in the terminal for this layer write their logs to `${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>/`; create it with `mkdir -p` first.
- Read job logs with `hops job logs <job> --stdout --tail 200`, or download them with `--dir ${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>`.

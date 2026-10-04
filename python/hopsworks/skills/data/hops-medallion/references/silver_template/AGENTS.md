# A silver medallion layer built from layer.yaml

This directory is a silver layer on Hopsworks, built with Claude Code by `/hops-silver <slug>` (the Factory's **New Medallion Layer** in the Hopsworks UI), where `<slug>` is this directory's name.
`layer.yaml` is the specification: the bronze sources, the silver tasks, the engine, the schedule, and the silver tables, job and decisions the build made.
The skill **hops-medallion** describes how a silver layer is built.

## layer.yaml always describes the layer as it is

Every change you make to this layer updates `layer.yaml` in the same commit, whatever it is for: a fix, maintenance, a new silver table or column, a changed task, schedule, engine or source.
Record why in `decisions`.
`layer.yaml` must always reflect what runs: before you finish, check that every table, version, job and schedule it names is the one that exists, and correct it where it is not.

## Silver tables are materialized

Every silver table is a feature group written by this layer's job, never a view or an external feature group over bronze.
Bronze feature groups are never modified.
Every silver feature group carries the `medallion_table` tag with `layer: silver` and the lifecycle in `layer.yaml`.

## Incremental runs

The job runs on a schedule and processes only the bronze rows whose arrival column is in `[HOPS_START_TIME, HOPS_END_TIME)`, the window Hopsworks injects into each scheduled run.
A run without the variables processes the whole bronze history; that is how the first backfill runs.
A replayed window must leave the silver tables unchanged.

## Logs stay out of the repository

Never write a log file or a directory of logs into this directory: logs are not checked in.

- The job logs to stdout and stderr, which Hopsworks archives in the project's `Logs` dataset; never redirect them into a file here.
- Programs you run in the terminal for this layer write their logs to `${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>/`; create it with `mkdir -p` first.
- Read job logs with `hops job logs <job> --stdout --tail 200`, or download them with `--dir ${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>`.

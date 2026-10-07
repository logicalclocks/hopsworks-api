# A bronze analytics layer of generated data, built from system.yaml

This directory is a bronze layer on Hopsworks, built with Claude Code by `/hops-bronze <slug>` (an example under the Factory's **New Analytics** in the Hopsworks UI), where `<slug>` is this directory's name.
`system.yaml` is the specification: the generator program, the bronze tables it writes, the jobs that run it and their schedules, and the decisions the build made.
The skill **hops-analytics** describes the analytics layers; silver and gold layers are built on these tables with **New Analytics**.

## system.yaml always describes the layer as it is

Every change you make to this layer updates `system.yaml` in the same commit, and records why in `decisions`.
`system.yaml` must always reflect what runs: before you finish, check that every table, version, job and schedule it names is the one that exists, and correct it where it is not.
Commit to this directory only (`git add -A -- .`, `git commit -- .`) and push every commit to the analytics pipeline's GitHub repository (`layer.repo`), which its silver and gold layers share.

## Bronze tables hold data as it arrived

Every bronze table is an offline Delta feature group, written only by this layer's jobs and tagged `analytics_table` with `layer: bronze` and the lifecycle in `system.yaml`.
The generator writes raw data on purpose: duplicate deliveries, nested JSON, rows that change over time.
Never clean it here; cleansing is the silver layer's job.

## Incremental runs

Each scheduled job writes only the window `[HOPS_START_TIME, HOPS_END_TIME)` that Hopsworks injects into the run, and seeds its random generator with the window's start, so a replayed window rewrites the same rows.
A run without the variables writes the last full window of its cadence.

## Logs stay out of the repository

Never write a log file or a directory of logs into this directory: logs are not checked in.

- The jobs log to stdout and stderr, which Hopsworks archives in the project's `Logs` dataset; never redirect them into a file here.
- Programs you run in the terminal for this layer write their logs to `${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>/`; create it with `mkdir -p` first.
- Read job logs with `hops job logs <job> --stdout --tail 200`.

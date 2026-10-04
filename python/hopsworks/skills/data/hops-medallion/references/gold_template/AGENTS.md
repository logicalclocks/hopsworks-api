# A gold medallion layer built from system.yaml

This directory is a gold layer on Hopsworks, built with Claude Code by `/hops-gold <slug>` (the Factory's **New Medallion Layer**, gold, in the Hopsworks UI), where `<slug>` is this directory's name.
`system.yaml` is the specification: the queries the layer serves, its Kimball model (star or snowflake), the silver tables it reads, the standards every mart follows, and its data marts, each with its requirements, tables and jobs.
The skill **hops-medallion** (and its `references/gold-marts.md`) describes how a gold layer is built.

## system.yaml always describes the layer as it is

Every change you make to this layer updates `system.yaml` in the same commit, whatever it is for: a fix, maintenance, a new mart, table or metric, a changed requirement or schedule.
Record why in `decisions`, naming the mart.
`system.yaml` must always reflect what runs: before you finish, check that every mart, table, version, job and schedule it names is the one that exists, and correct it where it is not.
Commit each change and push it to the layer's GitHub repository (`layer.repo.url`).

## Data marts

A data mart is the unit that is added, changed and deleted: its own requirements, fact and dimension tables, and jobs at its own cadence.
A dimension used by several marts is conformed: built once, by the first mart that needs it, and marked `shared` in each mart that reads it; deleting a mart never deletes a table another mart lists.
Gold tables are materialized feature groups, tagged `medallion_table` with `layer: gold`, created with the silver tables they read as `parents`.
Silver and bronze tables are never modified or deleted from here.

## When system.yaml has changed

Compare each mart's `requirements` and `cadence` with its `applied` snapshot, and the layer's spec with `outputs.applied_spec`: a difference is a change to apply with `/hops-gold <slug> apply`.

## Logs stay out of the repository

Never write a log file or a directory of logs into this directory.
Jobs log to stdout and stderr, archived in the project's `Logs` dataset; programs you run in the terminal write to `${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>/`.

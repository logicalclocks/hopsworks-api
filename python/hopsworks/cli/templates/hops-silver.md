---
description: Hopsworks silver layer builder. Builds the silver medallion layer that system.yaml describes, from bronze feature groups to materialized silver feature groups refreshed incrementally by a scheduled job (dbt on Trino by default, PySpark when a task needs it), and tags them. Phases can be run alone.
argument-hint: "[<slug>] [profile|design|code|backfill|schedule|verify|apply]"
---

You are running `/hops-silver` with arguments: `$ARGUMENTS`

Already known, no need to look again before the first question:
- Layers here (`system.yaml` alone: this directory is the layer): !`ls -d system.yaml */system.yaml 2>/dev/null | head -5 || true`
- UTC now: !`date -u +%FT%H:%MZ`
- Feature groups: !`hops fg list 2>&1 | head -40`

## Dispatch

| First word | Do |
| --- | --- |
| the slug of a layer above | work on that layer, and dispatch the rest of the arguments by this table (the Factory and `hops factory run medallion-silver` start the build this way) |
| none | with a `system.yaml` above: resume it from its first phase that is not `done`; without one: reply that the Factory's **New Medallion Layer**, or `hops factory run medallion-silver --answers`, records a layer first, and stop |
| `profile`, `design`, `code`, `backfill`, `schedule`, `verify` | that phase, then every later phase that is not `done` |
| `apply` | apply the changes to `system.yaml` since the layer was built (below) |

When every phase is `done` and the spec differs from `outputs.applied_spec`, a bare `/hops-silver <slug>` also runs `apply`.

Load **hops-medallion** first: its `SKILL.md` (the tag, the tasks, the settings, the engine, incremental processing, lineage, partitioning, schedule, the phases) and `references/silver-tasks.md` are what this builder runs on, and **hops-partitioning** for the design.
Load **hops-dbt** for a `dbt_trino` engine and **hops-spark** for `pyspark`, and **hops-job** and **hops-fg** before deploying the job or creating a feature group.
Find a skill at `.claude/skills/<name>/` in the repository, else `~/.claude/skills/<name>/` (in a Hopsworks terminal that links to `/opt/hops/agent-skills/`).

**Where it runs.** The Factory starts Claude Code in the layer's directory, `<slug>/`, so it reads the layer's `AGENTS.md`; every `<slug>/<path>` here is `<path>` in the current directory.

## Rules

- **A cloned factory's additions.** `system.yaml`'s `factory` block names the factory that created the system. When it has `instructions`, a project factory cloned from a built-in one added them: follow them as well as this command, and treat `requirements.extra` as further requirements the user gave.
- **system.yaml is the record.** Set `layer.status` to `building` when you start and `built` or `failed` when you end; set each phase's `status`, `started` and `finished` (UTC, from `date -u`) as it runs, and `progress.now` to one line on what you are doing. The Factory reads it every few seconds.
- **Ask, never guess.** When the arrival column of a source, the business key of a table, a match rule between sources, or the meaning of an extra task is unclear, ask with `AskUserQuestion`: one call, up to four questions, two to four concrete options each with the recommended one first. Record every answer and every choice you make yourself in `decisions` (`at`, `by: user` or `by: claude`, `what`, `why`).
- **The engine.** Keep `dbt_trino` unless a task needs code SQL cannot express well (hops-medallion, The engine); then say why, record it in `decisions`, and switch `engine` to `pyspark`.
- **Materialized, incremental, idempotent.** Silver tables are feature groups the job writes; the job reads only `[HOPS_START_TIME, HOPS_END_TIME)` of each source's arrival column when the variables are set, and the whole history when they are not; replaying a window changes nothing.
- **Third normal form.** Every silver table is in 3NF; a design that is not, or a change that would break it, is redesigned rather than built.
- **Bronze is read-only.** Never insert into, update, delete or retag a bronze feature group.
- **Commit each phase** in the layer's git work tree: `[<slug>] <phase>: <what>`, `system.yaml` with the code.
- **The GitHub repository.** A medallion's silver and gold layers share one work tree and one private GitHub repository, `layer.repo.name` (`hops-<prefix>`, the parent of this directory); each layer is a directory in it. With no `layer.repo.url`: when the work tree already has an `origin` (another layer of the medallion set it), record its URL; else when `gh repo view <layer.repo.name>` finds the repository, add it as `origin`; else, with `gh auth status` logged in, create it (`gh repo create <layer.repo.name> --private --source .. --remote origin --push`). Record the URL as `layer.repo.url`. Commit only this layer's directory (`git add -A -- .`, `git commit -m ... -- .`), so another layer's work in progress stays out of the commit, and push after every commit. Without a GitHub login, say once that `github-login` connects one, and keep committing locally.
- **Logs stay out of the directory**, as `AGENTS.md` says.
- **No secrets** in arguments, `system.yaml` or the code: a salt or key is a Hopsworks secret read at run time.
- **Never delete** what this build did not create.

## The phases

### profile

For each source in `sources`: `hops fg info <name> --version <v>`, `hops fg preview <name> --version <v> -n 20`, and Trino queries (`hops trino query`) for the row count, the duplicate rate on each key candidate, null rates, the distinct values of low-cardinality strings, and the range of each timestamp.
Set `sources[].arrival_column`: the feature group's `event_time`, else a load timestamp the ingestion wrote; when neither exists, ask (the column to use, or full reprocessing each run for a small table).
Record the profile in `system.yaml` under each source (`rows`, `key_candidates`, `duplicate_rate`, `nulls`, `notes`).

### design

Design the silver tables from the sources, the `tasks` and `extra_tasks`, in third normal form (hops-medallion, Silver is in third normal form): find the entities and events in the bronze tables and the functional dependencies between their columns from the profile, split repeating groups into child tables, move each attribute to the table of the key it depends on, and make lookups their own tables.
One table per entity or event, named after it, lowercase with underscores (`customers`, `order_lines`), each with its sources, business key (the feature group's primary key, a surrogate key when `surrogate_keys` is a task), foreign keys (`references`), `event_time`, columns, the tasks applied to each, and the dependencies that justify the split.
No aggregates, derived totals or denormalized copies of another table's attributes: those are gold.
No silver table takes the name of an existing feature group (check `hops fg list`): `get_or_create_feature_group` would return that group, a bronze one included, and the job would write into it; prefix the names with the domain or the layer when they would collide.
With `validate`, a `<table>_rejects` feature group per table.
Decide each table's partitioning with **hops-partitioning**: run its `partition_advisor.py` on the bronze tables' files under `/hopsfs/featurestore/<project>_featurestore.db/<fg>_<version>` with the arrival column, and record the decision, column and evidence in `partitioning` (per table when they differ).
Record each bronze table's schema (column names and types) under its source, for the schema-change check.
Give each silver table the cadence of its most frequently updated source (`sources[].cadence`; hourly before daily before weekly), and group the tables into one job per cadence: `<slug>-silver-<cadence>`, environment `dbt-pipeline` (or a PySpark job), cron and catch-up limit from `schedule.cadences.<cadence>`; record `outputs.tables[].cadence` and `outputs.jobs` (`{name, cadence, cron, tables}`). Ask when a table's sources span cadences and the user may want it slower.
Write it all to `outputs` (versions 1) before any code, and show the design as a short table; ask only what is unclear.

### code

Write one program that every job runs, taking `--cadence <cadence>` to build only that cadence's tables from their sources. For `dbt_trino`, a dbt project in `dbt/` (ephemeral models, `sources.yml` over the bronze tables, mapping models, data tests for `validate`) and a runner `run_silver.py` that passes `HOPS_START_TIME`/`HOPS_END_TIME` as dbt vars when set, runs `dbt build`, executes each compiled model on Trino and inserts the rows into its silver feature group (hops-dbt, Landing the model output); for `pyspark`, `silver.py` reading each source with `fg.read()` and inserting each table.
Create every silver feature group with `get_or_create_feature_group` (Delta, offline, `statistics_config=False` so an insert from a Python job starts no Spark statistics job, primary key as designed, `event_time` only with `history: full`, `partition_key` as decided, `parents=` the bronze feature groups it reads, a description, a description per feature) in the program, so the first run creates it.
Implement the settings (hops-medallion, The layer's settings): the read window starts `late_data.lookback` before `HOPS_START_TIME`; the bronze schemas are checked against the recorded ones first (`schema_changes`); rejected rows are counted and the run fails above `quality.max_reject_pct` before silver is written; deletes are propagated when `deletes: propagate`.
The program logs its window, the rows read, written and rejected per table, and each check's result.
Write unit tests in `tests/` (DuckDB over sample rows for the dbt SQL, pandas or local Spark for PySpark) for every task, and run them.

### backfill

Deploy one job per cadence (`hops job deploy <slug>-silver-<cadence> run_silver.py --env dbt-pipeline --args "--cadence <cadence>"`, uploading the dbt project with `hops files upload --overwrite`), run each once without a window, slowest cadence first (`hops job run <slug>-silver-<cadence> --wait`; before it is scheduled the run gets no window), and check each silver table: row count against its sources, no duplicate primary key (and `event_time` with `history: full`), no nulls in key columns, the rejects counted, and its parents (`hops fg lineage <name>`).
Fix and rerun until the checks pass; record the counts in `outputs`.

### schedule

Schedule each job with catch-up (`hops job schedule <slug>-silver-<cadence> "<schedule.cadences.<cadence>.cron>" --start-time <end of the backfill> --catchup --max-catchup-runs <its max_catchup_runs>`), so the windows continue from the backfill and missed ones are replayed.
With `quality.alert_on_failure`, create a failure alert on each job (`hops alert job create <slug>-silver-<cadence> --receiver <receiver> --status failed --severity critical`; `hops alert receiver list` for the receiver, asking which one when there are several, and recording it).
Tag every silver and rejects feature group `medallion_table` with `{"layer": "silver", "lifecycle": "<layer.lifecycle>"}` (`hops fg add-tag <name> medallion_table --value '...'`).

### verify

For each job, run one window of its cadence (`hops job run <slug>-silver-<cadence> --start-time <t0> --end-time <t1> --wait`) over a stretch of bronze arrivals, check that only that window's rows were read (the job's log states its window and row counts), that a second run of the same window leaves every silver table unchanged, and that the tags are set.
Run `hops factory system status <slug>` and fix anything it flags.
Set `outputs.applied_spec` to the spec just built (`sources` names and versions, `tasks`, `extra_tasks`, `engine`, `schedule`, `layer.lifecycle`, `history`, `deletes`, `schema_changes`, `late_data`, `quality`, `freshness`), set `layer.status: built`, and report the silver tables, the job, its schedule and the checks, in a few lines.

### apply

Compare the spec with `outputs.applied_spec`, and read `git log -p -- system.yaml` since the last build or apply commit for why it changed.
Each `additions` entry with `status: pending` asks for new tables from its sources, described in the user's words: profile those sources, design the tables in 3NF with the rest, and build them as a source added; set the entry's `status: applied` when they are verified.
Show what changed and what each change recomputes (hops-medallion, Changing a layer) as a short table, and ask only when a change is ambiguous.
Set `layer.status: building` and the affected phases back to `pending`, then run them: a lifecycle change retags every silver and rejects feature group; a schedule change reschedules the job; a changed task, extra task, engine or source changes and tests the code, creates the next version of each silver table whose content changes, backfills it over the whole bronze history, switches the job to it, and verifies one window.
Record each new version in `outputs`, and each superseded one in `decisions`; never delete one.
End with `outputs.applied_spec` set to the spec applied and `layer.status: built`, in one commit `[<slug>] apply: <what changed>`.

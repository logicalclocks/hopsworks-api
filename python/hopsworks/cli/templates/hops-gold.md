---
description: Hopsworks gold layer builder. Builds the data marts of the gold medallion layer that system.yaml describes, a Kimball star or snowflake model of fact and dimension feature groups built from silver tables, each mart with its own requirements and its own scheduled jobs (dbt on Trino by default), and tags them. A mart can be built, changed or rebuilt alone.
argument-hint: "[<slug>] [<mart>|apply] [requirements|design|code|backfill|schedule|verify|dashboards]"
---

You are running `/hops-gold` with arguments: `$ARGUMENTS`

Already known, no need to look again before the first question:
- Layers here (`system.yaml` alone: this directory is the layer): !`ls -d system.yaml */system.yaml 2>/dev/null | head -5 || true`
- UTC now: !`date -u +%FT%H:%MZ`
- Feature groups: !`hops fg list 2>&1 | head -60`

## Dispatch

| Arguments after the slug | Do |
| --- | --- |
| none | the pending change requests (Rules), then every mart, in order: build each from its first phase that is not `done`, and apply the changes of a built mart whose spec differs from its `applied` |
| `<mart>` | that mart alone, the same way |
| `<mart> <phase>` | that phase of that mart, then every later phase that is not `done` |
| `apply` | the changes to the layer-level spec since it was built: `layer.lifecycle`, `layer.modeling`, `standards`, `sources` (below) |

Without a slug and with no `system.yaml` here, reply that the Factory's **New Medallion Layer** (gold), or `hops factory run medallion-gold --answers`, records a layer first, and stop.

Load **hops-medallion** first: its `SKILL.md` (the tag, lineage, the schedule, incremental processing) and `references/gold-marts.md` (the requirement questions, Kimball modeling, the standards, the jobs) are what this builder runs on.
Load **hops-dbt** for the dbt project and its runner, **hops-job** before deploying a job, **hops-fg** before creating a feature group, and **hops-trino-sql** for the queries.
Find a skill at `.claude/skills/<name>/` in the repository, else `~/.claude/skills/<name>/` (in a Hopsworks terminal that links to `/opt/hops/agent-skills/`).

**Where it runs.** The Factory starts Claude Code in the layer's directory, `<slug>/`, so it reads the layer's `AGENTS.md`; every `<slug>/<path>` here is `<path>` in the current directory.

## Rules

- **Change requests.** The Factory and `hops factory run <factory> <slug> --change <id>` record a change to the system as a `changes` entry with `status: pending`, holding its `label`, `answers` and `instructions`. Before anything else, carry out each pending one, oldest first: follow its `instructions` with its `answers`, then set its `status: done` and `finished` (UTC), or `status: failed` with a `reason` you also report, and commit `[<slug>] <label>`. Delete a job or a feature group only with `hops factory system delete-assets <slug> --job <name> --table <name>:<version>`, which refuses what the system reads.
- **A cloned factory's additions.** `system.yaml`'s `factory` block names the factory that created the system. When it has `instructions`, a project factory cloned from a built-in one added them: follow them as well as this command, and treat `requirements.extra` as further requirements the user gave.
- **system.yaml is the record.** Set `layer.status` to `building` when you start and `built` or `failed` when you end, and each mart's `status` (`draft`, `building`, `built`, `failed`) and phases (`status`, `started`, `finished`, UTC from `date -u`) as they run, and `progress.now` to one line naming the mart and what you are doing. The Factory reads it every few seconds.
- **Ask, never guess.** A requirement left blank, a metric whose formula is ambiguous, a grain that does not identify a row: ask with `AskUserQuestion`, one call, up to four questions, two to four concrete options each with the recommended one first. Record every answer and every choice you make in `decisions` (`at`, `by: user` or `by: claude`, `what`, `why`, `mart`).
- **Kimball.** Facts at one declared grain, dimensions with surrogate keys, star or snowflake as `layer.modeling` says, conformed dimensions built once (references/gold-marts.md, Modeling).
- **The standards.** Every table, column, metric and test follows `standards`; a design that breaks one is redesigned, or the user agrees to the exception and it is recorded in `decisions`.
- **Materialized, incremental, idempotent.** Gold tables are feature groups the mart's jobs write; a job reads only `[HOPS_START_TIME, HOPS_END_TIME)` of its silver sources when the variables are set, and everything when they are not; replaying a window changes nothing.
- **Silver and bronze are read-only.** Never insert into, update, delete or retag them.
- **A mart owns its tables.** A table is listed in the mart that builds it; a mart that reads another mart's table lists it with `shared: true` and never writes it.
- **Commit each phase** in the layer's git work tree, `[<slug>] <mart> <phase>: <what>`, with `system.yaml`, and push it.
- **The GitHub repository.** A medallion's silver and gold layers share one work tree and one private GitHub repository, `layer.repo.name` (`hops-<prefix>`, the parent of this directory); each layer is a directory in it. With no `layer.repo.url`: when the work tree already has an `origin` (another layer of the medallion set it), record its URL; else when `gh repo view <layer.repo.name>` finds the repository, add it as `origin` and put the work tree's commits on its history (`git fetch origin` then `git rebase origin/<its default branch>`; the work tree was started fresh, and each layer's commits touch only its own directory); else, with `gh auth status` logged in, create it (`gh repo create <layer.repo.name> --private --source .. --remote origin --push`). Record the URL as `layer.repo.url`. Commit only this layer's directory (`git add -A -- .`, `git commit -m ... -- .`), so another layer's work in progress stays out of the commit, and push after every commit. Without a GitHub login, say once that `github-login` connects one, and keep committing locally.
- **Logs stay out of the directory**, as `AGENTS.md` says.
- **No secrets** in arguments, `system.yaml` or the code.
- **Never delete** what this build did not create; a mart or a job is deleted by a change request the user makes in the Factory.

## The phases of a data mart

### requirements

Read the mart's `requirements` against the questions in references/gold-marts.md.
For `existing_tables`, list the gold tables of every mart in this layer and of other gold layers in the project (`hops fg list`, the `medallion_table` tag `layer: gold`), with their grain and columns, and propose reusing or extending them where they fit; otherwise propose new tables from the silver `sources`.
The Factory's form asks only for the layer, its questions, its silver tables and the mart's refresh, so most requirements start blank.
Draft each blank one from the layer's `description` and `queries`, the mart's description and the silver tables' columns: the grain (what one row is, what identifies it, the kind of fact), the metrics with their formulas, the example questions with the answers you expect, the totals to reconcile, the invariants, the late-data handling and the dashboards.
Show the drafts as a short table and ask the user to confirm or correct them, the grain and the metrics' formulas first; never record a business definition the user has not confirmed.
Write the answers back into `requirements`, so they can be edited in `system.yaml` later, and the user's approval of the definitions, with the `approver`, into `decisions`.

### design

Design the mart's tables in the layer's model: the facts at the declared grain with their measures (formula, unit, additivity), keys and event time; the dimensions with their surrogate keys, attributes and slowly changing type; shared dimensions reused and listed with `shared: true`.
Name each after the standards; no gold table takes the name of an existing feature group (check `hops fg list`).
Decide each fact's partitioning with **hops-partitioning**.
Group the tables into jobs, one per cadence the mart needs (`<slug>-<mart>-<cadence>`, usually just the mart's `cadence`; a dimension refreshed more slowly than its fact may get its own), cron from the cadence, catch-up on.
Write `tables` (`name`, `version: 1`, `kind: fact | dimension`, `grain`, `shared`, `columns`, `metrics`) and `jobs` (`name`, `cadence`, `cron`, `tables`) into the mart before any code, and show the design as a short table; ask only what is unclear.

### code

In `marts/<mart>/` of the layer: a dbt project (`sources.yml` over the silver tables, one model per table, tests for keys, not-null foreign keys, accepted values, the `invariants` and the `reconcile` checks), and one runner `run_mart.py --mart <mart> --cadence <cadence>` that passes `HOPS_START_TIME`/`HOPS_END_TIME` as dbt vars, runs `dbt build`, executes each compiled model on Trino and inserts the rows into its gold feature group (hops-dbt, Landing the model output).
On a failed check the runner does what `on_check_failure` says: `fail` exits non-zero before writing, `quarantine` writes the failing rows to `<table>_quarantine` (listed in the mart's `tables` with `kind: quarantine`) and the rest to the table, `warn` writes and logs the failures.
Late data, updates and deletes are processed as `late_data` says; with `restate: true` a correction rewrites the affected published periods, else only later ones.
Create each gold feature group with `get_or_create_feature_group` (Delta, offline, `statistics_config=False` so an insert from a Python job starts no Spark statistics job, the primary key from the grain, `event_time` on facts, `partition_key` as decided, `parents=` the silver feature groups it reads, a description with every metric's formula, a description per feature).
Write `marts/<mart>/README.md` with the tables, grain, metrics, owner and approver, as the standards say.
Unit-test the models on sample rows (DuckDB) and run the tests.

### backfill

Deploy the mart's jobs (`hops job deploy <slug>-<mart>-<cadence> marts/<mart>/run_mart.py --env dbt-pipeline --args "--mart <mart> --cadence <cadence>"`, uploading the dbt project with `hops files upload --overwrite`) and run each once without a window (`hops job run <job> --wait`), dimensions before facts.
Check each table: row count, no duplicate key at the grain, no null foreign key, and its parents (`hops fg lineage <name>`).
Then the mart's verification (references/gold-marts.md, Verification): turn each `example_queries` question into SQL over the mart, run it (`hops trino query`), and compare the answer with the expected one in plain English; run each `reconcile` check against its reference.
Record each check under the mart's `verification` in `system.yaml` as `{check, sql, expected, result, passed: true | false, at}`; ask the user when an expected answer is ambiguous or a mismatch may be a wrong expectation rather than a wrong mart.
Fix and rerun until the checks pass; record the counts and the reconciliation in the mart.

### schedule

Schedule each job with catch-up (`hops job schedule <job> "<cron>" --start-time <end of the backfill> --catchup --max-catchup-runs <48 hourly, 14 daily, 4 weekly>`) and, with a receiver configured (`hops alert receiver list`), a failure alert (`hops alert job create <job> --receiver <receiver> --status failed --severity critical`).
Tag every gold feature group `medallion_table` with `{"layer": "gold", "lifecycle": "<layer.lifecycle>"}`.
Apply `access` and `share`: share the feature groups with the projects named (`hops files share` of the feature store dataset, or ask how when the requirement names rows or columns rather than projects).

### verify

Run one window of each job (`hops job run <job> --start-time <t0> --end-time <t1> --wait`), and prove each `refresh_checks` check with evidence from the runs: row counts and table versions before and after a rerun of the same window, the rows a refresh wrote against the window, a late row's effect on its period.
Rerun the `example_queries` and `reconcile` checks after the refresh, record every result under `verification`, and run `hops factory system status <slug>`, fixing what it flags.
A mart with a failing check is not `built`: fix it, or ask the user, and do not start its dashboards.

### dashboards

Build the Superset dashboards the mart's `requirements.dashboards` describes, in the user's words; with it empty, set the phase `done` with the note "no dashboards asked for" and build nothing.
Load **hops-superset**. For each dashboard: ask with `AskUserQuestion` only what the text leaves open (which metric a chart shows, a filter's default), then write `marts/<mart>/dashboards/<name>.py`, which creates the dashboard or updates it in place and prints its URL, with datasets over the mart's gold tables through Trino, charts for the questions it answers and filters on the mart's dimensions; run it, open each chart's data (`hops superset chart info`), and check it is not empty and agrees with the mart's verified numbers.
Record each in the mart's `dashboards` as `{name, id, url, script, charts}`, and commit the scripts.
Set the mart's `applied` to its spec (`name`, `description`, `cadence`, `freshness_hours`, `requirements`), its `status: built`, and report its tables, jobs, schedule, checks and dashboards with their URLs, in a few lines.

## Changing a mart

A built mart whose spec differs from its `applied` has been edited, by an **Edit data mart** change request.
Read the difference and `git log -p -- system.yaml` for it, show what it recomputes, and run the affected phases: a changed `cadence` reschedules the jobs (renaming them to the new cadence); a changed metric, grain, filter or `late_data` changes the models, creates the next version of each changed table, backfills it and switches the job to it; a changed `access` or `share` is reapplied; a changed `dashboards` reruns the dashboards phase, updating the dashboards in place, building new ones and deleting with `hops superset dashboard delete` only those this build made that the text no longer asks for; `analysts`, `decisions` or `approver` only update the README.
Keep superseded versions and name them in `decisions`; end with `applied` set and one commit `[<slug>] <mart> apply: <what changed>`.

## apply (the layer)

Compare `layer.lifecycle`, `layer.modeling`, `standards` and `sources` with `outputs.applied_spec`.
A lifecycle change retags every gold feature group; a standards change is checked against every mart, which is changed where it breaks the new standard; a modeling change (star to snowflake, or back) redesigns the dimensions of every mart, as new versions; a removed source stops the marts that read it, after asking.
End with `outputs.applied_spec` set and one commit `[<slug>] apply: <what changed>`.

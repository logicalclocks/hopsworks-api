---
description: Hopsworks bronze layer builder. Builds the bronze analytics layer of generated data that system.yaml describes, from its generator program to offline Delta feature groups tagged bronze, backfilled once and written by scheduled jobs. Phases can be run alone.
argument-hint: "[<slug>] [code|backfill|schedule|verify]"
---

You are running `/hops-bronze` with arguments: `$ARGUMENTS`

Already known, no need to look again before the first question:
- Layers here (`system.yaml` alone: this directory is the layer): !`ls -d system.yaml */system.yaml 2>/dev/null | head -5 || true`
- UTC now: !`date -u +%FT%H:%MZ`
- Feature groups: !`hops fg list 2>&1 | head -40`
- Python environments: !`hops env list 2>&1 | head -20`

## Dispatch

| First word | Do |
| --- | --- |
| the slug of a layer above | work on that layer: carry out its pending change requests, then resume it from its first phase that is not `done` (the Factory and `hops factory run analytics-bronze` start the build this way) |
| none | with a `system.yaml` above: resume it from its first phase that is not `done`; without one: reply that the Factory's **From Template > Analytics > Blueprints**, or `hops factory run analytics-bronze --preset <example>`, record a layer first, and stop |
| `code`, `backfill`, `schedule`, `verify` | that phase, then every later phase that is not `done` |

Load **hops-analytics** first (the `analytics_table` tag and the layers built on bronze), and **hops-job** and **hops-fg** before deploying a job or checking a feature group.
Find a skill at `.claude/skills/<name>/` in the repository, else `~/.claude/skills/<name>/` (in a Hopsworks terminal that links to `/opt/hops/agent-skills/`).

**Where it runs.** The Factory starts Claude Code in the layer's directory, `<slug>/`, so it reads the layer's `AGENTS.md`; every `<slug>/<path>` here is `<path>` in the current directory.

## Rules

- **Change requests.** A `changes` entry with `status: pending` in `system.yaml` is a change the Factory recorded: before anything else, carry out each, oldest first, following its `instructions` with its `answers`, then set its `status: done` and `finished` (UTC), or `status: failed` with a `reason`, and commit `[<slug>] <label>`. Delete a job or a feature group only with `hops factory system delete-assets <slug> --job <name> --table <name>:<version>`.
- **system.yaml is the record.** Set `layer.status` to `building` when you start and `built` or `failed` when you end; set each phase's `status`, `started` and `finished` (UTC, from `date -u`) as it runs, and `progress.now` to one line on what you are doing. The Factory reads it every few seconds.
- **The generator is given.** `generator.program` was copied from the hops-analytics references and its tests are in `tests/`. Run it as it is; change it only to fix a failure, recording the fix and why in `decisions`.
- **Ask, never guess.** When a feature group the generator writes already exists and was not written by this layer, or the jobs' Python environment is missing, ask with `AskUserQuestion` (one call, up to four questions, two to four concrete options each with the recommended one first). Record every answer and every choice you make yourself in `decisions` (`at`, `by: user` or `by: claude`, `what`, `why`).
- **Commit each phase** in the layer's git work tree: `[<slug>] <phase>: <what>`, `system.yaml` with the code.
- **The repository.** An analytics pipeline's layers share one work tree, `layer.repo.name` (`hops-<prefix>`, the parent of this directory), and one repository; each layer is a directory in it. A `layer.repo.url` with a `layer.repo.provider` is an existing repository the Factory was given (**Create new GitHub repo** unchecked): when the work tree has no `origin`, add that URL as `origin` and, when it has commits, put the work tree's commits on its history as below; for `gitlab`, `bitbucket` or `git` use git alone (no `gh`; access is a token for that host in Hopsworks Account Settings, Git providers, or an SSH key it accepts, and when `git ls-remote origin` fails say which fixes it and keep committing locally). With no `layer.repo.url`, a new private GitHub repository: when the work tree already has an `origin`, record its URL; else when `gh repo view <layer.repo.name>` finds the repository, add it as `origin` and put the work tree's commits on its history (`git fetch origin` then `git rebase origin/<its default branch>`; the work tree was started fresh, and each layer's commits touch only its own directory); else, with `gh auth status` logged in, create it (`gh repo create <layer.repo.name> --private --source .. --remote origin --push`). Record the URL as `layer.repo.url`. Commit only this layer's directory (`git add -A -- .`, `git commit -m ... -- .`) and push after every commit. Without a GitHub login, say once that `github-login` connects one, and keep committing locally.
- **Logs stay out of the directory**, as `AGENTS.md` says.
- **Never delete** what this build did not create.

## The phases

### code

Run the generator's tests: `python -m pytest -q tests` (`uv pip install pytest` first when it is missing).
Check that none of the feature groups in `tables` exists yet (`hops fg list`); one that does and is not tagged `analytics_table` `layer: bronze` belongs to someone else: ask before going on, and never write into it.
Check that `generator.environment` is one of the project's Python environments; when it is not, ask which to use.
Create the GitHub repository, as the rules say, and commit.

### backfill

Deploy the backfill job and run it once, waiting for it: `hops job deploy <slug>-backfill <generator.program> --env <generator.environment> --args "<generator.backfill.args>" --run --wait`.
It creates the feature groups in `tables` (offline, Delta), writes the history up to the last midnight (UTC), adds each feature's description and tags each `analytics_table` `{"layer": "bronze", "lifecycle": "<layer.lifecycle>"}`.
A scheduled run appends `-start_time <fire instant>` to the program's arguments, which the program accepts and ignores (**hops-job**, Windows and backfill).
When it fails, read its log (`hops job logs <slug>-backfill --stdout --tail 200`, and `--stderr`), fix the cause, and run it again; a rerun rewrites the same rows.
Check each table: its rows against `tables[].rows` (`hops trino query --catalog delta --schema <project>_featurestore "SELECT count(*) FROM <table>_1"`, the project name lowercased), that it is offline and Delta (`hops fg info <name>`), and its tag (`hops fg tags <name>`).
Record each in `outputs.tables` (`name`, `version`, `cadence`, `primary_key`, `event_time`, `rows`), the job in `outputs.jobs` (`{name, environment, tables}`, no cadence), and the backfill's end in `decisions`.

### schedule

For each cadence in `schedule.cadences`, deploy its job: `hops job deploy <slug>-<cadence> <generator.program> --env <generator.environment> --args "<its args>"`.
Schedule it from the backfill's end, with catch-up, so the windows continue from the history: `hops job schedule <slug>-<cadence> "<its cron>" --start-time <the backfill's end> --catchup --max-catchup-runs <its max_catchup_runs>`.
The hourly job's catch-up then writes today's hours so far, one run each; the daily job first runs at the next midnight.
Record each in `outputs.jobs` (`{name, cadence, cron, environment, tables}`).

### verify

Wait for the first catch-up run of the most frequent cadence to finish (`hops job history <slug>-<cadence>`), then check from its log that it wrote its window and from the table that the rows of that window are there.
Run the daily job once by hand for the last full day only when no daily window has run yet and the user wants it now: it writes yesterday's changes, which the next scheduled run would also write, and a rerun of the same window rewrites the same rows.
Check the duplicate deliveries a silver layer will have to remove, for the clickstream example: `SELECT count(*) - count(DISTINCT click_id) FROM clickstream_clicks_1` (same catalog and schema) is about 0.001% of the clicks.
Run `hops factory system status <slug>` and fix anything it flags.
Set `outputs.applied_spec` to the spec just built (`generator`, `tables`, `schedule`, `layer.lifecycle`), set `layer.status: built`, and report the tables with their rows, the jobs with their schedules, and that the tables are ready for a silver layer (**From Template > Analytics > Silver layer**), in a few lines.

---
description: Hopsworks semantic layer builder. Builds the semantic layer blueprint that system.yaml describes, from its generator program to offline Delta feature groups, the dbt project registered and compiled as a semantic project whose metric totals reconcile with SQL over the tables, a chat app that turns questions into metric queries through an LLM, and the daily job. Phases can be run alone.
argument-hint: "[<slug>] [code|backfill|semantic|verify|app|schedule]"
---

You are running `/hops-semantic-layer` with arguments: `$ARGUMENTS`

Already known, no need to look again before the first question:
- Layers here (`system.yaml` alone: this directory is the layer): !`ls -d system.yaml */system.yaml 2>/dev/null | head -5 || true`
- UTC now: !`date -u +%FT%H:%MZ`
- Feature groups: !`hops fg list 2>&1 | head -40`
- Semantic projects: !`hops semantic list 2>&1 | head -20`
- Python environments: !`hops env list 2>&1 | head -20`

## Dispatch

| First word | Do |
| --- | --- |
| the slug of a layer above | work on that layer: carry out its pending change requests, then resume it from its first phase that is not `done` (the Factory and `hops factory run analytics-semantic` start the build this way) |
| none | with a `system.yaml` above: resume it from its first phase that is not `done`; without one: reply that the Factory's **From Template > Analytics > Blueprints**, or `hops factory run analytics-semantic --preset <example>`, record a semantic layer first, and stop |
| `code`, `backfill`, `semantic`, `verify`, `app`, `schedule` | that phase, then every later phase that is not `done` |

Load **hops-semantic** first (registering, compiling, testing and querying a semantic project), **hops-analytics** (the `analytics_table` tag), **hops-job** and **hops-fg** before deploying a job or checking a feature group, and **hops-app** before deploying the app.
Find a skill at `.claude/skills/<name>/` in the repository, else `~/.claude/skills/<name>/` (in a Hopsworks terminal that links to `/opt/hops/agent-skills/`).

**Where it runs.** The Factory starts Claude Code in the layer's directory, `<slug>/`, so it reads the layer's `AGENTS.md`; every `<slug>/<path>` here is `<path>` in the current directory.

## Rules

- **Change requests.** A `changes` entry with `status: pending` in `system.yaml` is a change the Factory recorded: before anything else, carry out each, oldest first, following its `instructions` with its `answers`, then set its `status: done` and `finished` (UTC), or `status: failed` with a `reason`, and commit `[<slug>] <label>`. Delete a job or a feature group only with `hops factory system delete-assets <slug> --job <name> --table <name>:<version>`, before removing it from `system.yaml`.
- **system.yaml is the record.** Set `layer.status` to `building` when you start and `built` or `failed` when you end; set each phase's `status`, `started` and `finished` (UTC, from `date -u`) as it runs, and `progress.now` to one line on what you are doing. The Factory reads it every few seconds.
- **The blueprint is given.** The generator, its tests, the dbt project and the app were copied from the hops-analytics references. Run them as they are; change one only to fix a failure, recording the fix and why in `decisions`.
- **Ask, never guess.** When a feature group the generator writes, the semantic project or the app already exists and was not made by this layer, or a Python environment is missing, ask with `AskUserQuestion` (one call, up to four questions, two to four concrete options each with the recommended one first). Record every answer and every choice you make yourself in `decisions` (`at`, `by: user` or `by: claude`, `what`, `why`).
- **Commit each phase** in the layer's git work tree: `[<slug>] <phase>: <what>`, `system.yaml` with the code.
- **The repository.** As `/hops-bronze` handles it: `layer.repo.url: local` keeps the code in Hopsworks only (commit, never push); a `layer.repo.url` with a `layer.repo.provider` is an existing repository to add as `origin`; with no `layer.repo.url`, a new private GitHub repository `layer.repo.name` (`gh repo view`, else `gh repo create <layer.repo.name> --private --source .. --remote origin --push` when `gh auth status` is logged in). Record the URL as `layer.repo.url`, commit only this directory and push after every commit; without a GitHub login, say once that `github-login` connects one, and keep committing locally.
- **Logs stay out of the directory**, as `AGENTS.md` says.
- **Never delete** what this build did not create.

## The phases

### code

Run the tests of the generator and of the app's translator: `python -m pytest -q tests` (`uv pip install pytest` first when it is missing).
Check that none of the feature groups in `tables` exists yet (`hops fg list`), and that neither the semantic project `semantic_project.name` (`hops semantic list`) nor the app `app.name` (`hops app list`) does; one that does and is not this layer's: ask before going on, and never write into it.
Check that `generator.environment` and `app.environment` are Python environments of the project; when one is not, ask which to use.
Create the repository, as the rules say, and commit.

### backfill

Deploy the backfill job and run it once, waiting for it: `hops job deploy <slug>-backfill <generator.program> --env <generator.environment> --args "<generator.backfill.args>" --run --wait`.
It creates the feature groups in `tables` (offline, Delta), writes the two years of history up to the last midnight (UTC), adds each feature's description and tags each `analytics_table` `{"layer": "gold", "lifecycle": "<layer.lifecycle>"}`.
A scheduled run appends `-start_time <fire instant>` to the program's arguments, which the program accepts and ignores (**hops-job**, Windows and backfill).
When it fails, read its log (`hops job logs <slug>-backfill --stdout --tail 200`, and `--stderr`), fix the cause, and run it again; a rerun rewrites the same rows.
Check each table's rows (`hops trino query --catalog delta --schema <project>_featurestore "SELECT count(*) FROM <table>_1"`, the project name lowercased) against the counts the job's log printed, and its tag (`hops fg tags <name>`).
Record each in `outputs.tables` (`name`, `version`, `cadence`, `primary_key`, `event_time`, `rows`), the job in `outputs.jobs` (`{name, environment, tables}`, no cadence), and the backfill's end in `decisions`.

### semantic

The cumulative and year-over-year metrics need the day time spine: unless `hops fg list` shows `time_spine_day`, write it with `hops semantic time-spine create --start <semantic_project.time_spine.start> --end <semantic_project.time_spine.end>`.
Register the dbt project in `semantic_project.path` as the semantic project `semantic_project.name`: `hops semantic create <name> --path "$(pwd)/<semantic_project.path>"`, then `hops semantic compile <name> --wait`.
An `INVALID` build lists each issue at its file and line: fix the definitions, record why in `decisions`, and compile again.
Then `hops semantic test <name>`, which runs every metric once on Trino and exits 1 on any failure.
Record `outputs.semantic_project` (`name`, `id` from `hops semantic show <name>`, the serving `build`, and the number of `metrics` from `hops semantic metrics <name>`).

### verify

For each metric in `reconcile`, query its total over all time (`hops semantic query <name> --metrics <metric> -o json`) and run its SQL on Trino (`hops trino query --catalog delta --schema <project>_featurestore "<sql>"`); the two must be equal, to the cent for money.
Record each in `outputs.reconciled` (`metric`, `semantic`, `sql`, `match`); a mismatch is a defect in the definitions or the generator: find it, fix it, and verify again.
Then run the saved queries (`hops semantic saved-queries <name>`, then `hops semantic query <name> --saved-query <saved query> --limit 5`) and two of the app's example questions by hand, as metric queries (`app/app.json`, `examples`), checking that each returns rows and that the year-over-year growth for the last full month is about 0.3, the generator's trend.

### app

The chat app's LLM is the account environment variables `LLM_URL`, `LLM_MODEL` and `LLM_API_KEY` of the member who starts it, which the Factory's form saved; never read or print their values.
Deploy the app from this directory, as **hops-app** says for a custom app: `hops app create <app.name> --path "$(pwd)/<app.path>/app.py" --app-kind custom --entrypoint-command "python app.py" --app-port 8080 --readiness-probe-path /health --environment <app.environment> --start`, adding `app-requirements.txt` to the environment only when **hops-app**'s reuse rule says the environment lacks them.
Wait for it to serve (`hops app info <app.name>`), then smoke-test it through `kubectl port-forward`: `/health`, `/api/catalog` lists the semantic project's metrics, and `/api/run` with `{"query": {"metrics": ["revenue"], "group_by": ["metric_time__month"]}}` returns rows.
`/api/catalog` also says whether the LLM is configured (`llm.configured`): when it is, `/api/ask` with one example question returns `"kind": "answer"`; when it is not, say that the app answers questions once the three variables are set in the account settings and the app is restarted, and go on, since its Build a query form works without them.
When it fails, read `hops app logs <app.name>`, fix the cause, and `hops app redeploy <app.name>`.
Record `outputs.app` (`name`, `url` from `hops app url <app.name>`).

### schedule

For each cadence in `schedule.cadences`, deploy its job: `hops job deploy <slug>-<cadence> <generator.program> --env <generator.environment> --args "<its args>"`.
Schedule it from the backfill's end, with catch-up, so the days continue from the history: `hops job schedule <slug>-<cadence> "<its cron>" --start-time <the backfill's end> --catchup --max-catchup-runs <its max_catchup_runs>`.
The catch-up writes the days since the backfill, one run each; wait for the first run (`hops job history <slug>-<cadence>`) and check from its log that it wrote its day.
Record each in `outputs.jobs` (`{name, cadence, cron, environment, tables}`).
Run `hops factory system status <slug>` and fix anything it flags.
Set `outputs.applied_spec` to the spec just built (`generator`, `tables`, `schedule`, `semantic_project`, `app`, `layer.lifecycle`), set `layer.status: built`, and report in a few lines: the tables with their rows, the semantic project with its metrics and the reconciled totals, the app's URL and whether its LLM is configured, and the jobs with their schedules.

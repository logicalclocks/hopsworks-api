---
name: hops-agent-task
description: Use when creating, running, or scheduling an agent task in Hopsworks - a coding agent (Claude Code, OpenAI Codex or GitHub Copilot) that executes a prompt as a Hopsworks job, once or on a cron schedule. Auto-invoke when the user wants an autonomous or scheduled AI task or workflow, an "agent job", a nightly report or maintenance sweep described in natural language, or asks about agentJobConfiguration, permission presets, agent refs, or where an agent's result lands. Input a prompt (plus permissions, refs, schedule); output an AGENT job whose executions write result.md.
---

# Hopsworks Agent Tasks

An agent task is a Hopsworks **job of type `AGENT`**: a coding agent (Claude
Code by default, OpenAI Codex, or GitHub Copilot) runs your **prompt** once, non-interactively,
in a pod on the cluster with the `hops` CLI, the Python SDK and the project's
HopsFS at hand, writes its result to HopsFS and exits. It is a *background*
agent for routine, well-specified work (a nightly data-quality report, table
maintenance, a summary posted to Slack), on demand or on a schedule. In the UI it
is **Agents → Agent Tasks**. For an interactive, always-on agent behind an
endpoint use **hops-agent-deployment**.

Prefer a deterministic prompt (a fixed checklist with a fixed deliverable) over
an open-ended one: cheaper, more reliable, and reviewable. The agent cannot ask
questions at run time (`claude -p` / `codex exec` / `copilot -p`), so the prompt must state the
inputs, the steps, the definition of done, and where to write the result.

## Contract
- **Input:** a prompt, plus optional provider/model, permissions, Hopsworks resource refs, a skills file, env vars, resources and a cron schedule.
- **Output:** an `AGENT` job; each execution writes `result.md` and `metadata.json` to `Resources/jobs/<job>/<execution id>/` and its stdout/stderr to the Logs dataset.
- **Pre-condition:** `agent_jobs_enabled` is true on the cluster (the create is rejected with 400 otherwise), and the user has an AI-provider key on the platform (see Authentication).

## Smoke-test (cheap pre/post-flight)
```bash
hops job list                               # AGENT jobs are listed with the other jobs
hops job info <name>
hops job history <name>                     # executions and their states
hops job logs <name> --stdout --tail 200    # the last execution's result (stdout = result.md)
```

## Ask the user (only when state is ambiguous)
- **The deliverable.** A report in `result.md`, rows written to a feature group, a Slack post? The prompt has to say where the result goes.
- **Provider and model.** `claude` (default; model from the cluster's `agent_default_model`), `codex` (`gpt-*` models) or `copilot` (copilot's own ids such as `claude-sonnet-4.5`, or `auto`). Leave `model` empty for codex and copilot to take their own default.
- **Permissions.** Start narrow: `READ_ONLY` for reporting, `OPERATOR` when it must run `hops`/`python` and write under `/hopsfs`. Custom lists only when a preset does not fit.
- **Once or scheduled**, and the cron.
- **Context.** Which feature groups, feature views, models, deployments or jobs it should know about (`refs`).
- **Before deleting.** `hops job delete <name> --yes` removes the job and every execution's record; confirm the exact name.

## Authentication (check this first)
The pod authenticates with the user's AI-provider key, injected as an env var:
`ANTHROPIC_API_KEY` for `claude`, `OPENAI_API_KEY` for `codex`, `GH_TOKEN` for
`copilot`. Keys are stored once per user on the platform (`POST /users/ai/provider`
with `{"providerType": "ANTHROPIC" | "OPENAI" | "GITHUB", "apiKey": "..."}`; `GET`
lists them), and every stored key is injected regardless of provider. Copilot
takes an OAuth token or a **fine-grained** personal access token and refuses a
classic `ghp_` PAT outright. A login made in the terminal (`claude`, `codex`, or
`copilot` then `/login`) is reused by the pod too, so a user who has signed in
there needs no stored key. A per-job
`envVars: ["ANTHROPIC_API_KEY=..."]` overrides the account value (env precedence:
per-job `envVars` > account env vars > AI-provider secrets). A run that fails at
once with an authentication error in `stderr.log` means no key reached the pod.

## Create the task (SDK)

`hops job create` / `hops job deploy` cannot create agent tasks (their `--type`
has no `AGENT` and they require a script). Create it from Python, then operate it
with the `hops job` commands below.

```python
import hopsworks

project = hopsworks.login()
jobs = project.get_job_api()

config = jobs.get_configuration("AGENT")     # server defaults: provider, model, maxTurns
config.update({
    "appName": "fg-quality-report",
    "prompt": (
        "For every feature group in this project, run `hops fg stats <name> --version <v>` "
        "and list the features with more than 5% nulls. Write the findings as a markdown "
        "table to $AGENT_OUTPUT_PATH/result.md. Do not modify any data."
    ),
    "permissionPreset": "READ_ONLY",
    "refs": [{"type": "feature_group", "name": "transactions", "version": 1}],
    "maxTurns": 25,
    "maxBudgetUsd": 5.0,
    "resourceConfig": {"cores": 1, "memory": 2048, "gpus": 0},
})
job = jobs.create_job("fg-quality-report", config)   # PUT /project/{id}/jobs/{name}
execution = job.run(await_termination=True)          # run once now
```

An SDK older than the one carrying `agentJobConfiguration` in
`_validate_job_conf` rejects the config with `'appPath' not set in job
configuration`; send the same dict yourself in that case:

```python
import json
from hopsworks_common import client

_c = client._get_instance()
_c._send_request("PUT", ["project", _c._project_id, "jobs", config["appName"]],
                 headers={"content-type": "application/json"}, data=json.dumps(config))
```

The UI form (**Agents → Agent Tasks → Create Agent Task**) has the same fields.

### Configuration fields

| Field | Notes |
|---|---|
| `appName`, `prompt` | required; the prompt is the whole task specification |
| `provider` | `claude` (default), `codex` or `copilot` |
| `model` | `claude-*` for claude, `gpt-*` for codex, copilot's own ids (`claude-sonnet-4.5`, `auto`); the `agent_default_model` default applies to claude only |
| `maxTurns` | claude only; default from `agent_default_max_turns` (50) |
| `maxBudgetUsd` | claude only; max 100; the agent stops when spent |
| `permissionPreset` / `permissions` | a preset name, or a custom list of `Bash(...)`, `Read(...)`, `Write(...)` patterns; a custom list overrides the preset |
| `refs` | `[{"type": feature_group\|feature_view\|model\|deployment\|job, "name", "version"}]`; version required for FG/FV/model. Each becomes a JSON file under `/context/` (e.g. `/context/fg_transactions_v1.json`) and is listed in the agent's instructions |
| `skillsRef` | project-relative `.md` (e.g. `Resources/agent-skills.md`) appended to the agent's instructions |
| `hooks` | claude only: `[{"event": PreToolUse\|PostToolUse\|Stop, "matcher": "Bash(hops *)", "command": "..."}]` |
| `cliArgs` | codex and copilot: extra flags appended after the provider's defaults (codex: `--search --model gpt-5`; copilot: `--deny-tool=shell(rm:*)`, and a copilot `--deny-tool` beats every allow) |
| `environmentName` | `agent-task` (default) or a clone of it; anything else is rejected with 400 |
| `envVars` | `["KEY=VALUE", ...]`; names with the `HOPS_`, `HOPSWORKS_`, `HOPSFS_`, `AGENT_` prefixes are reserved (400) |
| `resourceConfig` | `{"cores", "memory" (MB), "gpus"}` |
| `outputPath` | overrides the result directory (default `Resources/jobs/<job>/<execution id>`) |

### Permission presets

| Preset | Allows |
|---|---|
| `READ_ONLY` | `Bash(hops fg info *)`, `Bash(hops fg preview *)`, `Bash(hops fv info *)`, `Bash(hops model info *)`, `Read(*)` |
| `OPERATOR` | `Bash(hops *)`, `Bash(python *)`, `Read(*)`, `Write(/hopsfs/*)` |
| `FULL` | `OPERATOR` plus `pip ls cat grep find curl wget echo mkdir cp mv`; writes still scoped to `/hopsfs/*` |

Note that `READ_ONLY` does not include `hops fg stats` or `hops fg list`; a
reporting task that needs them takes a custom list such as
`["Bash(hops fg *)", "Bash(hops fv *)", "Read(*)"]`. `Bash(*)` is unrestricted;
use it only when the user asks. For `copilot` the same list is translated to its
grammar (`Bash(hops fg *)` → `--allow-tool=shell(hops fg:*)`, `Write(...)` →
`--allow-tool=write`, `Read(*)` dropped since copilot does not gate reads), so
write the list in the claude form whatever the provider.

## Run, override, schedule, stop (CLI)

```bash
hops job run <name>                                   # the configured prompt
hops job run <name> --args "Only check the transactions feature group today"   # prompt override for this run only
hops job schedule <name> "0 0 6 * * ?"                # 6-field Quartz cron (@daily also accepted)
hops job schedule-info <name> / hops job unschedule <name>
hops job stop <name>                                  # stop the running execution
hops job logs <name> --stdout --tail 200
hops job delete <name> --yes
```

`job.run(args="...")` in Python does the same override. Schedules, alerts and
executions are the ordinary job mechanics (**hops-job**); an alert with
`passToAgent: true` on a Slack receiver injects `SLACK_WEBHOOK_URL` and
`SLACK_CHANNEL` into the pod and the entrypoint posts `result.md` to Slack when
the run ends.

## Where the result lands

- `Resources/jobs/<job>/<execution id>/result.md` — the agent's final answer (its stdout); also copied to `Logs/<job>/<execution id>/stdout.log`, which is what `hops job logs --stdout` prints.
- `Resources/jobs/<job>/<execution id>/metadata.json` — `{"exit_code", "completed_at"}`.
- `Logs/<job>/<execution id>/stderr.log` — the CLI's diagnostics; read this when the state is `FAILED`.
- The pod's instructions already tell the agent to write to `$AGENT_OUTPUT_PATH/result.md`; repeat it in the prompt when the deliverable is anything else (a feature group, a file elsewhere).

Execution states: `INITIALIZING` → `DEPLOYING`/`QUEUED` → `RUNNING` → `FINISHED` | `FAILED` | `KILLED`.

## Environment

Tasks run in the `agent-task` environment: the Claude Code and Codex CLIs plus
the Python feature-engineering libraries. To add libraries, clone `agent-task`
and pass the clone as `environmentName` (**hops-environments**). The user's home
carries a scaffolded `~/.claude/CLAUDE.md` and `~/.codex/AGENTS.md` that list
the Hopsworks skills, so the agent in the pod can load `hops-fg`, `hops-fv` and
the others the same way you do.

## Next Steps

- Job mechanics shared with every job type (schedules, alerts, executions): **hops-job**.
- An interactive, served agent instead of a scheduled one: **hops-agent-deployment**.
- Extra libraries in the pod: **hops-environments** (clone `agent-task`).
- A recurring table-layout report is a natural agent task: **hops-table-maintenance**.

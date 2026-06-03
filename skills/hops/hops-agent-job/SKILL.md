---
name: hops-agent-job
description: Use when creating, configuring, scheduling, or running claude-code or codex jobs/workflows. 
---

# Creating Agent Jobs

An agent **job** is a set of instructions and a coding agent (claude code or codex) to execute them. An agent job can be run on a schedule or on-demand. It is a *background agent*: it runs autonomously to automate routine tasks (workflow execution, maintenance) rather than serving interactive queries. Like any Hopsworks job, it is automatically containerized and runs in a project environment.

Prefer a deterministic LLM workflow over a fully autonomous agent when the task is predictable. Reach for an agent only when the task is open-ended and not well-defined in advance.

## Contract
- **Input:** an entry/instructions script living in HopsFS (under `/hopsfs/...`).
- **Output:** a created/scheduled coding-agent job (claude-code / codex), runnable on-demand or on a cron schedule.

## Smoke-test (cheap pre/post-flight)

```bash
hops job list            # confirm state before; verify the job exists after
```

Two equivalent interfaces:
- `hops job ...` CLI — preferred for one-off creation and scripted operations
- `project.get_job_api()` Python SDK — preferred from inside a program / notebook / pipeline script that creates and runs the agent job

## Prerequisites

1. The script must live in HopsFS (i.e. under `/hopsfs/...` which maps to `/Projects/<project>/...`). User notebooks/scripts in `/hopsfs/Users/<name>/...` are already there. Local files on your laptop must be uploaded first — use `project.get_dataset_api().upload(...)`.
2. The `appPath` used in the job config is **project-relative**, NOT absolute. Example: a script at `/hopsfs/Users/meb10000/analytics/foo.py` in project `af` is `Users/meb10000/analytics/foo.py`.

## CLI — quick path

An agent job is created and operated like any Hopsworks job (see **hops-job** for
the full `hops job` surface). Upload the instructions/entry, create the job, then
run once or schedule it:

```bash
# upload the agent's entry/instructions, then create + run
hops job deploy my-agent-job entry.py --type PYTHON --run --wait
# or schedule it (6-field Quartz cron; @daily shorthand also accepted)
hops job schedule my-agent-job "0 0 * * * ?"
hops job logs my-agent-job          # see the agent's run output
```

The coding-agent runtime (claude-code / codex) is selected by the job's
environment/entry as configured on the platform — confirm the exact entry/image
convention for your cluster rather than assuming flags.

## When to reach for which interface

- Quick one-off creation / ops from a shell → `hops job`.
- Creating a job as part of a Python pipeline that also builds the artefact (model register, FG materialisation trigger) → `project.get_job_api()` in the same script.

## Next Steps

- General job mechanics (create/run/schedule/logs): **hops-job**.
- An interactive (served) agent instead of a scheduled one: **hops-agent-deployment**.

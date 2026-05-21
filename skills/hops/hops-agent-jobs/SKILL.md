---
name: hops-agent-jobs
description: Use when creating, configuring, scheduling, or running claude-code or codex jobs/workflows. 
---

# Creating Agent Jobs

An agent **job** is a set of instructions and a coding agent (claude code or codex) to execute them. An agent job can be run on a schedule or on-demand. 

Two equivalent interfaces:
- `hops job ...` CLI — preferred for one-off creation and scripted operations
- `project.get_job_api()` Python SDK — preferred from inside a program / notebook / pipeline script that creates and runs the agent job

## Prerequisites

1. The script must live in HopsFS (i.e. under `/hopsfs/...` which maps to `/Projects/<project>/...`). User notebooks/scripts in `/hopsfs/Users/<name>/...` are already there. Local files on your laptop must be uploaded first — use `project.get_dataset_api().upload(...)`.
2. The `appPath` used in the job config is **project-relative**, NOT absolute. Example: a script at `/hopsfs/Users/meb10000/analytics/foo.py` in project `af` is `Users/meb10000/analytics/foo.py`.

## CLI — quick path

## When to reach for which interface

- Quick one-off creation / ops from a shell → `hops job`.
- Creating a job as part of a Python pipeline that also builds the artefact (model register, FG materialisation trigger) → `project.get_job_api()` in the same script.

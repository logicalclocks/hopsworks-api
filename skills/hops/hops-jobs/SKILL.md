---
name: hops-jobs
description: Use when creating, configuring, scheduling, or running Hopsworks jobs. Auto-invoke when the user wants to run a Python/PySpark/Spark/Docker/Flink script as a Hopsworks job, schedule recurring batch workloads, set resources (cores/memory/GPUs), attach cron schedules, or operate on executions (run, logs, status, history).
---

# Creating Hopsworks Jobs

A Hopsworks **job** runs a script (Python, PySpark, Spark JAR, Flink, or Docker) inside the project on a schedule or on-demand. Jobs are the standard way to operationalise feature pipelines, training, batch inference, and housekeeping.

Two equivalent interfaces:
- `hops job ...` CLI — preferred for one-off creation and scripted operations
- `project.get_job_api()` Python SDK — preferred from inside a notebook / pipeline script that also creates the artefact

## Prerequisites

1. The script must live in HopsFS (i.e. under `/hopsfs/...` which maps to `/Projects/<project>/...`). User notebooks/scripts in `/hopsfs/Users/<name>/...` are already there. Local files on your laptop must be uploaded first — use `project.get_dataset_api().upload(...)` or drop into the project's Resources/Jupyter folder via the UI.
2. The `appPath` used in the job config is **project-relative**, NOT absolute. Example: a script at `/hopsfs/Users/meb10000/analytics/foo.py` in project `af` is `Users/meb10000/analytics/foo.py`.
3. Pick a job type: `PYTHON` (single-container Python), `PYSPARK`, `SPARK` (JAR), `PYTHON_APP` (long-running Streamlit/Gradio), `DOCKER`, `FLINK`.

## CLI — quick path

```bash
# Python job, 1 core, 2 GB memory
hops job create my_job \
  --type python \
  --app-path "Users/me/analytics/script.py" \
  --cores 1 --memory 2048

# PySpark job with executor config
hops job create my_spark_job \
  --type pyspark \
  --app-path "Resources/jobs/etl.py" \
  --driver-mem 2048 --driver-cores 1 \
  --executor-mem 4096 --executor-cores 2 --executors 4

# Run (synchronous)
hops job run my_job --wait

# Run with args
hops job run my_job --args "--date 2026-04-22"

# Schedule — Quartz 6-field cron: SEC MIN HOUR DAY MONTH WEEKDAY
hops job schedule my_job "0 0 4 * * ?"          # daily 04:00 UTC
hops job schedule my_job "0 */15 * * * ?"       # every 15 min
hops job schedule my_job "0 0 8 ? * MON-FRI"    # weekdays 08:00

# Inspect / operate
hops job info my_job
hops job status my_job --wait --poll 5
hops job logs my_job --type out
hops job history my_job --limit 10
hops job stop my_job
hops job unschedule my_job
hops job delete my_job
```

**Python resources**: `--cores`, `--memory` (MB), `--gpus`.
**Spark resources**: `--driver-mem`, `--driver-cores`, `--executor-mem`, `--executor-cores`, `--executors`, `--dynamic`.
**Ray resources**: `--worker-mem`, `--worker-cores`, `--workers-min`, `--workers-max`.

## Python SDK — programmatic path

```python
import hopsworks
from datetime import datetime, timezone

project = hopsworks.login()
job_api = project.get_job_api()

# 1. Fetch the default config for the job type — edit in place
config = job_api.get_configuration("PYTHON")   # or "PYSPARK", "SPARK", "PYTHON_APP", "DOCKER", "FLINK"
config["appPath"] = "Users/me/analytics/script.py"
config["resourceConfig"]["cores"] = 1
config["resourceConfig"]["memory"] = 2048      # MB
config["resourceConfig"]["gpus"] = 0
# config["defaultArgs"] = "--date 2026-04-22"  # optional default args
# config["environmentName"] = "pandas-training-pipeline"  # optional env

# 2. Create (or update — PUT is idempotent on name)
job = job_api.create_job("my_job", config)

# 3. Run — await_termination=True blocks until the execution finishes
execution = job.run(args="--date 2026-04-22", await_termination=True)
print(execution.success, execution.final_status)
out_log, err_log = execution.download_logs()

# 4. Schedule (Quartz 6-field cron, UTC)
job.schedule(
    cron_expression="0 0 4 * * ?",          # daily 04:00
    start_time=datetime.now(tz=timezone.utc),
    # end_time=... optional
)
print(job.job_schedule.next_execution_date_time)

# Update: mutate config then save
job.config["resourceConfig"]["memory"] = 4096
job.save()

# Pause / resume / remove the schedule
job.pause_schedule()
job.resume_schedule()
job.unschedule()

# Cleanup
job.delete()
```

## Cron cheat sheet (Quartz 6-field)

Format: `SEC MIN HOUR DAY MONTH WEEKDAY`. Use `?` for "unspecified" — Quartz requires exactly one of `DAY` / `WEEKDAY` to be `?`.

| When                      | Expression            |
| ------------------------- | --------------------- |
| Every 15 min              | `0 */15 * * * ?`      |
| Every hour on the hour    | `0 0 * * * ?`         |
| Daily at 04:00 UTC        | `0 0 4 * * ?`         |
| Weekdays at 08:00         | `0 0 8 ? * MON-FRI`   |
| 1st of month at 00:00     | `0 0 0 1 * ?`         |
| Every Sunday at 23:30     | `0 30 23 ? * SUN`     |

All schedule times are **UTC**.

## Gotchas

- **`appPath` is project-relative.** Do not pass `/Projects/<project>/...` or `/hopsfs/...`.
- **Memory is in MB**, not GB. `2 GB → 2048`.
- **Job names are unique per project.** `create_job` with an existing name updates that job (PUT semantics).
- **`PYTHON_APP`** jobs (Streamlit, Gradio, FastAPI) run indefinitely; `run()` waits for RUNNING state, not completion, and returns an `app_url` on the execution.
- **Environment**: jobs execute in a Python env (default `pandas-training-pipeline`). If your script imports non-standard packages, set `config["environmentName"]` to a custom env you've built in the project.
- **Logs**: `job logs <name>` shows the latest execution by default. Pass `--exec <id>` for a specific run. `--type err` for stderr.
- **Ingestion jobs** (`oracle_to_*_fg_ingestion`) are auto-created when you register a feature group from a storage connector — don't recreate them manually; `hops job run <name>` them.

## When to reach for which interface

- Quick one-off creation / ops from a shell → `hops job`.
- Creating a job as part of a Python pipeline that also builds the artefact (model register, FG materialisation trigger) → `project.get_job_api()` in the same script.
- Exposing a Streamlit/Gradio UI → `PYTHON_APP` via SDK, read `execution.app_url` to surface the link.

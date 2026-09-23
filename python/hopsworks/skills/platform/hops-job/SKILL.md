---
name: hops-job
description: Use when creating, configuring, scheduling, or running Hopsworks jobs or Airflow jobs/DAGs/workflows. Input a script in HopsFS plus job config; output a created/scheduled job and its executions.
---

# Creating Hopsworks Jobs

Run a HopsFS-resident Python/PySpark script as a Hopsworks job — created, scheduled, executed, and (optionally) chained via Airflow.

A Hopsworks job is a **job orchestrator**: it schedules and runs a single program (one FTI pipeline — feature, training, or batch-inference). For a DAG of dependent programs you need a **workflow orchestrator** (Airflow, below). A single job is usually enough; reach for Airflow only when one program must run after another or fire on an event.

## Contract
- **Input:** a script in HopsFS + job config (name, environment, schedule).
- **Output:** a created/scheduled job + executions.
- **Pre-condition:** the script is uploaded to HopsFS (`hops job deploy` uploads a local script for you).

## Smoke-test (cheap pre/post-flight)
```bash
hops job list            # confirm state before; verify the job exists after
hops job info <name>
```

Two equivalent interfaces:
- `hops job ...` CLI — preferred for one-off creation and scripted operations as jobs.
- `project.get_job_api()` Python SDK — preferred from inside a program / notebook / pipeline script that creates and runs the job

## Creating a job, with its environment

Hopsworks does **automatic containerization**: you pick a base environment and it
builds/reuses the container behind the scenes — no Dockerfile to write. One
customized environment can back many jobs.

`hops job deploy` is the one-shot: it uploads a local script, sets the Python
environment, schedules, and runs — everything `create` + `schedule` + `run` do
separately, plus the environment selection `create` cannot do.

```bash
# Positional args are NAME then SCRIPT (a local script is uploaded for you).
# --overwrite replaces the uploaded script so a re-run does not error on the
# existing file; without it a second deploy of the same name fails.
hops job deploy feature-pipeline feature_pipeline.py \
  --env python-feature-pipeline --cron @daily --run --wait --overwrite
```

Key fact: `hops job create` (and a bare job config) **cannot set the Python
environment** — a job created without one silently takes the job type's default.
Set it with `hops job deploy --env`, or, from a program, via the SDK:

```python
api = project.get_job_api()
config = api.get_configuration("PYTHON")
config["appPath"] = "/Projects/<proj>/Resources/jobs/<name>/feature_pipeline.py"
config["environmentName"] = "python-feature-pipeline"
job = api.create_job(name="feature-pipeline", config=config)
job.run(await_termination=True)
```

Pick the environment for the job's role: `python-feature-pipeline` (feature
pipelines), `pandas-training-pipeline` (training). Inference environments (e.g.
`pandas-inference-pipeline`) are deployment-only and cannot run as jobs.

## Windows and backfill

A scheduled program processes one data window per fire. The scheduler sets
`HOPS_START_TIME` and `HOPS_END_TIME` (ISO-8601 with a trailing `Z`) on every
execution; by default the window is the previous fire to this one. Move it with
offsets when the window is not the interval, for example a month closed on the
1st and scored on the 4th:

```bash
hops job schedule telco-churn-inference "0 0 4 1 * ?" \
  --start-offset-seconds -2678400 --end-offset-seconds 0      # negative looks back from the fire
hops job schedule-info telco-churn-inference                  # verify cron, offsets, next fire
```

The **same program** serves history: `hops job backfill` runs it once over a past
interval with the same two variables set, so there is one code path.

```bash
hops job backfill telco-churn-features --start-time 2025-01-01 --end-time 2026-09-01 --wait
```

`--catchup` with `--max-catchup-runs` replays fires missed during an outage
instead of skipping them; `--max-active-runs 1` (the default) keeps a slow run
from overlapping the next. Write the program so a replayed or retried window is
an upsert on the sink's primary key and event time, never a duplicate.

## Continuous jobs

A 24x7 program consuming a stream is a PySpark Structured Streaming job in
`spark-feature-pipeline`: it reads a Kafka connector or an online-enabled feature
group's topic, applies the model-independent transformations, and writes with
`fg.insert_stream(...)`, checkpointing under `Resources/<slug>/checkpoints/<job>`
so a restart resumes where it stopped.

```python
query = fg.insert_stream(
    features_df,
    query_name="usage_stream",
    output_mode="append",
    await_termination=True,
    checkpoint_dir="Resources/telco-churn/checkpoints/usage-stream",
)
```

```bash
hops job deploy telco-churn-usage-stream usage_stream.py --type pyspark --env spark-feature-pipeline --overwrite
hops job run telco-churn-usage-stream          # never scheduled: an execution that stays up
hops job history telco-churn-usage-stream      # the check is a RUNNING execution
hops job stop telco-churn-usage-stream
```

Hopsworks jobs have no restart policy. Whatever checks the system (`/hops verify`,
`/hops status`) looks for a running execution and, when there is none, reports it
and starts it again with `hops job run`; the failure alert covers the time in
between. A continuous job holds a driver and its executors for as long as it
runs, so count it against the system's `budget.operations.streams`.

## Alerts

Every job a system owns gets a failure alert, so a failed run reaches a person
without anyone watching a terminal:

```bash
hops alert receiver list
hops alert receiver create ml-oncall --email oncall@acme.example --slack "#ml-alerts"
hops alert job create telco-churn-features --receiver ml-oncall --status failed --severity critical
hops alert job create telco-churn-train --receiver ml-oncall --status long_running --severity warning
hops alert job list telco-churn-features
```

## Orchestrating Hopsworks Jobs with Airflow

Airflow is the **workflow orchestrator**: it runs a DAG of jobs (tasks) with dependencies between them. Use it when you want to chain Hopsworks jobs together — e.g. derived-feature pipelines that run only after their upstream parents succeed — or trigger a job in response to an event like a file landing in HopsFS. One DAG to monitor beats five separate jobs. For a single pipeline, a plain scheduled job is enough.

A minimal DAG that runs one Hopsworks job (`HopsworksJobSuccessSensor` / `HopsworksHdfsSensor` from `hopsworks.airflow.sensors` add the wait-for-job and file-landed triggers):

```python
from datetime import datetime
from airflow import DAG
from hopsworks.airflow.operators import HopsworksLaunchOperator

# Project-scoped Hopsworks API key. The operators / sensors below pick this up
# via the same env-var fallback that jobs / notebooks / terminal use. Re-key
# the DAG (regenerate from the UI) to rotate.

with DAG(
    dag_id="p_<project>_<id>__feature_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hopsworks", "p:<project id>"],
) as dag:
    feature_pipeline = HopsworksLaunchOperator(
        task_id="feature_pipeline",
        project_id=<project id>,
        job_name="feature-pipeline",
        args="",
    )
```

---

## Next Steps

- What goes in the script: **hops-features** (feature pipeline), **hops-train** (training), **hops-batch-inference** (scoring).
- Custom libraries for the job: [hops-environments](../hops-environments/SKILL.md) — clone a base env and install requirements.
- Inspect runs: `hops job list`, `hops job info <name>`, `hops job logs <name>`, `hops job history <name>`.

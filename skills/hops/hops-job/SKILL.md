---
name: hops-job
description: Use when creating, configuring, scheduling, or running Hopsworks jobs or Airflow jobs/DAGs/workflows. Input a script in HopsFS plus job config; output a created/scheduled job and its executions.
---

# Creating Hopsworks Jobs

Run a HopsFS-resident Python/PySpark script as a Hopsworks job — created, scheduled, executed, and (optionally) chained via Airflow.

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

`hops job deploy` is the one-shot: it uploads a local script, sets the Python
environment, schedules, and runs — everything `create` + `schedule` + `run` do
separately, plus the environment selection `create` cannot do.

```bash
# Positional args are NAME then SCRIPT (a local script is uploaded for you).
hops job deploy feature-pipeline feature_pipeline.py \
  --env python-feature-pipeline --cron @daily --run --wait
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

# Orchestrating Hopsworks Jobs with Airflow
- When you want to chain Hopsworks jobs together or have a job triggered in response to an event like file landing in HopsFS, then write an Airflow DAG.

Here is an example of an airflow program that runs a Hopsworks Job:

```python
import os
from datetime import datetime
from airflow import DAG
from hopsworks.airflow.operators import HopsworksLaunchOperator
from hopsworks.airflow.sensors import HopsworksJobSuccessSensor, HopsworksHdfsSensor

# Project-scoped Hopsworks API key. The operators / sensors below pick this up
# via the same env-var fallback that jobs / notebooks / terminal use. Re-key
# the DAG (regenerate from the UI) to rotate.

with DAG(
    dag_id="p_jim_119__filesensor",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hopsworks", "p:119", "p_slug:jim"],
) as dag:
    hello_0 = HopsworksLaunchOperator(
        task_id="hello_0",
        project_id=119,
        job_name="hello",
        args="",
    )
```

---

## Next Steps

- What goes in the script: **hops-features** (feature pipeline), **hops-train** (training), **hops-batch-inference** (scoring).
- Inspect runs: `hops job list`, `hops job info <name>`, `hops job logs <name>`, `hops job history <name>`.

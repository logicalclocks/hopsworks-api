---
name: hops-job
description: Use when creating, configuring, scheduling, or running Hopsworks jobs or Airflow jobs/DAGs/workflows.
---

# Creating Hopsworks Jobs


Two equivalent interfaces:
- `hops job ...` CLI — preferred for one-off creation and scripted operations as jobs.
- `project.get_job_api()` Python SDK — preferred from inside a program / notebook / pipeline script that creates and runs the job

# Orchestrating Hopsworks Jobs with Airflow
- When you want to chain Hopsworks jobs together or have a job triggered in response to an event like file landing in HopsFS, then write an Airflow DAG.

Here is an example of an airflow program that runs a Hopsworks Job:

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

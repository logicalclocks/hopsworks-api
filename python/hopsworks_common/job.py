#
#   Copyright 2022 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from __future__ import annotations

import json
import warnings
from datetime import datetime, timezone
from typing import List, Literal, Optional

import humps
from hopsworks_common import alert, client, usage, util
from hopsworks_common.client.exceptions import JobException
from hopsworks_common.core import alerts_api, execution_api, job_api
from hopsworks_common.engine import execution_engine
from hopsworks_common.job_schedule import JobSchedule


class Job:
    NOT_FOUND_ERROR_CODE = 130009

    def __init__(
        self,
        id,
        name,
        creation_time,
        config,
        job_type,
        creator,
        executions=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        job_schedule=None,
        **kwargs,
    ):
        self._id = id
        self._name = name
        self._creation_time = creation_time
        self._config = config
        self._job_type = job_type
        self._creator = creator
        self._executions = executions
        self._href = href
        self._job_schedule = (
            JobSchedule.from_response_json(job_schedule)
            if job_schedule
            else job_schedule
        )

        self._execution_engine = execution_engine.ExecutionEngine()
        self._execution_api = execution_api.ExecutionApi()
        self._execution_engine = execution_engine.ExecutionEngine()
        self._job_api = job_api.JobApi()
        self._alerts_api = alerts_api.AlertsApi()

    @classmethod
    def from_response_json(cls, json_dict):
        if "items" in json_dict:
            jobs = []
            for job in json_dict["items"]:
                # Job config should not be decamelized when updated
                config = job.pop("config")
                json_decamelized = humps.decamelize(job)
                json_decamelized["config"] = config
                jobs.append(cls(**json_decamelized))
            return jobs
        elif "id" not in json_dict:
            return []
        else:
            # Job config should not be decamelized when updated
            config = json_dict.pop("config")
            json_decamelized = humps.decamelize(json_dict)
            json_decamelized["config"] = config
            return cls(**json_decamelized)

    @property
    def id(self):
        """Id of the job"""
        return self._id

    @property
    def name(self):
        """Name of the job"""
        return self._name

    @property
    def creation_time(self):
        """Date of creation for the job"""
        return self._creation_time

    @property
    def config(self):
        """Configuration for the job"""
        return self._config

    @config.setter
    def config(self, config: dict):
        """Update configuration for the job"""
        self._config = config

    @property
    def job_type(self):
        """Type of the job"""
        return self._job_type

    @property
    def creator(self):
        """Creator of the job"""
        return self._creator

    @property
    def job_schedule(self):
        """Return the Job schedule"""
        return self._job_schedule

    @property
    def executions(self):
        return self._executions

    @property
    def href(self):
        return self._href

    @property
    def config(self):
        """Configuration for the job"""
        return self._config

    @usage.method_logger
    def run(self, args: str = None, await_termination: bool = True):
        """Run the job.

        Run the job, by default awaiting its completion, with the option of passing runtime arguments.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instances
            fg = fs.get_or_create_feature_group(...)

            # insert in to feature group
            job, _ = fg.insert(df, write_options={"start_offline_materialization": False})

            # run job
            execution = job.run()

            # True if job executed successfully
            print(execution.success)

            # Download logs
            out_log_path, err_log_path = execution.download_logs()
            ```

        # Arguments
            args: Optional runtime arguments for the job.
            await_termination: Identifies if the client should wait for the job to complete, defaults to True.
        # Returns
            `Execution`: The execution object for the submitted run.
        """
        if self._is_materialization_running(args):
            return None
        print(f"Launching job: {self.name}")
        execution = self._execution_api._start(self, args=args)
        print(
            f"Job started successfully, you can follow the progress at \n{execution.get_url()}"
        )
        if await_termination:
            return self._execution_engine.wait_until_finished(self, execution)
        else:
            return execution

    def get_state(self):
        """Get the state of the job.

        # Returns
            `state`. Current state of the job, which can be one of the following:
            `INITIALIZING`, `INITIALIZATION_FAILED`, `FINISHED`, `RUNNING`, `ACCEPTED`,
            `FAILED`, `KILLED`, `NEW`, `NEW_SAVING`, `SUBMITTED`, `AGGREGATING_LOGS`,
            `FRAMEWORK_FAILURE`, `STARTING_APP_MASTER`, `APP_MASTER_START_FAILED`,
            `GENERATING_SECURITY_MATERIAL`, `CONVERTING_NOTEBOOK`. If no executions are found for the job,
            a warning is raised and it returns `UNDEFINED`.
        """
        last_execution = self._job_api.last_execution(self)
        if len(last_execution) != 1:
            raise JobException("No executions found for job")

        return last_execution[0].state

    def get_final_state(
        self,
    ) -> Literal[
        "UNDEFINED",
        "FINISHED",
        "FAILED",
        "KILLED",
        "FRAMEWORK_FAILURE",
        "APP_MASTER_START_FAILED",
        "INITIALIZATION_FAILED",
    ]:
        """Get the final state of the job.

        # Returns
            `final_state`. Final state of the job, which can be one of the following:
            `UNDEFINED`, `FINISHED`, `FAILED`, `KILLED`, `FRAMEWORK_FAILURE`,
            `APP_MASTER_START_FAILED`, `INITIALIZATION_FAILED`. `UNDEFINED` indicates
             that the job is still running.
        """
        last_execution = self._job_api.last_execution(self)
        if len(last_execution) != 1:
            raise JobException("No executions found for job")

        return last_execution[0].final_status

    def get_executions(self):
        """Retrieves all executions for the job ordered by submission time.

        # Returns
            `List[Execution]`: list of Execution objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._execution_api._get_all(self)

    @usage.method_logger
    def save(self):
        """Save the job.

        This function should be called after changing a property such as the job configuration to save it persistently.

        ```python
        job.config['appPath'] = "Resources/my_app.py"
        job.save()
        ```
        # Returns
            `Job`: The updated job object.
        """
        return self._job_api._update_job(self.name, self.config)

    @usage.method_logger
    def delete(self):
        """Delete the job
        !!! danger "Potentially dangerous operation"
            This operation deletes the job and all executions.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._job_api._delete(self)

    def _is_materialization_running(self, args: str) -> bool:
        if self.name.endswith("offline_fg_materialization") or self.name.endswith(
            "offline_fg_backfill"
        ):
            try:
                should_abort = self.get_final_state() == "UNDEFINED"
            except JobException as e:
                if "No executions found for job" in str(e):
                    should_abort = False
                else:
                    raise e
            if should_abort:
                warnings.warn(
                    "Materialization job is already running, aborting new execution."
                    "Please wait for the current execution to finish before triggering a new one."
                    "You can check the status of the current execution using `fg.materialization_job.get_state()`."
                    "or `fg.materialization_job.get_final_state()` or check it out in the Hopsworks UI."
                    f"at {self.get_url()}.\n"
                    f"Use fg.materialization_job.run(args={args}) to trigger the materialization job again.",
                    stacklevel=2,
                )
                return True
        return False

    def _wait_for_job(self, await_termination=True, timeout: Optional[float] = None):
        # If the user passed the wait_for_job option consider it,
        # otherwise use the default True
        if await_termination:
            executions = self._job_api.last_execution(self)
            if len(executions) > 0:
                execution = executions[0]
            else:
                return
            self._execution_engine.wait_until_finished(
                job=self, execution=execution, timeout=timeout
            )

    def schedule(
        self,
        cron_expression: str,
        start_time: datetime = None,
        end_time: datetime = None,
    ):
        """Schedule the execution of the job.

        If a schedule for this job already exists, the method updates it.

        ```python
        # Schedule the job
        job.schedule(
            cron_expression="0 */5 * ? * * *",
            start_time=datetime.datetime.now(tz=timezone.utc)
        )

        # Retrieve the next execution time
        print(job.job_schedule.next_execution_date_time)
        ```

        # Arguments
            cron_expression: The quartz cron expression
            start_time: The schedule start time in UTC. If None, the current time is used. The start_time can be a value in the past.
            end_time: The schedule end time in UTC. If None, the schedule will continue running indefinitely. The end_time can be a value in the past.
        # Returns
            `JobSchedule`: The schedule of the job
        """
        job_schedule = JobSchedule(
            id=self._job_schedule.id if self._job_schedule else None,
            start_date_time=start_time if start_time else datetime.now(tz=timezone.utc),
            cron_expression=cron_expression,
            end_time=end_time,
            enabled=True,
        )
        self._job_schedule = self._job_api._schedule_job(
            self._name, job_schedule.to_dict()
        )
        return self._job_schedule

    @usage.method_logger
    def unschedule(self):
        """Unschedule the exceution of a Job"""
        self._job_api._delete_schedule_job(self._name)
        self._job_schedule = None

    @usage.method_logger
    def resume_schedule(self):
        """Resumes the schedule of a Job execution"""
        if self._job_schedule is None:
            raise JobException("No schedule found for job")

        job_schedule = JobSchedule(
            id=self._job_schedule.id,
            start_date_time=self._job_schedule.start_date_time,
            cron_expression=self._job_schedule.cron_expression,
            end_time=self._job_schedule.end_date_time,
            enabled=False,
        )
        return self._update_schedule(job_schedule)

    @usage.method_logger
    def pause_schedule(self):
        """Pauses the schedule of a Job execution"""
        if self._job_schedule is None:
            raise JobException("No schedule found for job")

        job_schedule = JobSchedule(
            id=self._job_schedule.id,
            start_date_time=self._job_schedule.start_date_time,
            cron_expression=self._job_schedule.cron_expression,
            end_time=self._job_schedule.end_date_time,
            enabled=True,
        )
        return self._update_schedule(job_schedule)

    @usage.method_logger
    def get_alerts(self) -> List[alert.JobAlert]:
        """Get all alerts for the job.

        # Returns
            `List[JobAlert]`: list of JobAlert objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._alerts_api.get_job_alerts(self._name)

    @usage.method_logger
    def get_alert(self, alert_id: int) -> alert.JobAlert:
        """Get an alert for the job by ID.

        # Arguments
            alert_id: ID of the alert
        # Returns
            `JobAlert`: the JobAlert object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._alerts_api.get_job_alert(self._name, alert_id)

    @usage.method_logger
    def create_alert(
        self,
        receiver: str,
        status: str,
        severity: str,
    ) -> alert.JobAlert:
        """Create an alert for the job.

        ```python
        # Create alert for the job
        job.create_alert(
            receiver="email",
            status="failed",
            severity="critical"
        )
        ```
        # Arguments
            receiver: The receiver of the alert
            status: The status of the alert. Valid values are "long_running", "failed", "finished", "killed"
            severity: The severity of the alert. Valid values are "critical", "warning", "info"
        # Returns
            `JobAlert`: The created JobAlert object
        # Raises
            `ValueError`: If the status is not valid.
            `ValueError`: If the severity is not valid.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._alerts_api.create_job_alert(self._name, receiver, status, severity)

    def _update_schedule(self, job_schedule):
        self._job_schedule = self._job_api.create_or_update_schedule_job(
            self._name, job_schedule.to_dict()
        )
        return self._job_schedule

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"Job({self._name!r}, {self._job_type!r})"

    def get_url(self):
        _client = client.get_instance()
        path = "/p/" + str(_client._project_id) + "/jobs/named/" + self.name
        return util.get_hostname_replaced_url(path)

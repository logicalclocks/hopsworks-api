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

import logging
import os
import time
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from hopsworks_apigen import also_available_as
from hopsworks_common.client.exceptions import JobExecutionException, RestAPIError
from hopsworks_common.core import dataset_api, execution_api


if TYPE_CHECKING:
    from hopsworks_common.execution import Execution


@also_available_as("hopsworks.engine.execution_engine.ExecutionEngine")
class ExecutionEngine:
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()
        self._execution_api = execution_api.ExecutionApi()
        self._log = logging.getLogger(__name__)

    def download_logs(
        self, execution: Execution, path: str | None = None
    ) -> tuple[str | None, str | None]:
        """Download execution logs to current directory.

        Parameters:
            execution: Execution to download logs for.
            path: Path to download the logs.

        Returns:
            Downloaded stdout and stderr log path.

        Raises:
            hopsworks.client.exceptions.JobExecutionException: If path is provided but does not exist.
        """
        if path is not None and not os.path.exists(path):
            raise JobExecutionException(f"Path {path} does not exist")
        if path is None:
            path = os.getcwd()

        job_logs_dir = f"logs-job-{execution.job_name}-exec-{str(execution.id)}_{str(uuid.uuid4())[:16]}"
        download_log_dir = os.path.join(path, job_logs_dir)

        if not os.path.exists(download_log_dir):
            os.mkdir(download_log_dir)

        out_path = None
        if execution.stdout_path is not None and self._dataset_api.exists(
            execution.stdout_path
        ):
            out_path = self._download_log(execution.stdout_path, download_log_dir)

        err_path = None
        if execution.stderr_path is not None and self._dataset_api.exists(
            execution.stderr_path
        ):
            err_path = self._download_log(execution.stderr_path, download_log_dir)

        return out_path, err_path

    def _download_log(self, path, download_log_dir):
        max_num_retries = 12
        retries = 0
        download_path = None
        while retries < max_num_retries:
            try:
                download_path = self._dataset_api.download(
                    path, download_log_dir, overwrite=True
                )
                break
            except RestAPIError as e:
                if (
                    e.response.json().get("errorCode", "") == 110021
                    and e.response.status_code == 400
                    and retries < max_num_retries
                ):
                    retries += 1
                    time.sleep(5)
                else:
                    raise e
        return download_path

    def wait_for_running(self, job, execution, timeout: float = 120) -> Execution:
        """Wait until a Python App execution reaches RUNNING state.

        Parameters:
            job: Job of the execution.
            execution: Execution to monitor.
            timeout: Maximum waiting time in seconds (default 120).

        Returns:
            The updated execution once it reaches RUNNING, a final state, or when the timeout is exceeded.
            On timeout the execution may still be in an intermediate state.

        Raises:
            hopsworks.client.exceptions.JobExecutionException: If the execution reaches an error state before RUNNING.
        """
        from hopsworks_common import constants

        start_time = datetime.now()
        MAX_LAG = 3.0
        updated_execution = self._execution_api._get(job, execution.id)
        while (
            updated_execution.state not in ["RUNNING"]
            and updated_execution.state not in constants.JOBS.ERROR_STATES
            and updated_execution.state not in constants.JOBS.SUCCESS_STATES
        ):
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed + MAX_LAG >= timeout:
                self._log.info("Timed out waiting for app to reach RUNNING state.")
                return updated_execution
            time.sleep(MAX_LAG)
            updated_execution = self._execution_api._get(job, execution.id)
            self._log.info(
                f"Waiting for Python App to start. Current state: {updated_execution.state}"
            )

        if updated_execution.state in constants.JOBS.ERROR_STATES:
            raise JobExecutionException(
                f"Python App failed to start. State: {updated_execution.state}"
            )

        return updated_execution

    def wait_until_finished(
        self, job, execution, timeout: float | None = None
    ) -> Execution | None:
        """Wait until execution terminates.

        Parameters:
            job: Job of the execution.
            execution: Execution to monitor.
            timeout:
                The maximum waiting time in seconds, if `None` the waiting time is unbounded.
                **Note**: the actual waiting time may be bigger by approximately 3 seconds.

        Returns:
            The final execution or `None` if the timeout is exceeded.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
            hopsworks.client.exceptions.JobExecutionException: If the execution finished with a failure status.
        """
        start_time = datetime.now()

        def passed():
            return (datetime.now() - start_time).total_seconds()

        MAX_LAG = 3.0

        is_yarn_job = job.job_type is not None and (
            job.job_type.lower() == "spark"
            or job.job_type.lower() == "pyspark"
            or job.job_type.lower() == "flink"
        )

        updated_execution = self._execution_api._get(job, execution.id)
        execution_state = None
        while updated_execution.success is None:
            if timeout and timeout <= passed() + MAX_LAG:
                self._log.info("The waiting timeout was exceeded.")
                return updated_execution
            time.sleep(MAX_LAG)
            updated_execution = self._execution_api._get(job, execution.id)
            if execution_state != updated_execution.state:
                if is_yarn_job:
                    self._log.info(
                        f"Waiting for execution to finish. Current state: {updated_execution.state}. Final status: {updated_execution.final_status}"
                    )
                else:
                    self._log.info(
                        f"Waiting for execution to finish. Current state: {updated_execution.state}"
                    )
            execution_state = updated_execution.state

        # wait for log files to be aggregated, max 6 minutes
        await_time = 6 * 60.0
        log_aggregation_files_exist = self._dataset_api.exists(
            updated_execution.stdout_path
        ) and self._dataset_api.exists(updated_execution.stderr_path)
        log_aggregation_files_exist_already = log_aggregation_files_exist
        self._log.info("Waiting for log aggregation to finish.")
        while not log_aggregation_files_exist and await_time >= 0:
            if timeout and timeout <= passed() + MAX_LAG:
                break
            await_time -= MAX_LAG
            time.sleep(MAX_LAG)
            updated_execution = self._execution_api._get(job, execution.id)

            log_aggregation_files_exist = self._dataset_api.exists(
                updated_execution.stdout_path
            ) and self._dataset_api.exists(updated_execution.stderr_path)

        if not log_aggregation_files_exist_already and (
            not timeout or timeout > passed() + 5
        ):
            time.sleep(5)  # Helps for log aggregation to flush to filesystem

        if updated_execution.success is False:
            if is_yarn_job:
                status = updated_execution.final_status
            else:
                status = updated_execution.state
            self._log.error(
                f"Execution failed with status: {status}. See the logs for more information."
            )
            if status == "KILLED":
                raise JobExecutionException("The Hopsworks Job was stopped")
            if status == "FAILED":
                raise JobExecutionException(
                    "The Hopsworks Job failed, use the Hopsworks UI to access the job logs"
                )
            if status == "FRAMEWORK_FAILURE":
                raise JobExecutionException(
                    "The Hopsworks Job monitoring failed, could not determine the final status"
                )
            raise JobExecutionException(
                f"Execution failed with status: {status}. See the logs for more information."
            )
        self._log.info("Execution finished successfully.")

        return updated_execution

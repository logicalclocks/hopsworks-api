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
from typing import Any, Dict, List, Optional, Union

from hopsworks_common import (
    client,
    decorators,
    execution,
    job,
    job_schedule,
    usage,
    util,
)
from hopsworks_common.core import (
    ingestion_job_conf,
    job_configuration,
)


class JobApi:
    @usage.method_logger
    def create_job(self, name: str, config: dict) -> job.Job:
        """Create a new job or update an existing one.

        ```python

        import hopsworks

        project = hopsworks.login()

        job_api = project.get_job_api()

        spark_config = job_api.get_configuration("PYSPARK")

        spark_config['appPath'] = "/Resources/my_app.py"

        job = job_api.create_job("my_spark_job", spark_config)

        ```
        # Arguments
            name: Name of the job.
            config: Configuration of the job.
        # Returns
            `Job`: The Job object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()

        config = util.validate_job_conf(config, _client._project_name)

        path_params = ["project", _client._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        created_job = job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            )
        )
        print(f"Job created successfully, explore it at {created_job.get_url()}")
        return created_job

    @usage.method_logger
    @decorators.catch_not_found("hopsworks_common.job.Job", fallback_return=None)
    def get_job(self, name: str) -> Optional[job.Job]:
        """Get a job.

        # Arguments
            name: Name of the job.
        # Returns
            `Job`: The Job object or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            name,
        ]
        query_params = {"expand": ["creator"]}
        return job.Job.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

    @usage.method_logger
    def get_jobs(self) -> List[job.Job]:
        """Get all jobs.

        # Returns
            `List[Job]`: List of Job objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
        ]
        query_params = {"expand": ["creator"]}
        return job.Job.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

    @usage.method_logger
    def exists(self, name: str):
        """Check if a job exists.

        # Arguments
            name: Name of the job.
        # Returns
            `bool`: True if the job exists, otherwise False
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        job = self.get_job(name)
        return job is not None

    @usage.method_logger
    def get_configuration(self, type: str):
        """Get configuration for the specific job type.

        # Arguments
            type: Type of the job. Currently, supported types include: SPARK, PYSPARK, PYTHON, DOCKER, FLINK.
        # Returns
            `dict`: Default job configuration
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            type.lower(),
            "configuration",
        ]

        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

    def _delete(self, job):
        """Delete the job and all executions.
        :param job: metadata object of job to delete
        :type job: Job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            str(job.name),
        ]
        _client._send_request("DELETE", path_params)

    def _update_job(self, name: str, config: dict):
        """Update the job.
        :param name: name of the job
        :type name: str
        :param config: new job configuration
        :type config: dict
        :return: The updated Job object
        :rtype: Job
        """
        _client = client.get_instance()

        config = util.validate_job_conf(config, _client._project_name)

        path_params = ["project", _client._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            )
        )

    def _schedule_job(self, name, schedule_config):
        """Attach the `schedule_config` to the job with the given `name`."""
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "schedule", "v2"]
        headers = {"content-type": "application/json"}
        method = "PUT" if schedule_config["id"] else "POST"

        return job_schedule.JobSchedule.from_response_json(
            _client._send_request(
                method, path_params, headers=headers, data=json.dumps(schedule_config)
            )
        )

    def _delete_schedule_job(self, name):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "schedule", "v2"]

        return _client._send_request(
            "DELETE",
            path_params,
        )

    @usage.method_logger
    def create(
        self,
        name: str,
        job_conf: Union[
            job_configuration.JobConfiguration, ingestion_job_conf.IngestionJobConf
        ],
    ) -> job.Job:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        return job.Job.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=job_conf.json()
            )
        )

    @usage.method_logger
    def launch(self, name: str, args: str = None) -> None:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "executions"]

        _client._send_request("POST", path_params, data=args)

    @usage.method_logger
    def get(self, name: str) -> job.Job:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name]

        return job.Job.from_response_json(_client._send_request("GET", path_params))

    @usage.method_logger
    def last_execution(self, job: job.Job) -> execution.Execution:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", job.name, "executions"]

        query_params = {"limit": 1, "sort_by": "submissiontime:desc"}

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            ),
            job=job,
        )

    @usage.method_logger
    def create_or_update_schedule_job(
        self, name: str, schedule_config: Dict[str, Any]
    ) -> job_schedule.JobSchedule:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "schedule", "v2"]
        headers = {"content-type": "application/json"}
        method = "PUT" if schedule_config["id"] else "POST"

        return job_schedule.JobSchedule.from_response_json(
            _client._send_request(
                method, path_params, headers=headers, data=json.dumps(schedule_config)
            )
        )

    @usage.method_logger
    def delete_schedule_job(self, name: str) -> None:
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", name, "schedule", "v2"]

        return _client._send_request(
            "DELETE",
            path_params,
        )

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
import os
from typing import TYPE_CHECKING, Any, Literal

from hopsworks_common import (
    client,
    decorators,
    execution,
    job,
    job_schedule,
    usage,
    util,
)


if TYPE_CHECKING:
    from hopsworks_common.core import (
        ingestion_job_conf,
        job_configuration,
        sink_job_configuration,
    )

from hopsworks_apigen import public


_DEPLOY_DEFAULT_REMOTE_DIR = "/Resources/jobs"
_DEPLOY_EXTENSION_TO_TYPE: dict[str, str] = {
    ".py": "PYTHON",
    ".jar": "SPARK",
}


def _infer_job_type(local_path: str) -> str:
    """Pick a job type from a script's extension.

    Args:
        local_path: Path to the local script.

    Returns:
        A job type string accepted by :meth:`JobApi.get_configuration`.

    Raises:
        ValueError: When the extension is not in the lookup table.
    """
    ext = os.path.splitext(local_path)[1].lower()
    inferred = _DEPLOY_EXTENSION_TO_TYPE.get(ext)
    if inferred:
        return inferred
    raise ValueError(
        f"Cannot infer job type from extension '{ext}'. Pass the type explicitly."
    )


@public(
    "hopsworks.core.job_api.JobApi",
    "hopsworks.core.job_api.JobsApi",
    "hsfs.core.job_api.JobApi",
)
class JobApi:
    @public
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

        Parameters:
            name: Name of the job.
            config: Configuration of the job.

        Returns:
            The created job.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
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

    @public
    @usage.method_logger
    def deploy(
        self,
        local_path: str,
        name: str,
        type: Literal["SPARK", "PYSPARK", "PYTHON", "PYTHON_APP", "DOCKER", "FLINK"]
        | None = None,
        environment_name: str | None = None,
        args: str | None = None,
        remote_dir: str | None = None,
        overwrite: bool = True,
    ) -> job.Job:
        """Upload a local script and register or update a job that runs it.

        Composes the existing upload and create-or-update primitives so a
        caller can ship a pipeline file end-to-end in one call.
        The job type is inferred from the script's extension when not given
        (``.py`` becomes ``PYTHON``, ``.jar`` becomes ``SPARK``).
        Re-deploying the same ``name`` is idempotent: the script is
        overwritten in place and the job definition is updated, not
        duplicated.

        ```python
        import hopsworks

        project = hopsworks.login()
        api = project.get_job_api()

        job = api.deploy(
            "feature_pipeline.py",
            name="feature_pipeline",
            environment_name="python-feature-pipeline",
        )
        job.schedule(cron_expression="0 0 * * * ?")
        ```

        Parameters:
            local_path: Path to a local file (``.py``, ``.jar``, ...).
            name: Job name. Re-deploying with the same name updates in place.
            type: Job type override; inferred from the extension if omitted.
            environment_name: Python environment for PYTHON / PYSPARK jobs.
            args: Default arguments stored on the job definition.
            remote_dir: HopsFS directory the script lands in. Defaults to
                ``/Resources/jobs/<name>``.
            overwrite: Overwrite an existing script in HopsFS.

        Returns:
            The created or updated job.

        Raises:
            FileNotFoundError: When ``local_path`` does not exist.
            ValueError: When the job type cannot be inferred and was not
                supplied.
            hopsworks.client.exceptions.RestAPIError: When the backend
                rejects the upload or the job definition.
        """
        if not os.path.isfile(local_path):
            raise FileNotFoundError(local_path)

        resolved_type = type.upper() if type else _infer_job_type(local_path)

        dest_dir = (remote_dir or f"{_DEPLOY_DEFAULT_REMOTE_DIR}/{name}").rstrip("/")
        basename = os.path.basename(local_path)
        app_path = f"{dest_dir}/{basename}"

        from hopsworks_common.core import dataset_api

        dataset_api.DatasetApi().upload(
            local_path=local_path, upload_path=dest_dir, overwrite=overwrite
        )

        config = self.get_configuration(resolved_type)
        config["appPath"] = app_path
        if args:
            config["defaultArgs"] = args
        if environment_name:
            config["environmentName"] = environment_name

        return self.create_job(name=name, config=config)

    @public
    @usage.method_logger
    @decorators.catch_not_found("hopsworks_common.job.Job", fallback_return=None)
    def get_job(self, name: str) -> job.Job | None:
        """Get a job.

        Parameters:
            name: Name of the job.

        Returns:
            The Job object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
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

    @public
    @usage.method_logger
    def get_jobs(self) -> list[job.Job]:
        """Get all jobs.

        Returns:
            List of all jobs.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
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
    def exists(self, name: str) -> bool:
        """Check if a job exists.

        Parameters:
            name: Name of the job.

        Returns:
            `True` if the job exists, otherwise `False`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        job = self.get_job(name)
        return job is not None

    @public
    @usage.method_logger
    def get_configuration(
        self,
        type: Literal["SPARK", "PYSPARK", "PYTHON", "PYTHON_APP", "DOCKER", "FLINK"],
    ) -> dict:
        """Get configuration for the specific job type.

        Parameters:
            type: The job type to retrieve the configuration of.

        Returns:
            The default job configuration for the specific job type.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
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

        Parameters:
            job: Metadata object of job to delete.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            str(job.name),
        ]
        _client._send_request("DELETE", path_params)

    def _update_job(self, name: str, config: dict) -> job.Job:
        """Update the job.

        Parameters:
            name: Name of the job.
            config: New job configuration.

        Returns:
            The updated Job object.
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
        job_conf: (
            job_configuration.JobConfiguration
            | ingestion_job_conf.IngestionJobConf
            | sink_job_configuration.SinkJobConfiguration
        ),
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

        # The backend has two @POST handlers on this path (text/plain for legacy
        # args and application/json for logical-time params); without an explicit
        # Content-Type Jersey can't dispatch and returns 415.
        headers = {"content-type": "text/plain"}
        _client._send_request("POST", path_params, headers=headers, data=args)

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
        self, name: str, schedule_config: dict[str, Any]
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

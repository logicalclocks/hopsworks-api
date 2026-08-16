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
from typing import TYPE_CHECKING, Any, Literal

from hopsworks_common import (
    client,
    decorators,
    execution,
    job,
    job_schedule,
    tag,
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


@public(
    "hopsworks.core.job_api.JobApi",
    "hopsworks.core.job_api.JobsApi",
    "hsfs.core.job_api.JobApi",
)
class JobApi:
    def __init__(
        self, project_id: int | None = None, project_name: str | None = None
    ) -> None:
        """Jobs endpoint for a project.

        Parameters:
            project_id: The project to resolve jobs against; the connection's project if omitted.
                A search hit can come from another authorized project, whose jobs are not reachable through the login project's paths.
            project_name: Name of the same project, used where a job configuration path has to be
                expanded to an absolute one. Resolved from the connection when omitted.
        """
        self._project_id = project_id
        self._project_name = project_name

    def _pid(self) -> int:
        if self._project_id is not None:
            return self._project_id
        return client._get_instance()._project_id

    def _pname(self) -> str:
        if self._project_name is not None:
            return self._project_name
        return client._get_instance()._project_name

    def _bind(self, result):
        """Stamp this api's project onto the jobs it just built.

        A Job builds its own JobApi for update, delete, schedule, executions and tags. Left
        unbound, all of those address the connection's project, so a job fetched from another
        authorized project would be mutated in the login project instead.
        """
        jobs = result if isinstance(result, list) else [result]
        for j in jobs:
            if j is not None:
                j._bind_project(self._pid(), self._pname())
        return result

    @public
    @usage._method_logger
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
        _client = client._get_instance()

        config = util._validate_job_conf(config, self._pname())

        path_params = ["project", self._pid(), "jobs", name]

        headers = {"content-type": "application/json"}
        # Bound before the URL is built, not after: get_url reads the job's project, so printing
        # first named the login project even when this handle addresses another one.
        created_job = self._bind(
            job.Job.from_response_json(
                _client._send_request(
                    "PUT", path_params, headers=headers, data=json.dumps(config)
                )
            )
        )
        print(f"Job created successfully, explore it at {created_job.get_url()}")
        return created_job

    @public
    @usage._method_logger
    @decorators._catch_not_found("hopsworks_common.job.Job", fallback_return=None)
    def get_job(self, name: str) -> job.Job | None:
        """Get a job.

        Parameters:
            name: Name of the job.

        Returns:
            The Job object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            name,
        ]
        query_params = {"expand": ["creator"]}
        return self._bind(
            job.Job.from_response_json(
                _client._send_request("GET", path_params, query_params=query_params)
            )
        )

    @public
    @usage._method_logger
    def get_jobs(self) -> list[job.Job]:
        """Get all jobs.

        Returns:
            List of all jobs.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
        ]
        query_params = {"expand": ["creator"]}
        return self._bind(
            job.Job.from_response_json(
                _client._send_request("GET", path_params, query_params=query_params)
            )
        )

    @public
    @usage._method_logger
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
    @usage._method_logger
    def get_configuration(
        self,
        type: Literal["SPARK", "PYSPARK", "PYTHON", "PYTHON_APP", "DOCKER"],
    ) -> dict:
        """Get configuration for the specific job type.

        Parameters:
            type: The job type to retrieve the configuration of.

        Returns:
            The default job configuration for the specific job type.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
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
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
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
        _client = client._get_instance()

        config = util._validate_job_conf(config, self._pname())

        path_params = ["project", self._pid(), "jobs", name]

        headers = {"content-type": "application/json"}
        return self._bind(
            job.Job.from_response_json(
                _client._send_request(
                    "PUT", path_params, headers=headers, data=json.dumps(config)
                )
            )
        )

    def _schedule_job(self, name, schedule_config):
        """Attach the `schedule_config` to the job with the given `name`."""
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            name,
            "schedule",
            "v2",
        ]
        headers = {"content-type": "application/json"}
        method = "PUT" if schedule_config.get("id") else "POST"

        return job_schedule.JobSchedule.from_response_json(
            _client._send_request(
                method, path_params, headers=headers, data=json.dumps(schedule_config)
            )
        )

    def _delete_schedule_job(self, name):
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            name,
            "schedule",
            "v2",
        ]

        return _client._send_request(
            "DELETE",
            path_params,
        )

    @public
    @usage._method_logger
    def create(
        self,
        name: str,
        job_conf: (
            job_configuration.JobConfiguration
            | ingestion_job_conf.IngestionJobConf
            | sink_job_configuration.SinkJobConfiguration
        ),
    ) -> job.Job:
        _client = client._get_instance()
        path_params = ["project", self._pid(), "jobs", name]

        headers = {"content-type": "application/json"}
        return self._bind(
            job.Job.from_response_json(
                _client._send_request(
                    "PUT", path_params, headers=headers, data=job_conf.json()
                )
            )
        )

    @public
    @usage._method_logger
    def launch(self, name: str, args: str = None) -> None:
        _client = client._get_instance()
        path_params = ["project", self._pid(), "jobs", name, "executions"]

        # The backend has two @POST handlers on this path (text/plain for legacy
        # args and application/json for logical-time params); without an explicit
        # Content-Type Jersey can't dispatch and returns 415.
        headers = {"content-type": "text/plain"}
        _client._send_request("POST", path_params, headers=headers, data=args)

    @public
    @usage._method_logger
    def get(self, name: str) -> job.Job:
        _client = client._get_instance()
        path_params = ["project", self._pid(), "jobs", name]

        return self._bind(
            job.Job.from_response_json(_client._send_request("GET", path_params))
        )

    @public
    @usage._method_logger
    def last_execution(self, job: job.Job) -> execution.Execution:
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            job.name,
            "executions",
        ]

        query_params = {"limit": 1, "sort_by": "submissiontime:desc"}

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            ),
            job=job,
        )

    @public
    @usage._method_logger
    def create_or_update_schedule_job(
        self, name: str, schedule_config: dict[str, Any] | job_schedule.JobSchedule
    ) -> job_schedule.JobSchedule:
        _client = client._get_instance()
        # Callers may pass a JobSchedule object (e.g. from a SinkJobConfiguration,
        # which normalizes dict schedules into JobSchedule); serialize it to the
        # dict this endpoint expects.
        if not isinstance(schedule_config, dict):
            schedule_config = schedule_config.to_dict()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            name,
            "schedule",
            "v2",
        ]
        headers = {"content-type": "application/json"}
        method = "PUT" if schedule_config.get("id") else "POST"

        return job_schedule.JobSchedule.from_response_json(
            _client._send_request(
                method, path_params, headers=headers, data=json.dumps(schedule_config)
            )
        )

    @public
    @usage._method_logger
    def delete_schedule_job(self, name: str) -> None:
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            name,
            "schedule",
            "v2",
        ]

        return _client._send_request(
            "DELETE",
            path_params,
        )

    def _add_tag(self, job: job.Job, name: str, value: Any) -> None:
        """Attach a name/value tag to a job.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        Parameters:
            job: The job to attach the tag to.
            name: Name of the tag to be added.
            value: Value of the tag to be added.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            job.name,
            "tags",
            name,
        ]
        headers = {"content-type": "application/json"}
        json_value = json.dumps(value)
        _client._send_request("PUT", path_params, headers=headers, data=json_value)

    def _delete_tag(self, job: job.Job, name: str) -> None:
        """Delete a tag from a job.

        Tag names are unique identifiers.

        Parameters:
            job: The job to remove the tag from.
            name: Name of the tag to be removed.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            job.name,
            "tags",
            name,
        ]
        _client._send_request("DELETE", path_params)

    @decorators._catch_not_found("hopsworks_common.tag.Tag", fallback_return={})
    def _get_tags(self, job: job.Job) -> dict[str, Any]:
        """Get all tags attached to a job.

        Parameters:
            job: The job to get the tags from.

        Returns:
            Dict of tag name/values.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            job.name,
            "tags",
        ]
        # from_response_json already returns deserialized values.
        return {
            t._name: t._value
            for t in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }

    @decorators._catch_not_found("hopsworks_common.tag.Tag", fallback_return=None)
    def _get_tag(self, job: job.Job, name: str) -> Any | None:
        """Get the value of a tag attached to a job.

        Parameters:
            job: The job to get the tag from.
            name: Tag name.

        Returns:
            The value of the tag with the specified name, or `None` if it does not exist.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            job.name,
            "tags",
            name,
        ]
        tags = {
            t._name: t._value
            for t in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }
        return tags.get(name)

    @decorators._catch_not_found("hopsworks_common.tag.Tag", fallback_return={})
    def _get_tags_metadata(
        self, job: job.Job, name: str | None = None
    ) -> dict[str, tag.Tag]:
        """Get the tags of a job as Tag objects, keeping metadata such as created_on.

        Parameters:
            job: The job to get the tags from.
            name: Tag name; all tags if omitted.

        Returns:
            Dict of tag name to Tag object.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            self._pid(),
            "jobs",
            job.name,
            "tags",
        ]
        if name is not None:
            path_params.append(name)
        return {
            t._name: t
            for t in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }

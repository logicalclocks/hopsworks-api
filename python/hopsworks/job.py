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

import humps
import json
from hopsworks.engine import execution_engine
from hopsworks.core import job_api, execution_api
from hopsworks import util


class Job:
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
        project_id=None,
        project_name=None,
    ):
        self._id = id
        self._name = name
        self._creation_time = creation_time
        self._config = config
        self._job_type = job_type
        self._creator = creator
        self._executions = executions

        self._execution_engine = execution_engine.ExecutionEngine(project_id)
        self._execution_api = execution_api.ExecutionsApi(project_id)
        self._execution_engine = execution_engine.ExecutionEngine(project_id)
        self._job_api = job_api.JobsApi(project_id, project_name)

    @classmethod
    def from_response_json(cls, json_dict, project_id, project_name):
        if "count" in json_dict:
            jobs = []
            for job in json_dict["items"]:
                # Job config should not be decamelized when updated
                config = job.pop("config")
                json_decamelized = humps.decamelize(job)
                json_decamelized["config"] = config
                jobs.append(
                    cls(
                        **json_decamelized,
                        project_id=project_id,
                        project_name=project_name,
                    )
                )
            return jobs
        # TODO: fix backend to set count to 0 when no jobs exists
        elif "id" not in json_dict:
            return []
        else:
            # Job config should not be decamelized when updated
            config = json_dict.pop("config")
            json_decamelized = humps.decamelize(json_dict)
            json_decamelized["config"] = config
            return cls(
                **json_decamelized, project_id=project_id, project_name=project_name
            )

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

    def run(self, args: str = None, await_termination: bool = None):
        """Run the job, with the option of passing runtime arguments.

        Example of a blocking execution and downloading logs once execution is finished.

        ```python

        # Run the job
        execution = job.run(await_termination=True)

        # True if job executed successfully
        print(execution.success)

        # Download logs
        out_log_path, err_log_path = execution.download_logs()

        ```
        # Arguments
            args: optional runtime arguments for the job
            await_termination: if True wait until termination is complete
        # Returns
            `Execution`. The execution object for the submitted run.
        """
        execution = self._execution_api._start(self, args=args)
        if await_termination:
            return self._execution_engine.wait_until_finished(self, execution)
        else:
            return execution

    def get_executions(self):
        """Retrieves all executions for the job.

        # Returns
            `List[Execution]`
        # Raises
            `RestAPIError` in case the backend fails to retrieve executions.
        """
        return self._execution_api._get_all(self)

    def save(self):
        """Save the job.

        This function should be called after changing a property such as the job configuration to save it persistently.

        ```python
        job.config['appPath'] = "Resources/my_app.py"
        job.save()
        ```
        # Returns
            `Job`. The updated job object.
        """
        return self._job_api._update_job(self.name, self.config)

    def delete(self):
        """Delete the job
        !!! danger "Potentially dangerous operation"
            This operation deletes the job and all executions.
        # Raises
            `RestAPIError`.
        """
        self._job_api._delete(self)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Job({self._name!r}, {self._job_type!r})"

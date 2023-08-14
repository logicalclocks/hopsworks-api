#
#   Copyright 2023 Hopsworks AB
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

import os
import json
from hopsworks import client, flink_cluster, util
from hopsworks.core import job_api


class FlinkClusterApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name
        self._job_api = job_api.JobsApi(project_id, project_name)

    def get_configuration(self):
        return self._job_api.get_configuration("FLINK")

    def setup_cluster(self, name, config=None):
        if self._job_api.exists(name):
            # If the job already exists, retrieve it
            return self.get_cluster(name)
        else:
            # If the job doesn't exists, create a new job
            if config is None:
                config = self.get_configuration()
                config["appName"] = name
            return self.create_cluster(name, config)

    def create_cluster(self, name: str, config: dict):
        """Create a new job or update an existing one.

        ```python

        import hopsworks

        project = hopsworks.login()

        flink_cluster_api = project.get_flink_cluster_api()

        flink_config = flink_cluster_api.get_configuration()

        flink_config['appName'] = "myFlinkCluster"

        flink_cluster = flink_cluster_api.setup_cluster(name="producerTransactions", config=flink_config)

        ```
        # Arguments
            name: Name of the cluster.
            config: Configuration of the cluster.
        # Returns
            `Job`: The Job object
        # Raises
            `RestAPIError`: If unable to create the job
        """
        _client = client.get_instance()

        config = util.validate_job_conf(config, self._project_name)

        path_params = ["project", self._project_id, "jobs", name]

        headers = {"content-type": "application/json"}
        created_cluster = flink_cluster.FlinkCluster.from_response_json(
            _client._send_request(
                "PUT", path_params, headers=headers, data=json.dumps(config)
            ),
            self._project_id,
            self._project_name,
        )
        print(created_cluster.get_url())
        return created_cluster

    def get_cluster(self, name: str):
        """Get a flink cluster.

        # Arguments
            name: Name of the cluster.
        # Returns
            `Job`: The Job object
        # Raises
            `RestAPIError`: If unable to get the job
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
            "jobs",
            name,
        ]
        query_params = {"expand": ["creator"]}
        return flink_cluster.FlinkCluster.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params),
            self._project_id,
            self._project_name,
        )

    def get_jobs(self, execution):
        _client = client.get_instance()
        path_params = ["hopsworks-api", "flinkmaster", execution.app_id, "jobs"]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "GET", path_params, headers=headers, with_base_path_params=False
        )

    def stop(self, execution):
        _client = client.get_instance()
        path_params = ["hopsworks-api", "flinkmaster", execution.app_id, "cluster"]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "DELETE", path_params, headers=headers, with_base_path_params=False
        )

    def stop_job(self, execution, flink_cluster_job):
        _client = client.get_instance()
        path_params = [
            "hopsworks-api",
            "flinkmaster",
            execution.app_id,
            "jobs",
            flink_cluster_job["id"],
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "PATCH", path_params, headers=headers, with_base_path_params=False
        )

    def get_jars(self, execution):
        _client = client.get_instance()
        path_params = ["hopsworks-api", "flinkmaster", execution.app_id, "jars"]
        headers = {"content-type": "application/json"}
        response = _client._send_request(
            "GET", path_params, headers=headers, with_base_path_params=False
        )
        return response["files"]

    def upload_jar(self, execution, jar_file_path):
        _client = client.get_instance()
        path_params = [
            "hopsworks-api",
            "flinkmaster",
            execution.app_id,
            "jars",
            "upload",
        ]
        files = {
            "jarfile": (
                os.path.basename(jar_file_path),
                open(jar_file_path, "rb"),
                "application/x-java-archive",
            )
        }
        _client._send_request(
            "POST", path_params, files=files, with_base_path_params=False
        )
        print("Flink Jar uploaded.")

    def submit_job(self, execution, jar_id, main_class_name, job_arguments=None):
        _client = client.get_instance()
        # Submit execution
        if job_arguments:
            path_params = [
                "hopsworks-api",
                "flinkmaster",
                execution.app_id,
                "jars",
                jar_id,
                f"run?entry-class={main_class_name}&program-args={job_arguments}",
            ]
        else:
            path_params = [
                "hopsworks-api",
                "flinkmaster",
                execution.app_id,
                "jars",
                jar_id,
                f"run?entry-class{main_class_name}",
            ]

        headers = {"content-type": "application/json"}
        response = _client._send_request(
            "POST", path_params, headers=headers, with_base_path_params=False
        )

        job_id = response["jobid"]
        print("Submitted Job Id: {}".format(job_id))

        return job_id

    def job_status(self, execution, flink_cluster_job):
        _client = client.get_instance()
        path_params = [
            "hopsworks-api",
            "flinkmaster",
            execution.app_id,
            "jobs",
            flink_cluster_job["id"],
        ]
        response = _client._send_request(
            "GET", path_params, with_base_path_params=False
        )

        # Possible states: [ "INITIALIZING", "CREATED", "RUNNING", "FAILING", "FAILED", "CANCELLING", "CANCELED",
        # "FINISHED", "RESTARTING", "SUSPENDED", "RECONCILING" ]
        return response["state"]

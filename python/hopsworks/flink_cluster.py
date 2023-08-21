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

import time
from hopsworks import job
from hopsworks import flink_execution


class FlinkCluster(job.Job):
    pass

    def start(self, await_time=120):
        """Start flink job representing a flink cluster.

        ```python

        import hopsworks

        project = hopsworks.login()

        flink_cluster_api = project.get_flink_cluster_api()

        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        flink_cluster.start()
        ```
        # Arguments
            await_time: defaults to 120 seconds.
        # Returns
            `Execution`: The Execution object.
        # Raises
            `RestAPIError`: If unable to create the job
        """

        execution = self._execution_api._start(self)
        updated_execution = self._execution_api._get(self, execution.id)
        while updated_execution.state == "INITIALIZING":
            updated_execution = self._execution_api._get(self, execution.id)
            if updated_execution.state == "RUNNING":
                print("Cluster is running")
                return self._map_to_exection(updated_execution)

            self._execution_engine._log.info(
                "Waiting for cluster to start. Current state: {}.".format(
                    updated_execution.state
                )
            )

            await_time -= 1
            time.sleep(1)

        if execution.state != "RUNNING":
            raise "Execution {} did not start within the allocated time and exited with state {}".format(
                execution.id, execution.state
            )

    def get_executions(self):
        """Retrieves all executions for the flink cluster.

        # Returns
            `List[FlinkExecution]`
        # Raises
            `RestAPIError` in case the backend fails to retrieve executions.
        """
        execution_objects = self._execution_api._get_all(self)
        return [
            self._map_to_exection(execution_object)
            for execution_object in execution_objects
        ]

    def get_execution(self, id: str):
        """Retrieves execution for the flink cluster.

        # Arguments
            id: id if the execution
        # Returns
            `FlinkExecution`
        # Raises
            `RestAPIError` in case the backend fails to retrieve executions.
        """
        return self._map_to_exection(self._execution_api._get(self, id))

    def _map_to_exection(self, execution_object):
        return flink_execution.FlinkExecution(
            id=execution_object._id,
            state=execution_object._state,
            final_status=execution_object._final_status,
            submission_time=execution_object._submission_time,
            stdout_path=execution_object._submission_time,
            stderr_path=execution_object._stderr_path,
            app_id=execution_object._app_id,
            hdfs_user=execution_object._hdfs_user,
            args=execution_object._args,
            progress=execution_object._progress,
            user=execution_object._user,
            duration=execution_object._duration,
            monitoring=execution_object._monitoring,
            project_id=execution_object._project_id,
            project_name=self._job_api._project_name,
            job_name=execution_object._job_name,
            job_type=execution_object._job_type,
        )

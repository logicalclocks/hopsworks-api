#
#   Copyright 2023 Logical Clocks AB
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

from hopsworks import execution

from hopsworks.engine import execution_engine
from hopsworks.core import execution_api
from hopsworks.core import flink_cluster_api


class FlinkExecution(execution.Execution):
    def __init__(
        self,
        id=None,
        state=None,
        final_status=None,
        submission_time=None,
        stdout_path=None,
        stderr_path=None,
        app_id=None,
        hdfs_user=None,
        args=None,
        progress=None,
        user=None,
        duration=None,
        monitoring=None,
        project_id=None,
        project_name=None,
        job_name=None,
        job_type=None,
    ):
        self._id = id
        self._final_status = final_status
        self._state = state
        self._submission_time = submission_time
        self._stdout_path = stdout_path
        self._stderr_path = stderr_path
        self._args = args
        self._progress = progress
        self._user = user
        self._duration = duration
        self._monitoring = monitoring
        self._app_id = app_id
        self._hdfs_user = hdfs_user
        self._job_name = job_name
        self._job_type = job_type
        self._project_id = project_id

        self._execution_engine = execution_engine.ExecutionEngine(project_id)
        self._execution_api = execution_api.ExecutionsApi(project_id)
        self._flink_cluster_api = flink_cluster_api.FlinkClusterApi(
            project_id, project_name
        )

    def get_jobs(self):
        """Get jobs from the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # get jobs from this execution
        execution.get_jobs()
        ```

        # Returns
            `List[Dict]`: The array of dicts with flink job id and and status of the job.
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """

        return self._flink_cluster_api._get_jobs(self)

    def get_job(self, job_id):
        """Get specific job from the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # get jobs from this execution
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        execution.get_job( job_id)
        ```

        # Arguments
            job_id: id of the job within this execution
        # Returns
            `Dict`: Dict with flink job id and and status of the job.
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """

        return self._flink_cluster_api._get_job(self, job_id)

    def stop_job(self, job_id):
        """Stop specific job of the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # stop the job
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        execution.stop_job(job_id)
        ```

        # Arguments
            job_id: id of the job within this execution
        # Raises
            `RestAPIError`: If unable to stop the job
        """
        self._flink_cluster_api._stop_job(self, job_id)

    def get_jars(self):
        """Get already uploaded jars from the specific execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # get jar files from this execution
        execution.get_jars(execution)
        ```

        # Returns
            `List[Dict]`: The array of dicts with jar metadata.
        # Raises
            `RestAPIError`: If unable to get jars from the execution
        """
        return self._flink_cluster_api._get_jars(self)

    def upload_jar(self, jar_file):
        """Uploaded jar file to the specific execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # upload jar file jobs from this execution
        jar_file_path = "./flink-example.jar"
        execution.upload_jar(jar_file_path)
        ```

        # Arguments
            jar_file: path to the jar file.
        # Raises
            `RestAPIError`: If unable to upload jar file
        """

        self._flink_cluster_api._upload_jar(self, jar_file)

    def submit_job(self, jar_id, main_class, job_arguments=None):
        """Submit job using the specific jar file, already uploaded to this execution of the flink cluster.
        ```python
        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # upload jar file jobs from this execution
        main_class = "com.example.Main"
        job_arguments = "-arg1 arg1 -arg2 arg2"

        #get jar file metadata (and select the 1st one for demo purposes)
        jar_metadata = execution.get_jars()[0]
        jar_id = jar_metadata["id"]
        execution.submit_job(jar_id, main_class, job_arguments=job_arguments)
        ```

        # Arguments
            jar_id: id if the jar file
            main_class: path to the main class of the the jar file
            job_arguments: Job arguments (if any), defaults to none.
        # Returns
            `str`:  job id.
        # Raises
            `RestAPIError`: If unable to submit the job.
        """

        return self._flink_cluster_api._submit_job(
            self, jar_id, main_class, job_arguments
        )

    def job_state(self, job_id):
        """Gets state of the job from the specific execution of the flink cluster.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # get jobs from this execution
        job_id = '113a2af5b724a9b92085dc2d9245e1d6'
        execution.job_status(job_id)
        ```

        # Arguments
            job_id: id of the job within this execution
        # Returns
            `str`: status of the job. Possible states:  "INITIALIZING", "CREATED", "RUNNING", "FAILING", "FAILED",
            "CANCELLING", "CANCELED",  "FINISHED", "RESTARTING", "SUSPENDED", "RECONCILING".
        # Raises
            `RestAPIError`: If unable to get the jobs from the execution
        """

        return self._flink_cluster_api._job_state(self, job_id)

    def stop(self):
        """Stop this execution.
        ```python

        # log in to hopsworks
        import hopsworks
        project = hopsworks.login()

        # fetch flink cluster handle
        flink_cluster_api = project.get_flink_cluster_api()
        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        # get all executions(This will return empty list of no execution is running on this Flink cluster)
        executions = flink_cluster.get_executions()

        # select 1st execution
        execution = executions[0]

        # stop this execution
        execution.stop(execution)
        ```

        # Arguments
            execution: Execution object.
        # Raises
            `RestAPIError`: If unable to stop the execution
        """
        self._flink_cluster_api._stop_execution(self)

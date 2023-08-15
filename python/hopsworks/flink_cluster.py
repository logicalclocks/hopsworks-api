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


class FlinkCluster(job.Job):
    pass

    def start(self, start_time_out=120):
        """Start flink job representing a flink cluster.

        ```python

        import hopsworks

        project = hopsworks.login()

        flink_cluster_api = project.get_flink_cluster_api()

        flink_cluster = flink_cluster_api.get_cluster(name="myFlinkCluster")

        flink_cluster.start()
        ```
        # Arguments
            start_time_out: defaults to 120 seconds.
        # Returns
            `Execution`: The Execution object.
        # Raises
            `RestAPIError`: If unable to create the job
        """

        execution = super().run()

        polling_time = 0
        while polling_time < start_time_out:
            execution = self._execution_api._get(self, execution.id)
            if execution.state == "RUNNING":
                print("Cluster is running")
                return execution

            print(
                "Waiting for cluster to be running. Current state: {}".format(
                    execution.state
                )
            )

            polling_time += 1
            time.sleep(1)

        if execution.state != "RUNNING":
            raise "Execution {} did not start within the allocated time and exited with state {}".format(
                execution.id, execution.state
            )

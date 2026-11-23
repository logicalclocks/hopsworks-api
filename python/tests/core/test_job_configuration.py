#
#   Copyright 2022 Hopsworks AB
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


from hsfs.core import job_configuration


class TestJobConfiguration:
    def test_to_dict_defaults(self):
        # Arrange
        job_config = job_configuration.JobConfiguration()

        # Act
        result_dict = job_config.to_dict()

        # Assert
        expected_dict = {
            "spark.driver.memory": 4096,
            "spark.driver.cores": 1,
            "spark.executor.memory": 4096,
            "spark.executor.cores": 1,
            "spark.executor.instances": 1,
            "spark.dynamicAllocation.enabled": True,
            "spark.dynamicAllocation.minExecutors": 1,
            "spark.dynamicAllocation.maxExecutors": 2,
            "environmentName": "spark-feature-pipeline",
            "type": job_configuration.JobConfiguration.DTO_TYPE,
        }
        assert expected_dict == result_dict

    def test_to_dict_non_defaults(self):
        # Arrange
        job_config = job_configuration.JobConfiguration(
            driver_memory=8192,
            driver_cores=2,
            executor_memory=8192,
            executor_cores=2,
            executor_instances=2,
            dynamic_allocation=False,
            dynamic_min_executors=2,
            dynamic_max_executors=4,
            environment_name="spark-feature-pipeline",
        )

        # Act
        result_dict = job_config.to_dict()

        # Assert
        expected_dict = {
            "spark.driver.memory": 8192,
            "spark.driver.cores": 2,
            "spark.executor.memory": 8192,
            "spark.executor.cores": 2,
            "spark.executor.instances": 2,
            "spark.dynamicAllocation.enabled": False,
            "spark.dynamicAllocation.minExecutors": 2,
            "spark.dynamicAllocation.maxExecutors": 4,
            "environmentName": "spark-feature-pipeline",
            "type": job_configuration.JobConfiguration.DTO_TYPE,
        }
        assert expected_dict == result_dict

    def test_to_dict_memory_overhead_factors(self):
        # Arrange
        job_config = job_configuration.JobConfiguration(
            driver_memory_overhead_factor=0.2,
            executor_memory_overhead_factor=0.35,
        )

        # Act
        result_dict = job_config.to_dict()

        # Assert
        assert result_dict["spark.driver.memoryOverheadFactor"] == 0.2
        assert result_dict["spark.executor.memoryOverheadFactor"] == 0.35

    def test_positional_arguments_keep_their_meaning(self):
        # Arrange
        job_config = job_configuration.JobConfiguration(
            4096, 2, 8192, 2, 2, False, 2, 4, "spark-feature-pipeline"
        )

        # Act
        result_dict = job_config.to_dict()

        # Assert
        assert result_dict["spark.driver.memory"] == 4096
        assert result_dict["spark.dynamicAllocation.maxExecutors"] == 4
        assert "spark.driver.memoryOverheadFactor" not in result_dict
        assert "spark.executor.memoryOverheadFactor" not in result_dict

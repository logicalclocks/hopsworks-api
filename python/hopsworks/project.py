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

import hsfs.feature_store
import humps
from hopsworks import client, constants, util
from hopsworks.core import (
    dataset_api,
    environment_api,
    flink_cluster_api,
    git_api,
    job_api,
    kafka_api,
    opensearch_api,
)


class Project:
    def __init__(
        self,
        archived=None,
        created=None,
        description=None,
        docker_image=None,
        hops_examples=None,
        inodeid=None,
        is_old_docker_image=None,
        is_preinstalled_docker_image=None,
        owner=None,
        project_id=None,
        project_name=None,
        project_team=None,
        quotas=None,
        retention_period=None,
        services=None,
        datasets=None,
        creation_status=None,
        **kwargs,
    ):
        self._id = project_id
        self._name = project_name
        self._owner = owner
        self._description = description
        self._created = created

        self._opensearch_api = opensearch_api.OpenSearchApi()
        self._kafka_api = kafka_api.KafkaApi()
        self._jobs_api = job_api.JobsApi()
        self._flink_cluster_api = flink_cluster_api.FlinkClusterApi()
        self._git_api = git_api.GitApi()
        self._dataset_api = dataset_api.DatasetApi()
        self._environment_api = environment_api.EnvironmentApi()

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    @property
    def id(self):
        """Id of the project"""
        return self._id

    @property
    def name(self):
        """Name of the project"""
        return self._name

    @property
    def owner(self):
        """Owner of the project"""
        return self._owner

    @property
    def description(self):
        """Description of the project"""
        return self._description

    @property
    def created(self):
        """Timestamp when the project was created"""
        return self._created

    def get_feature_store(
        self, name: str = None, engine: str = None
    ) -> hsfs.feature_store.FeatureStore:
        """Connect to Project's Feature Store.

        Defaulting to the project name of default feature store. To get a
        shared feature store, the project name of the feature store is required.

        !!! example "Example for getting the Feature Store API of a project"
            ```python
            import hopsworks

            project = hopsworks.login()

            fs = project.get_feature_store()
            ```

        # Arguments
            name: Project name of the feature store.
            engine: Which engine to use, `"spark"`, `"python"` or `"training"`.
                Defaults to `"python"` when connected to [Serverless Hopsworks](https://app.hopsworks.ai).
                See hsfs.Connection.connection documentation for more information.
        # Returns
            `hsfs.feature_store.FeatureStore`: The Feature Store API
        # Raises
            `RestAPIError`: If unable to connect
        """
        from hsfs import connection

        _client = client.get_instance()
        if _client._is_external():
            if _client._host == constants.HOSTS.APP_HOST and engine is None:
                engine = "python"
            return connection(
                host=_client._host,
                port=_client._port,
                project=self.name,
                api_key_value=_client._auth._token,
                engine=engine,
                hostname_verification=_client._hostname_verification,
                trust_store_path=_client._hopsworks_ca_trust_store_path,
            ).get_feature_store(name)
        else:
            return connection(engine=engine).get_feature_store(
                name
            )  # If internal client

    def get_model_registry(self):
        """Connect to Project's Model Registry API.

        !!! example "Example for getting the Model Registry API of a project"
            ```python
            import hopsworks

            project = hopsworks.login()

            mr = project.get_model_registry()
            ```

        # Returns
            `hsml.model_registry.ModelRegistry`: The Model Registry API
        # Raises
            `RestAPIError`: If unable to connect
        """
        from hsml import connection

        _client = client.get_instance()
        if _client._is_external():
            return connection(
                host=_client._host,
                port=_client._port,
                project=self.name,
                api_key_value=_client._auth._token,
                hostname_verification=_client._hostname_verification,
                trust_store_path=_client._hopsworks_ca_trust_store_path,
            ).get_model_registry()
        else:
            return connection().get_model_registry()  # If internal client

    def get_model_serving(self):
        """Connect to Project's Model Serving API.

        !!! example "Example for getting the Model Serving API of a project"
            ```python
            import hopsworks

            project = hopsworks.login()

            ms = project.get_model_serving()
            ```

        # Returns
            `hsml.model_serving.ModelServing`: The Model Serving API
        # Raises
            `RestAPIError`: If unable to connect
        """
        from hsml import connection

        _client = client.get_instance()
        if _client._is_external():
            return connection(
                host=_client._host,
                port=_client._port,
                project=self.name,
                api_key_value=_client._auth._token,
                hostname_verification=_client._hostname_verification,
                trust_store_path=_client._hopsworks_ca_trust_store_path,
            ).get_model_serving()
        else:
            return connection().get_model_serving()  # If internal client

    def get_kafka_api(self):
        """Get the kafka api for the project.

        # Returns
            `KafkaApi`: The Kafka Api handle
        """
        _client = client.get_instance()
        if _client._is_external():
            _client.download_certs()
        return self._kafka_api

    def get_opensearch_api(self):
        """Get the opensearch api for the project.

        # Returns
            `OpenSearchApi`: The OpenSearch Api handle
        """
        _client = client.get_instance()
        if _client._is_external():
            _client.download_certs()
        return self._opensearch_api

    def get_jobs_api(self):
        """Get the jobs api for the project.

        # Returns
            `JobsApi`: The Jobs Api handle
        """
        return self._jobs_api

    def get_flink_cluster_api(self):
        """Get the flink cluster api for the project.

        # Returns
            `FlinkClusterApi`: The Flink Cluster Api handle
        """
        return self._flink_cluster_api

    def get_git_api(self):
        """Get the git repository api for the project.

        # Returns
            `GitApi`: The Git Api handle
        """
        return self._git_api

    def get_dataset_api(self):
        """Get the dataset api for the project.

        # Returns
            `DatasetApi`: The Datasets Api handle
        """
        return self._dataset_api

    def get_environment_api(self):
        """Get the Python environment AP

        # Returns
            `EnvironmentApi`: The Python Environment Api handle
        """
        return self._environment_api

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        if self._description is not None:
            return f"Project({self._name!r}, {self._owner!r}, {self._description!r})"
        else:
            return f"Project({self._name!r}, {self._owner!r})"

    def get_url(self):
        path = "/p/" + str(self.id)
        return util.get_hostname_replaced_url(path)

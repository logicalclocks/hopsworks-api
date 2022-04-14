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

from hopsworks import util
from hopsworks.core import job_api, git_api, dataset_api, kafka_api


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
    ):
        self._id = project_id
        self._name = project_name
        self._owner = owner
        self._description = description
        self._created = created

        self._kafka_api = kafka_api.KafkaApi(project_id)
        self._jobs_api = job_api.JobsApi(project_id, project_name)
        self._git_api = git_api.GitApi(project_id, project_name)
        self._dataset_api = dataset_api.DatasetApi(project_id)

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

    def get_kafka_api(self):
        """Get the kafka api for the project.
        # Returns
            `KafkaApi`: The Kafka Api handle
        """
        return self._kafka_api

    def get_jobs_api(self):
        """Get the jobs api for the project.
        # Returns
            `JobsApi`: The Jobs Api handle
        """
        return self._jobs_api

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

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        if self._description is not None:
            return f"Project({self._name!r}, {self._owner!r}, {self._description!r})"
        else:
            return f"Project({self._name!r}, {self._owner!r})"

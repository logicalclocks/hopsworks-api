#
#   Copyright 2021 Logical Clocks AB
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

from hopsworks_common.client.exceptions import ModelRegistryException
from hsml import client
from hsml.core import dataset_api
from hsml.model_registry import ModelRegistry


class ModelRegistryApi:
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()

    def _get(
        self, project: str | None = None, project_id: int | None = None
    ) -> ModelRegistry:
        """Get model registry for specific project.

        Parameters:
            project: project of the model registry
            project_id: id of the project of the model registry; takes precedence over `project`,
                since display names can repeat across projects

        Returns:
            the model registry metadata
        """
        _client = client._get_instance()

        model_registry_id = _client._project_id
        shared_registry_project_name = None
        models_path = None

        # In the case of shared model registry, validate that there is Models dataset shared to the connected project from the set project
        if project is not None or project_id is not None:
            path_params = ["project", _client._project_id, "modelregistries"]
            model_registries = _client._send_request("GET", path_params)
            matches = [
                registry
                for registry in model_registries["items"]
                if (
                    registry["id"] == project_id
                    if project_id is not None
                    else registry["name"] == project
                )
            ]
            if len(matches) > 1:
                raise ModelRegistryException(
                    f"Several model registries shared with current project {_client._project_name} are named {project}, "
                    f"use the project id: {sorted(registry['id'] for registry in matches)}"
                )
            if not matches:
                raise ModelRegistryException(
                    f"No model registry shared with current project {_client._project_name}, from project {project if project_id is None else project_id}"
                )
            model_registry_id = matches[0]["id"]
            shared_registry_project_name = matches[0]["name"]
            models_path = matches[0].get("modelsPath")
        # In the case of default model registry, validate that there is a Models dataset in the connected project
        elif project is None and not self._dataset_api.path_exists("Models"):
            raise ModelRegistryException(
                f"No Models dataset exists in project {_client._project_name}, Please enable the Serving service or create the dataset manually."
            )

        return ModelRegistry(
            _client._project_name,
            _client._project_id,
            model_registry_id,
            shared_registry_project_name=shared_registry_project_name,
            project_backing_name=_client._project_backing_name,
            models_path=models_path,
        )

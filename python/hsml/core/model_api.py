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

import json

from hsml import client, decorators, model, tag
from hsml.core import explicit_provenance


class ModelApi:
    def __init__(self):
        pass

    def put(self, model_instance: model.Model, query_params: dict) -> model.Model:
        """Save model metadata to the model registry.

        Parameters:
            model_instance: metadata object of model to be saved

        Returns:
            updated metadata object of the model
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.name + "_" + str(model_instance.version),
        ]
        headers = {"content-type": "application/json"}
        return model_instance.update_from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                headers=headers,
                query_params=query_params,
                data=model_instance.json(),
            )
        )

    @decorators.catch_not_found("hsml.model.Model", fallback_return=None)
    def get(
        self,
        name: str,
        version: int,
        model_registry_id: int,
        shared_registry_project_name: str | None = None,
    ) -> model.Model | None:
        """Get the metadata of a model with a certain name and version.

        Parameters:
            name: name of the model
            version: version of the model

        Returns:
            model metadata object
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            model_registry_id,
            "models",
            name + "_" + str(version),
        ]
        query_params = {"expand": "trainingdatasets"}

        model_json = _client._send_request("GET", path_params, query_params)
        model_meta = model.Model.from_response_json(model_json)

        model_meta.shared_registry_project_name = shared_registry_project_name

        return model_meta

    def get_models(
        self,
        name: str,
        model_registry_id: int,
        shared_registry_project_name: str | None = None,
        metric: str | None = None,
        direction: str | None = None,
    ) -> list[model.Model]:
        """Get the metadata of models based on the name or optionally the best model given a metric and direction.

        Parameters:
            name: name of the model
            metric: Name of the metric to maximize or minimize
            direction: Whether to maximize or minimize the metric, allowed values are 'max' or 'min'

        Returns:
            model metadata object
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            model_registry_id,
            "models",
        ]
        query_params = {
            "expand": "trainingdatasets",
            "filter_by": ["name_eq:" + name],
        }

        if metric is not None and direction is not None:
            if direction.lower() == "max":
                direction = "desc"
            elif direction.lower() == "min":
                direction = "asc"

            query_params["sort_by"] = metric + ":" + direction
            query_params["limit"] = "1"

        model_json = _client._send_request("GET", path_params, query_params)
        models_meta = model.Model.from_response_json(model_json)

        for model_meta in models_meta:
            model_meta.shared_registry_project_name = shared_registry_project_name

        return models_meta

    def delete(self, model_instance: model.Model) -> None:
        """Delete the model and metadata.

        Parameters:
            model_instance: metadata object of model to delete
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
        ]
        _client._send_request("DELETE", path_params)

    def set_tag(
        self, model_instance: model.Model, name: str, value: str | dict
    ) -> None:
        """Attach a name/value tag to a model.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        Parameters:
            model_instance: model instance to attach tag
            name: name of the tag to be added
            value: value of the tag to be added
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "tags",
            name,
        ]
        headers = {"content-type": "application/json"}
        json_value = json.dumps(value)
        _client._send_request("PUT", path_params, headers=headers, data=json_value)

    def delete_tag(self, model_instance: model.Model, name: str) -> None:
        """Delete a tag.

        Tag names are unique identifiers.

        Parameters:
            model_instance: model instance to delete tag from
            name: name of the tag to be removed
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "tags",
            name,
        ]
        _client._send_request("DELETE", path_params)

    @decorators.catch_not_found("hopsworks_common.tag.Tag", fallback_return={})
    def get_tags(self, model_instance: model.Model) -> dict:
        """Get the tags.

        Gets all tags if no tag name is specified.

        Parameters:
            model_instance: model instance to get the tags from
            name: tag name

        Returns:
            dict of tag name/values
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "tags",
        ]
        return {
            tag._name: json.loads(tag._value)
            for tag in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }

    @decorators.catch_not_found("hopsworks_common.tag.Tag", fallback_return=None)
    def get_tag(self, model_instance: model.Model, name: str) -> dict | None:
        """Get the tag.

        Gets the tag for a specific name

        Parameters:
            model_instance: model instance to get the tags from
            name: tag name

        Returns:
            dict of tag name/value
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "tags",
            name,
        ]

        return tag.Tag.from_response_json(_client._send_request("GET", path_params))[
            name
        ]

    def get_feature_view_provenance(
        self, model_instance
    ) -> explicit_provenance.Links | None:
        """Get the parent feature view of this model, based on explicit provenance.

        These feature views can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature views, only a minimal information is returned.

        Parameters:
            model_instance: Metadata object of model.

        Returns:
            The feature view used to generate this model or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case of a server error.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 2,
            "downstreamLvls": 0,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        links = explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.UPSTREAM,
            explicit_provenance.Links.Type.FEATURE_VIEW,
        )
        if not links.is_empty():
            return links
        return None

    def get_training_dataset_provenance(
        self, model_instance
    ) -> explicit_provenance.Links | None:
        """Get the parent training dataset of this model, based on explicit provenance.

        These training datasets can be accessible, deleted or inaccessible.
        For deleted and inaccessible training dataset, only a minimal information is returned.

        Parameters:
            model_instance: Metadata object of model.

        Returns:
            The training dataset used to generate this model or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case of a server error.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 1,
            "downstreamLvls": 0,
        }
        links_json = _client._send_request("GET", path_params, query_params)
        links = explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.UPSTREAM,
            explicit_provenance.Links.Type.TRAINING_DATASET,
        )
        if not links.is_empty():
            return links
        return None

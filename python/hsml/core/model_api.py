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

import json
from typing import Union

from hsml import client, decorators, model, tag
from hsml.core import explicit_provenance


class ModelApi:
    def __init__(self):
        pass

    def put(self, model_instance, query_params):
        """Save model metadata to the model registry.

        :param model_instance: metadata object of model to be saved
        :type model_instance: Model
        :return: updated metadata object of the model
        :rtype: Model
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
    def get(self, name, version, model_registry_id, shared_registry_project_name=None):
        """Get the metadata of a model with a certain name and version.

        :param name: name of the model
        :type name: str
        :param version: version of the model
        :type version: int
        :return: model metadata object
        :rtype: Model
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
        name,
        model_registry_id,
        shared_registry_project_name=None,
        metric=None,
        direction=None,
    ):
        """Get the metadata of models based on the name or optionally the best model given a metric and direction.

        :param name: name of the model
        :type name: str
        :param metric: Name of the metric to maximize or minimize
        :type metric: str
        :param direction: Whether to maximize or minimize the metric, allowed values are 'max' or 'min'
        :type direction: str
        :return: model metadata object
        :rtype: Model
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

    def delete(self, model_instance):
        """Delete the model and metadata.

        :param model_instance: metadata object of model to delete
        :type model_instance: Model
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

    def set_tag(self, model_instance, name, value: Union[str, dict]):
        """Attach a name/value tag to a model.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        :param model_instance: model instance to attach tag
        :type model_instance: Model
        :param name: name of the tag to be added
        :type name: str
        :param value: value of the tag to be added
        :type value: str or dict
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

    def delete_tag(self, model_instance, name):
        """Delete a tag.

        Tag names are unique identifiers.

        :param model_instance: model instance to delete tag from
        :type model_instance: Model
        :param name: name of the tag to be removed
        :type name: str
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
    def get_tags(self, model_instance):
        """Get the tags.

        Gets all tags if no tag name is specified.

        :param model_instance: model instance to get the tags from
        :type model_instance: Model
        :param name: tag name
        :type name: str
        :return: dict of tag name/values
        :rtype: dict
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
    def get_tag(self, model_instance, name: str):
        """Get the tag.

        Gets the tag for a specific name

        :param model_instance: model instance to get the tags from
        :type model_instance: Model
        :param name: tag name
        :type name: str
        :return: dict of tag name/value
        :rtype: dict
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

    def get_feature_view_provenance(self, model_instance):
        """Get the parent feature view of this model, based on explicit provenance.
        These feature views can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature views, only a minimal information is returned.

        # Arguments
            model_instance: Metadata object of model.

        # Returns
            `Links`: the feature view used to generate this model or `None` if it does not exist.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`.
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

    def get_training_dataset_provenance(self, model_instance):
        """Get the parent training dataset of this model, based on explicit provenance.
        These training datasets can be accessible, deleted or inaccessible.
        For deleted and inaccessible training dataset, only a minimal information is returned.

        # Arguments
            model_instance: Metadata object of model.

        # Returns
            `Links`: the training dataset used to generate this model or `None` if it does not exist.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`.
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

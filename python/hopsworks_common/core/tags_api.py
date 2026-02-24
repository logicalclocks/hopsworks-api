#
#   Copyright 2020 Logical Clocks AB
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
from typing import TYPE_CHECKING

from hopsworks_apigen import also_available_as
from hopsworks_common import client, decorators, tag, usage


if TYPE_CHECKING:
    from hsfs.feature_group import FeatureGroup
    from hsfs.training_dataset import TrainingDataset


@also_available_as("hopsworks.core.tags_api.TagsApi", "hsfs.core.tags_api.TagsApi")
class TagsApi:
    def __init__(self, feature_store_id: int, entity_type: str):
        """Tags endpoint for `trainingdatasets` and `featuregroups` resource.

        Parameters:
            feature_store_id: id of the respective featurestore
            entity_type: "trainingdatasets" or "featuregroups"
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type

    @usage.method_logger
    def add(
        self, metadata_instance: TrainingDataset | FeatureGroup, name: str, value: str, training_dataset_version=None
    ):
        """Attach a name/value tag to a training dataset or feature group.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        Parameters:
            metadata_instance: metadata object of the instance to add the tag for
            name: name of the tag to be added
            value: value of the tag to be added
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version) + [
            name
        ]
        headers = {"content-type": "application/json"}
        json_value = json.dumps(value)
        _client._send_request("PUT", path_params, headers=headers, data=json_value)

    @usage.method_logger
    def delete(self, metadata_instance: TrainingDataset | FeatureGroup, name: str, training_dataset_version=None):
        """Delete a tag from a training dataset or feature group.

        Tag names are unique identifiers.

        Parameters:
            metadata_instance: metadata object of training dataset to delete the tag for
            name: name of the tag to be removed
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version) + [
            name
        ]

        _client._send_request("DELETE", path_params)

    @usage.method_logger
    @decorators.catch_not_found("hopsworks_common.tag.Tag", fallback_return={})
    def get(
        self, metadata_instance: TrainingDataset | FeatureGroup, name: str | None = None, training_dataset_version=None
    ) -> dict:
        """Get the tags of a training dataset or feature group.

        Gets all tags if no tag name is specified.

        Parameters:
            metadata_instance: metadata object of training dataset to get the tags for
            name: tag name

        Returns:
            dict of tag name/values
        """
        _client = client.get_instance()
        path_params = self.get_path(metadata_instance, training_dataset_version)

        if name is not None:
            path_params.append(name)

        return {
            tag._name: json.loads(tag._value)
            for tag in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }

    @usage.method_logger
    def get_path(self, metadata_instance, training_dataset_version=None):
        _client = client.get_instance()
        if hasattr(metadata_instance, "training_data"):
            # Only FeatureView has training_data method
            path = [
                "project",
                _client._project_id,
                "featurestores",
                self._feature_store_id,
                "featureview",
                metadata_instance.name,
                "version",
                metadata_instance.version,
            ]
            if training_dataset_version:
                return path + [
                    "trainingdatasets",
                    "version",
                    training_dataset_version,
                    "tags",
                ]
            return path + ["tags"]
        return [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "tags",
        ]

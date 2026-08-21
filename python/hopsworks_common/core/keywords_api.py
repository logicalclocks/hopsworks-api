#
#   Copyright 2026 Hopsworks AB
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
from hopsworks_common import client, tag, usage


if TYPE_CHECKING:
    from datetime import datetime

    from hsfs.feature_group import FeatureGroup
    from hsfs.training_dataset import TrainingDataset


@also_available_as(
    "hopsworks.core.keywords_api.KeywordsApi", "hsfs.core.keywords_api.KeywordsApi"
)
class KeywordsApi:
    def __init__(
        self, feature_store_id: int | None = None, entity_type: str | None = None
    ):
        """Keywords endpoint for `trainingdatasets`, `featuregroups`, and `featureview` resources.

        Parameters:
            feature_store_id: id of the respective featurestore
            entity_type: "trainingdatasets" or "featuregroups"

        Both may be omitted when the instance is only used for `_get_all`, which is not scoped to an artifact.
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type

    @usage._method_logger
    def _get(
        self,
        metadata_instance: TrainingDataset | FeatureGroup,
        training_dataset_version=None,
    ) -> list[str]:
        """Get the keywords of a feature group, feature view, or training dataset.

        Parameters:
            metadata_instance: Metadata object of the instance to get the keywords for.
            training_dataset_version: Version of the training dataset.

        Returns:
            List of keywords.
        """
        _client = client._get_instance()
        path_params = self._get_path(metadata_instance, training_dataset_version)
        return _client._send_request("GET", path_params).get("keywords") or []

    @usage._method_logger
    def _get_with_metadata(
        self,
        metadata_instance: TrainingDataset | FeatureGroup,
        training_dataset_version=None,
    ) -> dict[str, datetime | None]:
        """Get the keywords with the time each was attached.

        An old server sends only the plain keyword list, without the `items` carrying the timestamps; every keyword then maps to `None`.

        Parameters:
            metadata_instance: Metadata object of the instance to get the keywords for.
            training_dataset_version: Version of the training dataset.

        Returns:
            Dict of keyword to attachment time, `None` when the time is unknown.
        """
        _client = client._get_instance()
        path_params = self._get_path(metadata_instance, training_dataset_version)
        response = _client._send_request("GET", path_params)
        created_on = {
            item["name"]: tag.Tag._parse_created_on(item.get("createdOn"))
            for item in response.get("items") or []
            if item.get("name")
        }
        return {name: created_on.get(name) for name in response.get("keywords") or []}

    @usage._method_logger
    def _replace(
        self,
        metadata_instance: TrainingDataset | FeatureGroup,
        keywords: list[str],
        training_dataset_version=None,
    ) -> list[str]:
        """Replace the whole keyword set of the artifact.

        Parameters:
            metadata_instance: Metadata object of the instance to set the keywords for.
            keywords: The new keyword set.
            training_dataset_version: Version of the training dataset.

        Returns:
            The updated list of keywords.
        """
        _client = client._get_instance()
        path_params = self._get_path(metadata_instance, training_dataset_version)
        headers = {"content-type": "application/json"}
        return (
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=json.dumps({"keywords": keywords}),
            ).get("keywords")
            or []
        )

    @usage._method_logger
    def _delete(
        self,
        metadata_instance: TrainingDataset | FeatureGroup,
        keyword: str,
        training_dataset_version=None,
    ) -> list[str]:
        """Delete a single keyword from the artifact.

        Parameters:
            metadata_instance: Metadata object of the instance to delete the keyword for.
            keyword: The keyword to remove.
            training_dataset_version: Version of the training dataset.

        Returns:
            The updated list of keywords.
        """
        _client = client._get_instance()
        path_params = self._get_path(metadata_instance, training_dataset_version)
        response = _client._send_request(
            "DELETE", path_params, query_params={"keyword": keyword}
        )
        return (response or {}).get("keywords") or []

    @usage._method_logger
    def _get_all(self) -> list[str]:
        """Get the keyword vocabulary in use across the whole cluster.

        Cluster-wide despite the project-scoped path: the backend serves the same vocabulary to every project.

        Returns:
            List of keywords.
        """
        _client = client._get_instance()
        path_params = ["project", _client._project_id, "featurestores", "keywords"]
        return _client._send_request("GET", path_params).get("keywords") or []

    @usage._method_logger
    def _get_path(self, metadata_instance, training_dataset_version=None):
        _client = client._get_instance()
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
                    "keywords",
                ]
            return path + ["keywords"]
        return [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            metadata_instance.id,
            "keywords",
        ]

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

import hsfs.feature_store
from hopsworks_common import client


class FeatureStoreApi:
    def get(self, identifier: int | str) -> hsfs.feature_store.FeatureStore:
        """Get feature store with specific id or name.

        Parameters:
            identifier: id or name of the feature store

        Returns:
            the featurestore metadata
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "featurestores", identifier]
        return hsfs.feature_store.FeatureStore.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_all(self) -> list[hsfs.feature_store.FeatureStore]:
        """Get every feature store accessible from the current project.

        Includes the project's own feature store and any feature stores
        shared with it, mirroring what the UI lists. The backend has no
        project-wide feature-group endpoint, so callers that want to see
        feature groups across shared stores must union ``get_feature_groups``
        over this list.

        Returns:
            a list of featurestore metadata objects
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "featurestores"]
        return [
            hsfs.feature_store.FeatureStore.from_response_json(fs)
            for fs in _client._send_request("GET", path_params)
        ]

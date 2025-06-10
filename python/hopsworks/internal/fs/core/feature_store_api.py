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

from typing import Union

import hsfs.feature_store
from hopsworks_common import client


class FeatureStoreApi:
    def get(self, identifier: Union[int, str]) -> hsfs.feature_store.FeatureStore:
        """Get feature store with specific id or name.

        :param identifier: id or name of the feature store
        :type identifier: int, str
        :return: the featurestore metadata
        :rtype: FeatureStore
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "featurestores", identifier]
        return hsfs.feature_store.FeatureStore.from_response_json(
            _client._send_request("GET", path_params)
        )

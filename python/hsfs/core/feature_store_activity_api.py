#
#   Copyright 2024 Hopsworks AB
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

from typing import List

from hopsworks_common import client
from hsfs import feature_store_activity as fsa_mod


class FeatureStoreActivityApi:
    def get_feature_group_activities(
        self,
        feature_store_id: int,
        feature_group_id: int,
        activity_type: List[fsa_mod.FeatureStoreActivityType],
        limit: int = 100,
        offset: int = 0,
    ) -> List[fsa_mod.FeatureStoreActivity]:
        """
        Get the activities for a feature group in the feature store
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "featuregroups",
            feature_group_id,
            "activity",
        ]

        query_params = {
            "limit": limit,
            "offset": offset,
            "expand": [
                "users",
                "commits",
                "jobs",
                "validationreport",
                "expectationsuite",
                "executions",
                "statistics",
            ],
            "sortby": "timestamp:desc",
        }
        if activity_type is not None and len(activity_type) > 0:
            query_params["activityType"] = activity_type

        response = _client._send_request("GET", path_params, query_params=query_params)

        return fsa_mod.FeatureStoreActivity.from_response_json(response)

#
#   Copyright 2025 Hopsworks AB
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

from typing import Dict, Optional

from hopsworks_common import client
from hsfs import feature_group as fg_mod
from hsfs.core import online_ingestion


class OnlineIngestionApi:
    def create_online_ingestion(
        self,
        feature_group_instance: fg_mod.FeatureGroup,
        online_ingestion_instance: online_ingestion.OnlineIngestion,
    ) -> online_ingestion.OnlineIngestion:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "online_ingestion",
        ]

        headers = {"content-type": "application/json"}
        return online_ingestion.OnlineIngestion.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=online_ingestion_instance.json(),
            ),
            feature_group=feature_group_instance,
        )

    def get_online_ingestion(
        self,
        feature_group_instance: fg_mod.FeatureGroup,
        query_params: Optional[Dict[str, str]] = None,
    ) -> online_ingestion.OnlineIngestion:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "online_ingestion",
        ]

        return online_ingestion.OnlineIngestion.from_response_json(
            _client._send_request("GET", path_params, query_params),
            feature_group=feature_group_instance,
        )

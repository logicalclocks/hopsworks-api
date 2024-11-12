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

from hopsworks_common import client
from hsfs import feature_group as fg_mod
from hsfs.core import ingestion_run


class IngestionRunApi:

    def save_ingestion_run(
        self,
        feature_group_instance: fg_mod.FeatureGroup,
        ingestion_run_instance: ingestion_run.IngestionRun,
    ):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "ingestionrun",
        ]

        headers = {"content-type": "application/json"}
        _client._send_request("POST", path_params, headers=headers, data=ingestion_run_instance.json())

    def get_ingestion_run(
        self,
        feature_group_instance: fg_mod.FeatureGroup,
        query_params: None,
    ):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_group_instance.feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "ingestionrun",
        ]

        ingestion_run_instance = ingestion_run.IngestionRun.from_response_json(
            _client._send_request("GET", path_params, query_params)
        )
        ingestion_run_instance.feature_group = feature_group_instance
        return ingestion_run_instance

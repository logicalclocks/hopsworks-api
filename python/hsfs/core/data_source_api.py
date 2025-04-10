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

from hopsworks_common import client
from hsfs.core import data_source as ds
from hsfs.core import data_source_data as dsd


class DataSourceApi:

    def get_databases(self, feature_store_id: int, name: str) -> list[str]:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
            "data_source",
            "databases",
        ]

        return _client._send_request("GET", path_params)

    def get_tables(self, feature_store_id: int, name: str, database: str) -> list[ds.DataSource]:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
            "data_source",
            "tables",
        ]

        query_params = {"database": database}

        return ds.DataSource.from_response_json(
            _client._send_request("GET", path_params, query_params)
        )

    def get_data(self, feature_store_id: int, name: str, data_source: ds.DataSource) -> dsd.DataSourceData:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
            "data_source",
            "data",
        ]

        query_params = data_source.to_dict()

        return dsd.DataSourceData.from_response_json(
            _client._send_request("GET", path_params, query_params)
        )


    def get_metadata(self, feature_store_id: int, name: str, data_source: ds.DataSource) -> dict:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
            "data_source",
            "metadata",
        ]

        query_params = data_source.to_dict()

        return _client._send_request("GET", path_params, query_params)

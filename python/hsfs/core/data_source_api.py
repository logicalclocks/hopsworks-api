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

import json
from typing import TYPE_CHECKING

from hopsworks_common import client
from hsfs.core import data_source as ds
from hsfs.core import data_source_data as dsd


if TYPE_CHECKING:
    from hsfs import storage_connector as sc


class DataSourceApi:
    def get_databases(self, storage_connector: sc.StorageConnector) -> list[str]:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector._featurestore_id,
            "storageconnectors",
            storage_connector._name,
            "data_source",
            "databases",
        ]

        return _client._send_request("GET", path_params)

    def get_crm_resources(
        self, storage_connector: sc.StorageConnector
    ) -> dsd.DataSourceData:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector._featurestore_id,
            "storageconnectors",
            storage_connector._name,
            "data_source",
            "resources",
        ]
        return dsd.DataSourceData.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_tables(
        self, storage_connector: sc.StorageConnector, database: str
    ) -> list[ds.DataSource]:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector._featurestore_id,
            "storageconnectors",
            storage_connector._name,
            "data_source",
            "tables",
        ]

        query_params = {"database": database}

        return ds.DataSource.from_response_json(
            _client._send_request("GET", path_params, query_params),
            storage_connector=storage_connector,
        )

    def get_no_sql_data(
        self,
        storage_connector: sc.StorageConnector,
        data_source: ds.DataSource,
    ) -> dsd.DataSourceData:
        if storage_connector.type == "REST":
            return self._get_rest_data(storage_connector, data_source)
        if storage_connector.type == "CRM":
            return self._get_crm_data(storage_connector, data_source)
        raise ValueError("This connector type does not support fetching NoSQL data.")

    def _get_rest_data(
        self,
        storage_connector: sc.StorageConnector,
        data_source: ds.DataSource,
    ) -> dsd.DataSourceData:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector._featurestore_id,
            "storageconnectors",
            storage_connector._name,
            "data_source",
            "resources",
            data_source.table,
        ]

        return dsd.DataSourceData.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers={"content-type": "application/json"},
                data=json.dumps(data_source.rest_endpoint.to_dict()),
            )
        )

    def _get_crm_data(
        self,
        storage_connector: sc.StorageConnector,
        data_source: ds.DataSource,
    ) -> dsd.DataSourceData:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector._featurestore_id,
            "storageconnectors",
            storage_connector._name,
            "data_source",
            "resources",
            data_source.table,
        ]

        return dsd.DataSourceData.from_response_json(
            _client._send_request(
                "GET", path_params, query_params=data_source.to_dict()
            )
        )

    def get_data(self, data_source: ds.DataSource) -> dsd.DataSourceData:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            data_source._storage_connector._featurestore_id,
            "storageconnectors",
            data_source._storage_connector._name,
            "data_source",
            "data",
        ]

        query_params = data_source.to_dict()

        return dsd.DataSourceData.from_response_json(
            _client._send_request("GET", path_params, query_params)
        )

    def get_metadata(self, data_source: ds.DataSource) -> dict:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            data_source._storage_connector._featurestore_id,
            "storageconnectors",
            data_source._storage_connector._name,
            "data_source",
            "metadata",
        ]

        query_params = data_source.to_dict()

        return _client._send_request("GET", path_params, query_params)

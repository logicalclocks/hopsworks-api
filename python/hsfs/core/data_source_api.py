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
from hopsworks_common.client.exceptions import (
    PlatformIntelligenceException,
    RestAPIError,
)
from hsfs.core import data_source as ds
from hsfs.core import data_source_data as dsd
from hsfs.core import inferred_metadata as im


if TYPE_CHECKING:
    from hsfs import storage_connector as sc


# Backend BrewerErrorCode values: range 520000 + the per-code offset, see
# RESTCodes.java::BrewerErrorCode in hopsworks-rest-utils.
_BREWER_LLM_NOT_CONFIGURED = 520012
_BREWER_METADATA_INFERENCE_FAILED = 520013


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

        # ``DataSource.from_response_json`` returns a single ``DataSource`` when
        # the backend payload has no ``items`` key (some connectors do this for
        # one-row responses) and ``None`` for an empty body. Normalize both so
        # the contract matches the type hint and callers can iterate freely.
        result = ds.DataSource.from_response_json(
            _client._send_request("GET", path_params, query_params),
            storage_connector=storage_connector,
        )
        if result is None:
            return []
        if isinstance(result, ds.DataSource):
            return [result]
        return result

    def get_no_sql_data(
        self,
        storage_connector: sc.StorageConnector,
        data_source: ds.DataSource,
        use_cached=True,
    ) -> dsd.DataSourceData:
        if storage_connector.type == "REST":
            return self._get_rest_data(storage_connector, data_source, use_cached)
        if storage_connector.type == "CRM":
            return self._get_crm_data(storage_connector, data_source, use_cached)
        raise ValueError("This connector type does not support fetching NoSQL data.")

    def _get_rest_data(
        self,
        storage_connector: sc.StorageConnector,
        data_source: ds.DataSource,
        use_cached=True,
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

        query_params = {"forceRefetch": not use_cached}
        return dsd.DataSourceData.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                query_params=query_params,
                headers={"content-type": "application/json"},
                data=json.dumps(data_source.rest_endpoint.to_dict()),
            )
        )

    def _get_crm_data(
        self,
        storage_connector: sc.StorageConnector,
        data_source: ds.DataSource,
        use_cached=True,
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
        query_params = data_source.to_dict()
        if use_cached is not None:
            query_params["forceRefetch"] = not use_cached

        return dsd.DataSourceData.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
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

    def infer_metadata(
        self,
        storage_connector: sc.StorageConnector,
        preview_data: dsd.DataSourceData,
    ) -> im.InferredMetadata:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector._featurestore_id,
            "storageconnectors",
            storage_connector._name,
            "data_source",
            "infer-metadata",
        ]

        # Backend Row.values is a list of Pair<String, String> rendered as
        # {"value0": col_name, "value1": cell_value}. Pull out the cell value
        # at column index i for each row to build per-column samples.
        preview_rows = preview_data.preview or []
        columns = []
        for i, feature in enumerate(preview_data.features or []):
            values = []
            for row in preview_rows:
                row_values = row.get("values") if isinstance(row, dict) else None
                cell = (
                    row_values[i]
                    if isinstance(row_values, list) and i < len(row_values)
                    else None
                )
                values.append(cell.get("value1") if isinstance(cell, dict) else None)
            columns.append(
                {"name": feature.name, "type": feature.type, "values": values}
            )

        try:
            response = _client._send_request(
                "POST",
                path_params,
                headers={"content-type": "application/json"},
                data=json.dumps({"columns": columns}),
            )
        except RestAPIError as err:
            # Translate the two backend BrewerErrorCodes the inference path
            # can raise into a typed exception so callers don't have to
            # string-match server messages.
            if err.error_code == _BREWER_LLM_NOT_CONFIGURED:
                raise PlatformIntelligenceException(
                    PlatformIntelligenceException.NOT_CONFIGURED,
                    "Platform intelligence is not enabled on this Hopsworks "
                    "cluster: the LLM API key is not configured. Ask the "
                    "cluster admin to set PLATFORM_INTELLIGENCE_LLM_API_KEY.",
                ) from err
            if err.error_code == _BREWER_METADATA_INFERENCE_FAILED:
                raise PlatformIntelligenceException(
                    PlatformIntelligenceException.INFERENCE_FAILED,
                    "Platform intelligence call failed while inferring "
                    f"metadata: {err}",
                ) from err
            raise

        return im.InferredMetadata.from_response_json(response)

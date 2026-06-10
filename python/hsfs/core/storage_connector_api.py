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
from typing import TYPE_CHECKING, Any

from hopsworks_common import client, util
from hsfs import decorators, storage_connector


if TYPE_CHECKING:
    from hsfs.core.explicit_provenance import Links


class StorageConnectorApi:
    @decorators._catch_not_found(
        "hsfs.storage_connector.StorageConnector", fallback_return=None
    )
    def _get_response(
        self, feature_store_id: int, name: str
    ) -> dict[str, Any] | None:
        """Returning response dict, or None if the connector is not found."""
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
        ]
        query_params = {"temporaryCredentials": True}

        return _client._send_request("GET", path_params, query_params=query_params)

    def _create(
        self,
        storage_connector_instance: storage_connector.StorageConnector,
    ) -> storage_connector.StorageConnector:
        """Create a new storage connector in the feature store.

        Parameters:
            storage_connector_instance: The storage connector to create.

        Returns:
            The created storage connector with its assigned id.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector_instance._featurestore_id,
            "storageconnectors",
        ]
        headers = {"content-type": "application/json"}
        return storage_connector.StorageConnector.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=json.dumps(storage_connector_instance, cls=util.Encoder),
            )
        )

    def _update(
        self,
        storage_connector_instance: storage_connector.StorageConnector,
    ) -> storage_connector.StorageConnector:
        """Update an existing storage connector in the feature store.

        Parameters:
            storage_connector_instance: The storage connector to update.

        Returns:
            The updated storage connector.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector_instance._featurestore_id,
            "storageconnectors",
            storage_connector_instance.name,
        ]
        headers = {"content-type": "application/json"}
        return storage_connector_instance.update_from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                headers=headers,
                data=json.dumps(storage_connector_instance, cls=util.Encoder),
            )
        )

    def _get(
        self, feature_store_id: int, name: str
    ) -> storage_connector.StorageConnector:
        """Get storage connector with name.

        Parameters:
            feature_store_id: feature store id
            name: name of the storage connector

        Returns:
            the storage connector
        """
        storage_connector_json = self._get_response(feature_store_id, name)
        if storage_connector_json:
            return storage_connector.StorageConnector.from_response_json(
                storage_connector_json
            )
        return None

    def _refetch(
        self, storage_connector_instance: storage_connector.StorageConnector
    ) -> storage_connector.StorageConnector:
        """Refetch the storage connector from Hopsworks, updating temporary credentials.

        Parameters:
            storage_connector_instance: The storage connector to refetch.

        Returns:
            The updated storage connector instance.
        """
        return storage_connector_instance.update_from_response_json(
            self._get_response(
                storage_connector_instance._featurestore_id,
                storage_connector_instance.name,
            )
        )

    def _get_uc_bearer(
        self,
        feature_store_id: int,
        name: str,
    ) -> dict[str, Any]:
        """Vend a Databricks Unity Catalog bearer for the SDK Spark read path.

        EE provides only the bearer; the SDK takes it from here and calls
        Databricks directly for vended S3 temp-credentials, then drives the
        Delta read.
        Response carries the bearer in clear and the EE side sets
        Cache-Control: no-store.

        Parameters:
            feature_store_id: Numeric id of the feature store containing the connector.
            name: Name of the Unity Catalog storage connector.

        Returns:
            Dict with keys ``access_token`` and ``expires_in_seconds``.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
            "uc_bearer",
        ]
        return _client._send_request("GET", path_params)

    def _get_online_connector(
        self, feature_store_id: int
    ) -> storage_connector.OnlineStorageConnector:
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            "onlinefeaturestore",
        ]

        return storage_connector.StorageConnector.from_response_json(
            _client._send_request("GET", path_params)
        )

    def _get_kafka_connector(
        self, feature_store_id: int, external: bool = False
    ) -> storage_connector.KafkaConnector:
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            "kafka_connector",
            "byok",
        ]
        query_params = {"external": external}

        return storage_connector.StorageConnector.from_response_json(
            _client._send_request("GET", path_params, query_params=query_params)
        )

    def _get_feature_groups_provenance(self, storage_connector_instance) -> Links:
        """Get the generated feature groups using this storage connector, based on explicit provenance.

        These feature groups can be accessible or inaccessible. Explicit
        provenance does not track deleted generated feature group links, so deleted
        will always be empty.
        For inaccessible feature groups, only a minimal information is returned.

        Parameters:
            storage_connector_instance: Metadata object of storage connector.

        Returns:
            The feature groups generated using this storage connector.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector_instance._featurestore_id,
            "storageconnectors",
            storage_connector_instance.name,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 0,
            "downstreamLvls": 1,
        }
        links_json = _client._send_request("GET", path_params, query_params)

        from hsfs.core import explicit_provenance

        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.DOWNSTREAM,
            explicit_provenance.Links.Type.FEATURE_GROUP,
        )

    def _get_training_datasets_provenance(self, storage_connector_instance) -> Links:
        """Get the generated training datasets using this storage connector, based on explicit provenance.

        These training datasets can be accessible or inaccessible.
        Explicit provenance does not track deleted generated training dataset links, so deleted will always be empty.
        For inaccessible training datasets, only a minimal information is returned.

        Parameters:
            storage_connector_instance: Metadata object of storage connector.

        Returns:
            The training datasets generated using this storage connector.
        """
        _client = client._get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector_instance._featurestore_id,
            "storageconnectors",
            storage_connector_instance.name,
            "provenance",
            "links",
        ]
        query_params = {
            "expand": "provenance_artifacts",
            "upstreamLvls": 0,
            "downstreamLvls": 1,
        }
        links_json = _client._send_request("GET", path_params, query_params)

        from hsfs.core import explicit_provenance

        return explicit_provenance.Links.from_response_json(
            links_json,
            explicit_provenance.Links.Direction.DOWNSTREAM,
            explicit_provenance.Links.Type.TRAINING_DATASET,
        )

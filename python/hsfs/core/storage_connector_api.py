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
import re
from typing import TYPE_CHECKING, Any

from hopsworks_common import client, tag, util
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import decorators, storage_connector


if TYPE_CHECKING:
    from hsfs.core.explicit_provenance import Links


class StorageConnectorApi:
    @decorators._catch_not_found(
        "hsfs.storage_connector.StorageConnector", fallback_return=None
    )
    def _get_response(
        self, feature_store_id: int, name: str, with_mandatory_tags: bool = False
    ) -> dict[str, Any] | None:
        """Returning response dict, or None if the connector is not found.

        Parameters:
            feature_store_id: The feature store the connector belongs to.
            name: The connector name.
            with_mandatory_tags: Whether the response carries the mandatory tags the connector lacks, which costs the backend extra queries.
        """
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
        if with_mandatory_tags:
            query_params["expand"] = "mandatorytags"

        return _client._send_request("GET", path_params, query_params=query_params)

    def _create(
        self,
        storage_connector_instance: storage_connector.StorageConnector,
        tags: tag.Tag | dict[str, Any] | list[tag.Tag | dict[str, Any]] | None = None,
    ) -> storage_connector.StorageConnector:
        """Create a new storage connector in the feature store.

        Parameters:
            storage_connector_instance: The storage connector to create.
            tags: Tags to attach in the same request, each naming its tag schema and carrying a value that the schema validates.

        Returns:
            The created storage connector with its assigned id.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If tags are passed to a Hopsworks backend older than 5.3.
        """
        body = storage_connector_instance.to_dict()
        tag_list = tag.Tag._normalize(tags)
        if tag_list:
            self._require_data_source_tags()
            # Each value encoded as _add_tag encodes it, so a tag stores the same whether attached here or later.
            # Tag.to_dict leaves strings and lists unencoded: "123" would read back as 123, and a list is refused.
            body["tags"] = {
                "count": len(tag_list),
                "items": [
                    {"name": t.name, "value": json.dumps(t.value)} for t in tag_list
                ],
            }
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
                data=json.dumps(body, cls=util.Encoder),
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
        self, feature_store_id: int, name: str, with_mandatory_tags: bool = False
    ) -> storage_connector.StorageConnector:
        """Get storage connector with name.

        Parameters:
            feature_store_id: feature store id
            name: name of the storage connector
            with_mandatory_tags: Whether to read the mandatory tags the connector lacks into its `missing_mandatory_tags`.

        Returns:
            the storage connector
        """
        storage_connector_json = self._get_response(
            feature_store_id, name, with_mandatory_tags
        )
        if storage_connector_json:
            return storage_connector.StorageConnector.from_response_json(
                storage_connector_json
            )
        return None

    @staticmethod
    def _require_data_source_tags() -> None:
        # An older backend has no tags field on the connector and ignores unknown JSON properties, so it would create the data source and drop its tags without an error.
        connection = client._get_connection()
        backend_version = connection.backend_version if connection else None
        match = re.match(r"(\d+)\.(\d+)", backend_version or "")
        if match and (int(match.group(1)), int(match.group(2))) < (5, 3):
            raise FeatureStoreException(
                f"Hopsworks {backend_version} cannot store tags on a data source; they need Hopsworks 5.3 or later. "
                "Create the data source without tags."
            )

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

    def _mail_path(
        self, feature_store_id: int, name: str, service: str, *rest: str
    ) -> list:
        """REST path of a mail data source's resources; `service` is `gmail` or `outlook`."""
        _client = client._get_instance()
        return [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "storageconnectors",
            name,
            service,
            *rest,
        ]

    def _get_mail_access_token(
        self, feature_store_id: int, name: str, service: str
    ) -> dict[str, Any]:
        return client._get_instance()._send_request(
            "POST", self._mail_path(feature_store_id, name, service, "token")
        )

    def _get_gmail_labels(
        self, feature_store_id: int, name: str
    ) -> list[dict[str, Any]]:
        return client._get_instance()._send_request(
            "GET", self._mail_path(feature_store_id, name, "gmail", "labels")
        )

    def _get_outlook_folders(
        self, feature_store_id: int, name: str
    ) -> list[dict[str, Any]]:
        return client._get_instance()._send_request(
            "GET", self._mail_path(feature_store_id, name, "outlook", "folders")
        )

    def _put_mail_sync_job(
        self,
        feature_store_id: int,
        name: str,
        service: str,
        job_name: str,
        sync_job: dict[str, Any],
    ) -> dict[str, Any]:
        return client._get_instance()._send_request(
            "PUT",
            self._mail_path(feature_store_id, name, service, "sync", job_name),
            headers={"content-type": "application/json"},
            data=json.dumps(sync_job),
        )

    def _get_mail_sync_jobs(
        self, feature_store_id: int, name: str, service: str
    ) -> list[dict[str, Any]]:
        return client._get_instance()._send_request(
            "GET", self._mail_path(feature_store_id, name, service, "sync")
        )

    def _get_mail_sync_job(
        self, feature_store_id: int, name: str, service: str, job_name: str
    ) -> dict[str, Any]:
        return client._get_instance()._send_request(
            "GET", self._mail_path(feature_store_id, name, service, "sync", job_name)
        )

    def _delete_mail_sync_job(
        self, feature_store_id: int, name: str, service: str, job_name: str
    ) -> None:
        client._get_instance()._send_request(
            "DELETE",
            self._mail_path(feature_store_id, name, service, "sync", job_name),
        )

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

    def _tags_path(
        self, storage_connector_instance: storage_connector.StorageConnector, *rest: str
    ) -> list:
        _client = client._get_instance()
        return [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector_instance._featurestore_id,
            "storageconnectors",
            storage_connector_instance.name,
            "tags",
            *rest,
        ]

    def _add_tag(
        self,
        storage_connector_instance: storage_connector.StorageConnector,
        name: str,
        value: Any,
    ) -> None:
        client._get_instance()._send_request(
            "PUT",
            self._tags_path(storage_connector_instance, name),
            headers={"content-type": "application/json"},
            data=tag.Tag._encode(value),
            timeout=60,
        )

    def _delete_tag(
        self, storage_connector_instance: storage_connector.StorageConnector, name: str
    ) -> None:
        client._get_instance()._send_request(
            "DELETE", self._tags_path(storage_connector_instance, name), timeout=60
        )

    @decorators._catch_not_found("hopsworks_common.tag.Tag", fallback_return={})
    def _get_tags_metadata(
        self,
        storage_connector_instance: storage_connector.StorageConnector,
        name: str | None = None,
    ) -> dict[str, tag.Tag]:
        """Get the tags attached to a storage connector, or the one named `name`.

        Returns:
            Dict of tag name to Tag object; empty when the named tag is not attached.
        """
        path_params = (
            self._tags_path(storage_connector_instance)
            if name is None
            else self._tags_path(storage_connector_instance, name)
        )
        return {
            t.name: t
            for t in tag.Tag.from_response_json(
                client._get_instance()._send_request("GET", path_params, timeout=60)
            )
        }

#
#   Copyright 2026 Hopsworks AB
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
from typing import Any

from hopsworks_apigen import public
from hopsworks_common import client, usage


@public("hopsworks.core.trino_catalog_api.TrinoCatalogApi")
class TrinoCatalogApi:
    """Trino catalogs of the current project, and the query engine restart that loads them.

    A catalog makes an external source queryable from the query engine. Creating one records the
    change; it becomes queryable once the query engine restarts, which happens on the cluster's
    schedule or immediately when a cluster administrator asks for it.
    """

    def _project_path(self, *tail: Any) -> list[Any]:
        _client = client._get_instance()
        return ["project", _client._project_id, "trino", *tail]

    def _send_catalog(
        self,
        method: str,
        path: list[Any],
        name: str,
        connector_type: str,
        properties: dict[str, str],
        data_source_name: str | None = None,
        featurestore_id: int | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "name": name,
            "connectorType": connector_type,
            "properties": properties,
        }
        # Sent only when deriving from a data source. Their joint presence is what tells the backend
        # to resolve credential references out of that source; without them the properties are used
        # exactly as given, which is the hand-written case.
        if (data_source_name is None) != (featurestore_id is None):
            raise ValueError(
                "data_source_name and featurestore_id must be passed together: "
                "they name the data source to resolve credentials from."
            )
        if data_source_name is not None:
            payload["dataSourceName"] = data_source_name
            payload["featurestoreId"] = featurestore_id
        _client = client._get_instance()
        return _client._send_request(
            method,
            path,
            headers={"content-type": "application/json"},
            data=json.dumps(payload),
        )

    @public
    @usage._method_logger
    def get_catalogs(self) -> list[dict[str, Any]]:
        """The project's Trino catalogs, plus the cluster's shared default catalogs.

        Returns:
            One entry per catalog: its `name`, `connectorType`, `status`, pending `operation`,
            `creator`, and `defaultCatalog` for a cluster-wide one. Properties are not included;
            use `get_catalog` for those.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        return _client._send_request("GET", self._project_path("catalog"))

    @public
    @usage._method_logger
    def get_catalog(self, name: str) -> dict[str, Any]:
        """One catalog with its properties, for inspection or editing.

        Secret-bearing property values come back masked (`********`). Submitting a masked value
        unchanged through `update_catalog` keeps the stored secret, so a masked property does not
        have to be retyped to change a different one.

        Parameters:
            name: The catalog's full name, including the `<project>__` prefix.

        Returns:
            The catalog: its `name`, `connectorType`, `status`, pending `operation`, and
            `properties` with secret-bearing values masked.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        return _client._send_request("GET", self._project_path("catalog", name))

    @public
    @usage._method_logger
    def get_capabilities(self) -> dict[str, Any]:
        """What this cluster's query engine supports, and when it next restarts.

        Returns:
            `connectors` a catalog may use, `testConnectionAvailable` for whether a live connection
            test is possible, `restarting` while the engine is mid-restart, `nextScheduledRestart`
            as the instant a pending catalog next goes live (null when no restart is scheduled), and
            `mountableSecretsAvailable` for whether credential files can be delivered.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        return _client._send_request("GET", self._project_path("capabilities"))

    @public
    @usage._method_logger
    def get_catalog_template(
        self, data_source_name: str, featurestore_id: int
    ) -> dict[str, Any]:
        """A proposed catalog derived from a data source, to be reviewed and then created.

        Nothing is created or written by asking for this. Credential properties come back as a
        reference to a Hopsworks secret or credential-file bundle rather than a value: the value is
        read from the data source when the catalog is created, so no credential is sent here.

        Parameters:
            data_source_name: Name of the data source (storage connector) to derive from.
            featurestore_id: The feature store the data source belongs to.

        Returns:
            `supported` and, when false, `reason` saying why the source cannot be mapped. When true:
            `suggestedName`, `connectorType`, and `properties`.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        return _client._send_request(
            "GET",
            self._project_path("catalog", "template"),
            query_params={
                "dataSourceName": data_source_name,
                "featurestoreId": featurestore_id,
            },
        )

    @public
    @usage._method_logger
    def create_catalog(
        self,
        name: str,
        connector_type: str,
        properties: dict[str, str],
        data_source_name: str | None = None,
        featurestore_id: int | None = None,
    ) -> dict[str, Any]:
        """Create a Trino catalog.

        The catalog is recorded now and becomes queryable when the query engine restarts. Check
        `get_capabilities()["nextScheduledRestart"]` for when that is, or call `restart()` as a
        cluster administrator to do it immediately.

        Parameters:
            name: Full catalog name. It must start with `<project>__` in lowercase.
            connector_type: A Trino connector from `get_capabilities()["connectors"]`.
            properties: Connector properties, without `connector.name`. A value may reference a
                Hopsworks secret as `${HOPSWORKS_SECRET:<name>}`, or a credential-file bundle as
                `${HOPSWORKS_MOUNT:<bundle>}`, instead of holding the value itself.
            data_source_name: Derive credential properties from this data source, for a catalog
                built from `get_catalog_template`. Leave unset for a hand-written catalog.
            featurestore_id: The data source's feature store. Required with `data_source_name`.

        Returns:
            The created catalog, including the `status` it is waiting in.

        Raises:
            ValueError: If only one of `data_source_name` and `featurestore_id` is passed.
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._send_catalog(
            "POST",
            self._project_path("catalog"),
            name,
            connector_type,
            properties,
            data_source_name,
            featurestore_id,
        )

    @public
    @usage._method_logger
    def update_catalog(
        self,
        name: str,
        connector_type: str,
        properties: dict[str, str],
    ) -> dict[str, Any]:
        """Change an existing catalog's connector or properties.

        Like a create, the change takes effect at the next query engine restart. A property left at
        its masked value keeps the secret already stored for it, which is how an update changes one
        property without retyping the others' credentials.

        Parameters:
            name: The catalog's full name, including the `<project>__` prefix.
            connector_type: The Trino connector the catalog uses.
            properties: The full set of connector properties to store, as from `get_catalog`.

        Returns:
            The updated catalog, including the `status` it is waiting in.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        return self._send_catalog(
            "PUT",
            self._project_path("catalog", name),
            name,
            connector_type,
            properties,
        )

    @public
    @usage._method_logger
    def delete_catalog(self, name: str) -> None:
        """Delete a catalog.

        The catalog stays queryable until the query engine restarts and unloads it.

        Parameters:
            name: The catalog's full name, including the `<project>__` prefix.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        _client._send_request("DELETE", self._project_path("catalog", name))

    @public
    @usage._method_logger
    def test_connection(
        self,
        name: str,
        connector_type: str,
        properties: dict[str, str],
        data_source_name: str | None = None,
        featurestore_id: int | None = None,
    ) -> None:
        """Check that a catalog definition can actually reach its source, without creating it.

        Available only where `get_capabilities()["testConnectionAvailable"]` is true, which needs the
        cluster's optional test coordinator. Returns nothing on success and raises with the engine's
        own message on failure, so a bad host or credential is reported before the catalog exists.

        Parameters:
            name: Full catalog name, as it would be created.
            connector_type: A Trino connector from `get_capabilities()["connectors"]`.
            properties: The connector properties to test, as they would be stored.
            data_source_name: Resolve credential properties from this data source, exactly as
                `create_catalog` would. Leave unset for a hand-written catalog.
            featurestore_id: The data source's feature store. Required with `data_source_name`.

        Raises:
            ValueError: If only one of `data_source_name` and `featurestore_id` is passed.
            hopsworks.client.exceptions.RestAPIError: If the connection fails, or the backend
                encounters an error when handling the request.
        """
        self._send_catalog(
            "POST",
            self._project_path("catalog", "test-connection"),
            name,
            connector_type,
            properties,
            data_source_name,
            featurestore_id,
        )

    @public
    @usage._method_logger
    def restart(self) -> dict[str, Any]:
        """Apply every pending catalog change now, instead of waiting for the schedule.

        Requires cluster administrator (`HOPS_ADMIN`) privileges; the backend refuses otherwise.
        This interrupts queries running anywhere on the cluster, so prefer waiting for
        `get_capabilities()["nextScheduledRestart"]` unless the change is needed sooner.

        Pending changes are written and the query engine is then restarted, in that order: a restart
        on its own would load nothing, because a newly created catalog is only a record until it is
        written.

        Returns:
            `restarted` false when the restart was skipped as unnecessary, and `quarantined` naming
            any catalog removed because the engine could not load it. A quarantined catalog is not
            applied; its own page carries the load error.

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the user is not a cluster administrator, or
                the backend encounters an error when handling the request.
        """
        _client = client._get_instance()
        return _client._send_request(
            "POST", ["admin", "trino", "catalogs", "sync-and-restart"]
        )

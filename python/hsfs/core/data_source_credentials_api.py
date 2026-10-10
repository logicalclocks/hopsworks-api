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
from typing import TYPE_CHECKING, Any

from hopsworks_common import client


if TYPE_CHECKING:
    from hsfs import storage_connector as sc


def _not_provided_message(name: str, status: str | None = None) -> str:
    """Why a data source with provided credentials cannot be used by the caller yet."""
    if status == "INCOMPLETE":
        return (
            f"Your credentials for data source '{name}' reference a secret or env var "
            "that no longer exists. Call `set_credentials()` on the data source again "
            f"(or `hops datasource credentials set {name}`)."
        )
    return (
        f"You have not added your credentials for data source '{name}'. Call "
        "`set_credentials()` on the data source first "
        f"(or `hops datasource credentials set {name}`)."
    )


class DataSourceCredentialsApi:
    """The calling member's own credentials for a data source with provided credentials."""

    def _path(self, storage_connector: sc.StorageConnector, *tail: str) -> list[Any]:
        _client = client._get_instance()
        return [
            "project",
            _client._project_id,
            "featurestores",
            storage_connector._featurestore_id,
            "storageconnectors",
            storage_connector._name,
            "credentials",
            *tail,
        ]

    def _get(self, storage_connector: sc.StorageConnector) -> dict[str, Any]:
        """The caller's binding, or one with status MISSING when there is none."""
        _client = client._get_instance()
        return _client._send_request("GET", self._path(storage_connector))

    def _validate(
        self, storage_connector: sc.StorageConnector, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        """Test the credentials against the data source without storing them."""
        _client = client._get_instance()
        return _client._send_request(
            "POST",
            self._path(storage_connector, "validate"),
            headers={"content-type": "application/json"},
            data=json.dumps(credentials),
        )

    def _set(
        self, storage_connector: sc.StorageConnector, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate, store the account entries and bind them to the data source."""
        _client = client._get_instance()
        return _client._send_request(
            "PUT",
            self._path(storage_connector),
            headers={"content-type": "application/json"},
            data=json.dumps(credentials),
        )

    def _delete(self, storage_connector: sc.StorageConnector) -> None:
        _client = client._get_instance()
        _client._send_request("DELETE", self._path(storage_connector))

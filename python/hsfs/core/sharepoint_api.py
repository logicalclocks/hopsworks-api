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

from hopsworks_common import client


_JSON = {"content-type": "application/json"}


def _items(response: Any) -> list[dict[str, Any]]:
    """The elements of a collection response, which the backend sends as a list or as `{"items": [...]}`."""
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        return response.get("items") or []
    return []


class SharePointApi:
    """The `storageconnectors/{name}/sharepoint` sub-resource of a SharePoint data source.

    The claim and result endpoints are left out: only the sync script calls them.
    """

    def _path(self, featurestore_id: int, connector_name: str, *tail: Any) -> list:
        _client = client._get_instance()
        return [
            "project",
            _client._project_id,
            "featurestores",
            featurestore_id,
            "storageconnectors",
            connector_name,
            "sharepoint",
            *tail,
        ]

    def _send(self, method: str, path: list, **kwargs: Any) -> Any:
        return client._get_instance()._send_request(method, path, **kwargs)

    def _get_drives(
        self, featurestore_id: int, connector_name: str
    ) -> list[dict[str, Any]]:
        return _items(
            self._send("GET", self._path(featurestore_id, connector_name, "drives"))
        )

    def _get_folders(
        self, featurestore_id: int, connector_name: str, drive_id: str, path: str
    ) -> list[dict[str, Any]]:
        return _items(
            self._send(
                "GET",
                self._path(
                    featurestore_id, connector_name, "drives", drive_id, "folders"
                ),
                query_params={"path": path},
            )
        )

    def _reset_script(self, featurestore_id: int, connector_name: str) -> Any:
        return self._send(
            "POST", self._path(featurestore_id, connector_name, "script", "reset")
        )

    def _get_syncs(
        self, featurestore_id: int, connector_name: str
    ) -> list[dict[str, Any]]:
        return _items(
            self._send(
                "GET",
                self._path(featurestore_id, connector_name, "syncs"),
                query_params={"expand": ["job", "dataset", "schedule"]},
            )
        )

    def _get_sync(
        self, featurestore_id: int, connector_name: str, sync_id: int
    ) -> dict[str, Any]:
        return self._send(
            "GET", self._path(featurestore_id, connector_name, "syncs", sync_id)
        )

    def _create_sync(
        self, featurestore_id: int, connector_name: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        return self._send(
            "POST",
            self._path(featurestore_id, connector_name, "syncs"),
            headers=_JSON,
            data=json.dumps(body),
        )

    def _update_sync(
        self,
        featurestore_id: int,
        connector_name: str,
        sync_id: int,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        return self._send(
            "PUT",
            self._path(featurestore_id, connector_name, "syncs", sync_id),
            headers=_JSON,
            data=json.dumps(body),
        )

    def _delete_sync(
        self,
        featurestore_id: int,
        connector_name: str,
        sync_id: int,
        keep_files: bool,
        force: bool,
    ) -> Any:
        return self._send(
            "DELETE",
            self._path(featurestore_id, connector_name, "syncs", sync_id),
            query_params={"keepFiles": keep_files, "force": force},
        )

    def _sync_action(
        self,
        featurestore_id: int,
        connector_name: str,
        sync_id: int,
        action: str,
        query_params: dict[str, Any] | None = None,
    ) -> Any:
        """POST one of the sync's action endpoints: `retry`, `run`, `pause`, `resume` or `resync`."""
        return self._send(
            "POST",
            self._path(featurestore_id, connector_name, "syncs", sync_id, action),
            query_params=query_params or {},
        )

    def _get_audit(
        self,
        featurestore_id: int,
        connector_name: str,
        sync_id: int,
        day: str | None,
        offset: int,
        limit: int,
    ) -> Any:
        query_params: dict[str, Any] = {"offset": offset, "limit": limit}
        if day is not None:
            query_params["day"] = day
        return self._send(
            "GET",
            self._path(featurestore_id, connector_name, "syncs", sync_id, "audit"),
            query_params=query_params,
        )

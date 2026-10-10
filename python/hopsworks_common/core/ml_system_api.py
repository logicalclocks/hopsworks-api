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
"""The project's ML systems registry (`/project/{id}/mlsystems`), used by `hops factory system`."""

from __future__ import annotations

import json

from hopsworks_common import client


def _path(*rest: str | int) -> list[str | int]:
    _client = client._get_instance()
    return ["project", _client._project_id, "mlsystems", *rest]


PAGE = 100


def _register(
    path_to_code: str,
    name: str | None = None,
    factory: str | None = None,
    factory_version: int | None = None,
    factory_digest: str | None = None,
) -> dict:
    """Register the system whose code is at `path_to_code`, or refresh it; returns its id, name, path and factory.

    `path_to_code` is a HopsFS directory in the project (absolute, or relative to the project
    root) or a Git repository URL.
    `factory` names the factory that built it; the backend takes mlsystem for a new system without one, and keeps the factory of a registered one.
    `factory_version` is the version of the factory's definition the system was generated from, and `factory_digest` the sha256 of that definition's YAML text; the backend refuses either when it does not match what it stores.
    Without them a new system records the factory's current version and a refreshed one keeps what it recorded.
    """
    body: dict = {"pathToCode": path_to_code}
    if name:
        body["name"] = name
    if factory:
        body["factory"] = factory
    if factory_version is not None:
        body["factoryVersion"] = factory_version
    if factory_digest:
        body["factoryDigest"] = factory_digest
    return client._get_instance()._send_request(
        "POST",
        _path(),
        headers={"content-type": "application/json"},
        data=json.dumps(body),
    )


def _page(factory: str | None = None, offset: int = 0, limit: int = PAGE) -> dict:
    """One page of the project's systems, or one factory's, most recently updated first: `{"items": [...], "count": N}`.

    Each item says whether the caller can open its code (`accessible`) and whether its directory could be checked (`available`).
    `limit=0` returns only the count.
    """
    query: dict = {"offset": offset, "limit": limit}
    if factory:
        query["factory"] = factory
    return client._get_instance()._send_request("GET", _path(), query_params=query)


def _list(factory: str | None = None) -> list[dict]:
    """Every system of the project, or of one factory, read page by page."""
    items: list[dict] = []
    while True:
        page = _page(factory, offset=len(items))
        batch = page.get("items") or []
        items.extend(batch)
        count = page.get("count")
        if len(batch) < PAGE or (count is not None and len(items) >= count):
            return items


def _get(system_id: int) -> dict:
    """One registered system by its id."""
    return client._get_instance()._send_request("GET", _path(system_id))


def _remove(system_id: int) -> None:
    """Remove a system from the registry; its code stays where it is."""
    client._get_instance()._send_request("DELETE", _path(system_id))

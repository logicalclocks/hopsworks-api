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
"""The project's ML systems registry (`/project/{id}/mlsystems`), used by `hops factory mlsystem`."""

from __future__ import annotations

import json

from hopsworks_common import client


def _path(*rest: str | int) -> list[str | int]:
    _client = client._get_instance()
    return ["project", _client._project_id, "mlsystems", *rest]


def _register(
    path_to_code: str, name: str | None = None, factory: str | None = None
) -> dict:
    """Register the system whose code is at `path_to_code`, or refresh it; returns its id, name, path and factory.

    `path_to_code` is a HopsFS directory in the project (absolute, or relative to the project
    root) or a Git repository URL.
    `factory` names the factory that built it; the backend takes mlsystem for a new system without one, and keeps the factory of a registered one.
    """
    body = {"pathToCode": path_to_code}
    if name:
        body["name"] = name
    if factory:
        body["factory"] = factory
    return client._get_instance()._send_request(
        "POST",
        _path(),
        headers={"content-type": "application/json"},
        data=json.dumps(body),
    )


def _list(factory: str | None = None) -> list[dict]:
    """The project's systems, or one factory's, most recently updated first, each with whether the caller can open its code."""
    query = {"factory": factory} if factory else None
    return (
        client._get_instance()
        ._send_request("GET", _path(), query_params=query)
        .get("items", [])
    )


def _remove(system_id: int) -> None:
    """Remove a system from the registry; its code stays where it is."""
    client._get_instance()._send_request("DELETE", _path(system_id))

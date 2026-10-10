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
"""The project's software factories (`/project/{id}/factories`), used by `hops factory`."""

from __future__ import annotations

import json

from hopsworks_common import client


YAML = {"content-type": "application/yaml"}


def _path(*rest: str | int) -> list[str | int]:
    _client = client._get_instance()
    return ["project", _client._project_id, "factories", *rest]


def _list() -> list[dict]:
    """The built-in factories and the project's own, each with its current definition and system count."""
    return client._get_instance()._send_request("GET", _path()).get("items", [])


def _get(name: str, version: int | None = None) -> dict:
    """One factory, with its definition at `version` or at its current version."""
    query = {"version": version} if version else None
    return client._get_instance()._send_request("GET", _path(name), query_params=query)


def _create(definition: str, name: str | None = None) -> dict:
    """A new project factory from YAML; `name` replaces the definition's name."""
    query = {"name": name} if name else None
    return client._get_instance()._send_request(
        "POST",
        _path(),
        query_params=query,
        headers=YAML,
        data=definition.encode("utf-8"),
    )


def _update(name: str, definition: str) -> dict:
    """A new version of a project factory's definition."""
    return client._get_instance()._send_request(
        "PUT", _path(name), headers=YAML, data=definition.encode("utf-8")
    )


def _set_enabled(name: str, enabled: bool) -> dict:
    return client._get_instance()._send_request(
        "PATCH",
        _path(name),
        headers={"content-type": "application/json"},
        data=json.dumps({"enabled": enabled}),
    )


def _delete(name: str) -> None:
    """Delete a project factory; the cluster refuses while it has systems."""
    client._get_instance()._send_request("DELETE", _path(name))

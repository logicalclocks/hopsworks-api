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
"""Stable internal REST helper for SDK consumers (CLI and ``core/<entity>_api`` modules).

The Hopsworks Python SDK already has an authenticated HTTP client at
``hopsworks_common.client.get_instance()``, but its surface
(``_send_request``, ``_project_id``) is private — the underscore prefix means
callers shouldn't depend on it. Multiple call sites (the ``hops`` CLI's
``fv list``, ``connector list``, ``model list``, …) reach through anyway
because the SDK's per-entity wrappers don't yet cover every endpoint.

This module exposes the bare minimum stable surface for those call sites:

* :func:`send_request` — perform a request against ``/hopsworks-api/api/...``
  with shared host/auth/TLS configuration.
* :func:`project_path` — build a ``project/<id>/...`` path tuple so callers
  don't reach into ``_project_id`` directly.

Internal SDK consumers should import from here rather than ``client.get_instance()``
directly so the underlying HTTP machinery can change shape without breaking
external callers.
"""

from __future__ import annotations

import json as _json
from typing import Any

from hopsworks_apigen import public
from hopsworks_common import client


@public
def project_path(*tail: Any) -> list[Any]:
    """Build a ``["project", <project_id>, *tail]`` path list.

    Args:
        *tail: Path components to append after ``project/<id>``. Numeric IDs
            are accepted alongside strings — :func:`send_request` URL-encodes
            them.

    Returns:
        A list ready to pass as ``path_params`` to :func:`send_request`.

    Raises:
        RuntimeError: When the SDK is not connected (``hopsworks.login``
            has not been called yet).
    """
    instance = client.get_instance()
    project_id = getattr(instance, "_project_id", None)
    if project_id is None:
        raise RuntimeError(
            "No active Hopsworks project. Call hopsworks.login() before "
            "issuing project-scoped REST calls."
        )
    return ["project", project_id, *tail]


@public
def send_request(
    method: str,
    path_params: list[Any],
    query_params: dict[str, Any] | None = None,
    json_body: Any | None = None,
) -> Any:
    """Issue an authenticated request to ``/hopsworks-api/api/<path_params>``.

    Wraps the SDK's internal client so per-call sites don't reach into
    ``_send_request`` directly. Auth, host normalization, TLS verification
    and base URL construction are all inherited from the active
    :func:`hopsworks_common.client.get_instance` instance — the same one
    the rest of the SDK uses.

    Args:
        method: HTTP method (``"GET"``, ``"POST"``, ``"PUT"``, ``"DELETE"``).
        path_params: List of path components (URL-encoded by the client).
        query_params: Optional query string parameters.
        json_body: Optional Python object to serialize as the JSON request
            body. The ``content-type: application/json`` header is set
            automatically.

    Returns:
        The JSON-decoded response body, or whatever the client's
        ``_send_request`` returns for non-JSON / empty responses.

    Raises:
        hopsworks_common.client.exceptions.RestAPIError: When the backend
            returns a non-2xx response.
    """
    instance = client.get_instance()
    headers = None
    data = None
    if json_body is not None:
        headers = {"content-type": "application/json"}
        data = _json.dumps(json_body)
    return instance._send_request(
        method,
        path_params,
        query_params=query_params,
        headers=headers,
        data=data,
    )

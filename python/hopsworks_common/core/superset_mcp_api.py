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
"""Calls tools on Superset's MCP service as the project's Superset user.

The service speaks MCP over streamable HTTP and accepts a bearer token Hopsworks mints per user and project, so the tools run with that user's Superset roles.
A few JSON-RPC requests are all the three tools need, so this does not depend on an MCP client library.
"""

from __future__ import annotations

import itertools
import json
import time
from typing import Any

import requests
from hopsworks_common import client
from hopsworks_common.client.exceptions import SupersetException


SUPERSET_MCP_SERVICE_NAME = "app.superset-mcp.service"
SUPERSET_MCP_PORT = 5008
_MCP_PATH = "/mcp"
_PROTOCOL_VERSION = "2025-06-18"
_TIMEOUT_SECONDS = 120
# Fetch a new token this long before the backend's expiry, so a call never starts on a token about to lapse.
_TOKEN_MARGIN_SECONDS = 60


class _SupersetMcpClient:
    """Calls tools on the cluster's Superset MCP service for one project."""

    def __init__(self, project_id: int, service_discovery_domain: str):
        self._project_id = project_id
        self._url = (
            f"http://{SUPERSET_MCP_SERVICE_NAME}.{service_discovery_domain}"
            f":{SUPERSET_MCP_PORT}{_MCP_PATH}"
        )
        self._token: str | None = None
        self._token_deadline = 0.0
        self._ids = itertools.count(1)
        self._http = requests.Session()

    def _call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Run `tool` with `arguments` and return its result.

        Raises:
            hopsworks.client.exceptions.SupersetException: When the service is not deployed, refuses the token, or the tool reports an error.
        """
        headers = {
            "Authorization": f"Bearer {self._bearer()}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        _, session_headers = self._rpc(
            headers,
            "initialize",
            {
                "protocolVersion": _PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "hopsworks", "version": "1"},
            },
        )
        headers = {**headers, "Mcp-Protocol-Version": _PROTOCOL_VERSION}
        # A stateless server answers without a session id; a stateful one needs it on every later request.
        if "mcp-session-id" in session_headers:
            headers["Mcp-Session-Id"] = session_headers["mcp-session-id"]
        self._post(
            headers,
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
        )
        result, _ = self._rpc(
            headers, "tools/call", {"name": tool, "arguments": arguments}
        )
        return _tool_result(tool, result)

    def _bearer(self) -> str:
        if self._token is None or time.monotonic() >= self._token_deadline:
            response = client._get_instance()._send_request(
                "GET",
                ["project", self._project_id, "superset", "mcp", "token"],
                timeout=_TIMEOUT_SECONDS,
            )
            self._token = response["token"]
            self._token_deadline = (
                time.monotonic()
                + int(response.get("expiresInSeconds", 600))
                - _TOKEN_MARGIN_SECONDS
            )
        return self._token

    def _rpc(
        self, headers: dict[str, str], method: str, params: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        request_id = next(self._ids)
        response = self._post(
            headers,
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
        )
        message = _response_message(response, request_id)
        if "error" in message:
            raise SupersetException(
                f"Superset MCP {method} failed: {message['error'].get('message', message['error'])}"
            )
        return message.get("result", {}), {
            k.lower(): v for k, v in response.headers.items()
        }

    def _post(self, headers: dict[str, str], body: dict[str, Any]) -> requests.Response:
        try:
            response = self._http.post(
                self._url, headers=headers, json=body, timeout=_TIMEOUT_SECONDS
            )
        except requests.ConnectionError as e:
            raise SupersetException(
                f"Superset's MCP service is not reachable at {self._url}: {e}"
            ) from e
        except requests.Timeout as e:
            raise SupersetException(
                f"Superset's MCP service at {self._url} did not answer within {_TIMEOUT_SECONDS} s: {e}"
            ) from e
        if response.status_code == 401:
            self._token = None
            raise SupersetException(
                "Superset's MCP service refused the Hopsworks token."
            )
        if response.status_code >= 400:
            raise SupersetException(
                f"Superset's MCP service answered {response.status_code}: {response.text[:500]}"
            )
        return response


def _response_message(response: requests.Response, request_id: int) -> dict[str, Any]:
    """The JSON-RPC response with `request_id`, from a JSON body or a server-sent event stream."""
    if response.headers.get("content-type", "").startswith("text/event-stream"):
        for line in response.text.splitlines():
            if not line.startswith("data:"):
                continue
            message = json.loads(line[len("data:") :].strip())
            if message.get("id") == request_id:
                return message
        raise SupersetException(
            f"Superset's MCP service sent no response to request {request_id}."
        )
    return response.json()


def _tool_result(tool: str, result: dict[str, Any]) -> dict[str, Any]:
    """The tool's own payload: structured content when sent, else the JSON in its text content."""
    text = "\n".join(
        part.get("text", "")
        for part in result.get("content", [])
        if part.get("type") == "text"
    )
    if result.get("isError"):
        raise SupersetException(f"Superset {tool} failed: {text or result}")
    if isinstance(result.get("structuredContent"), dict):
        payload = result["structuredContent"]
    else:
        try:
            payload = json.loads(text) if text else {}
        except json.JSONDecodeError:
            payload = {"text": text}
    # The tools report refusals in the payload rather than as MCP errors.
    error = payload.get("error") if isinstance(payload, dict) else None
    if error or (isinstance(payload, dict) and payload.get("success") is False):
        if isinstance(error, dict):
            error = error.get("message") or error.get("details") or json.dumps(error)
        raise SupersetException(f"Superset {tool} failed: {error or payload}")
    return payload

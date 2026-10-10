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
"""The agent's own line back to Hopsworks.

A deployment runs with the platform's address and a credential of its own in
the environment (``REST_ENDPOINT``, ``HOPSWORKS_PROJECT_ID``, ``DEPLOYMENT_ID``,
and the key the hopsworks client reads), which is how an agent reaches the
feature store from inside the cluster. This module uses the same to relay what
only the agent's callers know: an end user's verdict on a turn, posted to the
agent by a chat UI that has the gateway and nothing else.

Everything is imported lazily and only on the feedback route, so an agent that
never receives feedback never loads the hopsworks client.
"""

from __future__ import annotations

import json
import os
from typing import Any

from .models import AgentError


class PlatformClient:
    """Hopsworks as the deployment sees it. One per app, connected on first use."""

    def __init__(self) -> None:
        self._client: Any = None

    @property
    def configured(self) -> bool:
        return bool(os.environ.get("REST_ENDPOINT")) and bool(self.deployment_id)

    @property
    def deployment_id(self) -> str:
        return os.environ.get("DEPLOYMENT_ID", "")

    @property
    def project_id(self) -> str:
        return os.environ.get("HOPSWORKS_PROJECT_ID") or os.environ.get(
            "PROJECT_ID", ""
        )

    def post_feedback(self, trace_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Record a verdict on one of this deployment's traces; the platform's row comes back."""
        return self._request(
            "POST",
            ["otel", "servings", self.deployment_id, "traces", trace_id, "feedback"],
            body,
        )

    def latest_trace_id(self, conversation_id: str) -> str | None:
        """The newest trace of a conversation, as the platform recorded it."""
        rows = self._request(
            "GET",
            [
                "otel",
                "servings",
                self.deployment_id,
                "traces",
                "sessions",
                conversation_id,
            ],
            None,
            params={"limit": 1},
        )
        items = rows.get("items") if isinstance(rows, dict) else rows
        if not items:
            return None
        newest = max(items, key=lambda row: int(row.get("startTimeNs") or 0))
        return newest.get("traceId") or None

    # ── plumbing ────────────────────────────────────────────────────────────

    def _connect(self) -> Any:
        if self._client is None:
            if not self.configured:
                raise AgentError(
                    "this agent is not running as a Hopsworks deployment, so it has nowhere "
                    "to record feedback",
                    code="platform_unavailable",
                    status_code=503,
                )
            from hopsworks_common import client as hopsworks_client  # noqa: PLC0415

            try:
                self._client = hopsworks_client._get_instance()
            except Exception:  # noqa: BLE001 -- nothing connected yet: connect as the deployment
                hopsworks_client.init("hopsworks")
                self._client = hopsworks_client._get_instance()
        return self._client

    def _request(
        self,
        method: str,
        segments: list[str],
        body: dict[str, Any] | None,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        from hopsworks_common.client.exceptions import RestAPIError  # noqa: PLC0415

        client = self._connect()
        try:
            return client._send_request(
                method,
                ["project", self.project_id, *segments],
                query_params=params,
                headers={"content-type": "application/json"}
                if body is not None
                else None,
                data=None if body is None else json.dumps(body),
                timeout=30,
            )
        except RestAPIError as err:
            response = err.response
            message = _message(response)
            status = response.status_code if response is not None else 502
            raise AgentError(
                message,
                code="platform_refused",
                status_code=status if status >= 400 else 502,
            ) from err


def _message(response: Any) -> str:
    try:
        body = response.json()
    except Exception:  # noqa: BLE001 -- a body that is not JSON still has a status
        return f"Hopsworks returned HTTP {getattr(response, 'status_code', '?')}"
    if isinstance(body, dict):
        return str(
            body.get("usrMsg")
            or body.get("errorMsg")
            or f"Hopsworks returned HTTP {response.status_code}"
        )
    return f"Hopsworks returned HTTP {response.status_code}"

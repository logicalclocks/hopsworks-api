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
"""Calls to the app of an `ontology` factory system, through the Hopsworks app proxy.

The app serves the Auto-Ontology HTTP API under `/api` at `/hopsworks-api/pythonapp/<project>/<app>/`.
The proxy admits a client that holds an API key or a JWT when it sends a proxy token, minted for that app, as the `proxy_session` cookie; the proxy then signs the caller's username for the app, which decides what the caller's project role may do.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import requests
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


_logger = logging.getLogger(__name__)

MAX_ROWS = 100
_PROXY_COOKIE = "proxy_session"
_SSE_DATA = "data:"
_SSE_DONE = "[DONE]"
# A cached token is minted again this long before the backend's expiry.
_TOKEN_MARGIN_MS = 60_000
# The app sends an SSE comment every 0.25 s while it works, so a silence this long is a dead connection.
_READ_TIMEOUT_S = 120.0

_STATUS_PATH = "api/semantic-compilation/status"
_SOURCES_PATH = "api/sources"
_DATABASES_PATH = "api/datasources/dbs"

_tokens: dict[tuple[Any, str], tuple[str, int]] = {}


class _Unreadable:
    """A readiness probe the caller may not read, or the app does not serve."""

    __slots__ = ()


_UNVERIFIED = _Unreadable()


def _proxy_token(app_name: str) -> str:
    """A proxy token for the app, reused until shortly before it expires.

    Raises:
        hopsworks.client.exceptions.RestAPIError: The app does not exist, the caller is not a Data owner or Data scientist of the project, or the cluster has `hopsable_proxy_enabled` off.
    """
    _client = client._get_instance()
    key = (_client._project_id, app_name)
    cached = _tokens.get(key)
    if cached and cached[1] - _TOKEN_MARGIN_MS > time.time() * 1000:
        return cached[0]
    minted = _client._send_request(
        "POST", ["project", _client._project_id, "apps", app_name, "proxy-token"]
    )
    _tokens[key] = (minted["token"], int(minted.get("expiresAt") or 0))
    return minted["token"]


def _url(app_name: str, path: str) -> str:
    _client = client._get_instance()
    return "/".join(
        [
            _client._base_url.rstrip("/"),
            "hopsworks-api/pythonapp",
            quote(str(_client._project_name), safe=""),
            quote(app_name, safe=""),
            path.lstrip("/"),
        ]
    )


def _request(
    method: str,
    app_name: str,
    path: str,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    stream: bool = False,
    timeout: float | tuple[float, float] = 30.0,
) -> requests.Response:
    """Send one request to the app; only the proxy token goes with it, never the caller's API key or JWT.

    Raises:
        hopsworks.client.exceptions.RestAPIError: The proxy or the app answered with a status other than 2xx.
    """
    url = _url(app_name, path)
    response = requests.request(
        method,
        url,
        params={k: v for k, v in (params or {}).items() if v is not None} or None,
        json=body,
        headers={"Accept": "text/event-stream"} if stream else None,
        cookies={_PROXY_COOKIE: _proxy_token(app_name)},
        verify=client._get_instance()._verify,
        stream=stream,
        timeout=timeout,
    )
    if response.status_code // 100 != 2:
        raise RestAPIError(url, response)
    return response


def _get(app_name: str, path: str, params: dict[str, Any] | None = None) -> Any:
    return _request("GET", app_name, path, params=params).json()


def _post(app_name: str, path: str, body: dict[str, Any]) -> Any:
    return _request("POST", app_name, path, body=body).json()


def _ask(
    app_name: str,
    question: str,
    conversation_id: str | None = None,
    evidence: str | None = None,
    timeout: float = 900.0,
) -> Iterator[dict[str, Any]]:
    """Run one text-to-SQL turn and yield its SSE events as they arrive.

    Without `conversation_id` the turn is stateless: no history, nothing saved, and no `charts` event.

    Raises:
        hopsworks.client.exceptions.RestAPIError: The app refused the question, for example 403 for a role without chat or 409 when the ontology is not compiled or the conversation has a turn running.
        TimeoutError: The run took longer than `timeout` seconds.
    """
    body: dict[str, Any] = {"question": question}
    if conversation_id:
        body["conversation_id"] = conversation_id
    if evidence:
        body["evidence"] = evidence
    deadline = time.monotonic() + timeout
    response = _request(
        "POST",
        app_name,
        "api/chat/completions",
        body=body,
        stream=True,
        timeout=(30.0, min(timeout, _READ_TIMEOUT_S)),
    )
    response.encoding = "utf-8"

    def lines() -> Iterator[str]:
        for line in response.iter_lines(chunk_size=None, decode_unicode=True):
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"{app_name} did not answer within {timeout:.0f} seconds"
                )
            yield line

    with response:
        yield from _events(lines())


def _events(lines: Iterable[str]) -> Iterator[dict[str, Any]]:
    """The JSON objects on the `data:` lines of an SSE stream, up to `data: [DONE]`.

    Comments (`: ping`), blank lines and payloads that are not a JSON object are skipped: losing one progress event is better than losing the answer.
    """
    for line in lines:
        if not line.startswith(_SSE_DATA):
            continue
        payload = line[len(_SSE_DATA) :].strip()
        if payload == _SSE_DONE:
            return
        if not payload:
            continue
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            _logger.warning("Skipping an SSE payload that is not JSON: %.200s", payload)
            continue
        if isinstance(event, dict):
            yield event


def _rows(raw: Any) -> list[dict[str, Any]]:
    # The agent sends the records themselves, or a one-item list holding them as a JSON string.
    if isinstance(raw, list) and len(raw) == 1 and isinstance(raw[0], str):
        try:
            raw = json.loads(raw[0])
        except json.JSONDecodeError:
            return []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [row for row in raw if isinstance(row, dict)]


def _answer(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Fold the events of one turn into its answer.

    Returns:
        `answer`, `sql`, `rows` (at most `MAX_ROWS`), `row_count` (before the cut), `truncated`, `reasoning` (one line per step) and `chart` (the `charts` event's content, or None).

    Raises:
        RuntimeError: The run reported an error, or ended without a result.
    """
    steps: list[str] = []
    sql = ""
    result: dict[str, Any] | None = None
    chart = None
    for event in events:
        kind = event.get("type")
        if kind == "step":
            steps.append(_step_line(event))
        elif kind == "sql":
            sql = str(event.get("sql") or "")
        elif kind == "result":
            result = event.get("answer") or {}
        elif kind == "charts":
            chart = event.get("content")
        elif kind == "error":
            raise RuntimeError(
                f"the ontology agent failed: {event.get('message') or 'unknown error'}"
            )
    if result is None:
        raise RuntimeError(
            "the run ended without an answer: it was cancelled, or the question does not map to the data"
        )
    # A successful run puts its rows under sql_response_from_db; `result` only in the failure shape.
    rows = _rows(result.get("sql_response_from_db") or result.get("result"))
    return {
        "answer": str(result.get("response") or ""),
        "sql": str(result.get("sql_code") or sql),
        "rows": rows[:MAX_ROWS],
        "row_count": len(rows),
        "truncated": len(rows) > MAX_ROWS,
        "reasoning": str(result.get("thoughts") or "\n".join(steps)),
        "chart": chart,
    }


def _step_line(event: dict[str, Any]) -> str:
    label = event.get("label") or event.get("node") or "working"
    thought = event.get("thought")
    return f"{label}: {thought}" if thought else str(label)


def _refusal(exc: RestAPIError, app_name: str) -> str | None:
    """What a refusal by the app or its proxy means for the caller, or None when Hopsworks refused to mint the proxy token, whose own error says why."""
    if "/proxy-token" in str(getattr(exc, "url", "")):
        return None
    status = getattr(exc.response, "status_code", None)
    detail = _detail(exc.response)
    if status == RestAPIError.STATUS_CODE_FORBIDDEN:
        return (
            f"{app_name} refused: your role in the project cannot use this. "
            f"Observers and Feature store restricted members read the catalog in the app and get no chat. ({detail})"
        )
    if status == RestAPIError.STATUS_CODE_NOT_FOUND:
        return f"{app_name} has no such resource, or is not an app of this project: {detail}"
    if status == 409:
        return f"{app_name} cannot answer right now: {detail}"
    if status in (502, 503, 504):
        return f"{app_name} is not serving (HTTP {status}); start it with `hops app start {app_name}`"
    return f"{app_name} answered HTTP {status}: {detail}"


def _detail(response: Any) -> str:
    try:
        body = response.json()
    except ValueError:
        return (getattr(response, "text", "") or "").strip()[:300]
    if isinstance(body, dict):
        return str(
            body.get("detail") or body.get("error") or body.get("usrMsg") or body
        )
    return str(body)


def _probe(app_name: str, path: str) -> Any:
    """GET `path` for a readiness check: its JSON, `_UNVERIFIED` when refused (403) or not served (404), or None when unreadable.

    Raises:
        hopsworks.client.exceptions.RestAPIError: The proxy refused the credentials (401), which every probe would hit alike.
    """
    try:
        return _get(app_name, path)
    except RestAPIError as exc:
        status = getattr(exc.response, "status_code", None)
        if status == RestAPIError.STATUS_CODE_UNAUTHORIZED:
            raise
        if status in (
            RestAPIError.STATUS_CODE_FORBIDDEN,
            RestAPIError.STATUS_CODE_NOT_FOUND,
        ):
            return _UNVERIFIED
        _logger.warning("Readiness probe %s answered HTTP %s", path, status)
        return None
    except (requests.RequestException, ValueError) as exc:
        _logger.warning("Readiness probe %s failed: %s", path, exc)
        return None


def _readiness(app_name: str) -> dict[str, Any]:
    """Whether the app can answer questions: a compiled ontology, a source to run SQL on, and an ingested catalog.

    A fact the caller may not read is listed under `unverified`, not as a blocker.

    Returns:
        `ready`, `ontology_compiled`, `can_execute_sql`, `catalog_present` (each None when unverified), `databases`, `blockers` and `unverified`.
    """
    return _summarise(
        _probe(app_name, _STATUS_PATH),
        _probe(app_name, _SOURCES_PATH),
        _probe(app_name, _DATABASES_PATH),
    )


def _summarise(status: Any, sources: Any, databases: Any) -> dict[str, Any]:
    blockers: list[str] = []
    unverified: list[str] = []

    compiled: bool | None
    if status is _UNVERIFIED:
        blockers.append(
            "Your role in the project cannot use chat in this app, which runs SQL on the data sources; "
            "only Data owners and Data scientists can ask questions."
        )
        compiled = None
    elif status is None:
        blockers.append(
            f"The compilation status ({_STATUS_PATH}) could not be read; the app may still be starting."
        )
        compiled = False
    else:
        compiled = bool(status.get("calculated"))
        if not compiled:
            blockers.append(
                "The ontology is not compiled yet: run the system's compile job, or Compile in the app."
            )

    can_execute: bool | None
    if sources is _UNVERIFIED:
        unverified.append(
            f"Whether a data source is configured to run SQL on ({_SOURCES_PATH}) could not be checked."
        )
        can_execute = None
    elif sources is None:
        blockers.append(
            f"The configured data sources ({_SOURCES_PATH}) could not be read."
        )
        can_execute = False
    else:
        can_execute = bool(sources)
        if not can_execute:
            blockers.append(
                "No data source is configured, so generated SQL has nothing to run on: add one on the app's Sources page."
            )

    names: list[str] = []
    catalog: bool | None
    if databases is _UNVERIFIED:
        unverified.append(
            f"Whether any database is ingested ({_DATABASES_PATH}) could not be checked."
        )
        catalog = None
    elif databases is None:
        blockers.append(f"The catalog ({_DATABASES_PATH}) could not be read.")
        catalog = False
    else:
        names = [
            str(entry.get("name"))
            for entry in (databases.get("data") or [])
            if isinstance(entry, dict) and entry.get("name")
        ]
        catalog = bool(names)
        if not catalog:
            blockers.append(
                "No database is ingested yet: run the system's ingest job and check its log."
            )

    return {
        "ready": not blockers,
        "ontology_compiled": compiled,
        "can_execute_sql": can_execute,
        "catalog_present": catalog,
        "databases": names,
        "blockers": blockers,
        "unverified": unverified,
    }

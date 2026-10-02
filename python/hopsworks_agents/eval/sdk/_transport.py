"""One HTTP door for the whole client.

Every resource handle talks through this: it knows the project's base URL, how
the caller is entitled to talk to it, and how a refusal is turned into an error
that says which rule refused and why -- the API's own message, never a bare
status. Paths are relative to ``/hopsworks-api/api/project/{id}``, so the same
transport reaches the evaluation API and the tracing API.

The agents themselves answer somewhere else: behind the inference gateway
(istio), at ``/v1/{namespace}/{name}``. :meth:`Transport.agent_request` goes
there, through the model-serving client's istio connection when the caller is
logged in with ``hopsworks.login()`` and through the same session otherwise.
"""

from __future__ import annotations

import os
from typing import Any

from ..api import EvalApiError, _StaticAuth, hopsworks_session


TIMEOUT_S = 60


class AgentServingError(EvalApiError):
    """A refusal from the API, carrying the reason it gave and the status."""

    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


def _message(response: Any) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    if isinstance(body, dict):
        return (
            body.get("usrMsg") or body.get("errorMsg") or f"HTTP {response.status_code}"
        )
    return f"HTTP {response.status_code}"


class Transport:
    def __init__(
        self,
        host: str,
        project_id: int,
        *,
        api_key: str | None = None,
        verify: bool | str = True,
        session: Any = None,
        gateway_url: str | None = None,
    ):
        self.host = host.rstrip("/")
        self.project_id = int(project_id)
        self.base = f"{self.host}/hopsworks-api/api/project/{self.project_id}"
        self._gateway_url = gateway_url.rstrip("/") if gateway_url else None
        if session is not None:
            self._session = session
        elif api_key:
            import requests

            self._session = requests.Session()
            self._session.auth = _StaticAuth("ApiKey " + api_key)
            self._session.verify = verify
        else:
            # whatever this container has: a job's JWT, a notebook's connected client
            self._session = hopsworks_session()
            if verify is not True:
                self._session.verify = verify

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> Any:
        clean = None
        if params:
            # None means "not given", never the string "None"
            clean = {k: v for k, v in params.items() if v is not None} or None
        response = self._session.request(
            method, self.base + path, params=clean, json=json, timeout=TIMEOUT_S
        )
        if response.status_code >= 400:
            raise AgentServingError(_message(response), response.status_code)
        if not getattr(response, "content", b""):
            return None
        return response.json()

    def get(self, path: str, **params: Any) -> Any:
        return self.request("GET", path, params=params or None)

    # ── the agents, behind the inference gateway ───────────────────────────

    @property
    def gateway_url(self) -> str:
        """Where the inference gateway answers, without a trailing slash.

        Given to the constructor, or read off the model-serving client's istio
        connection, which the hopsworks library sets up from the cluster's
        inference endpoints on first use.
        """
        if self._gateway_url:
            return self._gateway_url
        istio = _istio_client()
        if istio is None:
            raise AgentServingError(
                "no inference gateway: log in with hopsworks.login() so the "
                "cluster's inference endpoint is known, or pass gateway_url="
            )
        return str(istio._base_url).rstrip("/")

    def agent_request(
        self,
        method: str,
        segments: list[str],
        *,
        json: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        stream: bool = False,
    ) -> Any:
        """Send a request to an agent at ``gateway_url/<segments>``.

        Through the istio client when the caller is logged in and gave no
        gateway of their own: it carries the credential the gateway accepts and
        the cluster's certificates. Otherwise through this transport's session,
        whose credential the gateway accepts as well.

        With ``stream`` the body is not read: the response's lines come back as
        an iterator of text, for server-sent events.
        """
        timeout = TIMEOUT_S if timeout is None else timeout
        if self._gateway_url is None:
            istio = _istio_client()
            if istio is not None:
                response = _through_istio(
                    istio, method, segments, json, headers, timeout, stream
                )
                return response.iter_lines(decode_unicode=True) if stream else response
        url = self.gateway_url + "/" + "/".join(segments)
        response = self._session.request(
            method, url, json=json, headers=headers, timeout=timeout, stream=stream
        )
        if response.status_code >= 400:
            raise AgentServingError(_message(response), response.status_code)
        if stream:
            return response.iter_lines(decode_unicode=True)
        if not getattr(response, "content", b""):
            return None
        return response.json()

    def post(self, path: str, json: Any = None, **params: Any) -> Any:
        return self.request("POST", path, params=params or None, json=json)

    def put(self, path: str, json: Any = None, **params: Any) -> Any:
        return self.request("PUT", path, params=params or None, json=json)

    def delete(self, path: str, **params: Any) -> Any:
        return self.request("DELETE", path, params=params or None)


def host_from_env() -> str:
    host = os.environ.get("HOPSWORKS_HOST") or os.environ.get("REST_ENDPOINT")
    if not host:
        raise AgentServingError("no host: pass host= or set HOPSWORKS_HOST")
    if not host.startswith("http"):
        host = "https://" + host
    return host


class HopsworksClientSession:
    """A ``requests``-shaped session over the connected hopsworks client.

    Inside Hopsworks -- a job, a notebook, ``hopsworks.login()`` -- the client
    already holds the caller's token and the cluster's certificates, so the
    evaluation client borrows it rather than building a second one. Only what
    :class:`Transport` calls is implemented: ``request`` returning an object
    with ``status_code``, ``content`` and ``json()``.
    """

    def __init__(self, client: Any):
        self._client = client

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        stream: bool = False,
    ) -> Any:
        import json as json_module  # noqa: PLC0415

        from hopsworks_common.client.exceptions import RestAPIError  # noqa: PLC0415

        # the client adds the scheme, host and "hopsworks-api/api" itself
        marker = "/hopsworks-api/api/"
        path = url.split(marker, 1)[1] if marker in url else url.lstrip("/")
        path_params = [segment for segment in path.split("/") if segment]
        sent = dict(headers or {})
        if json is not None:
            sent.setdefault("content-type", "application/json")
        try:
            body = self._client._send_request(
                method,
                path_params,
                query_params=params,
                headers=sent or None,
                data=None if json is None else json_module.dumps(json),
                stream=stream,
                timeout=timeout,
            )
        except RestAPIError as err:
            return _ClientResponse(err.response.status_code, _error_body(err))
        if stream:
            return body  # the response itself, as the client returns it when streaming
        return _ClientResponse(200, body)


class _ClientResponse:
    def __init__(self, status_code: int, body: Any):
        self.status_code = status_code
        self._body = body
        self.content = b"" if body is None else b"x"

    def json(self) -> Any:
        return self._body


def _error_body(err: Any) -> dict[str, Any]:
    try:
        body = err.response.json()
        return body if isinstance(body, dict) else {"errorMsg": str(body)}
    except Exception:  # noqa: BLE001 -- a body that is not JSON still has a status
        return {"errorMsg": str(err)}


def connected_transport() -> Transport | None:
    """A transport over the connected hopsworks client, or None when nothing is connected."""
    try:
        from hopsworks_common import client as hopsworks_client  # noqa: PLC0415

        instance = hopsworks_client._get_instance()
    except Exception:  # noqa: BLE001 -- not connected, or the library is not there
        return None
    project_id = getattr(instance, "_project_id", None)
    base_url = getattr(instance, "_base_url", None) or ""
    if project_id is None:
        return None
    host = base_url.split("/hopsworks-api", 1)[0] if base_url else "https://hopsworks"
    return Transport(host, int(project_id), session=HopsworksClientSession(instance))


def _istio_client() -> Any:
    """The model-serving client's istio connection, or None when there is none."""
    try:
        from hopsworks_common import client as hopsworks_client  # noqa: PLC0415

        hopsworks_client._get_instance()  # raises when nothing is connected
        return hopsworks_client.istio._get_instance()
    except Exception:  # noqa: BLE001 -- not logged in, or no inference endpoint
        return None


def _through_istio(
    istio: Any,
    method: str,
    segments: list[str],
    json: Any,
    headers: dict[str, str] | None,
    timeout: float,
    stream: bool = False,
) -> Any:
    import json as json_module  # noqa: PLC0415

    from hopsworks_common.client.exceptions import RestAPIError  # noqa: PLC0415

    sent = dict(headers or {})
    if json is not None:
        sent.setdefault("content-type", "application/json")
    try:
        return istio._send_request(
            method,
            segments,
            headers=sent or None,
            data=None if json is None else json_module.dumps(json),
            with_base_path_params=False,
            timeout=timeout,
            stream=stream,
        )
    except RestAPIError as err:
        response = err.response
        raise AgentServingError(_message(response), response.status_code) from err

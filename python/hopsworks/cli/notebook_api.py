"""The user's Jupyter server, for ``hops notebook``.

Hopsworks starts one Jupyter server per user and project (``/project/{id}/jupyter``) and proxies it
at ``/hopsworks-api/jupyter/{host}:{port}/``. Jupyter's own token is the credential for Jupyter, and the
Hopsworks proxy wants a ``proxy_session`` cookie, which ``proxy_token`` mints from the API key. These
helpers start it, then use Jupyter's REST API (contents, sessions) and the kernel's WebSocket to
write cells into an ``.ipynb`` and run them, so the notebook opens as-is in the user's JupyterLab.

Every request this module sends for a cell carries the message id ``hops:<cell id>:<uuid>``: a
client watching the kernel (Hopsable's notebook view) maps outputs to cells by that prefix.
"""

from __future__ import annotations

import json
import re
import ssl
import time
import uuid
from typing import TYPE_CHECKING, Any, NamedTuple

import requests
from hopsworks.cli import waits
from hopsworks_common import client
from hopsworks_common.client import exceptions


if TYPE_CHECKING:
    from collections.abc import Callable


NBFORMAT = 4
NBFORMAT_MINOR = 5
_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_POLL_SECONDS = 2.0
# Jupyter's root per project, as root() found it: one lookup per process.
_roots: dict[int, str] = {}


class Run(NamedTuple):
    """A cell's run.

    Its outputs in nbformat, its execution count, whether it finished in time, its timings
    as JupyterLab keeps them in the cell's ``metadata.execution``, and its message id.
    """

    outputs: list[dict]
    count: int | None
    finished: bool
    execution: dict[str, str]
    msg_id: str


class Unreached(RuntimeError):
    """The kernel's socket closed before the cell was sent: the kernel did not run it."""


# How long a kernel still starting may take to answer the kernel_info request sent before a cell.
_KERNEL_INFO_SECONDS = 60


def server(project_id: int) -> dict | None:
    """The caller's running Jupyter server in this project (host, port, token), or None."""
    return client._get_instance()._send_request(
        "GET", ["project", project_id, "jupyter", "running"]
    )


def settings(project_id: int) -> dict:
    """The caller's saved Jupyter settings: environment, resources, base directory."""
    return client._get_instance()._send_request(
        "GET", ["project", project_id, "jupyter", "settings"]
    )


def environment(saved: dict) -> str:
    """The Python environment the Jupyter server runs in, from the saved settings."""
    return (saved.get("environment") or {}).get("name", "?")


def start(project_id: int, env: str | None = None) -> dict:
    """Start the caller's Jupyter server with their saved Jupyter settings.

    With ``env``, in that Python environment, which the settings then keep.
    """
    _client = client._get_instance()
    saved = settings(project_id)
    if env is not None:
        # Hopsworks resolves the environment by its name.
        saved["environment"] = {"name": env}
    return _client._send_request(
        "POST",
        ["project", project_id, "jupyter", "start"],
        headers={"content-type": "application/json"},
        data=json.dumps(saved),
    )


def proxy_token(project_id: int) -> str | None:
    """Mint a Jupyter proxy token for the caller's server in this project, or None on an older cluster.

    The Hopsworks proxy in front of Jupyter takes a ``proxy_session`` cookie, which a browser gets at
    login and an API-key client does not have. The token is pinned to the Jupyter proxy and this project,
    non-renewable and short-lived: the proxy still requires the server to be the caller's own.

    Clusters from before the endpoint answer 404 and did not ask for the cookie either; the caller
    mints it only for a server that is running.
    """
    try:
        return client._get_instance()._send_request(
            "POST", ["project", project_id, "jupyter", "proxy-token"]
        )["token"]
    except exceptions.RestAPIError as exc:
        if exc.response.status_code != 404:
            raise
        return None


def stop(project_id: int) -> None:
    """Stop the caller's Jupyter server."""
    client._get_instance()._send_request(
        "POST", ["project", project_id, "jupyter", "stop"]
    )


class Setup(NamedTuple):
    """Where the user's Jupyter works, as paths in the project, and the environment it runs in.

    The home is the user's Jupyter base directory, ``Users/<username>`` by default. The root is the
    project with the ``hopsfsmount`` contents driver and the home with the others, as Hopsworks'
    own UI reads it.
    """

    root: str
    home: str
    environment: str


def setup(project_id: int) -> Setup:
    """The user's Jupyter setup, from the saved settings and the cluster's contents driver."""
    saved = settings(project_id)
    home = _home(saved)
    return Setup("" if _whole_project() else home, home, environment(saved))


def root(project_id: int) -> str:
    """Jupyter's root, as ``setup`` finds it, looked up once per process.

    With the ``hopsfsmount`` driver the root is the project, and the saved settings are not read.
    """
    if project_id not in _roots:
        _roots[project_id] = "" if _whole_project() else _home(settings(project_id))
    return _roots[project_id]


def _home(saved: dict) -> str:
    # baseDir is absolute: /Projects/<project>/<path in the project>.
    return "/".join(saved["baseDir"].strip("/").split("/")[2:])


def _whole_project() -> bool:
    """Whether Jupyter's contents driver serves the whole project (``hopsfsmount``)."""
    try:
        driver = client._get_instance()._send_request(
            "GET", ["variables", "jupyter_remote_fs_driver"]
        )
    except exceptions.RestAPIError as exc:
        if exc.response.status_code != 404:
            raise
        driver = None
    return (driver or {}).get("successMessage") == "hopsfsmount"


def in_jupyter(path: str, root: str) -> str:
    """A project path as Jupyter's contents API names it, relative to Jupyter's root."""
    path = path.strip("/")
    if not root:
        return path
    if not path.startswith(f"{root}/"):
        raise ValueError(f"Jupyter reaches only {root}/; put the notebook there.")
    return path[len(root) + 1 :]


def monitor(project_id: int) -> dict | None:
    """The Kubernetes view of the caller's Jupyter pod (a KubeMonitorDTO), or None when there is none yet."""
    try:
        return client._get_instance()._send_request(
            "GET", ["project", project_id, "jupyter", "monitor"]
        )
    except Exception:  # noqa: BLE001 - no pod yet, or no monitor on this cluster: no reason to show
        return None


def _ready(running: dict | None) -> bool:
    # Starting returns before the pod is scheduled; `ready` is absent on older clusters.
    return bool(
        running
        and running.get("port")
        and running.get("token")
        and running.get("ready", True)
    )


def connect(project_id: int, timeout: float, env: str | None = None) -> Jupyter:
    """A client for the caller's Jupyter server, started first when it is not running.

    A server that is not ready yet is waited for, printing each status it goes through. With
    ``env``, the server must run in that environment: a running one in another is left alone,
    since stopping it would end the user's kernels.
    """
    watch = waits.Watch("Jupyter", timeout, _POLL_SECONDS)
    running = server(project_id)
    if running and env is not None:
        current = environment(settings(project_id))
        if current != env:
            raise ValueError(
                f"Jupyter runs in the environment '{current}'. Stop it with "
                f"`hops notebook stop` to start it in '{env}'; that ends its kernels."
            )
    # A server that is up has no status changes to report, so `hops notebook run` stays quiet.
    quiet = _ready(running)
    running = running or start(project_id, env)
    while not _ready(running):
        status = (running or {}).get("status") or "starting"
        watch.see(status, waits.kube_reason(monitor(project_id)))
        if watch.expired():
            watch.fail("Check the cluster's free resources, or raise --timeout.")
        watch.sleep()
        running = server(project_id)
        if running is None:
            # It existed a poll ago, so it was stopped, not still starting.
            raise RuntimeError(
                "Jupyter was stopped by another client while it was starting. "
                "Start it again with `hops notebook start`."
            )
    jupyter = _jupyter(project_id, running)
    if not quiet:
        watch.see(running.get("status") or "running")
    try:
        jupyter.wait_ready(watch.left())
    except TimeoutError:
        watch.see(watch.state or "not answering")
        watch.fail("The server does not answer yet; raise --timeout.")
    if not quiet:
        watch.see("answering")
    return jupyter


def attach(project_id: int, timeout: float) -> Jupyter:
    """A client for the caller's running Jupyter server, for its contents only.

    Unlike ``connect``, a running server is not asked whether it answers: the first read does.
    A server not running is started and waited for by ``connect``.
    """
    running = server(project_id)
    if not _ready(running):
        return connect(project_id, timeout)
    return _jupyter(project_id, running)


def _jupyter(project_id: int, running: dict) -> Jupyter:
    """A client for the server ``jupyter/running`` describes, through the Hopsworks proxy."""
    _client = client._get_instance()
    host = running.get("hostName")
    address = f"{host}:{running['port']}" if host else str(running["port"])
    return Jupyter(
        f"{_client._base_url}/hopsworks-api/jupyter/{address}",
        running["token"],
        _client._verify,
        lambda: proxy_token(project_id),
    )


class Jupyter:
    """Jupyter Server's REST API and kernel channels, authenticated by the server's token.

    ``mint`` returns a Hopsworks proxy token (or None: no cookie needed). It is sent as the
    ``proxy_session`` cookie on every request, the WebSocket handshake included. The token is short-lived
    and not renewable, so an answer of 401 mints a new one and sends the request once more.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        verify: bool | str = True,
        mint: Callable[[], str | None] | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.verify = verify
        self.mint = mint
        self.proxy_session: str | None = None
        self._minted = False
        self.http = requests.Session()
        self.http.headers["Authorization"] = f"token {token}"

    def _renew(self) -> bool:
        """Mint a new proxy token. False when there is nothing to mint: no cookie is sent."""
        self._minted = True
        self.proxy_session = self.mint() if self.mint else None
        if self.proxy_session:
            self.http.cookies.set("proxy_session", self.proxy_session)
        else:
            self.http.cookies.clear()
        return self.proxy_session is not None

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """One request through the proxy; on a 401 with a fresh proxy token, once more."""
        if self.mint and not self._minted:
            self._renew()
        for attempt in (0, 1):
            response = self.http.request(
                method, f"{self.base_url}{path}", verify=self.verify, **kwargs
            )
            if response.status_code != 401:
                return response
            if attempt == 0 and self._renew():
                continue
            raise RuntimeError(
                f"Hopsworks refused the proxy token for Jupyter ({method} {path}): "
                f"{response.text[:300]}. Check that the cluster and the CLI are the same version."
            )
        raise AssertionError("unreachable")

    def _call(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._request(method, path, timeout=60, **kwargs)
        if response.status_code == 404:
            return None
        if response.status_code // 100 != 2:
            raise RuntimeError(
                f"Jupyter answered {response.status_code} to {method} {path}: {response.text[:300]}"
            )
        return response.json() if response.content else None

    def wait_ready(self, timeout: float) -> None:
        """Wait until the server answers, as a freshly started one takes a while."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                # A 401 is not a server still starting: _request raises once a new token is refused too.
                if self._request("GET", "/api/status", timeout=10).ok:
                    return
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"The Jupyter server did not answer within {timeout:.0f} s."
                )
            time.sleep(1)

    def _open(self, url: str, ssl_context: Any):
        """The kernel channels' WebSocket, with the proxy token; on a 401 with a fresh one, once more."""
        from websockets.exceptions import InvalidStatus  # noqa: PLC0415
        from websockets.sync.client import connect as ws_connect  # noqa: PLC0415

        if self.mint and not self._minted:
            self._renew()
        for attempt in (0, 1):
            headers = {"Authorization": f"token {self.token}"}
            if self.proxy_session:
                headers["Cookie"] = f"proxy_session={self.proxy_session}"
            try:
                return ws_connect(
                    url,
                    additional_headers=headers,
                    ssl=ssl_context,
                    max_size=None,
                    open_timeout=60,
                )
            except InvalidStatus as exc:
                if exc.response.status_code != 401:
                    raise
                if attempt == 0 and self._renew():
                    continue
                raise RuntimeError(
                    "Hopsworks refused the proxy token for the kernel channels (401). "
                    "Check that the cluster and the CLI are the same version."
                ) from exc
        raise AssertionError("unreachable")

    def read(self, path: str) -> dict | None:
        """The notebook at ``path`` (relative to Jupyter's root), or None when it does not exist."""
        model = self._call(
            "GET",
            f"/api/contents/{_quote(path)}",
            params={"type": "notebook", "content": 1},
        )
        return model["content"] if model else None

    def write(self, path: str, notebook: dict) -> None:
        self._call(
            "PUT",
            f"/api/contents/{_quote(path)}",
            json={"type": "notebook", "format": "json", "content": notebook},
        )

    def session(self, path: str, kernel_name: str | None = None) -> dict:
        """The Jupyter session of this notebook, created with a kernel when it has none.

        Jupyter keeps one session per path, so every run of a notebook reaches the same kernel and
        its variables, and JupyterLab opening the notebook joins that kernel too.
        """
        existing = self.find_session(path)
        if existing:
            return existing
        if kernel_name is None:
            kernel_name = self._call("GET", "/api/kernelspecs")["default"]
        return self._call(
            "POST",
            "/api/sessions",
            json={
                "path": path,
                "name": path.rsplit("/", 1)[-1],
                "type": "notebook",
                "kernel": {"name": kernel_name},
            },
        )

    def kernelspec(self, name: str | None = None) -> dict:
        """The notebook metadata's ``kernelspec`` for kernel ``name``, by default the server's default."""
        specs = self._call("GET", "/api/kernelspecs") or {}
        name = name or specs.get("default", "python3")
        spec = specs.get("kernelspecs", {}).get(name, {}).get("spec", {})
        return {
            "name": name,
            "display_name": spec.get("display_name", name),
            "language": spec.get("language", "python"),
        }

    def find_session(self, path: str) -> dict | None:
        for existing in self._call("GET", "/api/sessions") or []:
            if existing.get("path") == path:
                return existing
        return None

    def interrupt(self, kernel_id: str) -> None:
        self._call("POST", f"/api/kernels/{kernel_id}/interrupt")

    def execute(self, session: dict, cell_id: str, code: str, timeout: float) -> Run:
        """Run ``code`` on the session's kernel; a cell still running at ``timeout`` is interrupted."""
        from websockets.exceptions import ConnectionClosed  # noqa: PLC0415

        kernel_id = session["kernel"]["id"]
        url = (
            re.sub(r"^http", "ws", self.base_url)
            + f"/api/kernels/{kernel_id}/channels?session_id={session['id']}"
            # The token goes in the query too: Jupyter reads it there on a WebSocket handshake
            # whatever headers the Hopsworks proxy forwards.
            + f"&token={self.token}"
        )
        ssl_context = None
        if url.startswith("wss"):
            ssl_context = ssl.create_default_context()
            if self.verify is False:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            elif isinstance(self.verify, str):
                ssl_context.load_verify_locations(self.verify)
        msg_id = f"hops:{cell_id}:{uuid.uuid4().hex}"
        request = {
            "header": {
                "msg_id": msg_id,
                "msg_type": "execute_request",
                "username": "hops",
                "session": session["id"],
                "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "version": "5.3",
            },
            "parent_header": {},
            "metadata": {"cellId": cell_id},
            "content": {
                "code": code,
                "silent": False,
                "store_history": True,
                "user_expressions": {},
                "allow_stdin": False,
                "stop_on_error": True,
            },
            "channel": "shell",
            "buffers": [],
        }
        outputs: list[dict] = []
        count = None
        finished = True
        execution: dict[str, str] = {}
        idle = False
        sent = False
        try:
            with self._open(url, ssl_context) as ws:
                # A kernel still starting may close the socket. Its kernel_info reply shows it
                # takes requests on this socket, so a close before the cell is sent ran nothing.
                _kernel_info(ws, session["id"])
                sent = True
                ws.send(json.dumps(request))
                deadline = time.monotonic() + timeout
                while True:
                    left = deadline - time.monotonic()
                    if idle and (left <= 0 or "shell.execute_reply" in execution):
                        return Run(outputs, count, finished, execution, msg_id)
                    if left <= 0:
                        if not finished:
                            raise TimeoutError(
                                "The kernel did not stop the cell after an interrupt."
                            )
                        # Wait for the interrupted cell to end: a cell sent while the kernel is still
                        # handling the interrupt is aborted with it.
                        self.interrupt(kernel_id)
                        finished = False
                        deadline = time.monotonic() + 30
                        continue
                    try:
                        message = json.loads(ws.recv(timeout=left))
                    except TimeoutError:
                        continue
                    if message.get("parent_header", {}).get("msg_id") != msg_id:
                        continue
                    kind = message["header"]["msg_type"]
                    date = message["header"].get("date")
                    content = message.get("content", {})
                    if kind == "execute_reply":
                        if content.get("status") == "aborted":
                            raise RuntimeError(
                                "The kernel aborted the cell; run it again."
                            )
                        started = message.get("metadata", {}).get("started")
                        if started:
                            execution["shell.execute_reply.started"] = started
                        if date:
                            execution["shell.execute_reply"] = date
                    if message.get("channel") == "iopub":
                        state = content.get("execution_state")
                        if kind == "status" and state in ("busy", "idle") and date:
                            execution[f"iopub.status.{state}"] = date
                        if kind == "status" and state == "idle":
                            # The shell's reply, with the kernel's timings, may come just after.
                            idle = True
                            deadline = min(deadline, time.monotonic() + 2)
                        elif kind == "execute_input":
                            count = content.get("execution_count")
                            if date:
                                execution["iopub.execute_input"] = date
                        elif kind == "clear_output":
                            outputs.clear()
                        else:
                            output = to_output(kind, content)
                            if output is not None:
                                _append(outputs, output)

        except ConnectionClosed as exc:
            if not sent:
                raise Unreached(str(exc)) from exc
            # A reply lost on the way back does not show that the cell did not run.
            raise RuntimeError(
                "The connection to the kernel closed after the cell was sent, so it may have "
                "run. Read the notebook before running the cell again."
            ) from exc


def _kernel_info(ws: Any, session_id: str) -> None:
    """Ask the kernel for its info and wait for the reply, before any cell is sent on ``ws``."""
    msg_id = f"hops:kernel_info:{uuid.uuid4().hex}"
    ws.send(
        json.dumps(
            {
                "header": {
                    "msg_id": msg_id,
                    "msg_type": "kernel_info_request",
                    "username": "hops",
                    "session": session_id,
                    "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "version": "5.3",
                },
                "parent_header": {},
                "metadata": {},
                "content": {},
                "channel": "shell",
                "buffers": [],
            }
        )
    )
    deadline = time.monotonic() + _KERNEL_INFO_SECONDS
    while (left := deadline - time.monotonic()) > 0:
        try:
            message = json.loads(ws.recv(timeout=left))
        except TimeoutError:
            break
        if (
            message.get("parent_header", {}).get("msg_id") == msg_id
            and message["header"]["msg_type"] == "kernel_info_reply"
        ):
            return
    raise Unreached(f"The kernel did not answer within {_KERNEL_INFO_SECONDS} s.")


def to_output(kind: str, content: dict) -> dict | None:
    """An iopub message as an nbformat output, or None for messages that are not outputs."""
    if kind == "stream":
        return {
            "output_type": "stream",
            "name": content["name"],
            "text": content["text"],
        }
    if kind in ("display_data", "execute_result"):
        output = {
            "output_type": kind,
            "data": content.get("data", {}),
            "metadata": content.get("metadata", {}),
        }
        if kind == "execute_result":
            output["execution_count"] = content.get("execution_count")
        return output
    if kind == "error":
        return {
            "output_type": "error",
            "ename": content.get("ename", ""),
            "evalue": content.get("evalue", ""),
            "traceback": content.get("traceback", []),
        }
    return None


def _append(outputs: list[dict], output: dict) -> None:
    # Jupyter merges consecutive writes to one stream into one output, as JupyterLab shows them.
    last = outputs[-1] if outputs else None
    if (
        output["output_type"] == "stream"
        and last is not None
        and last["output_type"] == "stream"
        and last["name"] == output["name"]
    ):
        last["text"] += output["text"]
    else:
        outputs.append(output)


def new_notebook(kernelspec: dict) -> dict:
    return {
        "nbformat": NBFORMAT,
        "nbformat_minor": NBFORMAT_MINOR,
        "metadata": {"kernelspec": kernelspec},
        "cells": [],
    }


def rewrite_cell(old: dict, cell_type: str, source: str) -> dict:
    """The cell ``old`` with new text: only what the edit changes.

    Its id and metadata carry over, and so do a markdown or raw cell's attachments while it stays
    that type. A code cell starts unrun: no outputs, no run count, no run timing in its metadata.
    """
    new = new_cell(cell_type, source)
    new["id"] = old.get("id", new["id"])
    new["metadata"] = {
        k: v
        for k, v in old.get("metadata", {}).items()
        if not (cell_type == "code" and k == "execution")
    }
    if (
        cell_type in ("markdown", "raw")
        and old.get("cell_type") == cell_type
        and "attachments" in old
    ):
        new["attachments"] = old["attachments"]
    return new


def new_cell(cell_type: str, source: str) -> dict:
    cell: dict[str, Any] = {
        "id": uuid.uuid4().hex[:8],
        "cell_type": cell_type,
        "metadata": {},
        "source": source,
    }
    if cell_type == "code":
        cell["execution_count"] = None
        cell["outputs"] = []
    return cell


def source(cell: dict) -> str:
    text = cell.get("source", "")
    return "".join(text) if isinstance(text, list) else text


def describe(outputs: list[dict], limit: int) -> str:
    """The outputs as text for a terminal: text as-is, rich data by its type, ANSI codes removed."""
    parts = []
    for output in outputs:
        kind = output["output_type"]
        if kind == "stream":
            parts.append(_text(output["text"]))
        elif kind == "error":
            parts.append(
                _ANSI.sub(
                    "",
                    "\n".join(
                        output.get("traceback")
                        or [f"{output['ename']}: {output['evalue']}"]
                    ),
                )
            )
        else:
            data = output.get("data", {})
            rich = sorted(k for k in data if k != "text/plain")
            if "text/plain" in data:
                parts.append(_text(data["text/plain"]))
            if rich:
                parts.append(f"[{', '.join(rich)}]")
    text = "\n".join(p.rstrip("\n") for p in parts if p)
    if len(text) > limit:
        text = (
            text[:limit]
            + f"\n[... {len(text) - limit} more characters in the notebook]"
        )
    return text


def _text(value: str | list[str]) -> str:
    return "".join(value) if isinstance(value, list) else value


def _quote(path: str) -> str:
    return requests.utils.quote(path.strip("/"))

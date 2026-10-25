"""The user's Jupyter server, for ``hops notebook``.

Hopsworks starts one Jupyter server per user and project (``/project/{id}/jupyter``) and proxies it
at ``/hopsworks-api/jupyter/{host}:{port}/``, where Jupyter's own token is the credential. These
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
from typing import Any, NamedTuple

import requests
from hopsworks_common import client
from hopsworks_common.client import exceptions


NBFORMAT = 4
NBFORMAT_MINOR = 5
_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
# Jupyter's root per project, as root() found it: one lookup per process.
_roots: dict[int, str] = {}


class Run(NamedTuple):
    """A cell's run.

    Its outputs in nbformat, its execution count, whether it finished in time, and its timings
    as JupyterLab keeps them in the cell's ``metadata.execution``.
    """

    outputs: list[dict]
    count: int | None
    finished: bool
    execution: dict[str, str]


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


def connect(project_id: int, timeout: float, env: str | None = None) -> Jupyter:
    """A client for the caller's Jupyter server, started first when it is not running.

    With ``env``, the server must run in that environment: a running one in another is left alone,
    since stopping it would end the user's kernels.
    """
    running = server(project_id)
    if running and env is not None:
        current = environment(settings(project_id))
        if current != env:
            raise ValueError(
                f"Jupyter runs in the environment '{current}'. Stop it with "
                f"`hops notebook stop` to start it in '{env}'; that ends its kernels."
            )
    running = running or start(project_id, env)
    jupyter = _jupyter(running)
    jupyter.wait_ready(timeout)
    return jupyter


def attach(project_id: int, timeout: float) -> Jupyter:
    """A client for the caller's running Jupyter server, for its contents only.

    Unlike ``connect``, a running server is not asked whether it answers: the first read does.
    A server not running is started and waited for by ``connect``.
    """
    running = server(project_id)
    if not (
        running
        and running.get("port")
        and running.get("token")
        and running.get("ready", True)
    ):
        return connect(project_id, timeout)
    return _jupyter(running)


def _jupyter(running: dict) -> Jupyter:
    """A client for the server ``jupyter/running`` describes, through the Hopsworks proxy."""
    _client = client._get_instance()
    host = running.get("hostName")
    address = f"{host}:{running['port']}" if host else str(running["port"])
    return Jupyter(
        f"{_client._base_url}/hopsworks-api/jupyter/{address}",
        running["token"],
        _client._verify,
    )


class Jupyter:
    """Jupyter Server's REST API and kernel channels, authenticated by the server's token."""

    def __init__(self, base_url: str, token: str, verify: bool | str = True):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.verify = verify
        self.http = requests.Session()
        self.http.headers["Authorization"] = f"token {token}"

    def _call(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.http.request(
            method, f"{self.base_url}{path}", verify=self.verify, timeout=60, **kwargs
        )
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
                if self.http.get(
                    f"{self.base_url}/api/status", verify=self.verify, timeout=10
                ).ok:
                    return
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"The Jupyter server did not answer within {timeout:.0f} s."
                )
            time.sleep(1)

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
        from websockets.sync.client import connect as ws_connect  # noqa: PLC0415

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
        deadline = time.monotonic() + timeout
        with ws_connect(
            url,
            additional_headers={"Authorization": f"token {self.token}"},
            ssl=ssl_context,
            max_size=None,
            open_timeout=60,
        ) as ws:
            ws.send(json.dumps(request))
            while True:
                left = deadline - time.monotonic()
                if idle and (left <= 0 or "shell.execute_reply" in execution):
                    return Run(outputs, count, finished, execution)
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
                        raise RuntimeError("The kernel aborted the cell; run it again.")
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

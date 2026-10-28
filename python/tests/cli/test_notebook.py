"""``hops notebook``'s Jupyter side against a real Jupyter Server, with no Hopsworks calls.

The commands' path from Hopsworks (start, connect, the project's root) to Jupyter runs end to end
in Hopsable's deploy/hopsworks/notebook_proxy_live.py, against a deployed cluster.

The proxy tests below are unit tests of the client's renewal logic. ``StandInProxy`` is a stand-in for
the Hopsworks proxy, not the proxy: its tokens are random strings, its mint is a local counter and its
refusals are written to match what the Hopsworks source does. What it shows is how the client behaves
when it is refused; that Hopsworks really refuses and accepts as modelled is shown by the live test.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import socket
import subprocess
import sys
import threading
import time

import click
import pytest
import requests
from hopsworks.cli import notebook_api
from hopsworks.cli.commands import notebook
from hopsworks_common import client
from hopsworks_common.client import exceptions
from tornado import httpserver, netutil, web, websocket
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect
from websockets.sync.server import serve


CHART = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="


@pytest.fixture(scope="module")
def jupyter(tmp_path_factory):
    root = tmp_path_factory.mktemp("jupyter")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    token = secrets.token_hex(16)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "jupyter_server",
            f"--port={port}",
            f"--ServerApp.token={token}",
            f"--ServerApp.root_dir={root}",
            "--ServerApp.open_browser=False",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = notebook_api.Jupyter(f"http://127.0.0.1:{port}", token)
    try:
        client.wait_ready(60)
        yield client
    finally:
        process.terminate()
        process.wait(10)


@pytest.fixture
def path():
    return f"nb_{secrets.token_hex(4)}.ipynb"


def test_cells_share_one_kernel_and_keep_their_outputs(jupyter, path):
    session = jupyter.session(path)
    first = jupyter.execute(session, "c1", "x = 21\nprint('half', x)", 60)
    second = jupyter.execute(jupyter.session(path), "c2", "x * 2", 60)
    assert first.finished and second.finished
    assert first.outputs == [
        {"output_type": "stream", "name": "stdout", "text": "half 21\n"}
    ]
    assert second.outputs[0]["output_type"] == "execute_result"
    assert second.outputs[0]["data"]["text/plain"] == "42"
    assert second.count == first.count + 1
    assert {"shell.execute_reply.started", "shell.execute_reply"} <= set(
        second.execution
    )
    assert jupyter.find_session(path)["kernel"]["id"] == session["kernel"]["id"]


def test_an_error_comes_back_as_an_error_output(jupyter, path):
    run = jupyter.execute(jupyter.session(path), "c1", "1 / 0", 60)
    assert run.finished
    assert run.outputs[-1]["output_type"] == "error"
    assert run.outputs[-1]["ename"] == "ZeroDivisionError"
    assert "ZeroDivisionError" in notebook_api.describe(run.outputs, 2000)


def test_a_cell_past_its_timeout_is_interrupted_and_the_kernel_lives_on(jupyter, path):
    session = jupyter.session(path)
    started = time.monotonic()
    run = jupyter.execute(session, "c1", "import time\ntime.sleep(60)", 2)
    assert not run.finished
    assert time.monotonic() - started < 30
    after = jupyter.execute(session, "c2", "print('still alive')", 60)
    assert after.finished
    assert after.outputs[0]["text"] == "still alive\n"


def test_another_client_of_the_kernel_sees_the_outputs_by_cell_id(jupyter, path):
    """A watcher on the kernel (Hopsable's notebook view) maps outputs to cells by message id."""
    session = jupyter.session(path)
    url = (
        jupyter.base_url.replace("http", "ws", 1)
        + f"/api/kernels/{session['kernel']['id']}/channels?token={jupyter.token}"
    )
    with connect(url) as watcher:
        run = jupyter.execute(session, "abcd1234", "print(1)", 60)
        assert run.outputs == [
            {"output_type": "stream", "name": "stdout", "text": "1\n"}
        ]
        seen = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            message = json.loads(watcher.recv(timeout=10))
            if message["header"]["msg_type"] == "stream":
                seen.append(message["parent_header"]["msg_id"])
                break
    assert seen and seen[0].startswith("hops:abcd1234:")


class Relay:
    """A real WebSocket hop between the CLI and Jupyter that loses messages like a dropped proxy.

    ``close_on`` names the first request type the relay does not pass on: it closes the CLI's
    socket instead. ``lose_after`` names a request type the relay passes on, after which it
    drops every answer and closes the CLI's socket once the kernel is idle again.
    """

    def __init__(self, jupyter, close_on=None, lose_after=None):
        self.upstream = jupyter.base_url.replace("http", "ws", 1)
        self.close_on, self.lose_after = close_on, lose_after
        self.server = serve(self._handle, "127.0.0.1", 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        port = self.server.socket.getsockname()[1]
        self.jupyter = notebook_api.Jupyter(f"http://127.0.0.1:{port}", jupyter.token)

    def _handle(self, front):
        lost: list[str] = []
        with connect(self.upstream + front.request.path) as back:

            def forward():
                try:
                    for raw in front:
                        kind = json.loads(raw)["header"]["msg_type"]
                        if kind == self.close_on:
                            front.close()
                            break
                        back.send(raw)
                        if kind == self.lose_after:
                            lost.append(json.loads(raw)["header"]["msg_id"])
                except ConnectionClosed:
                    pass
                # The CLI's socket is gone: so is this hop, and the loop below ends.
                back.close()

            threading.Thread(target=forward, daemon=True).start()
            try:
                for raw in back:
                    message = json.loads(raw)
                    parent = message.get("parent_header", {}).get("msg_id")
                    if not lost:
                        front.send(raw)
                    elif (
                        parent == lost[0]
                        and message["header"]["msg_type"] == "status"
                        and message["content"].get("execution_state") == "idle"
                    ):
                        front.close()
                        return
            except ConnectionClosed:
                pass

    def stop(self):
        self.server.shutdown()


def _lines(path) -> int:
    return len(path.read_text().splitlines()) if path.exists() else 0


def test_a_cell_whose_answers_are_lost_is_not_sent_again(jupyter, path, tmp_path):
    effect = tmp_path / "effect.txt"
    code = f"open({str(effect)!r}, 'a').write('ran\\n')"
    session = jupyter.session(path)
    relay = Relay(jupyter, lose_after="execute_request")
    try:
        with pytest.raises(RuntimeError, match="may have run") as raised:
            relay.jupyter.execute(session, "c1", code, 60)
    finally:
        relay.stop()
    # Not Unreached: `hops notebook run` sends the cell again only on Unreached.
    assert not isinstance(raised.value, notebook_api.Unreached)
    time.sleep(2)
    assert _lines(effect) == 1


def test_a_socket_closed_before_the_cell_is_sent_ran_nothing(jupyter, path, tmp_path):
    effect = tmp_path / "effect.txt"
    code = f"open({str(effect)!r}, 'a').write('ran\\n')"
    session = jupyter.session(path)
    relay = Relay(jupyter, close_on="kernel_info_request")
    try:
        with pytest.raises(notebook_api.Unreached):
            relay.jupyter.execute(session, "c1", code, 60)
    finally:
        relay.stop()
    assert _lines(effect) == 0
    # What `hops notebook run` does next: the cell again, which runs once.
    assert jupyter.execute(jupyter.session(path), "c1", code, 60).finished
    assert _lines(effect) == 1


def test_a_missing_notebook_reads_as_none_and_has_no_session(jupyter, path):
    assert jupyter.read(path) is None
    assert jupyter.find_session(path) is None


def test_a_rewritten_cell_keeps_its_id_metadata_and_attachments_through_jupyter(
    jupyter, path
):
    notebook = notebook_api.new_notebook(jupyter.kernelspec())
    text = notebook_api.new_cell("markdown", "![chart](attachment:chart.png)\n# Old")
    text["attachments"] = {"chart.png": {"image/png": CHART}}
    text["metadata"] = {"tags": ["keep"], "custom": {"x": 1}}
    code = notebook_api.new_cell("code", "print(1)")
    code["execution_count"] = 4
    code["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "1\n"}]
    code["metadata"] = {
        "tags": ["parameters"],
        "execution": {"shell.execute_reply": "x"},
    }
    notebook["cells"] = [text, code]
    jupyter.write(path, notebook)

    saved = jupyter.read(path)
    saved["cells"] = [
        notebook_api.rewrite_cell(
            saved["cells"][0], "markdown", "![chart](attachment:chart.png)\n# New"
        ),
        notebook_api.rewrite_cell(saved["cells"][1], "code", "print(2)"),
    ]
    jupyter.write(path, saved)

    text, code = jupyter.read(path)["cells"]
    assert notebook_api.source(text) == "![chart](attachment:chart.png)\n# New"
    assert text["attachments"] == {"chart.png": {"image/png": CHART}}
    assert text["metadata"] == {"tags": ["keep"], "custom": {"x": 1}}
    assert [text["id"], code["id"]] == [c["id"] for c in notebook["cells"]]
    assert notebook_api.source(code) == "print(2)"
    assert code["metadata"]["tags"] == ["parameters"]
    assert "execution" not in code["metadata"]
    assert code["outputs"] == [] and code["execution_count"] is None


def test_a_cell_that_changes_type_keeps_its_id_and_metadata_only():
    old = notebook_api.new_cell("markdown", "# Notes")
    old["attachments"] = {"a.png": {"image/png": CHART}}
    old["metadata"] = {"tags": ["keep"]}
    new = notebook_api.rewrite_cell(old, "code", "print(1)")
    assert new["id"] == old["id"]
    assert new["metadata"] == {"tags": ["keep"]}
    assert "attachments" not in new
    assert new["outputs"] == [] and new["execution_count"] is None


def test_a_project_path_names_a_notebook_in_jupyter():
    assert notebook_api.in_jupyter("Users/alice/a.ipynb", "") == "Users/alice/a.ipynb"
    assert notebook_api.in_jupyter("/Users/alice/a.ipynb", "Users/alice") == "a.ipynb"
    with pytest.raises(ValueError, match="only Users/alice/"):
        notebook_api.in_jupyter("Resources/a.ipynb", "Users/alice")


def test_update_reads_its_edits_in_order():
    edits = notebook._edits(
        json.dumps(
            [{"cell": 2, "markdown": "## Part 2 \U0001f389"}, {"cell": 8, "code": "x"}]
        )
    )
    assert edits == [(2, "markdown", "## Part 2 \U0001f389"), (8, "code", "x")]


@pytest.mark.parametrize(
    ("stdin", "message"),
    [
        ("not json", "not JSON"),
        ("[]", "at least one edit"),
        ('[{"cell": 1}]', '"markdown"'),
        ('[{"cell": 0, "markdown": "x"}]', '"markdown"'),
        ('[{"cell": 1, "markdown": "x", "code": "y"}]', '"markdown"'),
        ('[{"cell": 1, "markdown": "x"}, {"cell": 1, "code": "y"}]', "twice"),
    ],
)
def test_update_rejects_a_bad_edit_list(stdin, message):
    with pytest.raises(click.UsageError, match=message):
        notebook._edits(stdin)


class StandInProxy:
    """A local HTTP and WebSocket hop in front of a real Jupyter Server, standing in for the Hopsworks proxy.

    Modelled on the Hopsworks source: a request without a valid ``proxy_session`` gets HTTP 401, a
    request carrying a Jupyter login cookie is not checked (JupyterProxyServlet), and a WebSocket with a
    bad token completes the handshake and is closed with 1008 (HopsWsBridgeBase). ``issue`` stands in
    for the mint endpoint, ``expire`` ends every token issued, ``refuse`` rejects all of them, and
    ``drop_after_execute`` closes the socket as soon as an execute_request arrives, before the kernel
    sees it.
    """

    def __init__(self, jupyter, ttl: float = 900):
        self.upstream = jupyter.base_url
        self.ttl = ttl
        self.valid: set[str] = set()
        self.issued = 0
        self.refused_http = 0
        self.refused_ws = 0
        self.executes = 0
        self.refuse = False
        self.drop_after_execute = False
        self.jupyter_token = jupyter.token
        self.loop = asyncio.new_event_loop()
        sockets = netutil.bind_sockets(0, "127.0.0.1")
        self.port = sockets[0].getsockname()[1]
        proxy = self

        class Http(web.RequestHandler):
            def prepare(self):
                if not proxy.allows(self.request.headers.get("Cookie", "")):
                    proxy.refused_http += 1
                    self.set_status(401)
                    self.finish("Unauthorized access.")

            async def _forward(self):
                from tornado.httpclient import AsyncHTTPClient  # noqa: PLC0415

                response = await AsyncHTTPClient().fetch(
                    proxy.upstream + self.request.uri,
                    method=self.request.method,
                    headers={
                        k: v
                        for k, v in self.request.headers.items()
                        if k in ("Authorization", "Content-Type")
                    },
                    body=self.request.body or None,
                    allow_nonstandard_methods=True,
                    raise_error=False,
                )
                self.set_status(response.code)
                self.set_header(
                    "Content-Type", response.headers.get("Content-Type", "text/plain")
                )
                for cookie in response.headers.get_list("Set-Cookie"):
                    self.add_header("Set-Cookie", cookie)
                self.write(response.body or b"")

            get = post = put = delete = patch = _forward

        class Channels(websocket.WebSocketHandler):
            def check_origin(self, origin):
                return True

            async def open(self):
                if not proxy.allows(self.request.headers.get("Cookie", "")):
                    proxy.refused_ws += 1
                    self.close(1008, "proxy_session JWT missing or invalid")
                    return
                self.up = await websocket.websocket_connect(
                    proxy.upstream.replace("http", "ws", 1) + self.request.uri,
                    on_message_callback=self.down,
                )

            def down(self, message):
                if message is None:
                    self.close()
                else:
                    self.write_message(message, binary=isinstance(message, bytes))

            async def on_message(self, message):
                if '"msg_type": "execute_request"' in message:
                    proxy.executes += 1
                    if proxy.drop_after_execute:
                        self.close()
                        return
                await self.up.write_message(message, binary=isinstance(message, bytes))

            def on_close(self):
                if hasattr(self, "up"):
                    self.up.close()

        app = web.Application(
            [(r"/api/kernels/[^/]+/channels", Channels), (r".*", Http)]
        )

        def serve():
            asyncio.set_event_loop(self.loop)
            self.server = httpserver.HTTPServer(app)
            self.server.add_sockets(sockets)
            self.loop.run_forever()

        threading.Thread(target=serve, daemon=True).start()
        self.base_url = f"http://127.0.0.1:{self.port}"

    def allows(self, cookies: str) -> bool:
        pairs = dict(c.strip().split("=", 1) for c in cookies.split(";") if "=" in c)
        if any(name.startswith("username-") for name in pairs):
            return True
        return not self.refuse and pairs.get("proxy_session") in self.valid

    def issue(self) -> notebook_api.ProxyToken:
        self.issued += 1
        token = secrets.token_hex(8)
        self.valid.add(token)
        return notebook_api.ProxyToken(token, time.time() + self.ttl)

    def expire(self):
        self.valid.clear()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)


@pytest.fixture
def gate(jupyter):
    hop = StandInProxy(jupyter)
    try:
        yield hop
    finally:
        hop.stop()


def test_a_client_without_a_proxy_token_is_refused_by_the_proxy(jupyter, gate):
    """What the CLI did before it minted one: the Jupyter token alone, and a 401 on every call."""
    bare = notebook_api.Jupyter(gate.base_url, jupyter.token)
    with pytest.raises(RuntimeError, match="proxy token"):
        bare.find_session("a.ipynb")


def test_every_call_and_the_kernel_socket_carry_the_proxy_token(jupyter, gate, path):
    through = notebook_api.Jupyter(gate.base_url, jupyter.token, mint=gate.issue)
    through.wait_ready(30)
    through.write(path, notebook_api.new_notebook(through.kernelspec()))
    assert through.read(path)["nbformat"] == notebook_api.NBFORMAT
    run = through.execute(through.session(path), "c1", "print(6 * 7)", 60)
    assert run.outputs == [{"output_type": "stream", "name": "stdout", "text": "42\n"}]
    assert gate.issued == 1
    assert gate.refused_http == gate.refused_ws == 0


def test_a_token_near_its_end_is_minted_again_before_the_call(jupyter, path):
    proxy = StandInProxy(jupyter, ttl=0.4)
    try:
        through = notebook_api.Jupyter(proxy.base_url, jupyter.token, mint=proxy.issue)
        through.write(path, notebook_api.new_notebook(through.kernelspec()))
        assert proxy.issued == 1
        time.sleep(0.5)
        assert through.read(path)["nbformat"] == notebook_api.NBFORMAT
        assert proxy.issued == 2
        assert proxy.refused_http == 0
    finally:
        proxy.stop()


def test_a_token_near_its_end_is_minted_again_before_the_kernel_socket(jupyter, path):
    proxy = StandInProxy(jupyter, ttl=0.4)
    try:
        through = notebook_api.Jupyter(proxy.base_url, jupyter.token, mint=proxy.issue)
        session = through.session(path)
        time.sleep(0.5)
        run = through.execute(session, "c1", "print(6 * 7)", 60)
        assert run.outputs[0]["text"] == "42\n"
        assert proxy.issued == 2
        assert proxy.refused_ws == 0
    finally:
        proxy.stop()


def test_an_expired_proxy_token_is_minted_again_after_a_401(jupyter, gate, path):
    through = notebook_api.Jupyter(gate.base_url, jupyter.token, mint=gate.issue)
    through.write(path, notebook_api.new_notebook(through.kernelspec()))
    gate.expire()
    assert through.read(path)["nbformat"] == notebook_api.NBFORMAT
    assert gate.issued == 2
    assert gate.refused_http == 1


def test_a_socket_closed_1008_before_any_message_is_opened_again_with_a_new_token(
    jupyter, gate, path
):
    """Hopsworks refuses a token on a WebSocket by closing 1008 after the handshake, not with a 401."""
    through = notebook_api.Jupyter(gate.base_url, jupyter.token, mint=gate.issue)
    session = through.session(path)
    gate.expire()
    run = through.execute(session, "c1", "print(6 * 7)", 60)
    assert run.outputs[0]["text"] == "42\n"
    assert gate.issued == 2
    assert gate.refused_ws == 1
    assert gate.executes == 1


def test_wait_ready_fails_at_once_when_the_proxy_refuses_the_token(jupyter, gate):
    gate.refuse = True
    through = notebook_api.Jupyter(gate.base_url, jupyter.token, mint=gate.issue)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="refused the proxy token"):
        through.wait_ready(300)
    assert time.monotonic() - started < 10
    # One mint, one new mint after the 401, and no more.
    assert gate.issued == 2


def test_a_refused_kernel_socket_is_opened_twice_and_then_fails_without_running_the_cell(
    jupyter, gate, path
):
    session = jupyter.session(path)
    gate.refuse = True
    through = notebook_api.Jupyter(gate.base_url, jupyter.token, mint=gate.issue)
    with pytest.raises(RuntimeError, match="refused the proxy token"):
        through.execute(session, "c1", "1", 60)
    assert gate.issued == 2
    assert gate.refused_ws == 2
    assert gate.executes == 0


def test_a_cell_is_never_sent_again_after_its_execute_request_was_sent(
    jupyter, gate, path
):
    through = notebook_api.Jupyter(gate.base_url, jupyter.token, mint=gate.issue)
    session = through.session(path)
    gate.drop_after_execute = True
    with pytest.raises(RuntimeError, match="may have run"):
        through.execute(session, "c1", "print(6 * 7)", 60)
    assert gate.executes == 1
    assert gate.issued == 1


def test_the_session_keeps_no_jupyter_login_cookie_that_would_hide_an_expired_token(
    jupyter, gate, path
):
    """Jupyter sets its login cookie after a token-authenticated call, and the proxy skips the token check for it."""
    through = notebook_api.Jupyter(gate.base_url, jupyter.token, mint=gate.issue)
    through.wait_ready(30)
    through.write(path, notebook_api.new_notebook(through.kernelspec()))
    assert not list(through.http.cookies)
    gate.expire()
    assert through.read(path)["nbformat"] == notebook_api.NBFORMAT
    assert gate.issued == 2


class _Hopsworks:
    def __init__(self, status):
        self.status = status
        self.calls = []

    def _send_request(self, method, path, **kwargs):
        self.calls.append((method, path))
        response = requests.Response()
        response.status_code = self.status
        if self.status == 404:
            raise exceptions.RestAPIError("http://x", response)
        return {"token": "jwt", "expiresAt": 1_800_000_000_000}


def test_the_proxy_token_is_minted_for_the_project(monkeypatch):
    hopsworks = _Hopsworks(200)
    monkeypatch.setattr(client, "_get_instance", lambda: hopsworks)
    assert notebook_api.proxy_token(7) == ("jwt", 1_800_000_000)
    assert hopsworks.calls == [("POST", ["project", 7, "jupyter", "proxy-token"])]


def test_a_cluster_without_the_proxy_token_endpoint_needs_no_cookie(monkeypatch):
    monkeypatch.setattr(client, "_get_instance", lambda: _Hopsworks(404))
    assert notebook_api.proxy_token(7) is None

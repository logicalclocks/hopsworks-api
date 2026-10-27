"""``hops notebook``'s Jupyter side against a real Jupyter Server, with no Hopsworks calls.

The commands' path from Hopsworks (start, connect, the project's root) to Jupyter runs end to end
in Hopsable's live tests, against the dev cluster.
"""

from __future__ import annotations

import json
import secrets
import socket
import subprocess
import sys
import threading
import time

import click
import pytest
from hopsworks.cli import notebook_api
from hopsworks.cli.commands import notebook
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

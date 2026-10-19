"""``hops notebook`` against a real Jupyter Server; only the Hopsworks lifecycle calls are patched."""

from __future__ import annotations

import json
import secrets
import socket
import subprocess
import sys
import time
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import notebook_api
from hopsworks.cli.commands import notebook
from hopsworks.cli.main import cli


pytest.importorskip("jupyter_server")
pytest.importorskip("ipykernel")
pytest.importorskip("websockets")


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
def hops(jupyter, mock_project, tmp_path):
    """Run `hops notebook ...` on the local server, with Jupyter's root at Users/alice."""
    name = f"nb_{secrets.token_hex(4)}.ipynb"
    with (
        mock.patch.object(notebook_api, "connect", return_value=jupyter),
        mock.patch.object(
            notebook_api,
            "setup",
            return_value=notebook_api.Setup(
                "Users/alice", "Users/alice", "python-feature-pipeline"
            ),
        ),
        mock.patch.object(notebook, "_project_id", return_value=119),
    ):

        def run(*args, stdin=None):
            return CliRunner().invoke(cli, ["notebook", *args], input=stdin)

        yield run, name


def test_cells_share_one_kernel_and_save_their_outputs(hops, jupyter):
    run, path = hops
    first = run("run", f"Users/alice/{path}", "--code", "x = 21\nprint('half', x)")
    assert first.exit_code == 0, first.output
    assert f"Cell 1 of Users/alice/{path}" in first.output
    assert "half 21" in first.output

    second = run("run", f"Users/alice/{path}", "--code", "-", stdin="x * 2")
    assert second.exit_code == 0, second.output
    assert "42" in second.output

    saved = jupyter.read(path)
    assert saved["metadata"]["kernelspec"]["name"]
    assert [c["cell_type"] for c in saved["cells"]] == ["code", "code"]
    assert saved["cells"][0]["outputs"] == [
        {"output_type": "stream", "name": "stdout", "text": "half 21\n"}
    ]
    result = saved["cells"][1]["outputs"][0]
    assert result["output_type"] == "execute_result"
    assert result["data"]["text/plain"] == "42"
    assert (
        saved["cells"][1]["execution_count"] == saved["cells"][0]["execution_count"] + 1
    )


def test_an_error_is_saved_and_fails_the_command(hops, jupyter):
    run, path = hops
    result = run("run", f"Users/alice/{path}", "--code", "1 / 0")
    assert result.exit_code == 1
    assert "ZeroDivisionError" in result.output
    assert jupyter.read(path)["cells"][0]["outputs"][0]["ename"] == "ZeroDivisionError"


def test_replace_a_cell_and_add_markdown(hops, jupyter):
    run, path = hops
    assert run("run", f"Users/alice/{path}", "--code", "print('old')").exit_code == 0
    assert (
        run("run", f"Users/alice/{path}", "--markdown", "--code", "# Notes").exit_code
        == 0
    )
    first_id = jupyter.read(path)["cells"][0]["id"]
    replaced = run(
        "run", f"Users/alice/{path}", "--cell", "1", "--code", "print('new')"
    )
    assert replaced.exit_code == 0, replaced.output
    cells = jupyter.read(path)["cells"]
    assert cells[0]["id"] == first_id
    assert "shell.execute_reply" in cells[0]["metadata"]["execution"]
    assert notebook_api.source(cells[0]) == "print('new')"
    assert cells[0]["outputs"][0]["text"] == "new\n"
    assert cells[1] == {**cells[1], "cell_type": "markdown", "source": "# Notes"}
    assert "outputs" not in cells[1]

    info = run("--json", "info", f"Users/alice/{path}")
    rows = json.loads(info.output)
    assert [(r["CELL"], r["TYPE"]) for r in rows] == [(1, "code"), (2, "markdown")]

    beyond = run("run", f"Users/alice/{path}", "--cell", "5", "--code", "1")
    assert beyond.exit_code == 2
    assert "has 2 cells" in beyond.output


def test_a_cell_past_its_timeout_is_interrupted(hops, jupyter):
    run, path = hops
    started = time.monotonic()
    result = run(
        "run",
        f"Users/alice/{path}",
        "--timeout",
        "2",
        "--code",
        "import time\ntime.sleep(60)",
    )
    assert result.exit_code == 1
    assert "interrupted" in result.output
    assert time.monotonic() - started < 30
    after = run("run", f"Users/alice/{path}", "--code", "print('still alive')")
    assert after.exit_code == 0, after.output
    assert "still alive" in after.output


def test_another_client_of_the_kernel_sees_the_outputs_by_cell_id(jupyter):
    """A watcher on the kernel (Hopsable's notebook view) maps outputs to cells by message id."""
    from websockets.sync.client import connect  # noqa: PLC0415

    session = jupyter.session(f"ids_{secrets.token_hex(4)}.ipynb")
    url = (
        jupyter.base_url.replace("http", "ws", 1)
        + f"/api/kernels/{session['kernel']['id']}/channels?token={jupyter.token}"
    )
    with connect(url) as watcher:
        outputs, count, finished, execution = jupyter.execute(
            session, "abcd1234", "print(1)", 60
        )
        assert {"shell.execute_reply.started", "shell.execute_reply"} <= set(execution)
        assert finished
        assert outputs == [{"output_type": "stream", "name": "stdout", "text": "1\n"}]
        seen = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            message = json.loads(watcher.recv(timeout=10))
            if message["header"]["msg_type"] == "stream":
                seen.append(message["parent_header"]["msg_id"])
                break
    assert seen and seen[0].startswith("hops:abcd1234:")


def test_a_path_must_be_a_notebook(hops):
    run, _ = hops
    result = run("run", "notes.txt", "--code", "1")
    assert result.exit_code == 2


def test_a_project_path_names_a_notebook_in_jupyter():
    assert notebook_api.in_jupyter("Users/alice/a.ipynb", "") == "Users/alice/a.ipynb"
    assert notebook_api.in_jupyter("/Users/alice/a.ipynb", "Users/alice") == "a.ipynb"
    with pytest.raises(ValueError, match="only Users/alice/"):
        notebook_api.in_jupyter("Resources/a.ipynb", "Users/alice")


def test_a_cell_the_user_edited_is_not_replaced(hops, jupyter):
    run, path = hops
    assert run("run", f"Users/alice/{path}", "--code", "print('agent')").exit_code == 0
    notebook = jupyter.read(path)
    notebook["cells"][0]["metadata"] = {"hopsable": {"editedBy": "user"}}
    jupyter.write(path, notebook)
    refused = run(
        "run", f"Users/alice/{path}", "--cell", "1", "--code", "print('again')"
    )
    assert refused.exit_code == 1
    assert "edited by the user" in refused.output
    assert notebook_api.source(jupyter.read(path)["cells"][0]) == "print('agent')"


def test_replacing_a_cell_of_a_missing_notebook_says_it_is_missing(hops):
    run, path = hops
    result = run("run", f"Users/alice/{path}", "--cell", "1", "--code", "1")
    assert result.exit_code == 1
    assert f"Notebook 'Users/alice/{path}' not found." in result.output


def test_a_running_server_in_another_environment_is_left_alone():
    running = {"port": 8888, "token": "t", "hostName": "h"}
    with (
        mock.patch.object(notebook_api, "server", return_value=running),
        mock.patch.object(
            notebook_api,
            "settings",
            return_value={"environment": {"name": "python-feature-pipeline"}},
        ),
        mock.patch.object(notebook_api, "start") as start,
        pytest.raises(
            ValueError, match="runs in the environment 'python-feature-pipeline'"
        ),
    ):
        notebook_api.connect(119, 1, env="pandas-training-pipeline")
    start.assert_not_called()


def test_start_sends_the_environment_by_name():
    instance = mock.Mock()
    instance._send_request.side_effect = [
        {"baseDir": "/Projects/p/Users/alice", "environment": {"name": "old", "id": 1}},
        {"port": 8888, "token": "t"},
    ]
    with mock.patch.object(notebook_api.client, "_get_instance", return_value=instance):
        notebook_api.start(119, "pandas-training-pipeline")
    sent = json.loads(instance._send_request.call_args.kwargs["data"])
    assert sent["environment"] == {"name": "pandas-training-pipeline"}
    assert sent["baseDir"] == "/Projects/p/Users/alice"

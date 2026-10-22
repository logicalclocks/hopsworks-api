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
        mock.patch.object(notebook_api, "attach", return_value=jupyter),
        mock.patch.object(notebook_api, "root", return_value="Users/alice"),
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


def _counted(jupyter):
    """Count the notebook's reads and writes, and the kernel sessions asked for."""
    return (
        mock.patch.object(jupyter, "read", wraps=jupyter.read),
        mock.patch.object(jupyter, "write", wraps=jupyter.write),
        mock.patch.object(jupyter, "session", wraps=jupyter.session),
    )


def _seven_markdown_cells_and_code(jupyter, path):
    notebook = notebook_api.new_notebook(jupyter.kernelspec("python3"))
    cells = [notebook_api.new_cell("markdown", f"## Part {i}") for i in range(1, 8)]
    code = notebook_api.new_cell("code", "print(1)")
    code["execution_count"] = 4
    code["outputs"] = [{"output_type": "stream", "name": "stdout", "text": "1\n"}]
    notebook["cells"] = [*cells, code]
    jupyter.write(path, notebook)
    return notebook["cells"]


def test_update_saves_several_cells_with_one_read_and_one_write(hops, jupyter):
    run, path = hops
    before = _seven_markdown_cells_and_code(jupyter, path)
    edits = [
        {
            "cell": i,
            "markdown": f'## Part {i} \U0001f389\nWith `code`, $HOME and "quotes"',
        }
        for i in range(1, 8)
    ]
    edits.append({"cell": 8, "code": "print(2)"})
    read, write, session = _counted(jupyter)
    with (
        read as reads,
        write as writes,
        session as sessions,
        mock.patch.object(notebook_api, "connect") as connect,
    ):
        result = run("update", f"Users/alice/{path}", stdin=json.dumps(edits))
    assert result.exit_code == 0, result.output
    assert reads.call_count == 1
    assert writes.call_count == 1
    sessions.assert_not_called()
    connect.assert_not_called()
    cells = jupyter.read(path)["cells"]
    assert [c["id"] for c in cells] == [c["id"] for c in before]
    assert (
        notebook_api.source(cells[2])
        == '## Part 3 \U0001f389\nWith `code`, $HOME and "quotes"'
    )
    assert cells[2]["cell_type"] == "markdown"
    assert notebook_api.source(cells[7]) == "print(2)"
    assert cells[7]["outputs"] == []
    assert cells[7]["execution_count"] is None
    assert jupyter.find_session(path) is None
    assert "not run" in result.output


def test_update_refuses_every_cell_when_one_was_edited_by_the_user(hops, jupyter):
    run, path = hops
    _seven_markdown_cells_and_code(jupyter, path)
    notebook = jupyter.read(path)
    notebook["cells"][4]["metadata"] = {"hopsable": {"editedBy": "user"}}
    jupyter.write(path, notebook)
    edits = [{"cell": 1, "markdown": "# New"}, {"cell": 5, "markdown": "# Mine"}]
    result = run("update", f"Users/alice/{path}", stdin=json.dumps(edits))
    assert result.exit_code == 1
    assert "Cell 5" in result.output and "edited by the user" in result.output
    assert jupyter.read(path) == notebook


@pytest.mark.parametrize(
    ("stdin", "message"),
    [
        ("not json", "not JSON"),
        ("[]", "at least one edit"),
        ('[{"cell": 1}]', '"markdown"'),
        ('[{"cell": 0, "markdown": "x"}]', '"markdown"'),
        ('[{"cell": 1, "markdown": "x", "code": "y"}]', '"markdown"'),
        ('[{"cell": 1, "markdown": "x"}, {"cell": 1, "code": "y"}]', "twice"),
        ('[{"cell": 9, "markdown": "x"}]', "has 8 cells"),
    ],
)
def test_update_rejects_a_bad_edit_list_and_saves_nothing(
    hops, jupyter, stdin, message
):
    run, path = hops
    _seven_markdown_cells_and_code(jupyter, path)
    saved = jupyter.read(path)
    result = run("update", f"Users/alice/{path}", stdin=stdin)
    assert result.exit_code == 2, result.output
    assert message in result.output
    assert jupyter.read(path) == saved


def test_update_of_a_missing_notebook_says_it_is_missing(hops):
    run, path = hops
    result = run(
        "update", f"Users/alice/{path}", stdin='[{"cell": 1, "markdown": "x"}]'
    )
    assert result.exit_code == 1
    assert f"Notebook 'Users/alice/{path}' not found." in result.output


def test_info_source_prints_every_cell_in_full(hops, jupyter):
    run, path = hops
    cells = _seven_markdown_cells_and_code(jupyter, path)
    cells[0]["source"] = "# Title\n\nA second paragraph."
    notebook = jupyter.read(path)
    notebook["cells"][0]["source"] = cells[0]["source"]
    jupyter.write(path, notebook)
    with (
        mock.patch.object(notebook_api, "connect") as connect,
        mock.patch.object(notebook_api, "setup") as setup,
    ):
        text = run("info", f"Users/alice/{path}", "--source")
        listed = run("--json", "info", f"Users/alice/{path}", "--source")
    connect.assert_not_called()
    setup.assert_not_called()
    assert text.exit_code == 0, text.output
    assert (
        f"[cell 1] markdown {cells[0]['id']}\n# Title\n\nA second paragraph.\n"
        in text.output
    )
    assert f"[cell 8] code {cells[7]['id']}\nprint(1)\n" in text.output
    assert json.loads(listed.output)[7] == {
        "cell": 8,
        "id": cells[7]["id"],
        "type": "code",
        "source": "print(1)",
    }


def test_a_markdown_run_starts_no_kernel(hops, jupyter):
    run, path = hops
    with mock.patch.object(notebook_api, "connect") as connect:
        result = run("run", f"Users/alice/{path}", "--markdown", "--code", "# Notes")
    assert result.exit_code == 0, result.output
    connect.assert_not_called()
    assert jupyter.find_session(path) is None
    saved = jupyter.read(path)
    assert saved["metadata"]["kernelspec"]["name"]
    assert notebook_api.source(saved["cells"][0]) == "# Notes"


def test_the_root_is_looked_up_once_per_process():
    instance = mock.Mock()
    instance._send_request.side_effect = [
        {"successMessage": "hopsfsmount"},
        {"successMessage": "default"},
        {"baseDir": "/Projects/p/Users/bob"},
    ]
    notebook_api._roots.clear()
    with mock.patch.object(notebook_api.client, "_get_instance", return_value=instance):
        assert notebook_api.root(119) == ""
        assert notebook_api.root(119) == ""
        assert notebook_api.root(120) == "Users/bob"
        assert notebook_api.root(120) == "Users/bob"
    notebook_api._roots.clear()
    assert [c.args for c in instance._send_request.call_args_list] == [
        ("GET", ["variables", "jupyter_remote_fs_driver"]),
        ("GET", ["variables", "jupyter_remote_fs_driver"]),
        ("GET", ["project", 120, "jupyter", "settings"]),
    ]


def test_attach_to_a_running_server_skips_the_status_check():
    running = {"port": 8888, "token": "t", "hostName": "h"}
    instance = mock.Mock(_base_url="https://c", _verify=True)
    with (
        mock.patch.object(notebook_api.client, "_get_instance", return_value=instance),
        mock.patch.object(notebook_api, "server", return_value=running),
        mock.patch.object(notebook_api, "connect") as connect,
        mock.patch.object(notebook_api.Jupyter, "wait_ready") as wait_ready,
    ):
        jupyter = notebook_api.attach(119, 300)
    assert jupyter.base_url == "https://c/hopsworks-api/jupyter/h:8888"
    assert jupyter.token == "t"
    wait_ready.assert_not_called()
    connect.assert_not_called()


def test_attach_starts_a_server_that_is_not_running():
    with (
        mock.patch.object(notebook_api, "server", return_value=None),
        mock.patch.object(notebook_api, "connect") as connect,
    ):
        assert notebook_api.attach(119, 300) is connect.return_value
    connect.assert_called_once_with(119, 300)

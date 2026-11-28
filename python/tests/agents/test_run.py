import os
import socket
import subprocess
import sys
import textwrap
import time
import urllib.request

import pytest
from hopsworks_agents import run
from hsml.model_serving import _package_runner_source


PYTHON_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
URL = f"http://127.0.0.1:{run.PORT}/health"


@pytest.fixture(autouse=True)
def port_free():
    # The runner serves where the deployment listens; a test that cannot bind it would hang or pass wrongly.
    with socket.socket() as s:
        assert s.connect_ex(("127.0.0.1", run.PORT)) != 0, f"port {run.PORT} is taken"


def start(script, *args):
    env = dict(os.environ, PYTHONPATH=PYTHON_DIR)
    return subprocess.Popen(
        [sys.executable, "-m", "hopsworks_agents.run", str(script), *args],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def finish(proc):
    out, err = proc.communicate(timeout=60)
    return proc.returncode, out, err


def serves(proc):
    """True once /health answers 200; False if the process ends first."""
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                return False
            try:
                with urllib.request.urlopen(URL, timeout=2) as r:
                    return r.status == 200
            except OSError:
                time.sleep(0.2)
        raise AssertionError("no answer on /health within 60 s")
    finally:
        proc.kill()
        proc.wait(timeout=30)


def write(path, source):
    path.write_text(textwrap.dedent(source))
    return path


def test_an_app_without_a_main_block_is_served(tmp_path):
    script = write(
        tmp_path / "agent.py",
        """
        from hopsworks_agents.protocol import AgentApp
        agent_app = AgentApp(name="t")
        """,
    )
    assert serves(start(script))


def test_a_main_block_that_serves_runs_as_today(tmp_path):
    script = write(
        tmp_path / "agent.py",
        f"""
        import uvicorn
        from hopsworks_agents.protocol import AgentApp
        agent_app = AgentApp(name="t")
        if __name__ == "__main__":
            uvicorn.run(agent_app, host="127.0.0.1", port={run.PORT})
        """,
    )
    assert serves(start(script))


def test_two_apps_exit_naming_both(tmp_path):
    script = write(
        tmp_path / "agent.py",
        """
        from hopsworks_agents.protocol import AgentApp
        first = AgentApp(name="a")
        second = AgentApp(name="b")
        """,
    )
    code, _, err = finish(start(script))
    assert code == 1
    assert "first, second" in err


def test_a_script_that_returns_without_an_app_exits_saying_why(tmp_path):
    script = write(tmp_path / "agent.py", "print('done')\n")
    code, out, err = finish(start(script))
    assert code == 1
    assert out == "done\n"
    assert "defines no AgentApp" in err


def test_an_explicit_exit_keeps_its_code(tmp_path):
    script = write(tmp_path / "agent.py", "import sys\nsys.exit(0)\n")
    code, _, err = finish(start(script))
    assert code == 0
    assert err == ""


def test_the_script_sees_its_own_directory_file_and_args(tmp_path):
    write(tmp_path / "helper.py", "VALUE = 'from sibling'\n")
    script = write(
        tmp_path / "agent.py",
        """
        import os, sys
        import helper
        print(helper.VALUE, os.path.basename(__file__), sys.argv[1:], __name__)
        sys.exit(0)
        """,
    )
    code, out, _ = finish(start(script, "--flag", "a b"))
    assert code == 0
    assert out == "from sibling agent.py ['--flag', 'a b'] __main__\n"


def test_a_package_runner_hands_its_app_over(tmp_path):
    pkg = tmp_path / "site" / "my_agent"
    pkg.mkdir(parents=True)
    write(pkg / "__init__.py", "")
    write(
        pkg / "__main__.py",
        """
        from hopsworks_agents.protocol import AgentApp
        agent_app = AgentApp(name="pkg")
        """,
    )
    runner = write(tmp_path / "runner.py", _package_runner_source("my_agent"))
    env = dict(
        os.environ, PYTHONPATH=os.pathsep.join([PYTHON_DIR, str(tmp_path / "site")])
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "hopsworks_agents.run", str(runner)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert serves(proc)

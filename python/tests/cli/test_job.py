"""`hops job create`: the job's configuration, including its environment."""

from __future__ import annotations

from types import SimpleNamespace

from click.testing import CliRunner
from hopsworks.cli import session
from hopsworks.cli.main import cli


def _project(created: list):
    api = SimpleNamespace(
        get_configuration=lambda job_type: {
            "type": job_type,
            "environmentName": "default-env",
        },
        create_job=lambda name, config: (
            created.append((name, config)) or SimpleNamespace(name=name)
        ),
    )
    return SimpleNamespace(get_job_api=lambda: api)


def test_create_runs_the_job_in_the_named_environment(monkeypatch):
    created: list = []
    monkeypatch.setattr(session, "get_project", lambda ctx: _project(created))
    args = [
        "job",
        "create",
        "ingest",
        "--type",
        "python",
        "--app-path",
        "Resources/a.py",
    ]
    done = CliRunner().invoke(
        cli, [*args, "--env", "helpdesk-example-env", "--args", "--docs d"]
    )
    assert done.exit_code == 0, done.output
    name, config = created[0]
    assert name == "ingest" and config["appPath"] == "Resources/a.py"
    assert config["environmentName"] == "helpdesk-example-env"
    assert config["defaultArgs"] == "--docs d"

    # Without --env the type's default stays.
    assert CliRunner().invoke(cli, args).exit_code == 0
    assert created[1][1]["environmentName"] == "default-env"


def test_logs_download_into_the_given_dir_not_the_working_directory(
    monkeypatch, tmp_path
):
    calls = []
    execution = SimpleNamespace(
        id=7,
        download_logs=lambda path=None: (
            calls.append(path) or (f"{path}/stdout.log", f"{path}/stderr.log")
        ),
    )
    job = SimpleNamespace(get_executions=lambda: [execution])
    monkeypatch.setattr(
        session,
        "get_project",
        lambda ctx: SimpleNamespace(
            get_job_api=lambda: SimpleNamespace(get_job=lambda name: job)
        ),
    )
    logs = tmp_path / "Logs" / "factory" / "churn"
    done = CliRunner().invoke(cli, ["job", "logs", "churn-train", "--dir", str(logs)])
    assert done.exit_code == 0, done.output
    assert calls == [str(logs)] and logs.is_dir()
    assert "working directory" not in done.output


def _failing_run(monkeypatch, stderr: str):
    from hopsworks_common.client.exceptions import JobExecutionException

    def download_logs(path=None):
        with open(f"{path}/stderr.log", "w") as f:
            f.write(stderr)
        return f"{path}/stdout.log", f"{path}/stderr.log"

    def run(**kwargs):
        raise JobExecutionException("Execution 7 failed")

    execution = SimpleNamespace(id=7, download_logs=download_logs)
    job = SimpleNamespace(run=run, get_executions=lambda: [execution])
    monkeypatch.setattr(
        session,
        "get_project",
        lambda ctx: SimpleNamespace(
            get_job_api=lambda: SimpleNamespace(get_job=lambda name: job)
        ),
    )
    return CliRunner().invoke(cli, ["job", "run", "etl", "--wait"])


def test_a_failed_wait_gives_the_egress_line_as_its_reason(monkeypatch):
    # The dlt job prints the line last, after the vendor's traceback.
    line = (
        "Egress from project demo to port 5432/TCP is closed. Ask for it: https://h/x."
    )
    done = _failing_run(
        monkeypatch,
        f"Traceback\npsycopg2.OperationalError: Connection refused\nhopsworks egress: {line}\n",
    )
    assert done.exit_code == 1
    assert f"Run failed: {line}" in done.output


def test_a_failed_wait_ignores_text_that_only_holds_the_egress_prefix(monkeypatch):
    done = _failing_run(monkeypatch, "log: hopsworks egress: not at the start\n")
    assert done.exit_code == 1
    assert "Run failed: Execution 7 failed" in done.output

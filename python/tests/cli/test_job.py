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


def _deploy_project(created: list, stored=None, lookup_error=None):
    def get_job(name):
        if lookup_error is not None:
            raise lookup_error
        return stored

    api = SimpleNamespace(
        get_configuration=lambda job_type: {
            "type": "pythonJobConfiguration",
            "resourceConfig": {"cores": 0.5, "memory": 2048, "gpus": 0},
        },
        get_job=get_job,
        create_job=lambda name, config: (
            created.append((name, config)) or SimpleNamespace(name=name)
        ),
    )
    return SimpleNamespace(get_job_api=lambda: api)


def _not_found():
    error = Exception("job not found")
    error.response = SimpleNamespace(status_code=404)
    return error


def test_create_sets_the_resources_given_and_keeps_the_other_defaults(monkeypatch):
    created: list = []
    project = _deploy_project(created)
    monkeypatch.setattr(session, "get_project", lambda ctx: project)
    done = CliRunner().invoke(
        cli,
        [
            "job",
            "create",
            "train",
            "--type",
            "python",
            "--app-path",
            "Resources/a.py",
            "--resources",
            '{"resourceConfig": {"cores": 2, "memory": 8192}}',
        ],
    )
    assert done.exit_code == 0, done.output
    assert created[0][1]["resourceConfig"] == {"cores": 2, "memory": 8192, "gpus": 0}


def test_create_refuses_resources_that_are_not_a_json_object(monkeypatch):
    created: list = []
    project = _deploy_project(created)
    monkeypatch.setattr(session, "get_project", lambda ctx: project)
    args = ["job", "create", "t", "--type", "python", "--app-path", "Resources/a.py"]
    for bad, says in (
        ("{cores: 2}", "--resources is not valid JSON"),
        ("[1, 2]", "--resources must be a JSON object"),
    ):
        done = CliRunner().invoke(cli, [*args, "--resources", bad])
        assert done.exit_code == 2, done.output
        assert says in done.output
    assert created == []


def test_redeploy_keeps_the_stored_configuration(monkeypatch):
    created: list = []
    stored = SimpleNamespace(
        job_type="PYTHON",
        config={
            "type": "pythonJobConfiguration",
            "appPath": "Resources/old.py",
            "resourceConfig": {"cores": 4, "memory": 16384, "gpus": 0},
            "defaultArgs": "--day 1",
        },
    )
    project = _deploy_project(created, stored=stored)
    monkeypatch.setattr(session, "get_project", lambda ctx: project)
    done = CliRunner().invoke(cli, ["job", "deploy", "train", "Resources/new.py"])
    assert done.exit_code == 0, done.output
    config = created[0][1]
    assert config["appPath"] == "Resources/new.py"
    assert config["resourceConfig"] == {"cores": 4, "memory": 16384, "gpus": 0}
    assert config["defaultArgs"] == "--day 1"


def test_redeploy_changes_only_the_resources_given(monkeypatch):
    created: list = []
    stored = SimpleNamespace(
        job_type="PYTHON",
        config={"resourceConfig": {"cores": 4, "memory": 16384, "gpus": 0}},
    )
    project = _deploy_project(created, stored=stored)
    monkeypatch.setattr(session, "get_project", lambda ctx: project)
    done = CliRunner().invoke(
        cli,
        [
            "job",
            "deploy",
            "train",
            "Resources/a.py",
            "--resources",
            '{"resourceConfig": {"memory": 32768}}',
        ],
    )
    assert done.exit_code == 0, done.output
    assert created[0][1]["resourceConfig"] == {"cores": 4, "memory": 32768, "gpus": 0}


def test_deploy_of_a_new_job_or_another_type_starts_from_the_default(monkeypatch):
    for stored, error in (
        (None, _not_found()),
        (SimpleNamespace(job_type="PYSPARK", config={"appPath": "x"}), None),
    ):
        created: list = []
        project = _deploy_project(created, stored=stored, lookup_error=error)
        monkeypatch.setattr(session, "get_project", lambda ctx, p=project: p)
        done = CliRunner().invoke(cli, ["job", "deploy", "train", "Resources/a.py"])
        assert done.exit_code == 0, done.output
        assert created[0][1]["resourceConfig"] == {
            "cores": 0.5,
            "memory": 2048,
            "gpus": 0,
        }
        assert created[0][1]["type"] == "pythonJobConfiguration"


def test_deploy_stops_when_the_job_cannot_be_read(monkeypatch):
    created: list = []
    error = Exception("server error")
    error.response = SimpleNamespace(status_code=500)
    project = _deploy_project(created, lookup_error=error)
    monkeypatch.setattr(session, "get_project", lambda ctx: project)
    done = CliRunner().invoke(cli, ["job", "deploy", "train", "Resources/a.py"])
    assert done.exit_code != 0
    assert "Could not read job 'train'" in done.output
    assert created == []

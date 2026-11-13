"""`hops job`: the job's configuration, its logs, and waits that follow an execution."""

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import session, waits
from hopsworks.cli.commands import job as job_cmd
from hopsworks.cli.main import cli
from hopsworks_common.client.exceptions import RestAPIError

from tests.cli.conftest import PENDING_REASON, execution


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


def test_kube_reason_names_why_a_pod_is_not_scheduled(unschedulable):
    assert waits.kube_reason(unschedulable) == PENDING_REASON


def test_kube_reason_is_the_newest_warning_event(unschedulable):
    pod = unschedulable["job"]["pods"]["items"][0]
    older = {**pod["events"]["items"][0], "age": "3m", "message": "0/6 nodes: older"}
    pod["events"]["items"].insert(0, older)
    assert waits.kube_reason(unschedulable) == PENDING_REASON


def test_kube_reason_falls_back_to_the_condition_of_a_pod_not_ready(unschedulable):
    unschedulable["job"]["pods"]["items"][0]["events"] = {"items": []}
    assert waits.kube_reason(unschedulable) == PENDING_REASON.removeprefix(
        "FailedScheduling: "
    )


def test_kube_reason_keeps_a_message_that_names_its_reason(unschedulable):
    event = {"type": "Warning", "reason": "BackOff", "message": "BackOff pulling image"}
    unschedulable["job"]["pods"]["items"][0]["events"] = {"items": [event]}
    assert waits.kube_reason(unschedulable) == "BackOff pulling image"


def test_kube_reason_without_a_reason():
    assert waits.kube_reason(None) is None
    assert waits.kube_reason({}) is None
    # A Python job's execution once its pod is gone, and a Python App's execution.
    assert waits.kube_reason(execution("get_python_failed")["kubeMonitor"]) is None
    assert waits.kube_reason(execution("get_app_running")["kubeMonitor"]) is None


# The backend's answer to an expansion it does not know, as captured.
NOT_FOUND = {
    "errorCode": 120004,
    "usrMsg": "HTTP 404 Not Found",
    "errorMsg": "Web application exception occurred",
}


def _rest_error(status: int, body: dict) -> RestAPIError:
    response = mock.Mock(status_code=status, reason="", content=b"")
    response.json.return_value = body
    return RestAPIError("https://h/hopsworks-api/api/project", response)


def test_execution_json_falls_back_when_the_backend_has_no_kube_monitor():
    fake = mock.MagicMock(_project_id=7)
    fake._send_request.side_effect = [
        _rest_error(404, NOT_FOUND),
        {"state": "FINISHED"},
    ]
    with mock.patch("hopsworks_common.client._get_instance", return_value=fake):
        assert job_cmd._execution_json("train", 3, kube=True) == {"state": "FINISHED"}
    first, second = fake._send_request.call_args_list
    assert first.kwargs == {"query_params": {"expand": "kube_monitor"}}
    assert second.args == ("GET", ["project", 7, "jobs", "train", "executions", 3])
    assert second.kwargs == {}


def test_execution_json_lets_any_other_error_through():
    fake = mock.MagicMock(_project_id=7)
    fake._send_request.side_effect = [_rest_error(500, {"errorMsg": "boom"})]
    with (
        mock.patch("hopsworks_common.client._get_instance", return_value=fake),
        pytest.raises(RestAPIError),
    ):
        job_cmd._execution_json("train", 3, kube=True)

"""CLI tests for ``hops app`` — Streamlit app lifecycle commands."""

from __future__ import annotations

from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


def _fake_app(**overrides):
    a = mock.MagicMock(name="App")
    a.id = overrides.get("id", 42)
    a.name = overrides.get("name", "my_app")
    a.state = overrides.get("state", "CREATED")
    a.serving = overrides.get("serving", False)
    a.environment = overrides.get("environment", "python-app-pipeline")
    a.memory = overrides.get("memory", 2048)
    a.cores = overrides.get("cores", 1.0)
    a.app_path = overrides.get("app_path", "Resources/app.py")
    a.app_url = overrides.get("app_url")
    return a


def test_app_list_renders_rows(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_apps.return_value = [
        _fake_app(name="a1", state="RUNNING", serving=True),
        _fake_app(id=43, name="a2", state="KILLED"),
    ]
    result = CliRunner().invoke(cli, ["app", "list"])
    assert result.exit_code == 0, result.output
    assert "a1" in result.output
    assert "a2" in result.output
    assert "RUNNING" in result.output


def test_app_info_shows_url(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash", state="RUNNING", serving=True, app_url="https://host/dash"
    )
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert result.exit_code == 0, result.output
    assert "https://host/dash" in result.output
    assert "RUNNING" in result.output


def test_app_url_exits_non_zero_when_not_serving(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(state="INITIALIZING", serving=False)
    result = CliRunner().invoke(cli, ["app", "url", "dash"])
    assert result.exit_code != 0
    assert "no URL yet" in result.output


def test_app_url_prints_plain_url(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        state="RUNNING", serving=True, app_url="https://host/dash"
    )
    result = CliRunner().invoke(cli, ["app", "url", "dash"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == "https://host/dash"


def test_app_create_forwards_args(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--path",
            "Resources/dash.py",
            "--memory",
            "4096",
            "--cores",
            "2",
            "--environment",
            "custom-env",
        ],
    )
    assert result.exit_code == 0, result.output
    apps.create_app.assert_called_once_with(
        name="dash",
        app_path="Resources/dash.py",
        environment="custom-env",
        memory=4096,
        cores=2.0,
    )


def test_app_create_with_start_triggers_run(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app(name="dash", state="RUNNING", serving=True, app_url="https://h/x")
    apps.create_app.return_value = a
    result = CliRunner().invoke(
        cli, ["app", "create", "dash", "--path", "Resources/dash.py", "--start"]
    )
    assert result.exit_code == 0, result.output
    a.run.assert_called_once_with(await_serving=True)
    assert "https://h/x" in result.output


def test_app_start_defaults_await_serving_true(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app(state="RUNNING", serving=True, app_url="https://h/x")
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "start", "dash"])
    assert result.exit_code == 0, result.output
    a.run.assert_called_once_with(await_serving=True)


def test_app_start_no_wait_passes_false(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app()
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "start", "dash", "--no-wait"])
    assert result.exit_code == 0, result.output
    a.run.assert_called_once_with(await_serving=False)


def test_app_stop_calls_sdk(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app()
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "stop", "dash"])
    assert result.exit_code == 0, result.output
    a.stop.assert_called_once()


def test_app_delete_requires_confirmation(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app()
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "delete", "dash"], input="n\n")
    assert result.exit_code != 0
    a.delete.assert_not_called()


def test_app_delete_yes_skips_prompt(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app()
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "delete", "dash", "--yes"])
    assert result.exit_code == 0, result.output
    a.delete.assert_called_once()
    a.stop.assert_not_called()


def test_app_delete_force_stops_first(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app()
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "delete", "dash", "--yes", "--force"])
    assert result.exit_code == 0, result.output
    a.stop.assert_called_once()
    a.delete.assert_called_once()


def test_app_info_surfaces_not_found(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.side_effect = RuntimeError("404")
    result = CliRunner().invoke(cli, ["app", "info", "ghost"])
    assert result.exit_code != 0
    assert "not found" in result.output.lower()

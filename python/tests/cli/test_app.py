"""CLI tests for ``hops app`` — Python app lifecycle commands."""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


def _fake_app(**overrides):
    a = mock.MagicMock(name="App")
    a.id = overrides.get("id", 42)
    a.name = overrides.get("name", "my_app")
    a.app_kind = overrides.get("app_kind", "STREAMLIT")
    a.state = overrides.get("state", "CREATED")
    a.serving = overrides.get("serving", False)
    a.environment = overrides.get("environment", "python-app-pipeline")
    a.memory = overrides.get("memory", 2048)
    a.cores = overrides.get("cores", 1.0)
    a.app_path = overrides.get("app_path", "Resources/app.py")
    a.app_port = overrides.get("app_port")
    a.entrypoint_command = overrides.get("entrypoint_command")
    a.git_url = overrides.get("git_url")
    a.git_provider = overrides.get("git_provider")
    a.git_branch = overrides.get("git_branch")
    a.latest_commit = overrides.get("latest_commit")
    a.entrypoint_script = overrides.get("entrypoint_script")
    a.monitoring_config = overrides.get("monitoring_config")
    a.monitoringConfig = overrides.get("monitoringConfig")
    a.description = overrides.get("description")
    a.app_base_path = overrides.get("app_base_path")
    a.readiness_probe_path = overrides.get("readiness_probe_path")
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


def test_app_logs_prints_streams(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app(name="dash")
    a.get_logs.return_value = {"stdout": "hello out", "stderr": "boom err"}
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "logs", "dash"])
    assert result.exit_code == 0, result.output
    assert "hello out" in result.output
    assert "boom err" in result.output


def test_app_logs_stderr_only(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app(name="dash")
    a.get_logs.return_value = {"stdout": "hello out", "stderr": "boom err"}
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "logs", "dash", "--stream", "stderr"])
    assert result.exit_code == 0, result.output
    assert "boom err" in result.output
    assert "hello out" not in result.output


def test_app_logs_running_points_to_ui(mock_project):
    # A running app has no execution log file yet; the CLI must point at the UI
    # live logs instead of hitting the endpoint that 400s (#11).
    apps = mock_project.get_app_api.return_value
    a = _fake_app(name="dash", state="RUNNING", serving=True)
    a.get_url.return_value = "https://hopsworks.ai.local/p/119/apps"
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "logs", "dash"])
    assert result.exit_code == 0, result.output
    assert "still running" in result.output
    assert "https://hopsworks.ai.local/p/119/apps" in result.output
    a.get_logs.assert_not_called()


def test_app_logs_handles_still_running_error(mock_project):
    # Race: state looked final but the backend still rejected with 130010.
    # Fall back to the same live-logs guidance, not a raw error.
    apps = mock_project.get_app_api.return_value
    a = _fake_app(name="dash", state="CREATED", serving=False)
    a.get_url.return_value = "https://hopsworks.ai.local/p/119/apps"
    a.get_logs.side_effect = RuntimeError(
        "errorCode 130010 Job still running. Execution state is invalid."
    )
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "logs", "dash"])
    assert result.exit_code == 0, result.output
    assert "still running" in result.output


def test_app_create_drops_unsupported_kwargs(mock_project):
    """Create stays resilient when the deployed SDK predates app_kind etc."""
    apps = mock_project.get_app_api.return_value
    created = _fake_app(name="legacy")

    def old_create_app(
        name,
        app_path=None,
        environment="python-app-pipeline",
        memory=2048,
        cores=1.0,
        env_vars=None,
    ):
        return created

    apps.create_app = old_create_app
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "legacy",
            "--path",
            "Resources/app.py",
            "--app-base-path",
            "/myapp",
            "--readiness-probe-path",
            "/health",
        ],
    )
    # would TypeError on app_kind without the signature filter
    assert result.exit_code == 0, result.output


def test_app_info_shows_url(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash", state="RUNNING", serving=True, app_url="https://host/dash"
    )
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert result.exit_code == 0, result.output
    assert "https://host/dash" in result.output
    assert "RUNNING" in result.output


def test_app_info_shows_custom_metadata(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash",
        state="RUNNING",
        serving=True,
        app_kind="CUSTOM",
        app_port=8080,
        entrypoint_command='python -m uvicorn dash:app --port "$APP_PORT"',
        description="FastAPI demo",
        app_base_path="/myapp",
        readiness_probe_path="/health",
    )
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert result.exit_code == 0, result.output
    assert "CUSTOM" in result.output
    assert "8080" in result.output
    assert "FastAPI demo" in result.output
    assert "/myapp" in result.output
    assert "/health" in result.output
    assert 'python -m uvicorn dash:app --port "$APP_PORT"' in result.output


def test_app_info_shows_monitoring_routes(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash",
        state="RUNNING",
        serving=True,
        monitoringConfig={
            "enabled": True,
            "routes": [
                {"path": "/api", "matchType": "prefix"},
                {"path": "/predict", "matchType": "exact"},
            ],
        },
    )
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert result.exit_code == 0, result.output
    assert "Monitoring routes" in result.output
    assert "/api (prefix)" in result.output
    assert "/predict (exact)" in result.output


def test_app_info_shows_git_metadata(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash",
        state="RUNNING",
        serving=True,
        app_kind="STREAMLIT",
        git_url="https://github.com/gibchikafa/appshopsworkstests.git",
        git_provider="GitHub",
        git_branch="main",
        latest_commit="0123456789abcdef0123456789abcdef01234567",
        entrypoint_script="streamlitapp.py",
    )
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert result.exit_code == 0, result.output
    assert "Git repository" in result.output
    assert "https://github.com/gibchikafa/appshopsworkstests.git" in result.output
    assert "GitHub" in result.output
    assert "main" in result.output
    assert "Latest commit" in result.output
    assert "0123456789abcdef0123456789abcdef01234567" in result.output
    assert "streamlitapp.py" in result.output


def test_app_info_json_includes_custom_metadata(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash",
        state="RUNNING",
        serving=True,
        app_kind="CUSTOM",
        app_port=8080,
        entrypoint_command='python -m uvicorn dash:app --port "$APP_PORT"',
        description="FastAPI demo",
        app_base_path="/myapp",
        readiness_probe_path="/health",
    )
    result = CliRunner().invoke(cli, ["--json", "app", "info", "dash"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["app_kind"] == "CUSTOM"
    assert payload["app_port"] == 8080
    assert (
        payload["entrypoint_command"] == 'python -m uvicorn dash:app --port "$APP_PORT"'
    )
    assert payload["description"] == "FastAPI demo"
    assert payload["app_base_path"] == "/myapp"
    assert payload["readiness_probe_path"] == "/health"
    assert payload["source"] == "Project file"


def test_app_info_json_includes_git_metadata(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash",
        state="RUNNING",
        serving=True,
        app_kind="STREAMLIT",
        git_url="https://github.com/gibchikafa/appshopsworkstests.git",
        git_provider="GitHub",
        git_branch="main",
        latest_commit="0123456789abcdef0123456789abcdef01234567",
        entrypoint_script="streamlitapp.py",
    )
    result = CliRunner().invoke(cli, ["--json", "app", "info", "dash"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["source"] == "Git repository"
    assert payload["git_url"] == "https://github.com/gibchikafa/appshopsworkstests.git"
    assert payload["git_provider"] == "GitHub"
    assert payload["git_branch"] == "main"
    assert payload["latest_commit"] == "0123456789abcdef0123456789abcdef01234567"
    assert payload["entrypoint_script"] == "streamlitapp.py"


def test_app_info_json_includes_monitoring_config(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash",
        state="RUNNING",
        serving=True,
        monitoring_config={
            "enabled": True,
            "routes": [
                {"path": "/api", "matchType": "prefix"},
                {"path": "/predict", "matchType": "exact"},
            ],
        },
    )
    result = CliRunner().invoke(cli, ["--json", "app", "info", "dash"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["monitoring"] == "enabled"
    assert payload["monitoring_config"]["enabled"] is True
    assert payload["monitoring_config"]["routes"] == [
        {"path": "/api", "matchType": "prefix"},
        {"path": "/predict", "matchType": "exact"},
    ]


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
            "--app-base-path",
            "/myapp",
            "--readiness-probe-path",
            "/health",
        ],
    )
    assert result.exit_code == 0, result.output
    apps.create_app.assert_called_once_with(
        name="dash",
        app_path="Resources/dash.py",
        app_kind="STREAMLIT",
        environment="custom-env",
        memory=4096,
        cores=2.0,
        app_base_path="/myapp",
        readiness_probe_path="/health",
    )


def test_app_create_custom_forwards_args(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--app-kind",
            "CUSTOM",
            "--entrypoint-command",
            'python -m uvicorn dash:app --host 0.0.0.0 --port "$APP_PORT"',
            "--app-port",
            "8080",
            "--description",
            "FastAPI demo",
            "--memory",
            "4096",
            "--cores",
            "2",
            "--environment",
            "custom-env",
            "--app-base-path",
            "/myapp",
            "--readiness-probe-path",
            "/health",
        ],
    )
    assert result.exit_code == 0, result.output
    apps.create_app.assert_called_once_with(
        name="dash",
        app_kind="CUSTOM",
        entrypoint_command='python -m uvicorn dash:app --host 0.0.0.0 --port "$APP_PORT"',
        app_port=8080,
        description="FastAPI demo",
        environment="custom-env",
        memory=4096,
        cores=2.0,
        app_base_path="/myapp",
        readiness_probe_path="/health",
    )


def test_app_create_streamlit_git_forwards_args(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--app-kind",
            "STREAMLIT",
            "--git-url",
            "https://github.com/gibchikafa/appshopsworkstests.git",
            "--git-provider",
            "github",
            "--git-branch",
            "main",
            "--entrypoint-script",
            "streamlitapp.py",
            "--environment",
            "custom-env",
        ],
    )
    assert result.exit_code == 0, result.output
    apps.create_app.assert_called_once_with(
        name="dash",
        app_kind="STREAMLIT",
        environment="custom-env",
        memory=2048,
        cores=1.0,
        git_url="https://github.com/gibchikafa/appshopsworkstests.git",
        git_provider="GitHub",
        git_branch="main",
        entrypoint_script="streamlitapp.py",
    )


def test_app_create_custom_git_forwards_args(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--app-kind",
            "CUSTOM",
            "--git-url",
            "https://github.com/gibchikafa/appshopsworkstests.git",
            "--git-provider",
            "GitHub",
            "--git-branch",
            "main",
            "--entrypoint-command",
            'python -m uvicorn dash:app --host 0.0.0.0 --port "$APP_PORT"',
            "--app-port",
            "8080",
            "--app-base-path",
            "/myapp",
            "--readiness-probe-path",
            "/health",
        ],
    )
    assert result.exit_code == 0, result.output
    apps.create_app.assert_called_once_with(
        name="dash",
        app_kind="CUSTOM",
        environment="python-app-pipeline",
        memory=2048,
        cores=1.0,
        entrypoint_command='python -m uvicorn dash:app --host 0.0.0.0 --port "$APP_PORT"',
        app_port=8080,
        git_url="https://github.com/gibchikafa/appshopsworkstests.git",
        git_provider="GitHub",
        git_branch="main",
        app_base_path="/myapp",
        readiness_probe_path="/health",
    )


def test_app_create_streamlit_requires_path(mock_project):
    result = CliRunner().invoke(cli, ["app", "create", "dash"])
    assert result.exit_code != 0
    assert "Streamlit apps require --path" in result.output


def test_app_create_custom_requires_entrypoint(mock_project):
    result = CliRunner().invoke(cli, ["app", "create", "dash", "--app-kind", "CUSTOM"])
    assert result.exit_code != 0
    assert "Custom apps require --entrypoint-command" in result.output


def test_app_create_streamlit_git_requires_script(mock_project):
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--git-url",
            "https://github.com/gibchikafa/appshopsworkstests.git",
            "--git-provider",
            "GitHub",
        ],
    )
    assert result.exit_code != 0
    assert "require --entrypoint-script" in result.output


def test_app_create_with_start_triggers_run(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app(name="dash", state="RUNNING", serving=True, app_url="https://h/x")
    apps.create_app.return_value = a
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--path",
            "Resources/dash.py",
            "--start",
        ],
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


def test_app_redeploy_defaults_await_serving_true(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app(state="RUNNING", serving=True, app_url="https://h/x")
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "redeploy", "dash"])
    assert result.exit_code == 0, result.output
    a.redeploy.assert_called_once_with(await_serving=True)


def test_app_redeploy_no_wait_passes_false(mock_project):
    apps = mock_project.get_app_api.return_value
    a = _fake_app()
    apps.get_app.return_value = a
    result = CliRunner().invoke(cli, ["app", "redeploy", "dash", "--no-wait"])
    assert result.exit_code == 0, result.output
    a.redeploy.assert_called_once_with(await_serving=False)


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

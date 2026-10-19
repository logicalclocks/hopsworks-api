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
    a.proxy_path_mode = overrides.get("proxy_path_mode")
    a.readiness_probe_path = overrides.get("readiness_probe_path")
    a.db_access = overrides.get("db_access", True)
    a.idle_scale_to_zero = overrides.get("idle_scale_to_zero", False)
    a.idle_cooldown_seconds = overrides.get("idle_cooldown_seconds")
    a.cold_start_timeout_seconds = overrides.get("cold_start_timeout_seconds")
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


def test_app_info_shows_the_routing_mode(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(name="dash", proxy_path_mode="PREFIX")
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert result.exit_code == 0, result.output
    assert "compatibility prefix" in result.output
    apps.get_app.return_value = _fake_app(name="dash", proxy_path_mode="ROOT")
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert "root (the app serves at its base path)" in result.output


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
        proxy_path_mode="ROOT",
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
    assert payload["proxy_path_mode"] == "ROOT"
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
            "--env",
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


def test_app_create_no_db_access(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        ["app", "create", "dash", "--path", "Resources/dash.py", "--no-db-access"],
    )
    assert result.exit_code == 0, result.output
    assert apps.create_app.call_args.kwargs["db_access"] is False


def test_app_create_idle_scale_to_zero_forwards_the_timeouts(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--path",
            "Resources/app.py",
            "--idle-scale-to-zero",
            "--idle-cooldown-seconds",
            "120",
            "--cold-start-timeout-seconds",
            "45",
        ],
    )
    assert result.exit_code == 0, result.output
    kwargs = apps.create_app.call_args.kwargs
    assert kwargs["idle_scale_to_zero"] is True
    assert kwargs["idle_cooldown_seconds"] == 120
    assert kwargs["cold_start_timeout_seconds"] == 45


def test_app_create_idle_timeouts_reject_the_option_off(mock_project):
    apps = mock_project.get_app_api.return_value
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--path",
            "Resources/app.py",
            "--no-idle-scale-to-zero",
            "--idle-cooldown-seconds",
            "120",
        ],
    )
    assert result.exit_code != 0
    assert "--no-idle-scale-to-zero" in result.output
    apps.create_app.assert_not_called()


def test_app_create_idle_timeouts_alone_are_forwarded(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--path",
            "Resources/app.py",
            "--idle-cooldown-seconds",
            "120",
        ],
    )
    assert result.exit_code == 0, result.output
    kwargs = apps.create_app.call_args.kwargs
    assert "idle_scale_to_zero" not in kwargs
    assert kwargs["idle_cooldown_seconds"] == 120


def test_app_create_leaves_idle_scale_to_zero_to_the_cluster_default(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli, ["app", "create", "dash", "--path", "Resources/app.py"]
    )
    assert result.exit_code == 0, result.output
    assert "idle_scale_to_zero" not in apps.create_app.call_args.kwargs


def test_app_create_no_idle_scale_to_zero_opts_out(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "dash",
            "--path",
            "Resources/app.py",
            "--no-idle-scale-to-zero",
        ],
    )
    assert result.exit_code == 0, result.output
    assert apps.create_app.call_args.kwargs["idle_scale_to_zero"] is False


def test_app_info_shows_idle_scale_to_zero(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.get_app.return_value = _fake_app(
        name="dash",
        idle_scale_to_zero=True,
        idle_cooldown_seconds=120,
        cold_start_timeout_seconds=45,
    )
    result = CliRunner().invoke(cli, ["app", "info", "dash"])
    assert result.exit_code == 0, result.output
    assert "Scale to zero when idle" in result.output
    assert "after 120 s idle" in result.output
    assert "wakes within 45 s" in result.output


def test_app_create_defaults_to_db_access(mock_project):
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="dash")
    result = CliRunner().invoke(
        cli, ["app", "create", "dash", "--path", "Resources/dash.py"]
    )
    assert result.exit_code == 0, result.output
    assert "db_access" not in apps.create_app.call_args.kwargs


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
            "--env",
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


def test_app_create_flask_git_forwards_entrypoint_script(mock_project):
    """A Flask git app is launched from its file: an entrypoint script and a port, no command."""
    apps = mock_project.get_app_api.return_value
    apps.create_app.return_value = _fake_app(name="web", app_kind="FLASK")
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "web",
            "--app-kind",
            "FLASK",
            "--git-url",
            "https://github.com/gibchikafa/appshopsworkstests.git",
            "--git-provider",
            "GitHub",
            "--entrypoint-script",
            "flaskapp.py",
            "--app-port",
            "5000",
        ],
    )
    assert result.exit_code == 0, result.output
    apps.create_app.assert_called_once_with(
        name="web",
        app_kind="FLASK",
        environment="python-app-pipeline",
        memory=2048,
        cores=1.0,
        app_port=5000,
        git_url="https://github.com/gibchikafa/appshopsworkstests.git",
        git_provider="GitHub",
        entrypoint_script="flaskapp.py",
    )


def test_app_create_nodejs_rejects_an_entrypoint_command(mock_project):
    result = CliRunner().invoke(
        cli,
        [
            "app",
            "create",
            "api",
            "--app-kind",
            "NODEJS",
            "--path",
            "Resources/server.js",
            "--entrypoint-command",
            "node server.js",
        ],
    )
    assert result.exit_code != 0
    assert "Node.js apps do not accept --entrypoint-command" in result.output
    mock_project.get_app_api.return_value.create_app.assert_not_called()


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
            "--env",
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
    assert "could not find app 'ghost'" in result.output.lower()


# --- deploy ------------------------------------------------------------------

_SOURCE = "/Projects/demo/Users/scott/apps/shop/app.py"
_URL = "https://h/hopsworks-api/pythonapp/demo/shop/"


def _app_dir(tmp_path, requirements=None):
    root = tmp_path / "shop"
    (root / "static").mkdir(parents=True)
    (root / "app.py").write_text("app")
    (root / "static" / "index.html").write_text("<p>hi</p>")
    (root / ".env").write_text("SECRET=1")
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "app.cpython-312.pyc").write_bytes(b"\0")
    if requirements is not None:
        (root / "app-requirements.txt").write_text(requirements)
    return root


def _deploy(
    mock_project, root, *args, existing=None, polls=(), remote=None, json_mode=False
):
    """Run `hops app deploy shop <root>` against fakes.

    `existing` is the app before the deploy, `polls` what each later `get_app`
    returns, and `remote` the text of the requirements uploaded last time.
    """
    apps = mock_project.get_app_api.return_value
    apps.get_app.side_effect = [existing, *polls]
    dataset_api = mock_project.get_dataset_api.return_value
    present = {"Users/scott"}
    if remote is not None:
        present.add("Users/scott/apps/shop/app-requirements.txt")
    dataset_api.exists.side_effect = present.__contains__
    dataset_api.mkdir.side_effect = present.add
    dataset_api.read_content.return_value = (
        mock.MagicMock(content=remote.encode()) if remote is not None else None
    )
    user = mock.MagicMock()
    user.username = "scott"
    users_api = mock.MagicMock()
    users_api._get_current_user.return_value = user
    argv = [
        *(["--json"] if json_mode else []),
        "app",
        "deploy",
        "shop",
        str(root),
        *args,
    ]
    with (
        mock.patch("hopsworks.get_users_api", return_value=users_api, create=True),
        mock.patch("hopsworks.cli.commands.app._POLL_SECONDS", 0),
    ):
        result = CliRunner().invoke(cli, argv)
    return result, apps, dataset_api


def _app(execution_id, state="RUNNING", serving=True, env="python-agent-pipeline"):
    a = _fake_app(
        name="shop",
        state=state,
        serving=serving,
        app_url=_URL,
        app_path=f"hdfs://{_SOURCE}",
    )
    a.execution_id = execution_id
    a.environment_name = env
    return a


def test_deploy_creates_the_app_and_prints_the_url_last(mock_project, tmp_path):
    root = _app_dir(tmp_path)
    created = _fake_app(name="shop")
    mock_project.get_app_api.return_value.create_app.return_value = created
    result, apps, dataset_api = _deploy(
        mock_project, root, polls=[_app(5, "INITIALIZING", serving=False), _app(5)]
    )
    assert result.exit_code == 0, result.output
    assert result.stdout.strip().splitlines()[-1] == _URL
    kwargs = apps.create_app.call_args.kwargs
    assert kwargs == {
        "name": "shop",
        "app_kind": "CUSTOM",
        "app_path": _SOURCE,
        "entrypoint_command": "python app.py",
        "app_port": 8080,
        "readiness_probe_path": "/health",
        "environment": "python-agent-pipeline",
    }
    created.run.assert_called_once_with(await_serving=False)
    uploads = [
        (c.kwargs["local_path"], c.kwargs["upload_path"], c.kwargs["overwrite"])
        for c in dataset_api.upload.call_args_list
    ]
    assert uploads == [
        (str(root / "app.py"), "Users/scott/apps/shop", True),
        (str(root / "static" / "index.html"), "Users/scott/apps/shop/static", True),
    ]
    assert [c.args[0] for c in dataset_api.mkdir.call_args_list] == [
        "Users/scott/apps",
        "Users/scott/apps/shop",
        "Users/scott/apps/shop/static",
    ]


def test_deploy_waits_for_the_new_execution_of_a_running_app(mock_project, tmp_path):
    root = _app_dir(tmp_path)
    running = _app(3)
    result, apps, _ = _deploy(
        mock_project, root, existing=running, polls=[_app(3), _app(4)]
    )
    assert result.exit_code == 0, result.output
    running.redeploy.assert_called_once_with(await_serving=False)
    apps.create_app.assert_not_called()
    assert apps.get_app.call_count == 3


def test_deploy_starts_a_stopped_app(mock_project, tmp_path):
    root = _app_dir(tmp_path)
    stopped = _app(3, "KILLED", serving=False)
    result, _, _ = _deploy(mock_project, root, existing=stopped, polls=[_app(4)])
    assert result.exit_code == 0, result.output
    stopped.run.assert_called_once_with(await_serving=False)
    stopped.redeploy.assert_not_called()


def test_deploy_fails_when_the_new_execution_fails(mock_project, tmp_path):
    root = _app_dir(tmp_path)
    result, _, _ = _deploy(
        mock_project, root, existing=_app(3), polls=[_app(4, "FAILED", serving=False)]
    )
    assert result.exit_code == 1
    assert "Could not deploy app 'shop': it stopped in state FAILED" in result.output
    assert "hops app logs shop" in result.output


def test_deploy_refuses_an_app_in_another_environment(mock_project, tmp_path):
    root = _app_dir(tmp_path)
    result, _, dataset_api = _deploy(
        mock_project, root, existing=_app(3, env="python-app-pipeline")
    )
    assert result.exit_code == 1
    assert "--env python-app-pipeline" in result.output
    dataset_api.upload.assert_not_called()


def test_deploy_needs_app_py(mock_project, tmp_path):
    (tmp_path / "empty").mkdir()
    result = CliRunner().invoke(cli, ["app", "deploy", "shop", str(tmp_path / "empty")])
    assert result.exit_code == 2
    assert "has no app.py" in result.output


def test_deploy_clones_and_installs_the_requirements(mock_project, tmp_path):
    root = _app_dir(tmp_path, requirements="polars==1.0\n")
    mock_project.get_app_api.return_value.create_app.return_value = _fake_app()
    env_api = mock_project.get_environment_api.return_value
    env_api.get_environment.return_value = None
    clone = env_api.create_environment.return_value
    result, apps, _ = _deploy(mock_project, root, polls=[_app(1, env="shop-app-env")])
    assert result.exit_code == 0, result.output
    env_api.create_environment.assert_called_once_with(
        "shop-app-env",
        base_environment_name="python-agent-pipeline",
        await_creation=True,
    )
    clone.install_requirements.assert_called_once_with(
        "/Projects/demo/Users/scott/apps/shop/app-requirements.txt",
        await_installation=True,
    )
    assert apps.create_app.call_args.kwargs["environment"] == "shop-app-env"


def test_deploy_skips_the_install_when_the_requirements_did_not_change(
    mock_project, tmp_path
):
    root = _app_dir(tmp_path, requirements="polars==1.0\n")
    env_api = mock_project.get_environment_api.return_value
    clone = env_api.get_environment.return_value
    result, _, _ = _deploy(
        mock_project,
        root,
        existing=_app(3, env="shop-app-env"),
        polls=[_app(4, env="shop-app-env")],
        remote="polars==1.0\n",
    )
    assert result.exit_code == 0, result.output
    clone.install_requirements.assert_not_called()
    env_api.create_environment.assert_not_called()


def test_deploy_reinstalls_changed_requirements(mock_project, tmp_path):
    root = _app_dir(tmp_path, requirements="polars==1.1\n")
    env_api = mock_project.get_environment_api.return_value
    clone = env_api.get_environment.return_value
    result, _, _ = _deploy(
        mock_project,
        root,
        existing=_app(3, env="shop-app-env"),
        polls=[_app(4, env="shop-app-env")],
        remote="polars==1.0\n",
    )
    assert result.exit_code == 0, result.output
    clone.install_requirements.assert_called_once()


def test_deploy_json_lists_every_file_with_its_sha256(mock_project, tmp_path):
    import hashlib

    root = _app_dir(tmp_path)
    mock_project.get_app_api.return_value.create_app.return_value = _fake_app()
    result, _, _ = _deploy(mock_project, root, polls=[_app(1)], json_mode=True)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["url"] == _URL
    assert payload["environment"] == "python-agent-pipeline"
    assert payload["source"] == "Users/scott/apps/shop"
    assert payload["files"] == [
        {"path": "app.py", "sha256": hashlib.sha256(b"app").hexdigest()},
        {
            "path": "static/index.html",
            "sha256": hashlib.sha256(b"<p>hi</p>").hexdigest(),
        },
    ]


def test_deploy_checks_a_route_through_the_proxy(mock_project, tmp_path):
    root = _app_dir(tmp_path)
    mock_project.id = 119
    mock_project.get_app_api.return_value.create_app.return_value = _fake_app()
    proxy_client = mock.MagicMock(_verify=False)
    proxy_client._send_request.return_value = {"token": "jwt"}
    answer = mock.MagicMock(ok=False, status_code=500, text="Internal Server Error")
    with (
        mock.patch("hopsworks_common.client._get_instance", return_value=proxy_client),
        mock.patch("requests.get", return_value=answer) as get,
    ):
        result, _, _ = _deploy(
            mock_project, root, "--check", "api/top", polls=[_app(1)]
        )
    assert result.exit_code == 1
    assert "api/top answered 500" in result.output
    assert proxy_client._send_request.call_args.args == (
        "POST",
        ["project", 119, "apps", "shop", "proxy-token"],
    )
    assert get.call_args.args == (_URL + "api/top",)
    assert get.call_args.kwargs["cookies"] == {"proxy_session": "jwt"}

"""``hops app`` — Python app lifecycle.

Wraps the SDK's ``project.get_app_api()``: list, info, create, deploy, start, redeploy,
stop, delete, plus a convenience ``url`` that prints the public serving URL.
App scripts can either live in HopsFS or in a Git repository. File-backed
Streamlit apps use ``--path``; git-backed Streamlit apps use ``--git-url`` and
``--entrypoint-script``; custom apps use ``--entrypoint-command``. Use
``--app-base-path`` to mount the app at ``/`` or a subpath like ``/myapp``,
and ``--readiness-probe-path`` to override the readiness probe when needed.
Apps get the project's online feature store database by default (created on
demand at start, ``MYSQL_*`` env vars injected); pass ``--no-db-access`` to opt
out. When Trino is enabled, every app also gets the ``TRINO_*`` env vars for the
offline tables, independently of that flag.
App metadata can also carry monitoring config (``enabled`` plus optional
``routes`` with ``path`` and ``matchType``), and ``hops app info`` prints the
monitoring state and route list when it is present. Legacy apps that still
depend on ``APP_BASE_URL_PATH`` should be migrated to ``Root routing`` in the
app settings UI once the app code no longer needs the legacy prefix behavior.
"""

from __future__ import annotations

import inspect
from typing import Any

import click
from hopsworks.cli import output, session


@click.group("app")
def app_group() -> None:
    """Apps: Python web apps served by the cluster."""


@app_group.command("list")
@click.pass_context
def app_list(ctx: click.Context) -> None:
    """List every app in the active project.

    \f
    Args:
        ctx: Click context.
    """
    apps = _get_app_api(ctx)
    try:
        items = apps.get_apps()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list apps: {exc}") from exc

    rows = []
    for a in items or []:
        rows.append(
            [
                getattr(a, "id", "?"),
                getattr(a, "name", "?"),
                getattr(a, "state", "-"),
                "yes" if getattr(a, "serving", False) else "no",
                getattr(a, "environment_name", None) or "-",
            ]
        )
    output.print_table(["ID", "NAME", "STATE", "SERVING", "ENVIRONMENT"], rows)


@app_group.command("info")
@click.argument("name")
@click.pass_context
def app_info(ctx: click.Context, name: str) -> None:
    """Show details for a single app.

    \f
    Args:
        ctx: Click context.
        name: App name.
    """
    a = _get_app(ctx, name)
    if output.JSON_MODE:
        output.print_json(_app_to_dict(a))
        return

    rows = [
        ["ID", getattr(a, "id", "?")],
        ["Name", getattr(a, "name", "?")],
        ["Type", getattr(a, "app_kind", "-")],
        ["Source", _app_source(a)],
        ["State", getattr(a, "state", "-")],
        ["Serving", "yes" if getattr(a, "serving", False) else "no"],
        ["Environment", getattr(a, "environment_name", None) or "-"],
        ["Memory (MB)", getattr(a, "memory_requested", None) or "-"],
        ["Cores", getattr(a, "cpu_requested", None) or "-"],
        ["Path", getattr(a, "app_path", None) or "-"],
        ["Port", getattr(a, "app_port", None) or "-"],
        ["Git URL", getattr(a, "git_url", None) or "-"],
        ["Git provider", getattr(a, "git_provider", None) or "-"],
        [
            "Git branch",
            getattr(a, "git_branch", None) or getattr(a, "latest_branch", None) or "-",
        ],
        ["Auto-redeploy", _auto_redeploy_text(a)],
        ["Latest commit", getattr(a, "latest_commit", None) or "-"],
        ["Entrypoint script", getattr(a, "entrypoint_script", None) or "-"],
        ["Entrypoint", getattr(a, "entrypoint_command", None) or "-"],
        ["App base path", getattr(a, "app_base_path", None) or "-"],
        ["Routing", _routing_text(a)],
        ["Readiness", getattr(a, "readiness_probe_path", None) or "Default"],
        ["Database access", "Yes" if getattr(a, "db_access", True) else "No"],
        ["Scale to zero when idle", _idle_text(a)],
        ["Monitoring", _monitoring_state_text(a)],
        ["Monitoring routes", _monitoring_routes_text(a)],
        ["Description", getattr(a, "description", None) or "-"],
        ["URL", getattr(a, "app_url", None) or "-"],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@app_group.command("url")
@click.argument("name")
@click.pass_context
def app_url(ctx: click.Context, name: str) -> None:
    """Print the public serving URL of a running app.

    Exits non-zero with a clear message when the app is not serving yet,
    so this composes cleanly in shell pipelines.

    \f
    Args:
        ctx: Click context.
        name: App name.
    """
    a = _get_app(ctx, name)
    url = getattr(a, "app_url", None)
    if not url:
        raise click.ClickException(
            f"App '{name}' has no URL yet (state={getattr(a, 'state', '-')}, "
            f"serving={getattr(a, 'serving', False)})."
        )
    if output.JSON_MODE:
        output.print_json({"url": url})
    else:
        click.echo(url)


@app_group.command("logs")
@click.argument("name")
@click.option(
    "--stream",
    type=click.Choice(["stdout", "stderr", "both"]),
    default="both",
    show_default=True,
    help="Which stream to print.",
)
@click.pass_context
def app_logs(ctx: click.Context, name: str, stream: str) -> None:
    """Print stdout/stderr logs for the app's latest execution.

    Reads the execution's log file, which the backend writes only when the
    execution stops. While the app is still running that file does not exist,
    and the backend rejects the request. A long-running Streamlit app is the
    case where you most want logs, so this points at the live logs in the
    Hopsworks UI instead of failing with an opaque error.

    \f
    Args:
        ctx: Click context.
        name: App name.
        stream: Which stream(s) to print: `stdout`, `stderr`, or `both`.
    """
    a = _get_app(ctx, name)

    if _app_is_running(a):
        _report_running(name, a)
        return

    try:
        logs = a.get_logs()
    except Exception as exc:  # noqa: BLE001
        # Race: the app was running when the backend checked. The log file is
        # written only on stop, so give the live-logs guidance, not a raw 400.
        if _is_still_running_error(exc):
            _report_running(name, a)
            return
        raise click.ClickException(
            f"Could not read logs for app '{name}': {exc}"
        ) from exc

    if output.JSON_MODE:
        output.print_json(logs if stream == "both" else {stream: logs.get(stream, "")})
        return

    if stream in ("stdout", "both"):
        click.echo("=== stdout ===")
        click.echo(logs.get("stdout") or "(empty)")
    if stream in ("stderr", "both"):
        click.echo("=== stderr ===")
        click.echo(logs.get("stderr") or "(empty)")


# Execution states in which the log file is not written yet, so the
# execution-log endpoint returns a "still running" error (RESTCode 130010).
_RUNNING_APP_STATES = frozenset(
    {
        "NEW",
        "NEW_SAVING",
        "SUBMITTED",
        "ACCEPTED",
        "RUNNING",
        "INITIALIZING",
        "STARTING_APP_MASTER",
        "AGGREGATING_LOGS",
        "DEPLOYING",
    }
)


def _app_is_running(a: Any) -> bool:
    """Whether the app's execution has not reached a final state."""
    if getattr(a, "serving", False):
        return True
    return (getattr(a, "state", None) or "").upper() in _RUNNING_APP_STATES


def _is_still_running_error(exc: Exception) -> bool:
    """Whether ``exc`` is the backend's "execution still running" rejection."""
    text = str(exc).lower()
    return (
        "130010" in text
        or "still running" in text
        or "execution state is invalid" in text
    )


def _report_running(name: str, a: Any) -> None:
    """Explain that file logs wait for stop, and link the UI live logs."""
    url = a.get_url()
    msg = (
        f"App '{name}' is still running; its execution log file is written "
        f"only after it stops. View live logs in the Hopsworks UI: {url}"
    )
    if output.JSON_MODE:
        output.print_json({"running": True, "message": msg, "url": url})
        return
    click.echo(msg)


@app_group.command("create")
@click.argument("name")
@click.option(
    "--path",
    "app_path",
    help=(
        "HopsFS path to the app file (required for Streamlit apps; optional "
        "for custom apps)."
    ),
)
@click.option(
    "--app-kind",
    type=click.Choice(
        ["STREAMLIT", "FLASK", "GRADIO", "NODEJS", "CUSTOM"], case_sensitive=False
    ),
    default="STREAMLIT",
    show_default=True,
    help="App type to create.",
)
@click.option(
    "--entrypoint-command",
    default=None,
    help="Startup command for custom apps.",
)
@click.option(
    "--app-port",
    type=int,
    default=None,
    help="Port the app listens on, for every kind but STREAMLIT.",
)
@click.option(
    "--git-url",
    default=None,
    help="Git repository URL to clone on every app start.",
)
@click.option(
    "--git-provider",
    type=click.Choice(["GitHub", "GitLab", "BitBucket"], case_sensitive=False),
    default=None,
    help="Git provider for git-backed apps.",
)
@click.option(
    "--git-branch",
    default=None,
    help="Optional Git branch to clone.",
)
@click.option(
    "--git-auto-redeploy",
    is_flag=True,
    default=False,
    help="Roll the app to the branch HEAD whenever a new commit is pushed.",
)
@click.option(
    "--entrypoint-script",
    default=None,
    help="App file relative to the repository root, for Git repository apps of every kind but CUSTOM.",
)
@click.option(
    "--app-base-path",
    default=None,
    help="Public mount path for the app, for example / or /myapp.",
)
@click.option(
    "--readiness-probe-path",
    default=None,
    help="Optional readiness probe path override.",
)
@click.option(
    "--db-access/--no-db-access",
    default=True,
    show_default=True,
    help=(
        "Give the app access to the project's online feature store database: "
        "create it on demand at start and inject the MYSQL_* env vars. Trino access "
        "to the offline tables (TRINO_*) does not depend on this flag."
    ),
)
@click.option(
    "--idle-scale-to-zero/--no-idle-scale-to-zero",
    default=None,
    help=(
        "Scale the app to zero when idle: the pod goes away after a quiet period and "
        "comes back on the next request. On by default wherever the cluster has KEDA; "
        "--no-idle-scale-to-zero keeps the app always on."
    ),
)
@click.option(
    "--idle-cooldown-seconds",
    type=int,
    default=None,
    help="Seconds without a request before an idle app scales to zero (cluster default: 21600, six hours). Turns the option on.",
)
@click.option(
    "--cold-start-timeout-seconds",
    type=int,
    default=None,
    help="Seconds the first request waits for an idle app to wake (backend default and ceiling: the cluster's). Turns the option on.",
)
@click.option(
    "--description",
    default=None,
    help="Optional app description.",
)
@click.option(
    "--env",
    "environment",
    default="python-app-pipeline",
    show_default=True,
    help="Python environment name backing the app.",
)
@click.option(
    "--memory", type=int, default=2048, show_default=True, help="Memory in MB."
)
@click.option("--cores", type=float, default=1.0, show_default=True, help="CPU cores.")
@click.option(
    "--start",
    is_flag=True,
    help="Start the app immediately after creation (blocks until serving).",
)
@click.pass_context
def app_create(
    ctx: click.Context,
    name: str,
    app_path: str | None,
    app_kind: str,
    entrypoint_command: str | None,
    app_port: int | None,
    git_url: str | None,
    git_provider: str | None,
    git_branch: str | None,
    git_auto_redeploy: bool,
    entrypoint_script: str | None,
    app_base_path: str | None,
    readiness_probe_path: str | None,
    db_access: bool,
    idle_scale_to_zero: bool | None,
    idle_cooldown_seconds: int | None,
    cold_start_timeout_seconds: int | None,
    description: str | None,
    environment: str,
    memory: int,
    cores: float,
    start: bool,
) -> None:
    """Create a new app.

    Monitoring is enabled by default in the backend. Route filters are optional
    and narrow the traffic counted by Envoy.

    \f
    Args:
        ctx: Click context.
        name: App name.
        app_path: HopsFS path to the app script.
        app_kind: App type.
        entrypoint_command: Startup command for custom apps.
        app_port: Port the app listens on, for every kind but Streamlit.
        git_url: Git repository URL for git-backed apps.
        git_provider: Git provider for git-backed apps.
        git_branch: Optional Git branch.
        git_auto_redeploy: Roll the app to the branch HEAD on new commits.
        entrypoint_script: App file relative to the repository root, for git apps of every kind but custom.
        app_base_path: Public mount path for the app.
        readiness_probe_path: Optional readiness probe path override.
        db_access: Whether the app gets the project's online feature store
            database (MYSQL_*). Trino access (TRINO_*) does not depend on it.
        idle_scale_to_zero: Scale the app to zero when idle and wake it on the next request;
            None leaves the choice to the cluster (on wherever it has KEDA).
        idle_cooldown_seconds: Quiet seconds before an idle app scales to zero.
        cold_start_timeout_seconds: Seconds the first request waits for the app to wake.
        description: Optional app description.
        environment: Python environment name.
        memory: Memory in MB.
        cores: CPU cores.
        start: When True, start the app and wait for serving.
    """
    app_kind = app_kind.upper()
    git_repo_app = bool(git_url)
    # Every kind but CUSTOM is launched from its app file with a command the backend generates.
    if app_kind != "CUSTOM":
        label = app_kind.capitalize() if app_kind != "NODEJS" else "Node.js"
        if entrypoint_command:
            raise click.UsageError(f"{label} apps do not accept --entrypoint-command.")
        if git_repo_app:
            if not git_provider:
                raise click.UsageError(
                    f"{label} Git repository apps require --git-provider."
                )
            if not entrypoint_script:
                raise click.UsageError(
                    f"{label} Git repository apps require --entrypoint-script."
                )
        elif not app_path:
            raise click.UsageError(f"{label} apps require --path.")
        elif entrypoint_script:
            raise click.UsageError(
                "--entrypoint-script is only supported for Git repository apps."
            )
    else:
        if not entrypoint_command:
            raise click.UsageError("Custom apps require --entrypoint-command.")
        if git_repo_app and not git_provider:
            raise click.UsageError("Git repository apps require --git-provider.")
        if entrypoint_script:
            raise click.UsageError(
                "--entrypoint-script is only supported for Git repository apps of the other kinds."
            )

    apps = _get_app_api(ctx)
    create_kwargs: dict[str, Any] = {
        "name": name,
        "app_kind": app_kind,
        "environment": environment,
        "memory": memory,
        "cores": cores,
    }
    if description is not None:
        create_kwargs["description"] = description
    if app_path is not None:
        create_kwargs["app_path"] = app_path
    if entrypoint_command is not None:
        create_kwargs["entrypoint_command"] = entrypoint_command
    if app_port is not None:
        create_kwargs["app_port"] = app_port
    if git_url is not None:
        create_kwargs["git_url"] = git_url
    if git_provider is not None:
        create_kwargs["git_provider"] = git_provider
    if git_branch is not None:
        create_kwargs["git_branch"] = git_branch
    if git_auto_redeploy:
        create_kwargs["git_auto_redeploy"] = True
    if entrypoint_script is not None:
        create_kwargs["entrypoint_script"] = entrypoint_script
    if app_base_path is not None:
        create_kwargs["app_base_path"] = app_base_path
    if readiness_probe_path is not None:
        create_kwargs["readiness_probe_path"] = readiness_probe_path
    if not db_access:
        create_kwargs["db_access"] = False
    has_idle_timeouts = (
        idle_cooldown_seconds is not None or cold_start_timeout_seconds is not None
    )
    if idle_scale_to_zero is False and has_idle_timeouts:
        raise click.UsageError(
            "--idle-cooldown-seconds and --cold-start-timeout-seconds cannot go with --no-idle-scale-to-zero."
        )
    if idle_scale_to_zero is not None:
        create_kwargs["idle_scale_to_zero"] = idle_scale_to_zero
    if idle_cooldown_seconds is not None:
        create_kwargs["idle_cooldown_seconds"] = idle_cooldown_seconds
    if cold_start_timeout_seconds is not None:
        create_kwargs["cold_start_timeout_seconds"] = cold_start_timeout_seconds
    create_kwargs = _accepted_kwargs(apps.create_app, create_kwargs)
    try:
        a = apps.create_app(**create_kwargs)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not create app: {exc}") from exc

    output.success("✓ Created app %s", getattr(a, "name", name))
    if start:
        _start(a)


@app_group.command("start")
@click.argument("name")
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait until the app is serving.",
)
@click.pass_context
def app_start(ctx: click.Context, name: str, wait: bool) -> None:
    """Start an app.

    Blocks until the app is `serving` unless `--no-wait` is set.

    \f
    Args:
        ctx: Click context.
        name: App name.
        wait: Whether to wait until the app is serving.
    """
    a = _get_app(ctx, name)
    _start(a, await_serving=wait)


@app_group.command("redeploy")
@click.argument("name")
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait until the app is serving.",
)
@click.pass_context
def app_redeploy(ctx: click.Context, name: str, wait: bool) -> None:
    """Redeploy a running app.

    Blocks until the app is `serving` unless `--no-wait` is set.

    \f
    Args:
        ctx: Click context.
        name: App name.
        wait: Whether to wait until the app is serving.
    """
    a = _get_app(ctx, name)
    _redeploy(a, await_serving=wait)


_DEPLOY_BASE_ENV = "python-agent-pipeline"
_DEPLOY_REQUIREMENTS = "app-requirements.txt"
_DEPLOY_PORT = 8080
_POLL_SECONDS = 3.0


@app_group.command("deploy")
@click.argument("name")
@click.argument(
    "local_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
@click.option(
    "--env",
    "environment",
    default=None,
    help=(
        f"Python environment of the app. Default: <name>-app-env cloned from "
        f"{_DEPLOY_BASE_ENV} when LOCAL_DIR has {_DEPLOY_REQUIREMENTS}, "
        f"else {_DEPLOY_BASE_ENV}."
    ),
)
@click.option(
    "--timeout",
    type=int,
    default=600,
    show_default=True,
    help="Seconds to wait for the app to serve the new files.",
)
@click.option(
    "--check",
    "check_path",
    default=None,
    help="A route to fetch through the Hopsworks proxy once the app serves (api/top); a non-2xx answer fails the deploy.",
)
@click.pass_context
def app_deploy(
    ctx: click.Context,
    name: str,
    local_dir: str,
    environment: str | None,
    timeout: int,
    check_path: str | None,
) -> None:
    """Create or update an app from a local directory.

    Uploads LOCAL_DIR to `Users/<user>/apps/<name>/` (overwriting, skipping
    dotfiles and `__pycache__`), prepares the environment, creates the app
    (`app.py` as a custom app on port 8080, readiness on `/health`) or
    redeploys it, and waits until the new execution serves. Prints the URL
    on the last line; with `--json`, the app and every file's sha256.

    \f
    Args:
        ctx: Click context.
        name: App name.
        local_dir: Directory holding `app.py`.
        environment: Python environment; derived from LOCAL_DIR when None.
        timeout: Seconds to wait for serving.
        check_path: A route to fetch through the proxy after serving.
    """
    import hashlib
    import os
    from pathlib import Path

    import hopsworks
    from hopsworks.cli.commands.files import makedirs

    if not os.path.isfile(os.path.join(local_dir, "app.py")):
        raise click.UsageError(f"{local_dir} has no app.py.")
    files = _deploy_files(local_dir)
    project = session.get_project(ctx)
    try:
        username = hopsworks.get_users_api()._get_current_user().username
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not resolve your username: {exc}") from exc
    remote_dir = f"Users/{username}/apps/{name}"
    app_path = f"/Projects/{project.name}/{remote_dir}/app.py"
    has_requirements = _DEPLOY_REQUIREMENTS in files
    env_name = environment or (
        f"{name}-app-env" if has_requirements else _DEPLOY_BASE_ENV
    )

    apps = project.get_app_api()
    try:
        existing = apps.get_app(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not find app '{name}': {exc}") from exc
    if existing is not None:
        # The SDK cannot change an app's file or environment, so a mismatch
        # would keep serving something else than what was uploaded.
        if not (getattr(existing, "app_path", None) or "").endswith(
            f"{remote_dir}/app.py"
        ):
            raise click.ClickException(
                f"App '{name}' runs {existing.app_path}, not {app_path}. "
                f"Pick another name, or `hops app delete {name} --yes --force` first."
            )
        if getattr(existing, "environment_name", None) != env_name:
            raise click.ClickException(
                f"App '{name}' runs in environment '{existing.environment_name}', "
                f"not '{env_name}'. Pass --env {existing.environment_name}, or "
                f"`hops app delete {name} --yes --force` first."
            )

    dataset_api = project.get_dataset_api()
    requirements_remote = f"{remote_dir}/{_DEPLOY_REQUIREMENTS}"
    try:
        requirements_changed = has_requirements and _remote_text(
            dataset_api, requirements_remote
        ) != _local_text(os.path.join(local_dir, _DEPLOY_REQUIREMENTS))
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not read {requirements_remote}: {exc}"
        ) from exc

    try:
        for relative in sorted({os.path.dirname(f) for f in files}):
            makedirs(dataset_api, f"{remote_dir}/{relative}".rstrip("/"))
        for relative in files:
            parent = f"{remote_dir}/{os.path.dirname(relative)}".rstrip("/")
            dataset_api.upload(
                local_path=os.path.join(local_dir, relative),
                upload_path=parent,
                overwrite=True,
            )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not upload app '{name}' to {remote_dir}: {exc}"
        ) from exc
    output.success("✓ Uploaded %d files to %s", len(files), remote_dir)

    if environment is None and has_requirements:
        _deploy_environment(
            project.get_environment_api(),
            env_name,
            f"/Projects/{project.name}/{requirements_remote}",
            requirements_changed,
        )

    previous = None
    try:
        if existing is None:
            a = apps.create_app(
                **_accepted_kwargs(
                    apps.create_app,
                    {
                        "name": name,
                        "app_kind": "CUSTOM",
                        "app_path": app_path,
                        "entrypoint_command": "python app.py",
                        "app_port": _DEPLOY_PORT,
                        "readiness_probe_path": "/health",
                        "environment": env_name,
                    },
                )
            )
            output.success("✓ Created app %s", name)
            a.run(await_serving=False)
        elif _app_is_running(existing):
            previous = getattr(existing, "execution_id", None)
            existing.redeploy(await_serving=False)
        else:
            previous = getattr(existing, "execution_id", None)
            existing.run(await_serving=False)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not start app '{name}': {exc}") from exc

    a = _await_new_execution(apps, name, previous, timeout)
    url = getattr(a, "app_url", None)
    output.success("✓ App %s serves the new files", name)
    if check_path is not None:
        _check_route(project, name, url, check_path)
    if output.JSON_MODE:
        output.print_json(
            {
                "name": name,
                "url": url,
                "state": getattr(a, "state", None),
                "environment": env_name,
                "source": remote_dir,
                "files": [
                    {
                        "path": relative,
                        "sha256": hashlib.sha256(
                            Path(local_dir, relative).read_bytes()
                        ).hexdigest(),
                    }
                    for relative in files
                ],
            }
        )
    else:
        click.echo(url)


def _check_route(project: Any, name: str, url: str | None, path: str) -> None:
    """GET `path` of the deployed app through the proxy, as a browser would.

    The proxy wants a session, not an API key: an app proxy token minted with the
    key stands in for it, as the Hopsworks UI and Hopsable's previews do.
    """
    import requests
    from hopsworks_common import client

    if not url:
        raise click.ClickException(f"Could not check app '{name}': it has no URL.")
    _client = client._get_instance()
    try:
        token = _client._send_request(
            "POST", ["project", project.id, "apps", name, "proxy-token"]
        )["token"]
        answer = requests.get(
            url.rstrip("/") + "/" + path.lstrip("/"),
            cookies={"proxy_session": token},
            verify=_client._verify,
            timeout=60,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not check app '{name}' at {path}: {exc}"
        ) from exc
    if not answer.ok:
        raise click.ClickException(
            f"Could not check app '{name}': {path} answered {answer.status_code}: "
            f"{answer.text[:300]}. Read `hops app logs {name}`."
        )
    output.success("✓ %s answered %s", path, answer.status_code)


def _deploy_files(local_dir: str) -> list[str]:
    """Files of `local_dir`, relative and sorted, without dotfiles and `__pycache__`."""
    import os

    found = []
    for root, dirs, names in os.walk(local_dir):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "__pycache__"]
        for file_name in names:
            if not file_name.startswith("."):
                found.append(
                    os.path.relpath(os.path.join(root, file_name), local_dir).replace(
                        os.sep, "/"
                    )
                )
    return sorted(found)


def _local_text(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def _remote_text(dataset_api: Any, path: str) -> str | None:
    """The text of a HopsFS file, or None when it does not exist."""
    if not dataset_api.exists(path):
        return None
    content = dataset_api.read_content(path)
    return content.content.decode("utf-8") if content is not None else None


def _deploy_environment(
    env_api: Any, env_name: str, requirements: str, requirements_changed: bool
) -> None:
    """Clone `env_name` from the base environment once; install on a new clone or new requirements."""
    try:
        env = env_api.get_environment(env_name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not find environment '{env_name}': {exc}"
        ) from exc
    if env is None:
        output.info(
            "Cloning %s to %s; this takes a few minutes.", _DEPLOY_BASE_ENV, env_name
        )
        try:
            env = env_api.create_environment(
                env_name, base_environment_name=_DEPLOY_BASE_ENV, await_creation=True
            )
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not clone environment '{_DEPLOY_BASE_ENV}' to '{env_name}': {exc}"
            ) from exc
    elif not requirements_changed:
        output.info("Environment %s is up to date.", env_name)
        return
    output.info("Installing %s into %s.", _DEPLOY_REQUIREMENTS, env_name)
    try:
        env.install_requirements(requirements, await_installation=True)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not install into environment '{env_name}': {exc}"
        ) from exc
    output.success("✓ Environment %s ready", env_name)


def _await_new_execution(
    apps: Any, name: str, previous: int | None, timeout: int
) -> Any:
    """Poll until an execution other than `previous` serves, so the old pod never counts."""
    import time

    from hopsworks_common.app import _FAILED_STATES

    deadline = time.monotonic() + timeout
    state = None
    while True:
        a = apps.get_app(name)
        state = getattr(a, "state", None)
        execution = getattr(a, "execution_id", None)
        if execution is not None and execution != previous:
            if getattr(a, "serving", False):
                return a
            if state in _FAILED_STATES:
                raise click.ClickException(
                    f"Could not deploy app '{name}': it stopped in state {state}. "
                    f"Read `hops app logs {name}`."
                )
        if time.monotonic() >= deadline:
            raise click.ClickException(
                f"Could not deploy app '{name}': not serving after {timeout} s "
                f"(state {state}). Read `hops app logs {name}`, or raise --timeout."
            )
        time.sleep(_POLL_SECONDS)


@app_group.command("stop")
@click.argument("name")
@click.pass_context
def app_stop(ctx: click.Context, name: str) -> None:
    """Stop a running app.

    \f
    Args:
        ctx: Click context.
        name: App name.
    """
    a = _get_app(ctx, name)
    try:
        a.stop()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not stop app '{name}': {exc}") from exc
    output.success("✓ Stopped app %s", name)


@app_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.option(
    "--force", is_flag=True, help="Stop the app first if it is still running."
)
@click.pass_context
def app_delete(ctx: click.Context, name: str, yes: bool, force: bool) -> None:
    """Delete an app.

    \f
    Args:
        ctx: Click context.
        name: App name.
        yes: Skip confirmation when True.
        force: Stop the app first if still running.
    """
    a = _get_app(ctx, name)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete app '{name}'?", abort=True)
    if force:
        import contextlib

        with contextlib.suppress(Exception):
            a.stop()
    try:
        a.delete()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not delete app '{name}': {exc}") from exc
    output.success("✓ Deleted app %s", name)


def _accepted_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Drop kwargs the callable's signature rejects, warning on each.

    Guards ``hops app create`` against CLI/SDK drift: a deployed SDK older than
    this CLI may not accept newer create_app params (app_kind,
    entrypoint_command, app_port, git_*). A ``**kwargs`` signature accepts
    everything, so nothing is dropped.
    """
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return kwargs
    if any(p.kind == p.VAR_KEYWORD for p in params):
        return kwargs
    accepted = {p.name for p in params}
    dropped = [k for k in kwargs if k not in accepted]
    if dropped:
        output.warn(
            "Installed SDK's create_app does not accept %s; ignoring "
            "(CLI/SDK version drift).",
            ", ".join(dropped),
        )
    return {k: v for k, v in kwargs.items() if k in accepted}


def _get_app_api(ctx: click.Context) -> Any:
    project = session.get_project(ctx)
    return project.get_app_api()


def _get_app(ctx: click.Context, name: str) -> Any:
    apps = _get_app_api(ctx)
    try:
        a = apps.get_app(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not find app '{name}': {exc}") from exc
    if a is None:
        raise click.ClickException(f"App '{name}' not found.")
    return a


def _start(a: Any, await_serving: bool = True) -> None:
    try:
        a.run(await_serving=await_serving)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not start app '{getattr(a, 'name', '?')}': {exc}"
        ) from exc
    url = getattr(a, "app_url", None)
    state = getattr(a, "state", "-")
    serving = getattr(a, "serving", False)
    output.success(
        "✓ Started app %s (state=%s, serving=%s)",
        getattr(a, "name", "?"),
        state,
        "yes" if serving else "no",
    )
    if url:
        click.echo(url)


def _redeploy(a: Any, await_serving: bool = True) -> None:
    try:
        a.redeploy(await_serving=await_serving)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(
            f"Could not redeploy app '{getattr(a, 'name', '?')}': {exc}"
        ) from exc
    url = getattr(a, "app_url", None)
    state = getattr(a, "state", "-")
    serving = getattr(a, "serving", False)
    output.success(
        "✓ Redeployed app %s (state=%s, serving=%s)",
        getattr(a, "name", "?"),
        state,
        "yes" if serving else "no",
    )
    if url:
        click.echo(url)


def _monitoring_config(a: Any) -> Any:
    return getattr(a, "monitoring_config", None) or getattr(a, "monitoringConfig", None)


def _monitoring_enabled(a: Any) -> bool:
    config = _monitoring_config(a)
    if not config:
        return True
    if isinstance(config, dict):
        enabled = config.get("enabled")
    else:
        enabled = getattr(config, "enabled", None)
    return enabled is not False


def _monitoring_routes(a: Any) -> list[dict[str, str | None]]:
    config = _monitoring_config(a)
    if not config:
        return []
    if isinstance(config, dict):
        routes = config.get("routes") or []
    else:
        routes = getattr(config, "routes", None) or []
    normalized: list[dict[str, str | None]] = []
    for route in routes:
        if isinstance(route, dict):
            path = route.get("path") or route.get("route")
            match_type = route.get("matchType") or route.get("match_type")
        else:
            path = getattr(route, "path", None)
            match_type = getattr(route, "matchType", None) or getattr(
                route, "match_type", None
            )
        if not path:
            continue
        normalized.append(
            {"path": str(path), "matchType": str(match_type) if match_type else None}
        )
    return normalized


def _monitoring_state_text(a: Any) -> str:
    if _monitoring_config(a) is None:
        return "enabled"
    return "enabled" if _monitoring_enabled(a) else "disabled"


def _monitoring_routes_text(a: Any) -> str:
    if _monitoring_config(a) is not None and not _monitoring_enabled(a):
        return "-"
    routes = _monitoring_routes(a)
    if not routes:
        return "default ignored paths"
    parts = []
    for route in routes:
        text = route["path"]
        if route["matchType"]:
            text = f"{text} ({route['matchType']})"
        parts.append(text)
    return ", ".join(parts)


def _app_to_dict(a: Any) -> dict[str, Any]:
    source = _app_source(a)
    routes = _monitoring_routes(a)
    return {
        "id": getattr(a, "id", None),
        "name": getattr(a, "name", None),
        "app_kind": getattr(a, "app_kind", None),
        "source": source,
        "state": getattr(a, "state", None),
        "serving": bool(getattr(a, "serving", False)),
        "environment": getattr(a, "environment_name", None),
        "memory": getattr(a, "memory_requested", None),
        "cores": getattr(a, "cpu_requested", None),
        "app_path": getattr(a, "app_path", None),
        "app_port": getattr(a, "app_port", None),
        "git_url": getattr(a, "git_url", None),
        "git_provider": getattr(a, "git_provider", None),
        "git_branch": getattr(a, "git_branch", None),
        "latest_branch": getattr(a, "latest_branch", None),
        "git_auto_redeploy": bool(getattr(a, "git_auto_redeploy", False)),
        "latest_commit": getattr(a, "latest_commit", None),
        "entrypoint_script": getattr(a, "entrypoint_script", None),
        "app_base_path": getattr(a, "app_base_path", None),
        "proxy_path_mode": getattr(a, "proxy_path_mode", None),
        "readiness_probe_path": getattr(a, "readiness_probe_path", None),
        "db_access": bool(getattr(a, "db_access", True)),
        "idle_scale_to_zero": bool(getattr(a, "idle_scale_to_zero", False)),
        "idle_cooldown_seconds": getattr(a, "idle_cooldown_seconds", None),
        "cold_start_timeout_seconds": getattr(a, "cold_start_timeout_seconds", None),
        "entrypoint_command": getattr(a, "entrypoint_command", None),
        "monitoring": _monitoring_state_text(a),
        "monitoring_config": {
            "enabled": _monitoring_enabled(a),
            "routes": routes,
        },
        "description": getattr(a, "description", None),
        "app_url": getattr(a, "app_url", None),
    }


def _routing_text(app: Any) -> str:
    mode = getattr(app, "proxy_path_mode", None)
    if mode == "ROOT":
        return "root (the app serves at its base path)"
    if mode == "PREFIX":
        return "compatibility prefix (the app receives /hopsworks-api/pythonapp/...)"
    return "-"


def _idle_text(a: Any) -> str:
    if not getattr(a, "idle_scale_to_zero", False):
        return "No"
    cooldown = getattr(a, "idle_cooldown_seconds", None)
    cold_start = getattr(a, "cold_start_timeout_seconds", None)
    return (
        f"Yes (after {cooldown if cooldown is not None else 'default'} s idle, "
        f"wakes within {cold_start if cold_start is not None else 'default'} s)"
    )


def _auto_redeploy_text(a: Any) -> str:
    """Auto-redeploy state, or "-" for an app that is not git-backed.

    The backend rejects the flag without a git source, so "Disabled" there would
    imply a setting that could be turned on.
    """
    if not getattr(a, "git_url", None):
        return "-"
    return "Enabled" if getattr(a, "git_auto_redeploy", False) else "Disabled"


def _app_source(a: Any) -> str:
    return "Git repository" if getattr(a, "git_url", None) else "Project file"

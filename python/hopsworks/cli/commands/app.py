"""``hops app`` — Python app lifecycle.

Wraps the SDK's ``project.get_app_api()``: list, info, create, start, redeploy,
stop, delete, plus a convenience ``url`` that prints the public serving URL.
App scripts can either live in HopsFS or in a Git repository. File-backed
Streamlit apps use ``--path``; git-backed Streamlit apps use ``--git-url`` and
``--entrypoint-script``; custom apps use ``--entrypoint-command``. App metadata
can also carry monitoring config (``enabled`` plus optional ``routes`` with
``path`` and ``matchType``), and ``hops app info`` prints the monitoring state
and route list when it is present.
"""

from __future__ import annotations

import inspect
from typing import Any

import click
from hopsworks.cli import output, session


@click.group("app")
def app_group() -> None:
    """Python app commands."""


@app_group.command("list")
@click.pass_context
def app_list(ctx: click.Context) -> None:
    """List every app in the active project.

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
        ["Git branch", getattr(a, "git_branch", None) or "-"],
        ["Latest commit", getattr(a, "latest_commit", None) or "-"],
        ["Entrypoint script", getattr(a, "entrypoint_script", None) or "-"],
        ["Entrypoint", getattr(a, "entrypoint_command", None) or "-"],
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

    Args:
        ctx: Click context.
        name: App name.
        stream: Which stream(s) to print: ``stdout``, ``stderr``, or ``both``.
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
    type=click.Choice(["STREAMLIT", "CUSTOM", "FLASK", "GRADIO"], case_sensitive=False),
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
    help="Port exposed by custom apps.",
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
    "--entrypoint-script",
    default=None,
    help="Relative .py entrypoint script for Streamlit Git repository apps.",
)
@click.option(
    "--description",
    default=None,
    help="Optional app description.",
)
@click.option(
    "--environment",
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
    entrypoint_script: str | None,
    description: str | None,
    environment: str,
    memory: int,
    cores: float,
    start: bool,
) -> None:
    """Create a new app.

    Monitoring is enabled by default in the backend. Route filters are optional
    and narrow the traffic counted by Envoy.

    Args:
        ctx: Click context.
        name: App name.
        app_path: HopsFS path to the app script.
        app_kind: App type.
        entrypoint_command: Startup command for custom apps.
        app_port: Port for custom apps.
        git_url: Git repository URL for git-backed apps.
        git_provider: Git provider for git-backed apps.
        git_branch: Optional Git branch.
        entrypoint_script: Relative entrypoint script for Streamlit git apps.
        description: Optional app description.
        environment: Python environment name.
        memory: Memory in MB.
        cores: CPU cores.
        start: When True, start the app and wait for serving.
    """
    app_kind = app_kind.upper()
    git_repo_app = bool(git_url)
    if app_kind == "STREAMLIT":
        if entrypoint_command:
            raise click.ClickException(
                "Streamlit apps do not accept --entrypoint-command."
            )
        if git_repo_app:
            if not git_provider:
                raise click.ClickException(
                    "Streamlit Git repository apps require --git-provider."
                )
            if not entrypoint_script:
                raise click.ClickException(
                    "Streamlit Git repository apps require --entrypoint-script."
                )
        elif not app_path:
            raise click.ClickException("Streamlit apps require --path.")
        elif entrypoint_script:
            raise click.ClickException(
                "--entrypoint-script is only supported for Streamlit Git apps."
            )
    else:
        if not entrypoint_command:
            raise click.ClickException("Custom apps require --entrypoint-command.")
        if git_repo_app and not git_provider:
            raise click.ClickException("Git repository apps require --git-provider.")
        if entrypoint_script:
            raise click.ClickException(
                "--entrypoint-script is only supported for Streamlit Git apps."
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
    if entrypoint_script is not None:
        create_kwargs["entrypoint_script"] = entrypoint_script
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
    "--no-wait",
    is_flag=True,
    help="Return immediately after submission; do not wait for serving.",
)
@click.pass_context
def app_start(ctx: click.Context, name: str, no_wait: bool) -> None:
    """Start an app.

    Blocks until the app is ``serving`` unless ``--no-wait`` is set.

    Args:
        ctx: Click context.
        name: App name.
        no_wait: Skip the wait-for-serving loop.
    """
    a = _get_app(ctx, name)
    _start(a, await_serving=not no_wait)


@app_group.command("redeploy")
@click.argument("name")
@click.option(
    "--no-wait",
    is_flag=True,
    help="Return immediately after submission; do not wait for serving.",
)
@click.pass_context
def app_redeploy(ctx: click.Context, name: str, no_wait: bool) -> None:
    """Redeploy a running app.

    Blocks until the app is ``serving`` unless ``--no-wait`` is set.

    Args:
        ctx: Click context.
        name: App name.
        no_wait: Skip the wait-for-serving loop.
    """
    a = _get_app(ctx, name)
    _redeploy(a, await_serving=not no_wait)


@app_group.command("stop")
@click.argument("name")
@click.pass_context
def app_stop(ctx: click.Context, name: str) -> None:
    """Stop a running app.

    Args:
        ctx: Click context.
        name: App name.
    """
    a = _get_app(ctx, name)
    try:
        a.stop()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Stop failed: {exc}") from exc
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
        raise click.ClickException(f"Delete failed: {exc}") from exc
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
        raise click.ClickException(f"App '{name}' not found: {exc}") from exc
    if a is None:
        raise click.ClickException(f"App '{name}' not found.")
    return a


def _start(a: Any, await_serving: bool = True) -> None:
    try:
        a.run(await_serving=await_serving)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Start failed: {exc}") from exc
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
        raise click.ClickException(f"Redeploy failed: {exc}") from exc
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
            match_type = getattr(route, "matchType", None) or getattr(route, "match_type", None)
        if not path:
            continue
        normalized.append({"path": str(path), "matchType": str(match_type) if match_type else None})
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
            text = f'{text} ({route["matchType"]})'
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
        "latest_commit": getattr(a, "latest_commit", None),
        "entrypoint_script": getattr(a, "entrypoint_script", None),
        "entrypoint_command": getattr(a, "entrypoint_command", None),
        "monitoring": _monitoring_state_text(a),
        "monitoring_config": {
            "enabled": _monitoring_enabled(a),
            "routes": routes,
        },
        "description": getattr(a, "description", None),
        "app_url": getattr(a, "app_url", None),
    }


def _app_source(a: Any) -> str:
    return "Git repository" if getattr(a, "git_url", None) else "Project file"

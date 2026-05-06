"""``hops app`` — Streamlit application lifecycle.

Wraps the SDK's ``project.get_app_api()``: list, info, create, start, stop,
delete, plus a convenience ``url`` that prints the public serving URL.
App scripts must be uploaded to HopsFS first (``hops files upload``);
``create`` takes the HopsFS path, same as ``hops job create``.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


@click.group("app")
def app_group() -> None:
    """Streamlit application commands."""


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
                getattr(a, "environment", "-"),
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
        ["State", getattr(a, "state", "-")],
        ["Serving", "yes" if getattr(a, "serving", False) else "no"],
        ["Environment", getattr(a, "environment", "-")],
        ["Memory (MB)", getattr(a, "memory", "-")],
        ["Cores", getattr(a, "cores", "-")],
        ["Path", getattr(a, "app_path", "-")],
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


@app_group.command("create")
@click.argument("name")
@click.option(
    "--path",
    "app_path",
    required=True,
    help="HopsFS path to the Streamlit .py file (upload with `hops files upload` first).",
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
    app_path: str,
    environment: str,
    memory: int,
    cores: float,
    start: bool,
) -> None:
    """Create a new Streamlit app.

    Args:
        ctx: Click context.
        name: App name.
        app_path: HopsFS path to the Streamlit script.
        environment: Python environment name.
        memory: Memory in MB.
        cores: CPU cores.
        start: When True, start the app and wait for serving.
    """
    apps = _get_app_api(ctx)
    try:
        a = apps.create_app(
            name=name,
            app_path=app_path,
            environment=environment,
            memory=memory,
            cores=cores,
        )
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


def _app_to_dict(a: Any) -> dict[str, Any]:
    return {
        "id": getattr(a, "id", None),
        "name": getattr(a, "name", None),
        "state": getattr(a, "state", None),
        "serving": bool(getattr(a, "serving", False)),
        "environment": getattr(a, "environment", None),
        "memory": getattr(a, "memory", None),
        "cores": getattr(a, "cores", None),
        "app_path": getattr(a, "app_path", None),
        "app_url": getattr(a, "app_url", None),
    }

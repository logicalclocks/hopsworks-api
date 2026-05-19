"""``hops agent`` — Python agent deployment lifecycle.

Agents are server-only deployments (no model attached) created from a local
``.py`` script or a directory with ``pyproject.toml``.
``create`` calls ``ms.deploy_agent`` which builds/uploads the code, provisions
a Python environment from the ``python-agent-pipeline`` base image, and
creates or updates a regular Hopsworks deployment.
The remaining verbs (``info``, ``start``, ``stop``, ``query``, ``logs``,
``delete``) mirror ``hops deployment`` but resolve names through an
agent-only lookup that excludes model-backed deployments.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output, session


@click.group("agent")
def agent_group() -> None:
    """Python agent deployment commands."""


@agent_group.command("list")
@click.pass_context
def agent_list(ctx: click.Context) -> None:
    """List every agent deployment in the active project.

    Args:
        ctx: Click context.
    """
    ms = _get_model_serving(ctx)
    try:
        deployments = ms.get_deployments()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list agents: {exc}") from exc

    rows = []
    for d in deployments or []:
        if not _is_agent(d):
            continue
        rows.append(
            [
                getattr(d, "id", "?"),
                getattr(d, "name", "?"),
                _predictor_attr(d, "environment", "-"),
                _predictor_attr(d, "script_file", "-"),
                _deployment_status(d),
            ]
        )
    output.print_table(["ID", "NAME", "ENVIRONMENT", "SCRIPT", "STATUS"], rows)


@agent_group.command("info")
@click.argument("name")
@click.pass_context
def agent_info(ctx: click.Context, name: str) -> None:
    """Show details for a single agent.

    Args:
        ctx: Click context.
        name: Agent name.
    """
    agent = _get_agent(ctx, name)

    if output.JSON_MODE:
        output.print_json(_agent_to_dict(agent))
        return

    rows = [
        ["ID", getattr(agent, "id", "?")],
        ["Name", getattr(agent, "name", "?")],
        ["Environment", _predictor_attr(agent, "environment", "-")],
        ["Script", _predictor_attr(agent, "script_file", "-")],
        ["Serving tool", getattr(agent, "serving_tool", "-")],
        ["Model server", getattr(agent, "model_server", "-")],
        ["API protocol", getattr(agent, "api_protocol", "-")],
        ["Description", getattr(agent, "description", "-") or "-"],
        ["Status", _deployment_status(agent)],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@agent_group.command("create")
@click.argument("entry", type=click.Path(exists=True))
@click.option("--name", help="Agent name; defaults to the basename of ENTRY.")
@click.option(
    "--requirements",
    type=click.Path(exists=True),
    help="Local requirements.txt to install into the agent environment.",
)
@click.option(
    "--environment",
    help="Python environment name; defaults to the agent name.",
)
@click.option(
    "--upload-dir",
    default="Resources/agents",
    show_default=True,
    help="HopsFS directory under which agent files are placed.",
)
@click.option("--description", default=None, help="Agent description.")
@click.pass_context
def agent_create(
    ctx: click.Context,
    entry: str,
    name: str | None,
    requirements: str | None,
    environment: str | None,
    upload_dir: str,
    description: str | None,
) -> None:
    """Create (or update) an agent from a local script or package.

    ``ENTRY`` is a ``.py`` file or a directory containing ``pyproject.toml``.
    On re-runs, the latest code is uploaded and the deployment's predictor is
    rewritten; the running state is left untouched (use ``start`` or invoke
    ``restart`` via the SDK to roll a running agent onto new code).

    Args:
        ctx: Click context.
        entry: Local path to script or package.
        name: Agent name; defaults to the basename of ``entry``.
        requirements: Optional ``requirements.txt`` path.
        environment: Python environment name.
        upload_dir: HopsFS upload directory.
        description: Description string.
    """
    ms = _get_model_serving(ctx)
    try:
        agent = ms.deploy_agent(
            entry=entry,
            name=name,
            requirements=requirements,
            environment=environment,
            upload_dir=upload_dir,
            description=description,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Agent create failed: {exc}") from exc

    output.success("✓ Created agent %s", getattr(agent, "name", name or entry))


@agent_group.command("start")
@click.argument("name")
@click.option(
    "--wait",
    type=int,
    default=600,
    show_default=True,
    help="Seconds to wait for RUNNING.",
)
@click.pass_context
def agent_start(ctx: click.Context, name: str, wait: int) -> None:
    """Start an agent and optionally wait for the RUNNING state.

    Args:
        ctx: Click context.
        name: Agent name.
        wait: Seconds to wait for the agent to become RUNNING.
    """
    agent = _get_agent(ctx, name)
    try:
        agent.start(await_running=wait)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Start failed: {exc}") from exc
    output.success("✓ Started agent %s", name)


@agent_group.command("stop")
@click.argument("name")
@click.option(
    "--wait",
    type=int,
    default=600,
    show_default=True,
    help="Seconds to wait for STOPPED.",
)
@click.pass_context
def agent_stop(ctx: click.Context, name: str, wait: int) -> None:
    """Stop a running agent.

    Args:
        ctx: Click context.
        name: Agent name.
        wait: Seconds to wait for the agent to stop.
    """
    agent = _get_agent(ctx, name)
    try:
        agent.stop(await_stopped=wait)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Stop failed: {exc}") from exc
    output.success("✓ Stopped agent %s", name)


@agent_group.command("query")
@click.argument("name")
@click.option(
    "--data",
    help='JSON body to POST, e.g. \'{"prompt": "hello"}\'.',
)
@click.option(
    "--file",
    "file_path",
    type=click.Path(exists=True),
    help="JSON file with the request body.",
)
@click.pass_context
def agent_query(
    ctx: click.Context, name: str, data: str | None, file_path: str | None
) -> None:
    """Send a request to a running agent.

    Args:
        ctx: Click context.
        name: Agent name.
        data: Inline JSON body.
        file_path: Alternative JSON file source.
    """
    if not data and not file_path:
        raise click.UsageError("Provide --data or --file.")
    payload_str = Path(file_path).read_text() if file_path else (data or "")
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError as exc:
        raise click.BadParameter(f"Invalid JSON: {exc}") from exc

    agent = _get_agent(ctx, name)
    try:
        response = agent.predict(data=payload)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Query failed: {exc}") from exc

    if output.JSON_MODE:
        output.print_json(response)
        return
    click.echo(json.dumps(response, indent=2, default=str))


@agent_group.command("logs")
@click.argument("name")
@click.option(
    "--component",
    default="predictor",
    show_default=True,
    help="Component to fetch logs for (predictor, transformer, ...).",
)
@click.option(
    "--tail", type=int, default=100, show_default=True, help="Number of lines."
)
@click.option(
    "--source",
    type=click.Choice(["opensearch", "kubernetes"]),
    default="opensearch",
    show_default=True,
    help=(
        "opensearch: historical logs from the project serving index "
        "(works for stopped agents). kubernetes: live pod-tailing."
    ),
)
@click.option("--since", help="ISO-8601 lower bound on log timestamp.")
@click.option("--until", help="ISO-8601 upper bound on log timestamp.")
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    help="Stream new log lines as they are written (Ctrl-C to stop).",
)
@click.option(
    "--interval",
    type=float,
    default=2.0,
    show_default=True,
    help="Seconds between polls when --follow is set.",
)
@click.pass_context
def agent_logs(
    ctx: click.Context,
    name: str,
    component: str,
    tail: int,
    source: str,
    since: str | None,
    until: str | None,
    follow: bool,
    interval: float,
) -> None:
    """Read or follow logs from an agent component.

    Args:
        ctx: Click context.
        name: Agent name.
        component: Component to query (e.g. ``predictor``, ``transformer``).
        tail: Number of lines.
        source: ``opensearch`` or ``kubernetes``.
        since: ISO-8601 lower bound on log timestamp.
        until: ISO-8601 upper bound on log timestamp.
        follow: Stream new lines instead of returning a one-shot tail.
        interval: Seconds between polls when following.
    """
    agent = _get_agent(ctx, name)

    if follow:
        try:
            for chunk in agent.tail_logs(
                component=component,
                interval=interval,
                source=source,
                since=since or "now",
            ):
                click.echo(chunk, nl=False)
        except KeyboardInterrupt:
            return
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Log follow failed: {exc}") from exc
        return

    try:
        text = agent.read_logs(
            component=component,
            tail=tail,
            source=source,
            since=since,
            until=until,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Log fetch failed: {exc}") from exc
    if output.JSON_MODE:
        output.print_json({"component": component, "logs": text})
        return
    click.echo(text or "<no logs>")


@agent_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.option("--force", is_flag=True, help="Force-delete even when running.")
@click.pass_context
def agent_delete(ctx: click.Context, name: str, yes: bool, force: bool) -> None:
    """Delete an agent deployment.

    Args:
        ctx: Click context.
        name: Agent name.
        yes: Skip confirmation when True.
        force: Pass ``force=True`` to the SDK (stops the agent first if running).
    """
    agent = _get_agent(ctx, name)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete agent '{name}'?", abort=True)
    try:
        agent.delete(force=force)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted agent %s", name)


def _get_model_serving(ctx: click.Context) -> Any:
    project = session.get_project(ctx)
    return project.get_model_serving()


def _get_agent(ctx: click.Context, name: str) -> Any:
    ms = _get_model_serving(ctx)
    try:
        d = ms.get_deployment(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Agent '{name}' not found: {exc}") from exc
    if d is None or not _is_agent(d):
        raise click.ClickException(f"Agent '{name}' not found.")
    return d


def _is_agent(d: Any) -> bool:
    """Identify server-only (agent) deployments with no attached model.

    Prefer the SDK-provided ``has_model`` attribute when available because it
    accounts for the full deployment model attachment state.
    Fall back to the legacy ``model_name`` heuristic for non-SDK objects.
    """
    has_model = getattr(d, "has_model", None)
    if has_model is not None:
        return not bool(has_model)
    return getattr(d, "model_name", None) in (None, "")


def _predictor_attr(d: Any, attr: str, default: Any) -> Any:
    predictor = getattr(d, "predictor", None)
    return getattr(predictor, attr, default) if predictor is not None else default


def _deployment_status(d: Any) -> str:
    for attr in ("status", "deployment_state", "_state"):
        value = getattr(d, attr, None)
        if value:
            return str(value)
    return "-"


def _agent_to_dict(d: Any) -> dict[str, Any]:
    return {
        "id": getattr(d, "id", None),
        "name": getattr(d, "name", None),
        "environment": _predictor_attr(d, "environment", None),
        "script_file": _predictor_attr(d, "script_file", None),
        "serving_tool": getattr(d, "serving_tool", None),
        "model_server": getattr(d, "model_server", None),
        "api_protocol": getattr(d, "api_protocol", None),
        "description": getattr(d, "description", None),
        "status": _deployment_status(d),
    }

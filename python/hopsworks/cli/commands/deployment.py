"""``hops deployment`` — model serving deployments (reads + lifecycle writes).

Create, start, stop, predict, logs, and delete are all served by the SDK's
``Deployment`` object when obtained via ``ms.create_deployment`` or
``model.deploy`` — no raw REST needed.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import lineage, output, session


@click.group("deployment")
def deployment_group() -> None:
    """Model serving deployment commands."""


@deployment_group.command("list")
@click.pass_context
def deployment_list(ctx: click.Context) -> None:
    """List every deployment in the active project.

    Args:
        ctx: Click context.
    """
    project = session.get_project(ctx)
    ms = project.get_model_serving()
    try:
        deployments = ms.get_deployments()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list deployments: {exc}") from exc

    rows = []
    for d in deployments or []:
        rows.append(
            [
                getattr(d, "id", "?"),
                getattr(d, "name", "?"),
                getattr(d, "model_name", "?"),
                getattr(d, "model_version", "?"),
                getattr(d, "serving_tool", "-"),
                _deployment_status_live(d),
            ]
        )
    output.print_table(["ID", "NAME", "MODEL", "VERSION", "TOOL", "STATUS"], rows)


@deployment_group.command("info")
@click.argument("name")
@click.pass_context
def deployment_info(ctx: click.Context, name: str) -> None:
    """Show details for a single deployment.

    Args:
        ctx: Click context.
        name: Deployment name.
    """
    project = session.get_project(ctx)
    ms = project.get_model_serving()
    try:
        deployment = ms.get_deployment(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Deployment '{name}' not found: {exc}") from exc
    if deployment is None:
        raise click.ClickException(f"Deployment '{name}' not found.")

    if output.JSON_MODE:
        output.print_json(_deployment_to_dict(deployment))
        return

    rows = [
        ["ID", getattr(deployment, "id", "?")],
        ["Name", getattr(deployment, "name", "?")],
        ["Model", getattr(deployment, "model_name", "?")],
        ["Model version", getattr(deployment, "model_version", "?")],
        ["Serving tool", getattr(deployment, "serving_tool", "-")],
        ["Model server", getattr(deployment, "model_server", "-")],
        ["Status", _deployment_status_live(deployment)],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@deployment_group.command("status")
@click.argument("name")
@click.pass_context
def deployment_status(ctx: click.Context, name: str) -> None:
    """Show the current status of a deployment.

    Args:
        ctx: Click context.
        name: Deployment name.
    """
    deployment = _get_deployment(ctx, name)
    status = _deployment_status_live(deployment)
    if output.JSON_MODE:
        output.print_json({"name": name, "status": status})
        return
    click.echo(status)


def _deployment_status(d: Any) -> str:
    """Return a best-effort status string without triggering a network call.

    The no-network fallback used by ``_deployment_status_live`` when the live
    ``get_state()`` call fails. Reads whatever status attribute the SDK already
    cached on the deployment.

    Args:
        d: Deployment instance.

    Returns:
        A short human-readable status label.
    """
    for attr in ("status", "deployment_state", "_state"):
        value = getattr(d, attr, None)
        if value:
            return str(value)
    return "-"


def _deployment_status_live(d: Any) -> str:
    """Status for a deployment, fetching live state when available.

    Both ``list`` and ``info`` use this: ``get_state()`` issues a REST call so
    the Status reflects reality. The cached attributes are usually empty (which
    rendered "-"), so fall back to them only if the call fails.

    Args:
        d: Deployment instance.

    Returns:
        A short human-readable status label.
    """
    try:
        state = d.get_state()
    except Exception:  # noqa: BLE001 - degrade to the cached attributes
        state = None
    status = getattr(state, "status", None) if state is not None else None
    return str(status) if status else _deployment_status(d)


def _deployment_to_dict(d: Any) -> dict[str, Any]:
    return {
        "id": getattr(d, "id", None),
        "name": getattr(d, "name", None),
        "model_name": getattr(d, "model_name", None),
        "model_version": getattr(d, "model_version", None),
        "serving_tool": getattr(d, "serving_tool", None),
        "model_server": getattr(d, "model_server", None),
        "status": _deployment_status_live(d),
    }


# region Write commands


@deployment_group.command("create")
@click.argument("model_name")
@click.option("--version", type=int, help="Model version; latest when omitted.")
@click.option("--name", help="Deployment name; defaults to the model name.")
@click.option(
    "--script",
    "script_file",
    help="Predictor script: a local file (uploaded to HopsFS) or an "
    "existing HopsFS path.",
)
@click.option(
    "--env",
    "environment",
    help="Inference environment name (e.g. pandas-inference-pipeline).",
)
@click.option(
    "--serving-tool",
    type=click.Choice(["KSERVE", "DEFAULT"], case_sensitive=False),
    help="Serving backend.",
)
@click.option("--description", default="", help="Deployment description.")
@click.pass_context
def deployment_create(
    ctx: click.Context,
    model_name: str,
    version: int | None,
    name: str | None,
    script_file: str | None,
    environment: str | None,
    serving_tool: str | None,
    description: str,
) -> None:
    """Deploy a model from the registry.

    A local ``--script`` is uploaded to HopsFS first (the backend needs a
    HopsFS path); an existing HopsFS path is passed through. ``--env`` selects
    the inference environment.

    Args:
        ctx: Click context.
        model_name: Model registry name.
        version: Model version.
        name: Deployment name; defaults to the model name.
        script_file: Predictor script, local or HopsFS.
        environment: Inference environment name.
        serving_tool: ``KSERVE`` or ``DEFAULT``.
        description: Deployment description.
    """
    project = session.get_project(ctx)
    mr = project.get_model_registry()
    try:
        if version is not None:
            model = mr.get_model(model_name, version=version)
        else:
            models = mr.get_models(model_name)
            model = models[-1] if models else None
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Model '{model_name}' not found: {exc}") from exc
    if model is None:
        raise click.ClickException(f"Model '{model_name}' not found.")

    deploy_name = name or model_name
    # Upload a local predictor to HopsFS (the backend needs a HopsFS path);
    # an existing HopsFS path passes through untouched.
    if script_file and Path(script_file).is_file():
        dataset = project.get_dataset_api()
        dest_dir = f"Resources/deployments/{deploy_name}"
        with contextlib.suppress(Exception):  # directory may already exist
            dataset.mkdir(dest_dir)
        try:
            uploaded = dataset.upload(
                local_path=script_file, upload_path=dest_dir, overwrite=True
            )
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Could not upload predictor: {exc}") from exc
        script_file = uploaded or f"{dest_dir}/{Path(script_file).name}"
        # Serving needs an absolute /Projects/<project>/... path; dataset
        # upload returns one relative to the project root.
        if not script_file.startswith("/"):
            script_file = f"/Projects/{project.name}/{script_file}"
        output.success("✓ Uploaded predictor -> %s", script_file)

    try:
        deployment = model.deploy(
            name=deploy_name,
            description=description,
            script_file=script_file,
            environment=environment,
            serving_tool=(serving_tool or "").upper() or None,
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Deployment creation failed: {exc}") from exc

    output.success(
        "✓ Deployed model %s v%s as %s",
        model_name,
        getattr(model, "version", "?"),
        getattr(deployment, "name", name or model_name),
    )


@deployment_group.command("start")
@click.argument("name")
@click.option(
    "--wait",
    type=int,
    default=600,
    show_default=True,
    help="Seconds to wait for RUNNING.",
)
@click.pass_context
def deployment_start(ctx: click.Context, name: str, wait: int) -> None:
    """Start a deployment and optionally wait for the RUNNING state.

    Args:
        ctx: Click context.
        name: Deployment name.
        wait: Seconds to wait for the deployment to become RUNNING.
    """
    deployment = _get_deployment(ctx, name)
    try:
        deployment.start(await_running=wait)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Start failed: {exc}") from exc
    output.success("✓ Started deployment %s", name)


@deployment_group.command("stop")
@click.argument("name")
@click.option(
    "--wait",
    type=int,
    default=600,
    show_default=True,
    help="Seconds to wait for STOPPED.",
)
@click.pass_context
def deployment_stop(ctx: click.Context, name: str, wait: int) -> None:
    """Stop a running deployment.

    Args:
        ctx: Click context.
        name: Deployment name.
        wait: Seconds to wait for the deployment to stop.
    """
    deployment = _get_deployment(ctx, name)
    try:
        deployment.stop(await_stopped=wait)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Stop failed: {exc}") from exc
    output.success("✓ Stopped deployment %s", name)


@deployment_group.command("predict")
@click.argument("name")
@click.option(
    "--data",
    help="JSON body to POST, e.g. '{\"instances\": [[1, 2, 3]]}'.",
)
@click.option(
    "--file",
    "file_path",
    type=click.Path(exists=True),
    help="JSON file with the request body.",
)
@click.pass_context
def deployment_predict(
    ctx: click.Context, name: str, data: str | None, file_path: str | None
) -> None:
    """Send an inference request to a running deployment.

    Args:
        ctx: Click context.
        name: Deployment name.
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

    deployment = _get_deployment(ctx, name)
    try:
        response = deployment.predict(data=payload)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Predict failed: {exc}") from exc

    if output.JSON_MODE:
        output.print_json(response)
        return
    click.echo(json.dumps(response, indent=2, default=str))


@deployment_group.command("logs")
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
        "(works for stopped deployments). kubernetes: live pod-tailing."
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
def deployment_logs(
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
    """Read or follow logs from a deployment component.

    Without ``--follow``: prints the last ``--tail`` lines and exits.
    With ``--follow``: yields new chunks every ``--interval`` seconds
    until interrupted with Ctrl-C.

    Args:
        ctx: Click context.
        name: Deployment name.
        component: Component to query (e.g. ``predictor``, ``transformer``).
        tail: Number of lines.
        source: ``opensearch`` or ``kubernetes``.
        since: ISO-8601 lower bound on log timestamp.
        until: ISO-8601 upper bound on log timestamp.
        follow: Stream new lines instead of returning a one-shot tail.
        interval: Seconds between polls when following.
    """
    deployment = _get_deployment(ctx, name)

    if follow:
        try:
            for chunk in deployment.tail_logs(
                component=component,
                interval=interval,
                source=source,
                # When ``--since`` is provided, start there; otherwise stream
                # only brand-new lines from the moment the command starts.
                since=since or "now",
            ):
                click.echo(chunk, nl=False)
        except KeyboardInterrupt:
            # Clean exit on Ctrl-C; the generator itself drops out via
            # GeneratorExit and ``time.sleep`` is interruptible.
            return
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Log follow failed: {exc}") from exc
        return

    try:
        text = deployment.read_logs(
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


@deployment_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.option("--force", is_flag=True, help="Force-delete even when running.")
@click.pass_context
def deployment_delete(ctx: click.Context, name: str, yes: bool, force: bool) -> None:
    """Delete a deployment.

    Args:
        ctx: Click context.
        name: Deployment name.
        yes: Skip confirmation when True.
        force: Pass ``force=True`` to the SDK.
    """
    deployment = _get_deployment(ctx, name)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete deployment '{name}'?", abort=True)
    try:
        deployment.delete(force=force)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted deployment %s", name)


@deployment_group.command("lineage")
@click.argument("name")
@click.pass_context
def deployment_lineage(ctx: click.Context, name: str) -> None:
    """Show lineage for a deployment.

    A deployment serves a model, so its upstream lineage is that model plus
    the model's own feature view and training dataset provenance.

    Args:
        ctx: Click context.
        name: Deployment name.
    """
    deployment = _get_deployment(ctx, name)
    model = lineage.fetch(deployment.get_model)
    label = f"deployment {getattr(deployment, 'name', name)}"
    sections: list[tuple[str, str, Any]] = [("upstream", "model", model)]
    if model is not None:
        sections += [
            (
                "upstream",
                "feature_view",
                lineage.fetch(model.get_feature_view_provenance),
            ),
            (
                "upstream",
                "training_dataset",
                lineage.fetch(model.get_training_dataset_provenance),
            ),
        ]
    lineage.render(label, sections)


def _get_deployment(ctx: click.Context, name: str) -> Any:
    project = session.get_project(ctx)
    ms = project.get_model_serving()
    try:
        deployment = ms.get_deployment(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Deployment '{name}' not found: {exc}") from exc
    if deployment is None:
        raise click.ClickException(f"Deployment '{name}' not found.")
    return deployment

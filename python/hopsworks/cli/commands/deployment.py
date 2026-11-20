"""``hops deployment`` — model serving deployments (reads + lifecycle writes).

Create, start, stop, predict, logs, and delete are all served by the SDK's
``Deployment`` object when obtained via ``ms.create_deployment`` or
``model.deploy`` — no raw REST needed.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import lineage, output, session, versions


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
    ms = session.get_model_serving(ctx)
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
    deployment = _get_deployment(ctx, name)

    if output.JSON_MODE:
        output.print_json(_deployment_to_dict(deployment))
        return

    rows = [
        ["ID", getattr(deployment, "id", "?")],
        ["Name", getattr(deployment, "name", "?")],
        ["Model", getattr(deployment, "model_name", "?")],
        ["Model version", getattr(deployment, "model_version", "?")],
        ["Feature view", _feature_view_label(deployment)],
        ["Serving tool", getattr(deployment, "serving_tool", "-")],
        ["Model server", getattr(deployment, "model_server", "-")],
        ["Schema", _schema_label(deployment)],
        ["Schema id", getattr(deployment, "schema_id", None) or "-"],
        ["Status", _deployment_status_live(deployment)],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@deployment_group.command("schema")
@click.argument("name")
@click.option(
    "--json-schema",
    "as_json_schema",
    is_flag=True,
    help="Print the request and response JSON Schema instead of the field table.",
)
@click.option(
    "--openapi",
    "as_openapi",
    is_flag=True,
    help="Print an OpenAPI 3.1 document for the prediction endpoint.",
)
@click.pass_context
def deployment_schema(
    ctx: click.Context, name: str, as_json_schema: bool, as_openapi: bool
) -> None:
    """Show the request contract of a deployment.

    Args:
        ctx: Click context.
        name: Deployment name.
        as_json_schema: Print JSON Schema documents.
        as_openapi: Print an OpenAPI document.
    """
    deployment = _get_deployment(ctx, name)
    try:
        schema = deployment.schema
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not read the schema: {exc}") from exc
    if schema is None:
        raise click.ClickException(
            f"Deployment '{name}' has no deployment schema. Deploy with the default "
            "predictor or set deployment.schema and save()."
        )
    if as_openapi:
        url = None
        with contextlib.suppress(Exception):  # URL needs an Istio endpoint
            url = deployment.get_inference_url()
        output.print_json(schema.to_openapi(name, url=url))
        return
    if as_json_schema or output.JSON_MODE:
        payload = schema.to_json_schema() if as_json_schema else schema.to_dict()
        output.print_json(payload)
        return

    rows = []
    for group in (
        "serving_keys",
        "passed_features",
        "request_parameters",
        "extra_logging_features",
    ):
        for field in getattr(schema, group):
            rows.append(
                [
                    group,
                    field.name,
                    field.type or "unresolved",
                    "yes" if field.nullable else "no",
                    "no" if group == "extra_logging_features" else "yes",
                ]
            )
    output.print_table(["GROUP", "NAME", "TYPE", "NULLABLE", "REQUIRED"], rows)
    click.echo(f"schema id: {schema.schema_id}")
    if schema.unresolved:
        click.echo("unresolved types (not validated): " + ", ".join(schema.unresolved))


def _feature_view_label(d: Any) -> str:
    name = getattr(d, "feature_view_name", None)
    if not name:
        return "-"
    return f"{name} v{getattr(d, 'feature_view_version', '?')}"


def _schema_label(d: Any) -> str:
    if not getattr(d, "schema_id", None):
        return "-"
    try:
        schema = d.schema
    except Exception:  # noqa: BLE001 - info must not fail on a missing file
        return "unreadable"
    if schema is None:
        return "-"
    return "inferred" if schema.inferred else "manual"


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
        "feature_view_name": getattr(d, "feature_view_name", None),
        "feature_view_version": getattr(d, "feature_view_version", None),
        "serving_tool": getattr(d, "serving_tool", None),
        "model_server": getattr(d, "model_server", None),
        "schema_id": getattr(d, "schema_id", None),
        "status": _deployment_status_live(d),
    }


# region Write commands

# The backend's default limits for a predictor instance.
DEFAULT_CORES = 1.0
DEFAULT_MEMORY_MB = 1024


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
@click.option(
    "--knative/--standard",
    "knative_mode",
    default=None,
    help="KServe mode: Knative (scale-to-zero) or Standard (no scale-to-zero, CPU/memory autoscaling or fixed replicas when min equals max). "
    "Omit to let the backend decide (vLLM deployments default to Standard, others to Knative mode). "
    "On an update, omitting it keeps the deployment's current mode.",
)
@click.option("--description", default="", help="Deployment description.")
@click.option(
    "--passed-feature",
    "passed_features",
    multiple=True,
    help="Feature whose value clients send with each request (repeatable). "
    "Only with the default predictor.",
)
@click.option(
    "--no-default-predictor",
    "no_default_predictor",
    is_flag=True,
    help="Never use the library's default predictor.",
)
@click.option(
    "--cores",
    type=click.FloatRange(min=0, min_open=True),
    help="CPU cores each instance may use; half of them are requested. "
    f"Default {DEFAULT_CORES:g}.",
)
@click.option(
    "--memory",
    type=click.IntRange(min=1),
    help=f"Memory in MB each instance may use, all of it requested. Default {DEFAULT_MEMORY_MB}.",
)
@click.pass_context
def deployment_create(
    ctx: click.Context,
    model_name: str,
    version: int | None,
    name: str | None,
    script_file: str | None,
    environment: str | None,
    serving_tool: str | None,
    knative_mode: bool | None,
    description: str,
    passed_features: tuple[str, ...],
    no_default_predictor: bool,
    cores: float | None,
    memory: int | None,
) -> None:
    """Deploy a model from the registry.

    A local ``--script`` is uploaded to HopsFS first (the backend needs a
    HopsFS path); an existing HopsFS path is passed through. ``--env`` selects
    the inference environment. Without a script, a Python model registered
    with a feature view gets the default predictor and a deployment schema.

    Args:
        ctx: Click context.
        model_name: Model registry name.
        version: Model version.
        name: Deployment name; defaults to the model name.
        script_file: Predictor script, local or HopsFS.
        environment: Inference environment name.
        serving_tool: ``KSERVE`` or ``DEFAULT``.
        knative_mode: KServe Knative (True) vs Standard (False) mode; None lets the backend decide.
        description: Deployment description.
        passed_features: Features clients send with each request.
        no_default_predictor: Disable the default predictor.
        cores: CPU cores per instance; None keeps the backend's default resources.
        memory: Memory per instance in MB; None keeps the backend's default resources.
    """
    project = session.get_project(ctx)
    mr = project.get_model_registry()
    try:
        model = versions.model(mr, model_name, version)
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
            passed_features=list(passed_features) or None,
            default_predictor=False if no_default_predictor else None,
            knative_mode=knative_mode,
            resources=_resources(cores, memory),
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Deployment creation failed: {exc}") from exc

    output.success(
        "✓ Deployed model %s v%s as %s",
        model_name,
        getattr(model, "version", "?"),
        getattr(deployment, "name", name or model_name),
    )
    _report_schema(deployment)


def _resources(cores: float | None, memory: int | None) -> Any:
    """One instance with `cores` and `memory` as its limits, or None for the backend's defaults.

    Requests follow the defaults' shape: half the cores, all the memory.
    """
    if cores is None and memory is None:
        return None
    from hsml.resources import PredictorResources, Resources

    cores = DEFAULT_CORES if cores is None else cores
    memory = DEFAULT_MEMORY_MB if memory is None else memory
    return PredictorResources(
        num_instances=1,
        requests=Resources(cores=cores / 2, memory=memory, gpus=0),
        limits=Resources(cores=cores, memory=memory, gpus=0),
    )


def _report_schema(deployment: Any) -> None:
    schema_id = getattr(deployment, "schema_id", None)
    if not schema_id:
        return
    with contextlib.suppress(Exception):  # the schema is pending in memory here
        schema = deployment.schema
        output.success(
            "✓ Deployment schema %s with %d field(s)%s",
            schema_id,
            len(schema.columns),
            f"; unresolved types: {', '.join(schema.unresolved)}"
            if schema.unresolved
            else "",
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
@click.option(
    "--no-validate",
    "no_validate",
    is_flag=True,
    help="Skip client-side validation against the deployment schema.",
)
@click.option(
    "--via-api",
    "via_api",
    is_flag=True,
    help="Send the request through the Hopsworks API "
    "(/hopsworks-api/api/project/<id>/inference/models/<name>:predict), for "
    "callers that reach Hopsworks but not the model serving ingress. The body "
    "goes as given; the deployment validates it.",
)
@click.option(
    "--timing",
    is_flag=True,
    help="Report the HTTP status and the call's milliseconds; with --json the "
    "output is {status, ms, response}, also when the call fails.",
)
@click.pass_context
def deployment_predict(
    ctx: click.Context,
    name: str,
    data: str | None,
    file_path: str | None,
    no_validate: bool,
    via_api: bool,
    timing: bool,
) -> None:
    """Send an inference request to a running deployment.

    Args:
        ctx: Click context.
        name: Deployment name.
        data: Inline JSON body.
        file_path: Alternative JSON file source.
        no_validate: Skip client-side schema validation.
        via_api: Send through the Hopsworks API instead of the ingress.
        timing: Report the status and milliseconds of the call.
    """
    if not data and not file_path:
        raise click.UsageError("Provide --data or --file.")
    payload_str = (
        Path(file_path).read_text(encoding="utf-8") if file_path else (data or "")
    )
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError as exc:
        raise click.BadParameter(f"Invalid JSON: {exc}") from exc

    deployment = _get_deployment(ctx, name)
    started = time.monotonic()
    status: int | None = 200
    try:
        if via_api:
            response = _predict_via_api(deployment, payload)
        else:
            response = deployment.predict(data=payload, validate=not no_validate)
    except Exception as exc:  # noqa: BLE001
        if not timing:
            raise click.ClickException(f"Predict failed: {exc}") from exc
        ms = round((time.monotonic() - started) * 1000)
        error_response = getattr(exc, "response", None)
        status = getattr(error_response, "status_code", None)
        body = _response_body(error_response)
        if output.JSON_MODE:
            from hopsworks.cli.main import _explain_rest_error  # noqa: PLC0415

            error = (
                _explain_rest_error(exc) or (str(exc).splitlines() or [repr(exc)])[0]
            )
            output.print_json(
                {"status": status, "ms": ms, "response": body, "error": error}
            )
            ctx.exit(1)
        raise click.ClickException(
            f"Predict failed with status {status or 'none'} in {ms} ms: "
            f"{body if body is not None else exc}"
        ) from exc
    ms = round((time.monotonic() - started) * 1000)

    if timing and output.JSON_MODE:
        output.print_json({"status": status, "ms": ms, "response": response})
        return
    if output.JSON_MODE:
        output.print_json(response)
        return
    click.echo(json.dumps(response, indent=2, default=str))
    if timing:
        click.echo(f"status {status} in {ms} ms", err=True)


# A prediction through Hopsworks that has not answered by then has stalled.
_VIA_API_SECONDS = 60


def _predict_via_api(deployment: Any, payload: Any) -> Any:
    """POST the body to the deployment through the Hopsworks API, which forwards it to the model."""
    from hopsworks_common import client  # noqa: PLC0415

    _client = client._get_instance()
    path = deployment._serving_api._get_hopsworks_inference_path(
        _client._project_id, deployment
    )
    return _client._send_request(
        "POST",
        path,
        headers={"content-type": "application/json"},
        data=json.dumps(payload),
        deadline=time.monotonic() + _VIA_API_SECONDS,
    )


def _response_body(response: Any) -> Any:
    """An error response's body: its JSON when it has one, else its text."""
    if response is None:
        return None
    try:
        return response.json()
    except Exception:  # noqa: BLE001 - an HTML page from a proxy
        return getattr(response, "text", None)


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
    default="kubernetes",
    show_default=True,
    help=(
        "kubernetes: live pod logs. opensearch: deprecated; new backends "
        "serve it from the Kubernetes path as well. Logs of stopped "
        "deployments are retrieved with --download."
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
@click.option(
    "--download",
    is_flag=True,
    help=(
        "Download the HopsFS log archives written when the deployment was "
        "stopped or deleted. Cannot be combined with --follow or --source."
    ),
)
@click.option(
    "--dir",
    "log_dir",
    type=click.Path(file_okay=False),
    help="With --download, the directory to download into, created if missing; "
    "the working directory by default. Keep logs out of a git work tree, such "
    "as an ML system's directory: in the factory, "
    "${HOPSFS_USER_HOME_DIR:-$HOME}/Logs/factory/<slug>.",
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
    download: bool,
    log_dir: str | None,
) -> None:
    """Read or follow logs from a deployment component.

    Without ``--follow``: prints the last ``--tail`` lines and exits.
    With ``--follow``: yields new chunks every ``--interval`` seconds
    until interrupted with Ctrl-C.
    With ``--download``: downloads the HopsFS log archives of a stopped
    or deleted deployment and prints the local paths.

    Args:
        ctx: Click context.
        name: Deployment name.
        component: Component to query (e.g. ``predictor``, ``transformer``).
        tail: Number of lines.
        source: ``kubernetes`` or ``opensearch`` (deprecated).
        since: ISO-8601 lower bound on log timestamp.
        until: ISO-8601 upper bound on log timestamp.
        follow: Stream new lines instead of returning a one-shot tail.
        interval: Seconds between polls when following.
        download: Download the HopsFS log archives instead of reading pods.
        log_dir: With ``--download``, the directory to download into.
    """
    deployment = _get_deployment(ctx, name)

    if download:
        source_given = (
            ctx.get_parameter_source("source") is not click.core.ParameterSource.DEFAULT
        )
        if follow or source_given:
            raise click.UsageError(
                "--download cannot be combined with --follow or --source."
            )
        try:
            path = None
            if log_dir is not None:
                path = os.path.expanduser(log_dir)
                os.makedirs(path, exist_ok=True)
            local_paths = deployment.download_logs(path=path)
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Log download failed: {exc}") from exc
        if output.JSON_MODE:
            output.print_json({"paths": local_paths})
            return
        for local_path in local_paths:
            click.echo(local_path)
        return

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
    session.get_project(ctx)  # the model registry needs the full login
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
    ms = session.get_model_serving(ctx)
    try:
        deployment = ms.get_deployment(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Deployment '{name}' not found: {exc}") from exc
    if deployment is None:
        raise click.ClickException(f"Deployment '{name}' not found.")
    return deployment


# region Environment and versions

# A rollout counts as ready in these states; Idle is a scaled-to-zero deployment.
_READY = ("Running", "Idle")
# A save or rollback in these states changes the stored version and starts nothing.
_AT_REST = ("Stopped", "Created", "Creating")
# The SDK refuses a save in these states before it writes anything.
_BUSY = ("Starting", "Stopping", "Updating")
_POLL_SECONDS = 5
# The end of --timeout kept for the read after the write, so a write that
# stalls still leaves time to read what it did.
_READ_RESERVE_SECONDS = 10


def _reserved_env_vars() -> tuple[str, ...]:
    from hopsworks_common.constants import MODEL_SERVING  # noqa: PLC0415

    return MODEL_SERVING.RESERVED_ENV_VARS


def _env_digest(env: dict[str, str] | None) -> str:
    """SHA-256 of the user's variables, sorted: the names Hopsworks sets are left out, values never printed."""
    reserved = _reserved_env_vars()
    user = {k: v for k, v in (env or {}).items() if k not in reserved}
    return hashlib.sha256(
        json.dumps(user, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _version_env(version: Any) -> dict[str, str]:
    pairs = getattr(version, "predictor_env_vars", None) or []
    return dict(p.split("=", 1) if "=" in p else (p, "") for p in pairs)


def _budget(timeout: int) -> tuple[float, float]:
    """The command's deadline, and the earlier one its login, reads and write run under."""
    deadline = time.monotonic() + timeout
    return deadline - min(_READ_RESERVE_SECONDS, timeout / 2), deadline


def _live(ms: Any, name: str) -> dict[str, Any]:
    """Read the deployment, its state and its versions fresh from Hopsworks."""
    deployment = ms.get_deployment(name)
    if deployment is None:
        raise click.ClickException(f"Deployment '{name}' not found.")
    state = deployment.get_state()
    versions = deployment.get_versions()
    active = next((v for v in versions if v.active), None)
    condition = getattr(state, "condition", None) if state is not None else None
    available = None
    if state is not None and state.available_predictor_instances is not None:
        available = state.available_predictor_instances + (
            state.available_transformer_instances or 0
        )
    return {
        "deployment": deployment,
        "versions": versions,
        "version": active.version if active is not None else None,
        "latest": max((v.version for v in versions), default=None),
        "digest": _env_digest(_version_env(active)) if active is not None else None,
        "status": getattr(state, "status", None) if state is not None else None,
        "revision": getattr(state, "revision", None) if state is not None else None,
        "available_instances": available,
        "reason": getattr(condition, "reason", None) if condition else None,
    }


def _public(live: dict[str, Any] | None) -> dict[str, Any] | None:
    if live is None:
        return None
    return {k: v for k, v in live.items() if k not in ("deployment", "versions")}


def _rolled(live: dict[str, Any], prior_revision: str | None) -> bool:
    return live["status"] in _READY and live["revision"] != prior_revision


def _wait_ready(
    ms: Any,
    name: str,
    prior_revision: str | None,
    live: dict[str, Any],
    until: float,
) -> dict[str, Any]:
    """Poll from ``live`` until a new rollout is ready (a revision other than the prior one) or failed, or ``until`` passes."""
    while not _rolled(live, prior_revision) and live["status"] != "Failed":
        remaining = until - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(_POLL_SECONDS, remaining))
        live = _live(ms, name)
    return live


def _never_connected(exc: BaseException) -> bool:
    """Whether a transport error came before any byte of the request could reach the server."""
    import requests  # noqa: PLC0415
    import urllib3  # noqa: PLC0415

    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return True
    if not isinstance(exc, requests.exceptions.ConnectionError):
        return False
    reason = exc.args[0] if exc.args else None
    return isinstance(
        getattr(reason, "reason", reason), urllib3.exceptions.NewConnectionError
    )


@contextlib.contextmanager
def _watched_write(is_write: Any):
    """Record what became of the write request sent inside the block.

    The outcome is ``not_sent`` (no write request left this process, or it
    never connected), ``rejected`` (Hopsworks answered with an error of its
    own: an error code in the body), ``sent`` (the request went out and no
    answer from Hopsworks says what became of it: a reset, a timeout, a
    proxy's error page) or ``ok`` (a 2xx answer).
    """
    import requests  # noqa: PLC0415
    from hopsworks_common import client  # noqa: PLC0415
    from hopsworks_common.client import base  # noqa: PLC0415
    from hopsworks_common.client.exceptions import RestAPIError  # noqa: PLC0415

    _client = client._get_instance()
    send = _client._send_request
    seen = {"outcome": "not_sent"}

    def watched(method: str, path_params: list, *args: Any, **kwargs: Any) -> Any:
        if not is_write(method, [str(p) for p in path_params]):
            return send(method, path_params, *args, **kwargs)
        deadline = base._request_deadline.get()
        if deadline is not None and time.monotonic() >= deadline:
            raise requests.exceptions.Timeout(
                "the time ran out before the write was sent"
            )
        seen["outcome"] = "sent"
        try:
            response = send(method, path_params, *args, **kwargs)
        except RestAPIError as exc:
            if getattr(exc, "error_code", None) not in (None, ""):
                seen["outcome"] = "rejected"
            raise
        except Exception as exc:
            if _never_connected(exc):
                seen["outcome"] = "not_sent"
            raise
        seen["outcome"] = "ok"
        return response

    _client._send_request = watched
    try:
        yield seen
    finally:
        del _client._send_request


def _finish(ctx: click.Context, result: dict[str, Any], text: str) -> None:
    """Print a write verb's result: the JSON contract with --json, a line otherwise; non-zero unless done."""
    ok = result["result"] in ("unchanged", "applied")
    if output.JSON_MODE:
        output.print_json(result)
        if not ok:
            ctx.exit(1)
        return
    if not ok:
        raise click.ClickException(text)
    click.echo(text)


def _apply_write(
    ms: Any,
    name: str,
    before: dict[str, Any],
    write: Any,
    is_write: Any,
    is_target: Any,
    budget: tuple[float, float],
    result: dict[str, Any],
) -> dict[str, Any]:
    """Run a save or rollback, wait for it, and judge it from what Hopsworks answered and a fresh read.

    ``accepted`` is True once a read shows the target. A write is
    ``refused`` (``accepted`` False) only when it was never sent or
    Hopsworks answered it with an error. A write that was sent and not
    answered (a reset, a timeout) and is not seen in the read is ``unknown``
    (``accepted`` None): Hopsworks may still be applying it.
    """
    from hopsworks_common.client.base import request_deadline  # noqa: PLC0415

    work_end, deadline = budget
    result["phase"] = "save"
    with _watched_write(is_write) as seen:
        try:
            # The SDK prints progress to stdout, which carries the --json payload.
            with request_deadline(work_end), contextlib.redirect_stdout(sys.stderr):
                write()
        except Exception as exc:  # noqa: BLE001 - judged below
            from hopsworks.cli.main import _explain_rest_error  # noqa: PLC0415

            response = getattr(exc, "response", None)
            result["http_status"] = getattr(response, "status_code", None)
            result["error"] = (
                _explain_rest_error(exc) or (str(exc).splitlines() or [repr(exc)])[0]
            )[:500]
    result["write"] = seen["outcome"]
    if seen["outcome"] == "sent" and result["http_status"] is not None:
        result["error"] = (
            f"HTTP {result['http_status']} without an answer from Hopsworks "
            "(no error code: a proxy or gateway page)"
        )
    proven_not_applied = seen["outcome"] in ("not_sent", "rejected")

    result["phase"] = "read"
    with request_deadline(deadline):
        try:
            after = _live(ms, name)
        except Exception as exc:  # noqa: BLE001
            result["after"] = None
            if proven_not_applied:
                result.update(result="refused", accepted=False)
            else:
                result.update(result="unknown", accepted=None)
                result["error"] = result.get("error") or f"read failed: {exc}"
            return result
        result["after"] = _public(after)
        if not is_target(after):
            if proven_not_applied:
                result.update(result="refused", accepted=False)
            else:
                result.update(result="unknown", accepted=None)
                result["error"] = (
                    result.get("error")
                    or "the write was sent, the read does not show it"
                )
            return result
        result["accepted"] = True

        if before["status"] not in _AT_REST:
            result["phase"] = "wait"
            try:
                after = _wait_ready(ms, name, before["revision"], after, work_end)
            except Exception as exc:  # noqa: BLE001
                result["result"] = "unknown"
                result["error"] = f"read failed: {exc}"
                return result
    ready = before["status"] in _AT_REST or _rolled(after, before["revision"])
    result["after"] = _public(after)
    result["result"] = "applied" if is_target(after) and ready else "not_ready"
    result["phase"] = "done" if result["result"] == "applied" else "wait"
    return result


def _read_before(
    ctx: click.Context, name: str, work_end: float, result: dict[str, Any]
) -> tuple[Any, dict[str, Any]]:
    """Log in and read the deployment as it is before the write, within the command's time."""
    from hopsworks_common.client.base import request_deadline  # noqa: PLC0415

    with request_deadline(work_end):
        ms = session.get_model_serving(ctx)
        result["phase"] = "read"
        try:
            before = _live(ms, name)
        except click.ClickException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not read deployment '{name}': {exc}"
            ) from exc
    result["before"] = _public(before)
    return ms, before


def _unknown_words(name: str, result: dict[str, Any]) -> str:
    return (
        f"Outcome unknown: {result['error']}. Hopsworks may still apply it. "
        f"Read it with `hops deployment versions {name}`."
    )


def _state_words(live: dict[str, Any] | None) -> str:
    if live is None:
        return "state unknown"
    words = f"version {live['version']} active, {live['status']}"
    if live.get("reason") and live["status"] not in _READY:
        words += f" ({live['reason']})"
    return words


_TIMEOUT_HELP = (
    "Seconds the whole command may take: login, reads, the write and the wait "
    "for Running. No request starts after it, and a request still running "
    "then (connecting, waiting for its answer or reading it) is cut off; the "
    f"last {_READ_RESERVE_SECONDS} s are kept for reading the outcome."
)


@deployment_group.command("env")
@click.argument("name")
@click.pass_context
def deployment_env(ctx: click.Context, name: str) -> None:
    """Print the predictor's environment variables, one KEY=VALUE per line.

    With --json, an object of names to values. Values are printed as stored.

    Args:
        ctx: Click context.
        name: Deployment name.
    """
    env = _get_deployment(ctx, name).env_vars or {}
    if output.JSON_MODE:
        output.print_json(env)
        return
    if not env:
        click.echo(f"{name} has no environment variables.", err=True)
    for key in sorted(env):
        click.echo(f"{key}={env[key]}")


def _parse_assignments(pairs: tuple[str, ...]) -> dict[str, str]:
    env: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key.strip():
            raise click.BadParameter(
                f"'{pair}' is not KEY=VALUE.", param_hint="KEY=VALUE"
            )
        env[key.strip()] = value
    return env


@deployment_group.command("set-env")
@click.argument("name")
@click.argument("assignments", nargs=-1, metavar="[KEY=VALUE]...")
@click.option(
    "--unset", "unset", multiple=True, metavar="KEY", help="Remove KEY (repeatable)."
)
@click.option(
    "--timeout",
    type=click.IntRange(min=1),
    default=300,
    show_default=True,
    help=_TIMEOUT_HELP,
)
@click.pass_context
def deployment_set_env(
    ctx: click.Context,
    name: str,
    assignments: tuple[str, ...],
    unset: tuple[str, ...],
    timeout: int,
) -> None:
    """Set or remove predictor environment variables, saved as a new version.

    The given variables are merged into the current ones. Nothing differs:
    "unchanged", nothing is written. A running deployment rolls to the new
    version without stopping; the old instances serve until it is Running.
    A stopped one is saved and stays stopped. Hopsworks refuses a version
    that does not fit in the project's quota; the refusal is passed on.
    Undo with `hops deployment rollback NAME <previous version>`.

    With --json: {name, result, accepted, write, phase, set, unset, before,
    after, http_status, error}. result is unchanged, applied, refused,
    not_ready or unknown. refused means nothing was saved: the save was never
    sent or Hopsworks answered it with an error. unknown means the save was
    sent and its outcome is not known; it may still be applied. accepted is
    True once a read shows the new version. Values are never printed, only
    key names and digests.

    Args:
        ctx: Click context.
        name: Deployment name.
        assignments: KEY=VALUE pairs to set.
        unset: Keys to remove.
        timeout: Seconds the command may take.
    """
    sets = _parse_assignments(assignments)
    if not sets and not unset:
        raise click.UsageError("Give KEY=VALUE pairs to set, --unset KEY, or both.")
    both = sorted(set(sets) & set(unset))
    if both:
        raise click.UsageError(f"Set and unset at once: {', '.join(both)}.")
    reserved = sorted((set(sets) | set(unset)) & set(_reserved_env_vars()))
    if reserved:
        raise click.UsageError(
            f"Environment variables {reserved} are set by Hopsworks for this "
            "deployment and cannot be changed."
        )

    budget = _budget(timeout)
    result: dict[str, Any] = {
        "name": name,
        "result": None,
        "accepted": False,
        "write": "not_sent",
        "phase": "login",
        "set": sorted(sets),
        "unset": sorted(unset),
        "before": None,
        "after": None,
        "http_status": None,
        "error": None,
    }
    ms, before = _read_before(ctx, name, budget[0], result)
    deployment = before["deployment"]
    current = dict(deployment.env_vars or {})
    merged = {**current, **sets}
    for key in unset:
        merged.pop(key, None)
    if merged == current:
        result.update(result="unchanged", phase="done")
        _finish(ctx, result, f"Unchanged: {name} already has these variables.")
        return
    result["target_digest"] = _env_digest(merged)

    if before["status"] in _BUSY:
        result.update(result="refused", error=f"deployment is {before['status']}")
        _finish(
            ctx,
            result,
            f"Refused, nothing saved: {name} is {before['status']}; "
            "try again once it is Running or Stopped.",
        )
        return

    def write() -> None:
        deployment.env_vars = merged
        # On a save the SDK uploads a file in the working directory named like
        # the stored script (a bare basename) in place of that script. This
        # save changes the variables only, so it runs from an empty directory.
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as empty:
            os.chdir(empty)
            try:
                deployment.save(await_update=0, new_version=True)
            finally:
                os.chdir(cwd)

    _apply_write(
        ms,
        name,
        before,
        write,
        lambda method, path: method == "PUT" and path[-1:] == ["serving"],
        lambda live: (
            live["version"] is not None
            and live["version"] > before["latest"]
            and live["digest"] == result["target_digest"]
        ),
        budget,
        result,
    )
    after = result["after"]
    if result["result"] == "applied":
        text = f"Saved {name} as version {after['version']}; {_state_words(after)}."
    elif result["result"] == "refused":
        text = (
            f"Refused, nothing saved: {result['error'] or 'the save was not applied'}."
        )
    elif result["result"] == "not_ready":
        text = (
            f"Saved {name} as version {after['version']}, not ready after "
            f"{timeout} s: {_state_words(after)}. The previous version "
            f"{before['version']} is `hops deployment rollback {name} "
            f"{before['version']}`."
        )
    else:
        text = _unknown_words(name, result)
    _finish(ctx, result, text)


@deployment_group.command("versions")
@click.argument("name")
@click.pass_context
def deployment_versions(ctx: click.Context, name: str) -> None:
    """List the deployment's versions, newest first, with their environment key names and digest.

    Args:
        ctx: Click context.
        name: Deployment name.
    """
    deployment = _get_deployment(ctx, name)
    try:
        versions = deployment.get_versions()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not read the versions: {exc}") from exc
    rows = []
    for v in versions:
        env = _version_env(v)
        rows.append(
            {
                "version": v.version,
                "active": bool(v.active),
                "created": v.created,
                "created_by": v.created_by,
                "env_keys": sorted(env),
                "env_digest": _env_digest(env),
            }
        )
    if output.JSON_MODE:
        output.print_json(rows)
        return
    output.print_table(
        ["VERSION", "ACTIVE", "CREATED", "CREATED BY", "ENV KEYS", "ENV DIGEST"],
        [
            [
                r["version"],
                "yes" if r["active"] else "",
                r["created"] or "-",
                r["created_by"] or "-",
                ",".join(r["env_keys"]) or "-",
                r["env_digest"][:12],
            ]
            for r in rows
        ],
    )


@deployment_group.command("rollback")
@click.argument("name")
@click.argument("version", type=int)
@click.option(
    "--timeout",
    type=click.IntRange(min=1),
    default=120,
    show_default=True,
    help=_TIMEOUT_HELP,
)
@click.pass_context
def deployment_rollback(
    ctx: click.Context, name: str, version: int, timeout: int
) -> None:
    """Make an earlier version of the deployment the active one again.

    Accepted while a new version is updating or has failed; the serving
    instances keep serving until the version is ready. A stopped deployment
    stays stopped. With --json, the same contract as set-env.

    Args:
        ctx: Click context.
        name: Deployment name.
        version: The version to activate.
        timeout: Seconds the command may take.
    """
    budget = _budget(timeout)
    result: dict[str, Any] = {
        "name": name,
        "result": None,
        "accepted": False,
        "write": "not_sent",
        "phase": "login",
        "version": version,
        "before": None,
        "after": None,
        "http_status": None,
        "error": None,
    }
    ms, before = _read_before(ctx, name, budget[0], result)
    if before["version"] == version:
        result.update(result="unchanged", phase="done")
        _finish(ctx, result, f"Unchanged: version {version} of {name} is active.")
        return
    if version not in {v.version for v in before["versions"]}:
        result.update(result="refused", error=f"no version {version}")
        _finish(
            ctx, result, f"Refused, nothing changed: {name} has no version {version}."
        )
        return
    if before["status"] in ("Starting", "Stopping"):
        result.update(result="refused", error=f"deployment is {before['status']}")
        _finish(
            ctx,
            result,
            f"Refused, nothing changed: {name} is {before['status']}.",
        )
        return

    deployment = before["deployment"]
    _apply_write(
        ms,
        name,
        before,
        lambda: deployment.rollback(version, await_update=0),
        lambda method, path: method == "POST" and path[-1:] == ["rollback"],
        lambda live: live["version"] == version,
        budget,
        result,
    )
    after = result["after"]
    if result["result"] == "applied":
        text = f"Rolled {name} back to version {version}; {_state_words(after)}."
    elif result["result"] == "refused":
        text = f"Refused, nothing changed: {result['error'] or 'the rollback was not applied'}."
    elif result["result"] == "not_ready":
        text = (
            f"Rolled {name} back to version {version}, not ready after {timeout} s: "
            f"{_state_words(after)}."
        )
    else:
        text = _unknown_words(name, result)
    _finish(ctx, result, text)


# endregion

"""``hops model`` — model registry read and write commands.

The SDK's ``ModelRegistry`` only exposes ``get_model(name, version)`` and
``get_models(name)`` — there is no list-all-models method. For ``hops model
list`` we hit the REST endpoint directly. ``register``/``download``/``delete``
drive the framework-specific ``create_model`` + ``save`` SDK flow in-process.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output, session


@click.group("model")
def model_group() -> None:
    """Model registry commands."""


@model_group.command("list")
@click.pass_context
def model_list(ctx: click.Context) -> None:
    """List every model registered in the active project.

    Args:
        ctx: Click context.
    """
    project = session.get_project(ctx)
    items = _list_models(project)
    rows = []
    for m in items:
        metrics = m.get("metrics") or {}
        rows.append(
            [
                m.get("name", "?"),
                m.get("version", "?"),
                m.get("framework", "-"),
                _short_metrics(metrics),
            ]
        )
    output.print_table(["NAME", "VERSION", "FRAMEWORK", "METRICS"], rows)


@model_group.command("info")
@click.argument("name")
@click.option("--version", type=int, help="Model version; defaults to latest.")
@click.pass_context
def model_info(ctx: click.Context, name: str, version: int | None) -> None:
    """Show details for a single model version.

    Args:
        ctx: Click context.
        name: Model name.
        version: Specific version; latest if omitted.
    """
    project = session.get_project(ctx)
    mr = project.get_model_registry()
    try:
        if version is not None:
            model = mr.get_model(name, version=version)
        else:
            versions = mr.get_models(name)
            model = versions[-1] if versions else None
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Model '{name}' not found: {exc}") from exc

    if model is None:
        raise click.ClickException(f"Model '{name}' not found.")

    if output.JSON_MODE:
        output.print_json(_model_to_dict(model))
        return

    rows = [
        ["Name", getattr(model, "name", "?")],
        ["Version", getattr(model, "version", "?")],
        ["Framework", getattr(model, "framework", "-")],
        ["Created", getattr(model, "created", "-")],
        ["Metrics", _short_metrics(getattr(model, "training_metrics", {}) or {})],
        ["Description", output.first_line(getattr(model, "description", ""))],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


def _list_models(project: Any) -> list[dict[str, Any]]:
    from hopsworks_common.core import rest

    mr = project.get_model_registry()
    mr_id = getattr(mr, "model_registry_id", None) or getattr(mr, "id", None)
    if mr_id is None:
        return []
    try:
        payload = rest.send_request(
            "GET", rest.project_path("modelregistries", mr_id, "models")
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list models: {exc}") from exc
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("items") or []
    return []


def _short_metrics(metrics: dict[str, Any]) -> str:
    if not metrics:
        return "-"
    parts = [f"{k}={v}" for k, v in list(metrics.items())[:3]]
    return ", ".join(parts)


def _model_to_dict(model: Any) -> dict[str, Any]:
    return {
        "name": getattr(model, "name", None),
        "version": getattr(model, "version", None),
        "framework": getattr(model, "framework", None),
        "created": getattr(model, "created", None),
        "metrics": dict(getattr(model, "training_metrics", {}) or {}),
        "description": getattr(model, "description", None),
    }


# region Write commands


_FRAMEWORK_ATTRS = {
    "sklearn": "sklearn",
    "scikit-learn": "sklearn",
    "tensorflow": "tensorflow",
    "tf": "tensorflow",
    "keras": "tensorflow",
    "torch": "torch",
    "pytorch": "torch",
    "python": "python",
    "llm": "llm",
}


@model_group.command("register")
@click.argument("name")
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--framework",
    type=click.Choice(sorted(set(_FRAMEWORK_ATTRS)), case_sensitive=False),
    default="python",
    show_default=True,
    help="ML framework category.",
)
@click.option(
    "--metrics",
    "metrics_spec",
    help='Comma-separated metrics, e.g. "accuracy=0.95,auc=0.91".',
)
@click.option("--description", default="", help="Model description.")
@click.option("--feature-view", "feature_view", help="Feature view used for training.")
@click.option(
    "--td-version",
    "td_version",
    type=int,
    help="Training dataset version used for training.",
)
@click.option(
    "--input-example",
    "input_example_path",
    type=click.Path(exists=True),
    help="JSON file with a representative input example.",
)
@click.option("--version", type=int, help="Force a specific model version.")
@click.pass_context
def model_register(
    ctx: click.Context,
    name: str,
    path: str,
    framework: str,
    metrics_spec: str | None,
    description: str,
    feature_view: str | None,
    td_version: int | None,
    input_example_path: str | None,
    version: int | None,
) -> None:
    """Upload a trained model artifact to the Hopsworks model registry.

    Args:
        ctx: Click context.
        name: Model name.
        path: Local path to the model artifact (file or directory).
        framework: ML framework category (``sklearn``/``tensorflow``/``torch``/``python``/``llm``).
        metrics_spec: Metrics as ``key=value`` pairs.
        description: Description string.
        feature_view: Feature view the model was trained on.
        td_version: Training dataset version used for training.
        input_example_path: JSON file holding a representative input.
        version: Explicit model version.
    """
    project = session.get_project(ctx)
    mr = project.get_model_registry()
    framework_key = _FRAMEWORK_ATTRS[framework.lower()]
    registry_section = getattr(mr, framework_key, None)
    if registry_section is None:
        raise click.ClickException(f"Framework '{framework}' not supported by the SDK.")

    metrics = _parse_metrics(metrics_spec)
    input_example = None
    if input_example_path:
        try:
            input_example = json.loads(Path(input_example_path).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise click.ClickException(f"Invalid input example: {exc}") from exc

    fv_obj = None
    if feature_view:
        fs = project.get_feature_store()
        try:
            fv_obj = fs.get_feature_view(feature_view)
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Feature view '{feature_view}' not found: {exc}"
            ) from exc

    try:
        model = registry_section.create_model(
            name=name,
            version=version,
            metrics=metrics or None,
            description=description,
            input_example=input_example,
            feature_view=fv_obj,
            training_dataset_version=td_version,
        )
        model.save(path)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Model register failed: {exc}") from exc

    output.success(
        "✓ Registered model %s v%s",
        getattr(model, "name", name),
        getattr(model, "version", "?"),
    )


@model_group.command("download")
@click.argument("name")
@click.option("--version", type=int, help="Model version; latest if omitted.")
@click.option(
    "--output",
    "output_dir",
    type=click.Path(),
    help="Destination directory. Defaults to the SDK's temp dir.",
)
@click.pass_context
def model_download(
    ctx: click.Context, name: str, version: int | None, output_dir: str | None
) -> None:
    """Download a model artifact from the registry.

    Args:
        ctx: Click context.
        name: Model name.
        version: Model version.
        output_dir: Local destination path.
    """
    model = _get_model(ctx, name, version)
    try:
        path = model.download(local_path=output_dir)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Download failed: {exc}") from exc
    output.success("✓ Downloaded model %s to %s", name, path)


@model_group.command("delete")
@click.argument("name")
@click.option("--version", type=int, required=True, help="Model version to delete.")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def model_delete(ctx: click.Context, name: str, version: int, yes: bool) -> None:
    """Delete a single model version.

    Args:
        ctx: Click context.
        name: Model name.
        version: Model version.
        yes: Skip confirmation when True.
    """
    model = _get_model(ctx, name, version)
    if not yes and not output.JSON_MODE:
        click.confirm(f"Delete model '{name}' v{version}?", abort=True)
    try:
        model.delete()
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Delete failed: {exc}") from exc
    output.success("✓ Deleted model %s v%s", name, version)


def _get_model(ctx: click.Context, name: str, version: int | None) -> Any:
    project = session.get_project(ctx)
    mr = project.get_model_registry()
    try:
        if version is not None:
            return mr.get_model(name, version=version)
        versions = mr.get_models(name)
        if not versions:
            raise click.ClickException(f"Model '{name}' not found.")
        return versions[-1]
    except click.ClickException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Model '{name}' not found: {exc}") from exc


def _parse_metrics(spec: str | None) -> dict[str, float]:
    if not spec:
        return {}
    out: dict[str, float] = {}
    for item in spec.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        key, _, value = item.partition("=")
        try:
            out[key.strip()] = float(value)
        except ValueError as exc:
            raise click.BadParameter(
                f"Metric '{item}' must be 'name=number': {exc}",
                param_hint="--metrics",
            ) from exc
    return out

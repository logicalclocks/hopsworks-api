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
from hopsworks.cli import lineage, output, session, versions
from hopsworks.cli.commands.job import _ts_label


@click.group("model")
def model_group() -> None:
    """Models in the model registry."""


@model_group.command("list")
@click.pass_context
def model_list(ctx: click.Context) -> None:
    """List every model registered in the active project.

    \f
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

    \f
    Args:
        ctx: Click context.
        name: Model name.
        version: Specific version; latest if omitted.
    """
    project = session.get_project(ctx)
    mr = project.get_model_registry()
    try:
        model = versions.model(mr, name, version)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not find model '{name}': {exc}") from exc

    if model is None:
        raise click.ClickException(f"Model '{name}' not found.")

    if output.JSON_MODE:
        output.print_json(_model_to_dict(model))
        return

    rows = [
        ["Name", getattr(model, "name", "?")],
        ["Version", getattr(model, "version", "?")],
        ["Framework", getattr(model, "framework", "-")],
        ["Created", _ts_label(getattr(model, "created", None))],
        ["Feature view", _feature_view_label(model)],
        ["Training dataset", getattr(model, "training_dataset_version", None) or "-"],
        ["Metrics", _short_metrics(getattr(model, "training_metrics", {}) or {}, None)],
        ["Description", output.first_line(getattr(model, "description", ""))],
        ["Tags", output.format_mapping(output.read_tags(model))],
    ]
    output.print_table(["FIELD", "VALUE"], rows)


@model_group.command("runs")
@click.argument("name")
@click.option("--sweep", help="Only the runs of this sweep id.")
@click.option(
    "--sort",
    help="Metric to rank by; defaults to the metric each run.json names.",
)
@click.pass_context
def model_runs(
    ctx: click.Context, name: str, sweep: str | None, sort: str | None
) -> None:
    """List the runs of model NAME, its versions, best first.

    Each version is one run. Its run.json (written next to the model files at
    registration) gives the run and sweep ids, the candidate and its params; a
    version without one shows its metrics and description only.

    \f
    Args:
        ctx: Click context.
        name: Model name, the experiment.
        sweep: Only this sweep's runs.
        sort: Metric to rank by.
    """
    project = session.get_project(ctx)
    try:
        models = project.get_model_registry().get_models(name) or []
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not find model '{name}': {exc}") from exc
    runs = _runs(project, models)
    if sweep:
        runs = [r for r in runs if r.get("sweep_id") == sweep]
    metric = sort or next((r["metric"] for r in runs if r.get("metric")), None)
    lower = bool(sort is None and runs and runs[0].get("metric_goal") == "min")
    ranked = [
        r for r in runs if metric and isinstance(r["metrics"].get(metric), (int, float))
    ]
    ranked.sort(key=lambda r: r["metrics"][metric], reverse=not lower)
    if ranked:
        ranked[0]["best"] = True
    runs = ranked + [r for r in runs if r not in ranked]
    if output.JSON_MODE:
        output.print_json(runs)
        return
    if not runs:
        output.info(
            f"No runs of '{name}'" + (f" in sweep {sweep}" if sweep else "") + "."
        )
        return
    output.print_table(
        [
            "VERSION",
            "RUN",
            "SWEEP",
            "CANDIDATE",
            "PARAMS",
            metric or "METRICS",
            "BASELINE",
            "TD",
            "CREATED",
        ],
        [
            [
                f"{r['version']}{' *' if r.get('best') else ''}",
                r.get("run_id") or "-",
                r.get("sweep_id") or "-",
                r.get("candidate") or output.first_line(r.get("description") or "-"),
                output.format_mapping(r.get("params") or {}) or "-",
                r["metrics"].get(metric, "-")
                if metric
                else _short_metrics(r["metrics"]),
                _baseline(r),
                r.get("training_dataset_version") or "-",
                _ts_label(r.get("created")),
            ]
            for r in runs
        ],
    )
    if ranked:
        output.info(f"* best by {metric}")


def _runs(project: Any, models: list[Any]) -> list[dict[str, Any]]:
    """One record per model version: its registry fields merged with its run.json."""
    from concurrent.futures import ThreadPoolExecutor

    dataset_api = project.get_dataset_api()

    def record(model: Any) -> dict[str, Any]:
        run: dict[str, Any] = {}
        try:
            # save() puts the model directory's files under Files/.
            response = dataset_api.read_content(f"{model.version_path}/Files/run.json")
            if response is not None:
                run = json.loads(response.content)
        except Exception:  # noqa: BLE001 - a version registered without run.json
            run = {}
        return {
            **run,
            "version": model.version,
            "metrics": dict(getattr(model, "training_metrics", None) or {}),
            "description": getattr(model, "description", None),
            "training_dataset_version": getattr(model, "training_dataset_version", None)
            or run.get("training_dataset_version"),
            "created": getattr(model, "created", None),
            "best": False,
        }

    with ThreadPoolExecutor(max_workers=8) as pool:
        return list(pool.map(record, models))


def _baseline(run: dict[str, Any]) -> Any:
    metrics = run["metrics"]
    return metrics.get(
        "test_baseline_accuracy", metrics.get("val_baseline_accuracy", "-")
    )


def _feature_view_label(model: Any) -> str:
    """The feature view the model was trained on, `name vN`, from its provenance; `-` without one."""
    try:
        view = model.get_feature_view(init=False)
    except Exception:  # noqa: BLE001 - provenance is optional; a model may have none
        return "-"
    return f"{view.name} v{view.version}" if view is not None else "-"


def _list_models(project: Any) -> list[dict[str, Any]]:
    from hopsworks_common.core import rest

    mr = project.get_model_registry()
    mr_id = getattr(mr, "model_registry_id", None) or getattr(mr, "id", None)
    if mr_id is None:
        return []
    try:
        payload = rest._send_request(
            "GET", rest._project_path("modelregistries", mr_id, "models")
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list models: {exc}") from exc
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("items") or []
    return []


def _short_metrics(metrics: dict[str, Any], limit: int | None = 3) -> str:
    if not metrics:
        return "-"
    parts = [f"{k}={v}" for k, v in list(metrics.items())[:limit]]
    return ", ".join(parts)


def _model_to_dict(model: Any) -> dict[str, Any]:
    return {
        "name": getattr(model, "name", None),
        "version": getattr(model, "version", None),
        "framework": getattr(model, "framework", None),
        "created": getattr(model, "created", None),
        "feature_view": _feature_view_label(model),
        "training_dataset_version": getattr(model, "training_dataset_version", None),
        "metrics": dict(getattr(model, "training_metrics", {}) or {}),
        "description": getattr(model, "description", None),
        "tags": output.read_tags(model),
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
    """Upload a trained model to the model registry.

    \f
    Args:
        ctx: Click context.
        name: Model name.
        path: Local path to the model artifact (file or directory).
        framework: ML framework category (`sklearn`/`tensorflow`/`torch`/`python`/`llm`).
        metrics_spec: Metrics as `key=value` pairs.
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
            input_example = json.loads(
                Path(input_example_path).read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise click.ClickException(f"Invalid input example: {exc}") from exc

    fv_obj = None
    if feature_view:
        fs = project.get_feature_store()
        try:
            fv_obj = versions.feature_view(fs, feature_view, None)
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(
                f"Could not find feature view '{feature_view}': {exc}"
            ) from exc
        if fv_obj is None:
            raise click.ClickException(f"Feature view '{feature_view}' not found.")

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
        raise click.ClickException(f"Could not register model '{name}': {exc}") from exc

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

    \f
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
        raise click.ClickException(f"Could not download model '{name}': {exc}") from exc
    output.success("✓ Downloaded model %s to %s", name, path)


@model_group.command("delete")
@click.argument("name")
@click.option("--version", type=int, required=True, help="Model version to delete.")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def model_delete(ctx: click.Context, name: str, version: int, yes: bool) -> None:
    """Delete a single model version.

    \f
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
        raise click.ClickException(f"Could not delete model '{name}': {exc}") from exc
    output.success("✓ Deleted model %s v%s", name, version)


@model_group.command("lineage")
@click.argument("name")
@click.option("--version", type=int, help="Model version; defaults to latest.")
@click.pass_context
def model_lineage(ctx: click.Context, name: str, version: int | None) -> None:
    """Show upstream lineage for a model.

    Upstream covers the feature view and training dataset the model was
    trained from.

    \f
    Args:
        ctx: Click context.
        name: Model name.
        version: Specific version; latest if omitted.
    """
    model = _get_model(ctx, name, version)
    label = f"model {getattr(model, 'name', name)} v{getattr(model, 'version', '?')}"
    sections = [
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


def _get_model(ctx: click.Context, name: str, version: int | None) -> Any:
    project = session.get_project(ctx)
    mr = project.get_model_registry()
    try:
        model = versions.model(mr, name, version)
        if model is None:
            raise click.ClickException(f"Model '{name}' not found.")
        return model
    except click.ClickException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not find model '{name}': {exc}") from exc


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

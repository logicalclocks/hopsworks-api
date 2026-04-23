"""``hops context`` — dump project state as Markdown or JSON.

Designed for LLM ingestion: one call produces a concise, structured summary
of everything in the active project that Claude or a downstream agent needs
to make decisions about next steps. Port of the Go CLI's ``cmd/context.go``,
but reads go through the SDK so failures surface as Python tracebacks.
"""

from __future__ import annotations

import sys
from typing import Any

import click
from hopsworks.cli import output, session
from hopsworks.cli.commands import fv as fv_cmd
from hopsworks.cli.commands import model as model_cmd


@click.command("context")
@click.pass_context
def context_cmd(ctx: click.Context) -> None:
    """Emit a Markdown or JSON summary of the active project.

    Args:
        ctx: Click context.
    """
    project = session.get_project(ctx)
    fs = project.get_feature_store()

    fgs = _safe(lambda: fs.get_feature_groups(), default=[])
    fvs = _safe(lambda: fv_cmd._list_feature_views(fs), default=[])
    jobs = _safe(lambda: project.get_job_api().get_jobs(), default=[])
    models = _safe(lambda: model_cmd._list_models(project), default=[])
    deployments = _safe(
        lambda: project.get_model_serving().get_deployments(), default=[]
    )

    if output.JSON_MODE:
        output.print_json(
            {
                "project": {
                    "name": getattr(project, "name", None),
                    "id": getattr(project, "id", None),
                    "feature_store_id": getattr(fs, "id", None),
                },
                "feature_groups": [_fg_summary(fg) for fg in fgs],
                "feature_views": [
                    {
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "version": item.get("version"),
                        "labels": item.get("labels"),
                        "description": item.get("description"),
                    }
                    for item in fvs
                ],
                "jobs": [
                    {
                        "name": getattr(j, "name", None),
                        "type": getattr(j, "job_type", None),
                    }
                    for j in jobs
                ],
                "models": [
                    {
                        "name": m.get("name"),
                        "version": m.get("version"),
                        "framework": m.get("framework"),
                    }
                    for m in models
                ],
                "deployments": [
                    {
                        "name": getattr(d, "name", None),
                        "model_name": getattr(d, "model_name", None),
                        "model_version": getattr(d, "model_version", None),
                    }
                    for d in deployments
                ],
            }
        )
        return

    _render_markdown(project, fs, fgs, fvs, jobs, models, deployments)


def _render_markdown(
    project: Any,
    fs: Any,
    fgs: list[Any],
    fvs: list[dict[str, Any]],
    jobs: list[Any],
    models: list[dict[str, Any]],
    deployments: list[Any],
) -> None:
    out = sys.stdout.write
    name = getattr(project, "name", "?")
    out(f"# Hopsworks Project: {name}\n")
    out(
        f"Project ID: {getattr(project, 'id', '?')} | "
        f"Feature Store ID: {getattr(fs, 'id', '?')}\n\n"
    )

    out(f"## Feature Groups ({len(fgs)})\n\n")
    for fg in fgs:
        online = " [online]" if getattr(fg, "online_enabled", False) else ""
        out(f"### {getattr(fg, 'name', '?')} v{getattr(fg, 'version', '?')}{online}\n")
        desc = getattr(fg, "description", "") or ""
        if desc:
            out(f"{desc}\n")
        features = getattr(fg, "features", []) or []
        if features:
            parts = []
            for f in features:
                pk = " (PK)" if getattr(f, "primary", False) else ""
                parts.append(f"{getattr(f, 'name', '?')}:{getattr(f, 'type', '?')}{pk}")
            out("Schema: " + ", ".join(parts) + "\n")
        out("\n")

    out(f"## Feature Views ({len(fvs)})\n\n")
    for item in fvs:
        out(f"### {item.get('name', '?')} v{item.get('version', '?')}\n")
        desc = item.get("description") or ""
        if desc:
            out(f"{desc}\n")
        labels = item.get("labels") or []
        if labels:
            out("Labels: " + ", ".join(labels) + "\n")
        out("\n")

    if jobs:
        out(f"## Jobs ({len(jobs)})\n\n")
        for j in jobs:
            out(f"- {getattr(j, 'name', '?')} ({getattr(j, 'job_type', '-')})\n")
        out("\n")

    if models:
        out(f"## Models ({len(models)})\n\n")
        for m in models:
            out(
                f"- {m.get('name', '?')} v{m.get('version', '?')} ({m.get('framework', '-')})\n"
            )
        out("\n")

    if deployments:
        out(f"## Deployments ({len(deployments)})\n\n")
        for d in deployments:
            out(
                f"- {getattr(d, 'name', '?')} "
                f"→ {getattr(d, 'model_name', '?')} "
                f"v{getattr(d, 'model_version', '?')}\n"
            )
        out("\n")


def _fg_summary(fg: Any) -> dict[str, Any]:
    return {
        "name": getattr(fg, "name", None),
        "version": getattr(fg, "version", None),
        "online_enabled": getattr(fg, "online_enabled", False),
        "description": getattr(fg, "description", None),
        "features": [
            {
                "name": getattr(f, "name", None),
                "type": getattr(f, "type", None),
                "primary": getattr(f, "primary", False),
            }
            for f in getattr(fg, "features", []) or []
        ],
    }


def _safe(fn: Any, default: Any) -> Any:
    """Invoke ``fn`` and swallow any exception, returning ``default`` on failure.

    The context command is a best-effort snapshot — a missing model registry
    or an auth scope issue on one sub-resource should not take down the whole
    dump.

    Args:
        fn: Callable with no args.
        default: Value returned when ``fn`` raises.

    Returns:
        ``fn()`` or ``default`` on failure.
    """
    try:
        return fn()
    except Exception:  # noqa: BLE001
        return default

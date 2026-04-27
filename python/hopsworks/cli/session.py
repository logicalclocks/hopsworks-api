"""Shared session helpers: resolved config and a memoized SDK ``Project``.

Every subcommand that needs an authenticated SDK connection goes through
``get_project(ctx)`` — it calls ``hopsworks.login()`` at most once per
invocation, caches the result on ``ctx.obj``, and surfaces a clean
``ClickException`` when no credentials are available.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import click
from hopsworks.cli import auth, config


if TYPE_CHECKING:
    from hopsworks.project import Project


def get_config(ctx: click.Context) -> config.HopsConfig:
    """Return the ``HopsConfig`` resolved at the root command.

    Args:
        ctx: Click context.

    Returns:
        The HopsConfig stashed on ``ctx.obj`` by the root command.
    """
    obj: dict[str, Any] = ctx.ensure_object(dict)
    cfg = obj.get("config")
    if cfg is None:
        cfg = config.load()
        obj["config"] = cfg
    return cfg


def require_auth(ctx: click.Context) -> config.HopsConfig:
    """Return the resolved config or raise a clear error if unauthenticated.

    Args:
        ctx: Click context.

    Returns:
        The HopsConfig, guaranteed to have ``is_authenticated()`` true.
    """
    cfg = get_config(ctx)
    if not cfg.is_authenticated():
        raise click.ClickException(
            "Not authenticated. Run `hops setup` to get started."
        )
    return cfg


def get_project(ctx: click.Context) -> Project:
    """Return an authenticated SDK ``Project``, creating one on first call.

    The ``Project`` is cached on ``ctx.obj`` so that nested command groups
    (for example ``hops fg preview`` calling helpers that themselves want
    the project) do not re-login.

    Args:
        ctx: Click context.

    Returns:
        The authenticated SDK ``Project``.
    """
    obj: dict[str, Any] = ctx.ensure_object(dict)
    if obj.get("project") is not None:
        return obj["project"]
    cfg = require_auth(ctx)
    try:
        # In internal mode pass ``internal=True`` so the SDK takes its own
        # ``REST_ENDPOINT`` + JWT path. Otherwise pass the user's API key.
        project = auth.login(
            host=cfg.host or "",
            api_key_value=cfg.api_key,
            project=cfg.project,
            internal=cfg.internal,
        )
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Login failed: {exc}") from exc
    obj["project"] = project
    return project


def get_feature_store(ctx: click.Context) -> Any:
    """Return the project's feature store; cached alongside the project.

    Args:
        ctx: Click context.

    Returns:
        The feature store object from ``project.get_feature_store()``.
    """
    obj: dict[str, Any] = ctx.ensure_object(dict)
    if obj.get("fs") is not None:
        return obj["fs"]
    project = get_project(ctx)
    fs = project.get_feature_store()
    obj["fs"] = fs
    return fs

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
            hostname_verification=cfg.hostname_verification,
        )
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Login failed: {exc}") from exc
    obj["project"] = project
    return project


def get_model_serving(ctx: click.Context) -> Any:
    """Return the project's ``ModelServing`` handle, logging in for serving only when nothing else did.

    The ``hops deployment`` verbs that touch only a deployment run with a key
    that holds just the SERVING scope (``auth.login_serving``). A command that
    already logged in fully reuses that project. Cached on ``ctx.obj``.

    Args:
        ctx: Click context.

    Returns:
        The SDK ``ModelServing`` handle.
    """
    obj: dict[str, Any] = ctx.ensure_object(dict)
    if obj.get("model_serving") is not None:
        return obj["model_serving"]
    if obj.get("project") is not None:
        ms = obj["project"].get_model_serving()
    else:
        cfg = require_auth(ctx)
        if cfg.internal:
            ms = get_project(ctx).get_model_serving()
        else:
            if not cfg.project:
                raise click.ClickException(
                    "No project set. Pass --project or set HOPSWORKS_PROJECT."
                )
            try:
                ms = auth.login_serving(
                    host=cfg.host or "",
                    api_key_value=cfg.api_key or "",
                    project=cfg.project,
                    project_id=cfg.project_id,
                    hostname_verification=cfg.hostname_verification,
                )
            except click.ClickException:
                raise
            except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
                raise click.ClickException(f"Login failed: {exc}") from exc
    obj["model_serving"] = ms
    return ms


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


def get_accessible_feature_stores(ctx: click.Context) -> list[Any]:
    """Return every feature store the project can access (own + shared).

    Mirrors what the UI lists: the project's own feature store plus any shared
    with it. The own store is placed first so callers can prefer it when a
    feature group name is ambiguous. Cached on ``ctx.obj``.

    Args:
        ctx: Click context.

    Returns:
        A list of feature store objects, own store first, de-duplicated by id.
    """
    obj: dict[str, Any] = ctx.ensure_object(dict)
    if obj.get("all_fs") is not None:
        return obj["all_fs"]
    own = get_feature_store(ctx)
    from hsfs.core.feature_store_api import FeatureStoreApi

    try:
        stores = FeatureStoreApi().get_all()
    except Exception:  # noqa: BLE001 - degrade to just the project store
        stores = [own]
    own_id = getattr(own, "id", None)
    ordered = [own] + [s for s in stores if getattr(s, "id", None) != own_id]
    obj["all_fs"] = ordered
    return ordered

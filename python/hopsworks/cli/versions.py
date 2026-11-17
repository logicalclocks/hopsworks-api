"""Version resolution for the ``hops`` CLI: an omitted ``--version`` means the latest.

A destructive command passes ``exact=True``: it never guesses, so an omitted
version is refused when more than one version exists.

The SDK's getters default a missing version to 1 (``get_feature_group``,
``get_feature_view`` and ``get_model`` warn and fetch version 1), and the
lists are not ordered oldest first (the model registry lists newest first), so
neither the getter nor the list's last entry is the latest version.
"""

from __future__ import annotations

from typing import Any

import click


class AmbiguousVersionError(click.ClickException):
    """More than one version exists and the command needs `--version`."""


def _latest(items: list[Any] | None, kind: str, name: str, exact: bool) -> Any:
    if not items:
        return None
    if exact and len(items) > 1:
        found = ", ".join(str(v) for v in sorted(item.version for item in items))
        raise AmbiguousVersionError(
            f"{kind} '{name}' has versions {found}; pass --version to choose one."
        )
    return max(items, key=lambda item: item.version)


def feature_group(fs: Any, name: str, version: int | None, exact: bool = False) -> Any:
    """The feature group at `version`, or its highest version; None when there is none.

    Args:
        fs: The feature store.
        name: The feature group.
        version: Its version; None for the highest.
        exact: Refuse a missing version when there are several.

    Returns:
        The feature group.
    """
    if version is not None:
        return fs.get_feature_group(name, version=version)
    return _latest(fs.get_feature_groups(name), "Feature group", name, exact)


def feature_view(fs: Any, name: str, version: int | None, exact: bool = False) -> Any:
    """The feature view at `version`, or its highest version; None when there is none.

    Args:
        fs: The feature store.
        name: The feature view.
        version: Its version; None for the highest.
        exact: Refuse a missing version when there are several.

    Returns:
        The feature view.
    """
    if version is not None:
        return fs.get_feature_view(name, version=version)
    return _latest(fs.get_feature_views(name), "Feature view", name, exact)


def model(registry: Any, name: str, version: int | None, exact: bool = False) -> Any:
    """The model at `version`, or its highest version; None when there is none.

    Args:
        registry: The model registry.
        name: The model.
        version: Its version; None for the highest.
        exact: Refuse a missing version when there are several.

    Returns:
        The model.
    """
    if version is not None:
        return registry.get_model(name, version=version)
    return _latest(registry.get_models(name), "Model", name, exact)

"""Version resolution for the ``hops`` CLI: an omitted ``--version`` means the latest.

The SDK's getters default a missing version to 1 (``get_feature_group``,
``get_feature_view`` and ``get_model`` warn and fetch version 1), and the
lists are not ordered oldest first (the model registry lists newest first), so
neither the getter nor the list's last entry is the latest version.
"""

from __future__ import annotations

from typing import Any


def _latest(items: list[Any] | None) -> Any:
    return max(items, key=lambda item: item.version) if items else None


def feature_group(fs: Any, name: str, version: int | None) -> Any:
    """The feature group at `version`, or its highest version; None when there is none.

    Args:
        fs: The feature store.
        name: The feature group.
        version: Its version; None for the highest.

    Returns:
        The feature group.
    """
    if version is not None:
        return fs.get_feature_group(name, version=version)
    return _latest(fs.get_feature_groups(name))


def feature_view(fs: Any, name: str, version: int | None) -> Any:
    """The feature view at `version`, or its highest version; None when there is none.

    Args:
        fs: The feature store.
        name: The feature view.
        version: Its version; None for the highest.

    Returns:
        The feature view.
    """
    if version is not None:
        return fs.get_feature_view(name, version=version)
    return _latest(fs.get_feature_views(name))


def model(registry: Any, name: str, version: int | None) -> Any:
    """The model at `version`, or its highest version; None when there is none.

    Args:
        registry: The model registry.
        name: The model.
        version: Its version; None for the highest.

    Returns:
        The model.
    """
    if version is not None:
        return registry.get_model(name, version=version)
    return _latest(registry.get_models(name))

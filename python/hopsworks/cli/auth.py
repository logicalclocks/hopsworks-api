"""Thin wrapper around ``hopsworks.login()`` for the CLI.

Importing ``hopsworks`` transitively imports ``hsfs``/``hsml`` which adds ~1s
of startup cost, so every function here defers that import until the caller
actually needs a live SDK connection.
The module also exposes a ``normalize_host()`` helper shared by the token-flow
commands and ``hops login``.
"""

from __future__ import annotations

import logging
import urllib.parse
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:  # pragma: no cover - import only for type hints
    from hopsworks.project import Project


_logger = logging.getLogger(__name__)


def normalize_host(host: str) -> str:
    """Return ``host`` as ``https://<netloc>`` with trailing API paths stripped.

    The SDK stores REST endpoints as ``https://host/hopsworks-api/api``; the
    user may paste any of the above forms. Stripping them here keeps URL
    construction in other modules a simple ``f"{host}/hopsworks-api/api/..."``.

    Args:
        host: Raw host string as typed by the user or loaded from config.

    Returns:
        A canonical ``scheme://netloc`` form with no trailing path.
    """
    if "://" not in host:
        host = "https://" + host
    parsed = urllib.parse.urlparse(host.strip())
    netloc = parsed.netloc or parsed.path
    base = f"{parsed.scheme or 'https'}://{netloc}"
    return base.rstrip("/")


def api_base(host: str) -> str:
    """Return the ``/hopsworks-api/api`` base URL for a given host.

    Args:
        host: Raw or normalized host string.

    Returns:
        A full REST base URL ready for endpoint concatenation.
    """
    return normalize_host(host) + "/hopsworks-api/api"


def login(
    host: str,
    api_key_value: str | None = None,
    project: str | None = None,
    engine: str | None = None,
) -> Project:
    """Call ``hopsworks.login()`` with a stable argument shape.

    Separated from ``hopsworks.login`` so the CLI can swap in test doubles and
    so the heavy SDK import stays lazy.

    Args:
        host: Hopsworks host; ``https://`` prefix and trailing paths are tolerated.
        api_key_value: API key; omit to let the SDK pick up its own env/cache.
        project: Default project to attach to.
        engine: Optional SDK engine override (``python``, ``spark``, …).

    Returns:
        The authenticated SDK ``Project`` object.
    """
    parsed = urllib.parse.urlparse(host if "://" in host else "https://" + host)
    netloc = parsed.netloc or parsed.path
    port = parsed.port or 443
    hostname = netloc.split(":")[0]

    import hopsworks  # noqa: PLC0415 - intentionally lazy

    kwargs: dict[str, Any] = {
        "host": hostname,
        "port": port,
    }
    if api_key_value:
        kwargs["api_key_value"] = api_key_value
    if project:
        kwargs["project"] = project
    if engine:
        kwargs["engine"] = engine
    return hopsworks.login(**kwargs)


def verify(host: str, api_key_value: str, project: str | None = None) -> Project:
    """Run a minimal login to confirm ``api_key_value`` works against ``host``.

    Raises whatever ``hopsworks.login()`` raises on failure so the caller can
    surface a specific error message to the user.

    Args:
        host: Hopsworks host.
        api_key_value: API key to validate.
        project: Optional project to attach to.

    Returns:
        The authenticated SDK ``Project`` object.
    """
    return login(host=host, api_key_value=api_key_value, project=project)

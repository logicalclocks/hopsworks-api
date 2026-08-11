"""Thin REST wrappers for the Hopsworks web-terminal resource.

The terminal is a hopsworks-ee feature (feature-flag ``ENABLE_TERMINAL``): a
per-user Kubernetes pod that runs the user's shell inside ``tmux`` behind a
WebSocket proxy, with Claude Code pre-installed in the image. These helpers
call the REST side of that resource (``/project/{id}/terminal/...``); the
WebSocket side (typing into ``tmux``) is a separate, later concern.

Kept under ``cli/`` rather than ``core/`` on purpose: this is CLI-only
plumbing for ``hops session``, not a sanctioned SDK surface, so it must not
grow a ``@public`` annotation until that decision is made deliberately.
"""

from __future__ import annotations

from hopsworks_common import client


def start_session(project_id: int) -> dict:
    """Start (or return the already-running) terminal session for the caller.

    The backend spins up the per-user terminal pod with default compute when
    none is running and returns its connection descriptor: at least a session
    id and a WebSocket URL (``wsUrl``) plus a short-lived token.

    Args:
        project_id: The target project's numeric id.

    Returns:
        The raw session descriptor from the backend.

    Raises:
        hopsworks.client.exceptions.RestAPIError: When the terminal feature is
            disabled on the cluster, or the caller lacks access.
    """
    _client = client._get_instance()
    return _client._send_request("POST", ["project", project_id, "terminal", "start"])


def get_proxy_token(project_id: int) -> str:
    """Mint a restricted proxy token for attaching to the terminal WebSocket.

    The web terminal's WebSocket authenticates on a ``proxy_session`` cookie the
    browser gets at login; an API-key client has no such cookie. This endpoint
    mints one, pinned to the terminal proxy (rejected by every other proxy) and
    non-renewable. The caller sends it as the ``proxy_session`` cookie on the WS
    handshake; the handshake still enforces session ownership, so the token
    grants nothing the caller could not already reach through this resource.

    Args:
        project_id: The target project's numeric id.

    Returns:
        The signed proxy token string.

    Raises:
        hopsworks.client.exceptions.RestAPIError: When the terminal feature is
            disabled on the cluster, or the caller lacks access.
    """
    _client = client._get_instance()
    resp = _client._send_request(
        "POST", ["project", project_id, "terminal", "proxy-token"]
    )
    return resp["token"]


def get_session(project_id: int) -> dict | None:
    """Return the caller's current terminal session descriptor, or ``None``.

    Args:
        project_id: The target project's numeric id.

    Returns:
        The session descriptor when one is running, else ``None``.
    """
    _client = client._get_instance()
    return _client._send_request("GET", ["project", project_id, "terminal", "session"])


def stop_session(project_id: int) -> None:
    """Force-stop the caller's terminal for the project.

    A DELETE on the terminal resource root: it tears down the terminal pod (and
    every session tab in it) regardless of DB state, so a user can close their
    terminal from the CLI without Kubernetes access.

    Args:
        project_id: The target project's numeric id.

    Raises:
        hopsworks.client.exceptions.RestAPIError: When the terminal feature is
            disabled on the cluster, or the caller lacks access.
    """
    _client = client._get_instance()
    _client._send_request("DELETE", ["project", project_id, "terminal"])

"""``hops setup`` — browser-based authentication flow.

Implements the Modal-style token flow backed by ``/api/token-flow/*`` in
Hopsworks. The CLI creates a flow, opens a browser to a frontend page where
the user picks a project and (optionally) edits the API key name, then
long-polls ``/wait`` until the server returns a generated API key.
The key is written to ``~/.hops.toml`` with mode ``0600``.
"""

from __future__ import annotations

import os
import platform
import re
import socket
import time
import webbrowser

import click
import requests
from hopsworks.cli import auth, config, output


TOKEN_FLOW_CREATE = "/token-flow/create"
TOKEN_FLOW_WAIT = "/token-flow/wait"
POLL_TIMEOUT_SECONDS = 40
REQUEST_TIMEOUT_SECONDS = 60
KEY_NAME_REGEX = re.compile(r"^[a-zA-Z0-9_-]{1,45}$")


def _suggest_key_name() -> str:
    """Produce a default API key name like ``jim-laptop`` or ``jdowling-dev``.

    Uses ``$USER`` and the short hostname; any character outside ``[a-z0-9_-]``
    is stripped so the result satisfies the backend's validation regex on the
    first try. Length is clipped to 45.
    """
    user = os.environ.get("USER") or os.environ.get("USERNAME") or "hops"
    host = socket.gethostname().split(".", 1)[0] or platform.node().split(".", 1)[0]
    raw = f"{user}-{host}".lower()
    sanitized = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-") or "hops-cli"
    return sanitized[:45]


def _open_browser(url: str, headless: bool) -> bool:
    """Try to open ``url`` in the user's default browser.

    Returns True when the browser was launched; False when we detected a
    headless context (no ``$DISPLAY`` on Linux, SSH session) or the launch
    raised. The caller then falls back to printing the URL.
    """
    if headless:
        return False
    if os.environ.get("SSH_CONNECTION") and not os.environ.get("DISPLAY"):
        return False
    try:
        return webbrowser.open(url, new=2, autoraise=True)
    except webbrowser.Error:
        return False


def _create_flow(api_base: str, key_name: str) -> dict:
    resp = requests.post(
        f"{api_base}{TOKEN_FLOW_CREATE}",
        params={"key_name": key_name, "utm_source": "hops-cli"},
        timeout=REQUEST_TIMEOUT_SECONDS,
        verify=False,  # noqa: S501 - Hopsworks clusters often use self-signed certs
    )
    resp.raise_for_status()
    return resp.json()


def _wait_for_key(
    api_base: str,
    flow_id: str,
    wait_secret: str,
    overall_timeout: int,
) -> dict:
    """Long-poll the backend until we receive a key or exceed ``overall_timeout`` seconds.

    The server responds within ~40s with either a completed payload or
    ``{"timeout": true}``; on timeout we re-issue immediately — Modal does the
    same — so the user sees continuous progress rather than gaps.
    """
    deadline = time.monotonic() + overall_timeout
    while time.monotonic() < deadline:
        resp = requests.get(
            f"{api_base}{TOKEN_FLOW_WAIT}/{flow_id}",
            params={"wait_secret": wait_secret, "timeout": POLL_TIMEOUT_SECONDS},
            timeout=POLL_TIMEOUT_SECONDS + 10,
            verify=False,  # noqa: S501
        )
        resp.raise_for_status()
        body = resp.json()
        if not body.get("timeout"):
            return body
    raise click.ClickException(
        "Timed out waiting for authentication. Re-run `hops setup` to try again."
    )


@click.command("setup")
@click.option("--host", "host_flag", help="Hopsworks host URL.")
@click.option(
    "--key-name",
    "key_name_flag",
    help="Name for the generated API key. Default suggested from $USER and hostname.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Re-run the browser flow even when a valid key is already cached.",
)
@click.option(
    "--no-browser",
    is_flag=True,
    help="Do not open a browser; just print the URL.",
)
@click.option(
    "--timeout",
    type=int,
    default=15 * 60,
    show_default=True,
    help="Total seconds to wait for browser completion before giving up.",
)
@click.pass_context
def setup_cmd(
    ctx: click.Context,
    host_flag: str | None,
    key_name_flag: str | None,
    force: bool,
    no_browser: bool,
    timeout: int,
) -> None:
    """Authenticate with Hopsworks and cache an API key in ``~/.hops.toml``.

    On first run: opens a browser, lets the user pick a project and confirm
    the API key name, then fetches the generated key via long polling.
    On re-run: verifies the cached key still works and exits silently unless
    ``--force`` is passed, which always starts a new browser flow.

    Args:
        ctx: Click context; used to return non-zero on verification failure.
        host_flag: Value of ``--host``; prompted when absent.
        key_name_flag: Value of ``--key-name``; when absent the CLI suggests a name.
        force: When True, skip the cached-key short-circuit.
        no_browser: When True, print the URL instead of opening a browser.
        timeout: Seconds to wait for browser completion before erroring out.
    """
    cfg = config.load(flag_host=host_flag)

    if cfg.internal:
        _handle_internal(cfg)
        return

    if not force and cfg.is_authenticated():
        _verify_and_report(cfg)
        return

    host = cfg.host or click.prompt("Hopsworks host", default=config.DEFAULT_HOST)
    host = auth.normalize_host(host)
    api_base = auth.api_base(host)

    key_name = key_name_flag or _suggest_key_name()
    if not key_name_flag:
        key_name = click.prompt("API key name", default=key_name)
    if not KEY_NAME_REGEX.match(key_name):
        raise click.BadParameter(
            "Key name must match [a-zA-Z0-9_-]{1,45}.",
            param_hint="--key-name",
        )

    try:
        created = _create_flow(api_base, key_name)
    except requests.RequestException as exc:
        raise click.ClickException(f"Could not start token flow: {exc}") from exc

    flow_id = created["flowId"]
    wait_secret = created["waitSecret"]
    web_url = created["webUrl"]

    opened = _open_browser(web_url, headless=no_browser)
    output.info("")
    output.info("The hops CLI needs to authenticate with Hopsworks.")
    if opened:
        output.info("Opening your browser: %s", web_url)
    else:
        output.info("Visit this URL in a browser to continue:")
        output.info("  %s", web_url)
    output.info("")
    output.info("Waiting for authentication...")

    try:
        completed = _wait_for_key(api_base, flow_id, wait_secret, timeout)
    except requests.RequestException as exc:
        raise click.ClickException(f"Token flow failed: {exc}") from exc

    api_key = completed.get("apiKey")
    project = completed.get("workspaceUsername")
    server_key_name = completed.get("apiKeyName") or key_name

    if not api_key:
        raise click.ClickException("Server did not return an API key.")

    cfg.host = host
    cfg.api_key = api_key
    cfg.api_key_name = server_key_name
    cfg.project = project
    config.save(cfg)

    try:
        auth.verify(host=host, api_key_value=api_key, project=project)
    except Exception as exc:  # noqa: BLE001 - SDK raises a bag of types
        output.error(
            "Saved key to %s but verification failed: %s. "
            "Re-run `hops setup --force` to retry.",
            config.CONFIG_PATH,
            exc,
        )
        ctx.exit(1)

    output.success(
        "✓ Connected to %s as %s",
        host.replace("https://", "").replace("http://", ""),
        project or "(no project)",
    )
    output.info(
        "Token written to %s with api-key name %s",
        config.CONFIG_PATH,
        server_key_name,
    )


def _handle_internal(cfg: config.HopsConfig) -> None:
    """Internal mode: JWT is already mounted, no browser flow needed."""
    try:
        project = auth.login(host=cfg.host or "", project=cfg.project)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Internal login failed: {exc}") from exc
    output.success(
        "✓ Connected (internal mode) to project %s",
        getattr(project, "name", cfg.project or "?"),
    )


def _verify_and_report(cfg: config.HopsConfig) -> None:
    """Already-configured short-circuit: verify the cached key, print status, exit 0."""
    try:
        project = auth.verify(
            host=cfg.host or "",
            api_key_value=cfg.api_key or "",
            project=cfg.project,
        )
    except Exception as exc:  # noqa: BLE001
        output.warn(
            "Cached key in %s no longer works: %s. Re-run with --force to refresh.",
            config.CONFIG_PATH,
            exc,
        )
        raise click.ClickException("Authentication failed.") from exc
    output.success(
        "✓ Connected to %s as %s (key: %s)",
        (cfg.host or "").replace("https://", "").replace("http://", ""),
        getattr(project, "name", cfg.project or "?"),
        cfg.api_key_name or "unnamed",
    )

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
from typing import Any

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


def _resolve_verify(insecure: bool, ca_bundle: str | None) -> Any:
    """Resolve the ``verify`` argument for token-flow ``requests`` calls.

    The token-flow ``/wait`` response carries a freshly minted API key, so a
    MITM on the channel can read or replace it. Defaults are conservative:

    * ``--ca-bundle <path>`` → verify against that CA.
    * Else verify against the system trust store (``True``).
    * Only ``--insecure`` opts out, with an explicit warning.

    Args:
        insecure: True when the user passed ``--insecure``.
        ca_bundle: Path to a CA cert bundle (PEM) when set.

    Returns:
        Value suitable for ``requests``' ``verify`` parameter.
    """
    if ca_bundle:
        return ca_bundle
    if insecure:
        # Disable urllib3's repeated InsecureRequestWarning so a single
        # warning is enough; the user already opted in explicitly.
        try:
            import urllib3  # noqa: PLC0415

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:  # noqa: BLE001
            pass
        return False
    return True


class _ClusterTooOldForTokenFlow(Exception):
    """Marker raised when /token-flow/create returns 404.

    Older Hopsworks releases do not ship the token-flow REST resource;
    rather than surface a raw "404 Not Found" we catch this case in the
    caller and fall back to a manual browser flow where the user creates
    an API key in the UI and pastes it back into the CLI.
    """


def _create_flow(api_base: str, key_name: str, verify: Any) -> dict:
    resp = requests.post(
        f"{api_base}{TOKEN_FLOW_CREATE}",
        params={"key_name": key_name, "utm_source": "hops-cli"},
        timeout=REQUEST_TIMEOUT_SECONDS,
        verify=verify,
    )
    if resp.status_code == 404:
        raise _ClusterTooOldForTokenFlow(
            "/token-flow/create returned 404 — cluster does not support browser flow"
        )
    resp.raise_for_status()
    return resp.json()


# Path the user is sent to in the Hopsworks UI to manage / create API keys.
# Kept relative so it composes cleanly with any host the user authenticated
# against; the page itself handles the "log in if not already" redirect.
_API_KEYS_UI_PATH = "/account/audit/api-keys"


def _manual_browser_flow(host: str, key_name: str) -> tuple[str, str | None]:
    """Open the API-keys UI in the user's browser and prompt for paste-back.

    Used when the cluster does not expose ``/token-flow/*``: the CLI has
    no server-side mediator, so the user logs in and creates the key in
    the browser themselves, copies it, and pastes it into the CLI.

    Hopsworks redirects unauthenticated visitors to ``/login`` and back
    on success, so the same URL handles both "already logged in" and
    "not yet logged in" cases — which is what the user-facing description
    of ``hops setup`` calls for.

    Args:
        host: Canonical ``https://<host>`` form (no trailing path).
        key_name: Suggested key name (echoed in the prompt; the user is
            free to ignore it and use whatever name they prefer in the
            UI — the value the backend records is what we save).

    Returns:
        ``(api_key, project_username)``. ``project_username`` is always
        ``None`` here — there's no server-side flow to tell us which
        project the key was minted for, so the caller falls back to
        prompting the user to pick a project on the next ``hops project
        use`` invocation.
    """
    url = f"{host.rstrip('/')}{_API_KEYS_UI_PATH}"
    opened = _open_browser(url, headless=False)

    output.info("")
    output.info("This cluster does not support the automated browser token flow.")
    output.info("Steps:")
    if opened:
        output.info("  1. A browser tab has opened at %s", url)
    else:
        output.info("  1. Open this URL in a browser: %s", url)
    output.info("  2. Log in (if not already logged in).")
    output.info("  3. Create a new API key. Suggested name: %s", key_name)
    output.info("  4. Copy the key value the UI shows you.")
    output.info("  5. Paste it below.")
    output.info("")
    api_key = click.prompt("API key", hide_input=True)
    if not api_key.strip():
        raise click.ClickException("No API key entered; nothing saved.")
    return api_key.strip(), None


def _wait_for_key(
    api_base: str,
    flow_id: str,
    wait_secret: str,
    overall_timeout: int,
    verify: Any,
) -> dict:
    """Long-poll the backend until we receive a key or exceed ``overall_timeout`` seconds.

    The server responds within ~40s with either a completed payload or
    ``{"timeout": true}``; on timeout we re-issue immediately — Modal does the
    same — so the user sees continuous progress rather than gaps.

    The wait secret travels in the JSON request body, never in the query
    string. Query parameters routinely end up in proxy access logs, browser
    history, distributed traces, and crash reporters; the body is confined
    to the TLS-encrypted payload that the standard log paths do not record.
    """
    deadline = time.monotonic() + overall_timeout
    while time.monotonic() < deadline:
        resp = requests.post(
            f"{api_base}{TOKEN_FLOW_WAIT}/{flow_id}",
            json={"waitSecret": wait_secret, "timeout": POLL_TIMEOUT_SECONDS},
            timeout=POLL_TIMEOUT_SECONDS + 10,
            verify=verify,
        )
        resp.raise_for_status()
        body = resp.json()
        if not body.get("timeout"):
            return body
    # click.ClickException isn't on ruff's known no-return allow-list, so it
    # would flag this otherwise-terminating path as a missing explicit return.
    raise click.ClickException(  # noqa: RET503
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
@click.option(
    "--ca-bundle",
    "ca_bundle",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to a custom CA bundle (PEM) for TLS verification.",
)
@click.option(
    "--insecure",
    is_flag=True,
    help=(
        "Skip TLS verification of the token-flow endpoints. "
        "ONLY for trusted private dev clusters — the /wait response carries "
        "the API key and is a MITM target."
    ),
)
@click.pass_context
def setup_cmd(
    ctx: click.Context,
    host_flag: str | None,
    key_name_flag: str | None,
    force: bool,
    no_browser: bool,
    timeout: int,
    ca_bundle: str | None,
    insecure: bool,
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
        ca_bundle: Path to a CA bundle for TLS verification.
        insecure: When True, skip TLS verification entirely (with warning).
    """
    cfg = config.load(flag_host=host_flag)

    if cfg.internal:
        _handle_internal(cfg)
        return None

    if not force and cfg.is_authenticated():
        _verify_and_report(cfg)
        return None

    host = cfg.host or click.prompt("Hopsworks host", default=config.DEFAULT_HOST)
    host = auth.normalize_host(host)
    api_base = auth.api_base(host)

    # Use the auto-suggested name silently (or whatever the user passed
    # via ``--key-name``). The previous interactive ``API key name``
    # prompt blocked the flow before the browser could launch — and is
    # also redundant on the manual-browser fallback, where the user is
    # the one naming the key in the UI. ``--key-name`` remains for the
    # rare case someone needs to override the default.
    key_name = key_name_flag or _suggest_key_name()
    if not KEY_NAME_REGEX.match(key_name):
        raise click.BadParameter(
            "Key name must match [a-zA-Z0-9_-]{1,45}.",
            param_hint="--key-name",
        )

    verify = _resolve_verify(insecure=insecure, ca_bundle=ca_bundle)
    if insecure:
        output.warn(
            "TLS verification disabled (--insecure). The /wait response carries "
            "your API key — only use this on a trusted network."
        )

    # Try the modern browser token flow first. On 404 we fall back to the
    # legacy email + password path so this command still works on older
    # Hopsworks clusters that predate the /token-flow/* REST resource.
    try:
        created = _create_flow(api_base, key_name, verify)
    except _ClusterTooOldForTokenFlow:
        api_key, project = _manual_browser_flow(host, key_name)
        server_key_name = key_name
        # Skip the browser-flow block below by jumping to the verify+save tail.
        return _finalize_setup(host, api_key, project, server_key_name, cfg)
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
        completed = _wait_for_key(api_base, flow_id, wait_secret, timeout, verify)
    except requests.RequestException as exc:
        raise click.ClickException(f"Token flow failed: {exc}") from exc

    api_key = completed.get("apiKey")
    project = completed.get("workspaceUsername")
    # Older Hopsworks releases do not return ``apiKeyName`` from
    # ``/token-flow/wait`` and ignore the ``key_name`` we sent on ``/create``;
    # in that case the server names the key ``cli-<timestamp>`` regardless of
    # the user's input. Persist only what the server actually confirms — leave
    # ``api_key_name`` unset rather than misrepresent the suggested name as
    # the real one. The ``hops project info`` line will then say "(unnamed)".
    server_key_name = completed.get("apiKeyName")

    if not api_key:
        raise click.ClickException("Server did not return an API key.")

    _finalize_setup(host, api_key, project, server_key_name, cfg)
    return None


def _finalize_setup(
    host: str,
    api_key: str,
    project: str | None,
    server_key_name: str | None,
    cfg: config.HopsConfig,
) -> None:
    """Verify the freshly-minted API key, then persist it to ``~/.hops.toml``.

    Verification happens *before* writing to disk — if the key fails to
    authenticate we don't want a stale or unintended credential left
    behind. The user can re-run ``hops setup --force`` and the config
    remains unchanged.
    """
    try:
        auth.verify(host=host, api_key_value=api_key, project=project)
    except Exception as exc:  # noqa: BLE001 - SDK raises a bag of types
        raise click.ClickException(
            f"Server returned an API key but verification failed: {exc}. "
            "No changes written to ~/.hops.toml. Re-run `hops setup`."
        ) from exc

    cfg.host = host
    cfg.api_key = api_key
    cfg.api_key_name = server_key_name
    cfg.project = project
    config.save(cfg)

    output.success(
        "✓ Connected to %s as %s",
        host.replace("https://", "").replace("http://", ""),
        project or "(no project)",
    )
    output.info(
        "Token written to %s with api-key name %s",
        config.CONFIG_PATH,
        server_key_name or "(server-assigned)",
    )


def _handle_internal(cfg: config.HopsConfig) -> None:
    """Internal mode: JWT is already mounted, no browser flow needed.

    Pass ``internal=True`` so ``auth.login`` invokes ``hopsworks.login()``
    with no host/port/key — the SDK then reads ``REST_ENDPOINT`` and
    ``$SECRETS_DIR/token.jwt`` from the pod environment itself. Doing the
    same lookup at the CLI layer would be duplicative and would silently
    drift if the SDK ever changes its in-pod auth contract.
    """
    try:
        project = auth.login(host=cfg.host or "", project=cfg.project, internal=True)
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

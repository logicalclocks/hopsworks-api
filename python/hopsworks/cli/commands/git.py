"""``hops git`` — the Git provider credentials Hopsworks keeps for your account.

Hopsworks stores one personal access token per Git provider host (GitHub,
GitLab, BitBucket) against your user. Jobs and the web terminal use it for
HTTPS git operations: the terminal pod writes it into ``~/.git-credentials``
with ``credential.helper store``, so ``git clone https://...`` needs no prompt.
``hops session push`` offers to register one when it syncs a checkout whose
remote is HTTPS, or when you prefer a token over shipping an SSH key.

The module-level helpers (``find_provider``, ``register_provider``) are what the
teleport flow calls; the click commands are the user-facing surface.
"""

from __future__ import annotations

import click
from hopsworks.cli import output, session
from hopsworks_common.core import git_provider_api


# CLI spellings -> the enum labels the backend deserializes (GitProvider@XmlEnumValue).
_PROVIDERS = {"github": "GitHub", "gitlab": "GitLab", "bitbucket": "BitBucket"}
# Same defaults as the account-settings form and GitProvider.getDefaultHost().
_DEFAULT_HOST = {
    "GitHub": "github.com",
    "GitLab": "gitlab.com",
    "BitBucket": "bitbucket.org",
}
_HOST_TO_PROVIDER = {v: k for k, v in _DEFAULT_HOST.items()}


def canonical_provider(name: str) -> str:
    """Map a user spelling (``github``, ``GitHub``) to the backend label."""
    label = _PROVIDERS.get((name or "").strip().lower())
    if not label:
        raise click.BadParameter(
            f"unknown provider {name!r}; use one of {', '.join(_PROVIDERS)}"
        )
    return label


def default_host(provider: str) -> str:
    return _DEFAULT_HOST[provider]


def provider_for_host(host: str) -> str | None:
    """The provider label a well-known host belongs to, or None for a custom host."""
    return _HOST_TO_PROVIDER.get((host or "").lower())


def list_providers() -> list:
    """The providers registered for the logged-in user (needs a live client)."""
    return git_provider_api.GitProviderApi()._get_providers() or []


def find_provider(provider: str, host: str):
    """The registered provider matching ``provider`` and ``host``, or None."""
    for p in list_providers():
        if (p.git_provider or "").lower() == provider.lower() and (
            (p.host or default_host(provider)).lower() == host.lower()
        ):
            return p
    return None


def register_provider(provider: str, username: str, token: str, host: str):
    """Store ``token`` for ``provider``@``host``; replaces an existing entry."""
    return git_provider_api.GitProviderApi()._set_provider(
        provider, username, token, host
    )


@click.group("git")
def git_group() -> None:
    """Git provider credentials Hopsworks holds for your account."""


@git_group.group("provider")
def provider_group() -> None:
    """List, register or remove a Git provider personal access token."""


@provider_group.command("list")
@click.pass_context
def list_cmd(ctx: click.Context) -> None:
    """List the Git providers registered for your account.

    Args:
        ctx: Click context.
    """
    session.get_project(ctx)
    rows = [
        {"provider": p.git_provider, "host": p.host or "", "username": p.username}
        for p in list_providers()
    ]
    if output.JSON_MODE:
        output.print_json({"providers": rows})
        return
    if not rows:
        output.info(
            "No Git providers registered. Add one with `hops git provider set`."
        )
        return
    output.print_table(
        ["PROVIDER", "HOST", "USERNAME"],
        [[r["provider"], r["host"], r["username"]] for r in rows],
    )


@provider_group.command("set")
@click.option(
    "--provider",
    default="github",
    show_default=True,
    help="github, gitlab or bitbucket.",
)
@click.option(
    "--username", prompt="Git username", help="Your username on the provider."
)
@click.option(
    "--token",
    help="Personal access token. Prompted for (hidden) when not given; never "
    "logged or echoed.",
)
@click.option("--host", help="Provider host (default: the provider's public host).")
@click.option(
    "--force",
    is_flag=True,
    help="Replace a token already registered for this provider and host.",
)
@click.pass_context
def set_cmd(
    ctx: click.Context,
    provider: str,
    username: str,
    token: str | None,
    host: str | None,
    force: bool,
) -> None:
    """Register a personal access token for a Git provider.

    Idempotent: an existing registration for the same provider and host is left
    alone unless ``--force`` is given, so scripts can call this unconditionally.

    Args:
        ctx: Click context.
        provider: github, gitlab or bitbucket.
        username: Your username on the provider.
        token: The personal access token; prompted for when absent.
        host: Provider host, defaulting to the provider's public host.
        force: Replace an existing registration.
    """
    label = canonical_provider(provider)
    host = (host or default_host(label)).strip()
    session.get_project(ctx)
    existing = find_provider(label, host)
    if existing and not force:
        if output.JSON_MODE:
            output.print_json(
                {
                    "provider": label,
                    "host": host,
                    "username": existing.username,
                    "registered": False,
                    "reason": "already registered",
                }
            )
            return
        output.info(
            "%s token for %s is already registered (username %s). "
            "Pass --force to replace it.",
            label,
            host,
            existing.username,
        )
        return
    if not token:
        token = click.prompt(f"{label} personal access token", hide_input=True)
    register_provider(label, username, token, host)
    if output.JSON_MODE:
        output.print_json(
            {"provider": label, "host": host, "username": username, "registered": True}
        )
        return
    output.success("✓ Registered %s token for %s (username %s)", label, host, username)


@provider_group.command("delete")
@click.option("--provider", default="github", show_default=True)
@click.option("--host", help="Provider host (default: the provider's public host).")
@click.pass_context
def delete_cmd(ctx: click.Context, provider: str, host: str | None) -> None:
    """Remove a registered Git provider token.

    Args:
        ctx: Click context.
        provider: github, gitlab or bitbucket.
        host: Provider host, defaulting to the provider's public host.
    """
    label = canonical_provider(provider)
    host = (host or default_host(label)).strip()
    session.get_project(ctx)
    if not find_provider(label, host):
        raise click.ClickException(f"No {label} token registered for {host}.")
    git_provider_api.GitProviderApi()._delete_provider(label, host)
    if output.JSON_MODE:
        output.print_json({"provider": label, "host": host, "deleted": True})
        return
    output.success("✓ Removed the %s token for %s", label, host)

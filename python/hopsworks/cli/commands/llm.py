"""``hops llm``: the LLM provider keys of the current user.

Each key is the user's private Hopsworks secret for that provider, read by the
Hopsworks terminal and by Hopsable. A key is read from stdin only, never from an
argument, and is never printed.
"""

from __future__ import annotations

import json
import sys

import click
from hopsworks.cli import auth, output, session


@click.group("llm")
def llm_group() -> None:
    """Save, test and remove your LLM provider keys."""


def _login(ctx: click.Context):
    cfg = session.require_auth(ctx)
    auth.login(host=cfg.host or "", api_key_value=cfg.api_key, project=cfg.project)
    from hopsworks_common import client  # noqa: PLC0415 - keeps `hops --help` light

    return client._get_instance()


_JSON = {"content-type": "application/json"}


def _providers(rest) -> list[dict]:
    return rest._send_request("GET", ["users", "llm", "providers"]) or []


def _provider(rest, provider: str) -> dict:
    found = next((p for p in _providers(rest) if p.get("id") == provider.lower()), None)
    if found is None:
        raise click.ClickException(
            f"No LLM provider '{provider}'. See `hops llm list`."
        )
    return found


def _stdin_key() -> str:
    if sys.stdin.isatty():
        raise click.ClickException(
            "Pipe the key on stdin, for example: hops llm set-key anthropic < key.txt"
        )
    key = sys.stdin.read().strip()
    if not key:
        raise click.ClickException("No key on stdin.")
    return key


@llm_group.command("list")
@click.pass_context
def llm_list(ctx: click.Context) -> None:
    """Show each provider and whether a key is saved."""
    providers = _providers(_login(ctx))
    if output.JSON_MODE:
        output.print_json(
            [
                {k: p.get(k) for k in ("id", "label", "hasKey", "models")}
                for p in providers
            ]
        )
        return
    output.print_table(
        ["ID", "PROVIDER", "KEY", "MODELS"],
        [
            [
                p.get("id"),
                p.get("label"),
                "saved" if p.get("hasKey") else "-",
                ", ".join(p.get("models") or []),
            ]
            for p in providers
        ],
    )


@llm_group.command("set-key")
@click.argument("provider")
@click.pass_context
def llm_set_key(ctx: click.Context, provider: str) -> None:
    """Test the key on stdin with PROVIDER, then save it as your secret."""
    key = _stdin_key()
    rest = _login(ctx)
    p = _provider(rest, provider)
    verdict = rest._send_request(
        "POST",
        ["users", "llm", "providers", p["id"], "test"],
        headers=_JSON,
        data=json.dumps({"apiKey": key}),
    )
    if not verdict.get("ok"):
        raise click.ClickException(
            f"Not saved: {verdict.get('message', 'the provider refused the key')}"
        )
    rest._send_request(
        "PUT",
        ["users", "llm", "providers", p["id"], "key"],
        headers=_JSON,
        data=json.dumps({"apiKey": key}),
    )
    if not _provider(rest, p["id"]).get("hasKey"):
        raise click.ClickException(
            f"Hopsworks does not list a {p.get('label')} key after the save."
        )
    output.success("✓ %s key saved", p.get("label"))


@llm_group.command("test")
@click.argument("provider")
@click.pass_context
def llm_test(ctx: click.Context, provider: str) -> None:
    """Test the saved key of PROVIDER with the provider (no tokens spent where it lists models)."""
    rest = _login(ctx)
    p = _provider(rest, provider)
    verdict = rest._send_request(
        "POST", ["users", "llm", "providers", p["id"], "test"], headers=_JSON, data="{}"
    )
    if output.JSON_MODE:
        output.print_json(verdict)
    elif verdict.get("ok"):
        output.success("✓ %s", verdict.get("message"))
    else:
        raise click.ClickException(
            verdict.get("message", "The provider refused the key.")
        )


@llm_group.command("remove-key")
@click.argument("provider")
@click.option("--yes", is_flag=True, help="Remove without asking.")
@click.pass_context
def llm_remove_key(ctx: click.Context, provider: str, yes: bool) -> None:
    """Delete the saved key of PROVIDER (the terminal's too)."""
    rest = _login(ctx)
    p = _provider(rest, provider)
    if not p.get("hasKey"):
        output.info("No %s key saved.", p.get("label"))
        return
    if not yes and not click.confirm(
        f"Delete your {p.get('label')} key? The terminal uses it too.", default=False
    ):
        return
    rest._send_request("DELETE", ["users", "secrets", p["secretName"]])
    if _provider(rest, p["id"]).get("hasKey"):
        raise click.ClickException(f"Hopsworks still lists a {p.get('label')} key.")
    output.success("✓ %s key removed", p.get("label"))

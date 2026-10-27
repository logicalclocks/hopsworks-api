"""``hops logout`` — remove cached Hopsworks credentials from ``~/.hops.toml``."""

from __future__ import annotations

import click
from hopsworks.cli import config, output


@click.command("logout")
def logout_cmd() -> None:
    """Remove the cached credentials so the next ``hops setup`` starts clean.

    Clears the ``default`` profile (host, API key, project) from ``~/.hops.toml``.
    In internal mode (inside a Hopsworks pod) credentials come from the mounted
    JWT, not this file, so there is nothing to clear.
    """
    internal, _ = config._detect_internal()
    if internal:
        output.info(
            "Internal mode: credentials come from the pod, not %s; nothing to clear.",
            config.CONFIG_PATH,
        )
        return
    profile = config._read_toml(config.CONFIG_PATH).get("default", {})
    if not profile:
        output.info("Not logged in; nothing in %s to clear.", config.CONFIG_PATH)
        return
    config.clear()
    host = profile.get("host")
    if host:
        output.success("✓ Logged out from %s", host)
    else:
        output.success("✓ Logged out")

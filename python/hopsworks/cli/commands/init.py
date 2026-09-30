"""``hops init``: deprecated alias of ``hops skills install``.

The command only ever materialized skills, so it moved under the group that
lists and shows them. The alias stays for one release so instructions written
against the old name keep working, and is hidden from ``--help`` so nothing
new is written against it.
"""

from __future__ import annotations

import click
from hopsworks.cli import output
from hopsworks.cli.commands import skills as skills_cmd


@click.command("init", hidden=True, params=list(skills_cmd.skills_install.params))
def init_cmd(agents: tuple[str, ...], target_dir: str, force: bool) -> None:
    """Run ``hops skills install`` under its old name, with a warning.

    Args:
        agents: Agents to materialize for.
        target_dir: Repository root to write into.
        force: Replace skills that differ from the shipped copy.
    """
    output.warn("`hops init` is deprecated; use `hops skills install`.")
    skills_cmd.skills_install.callback(
        agents=agents, target_dir=target_dir, force=force
    )

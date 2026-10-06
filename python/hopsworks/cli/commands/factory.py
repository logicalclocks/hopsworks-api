"""``hops factory`` — the software factories that build in this project.

Two are built in: ``mlsystem`` builds ML systems (feature, training and
inference pipelines, and an app) and ``medallion`` builds silver and gold
layers. Each is a command group of its own, ``hops factory mlsystem ...`` and
``hops factory medallion ...``.
"""

from __future__ import annotations

import click
from hopsworks.cli import output
from hopsworks.cli.commands.medallion import medallion_group
from hopsworks.cli.commands.mlsystem import mlsystem_group


FACTORIES = (mlsystem_group, medallion_group)


@click.group("factory")
def factory_group() -> None:
    """The software factories: mlsystem builds ML systems, medallion builds silver and gold layers."""


@factory_group.command("list")
def factory_list() -> None:
    """List the factories, with the commands each one takes."""
    factories = [
        {
            "name": group.name,
            "description": group.get_short_help_str(limit=200),
            "commands": sorted(group.commands),
        }
        for group in FACTORIES
    ]
    if output.JSON_MODE:
        output.print_json(factories)
        return
    output.print_table(
        ["NAME", "DESCRIPTION", "COMMANDS"],
        [[f["name"], f["description"], ", ".join(f["commands"])] for f in factories],
    )


for _group in FACTORIES:
    factory_group.add_command(_group)

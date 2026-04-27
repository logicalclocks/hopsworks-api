"""``hops fs`` — list feature stores visible to the active project."""

from __future__ import annotations

import click
from hopsworks.cli import output, session


@click.group("fs")
def fs_group() -> None:
    """Inspect feature stores."""


@fs_group.command("list")
@click.pass_context
def fs_list(ctx: click.Context) -> None:
    """Show the project's own feature store.

    Hopsworks gives each project exactly one feature store, so this command
    is deliberately minimal — it exists mostly for parity with the Go CLI and
    to give scripts a single place to discover the feature store name.

    Args:
        ctx: Click context.
    """
    project = session.get_project(ctx)
    fs = project.get_feature_store()
    rows = [[getattr(fs, "id", "?"), getattr(fs, "name", "?")]]
    output.print_table(["ID", "NAME"], rows)

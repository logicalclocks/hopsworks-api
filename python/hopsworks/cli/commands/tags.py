"""``hops tags`` — tag-schema lifecycle (admin only).

Deprecating a schema refuses new attachments while existing values keep
working; restoring undoes it. Both require the platform-administrator role
(HOPS_ADMIN), matching schema creation and deletion.
"""

from __future__ import annotations

import click
from hopsworks.cli import output, session


@click.group("tags")
def tags_group() -> None:
    """Tag-schema lifecycle commands (admin only)."""


@tags_group.command("deprecate")
@click.argument("name")
@click.pass_context
def tags_deprecate(ctx: click.Context, name: str) -> None:
    """Deprecate the tag schema NAME, refusing new attachments of it.

    Args:
        ctx: Click context.
        name: Tag schema name.
    """
    session.get_project(ctx)
    from hopsworks_common.core.tag_schemas_api import TagSchemasApi  # noqa: PLC0415

    try:
        TagSchemasApi().deprecate(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not deprecate tag schema: {exc}") from exc
    output.success("✓ Deprecated tag schema '%s'", name)


@tags_group.command("restore")
@click.argument("name")
@click.pass_context
def tags_restore(ctx: click.Context, name: str) -> None:
    """Undo the deprecation of the tag schema NAME.

    Args:
        ctx: Click context.
        name: Tag schema name.
    """
    session.get_project(ctx)
    from hopsworks_common.core.tag_schemas_api import TagSchemasApi  # noqa: PLC0415

    try:
        TagSchemasApi().restore(name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not restore tag schema: {exc}") from exc
    output.success("✓ Restored tag schema '%s'", name)

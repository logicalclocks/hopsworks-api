"""``hops files`` — list, mkdir, upload/download, share, and remove in HopsFS.

All operations are backed by the SDK's ``DatasetApi`` (the REST resource
this wraps is still called ``/dataset/...`` for backwards compatibility,
but at the user-facing CLI level these are file-system commands; named
``files`` rather than ``fs`` so it doesn't collide with the standard
"feature store" abbreviation in Hopsworks docs). Output is a plain
``NAME/KIND/SIZE`` table for ``list`` and free-form success messages
for the rest.
"""

from __future__ import annotations

from typing import Any

import click
from hopsworks.cli import output, session


@click.group("files")
def files_group() -> None:
    """File-system commands on HopsFS (list, mkdir, upload, download, share, remove)."""


@files_group.command("list")
@click.argument("path", required=False, default="")
@click.pass_context
def dataset_list(ctx: click.Context, path: str) -> None:
    """List files under ``path`` in the project's HopsFS.

    An empty path lists the project root. The SDK returns string paths for the
    root and Inode objects for nested directories; we normalize both shapes to
    a simple NAME/KIND/SIZE table.

    Args:
        ctx: Click context.
        path: HopsFS path relative to the project root.
    """
    project = session.get_project(ctx)
    api = project.get_dataset_api()
    try:
        entries = api.list(path or "")
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not list '{path}': {exc}") from exc

    rows = [_entry_row(e) for e in entries or []]
    output.print_table(["NAME", "KIND", "SIZE"], rows)


def _entry_row(entry: Any) -> list[Any]:
    if isinstance(entry, str):
        return [entry, "-", "-"]
    name = getattr(entry, "name", None) or getattr(entry, "path", "?")
    kind = (
        "dir"
        if getattr(entry, "dir", False)
        or str(getattr(entry, "kind", "")).lower() == "dir"
        else "file"
    )
    size = getattr(entry, "size", "-")
    return [name, kind, size]


@files_group.command("mkdir")
@click.argument("path")
@click.pass_context
def dataset_mkdir(ctx: click.Context, path: str) -> None:
    """Create a directory in the project's HopsFS.

    Args:
        ctx: Click context.
        path: HopsFS path to create.
    """
    project = session.get_project(ctx)
    try:
        created = project.get_dataset_api().mkdir(path)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"mkdir failed: {exc}") from exc
    output.success("✓ Created %s", created or path)


@files_group.command("upload")
@click.argument("local_path", type=click.Path(exists=True))
@click.argument("remote_path")
@click.option("--overwrite", is_flag=True, help="Overwrite an existing remote file.")
@click.pass_context
def dataset_upload(
    ctx: click.Context, local_path: str, remote_path: str, overwrite: bool
) -> None:
    """Upload a local file to HopsFS.

    ``REMOTE_PATH`` is the destination path (``cp``/``scp``-style), not the
    parent directory. When its basename matches the local file's basename
    the parent is used as the SDK's ``upload_path``; otherwise the remote
    path is passed through unchanged and the SDK treats it as a directory
    (file lands under ``REMOTE_PATH/<local basename>``).

    Args:
        ctx: Click context.
        local_path: Local source file.
        remote_path: HopsFS destination path.
        overwrite: Pass-through to the SDK's upload call.
    """
    import os

    upload_path = remote_path
    if os.path.basename(remote_path) == os.path.basename(local_path):
        upload_path = os.path.dirname(remote_path) or "."

    project = session.get_project(ctx)
    try:
        uploaded = project.get_dataset_api().upload(
            local_path=local_path, upload_path=upload_path, overwrite=overwrite
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Upload failed: {exc}") from exc
    output.success("✓ Uploaded to %s", uploaded or remote_path)


@files_group.command("download")
@click.argument("remote_path")
@click.option(
    "--output",
    "local_path",
    type=click.Path(),
    help="Local destination; defaults to the current directory.",
)
@click.option("--overwrite", is_flag=True, help="Overwrite an existing local file.")
@click.pass_context
def dataset_download(
    ctx: click.Context, remote_path: str, local_path: str | None, overwrite: bool
) -> None:
    """Download a file from HopsFS to the local filesystem.

    Args:
        ctx: Click context.
        remote_path: HopsFS source path.
        local_path: Local destination.
        overwrite: Pass-through to the SDK.
    """
    project = session.get_project(ctx)
    try:
        path = project.get_dataset_api().download(
            path=remote_path, local_path=local_path, overwrite=overwrite
        )
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Download failed: {exc}") from exc
    output.success("✓ Downloaded to %s", path)


@files_group.command("share")
@click.argument("path")
@click.option(
    "--target",
    "target_project",
    required=True,
    help="Project to share the dataset with.",
)
@click.option(
    "--permission",
    type=click.Choice(["READ_ONLY", "EDITABLE", "EDITABLE_BY_OWNERS"]),
    default="READ_ONLY",
    show_default=True,
    help="Permission for the target project. Feature-store datasets must be READ_ONLY.",
)
@click.pass_context
def dataset_share(
    ctx: click.Context, path: str, target_project: str, permission: str
) -> None:
    """Share a dataset with another project.

    Requires the **Data Owner** role in the active (source) project; the
    backend rejects the share otherwise.

    Args:
        ctx: Click context.
        path: HopsFS path of the dataset to share (e.g. ``Resources/my_dir``).
        target_project: Name of the project to share with.
        permission: One of ``READ_ONLY``, ``EDITABLE``, ``EDITABLE_BY_OWNERS``.
    """
    project = session.get_project(ctx)
    try:
        project.get_dataset_api().share(
            path, target_project=target_project, permission=permission
        )
    except PermissionError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Share failed: {exc}") from exc
    output.success(
        "✓ Shared %s with project '%s' (%s)", path, target_project, permission
    )


@files_group.command("unshare")
@click.argument("path")
@click.option(
    "--target",
    "target_project",
    required=True,
    help="Project to revoke the share from.",
)
@click.pass_context
def dataset_unshare(ctx: click.Context, path: str, target_project: str) -> None:
    """Revoke a previously-granted dataset share with another project.

    Requires the **Data Owner** role in the active (source) project.

    Args:
        ctx: Click context.
        path: HopsFS path of the dataset.
        target_project: Name of the project to revoke the share from.
    """
    project = session.get_project(ctx)
    try:
        project.get_dataset_api().unshare(path, target_project=target_project)
    except PermissionError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Unshare failed: {exc}") from exc
    output.success("✓ Unshared %s from project '%s'", path, target_project)


@files_group.command("remove")
@click.argument("path")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def dataset_remove(ctx: click.Context, path: str, yes: bool) -> None:
    """Remove a file or directory from HopsFS.

    Args:
        ctx: Click context.
        path: HopsFS path.
        yes: Skip confirmation when True.
    """
    if not yes and not output.JSON_MODE:
        click.confirm(f"Remove '{path}' from HopsFS?", abort=True)
    project = session.get_project(ctx)
    try:
        project.get_dataset_api().remove(path)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Remove failed: {exc}") from exc
    output.success("✓ Removed %s", path)

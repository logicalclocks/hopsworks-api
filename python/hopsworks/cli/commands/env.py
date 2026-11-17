"""``hops env`` — manage the project's Python environments.

Wraps ``project.get_environment_api()``. Lists existing environments,
clones from a base environment (the SDK's ``create_environment`` call —
the API uses "create from base" rather than a dedicated "clone"), and
installs a requirements.txt into a target environment. Every command
that triggers a backend installation step blocks until it finishes
(``await_creation=True`` / ``await_installation=True``) and prints a
heads-up warning, since installs can take several minutes.
"""

from __future__ import annotations

import contextlib
import os
from typing import Any

import click
from hopsworks.cli import output, session


@click.group("env")
def env_group() -> None:
    """Python environment commands (list, clone, install)."""


def _api(ctx: click.Context) -> Any:
    return session.get_project(ctx).get_environment_api()


def _row(env: Any) -> dict[str, Any]:
    return {
        "name": getattr(env, "name", None),
        "python_version": getattr(env, "python_version", None),
        "description": output.first_line(getattr(env, "description", None) or ""),
    }


@env_group.command("list")
@click.pass_context
def env_list(ctx: click.Context) -> None:
    """List Python environments in the current project.

    Args:
        ctx: Click context.
    """
    envs = _api(ctx).get_environments() or []
    rows = [_row(e) for e in envs]
    if output.JSON_MODE:
        output.print_json(rows)
        return
    output.print_table(
        ["NAME", "PYTHON_VERSION", "DESCRIPTION"],
        [
            [r["name"] or "", r["python_version"] or "", r["description"] or ""]
            for r in rows
        ],
    )


@env_group.command("clone")
@click.argument("new_name")
@click.option(
    "--from",
    "source",
    default="python-feature-pipeline",
    show_default=True,
    help="Existing environment name to clone from.",
)
@click.option(
    "--description",
    "description",
    default=None,
    help="Description for the new environment.",
)
@click.pass_context
def env_clone(
    ctx: click.Context,
    new_name: str,
    source: str,
    description: str | None,
) -> None:
    """Clone SOURCE into a new environment NEW_NAME.

    Blocks until the backend finishes provisioning. This usually takes
    several minutes — the new environment is built from the base image
    and any post-install steps run before the call returns.

    Args:
        ctx: Click context.
        new_name: Name for the new environment.
        source: Existing environment to clone from.
        description: Optional description for the new environment.
    """
    output.warn(
        "Cloning '%s' to '%s' — this can take several minutes. Waiting for "
        "the backend to finish provisioning before returning.",
        source,
        new_name,
    )
    try:
        env = _api(ctx).create_environment(
            new_name,
            description=description,
            base_environment_name=source,
            await_creation=True,
        )
    except Exception as exc:  # noqa: BLE001 - SDK raises a mix of types
        raise click.ClickException(f"Clone failed: {exc}") from exc
    payload = _row(env)
    if output.JSON_MODE:
        output.print_json(payload)
    else:
        output.success(f"Created environment '{payload['name']}'")
        output.print_table(
            ["FIELD", "VALUE"],
            [[k.upper(), str(v) if v is not None else ""] for k, v in payload.items()],
        )


@env_group.command("install")
@click.argument("env_name")
@click.option(
    "-f",
    "--file",
    "requirements_file",
    required=True,
    help="requirements.txt: a local file (uploaded to HopsFS) or an existing "
    "HopsFS path.",
)
@click.option(
    "--upload-dir",
    default=None,
    help="HopsFS dir to upload a local requirements file to "
    "(default: Resources/environments/<env_name>).",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=True,
    show_default=True,
    help="Overwrite the uploaded requirements file if it exists.",
)
@click.pass_context
def env_install(
    ctx: click.Context,
    env_name: str,
    requirements_file: str,
    upload_dir: str | None,
    overwrite: bool,
) -> None:
    """Install a requirements.txt into ENV_NAME.

    ``--file`` is either a local file — uploaded to HopsFS first, since the
    backend can only read paths inside the project — or a path that already
    exists in the project (e.g. ``Users/<username>/requirements.txt``).

    Blocks until the install completes. This usually takes several
    minutes — conda/pip resolves the dependencies and the resulting
    libraries are committed to the environment image.

    Args:
        ctx: Click context.
        env_name: Environment to install into.
        requirements_file: Local file to upload, or an existing HopsFS path.
        upload_dir: HopsFS directory for a local requirements upload.
        overwrite: Overwrite an existing uploaded requirements file.
    """
    project = session.get_project(ctx)
    api = project.get_environment_api()
    try:
        env = api.get_environment(env_name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Lookup failed: {exc}") from exc
    if env is None:
        raise click.ClickException(
            f"No environment named '{env_name}'. Run `hops env list` to see what exists."
        )

    # Resolve the requirements path — upload when a local file was given, so the
    # backend (which resolves against /Projects/<project>/) can read it. Same
    # convention as `hops job deploy` resolving its script argument.
    remote_path = requirements_file
    if os.path.isfile(requirements_file):
        dataset = project.get_dataset_api()
        dest_dir = upload_dir or f"Resources/environments/{env_name}"
        with contextlib.suppress(Exception):  # directory may already exist
            dataset.mkdir(dest_dir)
        try:
            uploaded = dataset.upload(
                local_path=requirements_file, upload_path=dest_dir, overwrite=overwrite
            )
        except Exception as exc:  # noqa: BLE001
            raise click.ClickException(f"Could not upload requirements: {exc}") from exc
        remote_path = uploaded or f"{dest_dir}/{os.path.basename(requirements_file)}"
        output.success(
            "✓ Uploaded %s -> %s", os.path.basename(requirements_file), remote_path
        )

    output.warn(
        "Installing '%s' into '%s' — this can take several minutes. Waiting for "
        "the backend to finish before returning.",
        remote_path,
        env_name,
    )
    try:
        env.install_requirements(remote_path, await_installation=True)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Install failed: {exc}") from exc
    if output.JSON_MODE:
        output.print_json({"environment": env_name, "installed": remote_path})
    else:
        output.success(f"Installed '{remote_path}' into environment '{env_name}'")

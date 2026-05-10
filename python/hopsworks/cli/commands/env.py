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
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=True,
    help="Path to a requirements.txt file to install.",
)
@click.pass_context
def env_install(
    ctx: click.Context,
    env_name: str,
    requirements_file: str,
) -> None:
    """Install a requirements.txt into ENV_NAME.

    Blocks until the install completes. This usually takes several
    minutes — the file is uploaded, conda/pip resolves the dependencies,
    and the resulting libraries are committed to the environment image.

    Args:
        ctx: Click context.
        env_name: Environment to install into.
        requirements_file: Path to a requirements.txt file.
    """
    api = _api(ctx)
    try:
        env = api.get_environment(env_name)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Lookup failed: {exc}") from exc
    if env is None:
        raise click.ClickException(
            f"No environment named '{env_name}'. Run `hops env list` to see what exists."
        )
    output.warn(
        "Installing '%s' into '%s' — this can take several minutes. Waiting for "
        "the backend to finish before returning.",
        requirements_file,
        env_name,
    )
    try:
        env.install_requirements(requirements_file, await_installation=True)
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Install failed: {exc}") from exc
    if output.JSON_MODE:
        output.print_json({"environment": env_name, "installed": requirements_file})
    else:
        output.success(f"Installed '{requirements_file}' into environment '{env_name}'")

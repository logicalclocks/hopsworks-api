"""Top-level Click application for the ``hops`` CLI.

This module wires global flags, loads config once per invocation, and
registers every subcommand group. Commands are imported lazily — the
``setup``/``login`` groups only touch the stdlib and ``requests``, so running
``hops --version`` or ``hops setup`` does not pay the full SDK import cost.
"""

from __future__ import annotations

import click
from hopsworks.cli import config, output
from hopsworks.cli.commands.app import app_group
from hopsworks.cli.commands.chart import chart_group
from hopsworks.cli.commands.connector import connector_group
from hopsworks.cli.commands.context import context_cmd
from hopsworks.cli.commands.dashboard import dashboard_group
from hopsworks.cli.commands.dataset import dataset_group
from hopsworks.cli.commands.deployment import deployment_group
from hopsworks.cli.commands.fg import fg_group
from hopsworks.cli.commands.fs import fs_group
from hopsworks.cli.commands.fv import fv_group
from hopsworks.cli.commands.init import init_cmd
from hopsworks.cli.commands.job import job_group
from hopsworks.cli.commands.login import login_cmd
from hopsworks.cli.commands.model import model_group
from hopsworks.cli.commands.project import project_group
from hopsworks.cli.commands.setup import setup_cmd
from hopsworks.cli.commands.superset import superset_group
from hopsworks.cli.commands.td import td_group
from hopsworks.cli.commands.transformation import transformation_group
from hopsworks.cli.commands.update import update_cmd


try:
    from hopsworks_common.version import __version__ as _VERSION
except Exception:  # noqa: BLE001 - version file may be absent during bootstrap
    _VERSION = "0.0.0+unknown"


@click.group(
    name="hops",
    help=(
        "Hopsworks CLI — for humans and LLMs.\n\n"
        "Run `hops setup` to authenticate on a new machine."
    ),
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.version_option(_VERSION, "-v", "--version", prog_name="hops")
@click.option("--host", "host_flag", help="Hopsworks host URL (overrides config).")
@click.option("--api-key", "api_key_flag", help="API key (overrides config).")
@click.option("--project", "project_flag", help="Project name (overrides config).")
@click.option("--json", "json_flag", is_flag=True, help="Output as JSON (for LLMs).")
@click.pass_context
def cli(
    ctx: click.Context,
    host_flag: str | None,
    api_key_flag: str | None,
    project_flag: str | None,
    json_flag: bool,
) -> None:
    """Root ``hops`` command; child commands pull the resolved config via ``ctx.obj``.

    Args:
        ctx: Click context; we stash the resolved ``HopsConfig`` on ``ctx.obj``.
        host_flag: Value of ``--host`` if provided.
        api_key_flag: Value of ``--api-key`` if provided.
        project_flag: Value of ``--project`` if provided.
        json_flag: When True, every output helper switches to JSON mode.
    """
    output.set_json_mode(json_flag)
    ctx.ensure_object(dict)
    ctx.obj["config"] = config.load(
        flag_host=host_flag,
        flag_api_key=api_key_flag,
        flag_project=project_flag,
    )


cli.add_command(setup_cmd)
cli.add_command(login_cmd)
cli.add_command(project_group)
cli.add_command(fs_group)
cli.add_command(fg_group)
cli.add_command(fv_group)
cli.add_command(connector_group)
cli.add_command(td_group)
cli.add_command(model_group)
cli.add_command(deployment_group)
cli.add_command(job_group)
cli.add_command(app_group)
cli.add_command(dataset_group)
cli.add_command(transformation_group)
cli.add_command(chart_group)
cli.add_command(dashboard_group)
cli.add_command(superset_group)
cli.add_command(context_cmd)
cli.add_command(init_cmd)
cli.add_command(update_cmd)


def main() -> None:
    """Console-script entry point referenced by ``pyproject.toml``."""
    cli.main(prog_name="hops")


if __name__ == "__main__":  # pragma: no cover
    main()

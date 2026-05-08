"""Top-level Click application for the ``hops`` CLI.

This module wires global flags, loads config once per invocation, and
registers every subcommand group. Subcommand modules are imported on first
use via :class:`LazyGroup` so ``hops --help`` (and any direct subcommand
invocation) only pays the import cost of the modules it actually touches.
Loading every command's SDK transitively at startup made cold-path commands
like ``--help`` measurably slow (~2 s+); deferring keeps them snappy.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

import click
from hopsworks.cli import config, output


if TYPE_CHECKING:
    from collections.abc import Iterable


# Map ``<command-name> -> "<module>:<attribute>"``. The attribute is the
# already-decorated ``click.Command`` / ``click.Group`` exported by that
# module. The module itself is only imported when the user actually invokes
# the command (or asks for its --help).
_LAZY_SUBCOMMANDS: dict[str, str] = {
    "setup": "hopsworks.cli.commands.setup:setup_cmd",
    "login": "hopsworks.cli.commands.login:login_cmd",
    "project": "hopsworks.cli.commands.project:project_group",
    "files": "hopsworks.cli.commands.files:files_group",
    "fg": "hopsworks.cli.commands.fg:fg_group",
    "fv": "hopsworks.cli.commands.fv:fv_group",
    "datasource": "hopsworks.cli.commands.datasource:datasource_group",
    "td": "hopsworks.cli.commands.td:td_group",
    "model": "hopsworks.cli.commands.model:model_group",
    "deployment": "hopsworks.cli.commands.deployment:deployment_group",
    "env": "hopsworks.cli.commands.env:env_group",
    "job": "hopsworks.cli.commands.job:job_group",
    "app": "hopsworks.cli.commands.app:app_group",
    "transformation": "hopsworks.cli.commands.transformation:transformation_group",
    "superset": "hopsworks.cli.commands.superset:superset_group",
    "search": "hopsworks.cli.commands.search:search_group",
    "trino": "hopsworks.cli.commands.trino:trino_group",
    "context": "hopsworks.cli.commands.context:context_cmd",
    "init": "hopsworks.cli.commands.init:init_cmd",
    "update": "hopsworks.cli.commands.update:update_cmd",
}


try:
    from hopsworks_common.version import __version__ as _VERSION
except Exception:  # noqa: BLE001 - version file may be absent during bootstrap
    _VERSION = "0.0.0+unknown"


def _json_eager_callback(
    _ctx: click.Context, _param: click.Parameter, value: bool
) -> bool:
    """Toggle JSON mode whenever ``--json`` is parsed, at any command level."""
    if value:
        output.set_json_mode(True)
    return value


def _inject_json_option(cmd: click.Command) -> None:
    """Recursively add ``--json`` to *cmd* and every nested subcommand.

    Click only honours the ``--json`` flag at the level it was declared on. The
    root ``cli`` group already has it (so ``hops --json fg list`` works), but
    callers — especially LLM-driven tools that compose commands left-to-right
    — naturally write ``hops fg list --json``. Walking the loaded command's
    subtree on first dispatch and appending an eager ``--json`` to every node
    that doesn't already have one makes both forms work without touching
    every command file. ``expose_value=False`` keeps the existing leaf
    callbacks (which don't take a ``json_flag`` kwarg) source-compatible; the
    eager callback flips :func:`output.set_json_mode` before the leaf runs.
    """
    already_has_json = any(
        "--json" in (getattr(p, "opts", None) or []) for p in cmd.params
    )
    if not already_has_json:
        cmd.params.append(
            click.Option(
                ["--json"],
                is_flag=True,
                expose_value=False,
                is_eager=True,
                callback=_json_eager_callback,
                help="Output as JSON (machine-readable; for LLMs and scripts).",
            )
        )
    # ``LazyGroup`` does its own json injection on demand — recursing into it
    # would force-load every subcommand and defeat the lazy import. Recurse
    # only for already-loaded ordinary groups.
    if isinstance(cmd, click.Group) and not isinstance(cmd, LazyGroup):
        for sub in cmd.commands.values():
            _inject_json_option(sub)


class LazyGroup(click.Group):
    """A Click ``Group`` that imports its subcommand modules on first use.

    Each entry in ``lazy_subcommands`` maps a name to ``"module.path:attr"``.
    The module is imported only when the user asks for that command (or its
    --help), keeping cold paths (``hops --help``, ``hops setup``) free of
    SDK / pandas / pyarrow import cost.

    Loaded commands have :func:`_inject_json_option` applied to them so the
    ``--json`` flag works at every nesting level — exactly the behaviour the
    eager pre-load used to provide.
    """

    def __init__(
        self,
        *args,
        lazy_subcommands: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.lazy_subcommands: dict[str, str] = dict(lazy_subcommands or {})

    def list_commands(self, ctx: click.Context) -> Iterable[str]:
        return sorted({*self.commands.keys(), *self.lazy_subcommands.keys()})

    def get_command(self, ctx: click.Context, name: str) -> click.Command | None:
        if name in self.commands:
            return self.commands[name]
        target = self.lazy_subcommands.get(name)
        if target is None:
            return None
        module_path, _, attr = target.partition(":")
        module = importlib.import_module(module_path)
        cmd = getattr(module, attr)
        _inject_json_option(cmd)
        # Cache the loaded command so subsequent lookups (and Click's own
        # bookkeeping) reuse the same instance.
        self.commands[name] = cmd
        return cmd


@click.group(
    name="hops",
    cls=LazyGroup,
    lazy_subcommands=_LAZY_SUBCOMMANDS,
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


def main() -> None:
    """Console-script entry point referenced by ``pyproject.toml``."""
    cli.main(prog_name="hops")


if __name__ == "__main__":  # pragma: no cover
    main()

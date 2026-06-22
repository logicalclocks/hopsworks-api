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
import sys
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
    "agent": "hopsworks.cli.commands.agent:agent_group",
    "transformation": "hopsworks.cli.commands.transformation:transformation_group",
    "superset": "hopsworks.cli.commands.superset:superset_group",
    "search": "hopsworks.cli.commands.search:search_group",
    "trino": "hopsworks.cli.commands.trino:trino_group",
    "sql": "hopsworks.cli.commands.trino:trino_query",  # top-level alias of `trino query`
    "context": "hopsworks.cli.commands.context:context_cmd",
    "skills": "hopsworks.cli.commands.skills:skills_group",
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


def _verify_eager_callback(
    ctx: click.Context, _param: click.Parameter, value: bool | None
) -> bool | None:
    """Apply a ``--verify/--no-verify`` flag parsed at any command level.

    The root command resolves the config before subcommands parse, so a flag
    written after the subcommand (``hops sql ... --no-verify``) cannot reach
    :func:`config.load`. Instead it is applied here by overriding the
    already-resolved config that :mod:`hopsworks.cli.session` reads at login.
    ``None`` means neither flag was passed, so the resolved value stands.
    """
    if value is not None:
        cfg = ctx.ensure_object(dict).get("config")
        if cfg is not None:
            cfg.hostname_verification = value
            cfg.hostname_verification_explicit = True
    return value


def _inject_global_options(cmd: click.Command) -> None:
    """Recursively add the global ``--json`` and ``--verify/--no-verify`` flags.

    Click only honours a flag at the level it was declared on. The root ``cli``
    group already has both (so ``hops --json fg list`` works), but callers —
    especially LLM-driven tools that compose commands left-to-right — naturally
    write ``hops fg list --json`` or ``hops sql ... --no-verify``. Walking the
    loaded command's subtree on first dispatch and appending the flags to every
    node that does not already declare them makes both forms work without
    touching every command file. ``expose_value=False`` keeps the existing leaf
    callbacks (which take neither kwarg) source-compatible; the eager callbacks
    run before the leaf, flipping JSON mode and the resolved hostname-verification
    setting respectively.
    """
    if not any("--json" in (getattr(p, "opts", None) or []) for p in cmd.params):
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
    if not any("--verify" in (getattr(p, "opts", None) or []) for p in cmd.params):
        cmd.params.append(
            click.Option(
                ["--verify/--no-verify"],
                default=None,
                expose_value=False,
                is_eager=True,
                callback=_verify_eager_callback,
                help=(
                    "Verify the Hopsworks TLS certificate; --no-verify skips "
                    "verification for self-signed / IP-SAN-mismatched certs."
                ),
            )
        )
    # ``LazyGroup`` does its own injection on demand — recursing into it would
    # force-load every subcommand and defeat the lazy import. Recurse only for
    # already-loaded ordinary groups.
    if isinstance(cmd, click.Group) and not isinstance(cmd, LazyGroup):
        for sub in cmd.commands.values():
            _inject_global_options(sub)


class LazyGroup(click.Group):
    """A Click ``Group`` that imports its subcommand modules on first use.

    Each entry in ``lazy_subcommands`` maps a name to ``"module.path:attr"``.
    The module is imported only when the user asks for that command (or its
    --help), keeping cold paths (``hops --help``, ``hops setup``) free of
    SDK / pandas / pyarrow import cost.

    Loaded commands have :func:`_inject_global_options` applied to them so the
    ``--json`` and ``--verify/--no-verify`` flags work at every nesting level —
    exactly the behaviour the eager pre-load used to provide.
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
        _inject_global_options(cmd)
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
@click.option(
    "--api-key",
    "api_key_flag",
    help="API key (overrides config). Avoid in shared shells — a key in argv "
    "is visible in the process list and shell history. Prefer the "
    "HOPSWORKS_API_KEY env var or --api-key-stdin.",
)
@click.option(
    "--api-key-stdin",
    "api_key_stdin",
    is_flag=True,
    help="Read the API key from the first line of stdin instead of argv "
    "(keeps the secret out of the process list and shell history).",
)
@click.option("--project", "project_flag", help="Project name (overrides config).")
@click.option(
    "--verify/--no-verify",
    "verify_flag",
    default=None,
    help=(
        "Verify the Hopsworks TLS certificate (hostname verification). Use "
        "--no-verify to skip verification when a cluster serves a self-signed "
        "or IP-SAN-mismatched certificate. Maps to the SDK's "
        "hopsworks.login(hostname_verification=...); overridden by "
        "HOPSWORKS_HOSTNAME_VERIFICATION when that env var is set."
    ),
)
@click.option("--json", "json_flag", is_flag=True, help="Output as JSON (for LLMs).")
@click.pass_context
def cli(
    ctx: click.Context,
    host_flag: str | None,
    api_key_flag: str | None,
    api_key_stdin: bool,
    project_flag: str | None,
    verify_flag: bool | None,
    json_flag: bool,
) -> None:
    """Root ``hops`` command; child commands pull the resolved config via ``ctx.obj``.

    Args:
        ctx: Click context; we stash the resolved ``HopsConfig`` on ``ctx.obj``.
        host_flag: Value of ``--host`` if provided.
        api_key_flag: Value of ``--api-key`` if provided.
        api_key_stdin: When True, read the API key from stdin.
        project_flag: Value of ``--project`` if provided.
        verify_flag: True for ``--verify``, False for ``--no-verify``, None if neither was passed.
        json_flag: When True, every output helper switches to JSON mode.
    """
    output.set_json_mode(json_flag)
    if api_key_stdin:
        if api_key_flag:
            raise click.UsageError("Pass --api-key or --api-key-stdin, not both.")
        api_key_flag = sys.stdin.readline().strip() or None
    ctx.ensure_object(dict)
    ctx.obj["config"] = config.load(
        flag_host=host_flag,
        flag_api_key=api_key_flag,
        flag_project=project_flag,
        flag_hostname_verification=verify_flag,
    )


def main() -> None:
    """Console-script entry point referenced by ``pyproject.toml``."""
    cli.main(prog_name="hops")


if __name__ == "__main__":  # pragma: no cover
    main()

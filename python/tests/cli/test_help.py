"""Help text contract for every `hops` command.

A group listing shows one complete line per command.
Click renders help 78 columns wide by default.
`<command> -h` shows the summary, the description and the options, never the developer docstring sections.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli


def _walk(cmd, path, ctx):
    yield path, cmd, ctx
    if isinstance(cmd, click.Group):
        for name in cmd.list_commands(ctx):
            sub = cmd.get_command(ctx, name)
            if sub is not None and not sub.hidden:
                yield from _walk(
                    sub, [*path, name], click.Context(sub, parent=ctx, info_name=name)
                )


_COMMANDS = list(
    _walk(cli, ["hops"], click.Context(cli, info_name="hops", **cli.context_settings))
)


def _render(cmd, ctx):
    formatter = click.HelpFormatter(width=78)
    cmd.format_help(ctx, formatter)
    return formatter.getvalue()


@pytest.mark.parametrize(
    ("cmd", "ctx"),
    [(c, x) for _, c, x in _COMMANDS],
    ids=[" ".join(p) for p, _, _ in _COMMANDS],
)
def test_help_shows_no_developer_docstring(cmd, ctx):
    text = _render(cmd, ctx)
    for leak in ("Args:", "Returns:", "Raises:", "``", "\\b"):
        assert leak not in text
    assert (cmd.help or "").strip(), "every command needs a summary line"


@pytest.mark.parametrize(
    ("cmd", "ctx"),
    [(c, x) for _, c, x in _COMMANDS if isinstance(c, click.Group)],
    ids=[" ".join(p) for p, c, _ in _COMMANDS if isinstance(c, click.Group)],
)
def test_group_listing_is_never_truncated(cmd, ctx):
    commands = [
        (name, sub)
        for name in cmd.list_commands(ctx)
        if (sub := cmd.get_command(ctx, name)) is not None and not sub.hidden
    ]
    limit = 78 - 6 - max(len(name) for name, _ in commands)
    for name, sub in commands:
        assert not sub.get_short_help_str(limit).endswith("..."), name


def test_short_and_long_help_flags_match():
    for path, _, _ in _COMMANDS:
        short = CliRunner().invoke(cli, [*path[1:], "-h"])
        assert short.exit_code == 0, (path, short.output)
        assert short.output.startswith("Usage: hops"), path
        assert short.output == CliRunner().invoke(cli, [*path[1:], "--help"]).output

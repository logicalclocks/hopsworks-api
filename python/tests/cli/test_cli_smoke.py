#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Smoke test for the whole ``hops`` CLI command tree.

The CLI is excluded from the ``pep8_public`` check and consumes many internal
SDK symbols, so a rename in the SDK can leave a stale call inside a command
module that the import check (only import-time) and the targeted command tests
do not catch. This test walks every (sub)command — which forces the lazy import
of each command module — and runs ``--help`` on each, so a broken import,
command definition, or option declaration anywhere in the CLI surface fails CI.

It does not hit a backend: ``--help`` renders before any command callback runs.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli


def _walk(group: click.Group, ctx: click.Context, path: list[str]):
    """Yield (path, command) for every command reachable from ``group``.

    ``get_command`` forces the lazy import of each command module.
    """
    discovered = []
    for name in group.list_commands(ctx):
        cmd = group.get_command(ctx, name)
        assert cmd is not None, f"`hops {' '.join([*path, name])}` failed to load"
        discovered.append(([*path, name], cmd))
        if isinstance(cmd, click.Group):
            discovered.extend(_walk(cmd, ctx, [*path, name]))
    return discovered


def test_cli_command_tree_loads():
    """Every command module imports and every command/group is constructible."""
    ctx = click.Context(cli, info_name="hops")
    commands = _walk(cli, ctx, [])
    assert commands, "no CLI commands were discovered"


@pytest.mark.parametrize(
    "argv",
    [["--help"]]
    + [
        [*path, "--help"]
        for path, _cmd in _walk(cli, click.Context(cli, info_name="hops"), [])
    ],
    ids=lambda argv: "hops " + " ".join(argv),
)
def test_cli_help_renders(argv):
    """``--help`` renders cleanly for the root and every (sub)command."""
    result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 0, (
        f"`hops {' '.join(argv)}` exited {result.exit_code}:\n{result.output}"
    )

"""Every old command name still reaches its new command, with a warning."""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli.deprecations import (
    POSITIONAL_TO_OPTION,
    RENAMED_COMMANDS,
    RENAMED_FLAGS,
    WAIT_SECONDS,
    rewrite,
)
from hopsworks.cli.main import cli


@pytest.mark.parametrize(
    ("old", "new"),
    RENAMED_COMMANDS.items(),
    ids=[" ".join(o) for o in RENAMED_COMMANDS],
)
def test_old_name_reaches_the_new_command(old, new):
    result = CliRunner().invoke(cli, [*old, "-h"])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith(f"Usage: hops {' '.join(new)} ")
    assert f"use `hops {' '.join(new)}`" in result.stderr


def _no_subcommands(path):
    return set()


def test_old_job_schedule_call_runs_set():
    args = rewrite(["job", "schedule", "etl", "0 0 * * * ?"], _no_subcommands)
    assert args == ["job", "schedule", "set", "etl", "0 0 * * * ?"]


def test_old_stats_calls_run_info_or_compute():
    assert rewrite(["fg", "stats", "txn"], _no_subcommands)[2] == "info"
    assert rewrite(["--json", "fg", "stats", "txn", "--compute"], _no_subcommands) == [
        "--json",
        "fg",
        "stats",
        "compute",
        "txn",
    ]


def test_new_names_are_left_alone():
    args = ["job", "schedule", "info", "etl"]
    assert rewrite(args, lambda path: {"set", "info", "delete"}) is args
    assert rewrite(["fg", "tag", "list", "txn"], _no_subcommands) == [
        "fg",
        "tag",
        "list",
        "txn",
    ]


def _command(path):
    ctx = click.Context(cli, info_name="hops", **cli.context_settings)
    cmd = cli
    for name in path:
        cmd = cmd.get_command(ctx, name)
        ctx = click.Context(cmd, parent=ctx, info_name=name)
    return cmd


def _options(path):
    params = _command(path).params
    return {o for p in params for o in [*p.opts, *getattr(p, "secondary_opts", [])]}


@pytest.mark.parametrize(("path", "renames"), RENAMED_FLAGS.items())
def test_every_renamed_flag_exists_on_its_command(path, renames):
    options = _options(path)
    for old, new in renames.items():
        assert new in options
        assert old not in options


@pytest.mark.parametrize(("path", "move"), POSITIONAL_TO_OPTION.items())
def test_every_moved_positional_is_an_option_now(path, move):
    assert move[1] in _options(path)


@pytest.mark.parametrize("path", sorted(WAIT_SECONDS))
def test_wait_commands_take_wait_and_timeout(path):
    assert {"--wait", "--no-wait", "--timeout"} <= _options(path)


def test_old_flags_are_rewritten():
    assert rewrite(["fg", "preview", "txn", "--n", "3"], _no_subcommands) == [
        "fg",
        "preview",
        "txn",
        "--limit",
        "3",
    ]
    assert rewrite(["fg", "insert", "txn", "--online"], _no_subcommands)[-1] == (
        "--online-only"
    )
    assert rewrite(["agent", "create", "a.py", "--environment=e"], _no_subcommands)[
        -1
    ] == ("--env=e")


def test_old_positional_versions_become_options():
    assert rewrite(["td", "compute", "fv", "2", "--split", "x"], _no_subcommands) == [
        "td",
        "compute",
        "fv",
        "--split",
        "x",
        "--fv-version",
        "2",
    ]
    # The old keyword commands took FEATURE_VIEW TD_VERSION KEYWORD.
    assert rewrite(["td", "add-keyword", "fv", "3", "gold"], _no_subcommands) == [
        "td",
        "keyword",
        "add",
        "fv",
        "gold",
        "--td-version",
        "3",
    ]
    # A numeric keyword in the new form is left alone.
    new = ["td", "keyword", "add", "fv", "2024", "--td-version", "3"]
    assert rewrite(new, _no_subcommands) == new


def test_old_wait_seconds_become_timeout_or_no_wait():
    assert rewrite(["deployment", "start", "d", "--wait", "30"], _no_subcommands) == [
        "deployment",
        "start",
        "d",
        "--timeout",
        "30",
    ]
    assert rewrite(["agent", "stop", "a", "--wait=0"], _no_subcommands)[-1] == (
        "--no-wait"
    )
    new = ["agent", "start", "a", "--wait", "--timeout", "5"]
    assert rewrite(new, _no_subcommands) == new

"""Every old command name still reaches its new command, with a warning."""

from __future__ import annotations

import pytest
from click.testing import CliRunner
from hopsworks.cli.deprecations import RENAMED_COMMANDS, rewrite
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

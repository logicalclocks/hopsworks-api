"""Old command and flag names, kept working for one release after the ontology change.

The root group rewrites the argument list before Click parses it, so old names never show in help and every command exists once.
Each rewrite prints the new spelling on stderr.
See ONTOLOGY.md for the rules the new names follow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import click


if TYPE_CHECKING:
    from collections.abc import Callable


# Old token path -> new token path. Longest paths are matched first.
RENAMED_COMMANDS: dict[tuple[str, ...], tuple[str, ...]] = {
    **{
        (entity, old): (entity, *new)
        for entity in ("fg", "fv", "job")
        for old, new in {
            "tags": ("tag", "list"),
            "add-tag": ("tag", "add"),
            "remove-tag": ("tag", "remove"),
        }.items()
    },
    **{
        (entity, old): (entity, *new)
        for entity in ("fg", "fv", "td")
        for old, new in {
            "keywords": ("keyword", "list"),
            "add-keyword": ("keyword", "add"),
            "remove-keyword": ("keyword", "remove"),
        }.items()
    },
    ("fg", "features"): ("fg", "feature", "list"),
    ("fg", "append-features"): ("fg", "feature", "add"),
    ("fv", "get"): ("fv", "lookup"),
    ("job", "schedule-info"): ("job", "schedule", "info"),
    ("job", "unschedule"): ("job", "schedule", "delete"),
    # Not a former name: without it, the old `schedule` default turns `list` into a job name.
    ("job", "schedule", "list"): ("job", "schedule", "info"),
    ("job", "describe"): ("job", "update"),
    ("alert", "get"): ("alert", "info"),
    ("alert", "receiver", "get"): ("alert", "receiver", "info"),
    ("alert", "fg"): ("fg", "alert"),
    ("alert", "fv"): ("fv", "alert"),
    ("alert", "job", "get"): ("job", "alert", "info"),
    ("alert", "job"): ("job", "alert"),
    ("deployment", "status"): ("deployment", "info"),
    ("files", "remove"): ("files", "delete"),
    ("factory", "get"): ("factory", "info"),
    ("factory", "system", "lease", "show"): ("factory", "system", "lease", "info"),
    ("datasource", "credentials", "show"): ("datasource", "credentials", "info"),
    ("skills", "show"): ("skills", "info"),
    ("trino", "describe"): ("trino", "columns"),
    ("trino", "sql"): ("trino", "query"),
    ("session", "new"): ("session", "start"),
}

# Former commands that are now groups: the old call runs this subcommand.
GROUP_DEFAULTS: dict[tuple[str, ...], str] = {
    ("job", "schedule"): "set",
    ("fg", "stats"): "info",
}

# Command path -> {old flag: new flag}.
RENAMED_FLAGS: dict[tuple[str, ...], dict[str, str]] = {
    ("fg", "preview"): {"--n": "--limit"},
    ("fv", "read"): {"--n": "--limit"},
    ("td", "read"): {"--n": "--limit"},
    ("fg", "insert"): {"--online": "--online-only"},
    ("agent", "create"): {"--environment": "--env"},
    ("app", "create"): {"--environment": "--env"},
    ("factory", "system", "status"): {"--out": "--output"},
    ("fv", "deploy"): {"--training-dataset-version": "--td-version"},
    ("td", "list"): {"--version": "--fv-version"},
    ("td", "lineage"): {"--version": "--fv-version"},
}

# Command path -> (positional index, option it became, positionals the old form had).
# The old value is moved only when it is an integer and the old form is complete.
POSITIONAL_TO_OPTION: dict[tuple[str, ...], tuple[int, str, int]] = {
    **{
        ("td", verb): (1, "--fv-version", 2)
        for verb in ("list", "compute", "read", "delete", "lineage")
    },
    ("td", "keyword", "list"): (1, "--td-version", 2),
    ("td", "keyword", "add"): (1, "--td-version", 3),
    ("td", "keyword", "remove"): (1, "--td-version", 3),
}

# Commands whose --wait took seconds; it is now --wait/--no-wait with --timeout.
WAIT_SECONDS = {
    ("agent", "start"),
    ("agent", "stop"),
    ("deployment", "start"),
    ("deployment", "stop"),
}

_HELP = {"-h", "--help"}


def rewrite(
    args: list[str], subcommands: Callable[[tuple[str, ...]], set[str]]
) -> list[str]:
    """Rewrite deprecated command paths in a `hops` argument list.

    Args:
        args: Arguments after the program name.
        subcommands: Returns the subcommand names of a group in `GROUP_DEFAULTS`.

    Returns:
        The arguments with the new command path and flags, or `args` unchanged.
    """
    return _rewrite_flags(_rewrite_command(args, subcommands))


def _rewrite_command(
    args: list[str], subcommands: Callable[[tuple[str, ...]], set[str]]
) -> list[str]:
    for old in sorted(RENAMED_COMMANDS, key=len, reverse=True):
        start = _find(args, old)
        if start is not None:
            new = RENAMED_COMMANDS[old]
            _warn(old, new)
            return [*args[:start], *new, *args[start + len(old) :]]
    for group, default in GROUP_DEFAULTS.items():
        start = _find(args, group)
        if start is None:
            continue
        rest = args[start + len(group) :]
        if not rest or rest[0] in _HELP or rest[0] in subcommands(group):
            return args
        sub = default
        if group == ("fg", "stats") and "--compute" in rest:
            sub, rest = "compute", [a for a in rest if a != "--compute"]
        _warn((*group, "..."), (*group, sub, "..."))
        return [*args[:start], *group, sub, *rest]
    return args


def _rewrite_flags(args: list[str]) -> list[str]:
    for path, renames in RENAMED_FLAGS.items():
        start = _find(args, path)
        if start is None:
            continue
        head, rest = args[: start + len(path)], args[start + len(path) :]
        for i, token in enumerate(rest):
            name, eq, value = token.partition("=")
            if name in renames:
                _warn_flag(path, name, renames[name])
                rest[i] = renames[name] + eq + value
        args = head + rest
    for path, (index, option, old_count) in POSITIONAL_TO_OPTION.items():
        start = _find(args, path)
        if start is None:
            continue
        head, rest = args[: start + len(path)], args[start + len(path) :]
        leading = 0
        while leading < len(rest) and not rest[leading].startswith("-"):
            leading += 1
        if leading >= old_count and rest[index].isdigit():
            _warn_flag(path, rest[index], f"{option} {rest[index]}")
            value = rest.pop(index)
            args = [*head, *rest, option, value]
    for path in WAIT_SECONDS:
        start = _find(args, path)
        if start is None:
            continue
        head, rest = args[: start + len(path)], args[start + len(path) :]
        for i, token in enumerate(rest):
            name, eq, value = token.partition("=")
            if name != "--wait":
                continue
            if not eq:
                if i + 1 >= len(rest) or not rest[i + 1].isdigit():
                    continue
                value = rest.pop(i + 1)
            new = ["--no-wait"] if value == "0" else ["--timeout", value]
            _warn_flag(path, f"--wait {value}", " ".join(new))
            rest[i : i + 1] = new
            break
        args = head + rest
    return args


def _find(args: list[str], path: tuple[str, ...]) -> int | None:
    for i in range(len(args) - len(path) + 1):
        if tuple(args[i : i + len(path)]) == path:
            return i
    return None


def _warn_flag(path: tuple[str, ...], old: str, new: str) -> None:
    click.echo(
        f"Warning: `{old}` in `hops {' '.join(path)}` is deprecated; use `{new}`.",
        err=True,
    )


def _warn(old: tuple[str, ...], new: tuple[str, ...]) -> None:
    click.echo(
        f"Warning: `hops {' '.join(old)}` is deprecated; use `hops {' '.join(new)}`.",
        err=True,
    )

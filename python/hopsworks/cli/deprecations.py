"""Old command names, kept working for one release after the ontology change.

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

_HELP = {"-h", "--help"}


def rewrite(
    args: list[str], subcommands: Callable[[tuple[str, ...]], set[str]]
) -> list[str]:
    """Rewrite deprecated command paths in a `hops` argument list.

    Args:
        args: Arguments after the program name.
        subcommands: Returns the subcommand names of a group in `GROUP_DEFAULTS`.

    Returns:
        The arguments with the new command path, or `args` unchanged.
    """
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


def _find(args: list[str], path: tuple[str, ...]) -> int | None:
    for i in range(len(args) - len(path) + 1):
        if tuple(args[i : i + len(path)]) == path:
            return i
    return None


def _warn(old: tuple[str, ...], new: tuple[str, ...]) -> None:
    click.echo(
        f"Warning: `hops {' '.join(old)}` is deprecated; use `hops {' '.join(new)}`.",
        err=True,
    )

"""Output formatting for the ``hops`` CLI: human-readable tables and JSON.

The global ``--json`` / ``-o json`` flag flips a module-level ``JSON_MODE``
that causes every render helper to emit JSON instead of ANSI-decorated text.
When JSON mode is active the ``success``/``info``/``warn`` helpers are silent
so that stdout contains only the machine-readable payload.
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


JSON_MODE: bool = False

_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"


def _tty() -> bool:
    return sys.stdout.isatty()


def _wrap(code: str, text: str) -> str:
    return f"{code}{text}{_RESET}" if _tty() else text


def first_line(value: str | None, empty: str = "-") -> str:
    """Return the first non-empty line of ``value``, or ``empty`` when blank.

    Helper for table rendering: Hopsworks descriptions are commonly multi-line
    Markdown and we only want the first line in a ``FIELD / VALUE`` row.

    Args:
        value: Source string; ``None`` is treated as empty.
        empty: Fallback returned for blank input.

    Returns:
        A single line or the ``empty`` placeholder.
    """
    text = (value or "").strip()
    if not text:
        return empty
    return text.splitlines()[0]


def set_json_mode(enabled: bool) -> None:
    """Toggle JSON mode for the current process.

    Args:
        enabled: When True, render helpers emit JSON and info/warn are silenced.
    """
    global JSON_MODE
    JSON_MODE = enabled


def print_json(obj: Any) -> None:
    """Serialize ``obj`` as pretty-printed JSON on stdout.

    Uses ``default=str`` so SDK objects with ``datetime``/``UUID`` attributes
    serialize cleanly without the caller pre-converting them.

    Args:
        obj: The value to serialize.
    """
    sys.stdout.write(json.dumps(obj, indent=2, default=str, sort_keys=False))
    sys.stdout.write("\n")


def print_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> None:
    """Render ``rows`` under ``headers`` as an aligned ASCII table.

    Falls back to plain text when stdout is not a TTY so that the output is
    safe to pipe into ``grep`` or capture in files.
    When JSON mode is on, delegates to ``print_json`` with a list of dicts.

    Args:
        headers: Column titles.
        rows: Iterable of row values aligned with ``headers``.
    """
    rows_list = [list(r) for r in rows]
    if JSON_MODE:
        print_json([dict(zip(headers, r, strict=False)) for r in rows_list])
        return

    widths = [len(str(h)) for h in headers]
    for row in rows_list:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    header_line = "  ".join(
        _wrap(_BOLD, str(h).ljust(widths[i])) for i, h in enumerate(headers)
    )
    sys.stdout.write(header_line + "\n")
    sep = "  ".join("-" * widths[i] for i in range(len(headers)))
    sys.stdout.write(_wrap(_DIM, sep) + "\n")
    for row in rows_list:
        line = "  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))
        sys.stdout.write(line + "\n")


def success(msg: str, *args: Any) -> None:
    """Print a success message; silent in JSON mode.

    Args:
        msg: Printf-style format string.
        *args: Values substituted into ``msg``.
    """
    if JSON_MODE:
        return
    sys.stderr.write(_wrap(_GREEN, msg % args if args else msg) + "\n")


def info(msg: str, *args: Any) -> None:
    """Print an informational message; silent in JSON mode.

    Args:
        msg: Printf-style format string.
        *args: Values substituted into ``msg``.
    """
    if JSON_MODE:
        return
    sys.stderr.write((msg % args if args else msg) + "\n")


def warn(msg: str, *args: Any) -> None:
    """Print a warning; silent in JSON mode.

    Args:
        msg: Printf-style format string.
        *args: Values substituted into ``msg``.
    """
    if JSON_MODE:
        return
    sys.stderr.write(_wrap(_YELLOW, msg % args if args else msg) + "\n")


def error(msg: str, *args: Any) -> None:
    """Print an error; always visible, even in JSON mode.

    Args:
        msg: Printf-style format string.
        *args: Values substituted into ``msg``.
    """
    sys.stderr.write(_wrap(_RED, msg % args if args else msg) + "\n")

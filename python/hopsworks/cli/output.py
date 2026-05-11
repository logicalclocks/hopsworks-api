"""Output formatting for the ``hops`` CLI: human-readable tables and JSON.

The global ``--json`` / ``-o json`` flag flips a module-level ``JSON_MODE``
that causes every render helper to emit JSON instead of ANSI-decorated text.
When JSON mode is active the ``success``/``info``/``warn`` helpers are silent
and the SDK's own stdout chatter (login banner, ``Python Engine initialized``,
``Connection closed``, etc.) is rerouted to stderr so stdout carries the
machine-readable payload alone.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


JSON_MODE: bool = False
_REAL_STDOUT: Any = None
_NOISY_LOGGERS = ("hopsworks", "hopsworks_common", "hsfs", "hsml")

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

    Enabling JSON mode also isolates stdout from SDK chatter: every Python
    ``print``, the ``hopsworks``/``hsfs``/``hsml`` log handlers, and the
    SDK's "Logged in to project" banner all get rerouted to stderr so the
    payload emitted by :func:`print_json` / :func:`print_table` is the only
    thing on stdout.
    The isolation is idempotent and only takes effect once per process.

    Args:
        enabled: When True, render helpers emit JSON and info/warn are silenced.
    """
    global JSON_MODE
    JSON_MODE = enabled
    if enabled:
        _isolate_stdout()


def _isolate_stdout() -> None:
    """Move SDK stdout chatter to stderr; remember the real stdout for JSON.

    The SDK calls ``logging.basicConfig(stream=sys.stdout)`` at import time,
    so root-logger handlers already hold a reference to the original
    ``sys.stdout`` file object. We snapshot the current ``sys.stdout`` each
    time so JSON output lands wherever the caller expects (including a
    Click ``CliRunner`` buffer in tests, which is replaced per invocation),
    swap ``sys.stdout`` so any later ``print`` lands on stderr, patch
    existing root-logger handlers that point at the captured stdout, and
    raise the minimum level on the noisy SDK loggers so they do not spam
    the user in JSON mode.
    Safe to call more than once.
    """
    global _REAL_STDOUT

    _REAL_STDOUT = sys.stdout
    sys.stdout = sys.stderr

    root = logging.getLogger()
    for handler in root.handlers:
        if (
            isinstance(handler, logging.StreamHandler)
            and handler.stream is _REAL_STDOUT
        ):
            handler.setStream(sys.stderr)

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def print_json(obj: Any) -> None:
    """Serialize ``obj`` as pretty-printed JSON on the real stdout.

    Uses ``default=str`` so SDK objects with ``datetime``/``UUID`` attributes
    serialize cleanly without the caller pre-converting them.
    Writes to the stdout captured before :func:`_isolate_stdout` swapped
    ``sys.stdout`` so the payload survives the isolation hop. When isolation
    has not been triggered (e.g. JSON mode was never enabled, or print_json
    is called from a non-CLI context), falls back to the current
    ``sys.stdout``.

    Args:
        obj: The value to serialize.
    """
    out = _REAL_STDOUT if _REAL_STDOUT is not None else sys.stdout
    out.write(json.dumps(obj, indent=2, default=str, sort_keys=False))
    out.write("\n")
    out.flush()


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

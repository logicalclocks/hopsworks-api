"""Parser for the ``hops`` CLI join-spec mini-grammar.

The grammar is shared between ``hops fv create --join`` and ``hops fg derive
--join``. It was introduced by the Go CLI and we preserve it byte-for-byte so
existing scripts and docs keep working.

Grammar:

    <fg>[:<version>]  <TYPE>  <on>[=<right_on>][,<on>[=<right_on>]...]  [prefix]

Where ``TYPE`` is one of ``INNER | LEFT | RIGHT | FULL`` (case-insensitive),
``on`` is a left-side join column, ``right_on`` (optional) is the right-side
column when it differs, and ``prefix`` is an optional column-name prefix for
the joined features. A composite key lists its columns comma-separated, e.g.
``deal_id,snapshot_date``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


_SPEC_REGEX = re.compile(
    r"""
    ^\s*
    (?P<fg>[A-Za-z_][A-Za-z0-9_]*)        # feature group name
    (?::(?P<version>\d+))?                 # optional :version
    \s+
    (?P<type>INNER|LEFT|RIGHT|FULL)        # join type
    \s+
    (?P<key>[A-Za-z_][A-Za-z0-9_]*(?:=[A-Za-z_][A-Za-z0-9_]*)?(?:,[A-Za-z_][A-Za-z0-9_]*(?:=[A-Za-z_][A-Za-z0-9_]*)?)*)  # on[=right_on], comma-separated
    (?:\s+(?P<prefix>\S+))?                # optional prefix
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


@dataclass
class JoinSpec:
    """A parsed join clause.

    Attributes mirror the SDK's ``Query.join()`` arguments so callers can pass
    the spec through with minimal adaptation.
    """

    fg_name: str
    on: list[str]
    right_on: list[str] | None = None
    version: int | None = None
    join_type: str = "INNER"
    prefix: str | None = None

    def join_kwargs(self) -> dict[str, list[str] | None]:
        """The key arguments of ``Query.join()``: ``on``, or ``left_on`` and ``right_on``."""
        if self.right_on is None:
            return {"on": self.on, "left_on": None, "right_on": None}
        return {"on": None, "left_on": self.on, "right_on": self.right_on}


class JoinSpecError(ValueError):
    """Raised for malformed join specs so callers can wrap with a Click error."""


def parse(spec: str) -> JoinSpec:
    """Parse a join-spec string into a ``JoinSpec``.

    Args:
        spec: Raw flag value, e.g. ``"products LEFT product_id=id p_"``.

    Returns:
        A structured ``JoinSpec``.

    Raises:
        JoinSpecError: When the spec does not match the grammar.
    """
    match = _SPEC_REGEX.match(spec)
    if not match:
        raise JoinSpecError(
            f"Invalid join spec '{spec}'. "
            'Expected: "<fg>[:<ver>] <INNER|LEFT|RIGHT|FULL> <on>[=<right_on>][,...] [prefix]"'
        )
    pairs = [part.partition("=") for part in match.group("key").split(",")]
    return JoinSpec(
        fg_name=match.group("fg"),
        on=[left for left, _, _ in pairs],
        right_on=[right or left for left, _, right in pairs]
        if any(sep for _, sep, _ in pairs)
        else None,
        version=int(match.group("version")) if match.group("version") else None,
        join_type=match.group("type").upper(),
        prefix=match.group("prefix"),
    )

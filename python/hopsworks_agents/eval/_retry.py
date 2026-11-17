"""Retrying the platform calls an evaluation job cannot finish without.

Under load the platform can refuse a call it would answer a second later: twelve
monitors of one execution start together, every one of them lists traces and
writes trials, and each of those opens a fresh connection to the online store
after a fresh service lookup. Three of twelve runs failed that way in one
execution -- a 500 on the listing, a 500 fetching the trials feature group --
and the shared-sample guarantee went with them, since three evaluators then
graded nothing.

Only what is safe to repeat goes through here: reads, and online inserts, which
are upserts by primary key. A refusal that is about the request (4xx) is not
retried; it would be refused again.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, TypeVar


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

log = logging.getLogger(__name__)

T = TypeVar("T")

#: Waits between attempts, in seconds: three tries over about seven seconds.
TRANSIENT_DELAYS: tuple[float, ...] = (2.0, 5.0)


def _status_of(err: BaseException) -> int | None:
    response = getattr(err, "response", None)
    status = getattr(response, "status_code", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _transient(err: BaseException) -> bool:
    """A server error, or no answer at all; not a refusal of the request itself."""
    status = _status_of(err)
    return status is None or status >= 500


def _retrying(
    call: Callable[[], T],
    *,
    what: str,
    delays: Sequence[float] = TRANSIENT_DELAYS,
    sleep: Callable[[float], Any] = time.sleep,
) -> T:
    """Run ``call``, retrying a transient failure after each of ``delays``."""
    for delay in (*delays, None):
        try:
            return call()
        except Exception as err:  # noqa: BLE001 -- classified below, re-raised when final
            if delay is None or not _transient(err):
                raise
            log.warning("%s failed (%s); retrying in %.0f s", what, err, delay)
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover

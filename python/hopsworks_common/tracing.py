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
"""OpenTelemetry spans and timing for a deployment whose tracing is enabled.

Nothing here is active until the deployment's model server calls [`_enable`][] at startup, which it does only when the deployment has tracing turned on.
Until then every helper returns at its first check, without allocating or reading a clock, so the library costs nothing in notebooks, jobs or untraced deployments.
The library never configures a tracer provider; the model server does.
"""

from __future__ import annotations

import contextlib
import contextvars
import sys
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterator


_TRACER_NAME = "hopsworks"

# One shared instance: a disabled helper hands it out instead of creating a context manager per call.
_NO_SPAN = contextlib.nullcontext()

_enabled = False

# The rows a traced request may still open transformation spans for, as a one-element list so a request decrements its own budget.
_row_budget: contextvars.ContextVar[list[int] | None] = contextvars.ContextVar(
    "hopsworks_trace_row_budget", default=None
)


def _enable() -> None:
    """Turn telemetry on for this process; called once by the model server after it configured a tracer provider."""
    global _enabled
    _enabled = True


def _is_enabled() -> bool:
    return _enabled


def _trace_module():
    return sys.modules["opentelemetry.trace"]


def _recording() -> bool:
    """Whether telemetry is on and the current request is a sampled trace."""
    return _enabled and _trace_module().get_current_span().is_recording()


@contextlib.contextmanager
def _request_scope(traced_rows: int) -> Iterator[None]:
    """Bound the transformation spans of one request to its first `traced_rows` rows; the model server opens this around each request."""
    token = _row_budget.set([traced_rows])
    try:
        yield
    finally:
        _row_budget.reset(token)


def _take_row() -> bool:
    """Whether the current sampled request may trace one more row, consuming it from the request's budget."""
    budget = _row_budget.get()
    if not budget or budget[0] <= 0 or not _recording():
        return False
    budget[0] -= 1
    return True


def _capture() -> Any:
    """The current span's context, to parent work that runs later on another thread, or `None` when the request is not traced."""
    if not _recording():
        return None
    return _trace_module().get_current_span().get_span_context()


def _child_span(
    name: str, attributes: dict[str, Any] | None = None, parent: Any = None
):
    """A context manager for a span under the current one, or under `parent` from [`_capture`][]; a no-op when the request is not traced.

    The span records an exception raised in its body and is marked as failed.
    A span under `parent` may end after the span it was captured from.
    """
    if not _enabled:
        return _NO_SPAN
    trace = _trace_module()
    if parent is not None:
        context = trace.set_span_in_context(trace.NonRecordingSpan(parent))
    elif trace.get_current_span().is_recording():
        context = None
    else:
        return _NO_SPAN
    return trace.get_tracer(_TRACER_NAME).start_as_current_span(
        name, context=context, attributes=attributes
    )


def _record_span(
    name: str,
    start_time_ns: int,
    end_time_ns: int,
    attributes: dict[str, Any] | None = None,
    error: BaseException | None = None,
) -> None:
    """Record an already finished span under the current one, for work timed where a live span cannot be held open.

    Times are nanoseconds since the epoch.
    """
    if not _recording():
        return
    trace = _trace_module()
    span = trace.get_tracer(_TRACER_NAME).start_span(
        name, start_time=start_time_ns, attributes=attributes
    )
    if error is not None:
        span.record_exception(error)
        span.set_status(trace.Status(trace.StatusCode.ERROR, str(error)))
    span.end(end_time=end_time_ns)

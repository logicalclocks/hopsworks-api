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
import time
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from typing_extensions import Self


_TRACER_NAME = "hopsworks"

# One shared instance: a disabled helper hands it out instead of creating a context manager per call.
_NO_SPAN = contextlib.nullcontext()

# Whether the model server set tracing up for this process, fixed at start; telemetry state is built only then.
_available = False
# Whether requests are traced right now; switched while the process runs, and only ever on when available.
_enabled = False

# The rows a traced request may open transformation spans for, and the rows each caller has taken; a mutable pair so a request counts its own.
_row_budget: contextvars.ContextVar[tuple[int, dict] | None] = contextvars.ContextVar(
    "hopsworks_trace_row_budget", default=None
)

# Nanoseconds the current request has spent so far in each stage, keyed by the stage and the feature view or model it ran for.
_request_stages_ns: contextvars.ContextVar[dict[tuple[str, str], int] | None] = (
    contextvars.ContextVar("hopsworks_request_stages_ns", default=None)
)

# The stage being timed in the current context, so a stage inside another of the same kind is not counted twice.
_active_stage: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "hopsworks_active_stage", default=None
)

# Called with a request's time per stage and name when a request that ran any stage ends.
_request_observers: list[Callable[[dict[tuple[str, str], int]], None]] = []


def _enable(active: bool = True) -> None:
    """Make telemetry available to this process, on or off; called once by the model server after it configured a tracer provider."""
    global _available, _enabled
    _available = True
    _enabled = active
    from hopsworks_common import request_metrics

    request_metrics._register()


def _set_active(active: bool) -> None:
    """Switch telemetry on or off while the process runs; a no-op in a process [`_enable`][] was never called in."""
    global _enabled
    _enabled = _available and active


def _is_available() -> bool:
    return _available


def _trace_module():
    return sys.modules["opentelemetry.trace"]


def _recording() -> bool:
    """Whether telemetry is on and the current request is a sampled trace."""
    return _enabled and _trace_module().get_current_span().is_recording()


@contextlib.contextmanager
def _request_scope(traced_rows: int) -> Iterator[None]:
    """Bound the transformation spans of one request to its first `traced_rows` rows, and total its time per stage; the model server opens this around each request, sampled or not."""
    token = _row_budget.set((traced_rows, {}))
    stages: dict[tuple[str, str], int] = {}
    stages_token = _request_stages_ns.set(stages)
    try:
        yield
    finally:
        _request_stages_ns.reset(stages_token)
        _row_budget.reset(token)
        if stages:
            for observer in _request_observers:
                observer(stages)


def _add_stage_ns(stage: str, name: str, duration_ns: int) -> None:
    """Add time to one stage of the current request for the feature view or model `name`; outside a request scope it is not counted."""
    stages = _request_stages_ns.get()
    if stages is not None:
        key = (stage, name)
        stages[key] = stages.get(key, 0) + duration_ns


class _Stage:
    """Times one stage of a request into its stage totals, under a span of the same name when the request is sampled."""

    __slots__ = ("_kind", "_name", "_span", "_start_ns", "_token")

    def __init__(self, kind: str, name: str, attributes: dict[str, Any] | None):
        self._kind = kind
        self._name = name
        self._span = _child_span(kind, attributes)

    def __enter__(self) -> Self:
        self._token = _active_stage.set(self._kind)
        self._span.__enter__()
        self._start_ns = time.perf_counter_ns()
        return self

    def __exit__(self, exc_type, exc, tb):
        _add_stage_ns(self._kind, self._name, time.perf_counter_ns() - self._start_ns)
        _active_stage.reset(self._token)
        return self._span.__exit__(exc_type, exc, tb)


def _stage(kind: str, name: str = "", attributes: dict[str, Any] | None = None):
    """A context manager timing one stage of the current request, such as `feature_lookup` of feature view `name`.

    A no-op while telemetry is off, and inside a stage of the same kind, such as a feature view lookup the default predictor's lookup makes.
    `name` is a feature view or model as `<name>:<version>`.
    """
    if not _enabled or _active_stage.get() == kind:
        return _NO_SPAN
    return _Stage(kind, name, attributes)


def _take_row(key: Any = None) -> bool:
    """Whether the current sampled request may trace one more row for `key`, consuming it from the request's budget.

    Each key has its own allowance, so a request whose phases run as separate passes over its rows traces the same first rows in each.
    """
    budget = _row_budget.get()
    if budget is None or not _recording():
        return False
    allowance, taken = budget
    used = taken.get(key, 0)
    if used >= allowance:
        return False
    taken[key] = used + 1
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

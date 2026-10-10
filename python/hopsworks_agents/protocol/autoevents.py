"""Automatic tool-event emission from OpenInference spans.

When ``tool_events`` is on and tracing is active, this bridges the framework
instrumentation the SDK already runs (LangChain/LangGraph, LlamaIndex, OpenAI
Agents, Claude Agent SDK) to ``tool_event`` SSE frames — so tool calls show up
in the chat panel with zero code in the agent, the same way they do in
framework tracing.

A span processor watches for TOOL/RETRIEVER spans and, using the request's
:class:`HandlerContext` tracked in a context variable, emits a ``running``
event on span start and ``done``/``failed`` on end, keyed by span id so the
client collapses them into one updating chip.
"""

from __future__ import annotations

import contextvars
import logging
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from .context import HandlerContext

log = logging.getLogger(__name__)

# the HandlerContext of the turn currently executing on this async task; read
# by the span processor (which runs in sync OTel callbacks)
current_context: contextvars.ContextVar[HandlerContext | None] = contextvars.ContextVar(
    "hopsworks_agent_current_context", default=None
)

# OpenInference span-kind attribute -> the kinds we surface as tool events
_TOOL_KINDS = {"TOOL", "RETRIEVER"}
_SPAN_KIND_ATTR = "openinference.span.kind"


def _span_kind(span: Any) -> str | None:
    attrs = getattr(span, "attributes", None) or {}
    kind = attrs.get(_SPAN_KIND_ATTR)
    return kind.upper() if isinstance(kind, str) else None


def _tool_name(span: Any, kind: str) -> str:
    attrs = getattr(span, "attributes", None) or {}
    return attrs.get("tool.name") or getattr(span, "name", None) or kind.lower()


def install_auto_tool_events(tracer_provider: Any) -> bool:
    """Register the span processor on the given provider. Returns True when.

    installed. Best-effort: any failure (OTel missing, unexpected API) logs and
    returns False without disturbing tracing.
    """
    try:
        from opentelemetry.sdk.trace import SpanProcessor
    except ImportError:
        return False

    class _ToolEventSpanProcessor(SpanProcessor):
        def on_start(self, span: Any, parent_context: Any = None) -> None:
            ctx = current_context.get()
            if ctx is None:
                return
            kind = _span_kind(span)
            if kind not in _TOOL_KINDS:
                return
            span_id = format(span.get_span_context().span_id, "016x")
            try:
                ctx._emit_sync(_tool_name(span, kind), "running", None, None, span_id)
            except Exception:  # noqa: BLE001 — never disturb the span
                log.debug("auto tool-event on_start failed", exc_info=True)

        def on_end(self, span: Any) -> None:
            ctx = current_context.get()
            if ctx is None:
                return
            kind = _span_kind(span)
            if kind not in _TOOL_KINDS:
                return
            span_id = format(span.get_span_context().span_id, "016x")
            status = getattr(getattr(span, "status", None), "status_code", None)
            failed = status is not None and getattr(status, "name", "") == "ERROR"
            try:
                ctx._emit_sync(
                    _tool_name(span, kind),
                    "failed" if failed else "done",
                    None,
                    None,
                    span_id,
                )
            except Exception:  # noqa: BLE001
                log.debug("auto tool-event on_end failed", exc_info=True)

        def shutdown(self) -> None:
            pass

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            return True

    try:
        tracer_provider.add_span_processor(_ToolEventSpanProcessor())
        log.info("Automatic tool events enabled")
        return True
    except Exception:  # noqa: BLE001
        log.warning("Could not install the tool-event span processor", exc_info=True)
        return False

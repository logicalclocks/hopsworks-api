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


#: What a model call is called, and what it reports, in the conventions in use.
#: The same keys the sidecar classifies on: the two have to agree about what a
#: model call is, or a turn whose call one of them counted and the other did not
#: reports its usage twice -- once from the span, once from the turn's own
#: fallback figure.
_MODEL_KEYS = ("gen_ai.request.model", "llm.model_name", "gen_ai.response.model")
_USAGE_KEYS = (
    "gen_ai.usage.input_tokens",
    "gen_ai.usage.prompt_tokens",
    "llm.token_count.prompt",
    "llm.token_count.input",
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.completion_tokens",
    "llm.token_count.completion",
    "llm.token_count.output",
)


def _reports_usage(attrs: dict) -> bool:
    for key in _USAGE_KEYS:
        value = attrs.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return True
        if isinstance(value, str) and value.strip().isdigit() and int(value) > 0:
            return True
    return False


def _is_llm_span(span: Any) -> bool:
    """Whether this span is a model call, by any convention in use."""
    attrs = getattr(span, "attributes", None) or {}
    kind = attrs.get(_SPAN_KIND_ATTR)
    if isinstance(kind, str) and kind.upper() == "LLM":
        return True
    operation = attrs.get("gen_ai.operation.name")
    if isinstance(operation, str) and operation.lower() == "chat":
        return True
    # A span that names a model and reports what the call cost is a model call
    # whatever kind it declares: the Claude Agent SDK reports its calls on AGENT
    # spans. The sidecar reads those as model calls, so this must too.
    return bool(any(attrs.get(key) for key in _MODEL_KEYS)) and _reports_usage(attrs)


def install_llm_span_detection(tracer_provider: Any) -> bool:
    """Record, per turn, whether any model call was traced.

    The turn span carries the agent's own usage figure for the whole turn, and
    whether that figure is the only one a reader will see depends on this turn,
    not on what was instrumented at start-up: a custom agent may instrument
    itself (the global provider now reaches it), and an instrumented framework
    may call a model through a client its instrumentor does not see. Asked at
    start-up the answer was wrong both ways -- the sidecar counted the turn and
    its model calls, or neither.

    Best-effort, like the tool-event processor: a failure here costs an
    attribute, not a turn.
    """
    try:
        from opentelemetry.sdk.trace import SpanProcessor
    except ImportError:
        return False

    class _LlmSpanProcessor(SpanProcessor):
        def on_start(self, span: Any, parent_context: Any = None) -> None:
            pass

        def on_end(self, span: Any) -> None:
            ctx = current_context.get()
            if ctx is None or not _is_llm_span(span):
                return
            ctx._saw_llm_span = True

        def shutdown(self) -> None:
            pass

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            return True

    try:
        tracer_provider.add_span_processor(_LlmSpanProcessor())
        return True
    except Exception:  # noqa: BLE001
        log.warning("Could not install the LLM-span detector", exc_info=True)
        return False


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

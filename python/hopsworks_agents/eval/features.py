"""Turn raw OTLP spans into one row per trace.

The hard part is not the arithmetic, it is deciding *when* a trace is done. A
trace is never explicitly finished: spans arrive incrementally, the agent
batches its exports, the sidecar queues its inserts, and offline
materialisation adds its own lag. Featurizing on the first sight of a root span
produces a row describing half a trajectory, and a trajectory evaluator reading it
will confidently mark a correct agent wrong.

So completeness is a decision made here, from two clocks:

- **ingestion time** (``created_at``, stamped by the sidecar on insert) drives
  the watermark, so a span delayed in the sidecar's queue cannot be skipped by
  a watermark that has already moved past its start time;
- **the grace period** since the trace's newest span, which is what actually
  says "nothing more is coming".

Everything in this module is pure: spans in, rows out. The IO lives in
``featurize_job``, so the logic that decides whether a trace is gradable can be
tested without a cluster.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from hopsworks_agents.protocol import conventions


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


# Span rows arrive as dicts shaped like the sidecar's `otel_spans` rows, and
# attributes as `otel_span_attributes` rows. Kept as plain dicts rather than a
# model: the sidecar writes them, and a schema class here would be a second
# definition of the same thing, free to drift.
Span = dict[str, Any]
Attribute = dict[str, Any]
Event = dict[str, Any]

STATUS_ERROR = {"STATUS_CODE_ERROR", "ERROR", "2", 2}


@dataclass
class TraceCompleteness:
    """Which traces are ready to featurize, and which are still in flight."""

    ready: list[str] = field(default_factory=list)
    # root span never arrived within the longer timeout: featurized anyway and
    # flagged, because dropping it hides exactly the traces most likely to be
    # broken -- an agent that crashed before its root span was exported
    partial: list[str] = field(default_factory=list)
    # still inside the grace period; leave them for the next run
    pending: list[str] = field(default_factory=list)


def _is_root(span: Span) -> bool:
    return not span.get("parent_span_id")


def _ingested_at(span: Span) -> datetime | None:
    value = span.get("created_at")
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def select_ready_traces(
    spans: Sequence[Span],
    *,
    now: datetime,
    grace: timedelta = timedelta(minutes=3),
    root_timeout: timedelta = timedelta(minutes=30),
) -> TraceCompleteness:
    """Split traces into ready / partial / pending.

    A trace is ready when its root span has arrived *and* nothing new has been
    ingested for ``grace``. Without the second condition the root span's
    arrival would be treated as the end of the trace, which it is not: children
    complete before their parent but are not guaranteed to be *ingested* before
    it, and a slow tool span can land well after the answer was returned.
    """
    by_trace: dict[str, list[Span]] = {}
    for span in spans:
        trace_id = span.get("trace_id")
        if trace_id:
            by_trace.setdefault(trace_id, []).append(span)

    result = TraceCompleteness()
    for trace_id, trace_spans in by_trace.items():
        ingest_times = [t for t in (_ingested_at(s) for s in trace_spans) if t]
        # no usable ingestion time: treat as pending rather than guess, since
        # the alternative is featurizing on the strength of a missing clock
        if not ingest_times:
            result.pending.append(trace_id)
            continue
        quiet_for = now - max(ingest_times)
        has_root = any(_is_root(span) for span in trace_spans)

        if has_root and quiet_for >= grace:
            result.ready.append(trace_id)
        elif not has_root and quiet_for >= root_timeout:
            result.partial.append(trace_id)
        else:
            result.pending.append(trace_id)
    return result


def _attributes_by_span(attributes: Iterable[Attribute]) -> dict[str, dict[str, str]]:
    grouped: dict[str, dict[str, str]] = {}
    for attribute in attributes:
        span_id = attribute.get("span_id")
        key = attribute.get("attr_key")
        if span_id and key:
            grouped.setdefault(span_id, {})[key] = attribute.get("attr_value", "")
    return grouped


def _first(values: Iterable[str | None]) -> str:
    for value in values:
        if value:
            return value
    return ""


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return round(float(value))
        except (TypeError, ValueError):
            return 0


def _span_kind(attrs: dict[str, str]) -> str:
    kind = attrs.get(conventions.SPAN_KIND, "").upper()
    if kind:
        return kind
    operation = attrs.get(conventions.GEN_AI_OPERATION_NAME, "").lower()
    return {
        conventions.OPERATION_CHAT: conventions.SPAN_KIND_LLM,
        conventions.OPERATION_EXECUTE_TOOL: conventions.SPAN_KIND_TOOL,
        conventions.OPERATION_INVOKE_AGENT: conventions.SPAN_KIND_AGENT,
    }.get(operation, "")


def _parse_token_count(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return round(float(value))
        except (TypeError, ValueError):
            return None


def _token_count(attrs: dict[str, str], keys: Sequence[str]) -> int:
    for key in keys:
        value = attrs.get(key)
        if value is None or value == "":
            continue
        parsed = _parse_token_count(value)
        if parsed is not None:
            return parsed
    return 0


def _messages(span: Span) -> list[dict[str, str]]:
    raw = span.get("messages")
    if not raw:
        return []
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def trace_features(
    spans: Sequence[Span],
    attributes: Sequence[Attribute] = (),
    events: Sequence[Event] = (),
    *,
    is_partial: bool = False,
    input_token_price_per_million: float | None = None,
    output_token_price_per_million: float | None = None,
) -> dict[str, Any]:
    """One ``agent_trace_features`` row from every span of a single trace."""
    if not spans:
        raise ValueError("trace_features needs at least one span")

    attrs_by_span = _attributes_by_span(attributes)
    ordered = sorted(spans, key=lambda s: _as_int(s.get("start_time_ns")))
    root = next((s for s in ordered if _is_root(s)), None)
    root_attrs = attrs_by_span.get(root.get("span_id"), {}) if root else {}

    def any_attr(*keys: str) -> str:
        """First value for these keys anywhere in the trace, root span first.

        Attributes like the model name sit on the LLM span, not the root, so a
        root-only read would report nothing for most traces.
        """
        for key in keys:
            if root_attrs.get(key):
                return root_attrs[key]
        for span in ordered:
            span_attrs = attrs_by_span.get(span.get("span_id"), {})
            for key in keys:
                if span_attrs.get(key):
                    return span_attrs[key]
        return ""

    kinds = {
        span.get("span_id"): _span_kind(attrs_by_span.get(span.get("span_id"), {}))
        for span in ordered
    }
    tool_spans = [
        s for s in ordered if kinds.get(s.get("span_id")) == conventions.SPAN_KIND_TOOL
    ]
    token_spans = [
        s
        for s in ordered
        if kinds.get(s.get("span_id")) == conventions.SPAN_KIND_LLM
        or (
            kinds.get(s.get("span_id")) == conventions.SPAN_KIND_AGENT
            and (
                _token_count(
                    attrs_by_span.get(s.get("span_id"), {}),
                    conventions.INPUT_TOKEN_KEYS,
                )
                or _token_count(
                    attrs_by_span.get(s.get("span_id"), {}),
                    conventions.OUTPUT_TOKEN_KEYS,
                )
            )
        )
    ]

    tool_names = []
    for span in tool_spans:
        span_attrs = attrs_by_span.get(span.get("span_id"), {})
        name = _first(
            [
                span_attrs.get(conventions.TOOL_NAME),
                span_attrs.get(conventions.GEN_AI_TOOL_NAME),
                span.get("name"),
            ]
        )
        if name and name not in tool_names:
            tool_names.append(name)

    input_tokens = sum(
        _token_count(
            attrs_by_span.get(s.get("span_id"), {}), conventions.INPUT_TOKEN_KEYS
        )
        for s in token_spans
    )
    output_tokens = sum(
        _token_count(
            attrs_by_span.get(s.get("span_id"), {}), conventions.OUTPUT_TOKEN_KEYS
        )
        for s in token_spans
    )

    guardrail_events = [
        e for e in events if e.get("name") == conventions.GUARDRAIL_EVENT
    ]

    start_ns = _as_int(ordered[0].get("start_time_ns"))
    end_ns = max(_as_int(s.get("end_time_ns")) for s in ordered)

    # Authoritative for SDK agents, which stamp the response object's text on
    # the root span. Falls back to the last assistant message for agents whose
    # root span was never stamped; left empty rather than guessed when there is
    # neither, since a wrong final_output silently corrupts every evaluator that
    # reads it.
    final_output = root_attrs.get(conventions.OUTPUT_VALUE, "")
    if not final_output:
        assistant = [
            m
            for span in ordered
            for m in _messages(span)
            if m.get("role") == "assistant" and m.get("content")
        ]
        final_output = assistant[-1]["content"] if assistant else ""

    all_messages = _messages(root) if root else []
    if not all_messages:
        for span in ordered:
            all_messages = _messages(span)
            if all_messages:
                break

    eval_run_id = any_attr(conventions.EVAL_RUN_ID)
    cost = _estimated_cost(
        input_tokens,
        output_tokens,
        input_token_price_per_million,
        output_token_price_per_million,
    )

    return {
        "deployment_id": (root or ordered[0]).get("deployment_id"),
        "trace_id": ordered[0].get("trace_id"),
        "root_span_id": root.get("span_id") if root else "",
        "agent_name": any_attr(conventions.GEN_AI_AGENT_NAME),
        "agent_version": any_attr(conventions.GEN_AI_AGENT_VERSION),
        "model_name": any_attr(
            conventions.GEN_AI_REQUEST_MODEL,
            conventions.LLM_MODEL_NAME,
            conventions.GEN_AI_RESPONSE_MODEL,
        ),
        "provider_name": any_attr(conventions.GEN_AI_PROVIDER_NAME),
        "session_id": _first([s.get("session_id") for s in ordered]),
        "user_id": _first([s.get("user_id") for s in ordered]),
        "start_time": datetime.fromtimestamp(start_ns / 1e9, tz=timezone.utc),
        "end_time": datetime.fromtimestamp(end_ns / 1e9, tz=timezone.utc),
        "latency_ms": max(0.0, (end_ns - start_ns) / 1e6),
        "status_code": (root or ordered[0]).get("status_code", ""),
        "status_message": (root or ordered[0]).get("status_message", ""),
        "input_messages": json.dumps(
            [m for m in all_messages if m.get("role") != "assistant"]
        ),
        "output_messages": json.dumps(
            [m for m in all_messages if m.get("role") == "assistant"]
        ),
        "final_output": final_output,
        "message_count": len(all_messages),
        "turn_count": sum(1 for m in all_messages if m.get("role") == "user"),
        "tool_call_count": len(tool_spans),
        "tool_names": json.dumps(tool_names),
        "tool_error_count": sum(
            1 for s in tool_spans if s.get("status_code") in STATUS_ERROR
        ),
        "guardrail_trigger_count": len(guardrail_events),
        "guardrail_names": json.dumps([]),
        "was_blocked": any_attr(conventions.GUARDRAIL_BLOCKED).lower() == "true",
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "estimated_cost": cost,
        "prompt_template": _first([s.get("prompt_template") for s in ordered]),
        "prompt_template_version": _first(
            [s.get("prompt_template_version") for s in ordered]
        ),
        "prompt_template_variables": _first(
            [s.get("prompt_template_variables") for s in ordered]
        ),
        "metadata": _first([s.get("metadata") for s in ordered]),
        "tags": _first([s.get("tags") for s in ordered]),
        # The separation every downstream query depends on: eval trials run
        # through the same deployment and tables as real traffic.
        "is_eval": bool(eval_run_id),
        "eval_run_id": eval_run_id,
        "trace_status": "PARTIAL" if is_partial else "RECEIVED",
        "created_at": datetime.now(tz=timezone.utc),
    }


def _estimated_cost(
    input_tokens: int,
    output_tokens: int,
    input_price_per_million: float | None,
    output_price_per_million: float | None,
) -> float | None:
    """Null, not zero, when no price is known.

    Zero is a claim that the trace was free, and it averages into cost
    dashboards as if it were true. Null is the honest answer and excludes
    itself from aggregates.
    """
    if input_price_per_million is None and output_price_per_million is None:
        return None
    return (
        input_tokens * (input_price_per_million or 0.0)
        + output_tokens * (output_price_per_million or 0.0)
    ) / 1_000_000

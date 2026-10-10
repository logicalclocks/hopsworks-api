"""Automatic OTel tracing for Hopsworks agent deployments.

When tracing is enabled on a Hopsworks agent deployment, the platform runs an
OTLP collector sidecar and injects ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`` into
the predictor container. This module keys off that env var: if it is present,
a TracerProvider exporting to the sidecar is built and the framework-matching
OpenInference instrumentor is activated — no tracing code in the agent.

Frameworks:
- ``langgraph``  -> openinference-instrumentation-langchain (LangChain/LangGraph)
- ``llamaindex`` -> openinference-instrumentation-llama-index
- ``openai_agents`` -> openinference-instrumentation-openai-agents
- ``claude_agents`` -> openinference-instrumentation-claude-agent-sdk
- ``custom``     -> provider only; instrument manually via ``app.tracer_provider``

It also owns the *turn span*: one SERVER span per request, continuing whatever
trace context the caller sent. Without it the agent starts a fresh trace on
every request, which means a caller that knows the trace id it asked for — the
eval runner — cannot find the trace the agent actually produced.

All imports are lazy and failures are non-fatal: a missing instrumentation
package logs a warning and the agent runs untraced.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from .conventions import BAGGAGE_PREFIXES


if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


log = logging.getLogger(__name__)

TRACES_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
FRAMEWORK_ENV = "AGENT_FRAMEWORK"

FRAMEWORK_LANGGRAPH = "langgraph"
FRAMEWORK_LLAMAINDEX = "llamaindex"
FRAMEWORK_OPENAI_AGENTS = "openai_agents"
FRAMEWORK_CLAUDE_AGENTS = "claude_agents"
FRAMEWORK_CUSTOM = "custom"
FRAMEWORK_ALIASES = {
    "openai-agents": FRAMEWORK_OPENAI_AGENTS,
    "openaiagents": FRAMEWORK_OPENAI_AGENTS,
    "claude-agents": FRAMEWORK_CLAUDE_AGENTS,
    "claudeagents": FRAMEWORK_CLAUDE_AGENTS,
    "claude-agent-sdk": FRAMEWORK_CLAUDE_AGENTS,
    "claude_agent_sdk": FRAMEWORK_CLAUDE_AGENTS,
}
KNOWN_FRAMEWORKS = (
    FRAMEWORK_LANGGRAPH,
    FRAMEWORK_LLAMAINDEX,
    FRAMEWORK_OPENAI_AGENTS,
    FRAMEWORK_CLAUDE_AGENTS,
    FRAMEWORK_CUSTOM,
)


def resolve_framework(framework: str | None) -> str:
    """Explicit argument wins; else the platform-injected AGENT_FRAMEWORK.

    env var; else 'custom'.
    """
    raw = framework or os.environ.get(FRAMEWORK_ENV) or FRAMEWORK_CUSTOM
    value = raw.strip().lower()
    value = FRAMEWORK_ALIASES.get(value, value)
    if value not in KNOWN_FRAMEWORKS:
        log.warning(
            "Unknown agent framework %r, falling back to 'custom' (known: %s)",
            value,
            ", ".join(KNOWN_FRAMEWORKS),
        )
        return FRAMEWORK_CUSTOM
    return value


def setup_tracing(framework: str, enabled: bool | None = None) -> Any | None:
    """Build a TracerProvider exporting to the deployment's OTLP sidecar and.

    instrument the given framework. Returns the provider, or None when tracing
    is off/unavailable.

    ``enabled=None`` (default) auto-detects from the presence of the
    ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`` env var; ``False`` disables even
    when the env var is present.
    """
    endpoint = os.environ.get(TRACES_ENDPOINT_ENV)
    if enabled is False:
        return None
    if endpoint is None:
        if enabled is True:
            log.warning(
                "Tracing requested but %s is not set — is tracing enabled on "
                "the deployment?",
                TRACES_ENDPOINT_ENV,
            )
        return None

    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk import trace as trace_sdk
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    except ImportError:
        log.warning(
            "Tracing endpoint %s is set but the OpenTelemetry SDK is not "
            "installed; running untraced. Install "
            "hopsworks-agent-protocol[tracing].",
            endpoint,
        )
        return None

    provider = trace_sdk.TracerProvider()
    # SimpleSpanProcessor on purpose: the Hopsworks OTLP sidecar relies on
    # spans arriving individually for its span propagation.
    provider.add_span_processor(
        SimpleSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    )
    install_baggage_propagation(provider)

    _instrument(framework, provider)
    log.info("Tracing enabled (framework=%s, endpoint=%s)", framework, endpoint)
    return provider


def install_baggage_propagation(tracer_provider: Any) -> bool:
    """Copy allowlisted baggage entries onto every span as attributes.

    Baggage rides the request context but does not become span attributes on
    its own, so the eval runner's ``hopsworks.eval.*`` ids would reach the
    agent and stop there — never reaching the sidecar, which only sees spans.

    Only entries under :data:`BAGGAGE_PREFIXES` are copied. Baggage arrives
    over the wire from whoever called the agent, so copying it wholesale would
    let any caller write arbitrary attributes into the project's trace tables.

    Returns True when installed. Best-effort: any failure logs and returns
    False without disturbing tracing.
    """
    try:
        from opentelemetry import baggage
        from opentelemetry.sdk.trace import SpanProcessor
    except ImportError:
        return False

    class _BaggageSpanProcessor(SpanProcessor):
        def on_start(self, span: Any, parent_context: Any = None) -> None:
            try:
                entries = baggage.get_all(parent_context)
            except Exception:  # noqa: BLE001 — never disturb the span
                log.debug("baggage read failed", exc_info=True)
                return
            for key, value in entries.items():
                if not str(key).startswith(BAGGAGE_PREFIXES):
                    continue
                try:
                    span.set_attribute(str(key), str(value))
                except Exception:  # noqa: BLE001
                    log.debug("baggage attribute failed", exc_info=True)

        def on_end(self, span: Any) -> None:
            pass

        def shutdown(self) -> None:
            pass

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            return True

    try:
        tracer_provider.add_span_processor(_BaggageSpanProcessor())
        return True
    except Exception:  # noqa: BLE001
        log.warning("Could not install the baggage span processor", exc_info=True)
        return False


def extract_context(headers: Mapping[str, str] | None) -> Any | None:
    """W3C ``traceparent`` + ``baggage`` from request headers as an OTel.

    Context, or None when unavailable.
    """
    if not headers:
        return None
    try:
        from opentelemetry.propagate import extract
    except ImportError:
        return None
    try:
        return extract({str(k).lower(): v for k, v in headers.items()})
    except Exception:  # noqa: BLE001 — a malformed header must not fail a turn
        log.debug("trace context extraction failed", exc_info=True)
        return None


@contextmanager
def turn_span(
    tracer_provider: Any,
    *,
    name: str,
    headers: Mapping[str, str] | None = None,
    attributes: dict[str, Any] | None = None,
) -> Iterator[Any]:
    """One SERVER span for the whole turn, parented to the caller's trace.

    Yields the span, or None when tracing is off or the OTel SDK is missing —
    so every call site is written the same way and degrades silently.

    The extracted context is *attached* rather than only passed as the span's
    parent: that keeps the caller's baggage active for the whole turn, so the
    baggage span processor sees it on child spans the framework creates too.
    """
    token, span_cm = _begin_turn_span(tracer_provider, name, headers, attributes)
    try:
        if span_cm is None:
            yield None
        else:
            with span_cm as span:
                yield span
    finally:
        if token is not None:
            try:
                from opentelemetry import context as context_api

                context_api.detach(token)
            except Exception:  # noqa: BLE001
                log.debug("context detach failed", exc_info=True)


def _begin_turn_span(
    tracer_provider: Any,
    name: str,
    headers: Mapping[str, str] | None,
    attributes: dict[str, Any] | None,
) -> tuple[Any | None, Any | None]:
    """Attach the caller's context and build the span context manager.

    Returns ``(detach_token, span_context_manager)``, either of which may be
    None. Kept separate from :func:`turn_span` so that a failure here yields
    None once rather than twice — a generator context manager cannot.
    """
    if tracer_provider is None:
        return None, None
    token = None
    try:
        from opentelemetry import context as context_api
        from opentelemetry import trace

        extracted = extract_context(headers)
        if extracted is not None:
            token = context_api.attach(extracted)
        tracer = tracer_provider.get_tracer("hopsworks_agents.protocol")
        return token, tracer.start_as_current_span(
            name,
            kind=trace.SpanKind.SERVER,
            attributes=attributes or {},
        )
    except Exception:  # noqa: BLE001 — tracing must never break a turn
        log.warning("Could not start the turn span", exc_info=True)
        return token, None


def _instrument(framework: str, provider: Any) -> None:
    if framework == FRAMEWORK_LANGGRAPH:
        try:
            from openinference.instrumentation.langchain import LangChainInstrumentor

            LangChainInstrumentor().instrument(tracer_provider=provider)
        except ImportError:
            log.warning(
                "framework='langgraph' but openinference-instrumentation-langchain "
                "is not installed; spans from the framework will be missing. "
                "Install hopsworks-agent-protocol[langgraph].",
            )
    elif framework == FRAMEWORK_LLAMAINDEX:
        try:
            from openinference.instrumentation.llama_index import LlamaIndexInstrumentor

            LlamaIndexInstrumentor().instrument(tracer_provider=provider)
        except ImportError:
            log.warning(
                "framework='llamaindex' but openinference-instrumentation-llama-index "
                "is not installed; spans from the framework will be missing. "
                "Install hopsworks-agent-protocol[llamaindex].",
            )
    elif framework == FRAMEWORK_OPENAI_AGENTS:
        try:
            from openinference.instrumentation.openai_agents import (
                OpenAIAgentsInstrumentor,
            )

            OpenAIAgentsInstrumentor().instrument(tracer_provider=provider)
        except ImportError:
            log.warning(
                "framework='openai_agents' but "
                "openinference-instrumentation-openai-agents is not installed; "
                "spans from the framework will be missing. Install "
                "hopsworks-agent-protocol[openai-agents].",
            )
    elif framework == FRAMEWORK_CLAUDE_AGENTS:
        try:
            from openinference.instrumentation.claude_agent_sdk import (
                ClaudeAgentSDKInstrumentor,
            )

            ClaudeAgentSDKInstrumentor().instrument(tracer_provider=provider)
        except ImportError:
            log.warning(
                "framework='claude_agents' but "
                "openinference-instrumentation-claude-agent-sdk is not "
                "installed; spans from the framework will be missing. Install "
                "hopsworks-agent-protocol[claude-agents].",
            )
    # 'custom': provider only — the agent instruments itself

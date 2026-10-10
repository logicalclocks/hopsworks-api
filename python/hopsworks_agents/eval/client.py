"""Calling a deployed agent, and finding the trace it produced.

This is the piece that only exists because the unit of evaluation is a deployed
endpoint rather than a local function. A client-side eval harness gets output
capture and trace correlation for free by being the caller; here both have to
be built, which is what the manifest read and the readiness poll are for.

Ported from ``scripts/stage1_correlation_probe.py``, which is the same logic
written to measure rather than to grade.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from hopsworks_agents.protocol import conventions

from .runner import AgentResponse


if TYPE_CHECKING:
    from collections.abc import Sequence

    from .evaluators import Trace


log = logging.getLogger(__name__)


class HopsworksAgentClient:
    """Calls an agent through the ingress, reads its traces back through the.

    Hopsworks API.
    """

    def __init__(
        self,
        session: Any,
        api_base: str,
        project_id: int,
        project_name: str,
        deployment_id: int,
        agent_url: str | None = None,
        timeout_s: float = 120.0,
    ):
        self._session = session
        self._api = f"{api_base.rstrip('/')}/hopsworks-api/api/project/{project_id}"
        self._deployment_id = deployment_id
        self._project_name = project_name
        self._agent_url = agent_url
        self._timeout_s = timeout_s
        self._manifest: dict[str, Any] | None = None
        self._chat_path = "/v1/chat"

    # ── the agent ───────────────────────────────────────────────────────────

    def _base_url(self) -> str:
        """Where the agent answers, from inside the cluster.

        The deployment's own Kubernetes service, which is what KServe creates and
        what the istio VirtualService routes on. The agent serves its manifest and
        its endpoints at the root of that host — there is no project-and-name path
        prefix, and assuming one produced a 404 that read as a missing agent.

        Built from the service name rather than read off the deployment: this
        cluster's serving DTO carries no addresses at all, only a
        `hopsworksInferencePath` for the KServe verb proxy, which cannot express
        the agent protocol's paths.

        `agent_url` overrides all of it, for reaching a deployment from outside.
        """
        if self._agent_url:
            return self._agent_url.rstrip("/")
        deployment = self._session.get(
            f"{self._api}/serving/{self._deployment_id}", timeout=60
        ).json()
        name = deployment.get("name")
        if not name:
            raise RuntimeError(
                f"deployment {self._deployment_id} reports no name, so there is no "
                "address to send trials to"
            )
        # The namespace a project's deployments live in, which is not always the
        # project name.
        namespace = deployment.get("projectNamespace") or self._project_name
        self._agent_url = f"http://{name}.{namespace}.svc.cluster.local"
        return self._agent_url

    def manifest(self) -> dict[str, Any]:
        """The agent's self-description, which is what the runner refuses on.

        Fetched rather than assumed: an agent on an SDK too old to continue a
        trace context does not report the capability, and running against it
        would write trial rows pointing at traces that were never created.
        """
        if self._manifest is None:
            response = self._session.get(
                f"{self._base_url()}/.well-known/hopsworks-agent.json", timeout=60
            )
            response.raise_for_status()
            self._manifest = response.json()
            endpoints = self._manifest.get("endpoints", {})
            self._chat_path = endpoints.get("chat", "/v1/chat")
        return self._manifest

    def call(
        self,
        prompt: str,
        *,
        traceparent: str,
        baggage: str,
        timeout_s: float,
        conversation_id: str | None = None,
    ) -> AgentResponse:
        import time

        # The payload is the hopsworks-agent protocol's, taken from the manifest
        # rather than configured per deployment: the SDK exists precisely so
        # every agent speaks one wire format regardless of its framework.
        payload: dict = {
            "message": {"role": "user", "content": [{"type": "text", "text": prompt}]}
        }
        # Threaded so the agent's own memory sees the turns as one conversation.
        # Without it every turn is a first turn, and a suite testing whether the
        # agent remembers what it was told would be testing nothing.
        if conversation_id:
            payload["conversation_id"] = conversation_id
        started = time.monotonic()
        try:
            response = self._session.post(
                f"{self._base_url()}{self._chat_path}",
                json=payload,
                headers={"traceparent": traceparent, "baggage": baggage},
                timeout=timeout_s,
            )
        except Exception as err:  # noqa: BLE001 — classified by the runner
            return AgentResponse(
                text="", latency_ms=(time.monotonic() - started) * 1000, error=str(err)
            )

        latency_ms = (time.monotonic() - started) * 1000
        if response.status_code >= 400:
            return AgentResponse(
                text="",
                latency_ms=latency_ms,
                error=f"agent returned {response.status_code}: {response.text[:200]}",
            )

        body = response.json()
        text = "".join(
            part.get("text", "")
            for part in (body.get("message") or {}).get("content", [])
            if part.get("type") == "text"
        )
        metadata = body.get("metadata") or {}
        return AgentResponse(
            text=text,
            # The SDK reports the trace id it actually used. The runner already
            # knows it from the traceparent it sent; reading it back is what
            # catches an agent that ignored the header rather than adopting it.
            trace_id=metadata.get("trace_id", ""),
            blocked_by_guardrail=bool(metadata.get("guardrail_blocked")),
            latency_ms=latency_ms,
            error="" if body.get("status") != "failed" else "agent reported failure",
        )

    # ── the trace ───────────────────────────────────────────────────────────

    def fetch_trace(self, trace_id: str) -> Trace | None:
        """The trace as a evaluator needs it: the aggregate plus what tools ran.

        Read through the backend rather than the feature store directly, so the
        runner needs no feature store credentials for grading and sees exactly
        what the UI sees.
        """
        response = self._session.get(
            f"{self._api}/otel/servings/{self._deployment_id}/traces/{trace_id}",
            timeout=60,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        trace = response.json()

        spans = trace.get("spans") or []
        if not spans:
            return None
        attributes = trace.get("spanAttributes") or []

        by_span: dict[str, dict[str, str]] = {}
        for attribute in attributes:
            span_id, key = attribute.get("spanId"), attribute.get("attrKey")
            if span_id and key:
                by_span.setdefault(span_id, {})[key] = attribute.get("attrValue") or ""

        def first(attrs: dict[str, str], keys: Sequence[str]) -> str:
            for key in keys:
                if attrs.get(key):
                    return attrs[key]
            return ""

        def span_kind(attrs: dict[str, str]) -> str:
            kind = attrs.get(conventions.SPAN_KIND, "").upper()
            if kind:
                return kind
            operation = attrs.get(conventions.GEN_AI_OPERATION_NAME, "").lower()
            return {
                conventions.OPERATION_CHAT: conventions.SPAN_KIND_LLM,
                conventions.OPERATION_EXECUTE_TOOL: conventions.SPAN_KIND_TOOL,
                conventions.OPERATION_INVOKE_AGENT: conventions.SPAN_KIND_AGENT,
            }.get(operation, "")

        def token_count(attrs: dict[str, str], keys: Sequence[str]) -> int:
            for key in keys:
                raw = attrs.get(key)
                if raw is None or raw == "":
                    continue
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    try:
                        return round(float(raw))
                    except (TypeError, ValueError):
                        continue
            return 0

        tool_spans = sorted(
            (
                s
                for s in spans
                if span_kind(by_span.get(s.get("spanId"), {}))
                == conventions.SPAN_KIND_TOOL
            ),
            key=lambda s: s.get("startTimeNs") or 0,
        )

        token_spans = [
            s
            for s in spans
            if span_kind(by_span.get(s.get("spanId"), {})) == conventions.SPAN_KIND_LLM
            or (
                span_kind(by_span.get(s.get("spanId"), {}))
                == conventions.SPAN_KIND_AGENT
                and (
                    token_count(
                        by_span.get(s.get("spanId"), {}),
                        conventions.INPUT_TOKEN_KEYS,
                    )
                    or token_count(
                        by_span.get(s.get("spanId"), {}),
                        conventions.OUTPUT_TOKEN_KEYS,
                    )
                )
            )
        ]

        def tokens(keys: Sequence[str]) -> int:
            return sum(
                token_count(by_span.get(span.get("spanId"), {}), keys)
                for span in token_spans
            )

        tool_calls = []
        for span in tool_spans:
            attrs = by_span.get(span.get("spanId"), {})
            start = span.get("startTimeNs") or 0
            end = span.get("endTimeNs") or 0
            tool_calls.append(
                {
                    "span_id": span.get("spanId") or "",
                    "parent_span_id": span.get("parentSpanId") or "",
                    "name": first(
                        attrs, (conventions.TOOL_NAME, conventions.GEN_AI_TOOL_NAME)
                    )
                    or span.get("name")
                    or "",
                    "call_id": attrs.get(conventions.GEN_AI_TOOL_CALL_ID, ""),
                    "arguments": first(attrs, conventions.TOOL_ARGUMENT_KEYS),
                    "result": first(attrs, conventions.TOOL_RESULT_KEYS),
                    "status": str(span.get("statusCode") or ""),
                    # None rather than 0 when a span is missing an end time: a tool
                    # that "took 0ms" and one that was never closed are different
                    # facts, and a latency budget must not pass on the second.
                    "duration_ms": (end - start) / 1e6 if end and start else None,
                    "start_time_ns": start,
                }
            )

        return {
            "trace_id": trace_id,
            "root_span_id": next(
                (s.get("spanId") for s in spans if not s.get("parentSpanId")), ""
            ),
            # The full ordered sequence, repeats included. `agent_trace_features`
            # stores distinct names instead, which is why retry and
            # unnecessary-call analysis reads tool_calls and not that row.
            "tool_names": [call["name"] for call in tool_calls],
            "tool_calls": tool_calls,
            "tool_error_count": sum(
                1 for call in tool_calls if call["status"].endswith("ERROR")
            ),
            "span_count": len(spans),
            "input_tokens": tokens(conventions.INPUT_TOKEN_KEYS),
            "output_tokens": tokens(conventions.OUTPUT_TOKEN_KEYS),
        }

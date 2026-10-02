"""What the API returns, as objects with the API's own field names in snake case.

Each model keeps ``raw``, the dict as it arrived, so a field this module does
not name is still reachable; the named fields are the ones a script reaches for.
Timestamps stay as the API sends them (ISO-8601 text, or epoch milliseconds for
windows), with ``as_datetime`` for the conversion when one is wanted.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import Any, TypeVar

from typing_extensions import Self


T = TypeVar("T", bound="ApiModel")

_CAMEL = re.compile(r"_([a-z])")


def to_camel(name: str) -> str:
    return _CAMEL.sub(lambda m: m.group(1).upper(), name)


def as_datetime(value: Any) -> datetime | None:
    """A timestamp as the API sends it -- ISO text or epoch milliseconds -- as a datetime, UTC."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def as_json(value: Any, default: Any) -> Any:
    """A JSON-as-text column parsed, or the default when empty or malformed."""
    if value in (None, ""):
        return default
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except ValueError:
        return default


@dataclass
class ApiModel:
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def from_api(cls, data: dict[str, Any] | None) -> Self:
        data = data or {}
        values: dict[str, Any] = {}
        for f in fields(cls):
            if f.name == "raw" or f.name.startswith("_"):
                continue
            key = to_camel(f.name)
            if key in data:
                values[f.name] = data[key]
            elif f.name in data:
                values[f.name] = data[f.name]
        model = cls(**values)  # type: ignore[arg-type]
        model.raw = dict(data)
        return model

    @classmethod
    def list_from_api(cls, data: Any) -> list[Self]:
        items = data.get("items") if isinstance(data, dict) else data
        return [cls.from_api(item) for item in (items or [])]


# ── evaluation ─────────────────────────────────────────────────────────────


@dataclass
class Check(ApiModel):
    """One check a suite grades by: a type, the name results are reported under, its config."""

    type: str = ""
    name: str = ""
    position: int | None = None
    config: str = ""

    @property
    def settings(self) -> dict[str, Any]:
        return as_json(self.config, {})


@dataclass
class Suite(ApiModel):
    suite_id: str = ""
    version: int = 1
    name: str = ""
    tags: str = ""
    execution_mode: str = "read_only"
    blocks_are_success: bool = False
    run_on_update: bool = False
    gate_metric: str | None = None
    gate_threshold: float | None = None
    description: str = ""
    status: str = "DRAFT"
    evaluators: list[dict[str, Any]] = field(default_factory=list)
    pass_policy: str = "all"
    pass_threshold: float | None = None
    created_by: str | None = None
    created_at: str | None = None
    task_count: int | None = None

    @property
    def tag_list(self) -> list[str]:
        return [str(t) for t in as_json(self.tags, [])]

    @property
    def checks(self) -> list[Check]:
        return [Check.from_api(e) for e in self.evaluators or []]

    @property
    def published(self) -> bool:
        return self.status == "PUBLISHED"


@dataclass
class EvaluatorTemplate(ApiModel):
    """A saved, named set of checks in the project's library."""

    template_id: str = ""
    name: str = ""
    description: str = ""
    spec: str = "[]"
    created_by: str | None = None
    created_at: str | None = None
    used_in: list[dict[str, Any]] = field(default_factory=list)

    @property
    def checks(self) -> list[dict[str, Any]]:
        return as_json(self.spec, [])


@dataclass
class Task(ApiModel):
    task_id: str = ""
    version: int = 1
    suite_id: str | None = None
    suite_version: int | None = None
    task_type: str = "single_turn"
    input_messages: str = ""
    expectations: dict[str, str] = field(default_factory=dict)
    source_trace_id: str | None = None
    source_deployment_id: int | None = None
    redaction_status: str = "NOT_REQUIRED"
    reviewed_by: str | None = None
    reviewed_at: str | None = None
    category: str | None = None
    created_by: str | None = None
    created_at: str | None = None

    @property
    def messages(self) -> list[dict[str, Any]]:
        return as_json(self.input_messages, [])

    @property
    def question(self) -> str:
        """The user's first turn, which is what most tasks are."""
        for message in self.messages:
            if message.get("role") == "user":
                return str(message.get("content") or "")
        return ""

    @property
    def needs_review(self) -> bool:
        return self.redaction_status == "PENDING_REDACTION"


@dataclass
class Run(ApiModel):
    run_id: str = ""
    run_type: str = "SUITE"
    suite_id: str | None = None
    suite_version: int | None = None
    sample_evaluators: str | None = None
    sample_source: str | None = None
    sample_from: int | None = None
    sample_to: int | None = None
    deployment_id: int | None = None
    status: str = "PENDING"
    n_trials: int = 1
    execution_id: int | None = None
    job_name: str | None = None
    error_message: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    created_by: str | None = None
    created_at: str | None = None

    @property
    def finished(self) -> bool:
        return self.status in ("SUCCEEDED", "FAILED", "CANCELLED", "KILLED")

    @property
    def succeeded(self) -> bool:
        return self.status == "SUCCEEDED"


@dataclass
class Trial(ApiModel):
    run_id: str = ""
    trial_id: str = ""
    task_id: str = ""
    trial_index: int = 0
    trace_id: str | None = None
    trace_status: str | None = None
    status: str = ""
    latency_ms: float | None = None
    final_output: str | None = None
    error_type: str | None = None
    error_message: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == "PASSED"


@dataclass
class EvaluatorResult(ApiModel):
    trial_id: str = ""
    evaluator_name: str = ""
    evaluator_type: str = ""
    score: float | None = None
    passed: bool | None = None
    ungradable: bool = False
    reason: str | None = None
    assertions_json: str | None = None
    judge_model: str | None = None

    @property
    def assertions(self) -> Any:
        return as_json(self.assertions_json, None)


@dataclass
class RunMetric(ApiModel):
    run_id: str = ""
    metric_scope: str = "run"
    metric_scope_value: str | None = None
    metric_name: str = ""
    metric_value: float = 0.0
    task_count: int | None = None
    trial_count: int | None = None


@dataclass
class EvalJob(ApiModel):
    """The job that evaluates one deployment: what it runs, how it is sized, where it runs."""

    name: str = ""
    id: int | None = None
    deployment_id: int | None = None
    job_type: str | None = None
    creator: str | None = None
    environment_name: str | None = None
    cores: int | None = None
    memory: int | None = None
    gpus: int | None = None
    suites: list[str] = field(default_factory=list)
    evaluators: list[str] = field(default_factory=list)
    monitor: bool = False
    created: bool = False
    exists: bool = False
    environments: list[str] = field(default_factory=list)


@dataclass
class ReviewJob(ApiModel):
    """The job that analyses one deployment's failures with a model."""

    name: str = ""
    id: int | None = None
    deployment_id: int | None = None
    job_type: str | None = None
    creator: str | None = None
    environment_name: str | None = None
    cores: int | None = None
    memory: int | None = None
    provider: str = "anthropic"
    model: str = ""
    reasoning_effort: str = ""
    api_key_env: str = ""
    base_url: str = ""
    headers: str = ""
    budget_calls: int = 200
    context_turns: int = 20
    auto_promote: bool = False
    auto_promote_min_cluster: int = 3
    auto_promote_min_confidence: float = 0.8
    auto_promote_min_acceptance: float = 0.85
    auto_promote_min_decisions: int = 50
    sources: str = "feedback,errors,judge"
    read_source_code: bool = True
    source_location: str = ""
    created: bool = False
    exists: bool = False
    environments: list[str] = field(default_factory=list)
    next_window_from: int | None = None

    @property
    def source_list(self) -> list[str]:
        return [s.strip() for s in (self.sources or "").split(",") if s.strip()]


@dataclass
class RegressionSuite(ApiModel):
    exists: bool = False
    suite_id: str | None = None
    name: str = ""
    version: int | None = None
    status: str | None = None
    task_count: int = 0
    published_version: int | None = None
    published_task_count: int = 0
    unpublished_task_count: int = 0
    judge_check: str | None = None


@dataclass
class GateCheck(ApiModel):
    suite_id: str | None = None
    suite_name: str | None = None
    suite_type: str | None = None
    run_id: str | None = None
    value: float | None = None
    threshold: float | None = None
    metric_name: str | None = None
    passed: bool = False
    detail: str | None = None


@dataclass
class GateResult(ApiModel):
    passed: bool = True
    has_evidence: bool = False
    checks: list[dict[str, Any]] = field(default_factory=list)

    @property
    def gate_checks(self) -> list[GateCheck]:
        return [GateCheck.from_api(c) for c in self.checks or []]


# ── tracing ────────────────────────────────────────────────────────────────


@dataclass
class ToolEvent(ApiModel):
    """A step the agent reported while answering: a tool call, a retrieval, a code run."""

    id: str | None = None
    name: str = ""
    status: str = "running"
    message: str | None = None
    data: Any = None

    @property
    def done(self) -> bool:
        return self.status in ("done", "failed")

    @property
    def failed(self) -> bool:
        return self.status == "failed"


@dataclass
class ChatReply(ApiModel):
    """The agent's answer to one `Agent.chat()` message, in the agent protocol's shape."""

    id: str = ""
    conversation_id: str = ""
    message: dict[str, Any] = field(default_factory=dict)
    citations: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, int] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    status: str = "completed"

    @property
    def parts(self) -> list[dict[str, Any]]:
        """The reply's content parts: text, and any image, file or audio the agent returned."""
        return list(self.message.get("content") or [])

    @property
    def text(self) -> str:
        """The reply as plain text: its text parts joined."""
        return "".join(
            str(part.get("text") or "")
            for part in self.parts
            if part.get("type") == "text"
        )

    @property
    def trace_id(self) -> str | None:
        """The trace the agent recorded for this turn; what feedback and evaluation refer to."""
        value = self.metadata.get("trace_id")
        return str(value) if value else None

    @property
    def failed(self) -> bool:
        return self.status == "failed"

    @property
    def tool_events(self) -> list[ToolEvent]:
        """The steps the agent reported for this turn, as its metadata carries them."""
        return ToolEvent.list_from_api(self.metadata.get("tool_events") or [])


@dataclass
class TraceSummary(ApiModel):
    """One trace as the list shows it: its root span, with the conversation joined on."""

    trace_id: str = ""
    span_id: str = ""
    deployment_id: int | None = None
    name: str = ""
    kind: str | None = None
    start_time_ns: int = 0
    end_time_ns: int | None = None
    status_code: str | None = None
    status_message: str | None = None
    session_id: str | None = None
    user_id: str | None = None
    subject: str | None = None
    subject_source: str | None = None
    messages: str | None = None
    metadata: str | None = None
    tags: str | None = None

    @property
    def started_at(self) -> datetime | None:
        return (
            as_datetime(self.start_time_ns / 1_000_000) if self.start_time_ns else None
        )

    @property
    def latency_ms(self) -> float | None:
        if self.end_time_ns and self.start_time_ns:
            return (self.end_time_ns - self.start_time_ns) / 1_000_000
        return None

    @property
    def conversation(self) -> list[dict[str, Any]]:
        return as_json(self.messages, [])

    @property
    def failed(self) -> bool:
        return self.status_code == "STATUS_CODE_ERROR"


@dataclass
class Trace(ApiModel):
    """A whole trace: spans, their attributes and events, and the totals."""

    spans: list[dict[str, Any]] = field(default_factory=list)
    span_attributes: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)
    event_attributes: list[dict[str, Any]] = field(default_factory=list)
    total_input_tokens: int | None = None
    total_output_tokens: int | None = None
    total_input_cost: float | None = None
    total_output_cost: float | None = None

    @property
    def trace_id(self) -> str:
        return str(self.spans[0].get("traceId") or "") if self.spans else ""

    @property
    def root(self) -> dict[str, Any] | None:
        return next(
            (s for s in self.spans if not s.get("parentSpanId")),
            self.spans[0] if self.spans else None,
        )

    def attributes_of(self, span_id: str) -> dict[str, str]:
        return {
            a.get("attrKey", ""): a.get("attrValue", "")
            for a in self.span_attributes
            if a.get("spanId") == span_id
        }

    @property
    def tool_calls(self) -> list[dict[str, Any]]:
        """The tool spans, in order, with name, arguments and result read off their attributes."""
        calls = []
        for span in sorted(self.spans, key=lambda s: s.get("startTimeNs") or 0):
            attrs = self.attributes_of(span.get("spanId", ""))
            kind = (attrs.get("openinference.span.kind") or "").upper()
            if kind != "TOOL" and attrs.get("gen_ai.operation.name") != "execute_tool":
                continue
            calls.append(
                {
                    "name": attrs.get("tool.name")
                    or attrs.get("gen_ai.tool.name")
                    or span.get("name"),
                    "arguments": attrs.get("input.value")
                    or attrs.get("gen_ai.tool.call.arguments")
                    or "",
                    "result": attrs.get("output.value")
                    or attrs.get("gen_ai.tool.call.result")
                    or "",
                    "status": span.get("statusCode") or "",
                    "span_id": span.get("spanId"),
                }
            )
        return calls


@dataclass
class Feedback(ApiModel):
    feedback_id: str = ""
    deployment_id: int | None = None
    trace_id: str = ""
    session_id: str | None = None
    reviewer: str | None = None
    verdict: str = "negative"
    issue_category: str | None = None
    corrected_answer: str | None = None
    expected_tool_behavior: str | None = None
    note: str | None = None
    promoted_task_id: str | None = None
    created_at: str | None = None
    trace_messages: str | None = None

    @property
    def source(self) -> str:
        """Who gave it: a reviewer, an end user of the agent, a detector, or an online judge.

        ``human`` is a reviewer in Hopsworks; ``end_user`` someone talking to the agent
        through its feedback endpoint; ``detector`` the platform reading the trace; ``judge``
        an online evaluator.
        """
        reviewer = self.reviewer or ""
        if reviewer.startswith("detector:"):
            return "detector"
        if reviewer.startswith("judge:"):
            return "judge"
        if reviewer.startswith("user:"):
            return "end_user"
        return "human"


@dataclass
class FeedbackPage(ApiModel):
    count: int = 0
    items: list[dict[str, Any]] = field(default_factory=list)

    @property
    def feedback(self) -> list[Feedback]:
        return [Feedback.from_api(f) for f in self.items or []]


@dataclass
class FeedbackSummary(ApiModel):
    from_ms: int | None = None
    to_ms: int | None = None
    window_ms: int | None = None
    positive: int = 0
    negative: int = 0
    false_alarm: int = 0
    issue_categories: dict[str, int] = field(default_factory=dict)
    windows: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_api(cls, data: dict[str, Any] | None) -> FeedbackSummary:
        data = dict(data or {})
        # "from" and "to" are keywords; the model names them with a unit
        data.setdefault("fromMs", data.get("from"))
        data.setdefault("toMs", data.get("to"))
        return super().from_api(data)  # type: ignore[return-value]


@dataclass
class Triage(ApiModel):
    """What the analysis model proposed about one verdict."""

    triage_id: str = ""
    feedback_id: str = ""
    deployment_id: int | None = None
    trace_id: str = ""
    session_id: str | None = None
    run_id: str | None = None
    ungradable: bool = False
    error: str | None = None
    category: str | None = None
    severity: str | None = None
    failure_summary: str | None = None
    failure_signature: str | None = None
    correction_status: str | None = None
    correction_grounding: str | None = None
    normalized_correction: str | None = None
    proposed_rubric: str | None = None
    proposed_expected_tool_behavior: str | None = None
    proposed_assertions: str | None = None
    redaction_findings: str | None = None
    needs_human: bool = False
    confidence: float | None = None
    provider: str | None = None
    model: str | None = None
    prompt_version: str | None = None
    cluster_id: str | None = None
    human_decision: str | None = None
    human_category: str | None = None
    decided_by: str | None = None
    decided_at: str | None = None
    created_at: str | None = None
    suspected_code_bug: bool = False
    code_findings: str | None = None

    @property
    def assertions(self) -> list[dict[str, Any]]:
        return as_json(self.proposed_assertions, [])

    @property
    def findings(self) -> list[dict[str, Any]]:
        """Where in the agent's code the model pointed: file, line, finding, fix, and the patch."""
        return as_json(self.code_findings, [])

    @property
    def decided(self) -> bool:
        return bool(self.human_decision and self.human_decision != "pending")


@dataclass
class Cluster(ApiModel):
    """One recurring failure across many verdicts."""

    cluster_id: str = ""
    deployment_id: int | None = None
    signature: str = ""
    label: str = ""
    severity: str | None = None
    size: int = 0
    first_seen: str | None = None
    last_seen: str | None = None
    representative_trace_id: str | None = None
    representative_feedback_id: str | None = None
    status: str = "open"
    promoted_task_id: str | None = None
    dismiss_reason: str | None = None
    decided_by: str | None = None
    decided_at: str | None = None
    updated_at: str | None = None


@dataclass
class Calibration(ApiModel):
    """How often reviewers agreed with the analysis model, overall and per category."""

    decisions: int = 0
    accepted: int = 0
    acceptance_rate: float = 0.0
    categories: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TraceMetric(ApiModel):
    deployment_id: int | None = None
    window_start: int = 0
    window_end: int = 0
    trace_count: int = 0
    trace_error_count: int = 0
    trace_latency_p50_ms: float | None = None
    trace_latency_p99_ms: float | None = None


@dataclass
class LlmMetric(ApiModel):
    deployment_id: int | None = None
    model_name: str | None = None
    window_start: int = 0
    window_end: int = 0
    llm_call_count: int = 0
    llm_error_count: int = 0
    llm_latency_p50_ms: float | None = None
    llm_latency_p99_ms: float | None = None
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    estimated_cost_usd: float | None = None


@dataclass
class ToolMetric(ApiModel):
    deployment_id: int | None = None
    tool_name: str | None = None
    window_start: int = 0
    window_end: int = 0
    tool_call_count: int = 0
    tool_error_count: int = 0
    tool_latency_p50_ms: float | None = None
    tool_latency_p99_ms: float | None = None

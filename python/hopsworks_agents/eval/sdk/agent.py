"""One deployed agent: talking to it, and everything Hopsworks recorded about it.

What the chat panel, the Trace Summaries card, the Feedback tab, the failure
analysis and the monitoring dashboards do for one deployment, as methods on an
agent handle. The handle is fetched through :meth:`AgentServing.get_agent`.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public

from ._transport import AgentServingError
from .evals import epoch_ms
from .models import (
    Calibration,
    ChatReply,
    Cluster,
    Feedback,
    FeedbackPage,
    FeedbackSummary,
    GateResult,
    LlmMetric,
    RegressionSuite,
    Run,
    ToolEvent,
    ToolMetric,
    Trace,
    TraceMetric,
    TraceSummary,
    Triage,
)


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from .client import AgentServing
    from .models import EvalJob, ReviewJob, Task

VERDICTS = ("positive", "negative", "false_alarm")
MANIFEST_PATH = ".well-known/hopsworks-agent.json"
DEFAULT_CHAT_PATH = "/v1/chat"
DEFAULT_STREAM_PATH = "/v1/chat/stream"


def is_agent(serving: dict[str, Any]) -> bool:
    """Whether a serving row is an agent: a Python server with no model artifact behind it."""
    return str(serving.get("modelServer") or "").upper() == "PYTHON" and not (
        serving.get("modelName") or serving.get("modelPath")
    )


@public
class Agent:
    """One agent deployment: send it messages, read its traces and feedback, evaluate it.

    Fetched with `agent_serving.get_agent(name_or_id)`; never constructed directly.
    """

    def __init__(
        self,
        client: AgentServing,
        deployment_id: int,
        *,
        serving: dict[str, Any] | None = None,
    ):
        self._client = client
        self._http = client.http
        self.id = int(deployment_id)
        self._serving = dict(serving or {})
        self._otel = f"/otel/servings/{self.id}"
        self._endpoints: dict[str, str] | None = None
        self._deployment: Any = None

    def __repr__(self) -> str:
        return f"Agent({self.id}, name={self.name!r})"

    # ── what the deployment is ─────────────────────────────────────────────

    @property
    def serving(self) -> dict[str, Any]:
        """The deployment as the serving API describes it."""
        if not self._serving:
            self._serving = dict(self._http.get(f"/serving/{self.id}") or {})
        return self._serving

    @public
    @property
    def name(self) -> str:
        """Name of the agent deployment."""
        return str(self.serving.get("name") or "")

    @property
    def namespace(self) -> str:
        """The Kubernetes namespace the agent runs in; what the gateway routes on."""
        return str(self.serving.get("projectNamespace") or "")

    @public
    @property
    def url(self) -> str:
        """Base URL of the agent behind the inference gateway, e.g. `https://gw/v1/ns/name`."""
        return f"{self._http.gateway_url}/v1/{self.namespace}/{self.name}"

    @public
    def refresh(self) -> Agent:
        """Re-read the deployment from the serving API."""
        self._serving = {}
        self._deployment = None
        _ = self.serving
        return self

    # ── talking to it ──────────────────────────────────────────────────────

    @public
    def manifest(self) -> dict[str, Any]:
        """The agent's self-description: its protocol version, endpoints and capabilities."""
        manifest = self._agent_get(MANIFEST_PATH) or {}
        self._endpoints = dict(manifest.get("endpoints") or {})
        return manifest

    @public
    def chat(
        self,
        text: str,
        *,
        conversation_id: str | None = None,
        subject: str | None = None,
        context: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        timeout: float = 120.0,
    ) -> ChatReply:
        """Send the agent one message and get its reply.

        Example:
            ```python
            reply = agent.chat("Where is order 42?")
            print(reply.text)
            follow_up = agent.chat("And order 43?", conversation_id=reply.conversation_id)
            ```

        Parameters:
            text: The user's message.
            conversation_id: Thread this message onto an earlier reply's conversation, so the agent's own memory sees one conversation. A new conversation when omitted.
            subject: Who the end user is, for agents that keep per-user memory.
            context: Extra request context the agent reads.
            metadata: Extra request metadata the agent reads.
            timeout: Seconds to wait for the reply.

        Returns:
            `ChatReply`: the reply, with its text, its conversation id and the trace id the agent recorded.

        Raises:
            `AgentServingError`: If the agent or the gateway refuses the request.
        """
        payload = _chat_payload(text, conversation_id, subject, context, metadata)
        body = self._http.agent_request(
            "POST",
            self._segments(self._endpoint("chat", DEFAULT_CHAT_PATH)),
            json=payload,
            timeout=timeout,
        )
        return ChatReply.from_api(body)

    @public
    def chat_stream(
        self,
        text: str,
        *,
        conversation_id: str | None = None,
        subject: str | None = None,
        context: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        timeout: float = 120.0,
    ) -> ChatStream:
        """Send the agent one message and read its reply as it is produced.

        The stream is the protocol's server-sent events: text deltas, the steps
        the agent reports while working (tool calls, retrievals, code runs) and
        the completed reply. Iterating the stream yields the text deltas;
        `events()` yields every frame; `reply` is the completed `ChatReply`,
        reading the rest of the stream first if needed.

        Example:
            ```python
            stream = agent.chat_stream("Where is order 42?")
            for delta in stream:
                print(delta, end="", flush=True)
            print(stream.reply.trace_id)

            for frame in agent.chat_stream("Cancel it").events():
                if frame.tool:
                    print(frame.tool.name, frame.tool.status)
            ```

        Parameters:
            text: The user's message.
            conversation_id: Thread this message onto an earlier reply's conversation.
            subject: Who the end user is, for agents that keep per-user memory.
            context: Extra request context the agent reads.
            metadata: Extra request metadata the agent reads.
            timeout: Seconds to wait for each piece of the reply.

        Returns:
            `ChatStream`: the reply as it arrives.

        Raises:
            `AgentServingError`: If the gateway refuses the request, or, while reading, if the agent reports an error.
        """
        payload = _chat_payload(text, conversation_id, subject, context, metadata)
        lines = self._http.agent_request(
            "POST",
            self._segments(self._endpoint("stream", DEFAULT_STREAM_PATH)),
            json=payload,
            timeout=timeout,
            stream=True,
        )
        return ChatStream(lines)

    def _endpoint(self, name: str, default: str) -> str:
        """The agent's route for ``name``, from its manifest; the protocol's default without one."""
        if self._endpoints is None:
            # not a protocol agent, or the manifest is not served: the
            # protocol's default routes are the best guess there is
            with contextlib.suppress(AgentServingError):
                self.manifest()
            self._endpoints = dict(self._endpoints or {})
        return str(self._endpoints.get(name) or default)

    def _agent_get(self, path: str) -> Any:
        return self._http.agent_request("GET", self._segments(path))

    def _segments(self, path: str) -> list[str]:
        return ["v1", self.namespace, self.name, *[p for p in path.split("/") if p]]

    # ── the deployment's lifecycle, through model serving ──────────────────

    @public
    @property
    def deployment(self) -> Any:
        """The same deployment as model serving sees it (`hsml.deployment.Deployment`).

        For what only model serving does: resources, scaling, logs, describe.
        """
        if self._deployment is None:
            from hopsworks_common import client as hopsworks_client  # noqa: PLC0415
            from hsml.deployment import Deployment  # noqa: PLC0415

            try:
                instance = hopsworks_client._get_instance()
            except Exception as err:  # noqa: BLE001 -- the client's own "not connected"
                raise AgentServingError(
                    "the deployment handle needs hopsworks.login(); this client "
                    "is not connected"
                ) from err
            deployment = Deployment.from_response_json(dict(self.serving))
            deployment.model_registry_id = instance._project_id
            deployment.project_name = instance._project_name
            self._deployment = deployment
        return self._deployment

    @public
    def start(self, await_running: int | None = 600) -> None:
        """Start the agent. Waits up to `await_running` seconds for it to be running."""
        self.deployment.start(await_running=await_running)

    @public
    def stop(self, await_stopped: int | None = 600) -> None:
        """Stop the agent. Waits up to `await_stopped` seconds for it to be stopped."""
        self.deployment.stop(await_stopped=await_stopped)

    @public
    def restart(
        self, await_stopped: int | None = 600, await_running: int | None = 600
    ) -> None:
        """Restart the agent, rolling it onto the code it was last deployed with."""
        self.deployment.restart(
            await_stopped=await_stopped, await_running=await_running
        )

    @public
    def is_running(self) -> bool:
        """Whether the agent is running (or idle) and can take messages."""
        return bool(self.deployment.is_running())

    @public
    def delete(self, force: bool = False) -> None:
        """Delete the agent deployment. `force=True` deletes it while it is running."""
        self.deployment.delete(force=force)

    @public
    def get_logs(self, component: str = "predictor", tail: int = 10) -> Any:
        """The agent's recent log lines, as model serving returns them."""
        return self.deployment.get_logs(component=component, tail=tail)

    # ── traces ─────────────────────────────────────────────────────────────
    def traces(
        self,
        *,
        limit: int = 10,
        search: str | None = None,
        search_field: str | None = None,
        exclude_sessions: Sequence[str] = (),
    ) -> list[TraceSummary]:
        """Recent traces, one per conversation unless searching.

        ``search_field`` is ``messages`` (default), ``subject`` (exact), ``session_id`` or ``trace_id``.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "search": search,
            "searchField": search_field,
        }
        if exclude_sessions:
            params["excludeSessions"] = list(exclude_sessions)
        return TraceSummary.list_from_api(
            self._http.get(f"{self._otel}/traces", **params)
        )

    def trace(self, trace_id: str) -> Trace:
        """A whole trace: spans, attributes, events and totals."""
        return Trace.from_api(self._http.get(f"{self._otel}/traces/{trace_id}"))

    def session(self, session_id: str, *, limit: int = -1) -> list[TraceSummary]:
        """Every trace of a conversation, oldest first."""
        rows = TraceSummary.list_from_api(
            self._http.get(f"{self._otel}/traces/sessions/{session_id}", limit=limit)
        )
        return sorted(rows, key=lambda t: t.start_time_ns)

    def conversation(self, session_id: str) -> list[dict[str, str]]:
        """The conversation as the user had it: one user and one assistant message per turn."""
        turns: list[dict[str, str]] = []
        previous: list[dict[str, Any]] = []
        for trace in self.session(session_id):
            messages = trace.conversation
            seen = {m.get("content") for m in previous if m.get("role") == "user"}
            users = [m for m in messages if m.get("role") == "user"]
            new_user = next(
                (m for m in users if m.get("content") not in seen),
                users[-1] if users else None,
            )
            if new_user is None:
                previous = messages
                continue
            after = messages[messages.index(new_user) + 1 :]
            answer = "".join(
                str(m.get("content") or "")
                for m in after
                if m.get("role") == "assistant"
            )
            turns.append(
                {
                    "trace_id": trace.trace_id,
                    "user": str(new_user.get("content") or ""),
                    "assistant": answer,
                }
            )
            previous = messages
        return turns

    # ── feedback ───────────────────────────────────────────────────────────

    def feedback(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        verdict: str | None = None,
        reviewer: str | None = None,
        search: str | None = None,
        session_id: str | None = None,
        trace_id: str | None = None,
        feedback_id: str | None = None,
    ) -> FeedbackPage:
        """A page of feedback, newest first. ``verdict="negative"`` is everything that needs attention."""
        return FeedbackPage.from_api(
            self._http.get(
                f"{self._otel}/feedback",
                limit=limit,
                offset=offset,
                verdict=verdict,
                reviewer=reviewer,
                search=search,
                sessionId=session_id,
                traceId=trace_id,
                feedbackId=feedback_id,
            )
        )

    def all_feedback(self, **filters: Any) -> list[Feedback]:
        """Every matching feedback row, walking the pages."""
        rows: list[Feedback] = []
        offset = 0
        page_size = int(filters.pop("limit", 100))
        while True:
            page = self.feedback(limit=page_size, offset=offset, **filters)
            rows.extend(page.feedback)
            offset += len(page.items)
            if not page.items or offset >= page.count:
                return rows

    def trace_feedback(self, trace_id: str) -> list[Feedback]:
        return Feedback.list_from_api(
            self._http.get(f"{self._otel}/traces/{trace_id}/feedback")
        )

    def give_feedback(
        self,
        trace_id: str,
        verdict: str,
        *,
        issue_category: str | None = None,
        corrected_answer: str | None = None,
        expected_tool_behavior: str | None = None,
        note: str | None = None,
    ) -> Feedback:
        """Your verdict on a trace. One per reviewer per trace: giving it again replaces yours."""
        if verdict not in VERDICTS:
            raise ValueError(f"verdict must be one of {', '.join(VERDICTS)}")
        body = {
            "verdict": verdict,
            "issueCategory": issue_category,
            "correctedAnswer": corrected_answer,
            "expectedToolBehavior": expected_tool_behavior,
            "note": note,
        }
        return Feedback.from_api(
            self._http.post(
                f"{self._otel}/traces/{trace_id}/feedback",
                {k: v for k, v in body.items() if v is not None},
            )
        )

    def retract_feedback(self, trace_id: str) -> None:
        """Take back your own verdict on a trace. Never anyone else's."""
        self._http.delete(f"{self._otel}/traces/{trace_id}/feedback")

    def feedback_summary(
        self,
        *,
        since: Any = None,
        until: Any = None,
        window_ms: int | None = None,
        human_only: bool = True,
    ) -> FeedbackSummary:
        """Verdict counts over a range, by window and by issue category."""
        return FeedbackSummary.from_api(
            self._http.get(
                f"{self._otel}/feedback/summary",
                **{"from": epoch_ms(since), "to": epoch_ms(until)},
                windowMs=window_ms,
                source="human" if human_only else "all",
            )
        )

    def reviewers(self) -> list[str]:
        return list(self._http.get(f"{self._otel}/feedback/reviewers") or [])

    # ── analysis ───────────────────────────────────────────────────────────

    def triage(self, feedback: Sequence[Feedback | str]) -> list[Triage]:
        """The analysis model's latest proposal about each of these feedback rows."""
        ids = [f.feedback_id if isinstance(f, Feedback) else f for f in feedback]
        if not ids:
            return []
        rows = self._http.get(f"{self._otel}/feedback/triage", feedbackId=ids)
        return Triage.list_from_api(rows)

    def decide_triage(
        self, triage: Triage | str, decision: str, *, category: str | None = None
    ) -> None:
        """What you think of a proposal: ``accepted``, ``edited`` or ``rejected``. This is calibration data."""
        triage_id = triage.triage_id if isinstance(triage, Triage) else triage
        self._http.put(
            f"{self._otel}/feedback/triage/{triage_id}/decision",
            decision=decision,
            category=category,
        )

    def calibration(self) -> Calibration:
        return Calibration.from_api(
            self._http.get(f"{self._otel}/feedback/triage/calibration")
        )

    def clusters(self, status: str | None = "open") -> list[Cluster]:
        """Failure clusters, most worth a reviewer's time first. ``status=None`` for all."""
        return Cluster.list_from_api(
            self._http.get(f"{self._otel}/feedback/clusters", status=status)
        )

    def cluster_members(self, cluster: Cluster | str) -> list[Triage]:
        cluster_id = cluster.cluster_id if isinstance(cluster, Cluster) else cluster
        return Triage.list_from_api(
            self._http.get(f"{self._otel}/feedback/clusters/{cluster_id}/members")
        )

    def rename_cluster(self, cluster: Cluster | str, label: str) -> None:
        self._decide_cluster(cluster, label=label)

    def dismiss_cluster(self, cluster: Cluster | str, reason: str) -> None:
        """``working_as_intended``, ``duplicate_of``, ``cannot_reproduce`` or ``out_of_scope``."""
        self._decide_cluster(cluster, status="dismissed", dismissReason=reason)

    def reopen_cluster(self, cluster: Cluster | str) -> None:
        self._decide_cluster(cluster, status="open")

    def mark_cluster_promoted(self, cluster: Cluster | str, task: Task | str) -> None:
        """Record the task a cluster became; every member's feedback is marked covered."""
        task_id = getattr(task, "task_id", task)
        self._decide_cluster(cluster, status="promoted", promotedTaskId=task_id)

    def promote_cluster(
        self, cluster: Cluster, *, expectations: dict[str, str] | None = None
    ) -> Task:
        """Promote the cluster's representative trace to a task and record it on the cluster."""
        if not cluster.representative_trace_id:
            raise ValueError("the cluster has no representative trace")
        if expectations is None:
            expectations = {}
            members = self.cluster_members(cluster)
            representative = next(
                (
                    m
                    for m in members
                    if m.feedback_id == cluster.representative_feedback_id
                ),
                None,
            )
            if representative and representative.normalized_correction:
                expectations = {"expected": representative.normalized_correction}
        task = self._client.tasks.promote(
            self.id, cluster.representative_trace_id, expectations=expectations
        )
        self.mark_cluster_promoted(cluster, task)
        return task

    def _decide_cluster(self, cluster: Cluster | str, **params: Any) -> None:
        cluster_id = cluster.cluster_id if isinstance(cluster, Cluster) else cluster
        self._http.put(f"{self._otel}/feedback/clusters/{cluster_id}", **params)

    # ── metrics ────────────────────────────────────────────────────────────

    def trace_metrics(
        self, *, since: Any = None, until: Any = None
    ) -> list[TraceMetric]:
        return TraceMetric.list_from_api(
            self._http.get(
                f"{self._otel}/metrics/traces",
                **{"from": epoch_ms(since), "to": epoch_ms(until)},
            )
        )

    def llm_metrics(self, *, since: Any = None, until: Any = None) -> list[LlmMetric]:
        return LlmMetric.list_from_api(
            self._http.get(
                f"{self._otel}/metrics/llm",
                **{"from": epoch_ms(since), "to": epoch_ms(until)},
            )
        )

    def tool_metrics(self, *, since: Any = None, until: Any = None) -> list[ToolMetric]:
        return ToolMetric.list_from_api(
            self._http.get(
                f"{self._otel}/metrics/tools",
                **{"from": epoch_ms(since), "to": epoch_ms(until)},
            )
        )

    def tracing_ready(self) -> bool:
        body = self._http.get("/otel/ready") or {}
        return bool(body.get("ready", body) if isinstance(body, dict) else body)

    # ── evaluation, from this deployment's side ────────────────────────────

    def runs(self) -> list[Run]:
        return self._client.runs.list(self.id)

    def run(self, suite: Any, *, version: int | None = None, n_trials: int = 1) -> Run:
        return self._client.runs.start(
            suite, self.id, version=version, n_trials=n_trials
        )

    def sample(self, **kwargs: Any) -> Run:
        return self._client.runs.sample(self.id, **kwargs)

    def gates(self) -> GateResult:
        """Whether this deployment's evaluation evidence clears the bar for promotion."""
        return GateResult.from_api(
            self._http.get("/agent-evals/gates", deploymentId=self.id)
        )

    def canary(self) -> list[Run]:
        """Start the project's canary suites against this deployment."""
        return self._client._bind_all(
            Run.list_from_api(
                self._http.post("/agent-evals/canary", deploymentId=self.id)
            )
        )

    def eval_job(self) -> EvalJob:
        return self._client.jobs.eval_job(self.id)

    def review_job(self) -> ReviewJob | None:
        return self._client.jobs.review_job(self.id)

    def regressions(self) -> RegressionSuite:
        return self._client.jobs.regressions(self.id)

    def analyse(self, **kwargs: Any) -> Run:
        """Run the failure analysis now; the job is created with defaults if the deployment has none."""
        job = self.review_job() or self._client.jobs.ensure_review_job(self.id)
        return self._client.jobs.analyse(job, **kwargs)


def _chat_payload(
    text: str,
    conversation_id: str | None,
    subject: str | None,
    context: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "message": {"role": "user", "content": [{"type": "text", "text": text}]}
    }
    if conversation_id:
        payload["conversation_id"] = conversation_id
    if subject:
        payload["subject"] = subject
    if context:
        payload["context"] = context
    if metadata:
        payload["metadata"] = metadata
    return payload


# ── streaming ──────────────────────────────────────────────────────────────


@dataclass
class StreamFrame:
    """One server-sent event of a streamed reply.

    ``type`` is ``delta`` (a piece of text, in ``text``), ``tool`` (a step the
    agent reported, in ``tool``) or ``completed`` (the whole reply, in ``reply``).
    """

    type: str
    text: str = ""
    tool: ToolEvent | None = None
    reply: ChatReply | None = None
    raw: Any = field(default=None, repr=False)


@public
class ChatStream:
    """A reply as the agent produces it.

    Iterate it for the text deltas; `events()` for every frame; `reply` for the
    completed `ChatReply`. Whichever is used, the stream is read once.
    """

    def __init__(self, lines: Iterable[str]):
        self._frames = self._parse(lines)
        self._chunks: list[str] = []
        self._tool_events: list[ToolEvent] = []
        self._completed: ChatReply | None = None
        self._exhausted = False

    def __iter__(self) -> Iterator[str]:
        for frame in self.events():
            if frame.type == "delta" and frame.text:
                yield frame.text

    def events(self) -> Iterator[StreamFrame]:
        """Every frame in order: deltas, tool events and the completed reply."""
        yield from self._frames
        self._exhausted = True

    @property
    def text(self) -> str:
        """The text streamed so far; all of it once the stream is read."""
        return "".join(self._chunks)

    @property
    def tool_events(self) -> list[ToolEvent]:
        """The steps the agent reported so far."""
        return list(self._tool_events)

    @property
    def reply(self) -> ChatReply:
        """The completed reply, reading the rest of the stream first if needed.

        An agent whose stream ends without a completed frame gets a reply made
        of the streamed text, so the caller always has one.
        """
        if not self._exhausted:
            for _ in self.events():
                pass
        if self._completed is None:
            self._completed = ChatReply(
                message={
                    "role": "assistant",
                    "content": [{"type": "text", "text": self.text}],
                },
                metadata={"tool_events": [t.raw for t in self._tool_events]},
            )
        return self._completed

    def _parse(self, lines: Iterable[str]) -> Iterator[StreamFrame]:
        for event, data in _sse_events(lines):
            try:
                body = json.loads(data) if data else {}
            except ValueError:
                continue
            if event == "message.delta":
                delta = (
                    (body.get("delta") or {}).get("text")
                    if isinstance(body, dict)
                    else None
                )
                if isinstance(delta, str) and delta:
                    self._chunks.append(delta)
                    yield StreamFrame("delta", text=delta, raw=body)
            elif event == "tool_event":
                if isinstance(body, dict) and body.get("name"):
                    tool = ToolEvent.from_api(body)
                    self._tool_events.append(tool)
                    yield StreamFrame("tool", tool=tool, raw=body)
            elif event == "message.completed":
                self._completed = ChatReply.from_api(
                    body if isinstance(body, dict) else {}
                )
                yield StreamFrame(
                    "completed",
                    text=self._completed.text,
                    reply=self._completed,
                    raw=body,
                )
            elif event == "error":
                message = body.get("message") if isinstance(body, dict) else None
                raise AgentServingError(str(message or "the agent returned an error"))


def _sse_events(lines: Iterable[str]) -> Iterator[tuple[str, str]]:
    """Server-sent events from lines: ``event:`` names a frame, ``data:`` lines fill it, a blank line ends it."""
    event = "message"
    data: list[str] = []
    for raw in lines:
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
        line = line.rstrip("\r")
        if not line:
            if data:
                yield event, "\n".join(data)
            event, data = "message", []
        elif line.startswith(":"):
            continue
        elif line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())
    if data:
        yield event, "\n".join(data)

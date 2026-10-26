"""One deployed agent: talking to it, and everything Hopsworks recorded about it.

What the chat panel, the Trace Summaries card, the Feedback tab, the failure
analysis and the monitoring dashboards do for one deployment, as methods on an
agent handle. The handle is fetched through :meth:`AgentServing.get_agent`.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    from collections.abc import Callable, Iterable, Iterator, Sequence

    from .client import AgentServing
    from .models import EvalJob, ReviewJob, Task

VERDICTS = ("positive", "negative", "false_alarm")
MANIFEST_PATH = ".well-known/hopsworks-agent.json"
DEFAULT_CHAT_PATH = "/v1/chat"
DEFAULT_RUN_PATH = "/v1/run"
#: The feature groups an agent's memory is kept in, every deployment's rows together.
MEMORY_MESSAGES_FG = "agent_memory_messages"
MEMORY_CONVERSATIONS_FG = "agent_memory_conversations"
MEMORY_FACTS_FG = "agent_memory_facts"


@public
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
        # The manifest too: a restarted deployment may declare different
        # routes, and this is the call someone makes when they expect it to.
        self._endpoints = None
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

        The message is sent as an AG-UI run, and the stream is its events: text
        deltas, the tool calls and steps the agent reports while working, and
        the completed reply.
        Iterating the stream yields the text deltas; `events()` yields every
        frame; `reply` is the completed `ChatReply`, reading the rest of the
        stream first if needed.

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
        payload = _run_payload(text, conversation_id, subject, context, metadata)
        lines = self._http.agent_request(
            "POST",
            self._segments(self._endpoint("run", DEFAULT_RUN_PATH)),
            json=payload,
            timeout=timeout,
            stream=True,
        )
        return ChatStream(lines)

    def _endpoint(self, name: str, default: str) -> str:
        """The agent's route for ``name``, from its manifest; the protocol's default without one."""
        if self._endpoints is None:
            # not a protocol agent, or the manifest is not served: the
            # protocol's default routes are the best guess there is.
            #
            # A failure here leaves it None rather than caching {}. A first
            # chat() against an agent that is still starting answers 503, and
            # cached that way an agent with custom routes sent every later call
            # on this object to /v1/chat, which 404s for it, for the lifetime
            # of the object.
            try:
                self.manifest()
            except AgentServingError:
                return default
        return str((self._endpoints or {}).get(name) or default)

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
    @public
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

    @public
    def trace(self, trace_id: str, *, wait: float = 0.0) -> Trace:
        """A whole trace: spans, attributes, events and totals.

        A trace is written a moment after the reply, span by span, so one read
        straight after ``chat()`` can be missing -- or worse, present without its
        model calls: no LLM span, zero tokens, zero cost, which reads as a turn
        that cost nothing. ``wait`` (seconds) polls until the turn has closed --
        its span carries the response id, which the SDK stamps last -- or the time
        runs out, and returns what is there by then. An agent that does not use
        the Hopsworks agent SDK never stamps it, so ``wait`` costs it the full time.

        Raises:
            AgentServingError: the trace was not found, after ``wait`` if one was given.
        """
        deadline = time.monotonic() + max(0.0, wait)
        while True:
            try:
                trace = Trace.from_api(
                    self._http.get(f"{self._otel}/traces/{trace_id}")
                )
            except AgentServingError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
                continue
            if wait <= 0 or _turn_closed(trace) or time.monotonic() >= deadline:
                return trace
            time.sleep(0.5)

    @public
    def session(self, session_id: str, *, limit: int = -1) -> list[TraceSummary]:
        """Every trace of a conversation, oldest first.

        The id travels as a query parameter, not in the path: an evaluation
        trial's session id is its trial id, ``"<run>/<task>/<version>/<index>"``,
        and interpolating that into the path made it four path segments -- a
        different route, answering 404 -- so the earlier turns of a multi-turn
        trial could not be read at all.
        """
        rows = TraceSummary.list_from_api(
            self._http.get(
                f"{self._otel}/traces/sessions", sessionId=session_id, limit=limit
            )
        )
        return sorted(rows, key=lambda t: t.start_time_ns)

    @public
    def conversation(self, session_id: str) -> list[dict[str, str]]:
        """The conversation as the user had it, one entry per turn.

        Each entry is ``{"trace_id": ..., "user": ..., "assistant": ...}`` --
        the turn's two sides already paired, which is the shape a reader of a
        conversation wants. Not ``role``/``content``: that is what
        :meth:`session` gives, one message per element, and pairing those is
        what this does.
        """
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

    # ── what the agent's memory holds ────────────────────────────────────────

    @public
    def memory_conversations(
        self,
        *,
        subject: str | None = None,
        limit: int = 50,
        feature_store: Any = None,
    ) -> list[dict[str, Any]]:
        """The conversations the agent's memory holds, most recently active first.

        Read from the agent's memory feature groups, `agent_memory_messages` and `agent_memory_conversations`, so the agent need not be running.
        It lists what the agent will remember, under the person each conversation is filed under.
        `conversation` reconstructs what was said from the traces instead, which exist whether or not the agent keeps memory.

        Example:
            ```python
            for c in agent.memory_conversations(subject="marcus@example.com"):
                print(c["conversation_id"], c["message_count"], c["last_active_at"])
            ```

        Parameters:
            subject: Only the conversations filed under this person.
            limit: How many to return.
            feature_store: The feature store holding the agent's memory; the logged-in project's when omitted.

        Returns:
            One entry per conversation, with `conversation_id`, `subject`, `message_count` and `last_active_at`, and the rolling `summary` when the agent keeps one.
            Empty when the agent keeps no memory.
        """
        store = _memory_feature_store(feature_store)
        # Every message, not only those stamped with the subject: the turns before the
        # person was identified are filed under the conversation, and counting only the
        # subject's rows reported a conversation a turn short of its transcript. Whose
        # it is is its latest subject, filtered on below.
        rows = _memory_rows(
            store,
            MEMORY_MESSAGES_FG,
            str(self.id),
            memory_type="message",
            status="closed",
        )
        conversations: dict[str, dict[str, Any]] = {}
        for row in sorted(rows, key=lambda r: r.get("id") or 0):
            entry = conversations.setdefault(
                str(row.get("conversation_id")),
                {
                    "conversation_id": str(row.get("conversation_id")),
                    "subject": None,
                    "message_count": 0,
                    "last_active_at": None,
                },
            )
            entry["message_count"] += 1
            # The newest subject, as the store reports it: an agent that identifies
            # the caller mid-conversation rebinds, and it is the identity it arrived
            # at that the memory is filed under.
            if row.get("subject"):
                entry["subject"] = row["subject"]
            created = _iso(row.get("created_at"))
            if created and (entry["last_active_at"] or "") < created:
                entry["last_active_at"] = created
        listed = sorted(
            (
                c
                for c in conversations.values()
                if not subject or c["subject"] == subject
            ),
            key=lambda c: c["last_active_at"] or "",
            reverse=True,
        )[: max(1, limit)]
        summaries = {
            str(row.get("conversation_id")): row.get("summary")
            for row in _memory_rows(store, MEMORY_CONVERSATIONS_FG, str(self.id))
            if row.get("summary")
        }
        for entry in listed:
            if entry["conversation_id"] in summaries:
                entry["summary"] = summaries[entry["conversation_id"]]
        return listed

    @public
    def memory_transcript(
        self,
        conversation_id: str,
        *,
        include_events: bool = False,
        feature_store: Any = None,
    ) -> dict[str, Any]:
        """One conversation as the agent's memory stored it.

        Read from the agent's memory feature groups, so the agent need not be running.
        The stored record differs from what the model reads on its next turn.
        Once a conversation grows past the summarizer's limit, older turns are folded into `summary` and leave the model's history, but they stay in `messages`; `summarized_through` is the id of the last message folded.

        Example:
            ```python
            stored = agent.memory_transcript(reply.conversation_id)
            print(stored["subject"], stored["summary"])
            for message in stored["messages"]:
                print(message["role"], message["content"])
            ```

        Parameters:
            conversation_id: The conversation, as `ChatReply.conversation_id` or `memory_conversations` gives it.
            include_events: Include the turns' tool calls and events, and the turns the agent abandoned, not only the completed messages.
            feature_store: The feature store holding the agent's memory; the logged-in project's when omitted.

        Returns:
            The `conversation_id`, the `messages` in order (each with `id`, `role`, `content`, `turn_id`, `memory_type`, `status` and `created_at`), the rolling `summary`, `summarized_through`, and the `subject` the conversation is filed under, which may be one the agent bound mid-conversation and the client was never told.
        """
        store = _memory_feature_store(feature_store)
        rows = sorted(
            _memory_rows(
                store,
                MEMORY_MESSAGES_FG,
                str(self.id),
                conversation_id=conversation_id,
            ),
            key=lambda r: r.get("id") or 0,
        )
        subject = next((r["subject"] for r in reversed(rows) if r.get("subject")), None)
        if not include_events:
            rows = [
                r
                for r in rows
                if r.get("memory_type") == "message" and r.get("status") == "closed"
            ]
        session = next(
            iter(
                _memory_rows(
                    store,
                    MEMORY_CONVERSATIONS_FG,
                    str(self.id),
                    conversation_id=conversation_id,
                )
            ),
            {},
        )
        return {
            "conversation_id": conversation_id,
            "messages": [
                {
                    "id": r.get("id"),
                    "turn_id": r.get("turn_id"),
                    "message_id": r.get("message_id"),
                    "role": r.get("role"),
                    "content": r.get("content"),
                    "memory_type": r.get("memory_type"),
                    "status": r.get("status"),
                    "created_at": _iso(r.get("created_at")),
                }
                for r in rows
            ],
            "summary": session.get("summary"),
            "summarized_through": int(session.get("last_summarized_item_id") or 0),
            "subject": subject,
        }

    @public
    def memory_facts(
        self, subject: str, *, feature_store: Any = None
    ) -> list[dict[str, Any]]:
        """What the agent remembers about one person: their `user`-scope facts.

        Read from the agent's `agent_memory_facts` feature group, so the agent need not be running.
        These are the facts the agent stored with `remember` for this subject, which load into every later conversation with them; expired ones are left out, as the agent leaves them out.
        Worth reading after a conversation, because a person can talk an agent into remembering something false about them.

        Example:
            ```python
            for fact in agent.memory_facts("marcus@example.com"):
                print(fact["key"], fact["value"], fact["updated_at"])
            ```

        Parameters:
            subject: The person as the agent identified them, either the `subject` asserted on the request or the one the agent bound.
            feature_store: The feature store holding the agent's memory; the logged-in project's when omitted.

        Returns:
            One entry per fact, most recently updated first, with `key`, `value`, `written_by`, `source_ref` (the turn it came from), `updated_at` and `expires_at`.
        """
        store = _memory_feature_store(feature_store)
        now = datetime.now(tz=timezone.utc).isoformat()
        facts = [
            {
                "key": row.get("state_key"),
                "value": row.get("value"),
                "scope": row.get("scope"),
                "owner": row.get("owner"),
                "written_by": row.get("written_by"),
                "source_ref": row.get("source_ref"),
                "updated_at": _iso(row.get("updated_at")),
                "expires_at": _iso(row.get("expires_at")),
            }
            for row in _memory_rows(
                store, MEMORY_FACTS_FG, str(self.id), scope="user", owner=subject
            )
        ]
        live = [f for f in facts if not f["expires_at"] or f["expires_at"] > now]
        return sorted(live, key=lambda f: f["updated_at"] or "", reverse=True)

    @public
    def memory_search(
        self,
        query: str,
        *,
        subject: str | None = None,
        conversation_id: str | None = None,
        k: int = 5,
        embedder: Callable[[str], list[float]] | None = None,
        feature_store: Any = None,
    ) -> list[dict[str, Any]]:
        """Messages from the agent's earlier conversations nearest in meaning to `query`.

        A similarity search over the agent's memory feature group, `agent_memory_<deployment id>`, which holds an embedding of every message the agent ingested.
        It is the search the agent itself runs with its `search` tool, done from outside: the same feature group, the same subject and conversation filters, and no running agent needed.

        The query has to be embedded by the model that embedded the messages, or the distances mean nothing.
        That model is recorded in the feature group's description; by default it is loaded as a sentence-transformers model, which needs `sentence-transformers` installed.
        Pass `embedder` for any other model.

        Example:
            ```python
            for hit in agent.memory_search("vegetarian", subject="dana@example.com"):
                print(hit["score"], hit["role"], hit["content"])
            ```

        Parameters:
            query: What to look for, in natural language.
            subject: Only that person's messages.
            conversation_id: Only one conversation's messages.
            k: How many messages to return.
            embedder: Embeds the query; it must be the model that embedded the messages, and when it carries a `model_id`, that is checked against the one recorded.
            feature_store: The feature store holding the agent's memory; the logged-in project's when omitted.

        Returns:
            The nearest messages, most similar first, each with `conversation_id`, `subject`, `role`, `content`, `created_at` and `score`.
            Empty when the agent keeps no embeddings or nothing matches.

        Raises:
            `ValueError`: If `embedder` was made with a different model than the one recorded on the feature group.
        """
        from hopsworks_agents.protocol.vectorstore import HopsworksVectorStore

        if feature_store is None:
            import hopsworks

            feature_store = hopsworks.get_current_project().get_feature_store()
        store = HopsworksVectorStore._existing(feature_store, str(self.id))
        if store is None:
            return []
        if embedder is None:
            from hopsworks_agents.protocol.summarizers import (
                sentence_transformer_embedder,
            )

            embedder = sentence_transformer_embedder(store._embedder_id)
        recorded = store._embedder_id
        given = getattr(embedder, "model_id", None)
        if given is not None and given != recorded:
            raise ValueError(
                f"the agent's memory was embedded with {recorded!r}; an embedder for "
                f"{given!r} would return distances that mean nothing"
            )
        return store.search(
            embedder(query), k=k, subject=subject, conversation_id=conversation_id
        )

    # ── feedback ───────────────────────────────────────────────────────────

    @public
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

    @public
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

    @public
    def trace_feedback(self, trace_id: str) -> list[Feedback]:
        return Feedback.list_from_api(
            self._http.get(f"{self._otel}/traces/{trace_id}/feedback")
        )

    @public
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
        """Your verdict on a trace. One per reviewer per trace: giving it again replaces yours.

        The whole verdict, not a patch: the row is replaced, so a field left out
        is cleared rather than kept. Re-saving a correction without the note that
        came with it drops the note.
        """
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

    @public
    def retract_feedback(
        self, trace: Feedback | str, *, reviewer: str | None = None
    ) -> None:
        """Take back your own verdict on a trace, or an end user's.

        Takes a trace id or the ``Feedback`` row itself, as ``promote_feedback``
        does: the two are used together, on the same row, and taking only the id
        here meant a row passed by mistake went into the URL as its repr and came
        back as "Invalid URI" rather than as anything about feedback.

        ``reviewer`` may only name an end user (``user:<subject>``), whose verdict
        the agent relayed and who has no session here to take it back from; a
        project member's verdict is theirs alone and naming an account is refused.
        """
        trace_id = trace.trace_id if isinstance(trace, Feedback) else str(trace)
        if not trace_id:
            raise ValueError("retract_feedback needs a trace id or a feedback row")
        self._http.delete(f"{self._otel}/traces/{trace_id}/feedback", reviewer=reviewer)

    @public
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

    @public
    def reviewers(self) -> list[str]:
        return list(self._http.get(f"{self._otel}/feedback/reviewers") or [])

    # ── analysis ───────────────────────────────────────────────────────────

    @public
    def triage(self, feedback: Sequence[Feedback | str]) -> list[Triage]:
        """The analysis model's latest proposal about each of these feedback rows."""
        ids = [f.feedback_id if isinstance(f, Feedback) else f for f in feedback]
        if not ids:
            return []
        rows = self._http.get(f"{self._otel}/feedback/triage", feedbackId=ids)
        return Triage.list_from_api(rows)

    @public
    def decide_triage(
        self, triage: Triage | str, decision: str, *, category: str | None = None
    ) -> None:
        """What you think of a proposal: ``accepted``, ``edited`` or ``rejected``. This is calibration data."""
        triage_id = triage.triage_id if isinstance(triage, Triage) else triage
        # `feedback/run`, so the same rule as a trial id: a parameter, because
        # the route takes one segment and this is two.
        self._http.put(
            f"{self._otel}/feedback/triage/decision",
            triageId=triage_id,
            decision=decision,
            category=category,
        )

    @public
    def calibration(self) -> Calibration:
        return Calibration.from_api(
            self._http.get(f"{self._otel}/feedback/triage/calibration")
        )

    @public
    def clusters(self, status: str | None = "open") -> list[Cluster]:
        """Failure clusters, most worth a reviewer's time first. ``status=None`` for all."""
        return Cluster.list_from_api(
            self._http.get(f"{self._otel}/feedback/clusters", status=status)
        )

    @public
    def cluster_members(self, cluster: Cluster | str) -> list[Triage]:
        cluster_id = cluster.cluster_id if isinstance(cluster, Cluster) else cluster
        return Triage.list_from_api(
            self._http.get(f"{self._otel}/feedback/clusters/{cluster_id}/members")
        )

    @public
    def rename_cluster(self, cluster: Cluster | str, label: str) -> None:
        self._decide_cluster(cluster, label=label)

    @public
    def dismiss_cluster(self, cluster: Cluster | str, reason: str) -> None:
        """``working_as_intended``, ``duplicate_of``, ``cannot_reproduce`` or ``out_of_scope``."""
        self._decide_cluster(cluster, status="dismissed", dismissReason=reason)

    @public
    def reopen_cluster(self, cluster: Cluster | str) -> None:
        self._decide_cluster(cluster, status="open")

    @public
    def mark_cluster_promoted(self, cluster: Cluster | str, task: Task | str) -> None:
        """Record the task a cluster became; every member's feedback is marked covered."""
        task_id = getattr(task, "task_id", task)
        self._decide_cluster(cluster, status="promoted", promotedTaskId=task_id)

    @public
    def promote_cluster(
        self, cluster: Cluster, *, expectations: dict[str, str] | None = None
    ) -> Task:
        """Promote the cluster's representative trace to a task and record it on the cluster."""
        if not cluster.representative_trace_id:
            raise ValueError("the cluster has no representative trace")
        promote_from = cluster.representative_feedback_id
        trace_id = cluster.representative_trace_id
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
            # The member whose correction this is, and that member's trace. A
            # cluster groups by failure signature, not by question, so members
            # ask different things -- pairing one member's correction with
            # another's question writes an expected answer to a question nobody
            # asked. Only some verdicts carry a correction, so when the
            # representative has none the task comes from the member that does,
            # conversation and all. The cluster is still marked promoted: the
            # task covers the failure, whichever of its members it was taken
            # from.
            corrected = next(
                (
                    m
                    for m in [representative, *members]
                    if m is not None and m.normalized_correction
                ),
                None,
            )
            if corrected is not None:
                expectations = {"expected": corrected.normalized_correction}
                promote_from = corrected.feedback_id or promote_from
                trace_id = corrected.trace_id or trace_id
        task = self._client.tasks.promote(
            self.id,
            trace_id,
            # What the user asked, off the verdict the cluster named. Without it
            # the promotion produces a task with no content: the platform stores
            # what it is given, and the redaction gate then refuses to confirm a
            # task with nothing in it -- so the cluster became a row nobody
            # could ever put in a suite.
            # Every user turn up to the one that went wrong, not only that turn:
            # a failure on the fourth turn cannot be reproduced by asking the
            # fourth question of a fresh agent. The platform stores what it is
            # given and does not go back to the transcript, so a promotion with
            # no question at all produces a task the redaction gate then refuses
            # to confirm -- a cluster nobody could ever put in a suite.
            question=self._asked_up_to(promote_from) or None,
            expectations=expectations,
        )
        self.mark_cluster_promoted(cluster, task)
        return task

    @public
    def promote_feedback(
        self,
        feedback: Feedback | str,
        *,
        conversation: bool = True,
        expectations: dict[str, str] | None = None,
        subject: str | None = None,
    ) -> Task:
        """One reviewer's verdict as a task, with what it was about and what it expected.

        It is asked as whoever the original turn ran as, read off the trace, so a
        verdict about what the agent remembered is replayed by the person it
        remembered. ``subject=""`` asks anonymously; a subject of your own asks as
        a test user instead of the customer.

        What ``tasks.promote`` cannot do on its own: it stores the content it is
        handed, and the question, the correction and the category are all on the
        feedback row rather than on the trace. Promoting a verdict meant reading
        those out and passing them yourself, and promoting it with no arguments
        produced a task with no content and no expectation -- one the redaction
        gate refuses, so it could never reach a suite.

        ``conversation`` carries every user turn up to the one the verdict is
        about; pass False for the failing turn alone, which is right when the
        turn stands on its own and wrong when it depends on what came before.

        ``feedback.promoted_task_id`` says whether a verdict has already been
        promoted, through its cluster or otherwise. This does not check it: two
        reviewers' verdicts on one trace are two tasks on purpose.
        """
        row = self._feedback_row(feedback)
        if row is None:
            raise ValueError(f"no such feedback: {feedback}")
        if expectations is None:
            expectations = (
                {"expected": row.corrected_answer} if row.corrected_answer else {}
            )
        return self._client.tasks.promote(
            self.id,
            row.trace_id,
            question=(self._asked_up_to(row) if conversation else row.questions)
            or None,
            category=row.issue_category or None,
            expectations=expectations,
            subject=subject,
        )

    def _feedback_row(self, feedback: Feedback | str) -> Feedback | None:
        if isinstance(feedback, Feedback):
            return feedback
        try:
            rows = self.feedback(feedback_id=feedback, limit=1).feedback
        except AgentServingError:
            return None
        return rows[0] if rows else None

    def _asked_in(self, feedback_id: str | None) -> list[str]:
        """The user's turns in the trace one verdict is about.

        Read off the feedback listing, which carries the trace's messages for
        exactly this: a promotion has to say what the question was, and the
        conversation is the only place it exists.
        """
        if not feedback_id:
            return []
        row = self._feedback_row(feedback_id) if feedback_id else None
        return row.questions if row else []

    def _asked_up_to(self, feedback: Feedback | str | None) -> list[str]:
        """Every user turn of the conversation up to and including that verdict's.

        A conversation is several traces sharing a session id, and a verdict is
        about one of them -- so the turn's own trace carries one question and
        the ones that set it up are in the traces before it. Reading only the
        verdict's trace made a four-turn failure into a one-turn task that asks
        the last question of an agent that was never told the first three.

        Falls back to that one turn when the trace names no session or the
        session cannot be read: a task with its last question is worth more than
        no task at all.
        """
        row = self._feedback_row(feedback) if feedback else None
        if row is None:
            return []
        if not row.session_id:
            return row.questions
        try:
            traces = self.session(row.session_id)
        except AgentServingError:
            return row.questions
        asked: list[str] = []
        for trace in traces:
            turns = [
                str(message.get("content") or "")
                for message in trace.conversation
                if message.get("role") == "user" and message.get("content")
            ]
            # By position and length, not by text. A trace carries either its
            # own turn or the whole conversation up to it, depending on the
            # framework, so a trace that repeats history must not be counted
            # twice -- but dropping anything already seen deduplicated genuine
            # repeats, and a user who says "continue" twice got one
            # "continue", replaying a different conversation from the one that
            # failed. A trace only repeats history when it carries more turns
            # than are already accounted for and begins with them; one turn is
            # always its own.
            if len(turns) > len(asked) and turns[: len(asked)] == asked:
                asked.extend(turns[len(asked) :])
            else:
                asked.extend(turns)
            if trace.trace_id == row.trace_id:
                break
        else:
            # The verdict's own trace was not in the session listing, so the
            # turns above are some other conversation's.
            return row.questions
        return asked or row.questions

    def _decide_cluster(self, cluster: Cluster | str, **params: Any) -> None:
        cluster_id = cluster.cluster_id if isinstance(cluster, Cluster) else cluster
        self._http.put(f"{self._otel}/feedback/clusters/{cluster_id}", **params)

    # ── metrics ────────────────────────────────────────────────────────────

    @public
    def trace_metrics(
        self, *, since: Any = None, until: Any = None
    ) -> list[TraceMetric]:
        return TraceMetric.list_from_api(
            self._http.get(
                f"{self._otel}/metrics/traces",
                **{"from": epoch_ms(since), "to": epoch_ms(until)},
            )
        )

    @public
    def llm_metrics(self, *, since: Any = None, until: Any = None) -> list[LlmMetric]:
        return LlmMetric.list_from_api(
            self._http.get(
                f"{self._otel}/metrics/llm",
                **{"from": epoch_ms(since), "to": epoch_ms(until)},
            )
        )

    @public
    def tool_metrics(self, *, since: Any = None, until: Any = None) -> list[ToolMetric]:
        return ToolMetric.list_from_api(
            self._http.get(
                f"{self._otel}/metrics/tools",
                **{"from": epoch_ms(since), "to": epoch_ms(until)},
            )
        )

    @public
    def tracing_ready(self) -> bool:
        body = self._http.get("/otel/ready") or {}
        return bool(body.get("ready", body) if isinstance(body, dict) else body)

    # ── evaluation, from this deployment's side ────────────────────────────

    @public
    def runs(self) -> list[Run]:
        return self._client.runs.list(self.id)

    @public
    def run(self, suite: Any, *, version: int | None = None, n_trials: int = 1) -> Run:
        return self._client.runs.start(
            suite, self.id, version=version, n_trials=n_trials
        )

    @public
    def sample(self, **kwargs: Any) -> Run:
        return self._client.runs.sample(self.id, **kwargs)

    @public
    def gates(self) -> GateResult:
        """Whether this deployment's evaluation evidence clears the bar for promotion."""
        return GateResult.from_api(
            self._http.get("/agent-evals/gates", deploymentId=self.id)
        )

    @public
    def canary(self) -> list[Run]:
        """Start the project's canary suites against this deployment."""
        return self._client._bind_all(
            Run.list_from_api(
                self._http.post("/agent-evals/canary", deploymentId=self.id)
            )
        )

    @public
    def eval_job(self) -> EvalJob:
        return self._client.jobs.eval_job(self.id)

    @public
    def review_job(self) -> ReviewJob | None:
        return self._client.jobs.review_job(self.id)

    @public
    def regressions(self) -> RegressionSuite:
        return self._client.jobs.regressions(self.id)

    @public
    def failure_history(self, **kwargs: Any) -> Any:
        """Which of each suite's failures are new, which are regressions, and how old the rest are."""
        return self._client.runs.failure_history(self.id, **kwargs)

    @public
    def analyse(self, **kwargs: Any) -> Run:
        """Run the failure analysis now; the job is created with defaults if the deployment has none."""
        job = self.review_job() or self._client.jobs.ensure_review_job(self.id)
        return self._client.jobs.analyse(job, **kwargs)


def _memory_feature_store(feature_store: Any) -> Any:
    if feature_store is not None:
        return feature_store
    import hopsworks

    return hopsworks.get_current_project().get_feature_store()


def _memory_rows(
    feature_store: Any, name: str, deployment_id: str, **equals: str
) -> list[dict[str, Any]]:
    """One deployment's rows of a memory feature group, read online, as dicts.

    `deployment_id` is text in these groups: the memory store also runs outside
    Hopsworks, where a deployment is any name, so it is compared as a string.
    No rows when the group does not exist, which is an agent that keeps no
    memory of that kind.
    """
    try:
        group = feature_store.get_feature_group(name, 1)
    except Exception:  # noqa: BLE001 -- hsfs raises its own types for "no such group"
        return []
    if group is None:
        return []
    condition = group.get_feature("deployment_id") == deployment_id
    for column, value in equals.items():
        condition = condition & (group.get_feature(column) == value)
    frame = group.select_all().filter(condition).read(online=True)
    return [] if frame is None else frame.to_dict("records")


def _iso(value: Any) -> str | None:
    """A timestamp as UTC ISO text, whatever the online read returned it as."""
    if value is None or value != value:  # None, or NaN and NaT
        return None
    if isinstance(value, str):
        return value
    if getattr(value, "tzinfo", None) is None and hasattr(value, "replace"):
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _turn_closed(trace: Trace) -> bool:
    """Whether the turn this trace records has finished landing.

    The response id is stamped on the turn span when the turn closes, and that span
    ends after every span inside it, so its presence means the rest are in.
    """
    return any(
        a.get("attrKey") == "hopsworks.response_id" for a in trace.span_attributes
    )


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


def _run_payload(
    text: str,
    conversation_id: str | None,
    subject: str | None,
    context: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    """One message as an AG-UI ``RunAgentInput``.

    Only the new message is sent: the agent keeps the conversation, and one with
    no memory gets this message alone, as it would on ``/v1/chat``.
    Hopsworks' own fields ride under ``forwardedProps.hopsworks``.
    """
    import uuid  # noqa: PLC0415

    hopsworks: dict[str, Any] = {}
    if subject:
        hopsworks["subject"] = subject
    if context:
        hopsworks["context"] = context
    if metadata:
        hopsworks["metadata"] = metadata
    return {
        "threadId": conversation_id or "",
        "runId": f"run_{uuid.uuid4().hex}",
        "messages": [
            {"id": f"msg_{uuid.uuid4().hex}", "role": "user", "content": text}
        ],
        "tools": [],
        "context": [],
        "forwardedProps": {"hopsworks": hopsworks} if hopsworks else {},
    }


# ── streaming ──────────────────────────────────────────────────────────────


@dataclass
class StreamFrame:
    """One frame of a streamed reply, read from the run's AG-UI events.

    ``type`` is ``delta`` (a piece of text, in ``text``), ``tool`` (a tool call
    or a step the agent reported, in ``tool``) or ``completed`` (the whole reply,
    in ``reply``). ``raw`` is the AG-UI event it was read from.
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

    @public
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
        of the streamed text, so the caller always has one — reported as
        failed, because it is not one the agent finished. A dropped connection
        or a killed pod leaves exactly this shape, and calling it completed
        hands an evaluator a truncated answer to grade and loses the
        conversation id the next turn would continue from.
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
                metadata={
                    "tool_events": [t.raw for t in self._tool_events],
                    "incomplete_stream": True,
                },
                status="failed",
            )
        return self._completed

    def _parse(self, lines: Iterable[str]) -> Iterator[StreamFrame]:
        thread_id = ""
        trace_id: str | None = None
        media: list[dict[str, Any]] = []
        calls: dict[str, dict[str, Any]] = {}
        for _, data in _sse_events(lines):
            try:
                event = json.loads(data) if data else {}
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            kind = event.get("type")
            hopsworks = _hopsworks_of(event.get("metadata"))
            if kind == "RUN_STARTED":
                thread_id = str(event.get("threadId") or "")
                trace_id = hopsworks.get("trace_id") or trace_id
            elif kind == "TEXT_MESSAGE_CONTENT":
                delta = event.get("delta")
                if isinstance(delta, str) and delta:
                    self._chunks.append(delta)
                    yield StreamFrame("delta", text=delta, raw=event)
            elif kind == "TOOL_CALL_START":
                call_id = str(event.get("toolCallId") or "")
                calls[call_id] = {
                    "id": call_id,
                    "name": event.get("toolCallName") or "tool",
                    "status": "running",
                }
                yield self._tool_frame(dict(calls[call_id]), event)
            elif kind == "TOOL_CALL_ARGS":
                call = calls.get(str(event.get("toolCallId") or ""))
                if call is not None:
                    data_ = call.setdefault("data", {})
                    data_["args"] = data_.get("args", "") + str(
                        event.get("delta") or ""
                    )
            elif kind == "TOOL_CALL_RESULT":
                call = calls.get(str(event.get("toolCallId") or ""))
                if call is None:
                    continue
                failed = _hopsworks_of(event.get("metadata")).get("status") == "failed"
                call["status"] = "failed" if failed else "done"
                content = event.get("content")
                if failed:
                    call["message"] = content
                else:
                    call.setdefault("data", {})["result"] = content
                yield self._tool_frame(dict(call), event)
            elif kind in ("STEP_STARTED", "STEP_FINISHED"):
                step = {
                    "name": event.get("stepName") or "step",
                    "status": hopsworks.get("status")
                    or ("running" if kind == "STEP_STARTED" else "done"),
                }
                for key in ("id", "message", "data"):
                    if hopsworks.get(key) is not None:
                        step[key] = hopsworks[key]
                yield self._tool_frame(step, event)
            elif kind == "CUSTOM" and event.get("name") == "hopsworks.media":
                if isinstance(event.get("value"), dict):
                    media.append(event["value"])
            elif kind == "RUN_FINISHED":
                result = _hopsworks_of(event.get("result"))
                metadata = dict(result.get("metadata") or {})
                metadata["trace_id"] = result.get("trace_id") or trace_id
                for key in ("subject", "subject_source"):
                    if result.get(key) is not None:
                        metadata[key] = result[key]
                metadata["tool_events"] = [t.raw for t in self._tool_events]
                self._completed = ChatReply(
                    id=str(result.get("response_id") or ""),
                    conversation_id=str(event.get("threadId") or thread_id),
                    message={
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": self.text},
                            *media,
                        ],
                    },
                    citations=list(result.get("citations") or []),
                    usage=_usage_of(event.get("usage")),
                    metadata=metadata,
                    status="completed",
                )
                yield StreamFrame(
                    "completed",
                    text=self._completed.text,
                    reply=self._completed,
                    raw=event,
                )
            elif kind == "RUN_ERROR":
                raise AgentServingError(
                    str(event.get("message") or "the agent returned an error"),
                    int(hopsworks.get("status") or 0),
                    code=str(event.get("code") or ""),
                    retryable=bool(hopsworks.get("retryable")),
                )

    def _tool_frame(self, body: dict[str, Any], event: dict[str, Any]) -> StreamFrame:
        tool = ToolEvent.from_api(body)
        self._tool_events.append(tool)
        return StreamFrame("tool", tool=tool, raw=event)


def _hopsworks_of(value: Any) -> dict[str, Any]:
    """What Hopsworks put under ``hopsworks`` in an AG-UI event's metadata or result."""
    if isinstance(value, dict) and isinstance(value.get("hopsworks"), dict):
        return value["hopsworks"]
    return {}


def _usage_of(usage: Any) -> dict[str, int] | None:
    """AG-UI's per-model token counts, summed into the reply's ``usage``."""
    if not isinstance(usage, list):
        return None
    totals: dict[str, int] = {}
    for entry in usage:
        if not isinstance(entry, dict):
            continue
        for wire, key in (
            ("inputTokens", "input_tokens"),
            ("outputTokens", "output_tokens"),
        ):
            if isinstance(entry.get(wire), int):
                totals[key] = totals.get(key, 0) + entry[wire]
    return totals or None


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

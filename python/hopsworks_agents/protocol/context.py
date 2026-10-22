"""Handler context — the optional second argument to a chat/stream handler.

Handlers may be written with one or two parameters; the SDK inspects the
signature and passes a :class:`HandlerContext` only when a second parameter is
declared, so ``def chat(request)`` and ``def chat(request, ctx)`` both work.

The context bundles per-turn conveniences (history, memory, logger,
deployment/framework identity, correlation ids) and ``emit_event`` for
surfacing intermediate progress (tool calls, retrieval, code execution) as
``tool_event`` SSE frames while streaming — without the author hand-crafting
SSE.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from .evaluation import EvalTrial
    from .memory import ChatMemory, Turn
    from .models import ChatRequest


class HandlerContext:
    def __init__(
        self,
        request: ChatRequest,
        memory: ChatMemory | None,
        framework: str,
        response_id: str,
        turn_id: str = "",
        subject: str | None = None,
        subjects: Any | None = None,
    ):
        self.request = request
        self.conversation_id = request.conversation_id or ""
        self.memory = memory
        self.framework = framework
        self.deployment_id = os.environ.get("DEPLOYMENT_ID")
        self.logger = logging.getLogger("hopsworks_agent")
        # correlation ids: stable for the life of this turn
        self.response_id = response_id
        self.message_id = request.message.id
        # turn identity: groups every memory row this turn writes, so a reply,
        # its tool calls, and anything derived from them stay linked
        self.turn_id = turn_id
        # whether the client actually named a subject. Kept separate from the
        # resolved value below because "we know who this is" and "we fell back"
        # are different security stories, and an audit view must not present a
        # per-conversation fallback as a user identity.
        self.has_subject = subject is not None
        # durable per-user memory is keyed by this. Without a subject it falls
        # back to the conversation, so memories degrade to per-conversation
        # durability rather than leaking into a shared bucket.
        self.subject = subject or self.conversation_id
        # where the subject came from: "client" (asserted on the request),
        # "conversation" (the fallback above), or "app" (the agent identified
        # the user mid-turn — see rebind_subject).
        self.subject_source = "client" if subject is not None else "conversation"
        # writes the conversation-to-subject index, so a customer key can be
        # traced back to that customer's conversations. None when the app did
        # not configure one; every use tolerates that.
        self._subjects = subjects
        # The evaluation trial this turn belongs to, when it is one: set by
        # AgentApp from the request's baggage, after verifying the run with
        # Hopsworks (or without verifying, under EVAL_MODE, where the whole
        # deployment is an evaluation). None means this is a customer's turn,
        # or a deployment that did not declare eval_per_request. Tools, which
        # get no context, ask hopsworks_agents.protocol.evaluation.in_evaluation().
        self.evaluation: EvalTrial | None = None
        # Whether the eval ids on the request were checked out with Hopsworks,
        # which is a narrower question than `evaluation`: a read-only suite may
        # be run against an agent that never declared eval_per_request, and that
        # turn is a trial to label even though its tools run for real. Only a
        # labelled turn carries hopsworks.eval.* onto its spans -- baggage comes
        # over the wire, and those attributes are what the platform reads back
        # to tell a test from a customer.
        self._eval_labelled = False
        # turn lifecycle bookkeeping, owned by AgentApp
        self._turn_open = False
        self._next_seq = 1
        # the turn's root OTel span, set by the route when tracing is active.
        # None when untraced, so every read must tolerate it.
        self._span: Any = None
        # emit plumbing, wired by the route: a queue while streaming, else a
        # buffer surfaced in the response metadata
        self._event_queue: asyncio.Queue[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event_buffer: list[dict[str, Any]] = []
        #: Tool names the handler has emitted an event for in this turn. The
        #: automatic span-driven emitter stays off them: an agent that reports
        #: its own progress has said what it wants shown.
        self._handler_emitted: set[str] = set()
        #: Spans the automatic emitter opened and then found to be the handler's
        #: after all, so its closing event is dropped with its opening one.
        self._auto_suppressed: set[str] = set()
        # set by the LLM-span detector when a model call is traced during this
        # turn: what says whether the turn's own usage figure is the only one
        # a reader of these spans will find
        self._saw_llm_span = False
        # every event this turn emitted, regardless of transport — the buffer
        # above is drained into the response metadata and is empty while
        # streaming, so it can't double as the record we persist
        self._recorded_events: list[dict[str, Any]] = []

    @property
    def history(self) -> list[Turn]:
        """Prior turns of this conversation from the configured memory store.

        (empty list when no memory is configured).

        With a summarizer configured this is the turns *since* the last fold —
        everything older is in :attr:`summary`. Pass both to the model.
        """
        if self.memory is None:
            return []
        return self.memory.get(self.conversation_id)

    @property
    def summary(self) -> str | None:
        """Rolling summary of the turns folded out of :attr:`history`, or None.

        None means nothing has been folded yet, so ``history`` is the whole
        conversation.
        """
        if self.memory is None:
            return None
        return self.memory.get_summary(self.conversation_id)

    @public
    def state(self, scope: str = "user") -> dict[str, str]:
        """Scoped durable state as a plain ``{key: value}`` dict.

        ``user`` is keyed by :attr:`subject` and outlives the conversation;
        ``session`` is this conversation only; ``app`` is agent-wide.
        """
        if self.memory is None:
            return {}
        owner = self._state_owner(scope)
        return {
            row["key"]: row["value"] for row in self.memory.list_state(scope, owner)
        }

    @public
    def recall(self, key: str, *, scope: str = "user") -> str | None:
        """One durable fact, read under the owner its scope is keyed by.

        The single-key read beside :meth:`state`, which returns the whole scope::

            language = ctx.recall("preferred_language")
            cart = ctx.recall("cart_id", scope="session")

        ``None`` when nothing is stored under that key, and for an agent running
        without memory. Any scope can be read, ``app`` included -- what a turn
        may not do is *write* that one.

        Raises:
            ValueError: no such scope. A typo would otherwise read as "nothing
                stored", which is the failure these methods exist to prevent.
        """
        from .memory import SCOPES

        if scope not in SCOPES:
            raise ValueError(f"unknown scope {scope!r}; expected one of {SCOPES}")
        if self.memory is None:
            return None
        return self.memory.get_state(scope, self._state_owner(scope), key)

    @public
    def remember(
        self,
        key: str,
        value: str,
        *,
        scope: str = "user",
        source_ref: str | None = None,
    ) -> bool:
        """Write one durable fact, under the owner the scope is keyed by.

        The write half of :meth:`state`, which reads with that owner::

            ctx.remember("preferred_language", "Swedish")
            ctx.remember("cart_id", cart.id, scope="session")

        Without it an agent writing state had to call
        ``memory.set_state(scope, owner, ...)`` and work the owner out for
        itself — the subject for ``user``, the conversation for ``session`` —
        which is a convention that reads correctly and is wrong in a way
        nothing reports: state written under the wrong owner is simply never
        read back.

        ``user`` outlives the conversation and ``session`` is this conversation
        only. ``app`` is agent-wide and may not be written from a turn: it is
        injected into every user's context, so one conversation writing it is
        how a prompt injection reaches everybody. Operators write that scope out
        of band, with :meth:`~hopsworks_agents.protocol.memory.ChatMemory.set_state`.

        The fact is recorded as the agent's, which is what makes it bounded: the
        value is cut to the store's cap and given an expiry, and it is listed in
        the subject's own audit view as agent-written. ``source_ref`` defaults to
        this turn, so "why does the agent believe this?" resolves to a
        conversation and a message rather than to nothing.

        Args:
            key: a short, stable identifier. Not normalised: a programmatic
                caller chose it, unlike the model-facing ``remember`` tool.
            value: the fact, written so it is useful without the chat around it.
            scope: ``user`` or ``session``.
            source_ref: what this fact came from; this turn by default.

        Returns:
            Whether there was a store to write to. False for an agent running
            without memory, which is the same way :meth:`state` and
            :attr:`history` degrade rather than raising.

        Raises:
            ValueError: the scope cannot be written from a turn.
        """
        from .memory import WRITABLE_SCOPES

        if scope not in WRITABLE_SCOPES:
            raise ValueError(
                f"cannot write scope {scope!r} from a turn; use 'user' for what "
                "is true about this person across conversations, or 'session' "
                f"for this conversation only (writable: {WRITABLE_SCOPES})"
            )
        if self.memory is None:
            return False
        self.memory.set_state(
            scope,
            self._state_owner(scope),
            key,
            value,
            source_ref=source_ref if source_ref is not None else self._provenance(),
        )
        return True

    @public
    def forget(self, key: str | None = None, *, scope: str = "user") -> int:
        """Erase one durable fact, or every fact in a scope when ``key`` is omitted.

        The counterpart to :meth:`remember`, with the same owner rule and the
        same restriction: ``app`` is not a turn's to change.

        Returns:
            How many facts were removed; 0 for an agent running without memory.

        Raises:
            ValueError: the scope cannot be written from a turn.
        """
        from .memory import WRITABLE_SCOPES

        if scope not in WRITABLE_SCOPES:
            raise ValueError(
                f"cannot erase scope {scope!r} from a turn (writable: {WRITABLE_SCOPES})"
            )
        if self.memory is None:
            return 0
        return self.memory.delete_state(scope, self._state_owner(scope), key)

    def _provenance(self) -> str:
        """Where a fact written this turn came from, as the store records it.

        The ids rather than the text: the content is in the conversation this
        points at, and a copy here would be a second place to erase it from.
        """
        import json

        return json.dumps(
            {
                "conversation_id": self.conversation_id,
                "turn_id": self.turn_id,
                "message_id": self.message_id,
            }
        )

    @public
    def rebind_subject(self, subject: str) -> None:
        """Bind durable memory to the end user this turn just identified.

        The transport authenticates the *chatbot*, not the person using it — a
        project-wide serving key is the bot's own credential, the same way any
        app holds an API key to reach its backend. So when the agent does what
        a real chatbot does and asks who it is speaking to, the answer arrives
        mid-turn, after :meth:`begin_turn` has already stamped the question with
        whatever the client asserted (often the operator, or the
        per-conversation fallback).

        Calling this points ``user`` scope, :meth:`system_context` and
        :meth:`search` at the person instead::

            ctx.rebind_subject(customer_key(first, last, phone))

        It also restamps the rows this turn already wrote, so the turn is not
        split between two subjects. Safe to call repeatedly; the last call wins.

        This is identification, not authentication. The subject it binds is
        only as trustworthy as whatever the agent used to derive it, and
        ``user`` scope is readable by anyone who can reproduce that value.
        """
        if not subject or subject == self.subject:
            return
        self.subject = subject
        # "the app worked it out", which is neither "the client told us" nor
        # "we fell back to the conversation" — an audit view must be able to
        # tell the three apart.
        self.subject_source = "app"
        self.has_subject = True
        if self.memory is not None:
            self.memory.rebind_turn_subject(self.conversation_id, self.turn_id, subject)
        # the same identification, recorded where a developer holding a
        # customer key will look for it. One writer for both, so the index and
        # the memory rows cannot end up disagreeing about who this is.
        self.record_subject(subject, source="app")

    @public
    def record_subject(self, subject: str, source: str = "app") -> bool:
        """Say who this conversation is with, without changing what it can read.

        This is the labelling half of :meth:`rebind_subject`, split out because
        the two carry very different authority. ``subject`` keys durable memory,
        so rebinding it decides which person's facts the agent may read;
        recording it decides what a traces table displays. Only the second is
        safe to expose to a model, which is why the ``identify`` tool calls this
        and nothing else.

        Best-effort: returns whether a row landed, and never raises.
        """
        if not subject or self._subjects is None:
            return False
        return self._subjects.record(self.conversation_id, subject, source)

    def _state_owner(self, scope: str) -> str:
        if scope == "session":
            return self.conversation_id
        if scope == "app":
            return ""
        return self.subject

    @public
    def system_context(
        self,
        header: str = "Context from earlier:",
        state_header: str = "What you know about this user:",
    ) -> str:
        """Memory assembled into a block to drop into your system prompt.

        The SDK builds this every turn with no model round-trip, but it does not
        place it: where context belongs is a property of your prompt, so you
        decide. Returns ``""`` when there is nothing to add, which is safe to
        concatenate unconditionally::

            system = MY_PROMPT + ctx.system_context()

        Carries short-term memory (the rolling summary) and semantic memory
        (scoped durable state), each only once configured.
        """
        blocks = []
        summary = self.summary
        if summary:
            blocks.append(f"{header}\n{summary}")
        if self.memory is not None:
            state = self.memory.state_block(self.subject, self.conversation_id)
            if state:
                blocks.append(f"{state_header}\n{state}")
        if not blocks:
            return ""
        return "\n\n" + "\n\n".join(blocks) + "\n"

    # ── the same three, off the event loop ───────────────────────────────
    #
    # Reading memory is a database round trip. In a `def` handler that is
    # already a worker thread, so the properties above are the right thing to
    # use. In an `async def` handler they run on the event loop, and while they
    # do, every other conversation in this replica waits -- and so do /health
    # and /ready. These are the same reads, awaited.

    @public
    async def ahistory(self) -> list[Turn]:
        """:attr:`history`, read in a worker thread."""
        if self.memory is None:
            return []
        return await asyncio.to_thread(self.memory.get, self.conversation_id)

    @public
    async def asummary(self) -> str | None:
        """:attr:`summary`, read in a worker thread."""
        if self.memory is None:
            return None
        return await asyncio.to_thread(self.memory.get_summary, self.conversation_id)

    @public
    async def arecall(self, key: str, *, scope: str = "user") -> str | None:
        """:meth:`recall`, read in a worker thread."""
        return await asyncio.to_thread(lambda: self.recall(key, scope=scope))

    @public
    async def aremember(
        self,
        key: str,
        value: str,
        *,
        scope: str = "user",
        source_ref: str | None = None,
    ) -> bool:
        """:meth:`remember`, written in a worker thread."""
        return await asyncio.to_thread(
            lambda: self.remember(key, value, scope=scope, source_ref=source_ref)
        )

    @public
    async def aforget(self, key: str | None = None, *, scope: str = "user") -> int:
        """:meth:`forget`, run in a worker thread."""
        return await asyncio.to_thread(lambda: self.forget(key, scope=scope))

    @public
    async def asystem_context(
        self,
        header: str = "Context from earlier:",
        state_header: str = "What you know about this user:",
    ) -> str:
        """:meth:`system_context`, assembled in a worker thread."""
        return await asyncio.to_thread(self.system_context, header, state_header)

    @public
    async def emit_event(
        self,
        name: str,
        status: str = "running",
        message: str | None = None,
        data: dict[str, Any] | None = None,
        event_id: str | None = None,
    ) -> None:
        """Surface an intermediate progress event (a tool call, a retrieval, a.

        code run). While streaming it is sent immediately as a ``tool_event``
        SSE frame; otherwise it is buffered into the response metadata.

        ``event_id`` ties a ``running`` event to its later ``done``/``failed``
        so a client renders one chip that updates rather than several. Pass the
        same id for the start and end of one call (the SDK's auto-emit uses the
        span id); omit it and each event stands alone.
        """
        # The handler is emitting for this tool itself, so the automatic emitter
        # must not emit for it too: two chips for one call, with different ids,
        # one closing under the tool's name and one under the event's.
        self._handler_emitted.add(name)
        self._emit_sync(name, status, message, data, event_id)

    def _emit_sync(
        self,
        name: str,
        status: str,
        message: str | None,
        data: dict[str, Any] | None,
        event_id: str | None,
    ) -> None:
        """Non-async emit used by both ``emit_event`` and the auto-emit span.

        processor (which runs in sync OTel callbacks, possibly off-thread).
        """
        payload: dict[str, Any] = {"name": name, "status": status}
        if event_id is not None:
            payload["id"] = event_id
        if message is not None:
            payload["message"] = message
        if data is not None:
            payload["data"] = data
        self._dispatch(payload)

    @public
    async def stream_langchain(
        self, events: AsyncIterator[dict[str, Any]]
    ) -> AsyncIterator[str]:
        """Pipe a LangChain/LangGraph ``astream_events(version="v2")`` stream.

        through, yielding assistant text deltas and turning tool calls into
        ``tool_event`` chips automatically — no manual ``emit_event`` and no
        dependency on tracing:

            async for delta in ctx.stream_langchain(agent.astream_events(...)):
                yield delta

        ``on_tool_start`` / ``on_tool_end`` / ``on_tool_error`` become
        running / done / failed events keyed by the tool's ``run_id`` so start
        and end collapse into one chip.
        """
        async for event in events:
            kind = event.get("event")
            if kind == "on_chat_model_stream":
                chunk = (event.get("data") or {}).get("chunk")
                content = getattr(chunk, "content", None)
                if isinstance(content, str):
                    if content:
                        yield content
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text = block.get("text", "")
                            if text:
                                yield text
            elif kind == "on_tool_start":
                tool_input = (event.get("data") or {}).get("input")
                await self.emit_event(
                    event.get("name", "tool"),
                    status="running",
                    message=str(tool_input) if tool_input else None,
                    event_id=event.get("run_id"),
                )
            elif kind == "on_tool_end":
                await self.emit_event(
                    event.get("name", "tool"),
                    status="done",
                    event_id=event.get("run_id"),
                )
            elif kind == "on_tool_error":
                err = (event.get("data") or {}).get("error")
                await self.emit_event(
                    event.get("name", "tool"),
                    status="failed",
                    message=str(err) if err else None,
                    event_id=event.get("run_id"),
                )

    @public
    async def stream_llamaindex(self, handler: Any) -> AsyncIterator[str]:
        """Pipe a LlamaIndex workflow agent run through, yielding assistant.

        text deltas and turning tool calls into ``tool_event`` chips:

            handler = agent.run(msg)          # a WorkflowHandler
            async for delta in ctx.stream_llamaindex(handler):
                yield delta

        Duck-typed on the workflow event class names (``AgentStream`` →
        deltas, ``ToolCall`` / ``ToolCallResult`` → running / done, keyed by
        ``tool_id``) so it works across LlamaIndex versions.
        """
        async for event in handler.stream_events():
            cls = type(event).__name__
            if cls == "AgentStream":
                delta = getattr(event, "delta", None)
                if isinstance(delta, str) and delta:
                    yield delta
            elif cls == "ToolCall":
                kwargs = getattr(event, "tool_kwargs", None)
                await self.emit_event(
                    getattr(event, "tool_name", "tool"),
                    status="running",
                    message=str(kwargs) if kwargs else None,
                    event_id=getattr(event, "tool_id", None),
                )
            elif cls == "ToolCallResult":
                await self.emit_event(
                    getattr(event, "tool_name", "tool"),
                    status="done",
                    event_id=getattr(event, "tool_id", None),
                )

    def _dispatch(self, payload: dict[str, Any]) -> None:
        self._recorded_events.append(payload)
        queue = self._event_queue
        loop = self._loop
        if queue is None or loop is None:
            self._event_buffer.append(payload)
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            # already on the request's event loop (handler called emit_event):
            # enqueue directly so ordering vs. yielded deltas is preserved
            queue.put_nowait(("tool_event", payload))
        else:
            # off-thread (a framework tool running in a threadpool): hop back
            # onto the loop safely
            loop.call_soon_threadsafe(queue.put_nowait, ("tool_event", payload))

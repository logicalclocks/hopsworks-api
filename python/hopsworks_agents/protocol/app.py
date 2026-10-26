"""AgentApp: a FastAPI subclass that serves the Hopsworks Agent Protocol.

Registers automatically:

- ``GET  /.well-known/hopsworks-agent.json``  — manifest: endpoints and
  AG-UI capabilities
- ``POST /v1/run``                            — one AG-UI run: ``RunAgentInput``
  in, AG-UI events out as server-sent events (see ``agui``)
- ``POST /v1/chat``                           — the same turn as one JSON reply,
  for callers that do not stream
- ``GET  /health``                            — liveness probe

Being a FastAPI subclass, it works anywhere a FastAPI app does (uvicorn,
KServe/Knative on Hopsworks) and extra routes can be added the normal way.
CORS is enabled by default so the Hopsworks UI chat panel can call the agent
directly through the Istio ingress.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from ag_ui.core import AgentCapabilities, RunAgentInput
from ag_ui.core.version import PROTOCOL_VERSION as AGUI_PROTOCOL_VERSION
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from hopsworks_apigen import public

from . import conventions
from .agui import RunStream, _resume_answer, _to_chat_request
from .context import HandlerContext, Interrupted
from .evaluation import RunVerifier, env_eval_mode, parse_baggage
from .evaluation import active as eval_active
from .memory import TURN_ABANDONED, TURN_CLOSED, ChatMemory, MemoryEraseError
from .memory_config import memory_from_env
from .models import (
    PROTOCOL,
    PROTOCOL_VERSION,
    AgentError,
    AgentResponse,
    ChatMessage,
    ChatRequest,
    ChatResponse,
    FeedbackRequest,
    ResumeAnswer,
    new_conversation_id,
    new_response_id,
)
from .tracing import resolve_framework, setup_tracing, turn_span


ChatHandler = Callable[..., Awaitable[ChatResponse | str] | ChatResponse | str]
StreamHandler = Callable[..., AsyncIterator["str | ChatResponse"]]


def _wants_context(handler: Callable[..., Any]) -> bool:
    """True when the handler declares a second positional parameter (the.

    HandlerContext), so ``def chat(request)`` and ``def chat(request, ctx)``
    both work.
    """
    try:
        params = [
            p
            for p in inspect.signature(handler).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        return len(params) >= 2
    except (ValueError, TypeError):
        return False


class _NotGiven:
    """Marker for a constructor argument nobody passed.

    `memory=None` has to keep meaning "no memory", so it cannot also mean
    "decide for me": an agent that switched memory off in code must stay off
    when the deployment configures some.
    """

    def __repr__(self) -> str:  # pragma: no cover - debugging only
        return "<not given>"


NOT_GIVEN = _NotGiven()


#: conversations whose latest trace the app keeps in memory for feedback by conversation
LAST_TRACE_LIMIT = 10_000


class AgentApp(FastAPI):
    def __init__(
        self,
        name: str = "Hopsworks agent",
        description: str = "",
        version: str = "1.0.0",
        welcome_message: str | None = None,
        suggested_prompts: list[str] | None = None,
        placeholder: str | None = None,
        input_modalities: list[str] | None = None,
        output_modalities: list[str] | None = None,
        framework: str | None = None,
        tracing: bool | None = None,
        memory: ChatMemory | None | _NotGiven = NOT_GIVEN,
        tool_events: bool = False,
        graph: Any = None,
        platform: Any | None = None,
        allow_cors: bool = True,
        eval_per_request: bool = False,
        memory_routes: bool = True,
        **fastapi_kwargs: Any,
    ):
        super().__init__(title=name, description=description, **fastapi_kwargs)
        self._agent_name = name
        self._agent_description = description
        self._agent_version = version
        self._welcome_message = welcome_message
        self._suggested_prompts = suggested_prompts or []
        self._placeholder = placeholder
        self._input_modalities = input_modalities or ["text"]
        self._output_modalities = output_modalities or ["text"]
        # Records which conversation was with whom, for anyone later holding a
        # customer key and no way to find their conversations. Constructed
        # unconditionally but connects nothing until something identifies a
        # conversation, so an agent that never does pays for a field.
        from .identity import ConversationSubjectIndex

        self._subjects = ConversationSubjectIndex()

        # Whether this agent serves the memory routes at all: listing
        # conversations, reading a transcript, deleting a conversation or a
        # subject's facts. On, because that is what the chat panel's memory
        # view and a client's "new session" call use, and the credential that
        # opens them is the one that opens chat.
        #
        # Off is for an agent whose serving key reaches people who should not
        # have that reach -- a public end-user client holding the key, say.
        # There is no second credential to hand such a client: any serving key
        # for the project opens these routes, so the only way to withhold them
        # is not to serve them.
        self._memory_routes = memory_routes

        # report tool calls the framework makes as AG-UI tool calls, from its
        # tracing spans; off by default
        self._tool_events = tool_events
        self._auto_tool_events = False

        # optional agent structure graph (e.g. agent.get_graph() for a compiled
        # LangGraph) served at /v1/graph for the chat panel's Graph tab
        from .graph import to_graph_spec

        self._graph_spec = to_graph_spec(graph)

        # End-user feedback, relayed to Hopsworks over the deployment's own
        # credential. The last trace of each conversation is remembered so a
        # client that never kept the trace id can still name the turn.
        from .platform import PlatformClient

        self._platform = platform if platform is not None else PlatformClient()
        self._last_trace: OrderedDict[str, str] = OrderedDict()
        # Evaluation trials' bindings, by trial conversation; see _resolve_subject.
        self._trial_subjects: _BoundedDict = _BoundedDict(LAST_TRACE_LIMIT)
        # What each paused conversation is waiting on, by conversation id: the
        # interrupt ids, when each expires, and the message the turn was asked
        # with, which a resume replays. In memory, like the trial bindings: a
        # pod that restarts forgets it, and a resume then replays whatever user
        # message the client sent, unchecked.
        self._pending_interrupts: _BoundedDict = _BoundedDict(LAST_TRACE_LIMIT)

        # framework: explicit arg > AGENT_FRAMEWORK env (platform-injected) >
        # 'custom'. Drives which OpenInference instrumentor tracing activates.
        self.framework = resolve_framework(framework)
        # platform-injected when the deployment runs with tools mocked or
        # pointed at scratch resources; reported in the manifest so an eval
        # runner can verify it rather than trust a naming convention
        self._eval_mode = env_eval_mode()
        # The agent's declaration that its tools consult
        # evaluation.in_evaluation() per turn, so one deployment can serve
        # customers and be evaluated at once. Only the author can make that
        # promise, hence an argument and not a default. Irrelevant under
        # EVAL_MODE, which makes every turn an evaluation regardless.
        self._eval_per_request = eval_per_request
        # Verifies a claimed run with Hopsworks before a turn is treated as an
        # evaluation; see evaluation.py for why a bare header is not enough.
        self._eval_verifier = RunVerifier()
        # tracing: None auto-detects from the platform-injected OTLP endpoint
        # env var (set iff tracing is enabled on the deployment)
        self.tracer_provider = setup_tracing(self.framework, enabled=tracing)
        if self.tracer_provider is not None:
            # The batch processor exports on its own thread, so the last turn's
            # spans can still be queued when the pod is told to stop. SIGTERM ends
            # the lifespan before the process exits, which is the last point at
            # which the queue can be drained in an orderly way -- the SDK's own
            # atexit hook runs later, and after the event loop is gone.
            @self.on_event("shutdown")
            async def _flush_spans() -> None:  # pragma: no cover - lifespan hook
                import logging  # noqa: PLC0415 -- the module's one logger use

                try:
                    await asyncio.to_thread(self.tracer_provider.force_flush, 5000)
                except Exception:  # noqa: BLE001 -- shutdown must not raise
                    logging.getLogger(__name__).debug(
                        "could not flush spans on shutdown", exc_info=True
                    )

        # optional conversation memory: the user message is recorded when the
        # turn opens and the reply when it closes, so a failed turn is marked
        # abandoned rather than leaving a question with no answer. Handlers read
        # history with self.memory.get(request.conversation_id). Skip it if your
        # framework persists state itself (e.g. a LangGraph checkpointer).
        # Code wins over configuration. An explicit store is used as passed,
        # an explicit None stays off, and only an agent that said nothing takes
        # the deployment's policy (AGENT_MEMORY_CONFIG) -- which is what lets an
        # agent be given memory without editing it. Reading it is still the
        # handler's call: see memory_config for why that cannot be automatic.
        if isinstance(memory, _NotGiven):
            self.memory = memory_from_env()
            self._memory_source = "deployment" if self.memory is not None else None
        else:
            self.memory = memory
            self._memory_source = "code" if memory is not None else None
        self._chat_handler: ChatHandler | None = None
        self._stream_handler: StreamHandler | None = None

        # when tool events are on and tracing is active, auto-emit tool events
        # from the framework's spans so tool calls show up with zero agent code
        if self._tool_events and self.tracer_provider is not None:
            from .autoevents import install_auto_tool_events

            self._auto_tool_events = install_auto_tool_events(self.tracer_provider)

        if allow_cors:
            self.add_middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_methods=["*"],
                allow_headers=["*"],
            )

        self._register_routes()

    # ── handler decorators ────────────────────────────────────────────────

    @public
    def chat(self, handler: ChatHandler) -> ChatHandler:
        """Register the chat handler.

        The handler receives a :class:`ChatRequest` whose ``conversation_id``
        is always set (generated on the first turn) and returns a
        :class:`ChatResponse` (see :class:`AgentResponse`) or a plain string.
        """
        self._chat_handler = handler
        return handler

    @public
    def stream(self, handler: StreamHandler) -> StreamHandler:
        """Register a streaming handler: an async generator yielding text.

        deltas (``str``). Optionally yield a final :class:`ChatResponse` to
        attach citations/usage/metadata to the completed message.

        Registering a stream handler turns on the ``streaming`` capability;
        if no plain chat handler is registered, ``/v1/chat`` is served by
        collecting the stream.
        """
        self._stream_handler = handler
        return handler

    @public
    def memory_tools(self, framework: str | None = None, include=None) -> list[Any]:
        """Framework-native ``remember`` / ``recall`` / ``forget`` / ``search`` tools.

        Add them to your agent's tool list yourself — the SDK cannot reach into
        an arbitrary framework's tools, and appending to them behind your back
        would be worse than asking::

            agent = create_react_agent(llm, [*tools, *app.memory_tools()])

        Defaults to the app's detected framework. ``include`` registers a
        subset by name, e.g. ``include=("recall", "search")`` for an agent that
        must not let the model write subject-scoped state — see
        :func:`~hopsworks_agents.protocol.tools.memory_tools`.
        """
        from .tools import memory_tools as _memory_tools

        return _memory_tools(framework or self.framework, include=include)

    @public
    def identity_tools(self, framework: str | None = None) -> list[Any]:
        """Framework-native ``identify`` tool, for agents that ask who they are with.

        Registered separately from :meth:`memory_tools` on purpose::

            agent = create_react_agent(llm, [*tools, *app.identity_tools()])

        What it records is what a traces view will show as the person on the
        other end of the conversation, and the caller is a model reading text a
        user typed. It labels only: an agent that must act on a verified
        identity should check the claim itself and call
        ``ctx.rebind_subject()``.
        """
        from .tools import identity_tools as _identity_tools

        return _identity_tools(framework or self.framework)

    # ── internals ─────────────────────────────────────────────────────────

    def _memory_capabilities(self) -> dict[str, Any]:
        """Which memory tiers this agent has, so the panel lights up the right.

        inspector views instead of guessing.

        The wire names predate the vocabulary the UI now uses: ``summary`` is
        short-term memory, ``state`` is semantic memory and ``search`` is
        episodic memory. They stay as they are — clients key off them.
        """
        if self.memory is None:
            return {
                "conversation_history": False,
                "summary": False,
                "state": False,
                "search": False,
                "source": None,
            }
        return {
            "source": self._memory_source,
            "conversation_history": True,
            "summary": getattr(self.memory, "_summarize", None) is not None,
            "state": bool(getattr(self.memory, "_long_term", False)),
            "search": getattr(self.memory, "_vector_store", None) is not None,
        }

    def _manifest(self) -> dict[str, Any]:
        """What this agent is and what it can do, for a client to discover it.

        AG-UI defines no discovery endpoint, so the manifest is ours, but its
        ``capabilities`` are AG-UI's ``AgentCapabilities``: a TypeScript client
        can hand them out of ``getCapabilities()`` as they are.
        What only Hopsworks reads is under ``capabilities.custom.hopsworks``,
        where ``streaming`` still means a stream handler is registered, so the
        answer arrives token by token rather than whole.
        """
        streaming = self._stream_handler is not None
        endpoints: dict[str, str] = {"run": "/v1/run", "chat": "/v1/chat"}
        if self.memory is not None and self._memory_routes:
            # server-managed history is available: clients can list/clear it
            endpoints["conversations"] = "/v1/conversations"
            if getattr(self.memory, "_long_term", False):
                # durable per-subject memory is inspectable and deletable
                endpoints["subjects"] = "/v1/subjects"
        if self._graph_spec is not None:
            endpoints["graph"] = "/v1/graph"
        # always served: a chat UI built on the gateway has nowhere else to send a verdict
        endpoints["feedback"] = "/v1/feedback"
        hopsworks: dict[str, Any] = {
            "streaming": streaming,
            # protocol-level statement that history is server-side. The
            # per-tier detail is under "memory" below; this was previously
            # hardcoded True even with no store configured, which was wrong.
            "conversation_history": self.memory is not None,
            "memory": self._memory_capabilities(),
            # server-side history is inspectable/clearable via the endpoints
            "conversation_management": self.memory is not None,
            "attachments": any(m != "text" for m in self._input_modalities),
            "input_modalities": self._input_modalities,
            "output_modalities": self._output_modalities,
            "citations": False,
            "tool_events": self._tool_events,
            # a structure graph is available to visualize the agent
            "graph": self._graph_spec is not None,
            # end users can rate a turn through the agent itself
            "feedback": True,
            # This agent continues an incoming W3C trace context and
            # propagates hopsworks.eval.* baggage onto its spans, so a
            # caller that generated the traceparent can find the trace
            # afterwards. False means tracing is off on the deployment;
            # *absent* means an SDK too old to do it at all. The eval
            # runner checks this before running a suite, rather than
            # discovering afterwards that every trial points at a trace
            # that was never created.
            "trace_correlation": self.tracer_provider is not None,
            # Running with tools mocked or pointed at scratch resources.
            # Lets the runner refuse to fire a sandboxed suite — one full
            # of injection and exfiltration attempts — at a deployment
            # whose tools can still mutate production systems.
            "eval_mode": self._eval_mode,
            # This agent reads hopsworks.eval.* baggage and skips its
            # production side effects for a turn that belongs to a
            # verified run — so a sandboxed suite may be fired at it even
            # while it serves real traffic. The runner accepts either this
            # or eval_mode.
            "eval_per_request": self._eval_per_request,
        }
        inputs = set(self._input_modalities)
        outputs = set(self._output_modalities)
        capabilities = AgentCapabilities.model_validate(
            {
                "identity": {
                    "name": self._agent_name,
                    "description": self._agent_description,
                    "version": self._agent_version,
                    "provider": "hopsworks",
                },
                "transport": {"streaming": True},
                "multimodal": {
                    "input": {
                        "image": "image" in inputs,
                        "audio": "audio" in inputs,
                        "file": "file" in inputs,
                    },
                    "output": {
                        "image": "image" in outputs,
                        "audio": "audio" in outputs,
                    },
                },
                "state": {
                    "memory": self.memory is not None,
                    "persistent_state": bool(getattr(self.memory, "_long_term", False)),
                },
                "human_in_the_loop": {"feedback": True},
                "custom": {"hopsworks": hopsworks},
            }
        )
        return {
            "protocol": PROTOCOL,
            "protocol_version": AGUI_PROTOCOL_VERSION,
            "hopsworks_version": PROTOCOL_VERSION,
            "agent": {
                "name": self._agent_name,
                "description": self._agent_description,
                "version": self._agent_version,
                "framework": self.framework,
            },
            "endpoints": endpoints,
            "capabilities": capabilities.model_dump(by_alias=True, exclude_none=True),
            "ui": {
                "welcome_message": self._welcome_message,
                "suggested_prompts": self._suggested_prompts,
                "placeholder": self._placeholder,
                "allow_markdown": True,
            },
        }

    def _turn_span_name(self) -> str:
        # OTel GenAI convention: "{operation} {target}"
        return f"{conventions.OPERATION_INVOKE_AGENT} {self._agent_name}"

    def _turn_span_attributes(self, ctx: HandlerContext) -> dict[str, Any]:
        """What is known before the handler runs.

        ``input.value`` matters more than it looks: it is the sidecar's
        last-resort message source, so a root span that sets it produces a
        correct transcript with no sidecar change — and retires the
        reconstruct-the-root-span-from-its-children heuristics for SDK agents.
        """
        attributes: dict[str, Any] = {
            conventions.SPAN_KIND: conventions.SPAN_KIND_AGENT,
            conventions.GEN_AI_OPERATION_NAME: conventions.OPERATION_INVOKE_AGENT,
            conventions.GEN_AI_AGENT_NAME: self._agent_name,
            conventions.GEN_AI_AGENT_VERSION: self._agent_version,
            conventions.GEN_AI_CONVERSATION_ID: ctx.conversation_id,
            conventions.CONVERSATION_ID: ctx.conversation_id,
            conventions.FRAMEWORK: ctx.framework,
        }
        if ctx.has_subject:
            # only when the client asserted one. The per-conversation fallback
            # is not an end user, and stamping it here would publish a
            # conversation id as somebody's identity.
            attributes[conventions.USER_ID] = ctx.subject
        if ctx.message_id:
            attributes[conventions.MESSAGE_ID] = ctx.message_id
        if ctx.deployment_id:
            attributes[conventions.DEPLOYMENT_ID] = ctx.deployment_id
        if ctx.request.resume:
            attributes[conventions.TURN_RESUMED] = [
                f"{a.interrupt_id}:{a.status}" for a in ctx.request.resume
            ]
        text = ctx.request.text
        if text:
            attributes[conventions.INPUT_VALUE] = text
        return attributes

    def _prepare(self, request: ChatRequest) -> HandlerContext:
        # handlers can always rely on conversation_id being present
        if not request.conversation_id:
            request.conversation_id = new_conversation_id()
        if request.message.id is None:
            from .models import new_message_id

            request.message.id = new_message_id()
        from .memory import new_turn_id

        # the response id is generated up front so ctx.response_id matches the
        # id on the response the client ultimately receives (correlation)
        return HandlerContext(
            request=request,
            memory=self.memory,
            framework=self.framework,
            response_id=new_response_id(),
            turn_id=new_turn_id(),
            subject=getattr(request, "subject", None),
            subjects=self._subjects,
            trial_subjects=self._trial_subjects,
        )

    async def _attach_evaluation(
        self, ctx: HandlerContext, headers: Any
    ) -> AgentError | None:
        """Decide whether this turn is an evaluation, from the request's baggage.

        Returns the refusal to send, or None to proceed. Called before the turn
        opens, so a refused trial records nothing — not even the question.

        The environment wins: under EVAL_MODE every turn is already an
        evaluation and the ids are kept only so ctx.evaluation can say which.
        Otherwise the baggage counts only if the app declared eval_per_request
        (an agent that never said its tools check would run them for real) and
        the run checks out with Hopsworks (a header anyone can send must not
        be enough to make the agent claim success while doing nothing).
        """
        trial = parse_baggage(headers)
        if trial is None:
            return None
        if self._eval_mode:
            ctx.evaluation = trial
            ctx._eval_labelled = True
            return None
        verified = await asyncio.to_thread(self._eval_verifier.verify, trial)
        if not self._eval_per_request:
            # The agent never promised that its tools check, so the turn is served
            # exactly as a customer's -- but if Hopsworks knows the run, the trace
            # is still a trial and is labelled as one. That is how a read-only
            # suite shows up as a run rather than as traffic. An id nobody can
            # vouch for labels nothing.
            ctx._eval_labelled = verified
            return None
        if not verified:
            return AgentError(
                f"Evaluation run {trial.run_id} could not be verified with "
                "Hopsworks, so this turn was not run: an unverified trial would "
                "either reach production systems or misreport what it did.",
                "eval_unverified",
                403,
            )
        ctx.evaluation = trial
        ctx._eval_labelled = True
        return None

    async def _resolve_subject(self, ctx: HandlerContext) -> None:
        """Fill in the person this conversation was bound to, when the client did not.

        A turn carries whatever the client asserted, and a client that reloaded
        has forgotten even that -- while an agent that identified the caller on
        an earlier turn bound durable memory to a subject the client was never
        told. Without this the next turn falls back to the conversation id, and
        the facts the agent filed under the person it worked out are invisible
        to every turn after the one that learned them.

        The store is the only thing that can answer it, so this is one read
        before the turn opens, and only for a turn that asserted no subject.
        """
        if ctx.has_subject:
            # The client said who this is, which is the one identity that actually
            # keys durable memory -- and it reached the span and nothing else, so
            # the traces table, feedback by user and failure analysis by user all
            # showed nothing while memory quietly followed the person. Recorded
            # here as the client's claim, which is what subject_source is for.
            if ctx.subject_source == "client":
                ctx.record_subject(ctx.subject, source="client")
            return
        if ctx._in_trial:
            # A trial's binding is kept in this process rather than read from the
            # store, which files a trial's messages under the trial so they never
            # join the person's own. Held here, the next turn of the trial finds the
            # person turn one identified, as the next turn of a real conversation
            # does. A deployment scaled to several replicas may send the next turn
            # elsewhere, which leaves it unbound as before.
            bound = self._trial_subjects.get(ctx.conversation_id)
            if bound:
                ctx.subject = bound
                ctx.has_subject = True
                ctx.subject_source = "conversation"
            return
        if self.memory is None:
            return
        try:
            subject = await asyncio.to_thread(
                self.memory.conversation_subject, ctx.conversation_id
            )
        except Exception:  # noqa: BLE001 -- memory must never break a turn
            import logging  # noqa: PLC0415 -- the module's one logger use

            logging.getLogger(__name__).exception(
                "Failed to read the conversation's subject"
            )
            return
        # A store with no per-row subject answers None, and a conversation
        # nobody identified answers with the fallback it was filed under.
        if not subject or subject == ctx.conversation_id:
            return
        ctx.subject = subject
        ctx.has_subject = True
        # the server's own record: neither the client's assertion nor a fallback
        ctx.subject_source = "conversation"

    async def _open_turn(self, ctx: HandlerContext) -> None:
        """Record the user message and open the turn, before the handler runs.

        Writing it up front is what lets a handler read back the message it is
        answering, and (from Phase 2) lets anything the agent remembers point at
        the turn that caused it. The row is invisible to ``get()`` until the
        turn closes, so an in-flight turn never shows up as a question with no
        answer.
        """
        if self.memory is None:
            return
        # A message with no text is still a message. Skipped, the turn never
        # opened, so neither the question nor the answer was recorded and a
        # conversation of photographs read back as empty. What goes in the
        # transcript is the text when there is any, and otherwise a note of
        # what arrived instead.
        recorded = ctx.request.text or _describe_parts(ctx.request)
        if not recorded:
            return
        try:
            await asyncio.to_thread(
                self.memory.begin_turn,
                ctx.conversation_id,
                ctx.turn_id,
                "user",
                recorded,
                message_id=ctx.message_id,
                subject=ctx._stamp_subject,
            )
            ctx._turn_open = True
        except Exception:  # noqa: BLE001 — memory must never break a turn
            import logging

            logging.getLogger(__name__).exception("Failed to open memory turn")

    async def _finalize_turn(
        self, ctx: HandlerContext, response: ChatResponse | None
    ) -> None:
        """Close the turn. Must run on every exit path, including failures.

        The pre-inserted user message makes this obligatory rather than
        housekeeping: skip it and the store keeps a question whose answer never
        arrived, which then reappears as history on the next turn.
        """
        if response is not None:
            # answered: whatever the conversation was waiting on is behind it
            self._pending_interrupts.pop(ctx.conversation_id, None)
        if self.memory is None or not ctx._turn_open:
            return
        ctx._turn_open = False
        try:
            completed = response is not None and response.status == "completed"
            if completed:
                await asyncio.to_thread(self._write_turn_items, ctx, response)
            await asyncio.to_thread(
                self.memory.end_turn,
                ctx.conversation_id,
                ctx.turn_id,
                status=TURN_CLOSED if completed else TURN_ABANDONED,
            )
            # After the last token: summarizing and embedding cost request
            # duration, never time-to-answer. Both are awaited rather than
            # fired off, because a scale-to-zero pod would take an un-awaited
            # task with it.
            if completed:
                await self.memory.ingest_turn(ctx.conversation_id, ctx.turn_id)
                await self.memory.maybe_summarize(ctx.conversation_id)
            await asyncio.to_thread(self.memory.maybe_reap)
        except Exception:  # noqa: BLE001 — memory failures must not break chat
            import logging

            logging.getLogger(__name__).exception("Failed to finalize memory turn")

    def _write_turn_items(self, ctx: HandlerContext, response: ChatResponse) -> None:
        """Assistant reply + any tool events, in emission order."""
        for event in ctx._recorded_events:
            self.memory.record_item(
                ctx.conversation_id,
                ctx.turn_id,
                "tool",
                json.dumps(event),
                memory_type="event",
                message_id=ctx.message_id,
                subject=ctx._stamp_subject,
            )
        answer = "".join(
            part.text for part in response.message.content if part.type == "text"
        )
        if answer:
            self.memory.record_item(
                ctx.conversation_id,
                ctx.turn_id,
                "assistant",
                answer,
                message_id=ctx.message_id,
                subject=ctx._stamp_subject,
            )

    def _finalize(
        self, result: ChatResponse | str, ctx: HandlerContext
    ) -> ChatResponse:
        from .models import AgentResponse

        response = AgentResponse.text(result) if isinstance(result, str) else result
        if not response.conversation_id:
            response.conversation_id = ctx.conversation_id
        response.id = ctx.response_id
        # non-streaming tool events (buffered on the context) ride on metadata
        if ctx._event_buffer:
            response.metadata.setdefault("tool_events", ctx._event_buffer)
        # Which subject this turn's memory was actually keyed on. Only reported
        # when the agent identified the user itself (rebind_subject): the client
        # already knows the subject it asserted, but it cannot know one the
        # agent derived mid-turn — and without this a memory inspector reads the
        # subject it sent, finds an empty bucket, and reports "nothing stored"
        # about a user the agent has been happily remembering.
        if ctx.subject_source == "app":
            response.metadata.setdefault("subject", ctx.subject)
        _annotate_span(ctx, response)
        self._remember_trace(response)
        return response

    def _merge_stream_result(
        self,
        chunks: list[str],
        final: ChatResponse | None,
        ctx: HandlerContext,
    ) -> ChatResponse:
        """A final ChatResponse yielded by a stream handler may carry only.

        citations/usage; fill its text from the streamed chunks when empty.
        """
        if final is None:
            return self._finalize("".join(chunks), ctx)
        if contentful(final):
            streamed = "".join(chunks)
            has_text = any(
                part.type == "text" and part.text for part in final.message.content
            )
            if streamed and not has_text:
                # final carried only non-text parts: keep the streamed text too
                from .models import TextContent

                final.message.content.insert(0, TextContent(text=streamed))
            return self._finalize(final, ctx)
        # The final response carried no content, so the streamed chunks are the
        # answer — but everything else about the turn is still the handler's
        # verdict on it. Its status most of all: a handler that streams a
        # partial answer and then yields ChatResponse(status="failed") is
        # reporting a failure, and building a fresh completed response over it
        # told the caller, the memory and the span that the turn went fine.
        #
        # Carried before finalization, not after, so the status the span records
        # and the answer the memory stores are the ones the handler meant.
        # _finalize only fills in what is missing (tool_events, trace_id, the
        # resolved subject), so the handler's own keys still win.
        from .models import AgentResponse

        merged = AgentResponse.text("".join(chunks))
        merged.status = final.status
        merged.citations = final.citations
        merged.usage = final.usage
        merged.metadata = {**final.metadata}
        if final.conversation_id:
            merged.conversation_id = final.conversation_id
        return self._finalize(merged, ctx)

    def _invoke_stream(self, ctx: HandlerContext) -> AsyncIterator[str | ChatResponse]:
        assert self._stream_handler is not None
        if _wants_context(self._stream_handler):
            return self._stream_handler(ctx.request, ctx)
        return self._stream_handler(ctx.request)

    async def _run_chat(self, ctx: HandlerContext) -> ChatResponse:
        from .autoevents import current_context

        token = current_context.set(ctx)
        try:
            if self._chat_handler is not None:
                handler = self._chat_handler
                call = (
                    (lambda: handler(ctx.request, ctx))
                    if _wants_context(handler)
                    else (lambda: handler(ctx.request))
                )
                if inspect.iscoroutinefunction(handler):
                    result = await call()
                else:
                    # A plain `def` handler is the documented form, and what it
                    # does is a blocking model or tool call lasting seconds.
                    # Called inline it holds the event loop for the whole turn:
                    # every other conversation waits, and so do /health and
                    # /ready — a liveness probe that times out restarts the pod
                    # in the middle of someone's chat. asyncio.to_thread copies
                    # the context, so current_context, the turn's span and the
                    # tool-event buffer are all visible inside the thread.
                    result = await asyncio.to_thread(call)
                if inspect.isawaitable(result):
                    # a sync function that returned a coroutine: it has not
                    # started, so awaiting it here runs it on the loop
                    result = await result
                return self._finalize(result, ctx)
            # collect the stream into a single response
            chunks: list[str] = []
            final: ChatResponse | None = None
            async for item in self._invoke_stream(ctx):
                if isinstance(item, ChatResponse):
                    final = item
                else:
                    chunks.append(item)
            return self._merge_stream_result(chunks, final, ctx)
        finally:
            current_context.reset(token)

    def _check_resume(
        self, conversation_id: str | None, answers: list[ResumeAnswer]
    ) -> tuple[ChatMessage | None, list[ResumeAnswer]]:
        """The message a resumed turn replays and every answer it has, after checking them.

        A turn that asks twice pauses twice, and a client answers only what is
        open, as AG-UI asks; the answers from earlier pauses are kept here and
        handed back, since the replay asks those questions again too.

        AG-UI asks a resume to answer every open interrupt and nothing else, and
        a question past its expiry cannot be answered any more. Returns None
        when the request resumes nothing, or resumes a pause this process does
        not know about (it restarted); the turn then replays the user message
        the client sent.

        Raises:
            AgentError: The answers leave a question open, answer one that was
                not asked, or answer one that has expired.
        """
        if not answers:
            return None, answers
        pending = self._pending_interrupts.get(conversation_id or "")
        if pending is None:
            return None, answers
        earlier = {a.interrupt_id: a for a in pending["answers"]}
        answers = [a for a in answers if a.interrupt_id not in earlier]
        asked = set(pending["expires"])
        answered = {answer.interrupt_id for answer in answers}
        if asked - answered:
            raise AgentError(
                "The resume leaves these questions unanswered: "
                + ", ".join(sorted(asked - answered)),
                "unanswered_interrupts",
                400,
            )
        if answered - asked:
            raise AgentError(
                "The resume answers questions this conversation was not asked: "
                + ", ".join(sorted(answered - asked)),
                "unknown_interrupt",
                400,
            )
        from datetime import datetime, timezone  # noqa: PLC0415

        now = datetime.now(timezone.utc).isoformat()
        expired = sorted(i for i, at in pending["expires"].items() if at and at < now)
        if expired:
            raise AgentError(
                "These questions expired before they were answered: "
                + ", ".join(expired),
                "interrupt_expired",
                400,
            )
        return pending["message"], [*earlier.values(), *answers]

    def _pause(self, ctx: HandlerContext, span: Any, interrupts: list[dict]) -> None:
        """Remember what the conversation now waits on, and say so on the span."""
        self._pending_interrupts[ctx.conversation_id] = {
            "message": ctx.request.message,
            "expires": {q["id"]: q.get("expires_at") for q in interrupts},
            "answers": list(ctx.request.resume),
        }
        _set_span_attribute(
            span, conventions.TURN_INTERRUPTS, [q["id"] for q in interrupts]
        )

    def _register_routes(self) -> None:
        @self.get("/.well-known/hopsworks-agent.json")
        async def manifest() -> dict[str, Any]:
            return self._manifest()

        @self.get("/health")
        async def health() -> dict[str, str]:
            # lightweight liveness: the process is up
            return {"status": "ok"}

        @self.get("/ready")
        async def ready() -> JSONResponse:
            # operational readiness: everything needed to actually serve chat
            checks = {
                "handler": self._chat_handler is not None
                or self._stream_handler is not None,
                "memory": self.memory is None or self.memory.healthcheck(),
            }
            ok = all(checks.values())
            return JSONResponse(
                {"status": "ready" if ok else "not_ready", "checks": checks},
                status_code=200 if ok else 503,
            )

        @self.post("/v1/chat")
        async def chat_route(request: ChatRequest, raw: Request) -> JSONResponse:
            if self._chat_handler is None and self._stream_handler is None:
                return _error_response(
                    AgentError("No chat handler registered.", "not_implemented", 501)
                )
            try:
                replay, request.resume = self._check_resume(
                    request.conversation_id, request.resume
                )
            except AgentError as err:
                return _error_response(err)
            if replay is not None:
                request.message = replay
            if request.message is None:
                return _error_response(
                    AgentError(
                        "The request has no message, and resumes no paused turn.",
                        "no_message",
                        422,
                    )
                )
            ctx = self._prepare(request)
            refused = await self._attach_evaluation(ctx, raw.headers)
            if refused is not None:
                return _error_response(refused)
            await self._resolve_subject(ctx)
            await self._open_turn(ctx)
            response: ChatResponse | None = None
            with (
                eval_active(ctx.evaluation),
                turn_span(
                    self.tracer_provider,
                    name=self._turn_span_name(),
                    headers=raw.headers,
                    attributes=self._turn_span_attributes(ctx),
                    # Only a turn this agent accepted as an evaluation carries the
                    # eval ids onto its spans. Baggage comes over the wire, and those
                    # attributes are what the platform reads back to tell a test from
                    # a customer -- so an unverified header must not set them.
                    keep_eval_baggage=ctx._eval_labelled,
                ) as span,
            ):
                ctx._span = span
                try:
                    response = await self._run_chat(ctx)
                except Interrupted as paused:
                    # Not finalized as an answer: the turn is abandoned, and the
                    # resume asks it again, so the transcript holds it once.
                    self._pause(ctx, span, paused.interrupts)
                    interrupted = AgentResponse.text(
                        "", conversation_id=ctx.conversation_id
                    )
                    interrupted.status = "interrupted"
                    interrupted.interrupts = paused.interrupts
                    interrupted.id = ctx.response_id
                    trace_id = _trace_id(span)
                    if trace_id:
                        interrupted.metadata["trace_id"] = trace_id
                    return JSONResponse(interrupted.model_dump())
                except AgentError as err:
                    _record_span_error(span, err)
                    return _error_response(err)
                except Exception as err:  # noqa: BLE001 -- answered, not swallowed
                    # Said in the body, as the stream route says it in its error
                    # event. Left to the framework it was a bare "500 Internal Server
                    # Error": a model refusing a parameter on every request read the
                    # same as a crash, and the reason was only in the pod log.
                    import logging  # noqa: PLC0415

                    logging.getLogger(__name__).exception("the chat handler failed")
                    failure = AgentError(
                        f"{type(err).__name__}: {err}", "agent_error", 500
                    )
                    _record_span_error(span, failure)
                    return _error_response(failure)
                finally:
                    # in a finally because an AgentError must not leave the turn
                    # open: its user message is already recorded
                    await self._finalize_turn(ctx, response)
            return JSONResponse(response.model_dump())

        @self.post("/v1/run")
        async def run_route(run: RunAgentInput, raw: Request) -> Response:
            # Refusals before the run starts are plain HTTP errors, as AG-UI
            # clients expect: nothing has streamed yet, so there is no run to end.
            if self._chat_handler is None and self._stream_handler is None:
                return _error_response(
                    AgentError("No chat handler registered.", "not_implemented", 501)
                )
            try:
                replay, answers = self._check_resume(
                    run.thread_id,
                    [_resume_answer(entry) for entry in run.resume or []],
                )
                request = _to_chat_request(
                    run,
                    server_history=self.memory is not None,
                    replay=replay,
                    answers=answers,
                )
            except AgentError as err:
                return _error_response(err)
            ctx = self._prepare(request)
            refused = await self._attach_evaluation(ctx, raw.headers)
            if refused is not None:
                return _error_response(refused)
            await self._resolve_subject(ctx)
            await self._open_turn(ctx)
            stream = RunStream(
                ctx.conversation_id,
                request.run_id or ctx.turn_id,
                accept=raw.headers.get("accept"),
            )
            return StreamingResponse(
                self._run_events(ctx, raw, stream), media_type=stream.media_type
            )

        self._register_feedback_route()
        if self.memory is not None and self._memory_routes:
            self._register_conversation_routes()

        if self._graph_spec is not None:

            @self.get("/v1/graph")
            async def graph() -> dict[str, Any]:
                return self._graph_spec

    def _remember_trace(self, response: ChatResponse) -> None:
        trace_id = (response.metadata or {}).get("trace_id")
        if not trace_id or not response.conversation_id:
            return
        self._last_trace[response.conversation_id] = str(trace_id)
        self._last_trace.move_to_end(response.conversation_id)
        while len(self._last_trace) > LAST_TRACE_LIMIT:
            self._last_trace.popitem(last=False)

    def _register_feedback_route(self) -> None:
        @self.post("/v1/feedback")
        async def feedback_route(request: FeedbackRequest) -> JSONResponse:
            try:
                trace_id = await asyncio.to_thread(self._resolve_trace, request)
                body = {
                    "verdict": request.verdict,
                    "issueCategory": request.issue_category,
                    "correctedAnswer": request.corrected_answer,
                    "expectedToolBehavior": request.expected_tool_behavior,
                    "note": request.note,
                    # filed under the end user, never under the deployment's own account
                    "reviewer": (request.subject or "").strip() or "anonymous",
                }
                stored = await asyncio.to_thread(
                    self._platform.post_feedback,
                    trace_id,
                    {k: v for k, v in body.items() if v is not None},
                )
            except AgentError as err:
                return _error_response(err)
            return JSONResponse(
                {
                    "feedback_id": (stored or {}).get("feedbackId"),
                    "trace_id": trace_id,
                    "verdict": request.verdict,
                    "reviewer": (stored or {}).get("reviewer"),
                }
            )

    def _resolve_trace(self, request: FeedbackRequest) -> str:
        """The turn a verdict is about: the trace named, or the conversation's latest."""
        if request.trace_id:
            return request.trace_id
        if not request.conversation_id:
            raise AgentError(
                "name the turn: trace_id from the reply, or conversation_id",
                code="invalid_request",
                status_code=400,
            )
        remembered = self._last_trace.get(request.conversation_id)
        if remembered:
            return remembered
        found = self._platform.latest_trace_id(request.conversation_id)
        if not found:
            raise AgentError(
                f"no trace recorded yet for conversation {request.conversation_id}",
                code="trace_not_found",
                status_code=404,
            )
        return found

    def _register_conversation_routes(self) -> None:
        @self.get("/v1/conversations")
        async def list_conversations(
            subject: str = "", limit: int = 50
        ) -> dict[str, Any]:
            # The manifest has always advertised this path; without it a client
            # can only show conversations it happens to remember locally, so
            # clearing a browser's storage loses a transcript the server still
            # holds in full.
            if self.memory is None:
                return {"conversations": []}
            conversations = await asyncio.to_thread(
                self.memory.list_conversations,
                subject=subject or None,
                limit=max(1, min(limit, 500)),
            )
            return {"conversations": conversations}

        @self.get("/v1/conversations/{conversation_id}/messages")
        async def list_messages(
            conversation_id: str, include: str = ""
        ) -> dict[str, Any]:
            # The human-facing record, which is NOT the same as what the model
            # reads: once turns are folded they leave ctx.history but stay here.
            # `summary` carries the folded part and `summarized_through` marks
            # where the two diverge, so a UI can render "older context,
            # compacted" instead of appearing to have lost messages.
            assert self.memory is not None
            include_events = "events" in {part.strip() for part in include.split(",")}
            messages = await asyncio.to_thread(
                self.memory.transcript,
                conversation_id,
                include_events=include_events,
            )
            summary = await asyncio.to_thread(self.memory.get_summary, conversation_id)
            cutoff = await asyncio.to_thread(
                self.memory.summarized_through, conversation_id
            )
            # The subject the server actually filed this conversation under,
            # which a client cannot derive: it may have been rebound mid-turn
            # to an identity the client was never told, and a client that
            # reloaded has forgotten even the subject it asserted.
            subject = await asyncio.to_thread(
                self.memory.conversation_subject, conversation_id
            )
            return {
                "conversation_id": conversation_id,
                "messages": messages,
                "summary": summary,
                "summarized_through": cutoff,
                "subject": subject,
            }

        @self.delete("/v1/conversations/{conversation_id}")
        async def clear_conversation(conversation_id: str) -> JSONResponse:
            # "new session" on the client can drop server-side memory too.
            # Session-scoped state goes with it; user- and app-scoped state does
            # NOT — those are subject/agent-scoped, and starting a new chat must
            # not erase what the agent knows about the person. Forgetting that
            # is a separate, explicit action (the /subjects routes below).
            assert self.memory is not None
            try:
                await asyncio.to_thread(self.memory.clear, conversation_id)
                await asyncio.to_thread(
                    self.memory.delete_state, "session", conversation_id
                )
            except MemoryEraseError as err:
                # A database outage answered 204 and the conversation was still
                # there on the next read. Erasure is the one operation where an
                # unreported failure is worse than an error.
                return _error_response(
                    AgentError(
                        f"the conversation could not be deleted: {err}",
                        code="memory_erase_failed",
                        status_code=500,
                        retryable=True,
                    )
                )
            # the vector store holds a copy of the content, so deleting from
            # SQL alone would leave it searchable
            try:
                await asyncio.to_thread(
                    self.memory.purge_vectors, conversation_id=conversation_id
                )
            except Exception as err:  # noqa: BLE001 -- answered, not swallowed
                # 204 here told the caller their data was gone while copies of
                # it were still searchable. The transcript is deleted either
                # way; what failed is the erasure the caller actually cares
                # about, so it is reported rather than logged.
                return _error_response(
                    AgentError(
                        "the conversation was deleted but its searchable copies "
                        f"could not be removed: {err}",
                        code="vector_purge_failed",
                        status_code=500,
                        retryable=True,
                    )
                )
            return JSONResponse(status_code=204, content=None)

        @self.delete("/v1/conversations/{conversation_id}/summary")
        async def clear_summary(conversation_id: str) -> JSONResponse:
            # The summary is written by a model out of what a user typed, and
            # it lands in every later system prompt — so a user who talks the
            # agent into a false one has put it somewhere that outlives the
            # turn. Facts can be dropped one key at a time; before this, the
            # only way out of a bad summary was deleting the conversation.
            assert self.memory is not None
            dropped = await asyncio.to_thread(
                self.memory.clear_summary, conversation_id
            )
            return JSONResponse(
                {"conversation_id": conversation_id, "dropped": dropped}
            )

        @self.get("/v1/subjects/{subject}/state")
        async def list_subject_state(subject: str) -> dict[str, Any]:
            # The audit half of durable memory. Model-written state is
            # attacker-influenceable — a user can talk the agent into
            # remembering something false about them, and it then loads into
            # every later conversation. Caps and TTLs bound that; being able to
            # see and delete it is what actually fixes it, so this ships with
            # the feature rather than after it.
            assert self.memory is not None
            values = await asyncio.to_thread(self.memory.list_state, "user", subject)
            return {"subject": subject, "state": values}

        @self.delete("/v1/subjects/{subject}/state")
        async def clear_subject_state(
            subject: str, key: str | None = None
        ) -> JSONResponse:
            assert self.memory is not None
            try:
                removed = await asyncio.to_thread(
                    self.memory.delete_state, "user", subject, key
                )
            except MemoryEraseError as err:
                return _error_response(
                    AgentError(
                        f"this subject's stored facts could not be deleted: {err}",
                        code="memory_erase_failed",
                        status_code=500,
                        retryable=True,
                    )
                )
            vectors = 0
            if key is None:
                # forgetting a whole subject includes their embedded messages
                try:
                    vectors = await asyncio.to_thread(
                        self.memory.purge_vectors, subject=subject
                    )
                except Exception as err:  # noqa: BLE001 -- answered, not swallowed
                    return _error_response(
                        AgentError(
                            f"{removed} stored facts were removed but this "
                            "subject's searchable copies could not be: "
                            f"{err}",
                            code="vector_purge_failed",
                            status_code=500,
                            retryable=True,
                        )
                    )
            return JSONResponse({"removed": removed, "vectors_removed": vectors})

    async def _run_events(
        self, ctx: HandlerContext, raw: Request, stream: RunStream
    ) -> AsyncIterator[str]:
        # One AG-UI run. With no stream handler the chat handler runs and its
        # answer goes out whole, so a caller never has to ask which was written.
        #
        # Every exit below — normal completion, AgentError, handler crash,
        # client disconnect — has to end the turn, because its user message is
        # already in the store. Hence one finally around the whole generator.
        #
        # One generator, deliberately: delegating the body to an inner async
        # generator reads better but breaks the disconnect path. GeneratorExit
        # at the yield unwinds this frame, and an inner generator's finally
        # would then be deferred to async-generator finalization instead of
        # running here — leaving the turn open with its question already
        # recorded, until the stale-turn reaper eventually caught it.
        #
        # The span is opened here rather than in the route for the
        # mirror-image reason: the route returns as soon as the
        # StreamingResponse is constructed, so a span opened there would
        # close before the first token.
        #
        # The evaluation flag is activated here too, and for the same reason:
        # the pump task below is created inside this frame and inherits the
        # context, so tools running under the handler see in_evaluation().
        with (
            eval_active(ctx.evaluation),
            turn_span(
                self.tracer_provider,
                name=self._turn_span_name(),
                headers=raw.headers,
                attributes=self._turn_span_attributes(ctx),
                keep_eval_baggage=ctx._eval_labelled,
            ) as span,
        ):
            ctx._span = span
            trace_id = _trace_id(span)
            completed: ChatResponse | None = None
            try:
                yield stream._started(trace_id)
                from .autoevents import current_context

                # The handler runs as a task that feeds a queue, so progress
                # events (ctx.emit_event, automatic tool calls) go out while it
                # works rather than after it.
                queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
                ctx._event_queue = queue
                ctx._loop = asyncio.get_running_loop()

                async def pump() -> None:
                    token = current_context.set(ctx)
                    try:
                        if self._stream_handler is None:
                            response = await self._run_chat(ctx)
                            await queue.put(("final", response))
                        else:
                            async for item in self._invoke_stream(ctx):
                                await queue.put(("item", item))
                    except Interrupted as paused:
                        await queue.put(("interrupt", paused.interrupts))
                    except AgentError as err:
                        await queue.put(("error", err))
                    except Exception as err:  # noqa: BLE001 — surfaced to the client
                        import logging  # noqa: PLC0415

                        logging.getLogger(__name__).exception("the handler failed")
                        await queue.put(
                            (
                                "error",
                                AgentError(
                                    f"{type(err).__name__}: {err}", "agent_error", 500
                                ),
                            )
                        )
                    finally:
                        current_context.reset(token)
                        await queue.put(("done", None))

                task = asyncio.create_task(pump())
                chunks: list[str] = []
                final: ChatResponse | None = None
                answered: ChatResponse | None = None
                try:
                    while True:
                        kind, payload = await queue.get()
                        if kind == "done":
                            break
                        if await raw.is_disconnected():
                            # Says so on the span: the turn ended without an
                            # answer, and left unmarked it counts as a success
                            # with a short latency -- the shape of a fast,
                            # healthy reply.
                            _mark_cancelled(span)
                            task.cancel()
                            return
                        if kind == "error":
                            _record_span_error(span, payload)
                            yield stream._error(payload, trace_id=trace_id)
                            return
                        if kind == "interrupt":
                            # The run ends here with the question; the turn is
                            # abandoned and asked again when it is resumed.
                            self._pause(ctx, span, payload)
                            yield stream._interrupted(payload, trace_id=trace_id)
                            return
                        if kind == "tool_event":
                            yield stream._progress(payload)
                        elif kind == "final":
                            answered = payload
                        elif isinstance(payload, ChatResponse):
                            final = payload
                        else:
                            chunks.append(payload)
                            yield stream._text(payload)
                except asyncio.CancelledError:
                    # What a client hanging up actually looks like. Starlette
                    # cancels the generator on disconnect, so the await above
                    # raises here and the is_disconnected() branch never runs.
                    # Unmarked, the span ended UNSET -- the shape of a fast,
                    # healthy reply -- and the turn stayed open until the
                    # reaper called it abandoned much later.
                    _mark_cancelled(span)
                    raise
                finally:
                    if not task.done():
                        task.cancel()

                response = answered or self._merge_stream_result(chunks, final, ctx)
                completed = response
                yield stream._finished(
                    response,
                    subject=ctx.subject if ctx.has_subject else None,
                    subject_source=ctx.subject_source if ctx.has_subject else None,
                )
            finally:
                # Best effort: if the whole task tree is being cancelled this may
                # not get to run, which is what the store's stale-turn reaper is
                # there to catch.
                await self._finalize_turn(ctx, completed)


def _set_span_attribute(span: Any, key: str, value: Any) -> None:
    if span is None:
        return
    import contextlib  # noqa: PLC0415

    with contextlib.suppress(Exception):  # never let telemetry break a turn
        span.set_attribute(key, value)


def _trace_id(span: Any) -> str | None:
    """The turn's trace id as a hex string, or None when the turn is untraced."""
    if span is None:
        return None
    try:
        context = span.get_span_context()
    except Exception:  # noqa: BLE001 — correlation is best-effort
        return None
    if not getattr(context, "is_valid", False):
        return None
    return format(context.trace_id, "032x")


def _stamp_request_context(span: Any, ctx: HandlerContext) -> None:
    """The client's request context, as JSON, for a promoted task to replay.

    Left off when empty, and when too large to keep whole: a cut JSON object is
    not a smaller context, it is no context, and a replay given one would fail
    for a reason the original turn never had.
    """
    context = getattr(ctx.request, "context", None) or {}
    if not context:
        return
    try:
        text = json.dumps(context, default=str)
    except (TypeError, ValueError):
        return
    if len(text) <= conventions.MAX_REQUEST_CONTEXT_CHARS:
        span.set_attribute(conventions.REQUEST_CONTEXT, text)


def _describe_parts(request: Any) -> str:
    """What a message carried, for a transcript, when it carried no text.

    Not the content itself: an image is megabytes of base64 and belongs in the
    request, not in a chat log. A reader needs to know a message happened and
    roughly what it was.
    """
    counts: OrderedDict[str, int] = OrderedDict()
    for part in request.message.content:
        if part.type == "text":
            continue
        counts[part.type] = counts.get(part.type, 0) + 1
    if not counts:
        return ""
    return (
        "["
        + ", ".join(
            kind if count == 1 else f"{count} {kind}s" for kind, count in counts.items()
        )
        + "]"
    )


@public
def contentful(response: ChatResponse) -> bool:
    # any non-text part counts as content; text parts count when non-empty
    return any(part.type != "text" or part.text for part in response.message.content)


class _BoundedDict(OrderedDict):
    """A dict that forgets its oldest entries past a limit."""

    def __init__(self, limit: int):
        super().__init__()
        self._limit = limit

    def __setitem__(self, key: Any, value: Any) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)
        while len(self) > self._limit:
            self.popitem(last=False)


def _error_response(err: AgentError) -> JSONResponse:
    return JSONResponse({"detail": err.detail()}, status_code=err.status_code)


def _mark_cancelled(span: Any) -> None:
    """Record that the caller hung up, without calling the turn an error."""
    if span is None:
        return
    import contextlib

    with contextlib.suppress(Exception):  # never let telemetry break a turn
        span.set_attribute(conventions.TURN_CANCELLED, True)


def _record_span_error(span: Any, err: Exception) -> None:
    """Mark the turn span as failed. Trace status is how the sidecar's error.

    metrics and the eval runner's failure classification tell a broken turn
    from a slow one, so a handled AgentError still has to land on the span.
    """
    if span is None:
        return
    try:
        from opentelemetry.trace import Status, StatusCode

        span.set_status(Status(StatusCode.ERROR, str(err)))
        span.record_exception(err)
    except Exception:  # noqa: BLE001 — never let telemetry break a turn
        pass


def _annotate_span(ctx: HandlerContext, response: ChatResponse) -> None:
    """Close out the turn span with what only the finished response knows, and.

    surface the trace id in the response metadata so a caller can join this
    turn to its trace without parsing headers.

    Prefers the turn span the route opened over the ambient current span: by
    the time this runs the framework's own spans have ended, so
    ``get_current_span()`` is only right because the turn span is still open —
    and relying on that was what made this function a silent no-op before the
    turn span existed. No-op when tracing is not active.
    """
    try:
        from opentelemetry import trace
    except ImportError:
        return
    span = ctx._span if ctx._span is not None else trace.get_current_span()
    if span is None:
        return
    ctxt = span.get_span_context()
    if not getattr(ctxt, "is_valid", False):
        return
    try:
        # Re-stamped rather than left to the turn span's start attributes: when
        # the SDK is untraced but the agent instruments itself, the span found
        # here is the agent's own and has never seen them.
        span.set_attribute(conventions.CONVERSATION_ID, ctx.conversation_id)
        span.set_attribute(conventions.FRAMEWORK, ctx.framework)
        if ctx.message_id:
            span.set_attribute(conventions.MESSAGE_ID, ctx.message_id)
        if ctx.deployment_id:
            span.set_attribute(conventions.DEPLOYMENT_ID, ctx.deployment_id)
        span.set_attribute(conventions.RESPONSE_ID, response.id)
        # Who the turn ran as, as it ended rather than as it began: an agent
        # that identified the person mid-turn ran its memory reads under them.
        if ctx.has_subject:
            span.set_attribute(conventions.USER_ID, ctx.subject)
            span.set_attribute(conventions.SUBJECT_SOURCE, ctx.subject_source)
        _stamp_request_context(span, ctx)
        # the authoritative final answer, straight from the response object,
        # rather than reconstructed downstream from whichever child span
        # happened to look most like an answer
        answer = "".join(
            part.text for part in response.message.content if part.type == "text"
        )
        if answer:
            span.set_attribute(conventions.OUTPUT_VALUE, answer)
        # Under hopsworks.turn.*, not gen_ai.usage.*: this span is the turn,
        # not a model call, and every reader that counts tokens was counting
        # the turn once as itself and again as each LLM call inside it. The
        # flag says whether those LLM calls exist to be counted.
        usage = response.usage or {}
        for key, attr in (
            ("input_tokens", conventions.TURN_INPUT_TOKENS),
            ("prompt_tokens", conventions.TURN_INPUT_TOKENS),
            ("output_tokens", conventions.TURN_OUTPUT_TOKENS),
            ("completion_tokens", conventions.TURN_OUTPUT_TOKENS),
        ):
            if isinstance(usage.get(key), int):
                span.set_attribute(attr, usage[key])
        if usage:
            span.set_attribute(conventions.TURN_LLM_SPANS, bool(ctx._saw_llm_span))
        if response.status == "failed":
            _record_span_error(span, AgentError("Handler returned status=failed"))
        response.metadata.setdefault("trace_id", format(ctxt.trace_id, "032x"))
    except Exception:  # noqa: BLE001 — correlation is best-effort
        pass

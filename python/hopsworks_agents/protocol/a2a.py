"""A2A on the wire: an A2A message becomes a turn, a turn becomes task events.

``AgentApp`` serves the A2A 1.0 protocol (https://a2a-protocol.org) next to
AG-UI, so any A2A client, or another agent, can discover a Hopsworks agent and
delegate work to it.
The turn pipeline behind it is the one ``/v1/run`` and ``/v1/chat`` use; this
module is only the translation at its two ends, built on the official
``a2a-sdk`` so the wire format, the bindings and the task store are the
reference implementation's rather than ours.

Served, under the app's base URL:

- ``GET /.well-known/agent-card.json``: the Agent Card, built from the app's
  fields and skills.
- ``POST /a2a``: the JSON-RPC binding.
- ``/a2a/rest/...``: the HTTP+JSON binding (``/message:send``, ``/tasks/{id}``
  and the rest), generated from the same handler.

What Hopsworks adds rides in ``metadata.hopsworks`` on messages, status updates
and tasks, declared as a non-required extension on the card, so a client that
knows nothing of it ignores it.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import logging
import os
import socket
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse, urlunparse

from a2a.helpers.proto_helpers import new_task
from a2a.server.agent_execution import (
    AgentExecutor,
    RequestContext,
    SimpleRequestContextBuilder,
)
from a2a.server.request_handlers import DefaultRequestHandlerV2
from a2a.server.routes import create_jsonrpc_routes, create_rest_routes
from a2a.server.routes.fastapi_routes import add_a2a_routes_to_fastapi
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.server.tasks.task_store import TaskStore
from a2a.types import AgentCard, Message, Part, Task, TaskState
from a2a.utils.constants import TERMINAL_TASK_STATES, TransportProtocol
from fastapi import Request  # noqa: TC002 -- FastAPI resolves the annotation at runtime
from fastapi.responses import JSONResponse
from google.protobuf.json_format import MessageToDict, ParseDict

from .context import Interrupted  # noqa: F401 -- re-raised through the run
from .models import (
    AgentError,
    AgentSkill,
    ChatMessage,
    ChatRequest,
    ChatResponse,
    ResumeAnswer,
    TextContent,
    UrlContent,
    _part_for,
)


if TYPE_CHECKING:
    from a2a.server.context import ServerCallContext
    from a2a.server.events import EventQueue

    from .app import AgentApp, _TurnRun
    from .context import HandlerContext


_logger = logging.getLogger(__name__)

#: The A2A protocol version the card's interfaces declare.
A2A_PROTOCOL_VERSION = "1.0"
#: Where the JSON-RPC binding is served, under the app's base URL.
RPC_PATH = "/a2a"
#: Where the HTTP+JSON binding is served, under the app's base URL.
REST_PREFIX = "/a2a/rest"
#: The Agent Card's path: A2A discovery expects it at the domain root.
CARD_PATH = "/.well-known/agent-card.json"
#: The Hopsworks extension: what ``metadata.hopsworks`` carries, declared on the card.
HOPSWORKS_EXTENSION_URI = "https://hopsworks.ai/a2a/extensions/hopsworks/v1"
#: The artifact every task's reply text is: a client always finds it.
RESPONSE_ARTIFACT_ID = "response"
#: How long the handler keeps an interrupted task in memory before a follow-up
#: recreates it from the store; the question itself expires by our own rule.
INTERRUPTED_TASK_TIMEOUT_S = 15 * 60
#: The most an allowed URL part may be when fetched for the handler.
MAX_FETCHED_BYTES = 20 * 1024 * 1024
#: Internal hosts or networks an allowed URL part may resolve to, besides public
#: addresses: comma-separated hostnames or CIDRs, set by the platform. The same
#: list the push sender honours.
INTERNAL_ALLOW_ENV = "HOPSWORKS_AGENT_PUSH_INTERNAL_ALLOW"
#: How many finished tasks the in-memory store keeps, oldest dropped first.
DEFAULT_MAX_TASKS = 1000
#: How much the kept tasks may hold together, artifacts included.
DEFAULT_MAX_TASK_BYTES = 256 * 1024 * 1024
#: The media types each modality accepts or produces when the app does not say.
#: Exact, because the SDK's validator checks a part's type by set membership.
MEDIA_TYPES: dict[str, list[str]] = {
    "text": ["text/plain", "text/markdown"],
    "image": ["image/png", "image/jpeg", "image/webp", "image/gif"],
    "audio": [
        "audio/wav",
        "audio/mpeg",
        "audio/mp4",
        "audio/ogg",
        "audio/webm",
        "audio/flac",
    ],
    "file": ["application/octet-stream", "text/csv", "application/pdf"],
}
#: Always accepted: ``data`` parts, which carry JSON.
DATA_MEDIA_TYPE = "application/json"

_EXTENSION_PARAMS = {
    "subject": "in, on the message: who the agent is talking to, for its memory",
    "context": "in, on the message: anything the handler should see with the request",
    "metadata": "in, on the message: free-form metadata for the handler",
    "trace_id": "out: the turn's trace, for feedback and evaluation",
    "citations": "out, on the final status: the sources the reply cites",
    "usage": "out, on the final status: token counts",
    "step": "out, on a working status: a step the agent reported",
    "tool_call": "out, on a working status: a tool call the agent made",
    "interrupts": "out, on an input- or auth-required status: the questions asked",
    "status": "out, on a failed status: the agent's error code",
    "retryable": "out, on a failed status: whether a retry is worth it",
}


def _media_types(modalities: list[str], explicit: list[str] | None) -> list[str]:
    """The media types a card lists, from the explicit list or the modalities."""
    if explicit:
        return list(dict.fromkeys(explicit))
    types: list[str] = []
    for modality in modalities:
        types.extend(MEDIA_TYPES.get(modality, []))
    return list(dict.fromkeys(types))


def _struct(value: dict[str, Any]) -> dict[str, Any]:
    """Metadata as the proto takes it: plain JSON, with nothing a Struct cannot hold."""
    return json.loads(json.dumps(value, default=str))


@dataclass
class A2AConfig:
    """How an ``AgentApp`` serves A2A, for the few things an author may want to set.

    Pass it as ``AgentApp(a2a=A2AConfig(...))``; ``a2a=True`` is the defaults.

    Parameters:
        serialize_context: Run the tasks of one `contextId` one at a time, in the order they arrived, instead of concurrently.
            A second task waits as `SUBMITTED` until the first reaches a terminal or interrupted state.
            Per replica: two replicas serving the same context do not coordinate until the durable stores land.
        max_tasks: How many finished tasks the in-memory store keeps; the oldest are dropped first.
        max_task_bytes: How much the kept tasks may hold together, artifacts included, before the oldest are dropped.
    """

    serialize_context: bool = False
    max_tasks: int = DEFAULT_MAX_TASKS
    max_task_bytes: int = DEFAULT_MAX_TASK_BYTES


class _BoundedTaskStore(TaskStore):
    """The SDK's in-memory store, with finished tasks dropped past a budget.

    The SDK's store keeps every task for ever; a long-running replica would
    hold every reply and artifact it ever produced.
    Finished tasks are kept newest-first within a count and a byte budget; a
    task still running, paused or being read is never dropped.
    """

    def __init__(self, max_tasks: int, max_bytes: int):
        self._store = InMemoryTaskStore()
        self._max_tasks = max(1, max_tasks)
        self._max_bytes = max(1, max_bytes)
        # finished tasks in save order, each with its size and the context that
        # may delete it (the store scopes every operation by owner)
        self._finished: OrderedDict[str, tuple[int, ServerCallContext]] = OrderedDict()
        self._bytes = 0

    async def save(self, task: Task, context: ServerCallContext) -> None:
        await self._store.save(task, context)
        self._forget(task.id)
        if task.status.state in TERMINAL_TASK_STATES:
            size = task.ByteSize()
            self._finished[task.id] = (size, context)
            self._bytes += size
            await self._evict()

    async def get(self, task_id: str, context: ServerCallContext) -> Task | None:
        return await self._store.get(task_id, context)

    async def list(self, *args: Any, **kwargs: Any) -> Any:
        return await self._store.list(*args, **kwargs)

    async def delete(self, task_id: str, context: ServerCallContext) -> None:
        await self._store.delete(task_id, context)
        self._forget(task_id)

    def _forget(self, task_id: str) -> None:
        entry = self._finished.pop(task_id, None)
        if entry is not None:
            self._bytes -= entry[0]

    async def _evict(self) -> None:
        while self._finished and (
            len(self._finished) > self._max_tasks or self._bytes > self._max_bytes
        ):
            task_id, (size, context) = self._finished.popitem(last=False)
            self._bytes -= size
            try:
                await self._store.delete(task_id, context)
            except Exception:  # noqa: BLE001 -- already gone is fine
                _logger.debug("could not drop finished task %s", task_id, exc_info=True)


class A2AServer:
    """The A2A face of one ``AgentApp``: its card, handler and routes.

    Built by ``AgentApp`` when ``a2a`` is on; nothing here is reached when it
    is off.
    """

    def __init__(self, app: AgentApp, config: A2AConfig | None = None):
        self._app = app
        self.config = config or A2AConfig()
        self.executor = HopsworksAgentExecutor(app, self.config)
        # In memory, bounded: tasks are per replica and gone with the pod.
        # Durable stores on the project's online database are the next phase.
        self.task_store = _BoundedTaskStore(
            self.config.max_tasks, self.config.max_task_bytes
        )
        self.handler = DefaultRequestHandlerV2(
            agent_executor=self.executor,
            task_store=self.task_store,
            agent_card=self._card(self._public_url() or "http://localhost"),
            request_context_builder=SimpleRequestContextBuilder(
                should_populate_referred_tasks=True, task_store=self.task_store
            ),
            validate_input_modes=True,
            interrupted_task_timeout=INTERRUPTED_TASK_TIMEOUT_S,
        )

        # The card is served by the app, not by the SDK's route: its interface
        # URLs are the request's when the platform set no public URL, and the
        # SDK's card modifier sees the card, never the request.
        @app.get(CARD_PATH)
        async def agent_card(request: Request) -> JSONResponse:
            base = self._public_url() or _base_url(request)
            return JSONResponse(
                MessageToDict(
                    self._card(base), always_print_fields_with_no_presence=True
                )
            )

        # The REST routes come with a mount at /{tenant} for tenant-scoped
        # paths. One deployment is one agent and tenants are ignored, and the
        # mount would swallow every later route with a path of two segments
        # (an author's own, say), so it is left out.
        rest_routes = [
            route
            for route in create_rest_routes(
                self.handler, path_prefix=REST_PREFIX, enable_v0_3_compat=True
            )
            if type(route).__name__ != "Mount"
        ]
        add_a2a_routes_to_fastapi(
            app,
            jsonrpc_routes=create_jsonrpc_routes(
                self.handler, rpc_url=RPC_PATH, enable_v0_3_compat=True
            ),
            rest_routes=rest_routes,
        )

    def _public_url(self) -> str | None:
        """The base URL the card's interfaces name, when something fixed it.

        The platform sets ``HOPSWORKS_AGENT_PUBLIC_URL`` on a deployment; an
        app may name one itself; otherwise the card is built per request.
        """
        return (
            os.environ.get("HOPSWORKS_AGENT_PUBLIC_URL") or self._app._public_url
        ) or None

    def _endpoints(self) -> dict[str, str]:
        """What the Hopsworks manifest lists for A2A."""
        return {"agent_card": CARD_PATH, "a2a": RPC_PATH, "a2a_rest": REST_PREFIX}

    def _capabilities(self) -> dict[str, Any]:
        """What the manifest says about this agent's A2A face."""
        return {
            "enabled": True,
            "bindings": [TransportProtocol.JSONRPC, TransportProtocol.HTTP_JSON],
            "push_notifications": False,
            "signed": False,
            "task_store": "memory",
            "task_limit": self.config.max_tasks,
            "serialize_context": self.config.serialize_context,
        }

    def _running(self) -> set[asyncio.Task]:
        """The tasks of the turns running now, for the drain on shutdown."""
        return {run.task for run in self.executor.runs.values() if not run.task.done()}

    def _input_modes(self) -> list[str]:
        app = self._app
        modes = _media_types(app._input_modalities, app._input_media_types)
        if DATA_MEDIA_TYPE not in modes:
            modes.append(DATA_MEDIA_TYPE)
        return modes

    def _output_modes(self) -> list[str]:
        app = self._app
        return _media_types(app._output_modalities, app._output_media_types)

    def _skills(self) -> list[AgentSkill]:
        app = self._app
        if app._skills:
            return app._skills
        return [
            AgentSkill(
                id="chat",
                name=app._agent_name,
                description=app._agent_description or app._agent_name,
                tags=["chat"],
                examples=list(app._suggested_prompts),
            )
        ]

    def _card(self, base_url: str) -> AgentCard:
        """The Agent Card for an agent reachable at ``base_url``."""
        app = self._app
        base = base_url.rstrip("/")
        card: dict[str, Any] = {
            "name": app._agent_name,
            "description": app._agent_description or app._agent_name,
            "version": app._agent_version,
            "supportedInterfaces": [
                {
                    "url": base + RPC_PATH,
                    "protocolBinding": TransportProtocol.JSONRPC,
                    "protocolVersion": A2A_PROTOCOL_VERSION,
                },
                {
                    "url": base + REST_PREFIX,
                    "protocolBinding": TransportProtocol.HTTP_JSON,
                    "protocolVersion": A2A_PROTOCOL_VERSION,
                },
            ],
            "provider": {
                "organization": "Hopsworks",
                "url": "https://www.hopsworks.ai",
            },
            "documentationUrl": "https://docs.hopsworks.ai/",
            "capabilities": {
                # a chat-only agent streams its whole answer as one artifact chunk
                "streaming": True,
                "pushNotifications": False,
                "extendedAgentCard": False,
                "extensions": [
                    {
                        "uri": HOPSWORKS_EXTENSION_URI,
                        "description": "What Hopsworks carries in metadata.hopsworks "
                        "on messages, status updates and tasks.",
                        "required": False,
                        "params": _EXTENSION_PARAMS,
                    }
                ],
            },
            # what the gateway in front of a deployment accepts; nothing is
            # enforced here, the card describes the gate
            "securitySchemes": {
                "hopsworks-api-key": {
                    "apiKeySecurityScheme": {
                        "location": "header",
                        "name": "Authorization",
                        "description": "ApiKey <serving api key>",
                    }
                },
                "hopsworks-session": {"httpAuthSecurityScheme": {"scheme": "bearer"}},
            },
            "securityRequirements": [
                {"schemes": {"hopsworks-api-key": {"list": []}}},
                {"schemes": {"hopsworks-session": {"list": []}}},
            ],
            "defaultInputModes": self._input_modes(),
            "defaultOutputModes": self._output_modes(),
            "skills": [
                {
                    "id": skill.id,
                    "name": skill.name,
                    "description": skill.description,
                    "tags": skill.tags,
                    "examples": skill.examples,
                    "inputModes": skill.input_modes,
                    "outputModes": skill.output_modes,
                }
                for skill in self._skills()
            ],
        }
        return ParseDict(card, AgentCard())


def _base_url(request: Request) -> str:
    """The base URL a request reached the app at, through whatever is in front of it."""
    headers = request.headers
    proto = headers.get("x-forwarded-proto") or request.url.scheme
    host = headers.get("x-forwarded-host") or headers.get("host") or request.url.netloc
    prefix = (headers.get("x-forwarded-prefix") or "").rstrip("/")
    return f"{proto}://{host}{prefix}"


class HopsworksAgentExecutor(AgentExecutor):
    """Runs one A2A message as a turn of the app, and reports it as task events."""

    def __init__(self, app: AgentApp, config: A2AConfig | None = None):
        self._app = app
        self._config = config or A2AConfig()
        #: The turns running now, by task id: what a cancel stops and a drain waits for.
        self.runs: dict[str, _TurnRun] = {}
        # one lock per context when tasks on a context run one at a time
        self._context_locks: dict[str, asyncio.Lock] = {}
        # executions in flight, for the drain to wait on after it aborted them
        self._executing: set[asyncio.Task] = set()

    async def _settled(self, timeout: float) -> None:
        """Wait for the executions in flight to end, for the drain."""
        pending = {task for task in self._executing if not task.done()}
        if pending:
            await asyncio.wait(pending, timeout=timeout)

    # ── the SDK's entry points ───────────────────────────────────────────

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        current = asyncio.current_task()
        if current is not None:
            self._executing.add(current)
        try:
            await self._execute(context, event_queue)
        finally:
            if current is not None:
                self._executing.discard(current)

    async def _execute(self, context: RequestContext, event_queue: EventQueue) -> None:

        app = self._app
        task_id = context.task_id or ""
        updater = TaskUpdater(event_queue, task_id, context.context_id or "")
        headers = _headers(context)
        # A new task is created by the executor, SUBMITTED with the message in
        # its history, before anything is said about it; a follow-up to a
        # paused task moves it back through SUBMITTED below.
        if context.current_task is None:
            await event_queue.enqueue_event(
                new_task(
                    task_id,
                    context.context_id or "",
                    TaskState.TASK_STATE_SUBMITTED,
                    history=[context.message] if context.message else [],
                )
            )
        if app._chat_handler is None and app._stream_handler is None:
            await self._reject(
                updater,
                AgentError("No chat handler registered.", "not_implemented", 501),
            )
            return
        try:
            request, replayed = await self._to_request(context)
        except AgentError as err:
            await self._refuse(updater, context, err)
            return
        ctx = app._prepare(request, protocol="a2a")
        ctx.task_id = task_id
        ctx._pending_key = _pending_key(context)
        ctx._claimed = replayed
        refused = await app._attach_evaluation(ctx, headers)
        if refused is not None:
            await app._settle_pending(ctx, answered=False)
            await self._reject(updater, refused)
            return
        if context.current_task is not None:
            await updater.submit()
        if not self._config.serialize_context:
            await self._turn(context, updater, ctx, headers)
            return
        # One task of the context at a time: the next waits, SUBMITTED, until
        # the running one ends or pauses. In this process only.
        key = context.context_id or ""
        lock = self._context_locks.setdefault(key, asyncio.Lock())
        async with lock:
            try:
                await self._turn(context, updater, ctx, headers)
            finally:
                if not lock.locked() or not lock._waiters:  # noqa: SLF001
                    self._context_locks.pop(key, None)

    async def _turn(
        self,
        context: RequestContext,
        updater: TaskUpdater,
        ctx: HandlerContext,
        headers: dict[str, str],
    ) -> None:
        """Run the turn and report it on the task, once nothing is in its way."""
        from .app import _mark_cancelled, _record_span_error, _trace_id, _TurnRun
        from .evaluation import active as eval_active
        from .tracing import turn_span

        app = self._app
        task_id = context.task_id or ""
        await app._resolve_subject(ctx)
        await app._open_turn(ctx)
        # WORKING follows, with the trace id, which is how a caller joins the
        # task to its trace.
        completed: ChatResponse | None = None
        run: _TurnRun | None = None
        with (
            eval_active(ctx.evaluation),
            turn_span(
                app.tracer_provider,
                name=app._turn_span_name(),
                headers=headers,
                attributes=app._turn_span_attributes(ctx),
                keep_eval_baggage=ctx._eval_labelled,
            ) as span,
        ):
            ctx._span = span
            trace_id = _trace_id(span)
            try:
                await updater.update_status(
                    TaskState.TASK_STATE_WORKING,
                    metadata=_hopsworks({"trace_id": trace_id, "protocol": "a2a"}),
                )
                run = _TurnRun(app, ctx)
                self.runs[task_id] = run
                artifacts = _Artifacts(updater, self._accepted(context))
                while True:
                    kind, payload = await run._next()
                    if kind == "done":
                        if run.aborted:
                            # the drain deadline cut the handler short: the
                            # turn is abandoned and the caller told to resend
                            _mark_cancelled(span)
                            await run._close(None)
                            await self._shutdown(updater, trace_id)
                            return
                        break
                    if kind == "delta":
                        await artifacts._text(payload)
                    elif kind == "tool_event":
                        await self._progress(updater, payload)
                    elif kind == "artifact":
                        await artifacts._chunk(payload)
                    elif kind == "interrupt":
                        unrecorded = await app._pause(ctx, span, payload)
                        # The turn is settled before the caller learns the task
                        # paused, failed or ended: a follow-up sent on seeing
                        # that must find the turn in the store, as it does on
                        # the other protocols, where the reply ends after it.
                        await run._close(None)
                        if unrecorded is not None:
                            await self._fail(updater, unrecorded, trace_id)
                            return
                        await self._interrupt(updater, payload, trace_id)
                        return
                    elif kind == "error":
                        _record_span_error(span, payload)
                        await run._close(None)
                        await self._fail(updater, payload, trace_id)
                        return
                response = run._response()
                completed = response
                await artifacts._finish(response)
                await run._close(completed)
                if response.status == "failed":
                    failure = AgentError(
                        "The agent reported the turn as failed.", "turn_failed", 500
                    )
                    await self._fail(updater, failure, trace_id, response=response)
                    return
                await updater.complete(
                    updater.new_agent_message(
                        [Part(text=_text_of(response))],
                        metadata=_hopsworks(
                            self._facts(ctx, response, trace_id, artifacts.dropped)
                        ),
                    )
                )
            except asyncio.CancelledError:
                # CancelTask, or the pod being told to stop: the SDK writes the
                # terminal state; the turn is ended the way a hang-up ends it.
                _mark_cancelled(span)
                if run is not None:
                    run._hangup()
                if app._draining and run is not None and not run.detached:
                    await self._shutdown(updater, trace_id)
                raise
            finally:
                self.runs.pop(task_id, None)
                if run is None:
                    await app._finalize_turn(ctx, completed)
                else:
                    await run._close(completed)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Stop the task's turn: the handler is interrupted at its next ``await``.

        A resumed turn running the action a person approved is left to finish
        instead, as it is when its client hangs up, and only the task is
        cancelled.
        The SDK records ``CANCELED`` once the producer has stopped.
        """
        run = self.runs.get(context.task_id or "")
        if run is not None:
            run._hangup()

    # ── from an A2A message to a turn ────────────────────────────────────

    async def _to_request(self, context: RequestContext) -> tuple[ChatRequest, bool]:
        """The turn a message asks for, and whether it replays a paused one.

        Raises:
            AgentError: The message carries content this agent cannot take, or
                answers a paused task wrongly.
        """
        message = context.message
        if message is None:
            raise AgentError("The request carries no message.", "no_message", 422)
        hopsworks = _hopsworks_of(message)
        current = context.current_task
        if current is not None and current.status.state in (
            TaskState.TASK_STATE_INPUT_REQUIRED,
            TaskState.TASK_STATE_AUTH_REQUIRED,
        ):
            # A follow-up to a paused task: the answers replay the request the
            # turn paused on, as a resume does on the other protocols.
            answers = _answers(message, _open_questions(current))
            if not answers:
                raise AgentError(
                    "This task is waiting for answers; the message answers none "
                    "of its questions.",
                    "unanswered_interrupts",
                    400,
                )
            replay = await self._app._check_resume(_pending_key(context), answers, None)
            if replay is None:  # pragma: no cover - answers were given
                raise AgentError("Nothing to resume.", "nothing_to_resume", 409)
            return replay, True
        content, data = await self._content(message)
        request_context: dict[str, Any] = {}
        for item in data:
            request_context.update(item)
        if isinstance(hopsworks.get("context"), dict):
            request_context.update(hopsworks["context"])
        if context.related_tasks:
            request_context["referenced_tasks"] = [
                _referenced(task) for task in context.related_tasks
            ]
        subject = hopsworks.get("subject")
        metadata = hopsworks.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, dict) else {}
        accepted = self._accepted(context)
        if accepted:
            metadata["accepted_output_modes"] = accepted
        return (
            ChatRequest(
                conversation_id=context.context_id or None,
                message=ChatMessage(
                    id=message.message_id or None, role="user", content=content
                ),
                subject=subject
                if isinstance(subject, str) and subject.strip()
                else None,
                context=request_context,
                metadata=metadata,
                run_id=context.task_id or None,
            ),
            False,
        )

    async def _content(self, message: Message) -> tuple[list[Any], list[dict]]:
        """The message's parts as the protocol's typed parts, and its data parts."""
        content: list[Any] = []
        data: list[dict[str, Any]] = []
        for part in message.parts:
            kind = part.WhichOneof("content")
            if kind == "text":
                content.append(TextContent(text=part.text))
            elif kind == "raw":
                content.append(
                    _part_for(
                        part.media_type or "application/octet-stream",
                        base64.b64encode(part.raw).decode(),
                        part.filename or None,
                    )
                )
            elif kind == "url":
                content.append(await self._url_part(part))
            elif kind == "data":
                value = MessageToDict(part).get("data")
                if isinstance(value, dict):
                    data.append(value)
                else:
                    data.append({"data": value})
            else:
                raise AgentError(
                    "A message part carries no content.", "unsupported_content", 422
                )
        return content, data

    async def _url_part(self, part: Part) -> Any:
        """A ``url`` part: fetched when its host is one the app allows, else as a URL."""
        url = part.url
        name = part.filename or None
        media_type = part.media_type or None
        host = (urlparse(url).hostname or "").lower()
        if not host or host not in self._app._fetch_urls:
            return UrlContent(url=url, media_type=media_type, name=name)
        try:
            fetched_type, payload = await asyncio.to_thread(_fetch, url)
        except AgentError:
            raise
        except Exception as err:  # noqa: BLE001 -- said to the caller
            raise AgentError(
                f"The URL part could not be fetched: {err}", "unsupported_content", 422
            ) from err
        return _part_for(
            media_type or fetched_type or "application/octet-stream",
            base64.b64encode(payload).decode(),
            name or (urlparse(url).path.rsplit("/", 1)[-1] or None),
        )

    @staticmethod
    def _accepted(context: RequestContext) -> list[str]:
        configuration = context.configuration
        if configuration is None:
            return []
        return list(configuration.accepted_output_modes)

    # ── from the turn to task events ─────────────────────────────────────

    async def _progress(self, updater: TaskUpdater, payload: dict[str, Any]) -> None:
        """A step or tool call, as a working status with a message: the A2A idiom for progress."""
        detail = {k: v for k, v in payload.items() if k != "kind"}
        key = "tool_call" if payload.get("kind") == "tool" else "step"
        text = payload.get("message") or payload.get("name") or key
        status = payload.get("status")
        if status and status != "running":
            text = f"{text} ({status})"
        await updater.update_status(
            TaskState.TASK_STATE_WORKING,
            message=updater.new_agent_message(
                [Part(text=str(text))], metadata=_hopsworks({key: detail})
            ),
        )

    async def _refuse(
        self, updater: TaskUpdater, context: RequestContext, err: AgentError
    ) -> None:
        """Refused before the handler ran.

        A new task is ``REJECTED`` with the reason.
        A follow-up that answers a paused task wrongly leaves the task paused,
        asked again with the reason in front, so one bad answer does not end
        the task; a question that expired fails it, as it does on every
        protocol.
        """
        current = context.current_task
        paused = current is not None and current.status.state in (
            TaskState.TASK_STATE_INPUT_REQUIRED,
            TaskState.TASK_STATE_AUTH_REQUIRED,
        )
        if paused and err.code == "interrupt_expired":
            await self._fail(updater, err, None)
            return
        if paused:
            await self._interrupt(
                updater, _open_questions(current), None, note=err.message
            )
            return
        await self._reject(updater, err)

    async def _interrupt(
        self,
        updater: TaskUpdater,
        interrupts: list[dict[str, Any]],
        trace_id: str | None,
        note: str | None = None,
    ) -> None:
        """The turn asked a person: ``INPUT_REQUIRED``, or ``AUTH_REQUIRED`` when every question is about credentials."""
        text = "\n".join(str(q.get("message") or q.get("id")) for q in interrupts)
        if note:
            text = f"{note}\n{text}" if text else note
        parts = [Part(text=text)]
        parts.append(ParseDict({"data": {"interrupts": interrupts}}, Part()))
        message = updater.new_agent_message(
            parts, metadata=_hopsworks({"trace_id": trace_id, "interrupts": interrupts})
        )
        if interrupts and all(q.get("reason") == "auth_required" for q in interrupts):
            await updater.requires_auth(message)
        else:
            await updater.requires_input(message)

    async def _fail(
        self,
        updater: TaskUpdater,
        err: AgentError,
        trace_id: str | None,
        response: ChatResponse | None = None,
    ) -> None:
        facts: dict[str, Any] = {
            "trace_id": trace_id,
            "status": err.code,
            "http_status": err.status_code,
            "retryable": err.retryable,
        }
        if response is not None and response.metadata:
            facts["metadata"] = response.metadata
        await updater.failed(
            updater.new_agent_message(
                [Part(text=err.message)], metadata=_hopsworks(facts)
            )
        )

    async def _reject(self, updater: TaskUpdater, err: AgentError) -> None:
        """Refused before the handler ran: the task is ``REJECTED``, with the reason."""
        await updater.reject(
            updater.new_agent_message(
                [Part(text=err.message)],
                metadata=_hopsworks(
                    {
                        "status": err.code,
                        "http_status": err.status_code,
                        "retryable": err.retryable,
                    }
                ),
            )
        )

    async def _shutdown(self, updater: TaskUpdater, trace_id: str | None) -> None:
        """The pod stopped before the turn ended: say so, best effort."""
        try:
            await self._fail(
                updater,
                AgentError(
                    "The agent was stopped before the task finished.",
                    "executor_shutdown",
                    503,
                    retryable=True,
                ),
                trace_id,
            )
        except Exception:  # noqa: BLE001 -- the queue may already be closed
            _logger.debug("could not report the shutdown on the task", exc_info=True)

    @staticmethod
    def _facts(
        ctx: HandlerContext,
        response: ChatResponse,
        trace_id: str | None,
        dropped: list[str] | None = None,
    ) -> dict[str, Any]:
        facts: dict[str, Any] = {
            "trace_id": trace_id,
            "response_id": response.id,
            "citations": response.citations or None,
            "usage": response.usage,
            # outputs the caller's accepted media types left out, by name
            "dropped_artifacts": dropped or None,
        }
        if ctx.has_subject:
            facts["subject"] = ctx.subject
            facts["subject_source"] = ctx.subject_source
        metadata = {
            k: v
            for k, v in (response.metadata or {}).items()
            if k not in ("trace_id", "tool_events", "subject")
        }
        if metadata:
            facts["metadata"] = metadata
        return {k: v for k, v in facts.items() if v is not None}


class _Artifacts:
    """The artifacts of one task, and the rules on them.

    The first chunk of an artifact id is sent with ``append: false``, which
    creates it; a later chunk appends unless the author said otherwise, and
    ``append: false`` on an artifact that exists replaces it, as A2A defines.
    The SDK refuses an append to an id it has not seen, so the executor keeps
    that rule rather than the author.
    A part whose media type the caller did not accept is left out, emitted or
    returned alike, and the artifact's name is reported as dropped.
    """

    def __init__(self, updater: TaskUpdater, accepted: list[str]):
        self._updater = updater
        self._accepted = {_essence(m) for m in accepted}
        self._seen: set[str] = set()
        self._closed: set[str] = set()
        self._streamed = ""
        #: Names of the artifacts the caller's accepted media types left out.
        self.dropped: list[str] = []

    async def _text(self, delta: str) -> None:
        if not delta:
            return
        await self._send(RESPONSE_ARTIFACT_ID, "response", [Part(text=delta)])
        self._streamed += delta

    async def _chunk(self, payload: dict[str, Any]) -> None:
        part = _to_part(payload["part"])
        if part is None:
            return
        if not self._takes(payload["part"]):
            self._drop(payload["name"])
            return
        await self._send(
            payload["id"],
            payload["name"],
            [part],
            append=bool(payload.get("append")),
            last=bool(payload.get("last")),
            description=payload.get("description"),
        )

    async def _finish(self, response: ChatResponse) -> None:
        """Close the reply artifact and send the rest: media parts and named artifacts."""
        text = _text_of(response)
        if RESPONSE_ARTIFACT_ID not in self._seen:
            # created even when empty, so a client always finds the reply
            await self._send(
                RESPONSE_ARTIFACT_ID, "response", [Part(text=text)], last=True
            )
        elif text.startswith(self._streamed):
            # the handler's final text adds to the streamed draft, or is it
            rest = text[len(self._streamed) :]
            await self._send(
                RESPONSE_ARTIFACT_ID, "response", [Part(text=rest)], last=True
            )
        else:
            # the final text rewrites the draft: the artifact is replaced
            await self._send(
                RESPONSE_ARTIFACT_ID,
                "response",
                [Part(text=text)],
                append=False,
                last=True,
            )
        for index, part in enumerate(response.message.content):
            if part.type == "text":
                continue
            name = getattr(part, "name", None) or f"{part.type}-{index + 1}"
            proto = _to_part(part.model_dump())
            if proto is None:
                continue
            if not self._takes(part):
                self._drop(name)
                continue
            await self._send(name, name, [proto], last=True)
        for artifact in response.artifacts:
            parts = []
            for part in artifact.parts:
                proto = _to_part(part.model_dump())
                if proto is None:
                    continue
                if not self._takes(part):
                    self._drop(artifact.name)
                    continue
                parts.append(proto)
            if not parts and artifact.parts:
                continue
            await self._send(
                artifact.artifact_id,
                artifact.name,
                parts,
                last=True,
                description=artifact.description,
            )
        # an artifact the handler streamed and never closed ends with the task
        for artifact_id in sorted(self._seen - self._closed):
            await self._send(artifact_id, artifact_id, [], last=True)

    def _takes(self, part: Any) -> bool:
        """Whether the caller accepts a part's media type; everything, when it said nothing."""
        if isinstance(part, dict):
            media_type = part.get("media_type")
        else:
            media_type = getattr(part, "media_type", None)
        if not self._accepted or not media_type:
            return True
        return _essence(media_type) in self._accepted

    def _drop(self, name: str) -> None:
        if name not in self.dropped:
            self.dropped.append(name)

    async def _send(
        self,
        artifact_id: str,
        name: str,
        parts: list[Part],
        *,
        append: bool | None = None,
        last: bool = False,
        description: str | None = None,
    ) -> None:
        first = artifact_id not in self._seen
        metadata = {"description": description} if description else None
        await self._updater.add_artifact(
            parts,
            artifact_id=artifact_id,
            name=name,
            metadata=metadata,
            # the first chunk creates; afterwards the author's word, which is
            # to append unless given, and replaces when False
            append=False if first else (True if append is None else append),
            last_chunk=last,
        )
        self._seen.add(artifact_id)
        if last:
            self._closed.add(artifact_id)
        else:
            self._closed.discard(artifact_id)


# ── helpers ──────────────────────────────────────────────────────────────


def _hopsworks(facts: dict[str, Any]) -> dict[str, Any]:
    return {"hopsworks": _struct({k: v for k, v in facts.items() if v is not None})}


def _hopsworks_of(message: Message) -> dict[str, Any]:
    metadata = MessageToDict(message).get("metadata") or {}
    hopsworks = metadata.get("hopsworks")
    return hopsworks if isinstance(hopsworks, dict) else {}


def _headers(context: RequestContext) -> dict[str, str]:
    state = getattr(context.call_context, "state", None) or {}
    headers = state.get("headers") if isinstance(state, dict) else None
    return {str(k).lower(): str(v) for k, v in (headers or {}).items()}


def _pending_key(context: RequestContext) -> str:
    """Where a task's questions are recorded: the task, since a context may hold several."""
    return f"{context.context_id or ''}/{context.task_id or ''}"


def _text_of(response: ChatResponse) -> str:
    return "".join(
        part.text for part in response.message.content if part.type == "text"
    )


def _essence(media_type: str) -> str:
    return media_type.split(";", 1)[0].strip().lower()


def _to_part(part: dict[str, Any] | None) -> Part | None:
    """A protocol part as an A2A ``Part``; None for one A2A cannot carry."""
    if not isinstance(part, dict):
        return None
    kind = part.get("type")
    if kind == "text":
        return Part(text=str(part.get("text") or ""))
    if kind in ("image", "audio", "file"):
        try:
            raw = base64.b64decode(part.get("data") or "")
        except (ValueError, TypeError):
            return None
        return Part(
            raw=raw,
            media_type=str(part.get("media_type") or "application/octet-stream"),
            filename=str(part.get("name") or ""),
        )
    if kind == "url":
        return Part(
            url=str(part.get("url") or ""),
            media_type=str(part.get("media_type") or ""),
            filename=str(part.get("name") or ""),
        )
    return None


def _open_questions(task: Task) -> list[dict[str, Any]]:
    """The questions a paused task asked, from the status message it paused with."""
    message = task.status.message
    for part in message.parts:
        if part.WhichOneof("content") == "data":
            data = MessageToDict(part).get("data")
            if isinstance(data, dict) and isinstance(data.get("interrupts"), list):
                return [q for q in data["interrupts"] if isinstance(q, dict)]
    return []


def _answers(
    message: Message, open_questions: list[dict[str, Any]]
) -> list[ResumeAnswer]:
    """What a follow-up message answers.

    A ``data`` part names the questions it answers by id; ``{"$cancelled": true}``
    declines one.
    A message of text only answers the one open question, when there is one:
    that is what a generic client, or a person in an inspector, sends.
    """
    open_ids = [str(q.get("id")) for q in open_questions]
    answers: list[ResumeAnswer] = []
    text: list[str] = []
    for part in message.parts:
        kind = part.WhichOneof("content")
        if kind == "data":
            data = MessageToDict(part).get("data")
            if not isinstance(data, dict):
                continue
            for key, payload in data.items():
                if key not in open_ids and open_ids:
                    continue
                if isinstance(payload, dict) and payload.get("$cancelled") is True:
                    answers.append(ResumeAnswer(interrupt_id=key, status="cancelled"))
                else:
                    answers.append(
                        ResumeAnswer(
                            interrupt_id=key, status="resolved", payload=payload
                        )
                    )
        elif kind == "text" and part.text:
            text.append(part.text)
    if not answers and text and len(open_ids) == 1:
        answers.append(
            ResumeAnswer(
                interrupt_id=open_ids[0], status="resolved", payload="\n".join(text)
            )
        )
    return answers


def _referenced(task: Task) -> dict[str, Any]:
    """What a referenced task said, for a handler asked to continue from it."""
    final = task.status.message
    text = "".join(p.text for p in final.parts if p.WhichOneof("content") == "text")
    return {
        "task_id": task.id,
        "context_id": task.context_id,
        "state": TaskState.Name(task.status.state),
        "text": text,
        "artifacts": [a.name or a.artifact_id for a in task.artifacts],
    }


@dataclass
class _Pinned:
    """Where a fetch actually connects: the address checked, with the name kept for TLS and ``Host``."""

    url: str
    host: str
    host_header: str


def _internal_allowed() -> tuple[set[str], list[Any]]:
    """The internal hosts and networks the platform allowed, from the environment."""
    hosts: set[str] = set()
    networks: list[Any] = []
    for entry in (os.environ.get(INTERNAL_ALLOW_ENV) or "").split(","):
        entry = entry.strip().lower()
        if not entry:
            continue
        try:
            networks.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            hosts.add(entry)
    return hosts, networks


def _address_blocked(address: Any) -> bool:
    """Whether an address is not a public unicast destination."""
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        address = mapped
    return (
        not address.is_global
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def _resolve(host: str, port: int) -> list[str]:
    """The addresses a hostname resolves to now, as the connection will see them."""
    return [
        str(info[4][0])
        for info in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    ]


def _pin(url: str) -> _Pinned:
    """Resolve a URL's host once, check every address, and pin the fetch to one.

    Checking a name and then letting the client resolve it again lets an
    answer that changes between the two reach an address the check refused.
    The request therefore goes to the address checked, with the hostname kept
    for certificate verification and the ``Host`` header.
    Public addresses are allowed; a private, loopback, link-local or metadata
    address only when the platform listed the host or its network.

    Raises:
        AgentError: The URL is not HTTPS or HTTP, its host does not resolve,
            or resolves to an address the agent must not reach.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in ("http", "https") or not host:
        raise AgentError(
            "The URL part is not an http(s) URL.", "unsupported_content", 422
        )
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    allowed_hosts, allowed_networks = _internal_allowed()
    try:
        addresses = _resolve(host, port)
    except OSError as err:
        raise AgentError(
            f"The URL part's host could not be resolved: {host}",
            "unsupported_content",
            422,
        ) from err
    chosen: str | None = None
    for raw in addresses:
        address = ipaddress.ip_address(raw.split("%", 1)[0])
        internal = host in allowed_hosts or any(
            address in network for network in allowed_networks
        )
        if _address_blocked(address) and not internal:
            raise AgentError(
                f"The URL part's host resolves to an address the agent must not "
                f"reach: {host}",
                "unsupported_content",
                422,
            )
        chosen = chosen or raw
    if chosen is None:
        raise AgentError(
            f"The URL part's host resolves to nothing: {host}",
            "unsupported_content",
            422,
        )
    literal = f"[{chosen}]" if ":" in chosen else chosen
    pinned = parsed._replace(netloc=f"{literal}:{port}")
    default_port = port == (443 if parsed.scheme == "https" else 80)
    host_header = host if default_port else f"{host}:{port}"
    return _Pinned(url=urlunparse(pinned), host=host, host_header=host_header)


def _fetch(url: str) -> tuple[str | None, bytes]:
    """Fetch an allowed URL part: pinned to the checked address, no redirects, bounded, in a thread."""
    import httpx  # noqa: PLC0415

    target = _pin(url)
    with (
        httpx.Client(follow_redirects=False, timeout=30.0) as client,
        client.stream(
            "GET",
            target.url,
            headers={"Host": target.host_header},
            extensions={"sni_hostname": target.host},
        ) as response,
    ):
        if response.status_code != 200:
            raise AgentError(
                f"The URL part answered {response.status_code}.",
                "unsupported_content",
                422,
            )
        chunks: list[bytes] = []
        size = 0
        for chunk in response.iter_bytes():
            size += len(chunk)
            if size > MAX_FETCHED_BYTES:
                raise AgentError(
                    "The URL part is larger than the agent takes inline.",
                    "unsupported_content",
                    422,
                )
            chunks.append(chunk)
        media_type = response.headers.get("content-type")
        return (_essence(media_type) if media_type else None), b"".join(chunks)

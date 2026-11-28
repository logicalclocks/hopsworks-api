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
import threading
import time
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
from a2a.server.cluster import ConcurrentTaskModificationError
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandlerV2
from a2a.server.request_handlers.response_helpers import agent_card_to_dict
from a2a.server.routes import create_jsonrpc_routes, create_rest_routes
from a2a.server.routes.fastapi_routes import add_a2a_routes_to_fastapi
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.server.tasks.task_store import TaskStore
from a2a.types import (
    AgentCard,
    Message,
    Part,
    Role,
    Task,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from a2a.utils.constants import (
    PROTOCOL_VERSION_0_3,
    TERMINAL_TASK_STATES,
    TransportProtocol,
)
from a2a.utils.errors import InvalidParamsError, UnsupportedOperationError
from fastapi import Request  # noqa: TC002 -- FastAPI resolves the annotation at runtime
from fastapi.responses import JSONResponse, Response
from google.protobuf.json_format import MessageToDict, ParseDict
from starlette.routing import Mount

from . import a2a_store as _store
from .a2a_artifacts import (
    ARTIFACTS_PATH,
    _read_spilled,
    _spill_backend,
    _SpillingStore,
)
from .a2a_push import _PushSender, _valid_webhook
from .a2a_store import (
    DATABASE_URL_ENV,
    MISSED_HEARTBEATS,
    OWNER_STATE_KEY,
    _database_url,
    _DurableStores,
    _Ownership,
)
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
    _printable,
)
from .pending import _task_key
from .scope import _principal, _principal_of


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

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
# a task waiting on a person: kept like a finished one, within the store budget
PAUSED_TASK_STATES = frozenset(
    {TaskState.TASK_STATE_INPUT_REQUIRED, TaskState.TASK_STATE_AUTH_REQUIRED}
)
INTERRUPTED_TASK_TIMEOUT_S = 15 * 60
#: The most an allowed URL part may be when fetched for the handler.
MAX_FETCHED_BYTES = 20 * 1024 * 1024
#: The most a fetch of an allowed URL part may take, whole: a host that drips
#: bytes would otherwise hold the turn through the whole size cap.
FETCH_DEADLINE_S = 30.0
#: How long one operation of a fetch (connect, a read) may take.
FETCH_OPERATION_TIMEOUT_S = 10.0
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
        max_tasks: How many settled tasks the in-memory store keeps, finished or paused on a question; the oldest are dropped first.
        max_task_bytes: How much the kept tasks may hold together, artifacts included, before the oldest are dropped.
        task_store: Where tasks are kept.
            `"database"`: in the project's online database, shared by every replica and kept when a pod goes; what a deployment uses when nothing is said.
            `"memory"`: in the process, per replica, gone with the pod; what a local run uses when nothing is said, and the explicit choice of an agent that should serve A2A on a deployment without a database.
            Unsaid on a deployment that has no database, the agent serves no A2A rather than tasks it cannot keep, and says so in its log and manifest.
        idempotent: The author's word that running a turn twice does no harm: its tools have no side effect a repeat would duplicate.
            Required for `retry_lost_tasks`; says nothing by itself.
        retry_lost_tasks: How many times a task whose replica was lost is run again, from the start, on a surviving replica, before it is reported as failed.
            Only with `idempotent=True`, and only with the database store; an agent that sets it should keep at least one replica running.
    """

    serialize_context: bool = False
    max_tasks: int = DEFAULT_MAX_TASKS
    max_task_bytes: int = DEFAULT_MAX_TASK_BYTES
    task_store: str | None = None
    idempotent: bool = False
    retry_lost_tasks: int = 0

    def __post_init__(self) -> None:
        if self.task_store is not None and self.task_store not in TASK_STORES:
            raise ValueError(
                f"task_store must be one of {sorted(TASK_STORES)}, "
                f"not {self.task_store!r}"
            )
        if self.retry_lost_tasks < 0:
            raise ValueError("retry_lost_tasks must not be negative")
        if self.retry_lost_tasks and not self.idempotent:
            raise ValueError(
                "retry_lost_tasks runs a turn again, which is only safe for an "
                "agent declared idempotent=True"
            )


#: Where tasks can be kept: the process, or the project's online database.
TASK_STORES = frozenset({"memory", "database"})
#: How often a replica looks for tasks whose replica was lost.
SWEEP_SECONDS = 30.0
#: Where the platform signs this deployment's card, set by the platform.
CARD_SIGN_URL_ENV = "HOPSWORKS_AGENT_CARD_SIGN_URL"
#: How many times a reaper writes FAILED again when the task moved under it.
LOST_WRITE_ATTEMPTS = 5
#: How many lost tasks a replica remembers it must not write to, before it starts over.
FENCED_OUT_LIMIT = 10_000
#: Where a call context made by the sweep says the execution is a retry of a lost task, and the fence and attempt it took.
RETRY_STATE_KEY = "hopsworks_retry"
#: How long shutdown waits for queued push notifications to go out.
PUSH_DRAIN_SECONDS = 5.0


class _NoDatabase(Exception):  # noqa: N818 -- a reason A2A is off, not a failure
    """A deployment that cannot keep tasks in a database, and no word that memory will do: A2A is not served."""

    def __init__(self, reason: str = "no_database"):
        super().__init__(reason)
        #: What the manifest says: ``no_database``, or ``no_database_driver``.
        self.reason = reason


def _driver_ready() -> bool:
    """Whether the async database stack the stores need is installed (the ``agents-memory`` extra)."""
    try:
        import aiomysql  # noqa: F401, PLC0415
        import greenlet  # noqa: F401, PLC0415
        import sqlalchemy.ext.asyncio  # noqa: F401, PLC0415
    except ImportError:
        return False
    return True


def _store_mode(config: A2AConfig) -> tuple[str, str | None]:
    """Where this agent keeps its tasks, and the database URL when it is a database.

    Raises:
        ValueError: The author asked for the database and there is none.
        _NoDatabase: A deployment with no database whose author did not choose memory.
    """
    if config.task_store == "memory":
        return "memory", None
    url = _database_url()
    if config.task_store == "database":
        if url is None:
            raise ValueError(
                'task_store="database" needs a database: a deployment with '
                f"access to the project's online store, or {DATABASE_URL_ENV}"
            )
        return "database", url
    if _on_platform():
        if url is None:
            raise _NoDatabase
        if not _driver_ready():
            raise _NoDatabase("no_database_driver")
        return "database", url
    # a local run keeps tasks in the process unless it names a database
    if os.environ.get(DATABASE_URL_ENV):
        return "database", url
    return "memory", None


class _BoundedTaskStore(TaskStore):
    """The SDK's in-memory store, with settled tasks dropped past a budget.

    The SDK's store keeps every task for ever; a long-running replica would
    hold every reply and artifact it ever produced, and every question nobody
    answered.
    Settled tasks, finished or paused on a question, are kept newest-first
    within a count and a byte budget; a task still running is not settled and
    so never dropped.
    A paused task dropped this way takes its open question with it, through
    ``on_drop``, so an answer that arrives later is refused rather than
    resumed against a task that is gone; dropped between a follow-up's
    admission and its re-read, the follow-up fails the task the same way.
    Of the call context a task was saved under, only what the store needs to
    delete it is kept (the owner and the tenant), not the request's headers.
    """

    def __init__(
        self,
        max_tasks: int,
        max_bytes: int,
        on_drop: Callable[[Task], Awaitable[None]] | None = None,
        owner: Callable[[ServerCallContext], str] | None = None,
    ):
        self._store = (
            InMemoryTaskStore(owner_resolver=owner) if owner else InMemoryTaskStore()
        )
        self._owner = owner
        self._max_tasks = max(1, max_tasks)
        self._max_bytes = max(1, max_bytes)
        self._on_drop = on_drop
        # settled tasks in save order, each with its size and the context that
        # may delete it (the store scopes every operation by owner)
        self._settled: OrderedDict[str, tuple[int, ServerCallContext]] = OrderedDict()
        self._bytes = 0

    async def save(self, task: Task, context: ServerCallContext) -> None:
        await self._store.save(task, context)
        self._forget(task.id)
        if (
            task.status.state in TERMINAL_TASK_STATES
            or task.status.state in PAUSED_TASK_STATES
        ):
            size = task.ByteSize()
            self._settled[task.id] = (size, _owner_only(context, self._owner))
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
        entry = self._settled.pop(task_id, None)
        if entry is not None:
            self._bytes -= entry[0]

    async def _evict(self) -> None:
        while self._settled and (
            len(self._settled) > self._max_tasks or self._bytes > self._max_bytes
        ):
            task_id, (size, context) = self._settled.popitem(last=False)
            self._bytes -= size
            try:
                dropped = await self._store.get(task_id, context)
                await self._store.delete(task_id, context)
            except Exception:  # noqa: BLE001 -- already gone is fine
                _logger.debug("could not drop settled task %s", task_id, exc_info=True)
                continue
            if (
                dropped is not None
                and dropped.status.state in PAUSED_TASK_STATES
                and self._on_drop is not None
            ):
                await self._on_drop(dropped)


class A2AServer:
    """The A2A face of one ``AgentApp``: its card, handler and routes.

    Built by ``AgentApp`` when ``a2a`` is on; nothing here is reached when it
    is off.
    """

    def __init__(self, app: AgentApp, config: A2AConfig | None = None):
        self._app = app
        self.config = config or A2AConfig()
        self.mode, url = _store_mode(self.config)
        # Whose a task is: the deployment's, or one caller's of it when the
        # agent is private (memory_routes=False). Every store files and
        # filters by it.
        self.ownership = _Ownership(
            private=not app._memory_routes, on_platform=_on_platform()
        )
        self.stores: _DurableStores | None = None
        self.spill: Any = None
        self._sweeper: asyncio.Task | None = None
        # the platform's signature over the card at the public URL, once asked for
        self._signature: dict[str, str] | None = None
        self.executor = HopsworksAgentExecutor(app, self.config)
        if self.mode == "database":
            # In the project's online database: any replica reads, cancels,
            # answers or follows a task, and a task outlives the pod that ran it.
            self.stores = _DurableStores(url or "", self.ownership)
            handler_store: Any = self.stores.task_store
            # A task is made to fit its row as it is saved (large parts go to
            # the artifact storage when there is one); the store refuses the
            # writes of a replica that lost the task's lease.
            self.spill = _spill_backend()
            if self.spill is not None:
                # A deleted task's files are filed for removal with the task,
                # under where they are: files in the project are removed by
                # whoever gets to it (this replica, another, or the platform),
                # files in an author's directory by a replica that has it.
                self.stores.cleanups.location = self.spill.location
                self.stores.task_store.cleanups = self.stores.cleanups
            handler_store = _SpillingStore(
                self.stores.task_store,
                self.spill,
                self._public_url,
                after_save=self._saved,
                cleanups=self.stores.task_store.cleanups,
            )
            self.task_store: Any = self.stores.task_store.as_task_store
            event_stream: Any = self.stores.event_stream
            _logger.info("A2A tasks are kept in the project's online database")
        else:
            # In memory, bounded: tasks are per replica and gone with the pod.
            # Said in the log on a deployment, where it is what an operator who
            # scaled the agent out would otherwise find out from a lost task.
            _logger.log(
                logging.WARNING if _on_platform() else logging.INFO,
                "A2A tasks are kept in memory, per replica: a task is read, "
                "cancelled or answered on the replica that runs it, and lost with it",
            )
            self.task_store = _BoundedTaskStore(
                self.config.max_tasks,
                self.config.max_task_bytes,
                on_drop=self._dropped,
                owner=self.ownership._owner,
            )
            handler_store = self.task_store
            event_stream = None
        #: The store the SDK's handler saves through.
        self.handler_store = handler_store
        # Push notifications: the configs live beside the tasks, and the
        # sender delivers from a queue so a slow receiver never holds a task's
        # own stream (see a2a_push).
        # On a deployment the configs, which hold a receiver's credentials,
        # are stored encrypted with the deployment's key; without the key push
        # is not served, rather than served with the credentials in the clear.
        self.push = not (
            self.stores is not None
            and _on_platform()
            and not os.environ.get(_store.PUSH_KEY_ENV)
        )
        if not self.push:
            _logger.warning(
                "the platform gave this deployment no key to encrypt push "
                "notification configs with, so push notifications are not served"
            )
            self.push_configs: Any = None
        elif self.stores is not None:
            self.push_configs = self.stores.push_configs
        else:
            from a2a.server.tasks import (  # noqa: PLC0415
                InMemoryPushNotificationConfigStore,
            )

            self.push_configs = InMemoryPushNotificationConfigStore(
                owner_resolver=self.ownership._owner
            )
        self.push_sender: _PushSender | None = (
            _PushSender(self.push_configs, on_terminal_delivered=self._terminal_pushed)
            if self.push
            else None
        )
        self.handler = DefaultRequestHandlerV2(
            agent_executor=self.executor,
            task_store=handler_store,
            agent_card=self._card(self._public_url() or "http://localhost"),
            request_context_builder=_RequestChecks(
                should_populate_referred_tasks=True, task_store=self.task_store
            ),
            validate_input_modes=True,
            interrupted_task_timeout=INTERRUPTED_TASK_TIMEOUT_S,
            event_stream=event_stream,
            push_config_store=self.push_configs,
            push_sender=self.push_sender,
            push_url_validator=_valid_webhook if self.push else None,
            # what an authenticated caller may ask for beyond the public card;
            # rebuilt on each request, since the manifest it carries is live
            extended_agent_card=self._extended_card(
                self._public_url() or "http://localhost"
            ),
            extended_card_modifier=self._current_extended_card,
        )

        # A part moved out of a task's row is served to whoever may read the
        # task: the lookup goes through the task, under the caller's owner.
        @app.get(ARTIFACTS_PATH + "/{task_id}/{digest}")
        async def spilled_part(task_id: str, digest: str, request: Request) -> Response:
            found = None
            if self.spill is not None:
                context = ServerCallContext(state={"headers": dict(request.headers)})
                task = await self.task_store.get(task_id, context)
                if task is not None:
                    found = await _read_spilled(self.spill, task, digest)
            if found is None:
                return JSONResponse(
                    {"detail": {"code": "not_found", "message": "No such artifact."}},
                    status_code=404,
                )
            data, media_type, filename = found
            headers = {"Cache-Control": "private, max-age=3600"}
            if filename:
                safe = "".join(c for c in filename if c.isalnum() or c in "._- ")
                headers["Content-Disposition"] = f'attachment; filename="{safe}"'
            return Response(content=data, media_type=media_type, headers=headers)

        # The card is served by the app, not by the SDK's route: its interface
        # URLs are the request's when the platform set no public URL, and the
        # SDK's card modifier sees the card, never the request.
        @app.get(CARD_PATH)
        async def agent_card(request: Request) -> JSONResponse:
            base = self._public_url() or _base_url(request)
            return JSONResponse(self._card_dict(base))

        # The REST routes come with a mount at /{tenant} for tenant-scoped
        # paths. One deployment is one agent and tenants are ignored, and the
        # mount would swallow every later route with a path of two segments
        # (an author's own, say), so it is left out.
        rest_routes = [
            route
            for route in create_rest_routes(
                self.handler, path_prefix=REST_PREFIX, enable_v0_3_compat=True
            )
            if not isinstance(route, Mount)
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
            "push_notifications": self.push,
            "extended_card": True,
            "signed": self._signature is not None,
            "task_store": self.mode,
            # the in-memory store's bound; the database keeps tasks by age
            "task_limit": self.config.max_tasks if self.mode == "memory" else None,
            "serialize_context": self.config.serialize_context,
            # whether each caller sees only its own tasks and conversations
            "private": self.ownership.private,
            "retry_lost_tasks": self.config.retry_lost_tasks,
        }

    async def _start(self) -> None:
        """At startup: the sweep for tasks whose replica was lost, when tasks are shared."""
        if self.stores is not None and self._sweeper is None:
            if not await self.stores._tables_exist():
                # the tables are the platform's to create; without them every
                # task request fails, which is better said once and plainly
                _logger.error(
                    "the A2A task tables are missing from the project's online "
                    "database, so A2A requests to this agent will fail: the "
                    "platform creates them when an agent deployment starts "
                    "(Hopsworks 5.3 or later); until then, serve A2A from "
                    'memory with a2a=A2AConfig(task_store="memory")'
                )
            self._sweeper = asyncio.create_task(self._sweep())
        await self._sign()

    async def _sign(self) -> None:
        """Ask the platform to sign the card this deployment serves at its public URL.

        The signing key never enters a pod: the platform holds it, checks that
        the card's URLs are the ones it gave this deployment, and returns a
        signature anyone can verify against the cluster's published keys.
        Only the card at the platform's URL is signed; the card built from a
        request's own headers (a port-forward, a local run) is served unsigned.
        An older platform has nothing to sign with, and the card stays unsigned.
        """
        public = os.environ.get("HOPSWORKS_AGENT_PUBLIC_URL")
        # the platform's own address for it, which an older platform does not set
        sign_url = os.environ.get(CARD_SIGN_URL_ENV)
        platform = self._app._platform
        if not public or not sign_url or not getattr(platform, "configured", False):
            return
        from a2a.utils.signing import _canonicalize_agent_card  # noqa: PLC0415

        try:
            payload = _canonicalize_agent_card(self._card(public))
            # as the deployment, with its own key: the platform signs a card
            # only for the deployment whose URLs it names
            answer = await asyncio.to_thread(
                platform._post_as_deployment, sign_url, {"payload": payload}
            )
            signature = answer["signature"]
            self._signature = {
                "protected": str(signature["protected"]),
                "signature": str(signature["signature"]),
            }
        except Exception as err:  # noqa: BLE001 -- unsigned is a state, not a failure
            _logger.warning("the Agent Card is served unsigned: %s", err)

    async def _stop(self) -> None:
        """At shutdown, after the drain: the sweep ends and the database connections close."""
        if self._sweeper is not None:
            self._sweeper.cancel()
            await asyncio.gather(self._sweeper, return_exceptions=True)
            self._sweeper = None
        # the notifications of the turns the drain just ended are still queued
        if self.push_sender is not None:
            await self.push_sender._drain(PUSH_DRAIN_SECONDS)
        if self.stores is not None:
            await self.stores._close()

    async def _saved(self, task: Task) -> None:
        """A task this replica runs was saved: when the state it reached ends the run, its lease ends with it.

        The lease is given up here, when the terminal or paused state is in
        the store, and not when the execution returns: what the execution
        enqueued is saved after it returned, and those writes are still this
        replica's to make, under the lease, and nobody else's.
        """
        stores = self.stores
        if stores is None:
            return
        state = task.status.state
        if state not in TERMINAL_TASK_STATES and state not in PAUSED_TASK_STATES:
            return
        held = self.executor._held.get(task.id)
        if state in PAUSED_TASK_STATES and held is not None and not held.paused:
            # still paused as the answer that resumes it is recorded: the
            # resumed turn holds the lease, and keeps it
            return
        fence = stores.fence.held.pop(task.id, None)
        if fence is None:
            return
        try:
            if state in PAUSED_TASK_STATES:
                await stores.leases._release(task.id, fence)
            else:
                await self._settle_lease(task.id, fence)
        except Exception:  # noqa: BLE001 -- a row left behind is reaped
            _logger.debug(
                "could not give up the lease of task %s", task.id, exc_info=True
            )

    async def _terminal_pushed(self, task_id: str) -> None:
        """A task's terminal notification reached its receivers: its outbox row has done its work."""
        if self.stores is not None:
            await self.stores.leases._pushed(task_id)

    async def _settle_lease(self, task_id: str, fence: int) -> None:
        """A task this replica held ended: its lease goes, or stays as the outbox of its terminal notification.

        It stays only while a receiver is registered and has not been told;
        the platform delivers from the row when this replica does not.
        """
        stores = self.stores
        if stores is None:
            return
        sender = self.push_sender
        if sender is None:
            await stores.leases._settle(task_id, fence, pushed=True)
            return
        record = sender.records.get(task_id)
        pushed = record is not None and record.terminal_delivered
        if not pushed:
            try:
                pushed = not await self.push_configs.get_info_for_dispatch(task_id)
            except Exception:  # noqa: BLE001 -- kept as an outbox row when unsure
                pushed = False
        await stores.leases._settle(task_id, fence, pushed=pushed)
        if not pushed:
            # the delivery may have finished while the row was being written,
            # and then found no row to clear
            record = sender.records.get(task_id)
            if record is not None and record.terminal_delivered:
                await stores.leases._pushed(task_id)

    async def _sweep(self) -> None:
        """While the replica lives: run the tasks handed back to it, fail the ones whose replica was lost, remove deleted tasks' files.

        Tasks the platform handed back to be run again are taken at start
        and at every heartbeat interval; lost leases and leftover files every
        sweep interval.
        The backend runs the same sweep, which is what holds when no replica
        is left; a surviving replica is only the faster of the two.
        """
        loop = asyncio.get_running_loop()
        next_sweep = loop.time() + SWEEP_SECONDS
        while True:
            try:
                await self._take_handed_back()
                if loop.time() >= next_sweep:
                    next_sweep = loop.time() + SWEEP_SECONDS
                    await self._reap()
                    await self._clean_spills()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 -- the next round tries again
                _logger.debug("the lost-task sweep failed", exc_info=True)
            await asyncio.sleep(_store.HEARTBEAT_SECONDS)

    async def _take_handed_back(self) -> int:
        """Run the tasks the platform handed back to this deployment to be run again; how many were taken."""
        stores = self.stores
        if stores is None:
            return 0
        taken = await stores.leases._take_handed_back()
        for lease in taken:
            await self._lost(lease)
        return len(taken)

    async def _clean_spills(self) -> int:
        """Remove the files of deleted tasks that are still filed for removal; how many were removed.

        Only the rows filed where this replica keeps its files: a replica
        with the project's files leaves an author's directory alone, and the
        other way round.
        """
        stores = self.stores
        if stores is None or self.stores.task_store.cleanups is None:
            return 0
        cleanups = stores.cleanups
        removed = 0
        for task_id in await cleanups._due():
            try:
                await asyncio.to_thread(self.spill._remove, task_id)
            except Exception:  # noqa: BLE001 -- counted, and given up on past the limit
                attempts = await cleanups._failed(task_id)
                if attempts >= cleanups.ATTEMPTS:
                    _logger.error(
                        "could not remove the files of deleted task %s after %d "
                        "attempts; they are left where they are",
                        task_id,
                        attempts,
                        exc_info=True,
                    )
                    await cleanups._done(task_id)
                continue
            await cleanups._done(task_id)
            removed += 1
        return removed

    async def _reap(self) -> int:
        """Take the lost leases of this deployment and settle each task; how many were taken."""
        stores = self.stores
        if stores is None:
            return 0
        taken = await stores.leases._take_lost()
        for lease in taken:
            await self._lost(lease)
        return len(taken)

    async def _retry(self, lease: dict[str, Any], task: Task) -> bool:
        """Run a lost task again here, from the start, when its author said that is safe; whether it was started.

        Only before any terminal state was published: the task stays
        ``WORKING`` throughout, and is failed only when no retry is left.
        The turn runs again from the message that started it, under the
        fence and attempt the take holds (the execution neither takes the
        lease again nor rewrites its policy), with the attempt counted in its
        metadata.
        How many retries there are is read from the lease, where the replica
        that started the task wrote its author's word, so this replica, any
        other and the platform decide alike. A task the platform handed back
        was decided there.
        """
        # the take counted this run: retries used are the attempts after the first
        retries_used = int(lease["attempt"]) - 1
        if not lease.get("handed_back") and retries_used > int(
            lease.get("retry_limit") or 0
        ):
            return False
        asked = [m for m in task.history if m.role == Role.ROLE_USER]
        if not asked or task.status.state in PAUSED_TASK_STATES:
            return False
        message = Message()
        message.CopyFrom(asked[-1])
        message.task_id = task.id
        message.context_id = task.context_id
        owner = str(lease["owner"])
        # The run is the recovery the take made: it runs under the fence and
        # attempt taken, and the lease keeps the policy it was written with.
        recovery = {"fence": int(lease["fence"]), "attempt": int(lease["attempt"])}
        state: dict[str, Any] = {OWNER_STATE_KEY: owner, RETRY_STATE_KEY: recovery}
        if self.ownership.private and _store.OWNER_SEPARATOR in owner:
            # the caller the task belongs to, for the memory the turn reads
            principal = owner.split(_store.OWNER_SEPARATOR, 1)[1]
            state["headers"] = {"x-hopsworks-principal": principal}
        from a2a.types import (  # noqa: PLC0415
            SendMessageConfiguration,
            SendMessageRequest,
        )

        try:
            await self.handler.on_message_send(
                SendMessageRequest(
                    message=message,
                    configuration=SendMessageConfiguration(return_immediately=True),
                ),
                ServerCallContext(state=state),
            )
        except Exception:  # noqa: BLE001 -- failed visibly instead
            _logger.warning(
                "could not run lost task %s again; it is failed", task.id, exc_info=True
            )
            return False
        _logger.info(
            "lost task %s is run again here (attempt %d)", task.id, lease["attempt"]
        )
        return True

    async def _lost(self, lease: dict[str, Any]) -> None:
        """A task whose replica was lost: failed, visibly, with what the caller needs to decide.

        Nothing is known about how far the handler got, so the outcome is
        ``unknown`` rather than failed, and the caller may resend.
        Run again instead when the lease allows a retry, or the platform
        handed the task back to be run again.
        """
        stores = self.stores
        if stores is None:  # pragma: no cover - only called with stores
            return
        task_id = lease["task_id"]
        context = ServerCallContext(state={OWNER_STATE_KEY: lease["owner"]})
        stored = await stores.task_store.get(task_id, context)
        if stored is None or stored.task.status.state in TERMINAL_TASK_STATES:
            await self._settle_lease(task_id, lease["fence"])
            return
        if await self._retry(lease, stored.task):
            return
        event = None
        # The replica that lost the lease may still have advanced the task
        # before it noticed, which fails the version check here; the write is
        # then made again on what the task has become, until it lands or the
        # task is found ended.
        for _ in range(LOST_WRITE_ATTEMPTS):
            task = stored.task
            status = TaskStatus(
                state=TaskState.TASK_STATE_FAILED,
                message=Message(
                    message_id=f"lost-{task_id}-{lease['fence']}",
                    role=Role.ROLE_AGENT,
                    task_id=task_id,
                    context_id=task.context_id,
                    parts=[
                        Part(
                            text="The replica running this task was lost before it "
                            "finished; what the task did before that is not known."
                        )
                    ],
                    metadata=_hopsworks(
                        {
                            "status": "executor_lost",
                            "outcome": "unknown",
                            "retryable": True,
                            "http_status": 503,
                            "attempt": lease["attempt"] - 1,
                        }
                    ),
                ),
            )
            status.timestamp.GetCurrentTime()
            task.status.CopyFrom(status)
            event = TaskStatusUpdateEvent(
                task_id=task_id, context_id=task.context_id, status=status
            )
            try:
                # under the lease this sweep took, not one an executor holds
                await stores.task_store.save(
                    task,
                    event=event,
                    prev=None,
                    prev_version=stored.version,
                    context=context,
                    fence=lease["fence"],
                )
                break
            except ConcurrentTaskModificationError:
                event = None
                stored = await stores.task_store.get(task_id, context)
                if stored is None or stored.task.status.state in TERMINAL_TASK_STATES:
                    break
        else:
            _logger.error(
                "lost task %s kept moving and could not be failed; its lease is "
                "kept for the next sweep",
                task_id,
            )
            return
        if event is not None and self.push_sender is not None:
            # no replica runs this task, so nobody else tells its receivers
            await self.push_sender.send_notification(task_id, event)
        await self._settle_lease(task_id, lease["fence"])

    def _running(self) -> set[asyncio.Task]:
        """The executions accepted and not yet over, for the drain on shutdown.

        Every execution, not only the turns whose handler runs: one queued
        behind a context lock has been accepted too, and would otherwise start
        after the drain looked.
        """
        return {task for task in self.executor._executing if not task.done()}

    async def _dropped(self, task: Task) -> None:
        """A paused task left the store: its open question goes with it."""
        await self.executor._forget_questions(task)

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
            # the same URLs serve the 0.3 protocol too (a request without
            # the A2A-Version header is taken for it), and a 0.3 client finds
            # the agent only through interfaces of its own version
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
                {
                    "url": base + RPC_PATH,
                    "protocolBinding": TransportProtocol.JSONRPC,
                    "protocolVersion": PROTOCOL_VERSION_0_3,
                },
                {
                    "url": base + REST_PREFIX,
                    "protocolBinding": TransportProtocol.HTTP_JSON,
                    "protocolVersion": PROTOCOL_VERSION_0_3,
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
                "pushNotifications": self.push,
                "extendedAgentCard": True,
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

    def _card_dict(self, base_url: str) -> dict[str, Any]:
        """The card as served: every 1.0 field, present even when false, plus the 0.3 fields.

        A 0.3 client reads ``url``, ``preferredTransport`` and
        ``protocolVersion`` at the top of the card; the SDK derives them from
        the 0.3 interfaces and adds them where the 1.0 card has nothing.
        """
        card = self._card(base_url)
        served = MessageToDict(card, always_print_fields_with_no_presence=True)
        _merge_missing(served, agent_card_to_dict(card))
        if self._signature is not None and base_url.rstrip("/") == (
            os.environ.get("HOPSWORKS_AGENT_PUBLIC_URL") or ""
        ).rstrip("/"):
            served["signatures"] = [dict(self._signature)]
        return served

    def _extended_card(self, base_url: str) -> AgentCard:
        """The card an authenticated caller may ask for: the public one, and what the Hopsworks manifest knows.

        The memory tiers, the conversation and feedback endpoints, tool
        events, the agent's graph and its evaluation mode ride as parameters
        of the Hopsworks extension, where a client that knows the extension
        finds them and any other ignores them.
        """
        card = self._card(base_url)
        manifest = self._app._manifest()
        capabilities = manifest.get("capabilities") or {}
        custom = (capabilities.get("custom") or {}).get("hopsworks") or {}
        for extension in card.capabilities.extensions:
            if extension.uri == HOPSWORKS_EXTENSION_URI:
                extension.params.update(
                    _struct(
                        {
                            "agent": {
                                "endpoints": manifest.get("endpoints"),
                                "protocol": manifest.get("protocol"),
                                "ui": manifest.get("ui"),
                                "hopsworks": custom,
                            }
                        }
                    )
                )
        return card

    async def _current_extended_card(
        self, card: AgentCard, context: ServerCallContext
    ) -> AgentCard:
        """The extended card for this request: at the public URL, or where the request came in."""
        public = self._public_url()
        if public:
            return self._extended_card(public)
        state = getattr(context, "state", None) or {}
        headers = {
            str(k).lower(): str(v) for k, v in (state.get("headers") or {}).items()
        }
        host = headers.get("x-forwarded-host") or headers.get("host")
        if not host:
            return self._extended_card("http://localhost")
        proto = headers.get("x-forwarded-proto") or "http"
        prefix = (headers.get("x-forwarded-prefix") or "").rstrip("/")
        return self._extended_card(f"{proto}://{host}{prefix}")


def _merge_missing(into: dict[str, Any], extra: dict[str, Any]) -> None:
    """Add what ``extra`` has and ``into`` lacks, key by key, into nested dicts too."""
    for key, value in extra.items():
        if key not in into:
            into[key] = value
        elif isinstance(value, dict) and isinstance(into[key], dict):
            _merge_missing(into[key], value)


def _owner_only(
    context: ServerCallContext,
    resolver: Callable[[ServerCallContext], str] | None = None,
) -> ServerCallContext:
    """What the task store scopes by, without the request's headers and the rest of its state."""
    state: dict[str, Any] = {}
    if resolver is not None:
        try:
            state[OWNER_STATE_KEY] = resolver(context)
        except Exception:  # noqa: BLE001 -- kept under the request's own identity
            state = {}
    return ServerCallContext(user=context.user, tenant=context.tenant, state=state)


class _RequestChecks(SimpleRequestContextBuilder):
    """The request context, after the refusals that must come before any task event.

    The SDK answers an error raised here as the named JSON-RPC error and
    touches no task; one raised by the executor fails the task instead. So
    what spec 4.3 and 4.5 refuse as ``InvalidParamsError`` is checked here:
    a message not from the user, a context id no record could be keyed by,
    and a follow-up that answers a question the paused task did not ask or
    leaves one unanswered.
    """

    async def build(
        self,
        context: ServerCallContext,
        params: Any = None,
        task_id: str | None = None,
        context_id: str | None = None,
        task: Task | None = None,
    ) -> RequestContext:
        message = params.message if params is not None else None
        if message is not None:
            if message.role != Role.ROLE_USER:
                raise InvalidParamsError(
                    message="Only messages with role ROLE_USER are accepted."
                )
            if context_id and not _printable(context_id):
                raise InvalidParamsError(
                    message="The contextId may not carry control characters."
                )
            # the handler builds the context before it reads the task, so a
            # follow-up's task is read here
            current = task
            if current is None and task_id and self._task_store is not None:
                current = await self._task_store.get(task_id, context)
            if current is not None and current.status.state in PAUSED_TASK_STATES:
                await self._check_answers(message, current, context)
        return await super().build(
            context,
            params=params,
            task_id=task_id,
            context_id=context_id,
            task=task,
        )

    async def _check_answers(
        self, message: Message, task: Task, context: ServerCallContext
    ) -> None:
        """A follow-up must answer every open question of the paused task, and nothing else."""
        open_questions = _open_questions(task)
        asked = {str(q.get("id")) for q in open_questions}
        answered = {a.interrupt_id for a in _answers(message, open_questions)}
        if answered - asked:
            raise InvalidParamsError(
                message="The message answers questions this task was not asked: "
                + ", ".join(sorted(answered - asked))
            )
        if asked - answered:
            raise InvalidParamsError(
                message="The message leaves these questions unanswered: "
                + ", ".join(sorted(asked - answered))
            )


def _base_url(request: Request) -> str:
    """The base URL a request reached the app at, through whatever is in front of it."""
    headers = request.headers
    proto = headers.get("x-forwarded-proto") or request.url.scheme
    host = headers.get("x-forwarded-host") or headers.get("host") or request.url.netloc
    prefix = (headers.get("x-forwarded-prefix") or "").rstrip("/")
    return f"{proto}://{host}{prefix}"


@dataclass
class _Held:
    """What one execution holds while it runs: its lease, and what the heartbeat learned about it."""

    #: The fence the lease is held under; None when tasks are not shared.
    fence: int | None
    #: Which run of the task this is: more than 1 for a lost task run again.
    attempt: int = 1
    #: The turn, once it runs, and its context, once it is built.
    run: Any = None
    ctx: Any = None
    #: The lease is someone else's now: nothing more is written to the task from here.
    lost: bool = False
    #: Another replica cancelled the task: CANCELED is already in the store.
    cancelled: bool = False
    #: The turn asked a person: the paused state it saves ends the lease.
    paused: bool = False


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
        # what each execution in flight holds, by task id
        self._held: dict[str, _Held] = {}

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
        # A private agent's stores are read and written as the caller, named by
        # the gateway on the request this execution serves.
        token = None
        if not self._app._memory_routes:
            token = _principal.set(_principal_of(_headers(context)))
        task_id = context.task_id or ""
        held: _Held | None = None
        beat: asyncio.Task | None = None
        try:
            # The lease comes first, before the task is created or the caller
            # is told anything: a task that was acknowledged is one a reaper
            # can find, whether its turn is running or queued behind another.
            held = await self._hold(context)
            self._held[task_id] = held
            beat = asyncio.create_task(self._heartbeat(context, held))
            await self._execute(context, event_queue, held)
        finally:
            if beat is not None:
                beat.cancel()
            if held is not None:
                self._held.pop(task_id, None)
                await self._leave_lease(task_id, held)
            if token is not None:
                _principal.reset(token)
            if current is not None:
                self._executing.discard(current)

    async def _execute(
        self, context: RequestContext, event_queue: EventQueue, held: _Held
    ) -> None:

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
        if app._draining:
            # accepted after the pod was told to stop: nothing starts outside
            # the drain budget, and the caller is told to resend
            await self._shutdown(updater, None)
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
        held.ctx = ctx
        refused = await app._attach_evaluation(ctx, headers)
        if refused is not None:
            await app._settle_pending(ctx, answered=False)
            await self._reject(updater, refused)
            return
        if (
            context.current_task is not None
            and context.current_task.status.state in PAUSED_TASK_STATES
        ):
            # a follow-up to a paused task; a lost task run again stays WORKING
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
                if held.lost or held.cancelled:
                    # while it waited, its lease was taken or the task was
                    # cancelled elsewhere: whoever did that settled the task
                    await app._settle_pending(ctx, answered=False)
                    return
                if app._draining:
                    # its turn came after the drain began: a running turn was
                    # given the budget, a queued one does not start
                    await app._settle_pending(ctx, answered=False)
                    await self._shutdown(updater, None)
                    return
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
        held = self._held.get(task_id) or _Held(None)
        if ctx._claimed:
            protected = await self._protect(task_id, held)
            if not protected:
                # a cancel landed first (or the task is no longer this
                # replica's): the approved action does not run
                held.cancelled = protected is False
                await app._settle_pending(ctx, answered=protected is False)
                return
        await app._resolve_subject(ctx)
        await app._open_turn(ctx)
        # WORKING follows, with the trace id, which is how a caller joins the
        # task to its trace.
        completed: ChatResponse | None = None
        run: _TurnRun | None = None
        attempt = held.attempt
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
                    metadata=_hopsworks(
                        {
                            "trace_id": trace_id,
                            "protocol": "a2a",
                            # said from the second run of a lost task on
                            "attempt": attempt if attempt > 1 else None,
                        }
                    ),
                )
                run = _TurnRun(app, ctx)
                self.runs[task_id] = run
                held.run = run
                if held.lost:
                    # the lease went while the turn was being set up
                    run._abort()
                artifacts = _Artifacts(updater, self._accepted(context))
                while True:
                    kind, payload = await run._next()
                    if kind == "done":
                        if held.lost:
                            # the lease is someone else's now, and so is the
                            # task: this replica writes nothing more to it
                            _mark_cancelled(span)
                            await run._close(None)
                            return
                        if held.cancelled and not run.detached:
                            # CANCELED is already in the store, written by the
                            # replica the cancel reached
                            _mark_cancelled(span)
                            await run._close(None)
                            return
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
                # the facts ride on the final message and on the event, so
                # they are on the task itself as well as on its status
                facts = _hopsworks(
                    {
                        **self._facts(ctx, response, trace_id, artifacts.dropped),
                        "attempt": attempt if attempt > 1 else None,
                    }
                )
                await updater.update_status(
                    TaskState.TASK_STATE_COMPLETED,
                    message=updater.new_agent_message(
                        [Part(text=_text_of(response))], metadata=facts
                    ),
                    metadata=facts,
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
                    run.finished.set()

    # ── owning the task while its turn runs ──────────────────────────────

    def _stores(self) -> _DurableStores | None:
        server = self._app._a2a
        return server.stores if server is not None else None

    async def _hold(self, context: RequestContext) -> _Held:
        """Record that this replica has the task, before anything else is done with it.

        A fresh turn takes the lease and writes its author's retry policy on
        it. A recovery (a lost task the sweep took, or one the platform
        handed back) already holds the lease under the fence and attempt it
        took: it runs under those, and the policy the task started with
        stays, whatever this replica's own configuration says.
        """
        stores = self._stores()
        task_id = context.task_id or ""
        if stores is None or context.call_context is None:
            return _Held(None)
        recovery = (context.call_context.state or {}).get(RETRY_STATE_KEY)
        if isinstance(recovery, dict):
            fence, attempt = int(recovery["fence"]), int(recovery["attempt"])
        else:
            owner = self._app._a2a.ownership._owner(context.call_context)
            config = self._config
            # the author's policy, on the row: whoever finds the turn lost reads it there
            retry_limit = config.retry_lost_tasks if config.idempotent else 0
            fence, _ = await stores.leases._acquire(task_id, owner, retry_limit)
            attempt = 1
        if len(stores.fence.out) > FENCED_OUT_LIMIT:
            stores.fence.out.clear()
        stores.fence.out.discard(task_id)
        stores.fence.held[task_id] = fence
        return _Held(fence, attempt)

    async def _protect(self, task_id: str, held: _Held) -> bool | None:
        """Before a resumed turn runs the action a person approved: make sure no cancel lands on it from now on.

        True when the action may run: the lease says so, in the same
        transaction that found the task not ended, and a cancel from any
        replica is refused from then on.
        False when the task ended first (a cancel landed before the turn
        got here) or the lease is no longer this replica's.
        None when the database could not be asked: the action does not run,
        and the person's answer is given back so it can be sent again.
        """
        stores = self._stores()
        if stores is None or held.fence is None:
            # tasks are per replica: the local cancel is the only one
            return True
        try:
            return await stores.task_store._protect(task_id, held.fence)
        except Exception:  # noqa: BLE001 -- fail closed: the action does not run
            _logger.error(
                "could not record that task %s runs an approved action, so it "
                "is not run",
                task_id,
                exc_info=True,
            )
            return None

    async def _leave_lease(self, task_id: str, held: _Held) -> None:
        """The execution returned: what is left of its lease to deal with here.

        Normally nothing: the lease ends when the state the execution reached
        is saved (``A2AServer._saved``), which happens after it returns.
        A task another replica cancelled is already ended in the store, so
        its lease is settled here.
        A lost lease is someone else's.
        An execution that returned without its last state ever being saved
        leaves a lease nobody renews, which a reaper takes.
        """
        stores = self._stores()
        if stores is None or held.fence is None:
            return
        if held.lost:
            stores.fence.held.pop(task_id, None)
            return
        if not held.cancelled:
            return
        if stores.fence.held.pop(task_id, None) is None:
            return
        try:
            await self._app._a2a._settle_lease(task_id, held.fence)
        except Exception:  # noqa: BLE001 -- a row left behind is reaped
            _logger.debug(
                "could not give up the lease of task %s", task_id, exc_info=True
            )

    async def _heartbeat(self, context: RequestContext, held: _Held) -> None:
        """While the execution lasts, every few seconds: keep what it holds, and notice what changed elsewhere.

        From before the task is acknowledged until its execution returns,
        whether the turn runs or waits its turn behind another.
        Renews the lease, so nobody takes the task for lost.
        Renews the claim of a resumed turn, so a turn longer than the claim's
        lease is not resumed a second time by another replica.
        Reads the task's state, so a cancel that reached another replica stops
        the handler here within one interval, even inside a tool that reports
        nothing.
        A lease that is no longer this replica's, or a database that could not
        be reached for several intervals, stops the turn: the task is then
        someone else's to settle.
        """
        task_id = context.task_id or ""
        stores = self._stores()
        missed = 0
        while True:
            await asyncio.sleep(_store.HEARTBEAT_SECONDS)
            ctx = held.ctx
            if ctx is not None and ctx._claimed:
                try:
                    await asyncio.to_thread(
                        self._app._pending_store()._renew, ctx._pending_key
                    )
                except Exception:  # noqa: BLE001 -- the next beat renews it
                    _logger.debug(
                        "could not renew the claim of %s", task_id, exc_info=True
                    )
            if stores is None or held.fence is None:
                continue
            try:
                kept = await stores.leases._heartbeat(task_id, held.fence)
                state = await stores._state(task_id)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 -- counted, and the turn stopped past the limit
                missed += 1
                _logger.warning(
                    "could not renew the lease of task %s (%d of %d)",
                    task_id,
                    missed,
                    MISSED_HEARTBEATS,
                )
                if missed < MISSED_HEARTBEATS:
                    continue
                kept, state = False, None
            else:
                missed = 0
            if not kept:
                _logger.error(
                    "the lease of task %s is lost: the turn is stopped and "
                    "nothing more is written to the task from this replica",
                    task_id,
                )
                held.lost = True
                stores.fence.out.add(task_id)
                if held.run is not None:
                    held.run._abort()
                return
            if state == TaskState.Name(TaskState.TASK_STATE_CANCELED):
                held.cancelled = True
                if held.run is not None:
                    held.run._hangup()
                return

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Stop the task's turn: the handler is interrupted at its next ``await``.

        A resumed turn is running the action a person approved (a refund, a
        delete), possibly in a thread a cancel would not stop; it is left to
        finish, and the cancel waits for it and is then refused, as
        ``TaskNotCancelableError``: the task ends in the state the action
        earned, never as ``CANCELED`` over an action that completed. (Refused
        before the turn ended, the SDK would fail the task.)
        A paused task has no turn to stop: its open question goes with it, so
        a late answer is refused rather than resumed.
        The SDK records ``CANCELED`` once the producer has stopped.

        Raises:
            UnsupportedOperationError: The task was a resumed turn, which has
                now finished; the SDK answers ``TaskNotCancelableError``.
        """
        run = self.runs.get(context.task_id or "")
        if run is not None:
            if run.ctx._claimed and not run.task.done():
                await run.finished.wait()
                await self._stored_as_terminal(context)
                raise UnsupportedOperationError(
                    message="The task ran an action a person approved, which "
                    "has finished; it was not cancelled."
                )
            run._hangup()
            return
        current = context.current_task
        if current is not None and current.status.state in PAUSED_TASK_STATES:
            # no turn runs: the executor records the cancel itself
            await self._forget_questions(current)
            await TaskUpdater(
                event_queue, current.id, current.context_id or ""
            ).cancel()

    async def _stored_as_terminal(self, context: RequestContext) -> None:
        """Wait until the task's terminal state is in the store, so a refusal does not land before it."""
        store = self._app._a2a.task_store if self._app._a2a is not None else None
        if store is None or context.call_context is None:
            return
        for _ in range(500):
            task = await store.get(context.task_id or "", context.call_context)
            if task is None or task.status.state in TERMINAL_TASK_STATES:
                return
            await asyncio.sleep(0.01)

    async def _forget_questions(self, task: Task) -> None:
        """Clear a paused task's open question: it is being cancelled or dropped."""
        key = _task_key(task.context_id, task.id)
        try:
            await asyncio.to_thread(self._app._pending_store()._clear, key)
        except Exception:  # noqa: BLE001 -- best effort, the record expires on its own
            _logger.debug(
                "could not clear the question of task %s", task.id, exc_info=True
            )

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
        if current is not None and current.status.state in PAUSED_TASK_STATES:
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
        # The fetch runs in a thread, which a cancel cannot interrupt: it is
        # told to stop instead, and stops at its next chunk.
        stop = threading.Event()
        try:
            fetched_type, payload = await asyncio.to_thread(_fetch, url, stop)
        except asyncio.CancelledError:
            stop.set()
            raise
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
        A follow-up that answers a question the paused task did not ask, or
        leaves one unanswered, was refused as ``InvalidParamsError`` before
        this ran (``_RequestChecks``), with the task left paused.
        A question that expired fails the task, as it does on every protocol,
        and so does a paused task whose record is gone (the pod restarted, or
        the task was dropped past the store's budget): asked again, it could
        never be answered.
        Anything else leaves the task paused, asked again with the reason in
        front, so a store that could not be read does not end the task.
        """
        current = context.current_task
        paused = current is not None and current.status.state in PAUSED_TASK_STATES
        if paused and err.code in ("interrupt_expired", "nothing_to_resume"):
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
        held = self._held.get(updater.task_id)
        if held is not None:
            # the pause this execution asks for, which ends its lease once saved
            held.paused = True
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
        # the artifacts sent so far, by id, with the name their author gave
        self._seen: dict[str, str] = {}
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
        for artifact_id in sorted(set(self._seen) - self._closed):
            await self._send(artifact_id, self._seen[artifact_id], [], last=True)

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
        self._seen[artifact_id] = name
        if last:
            self._closed.add(artifact_id)
        else:
            self._closed.discard(artifact_id)


# ── helpers ──────────────────────────────────────────────────────────────


def _hopsworks(facts: dict[str, Any]) -> dict[str, Any]:
    return {"hopsworks": _struct({k: v for k, v in facts.items() if v is not None})}


def _hopsworks_of(message: Message) -> dict[str, Any]:
    # the metadata alone: converting the whole message would base64 every raw part
    metadata = MessageToDict(message.metadata) if message.HasField("metadata") else {}
    hopsworks = metadata.get("hopsworks")
    return hopsworks if isinstance(hopsworks, dict) else {}


def _headers(context: RequestContext) -> dict[str, str]:
    state = getattr(context.call_context, "state", None) or {}
    headers = state.get("headers") if isinstance(state, dict) else None
    return {str(k).lower(): str(v) for k, v in (headers or {}).items()}


def _pending_key(context: RequestContext) -> str:
    """Where a task's questions are recorded: the task, since a context may hold several."""
    return _task_key(context.context_id or "", context.task_id or "")


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
            # every key, the ones that name no open question included: the
            # resume check refuses those, as the protocol asks
            for key, payload in data.items():
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


#: Cloud metadata services, refused by address as well as by class: the IPv6
#: one is a unique-local address, which an allowlist could otherwise admit.
_METADATA_ADDRESSES = frozenset(
    {ipaddress.ip_address("169.254.169.254"), ipaddress.ip_address("fd00:ec2::254")}
)


def _on_platform() -> bool:
    """Whether this is a deployment rather than a local run: the platform sets ``DEPLOYMENT_ID``."""
    return bool(os.environ.get("DEPLOYMENT_ID"))


def _unmapped(address: Any) -> Any:
    mapped = getattr(address, "ipv4_mapped", None)
    return mapped if mapped is not None else address


def _never_reached(address: Any) -> bool:
    """An address no fetch may reach, listed or not: loopback, link-local (the cloud metadata services among them), multicast, reserved, unspecified."""
    address = _unmapped(address)
    return (
        address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
        or address in _METADATA_ADDRESSES
    )


def _address_blocked(address: Any) -> bool:
    """Whether an address is not a public unicast destination."""
    address = _unmapped(address)
    return not address.is_global or _never_reached(address)


def _resolve(host: str, port: int) -> list[str]:
    """The addresses a hostname resolves to now, as the connection will see them."""
    return [
        str(info[4][0])
        for info in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    ]


def _pin(url: str, what: str = "The URL part") -> _Pinned:
    """Resolve a URL's host once, check every address, and pin the fetch to one.

    Checking a name and then letting the client resolve it again lets an
    answer that changes between the two reach an address the check refused.
    The request therefore goes to the address checked, with the hostname kept
    for certificate verification and the ``Host`` header.
    The policy is the push sender's: HTTPS only, HTTP for a local run; public
    addresses allowed; loopback, link-local, multicast, reserved and cloud
    metadata addresses refused whatever the platform listed, except loopback
    in a local run; a private address only when the platform listed the host
    or its network (the cluster's own services, say).

    Raises:
        AgentError: The URL is not HTTPS (or HTTP, locally), its host does not
            resolve, or resolves to an address the agent must not reach.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    platform = _on_platform()
    schemes = ("https",) if platform else ("http", "https")
    if parsed.scheme not in schemes or not host:
        raise AgentError(
            f"{what} is not an https URL."
            if platform
            else f"{what} is not an http(s) URL.",
            "unsupported_content",
            422,
        )
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    allowed_hosts, allowed_networks = _internal_allowed()
    try:
        addresses = _resolve(host, port)
    except OSError as err:
        raise AgentError(
            f"{what}'s host could not be resolved: {host}",
            "unsupported_content",
            422,
        ) from err
    chosen: str | None = None
    for raw in addresses:
        address = ipaddress.ip_address(raw.split("%", 1)[0])
        local_loopback = not platform and _unmapped(address).is_loopback
        if _never_reached(address) and not local_loopback:
            raise AgentError(
                f"{what}'s host resolves to an address the agent never reaches: {host}",
                "unsupported_content",
                422,
            )
        internal = host in allowed_hosts or any(
            address in network for network in allowed_networks
        )
        if _address_blocked(address) and not internal and not local_loopback:
            raise AgentError(
                f"{what}'s host resolves to a private address the platform "
                f"did not list: {host}",
                "unsupported_content",
                422,
            )
        chosen = chosen or raw
    if chosen is None:
        raise AgentError(
            f"{what}'s host resolves to nothing: {host}",
            "unsupported_content",
            422,
        )
    literal = f"[{chosen}]" if ":" in chosen else chosen
    pinned = parsed._replace(netloc=f"{literal}:{port}")
    default_port = port == (443 if parsed.scheme == "https" else 80)
    host_header = host if default_port else f"{host}:{port}"
    return _Pinned(url=urlunparse(pinned), host=host, host_header=host_header)


def _fetch(
    url: str,
    stop: threading.Event | None = None,
    deadline: float = FETCH_DEADLINE_S,
) -> tuple[str | None, bytes]:
    """Fetch an allowed URL part: pinned to the checked address, no redirects, bounded in size and time, in a thread.

    ``deadline`` bounds the whole fetch, not one read: a host that drips
    bytes would otherwise hold the turn through the whole size cap.
    ``stop`` is set by the turn when it is cancelled, which a thread cannot be.
    """
    import httpx  # noqa: PLC0415

    started = time.monotonic()

    def check() -> None:
        if stop is not None and stop.is_set():
            raise AgentError(
                "The URL part's fetch was cancelled.", "unsupported_content", 422
            )
        if time.monotonic() - started > deadline:
            raise AgentError(
                "The URL part took too long to fetch.", "unsupported_content", 422
            )

    target = _pin(url)
    with (
        httpx.Client(
            follow_redirects=False, timeout=FETCH_OPERATION_TIMEOUT_S
        ) as client,
        client.stream(
            "GET",
            target.url,
            headers={"Host": target.host_header},
            extensions={"sni_hostname": target.host},
        ) as response,
    ):
        check()
        if response.status_code != 200:
            raise AgentError(
                f"The URL part answered {response.status_code}.",
                "unsupported_content",
                422,
            )
        chunks: list[bytes] = []
        size = 0
        for chunk in response.iter_bytes():
            check()
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

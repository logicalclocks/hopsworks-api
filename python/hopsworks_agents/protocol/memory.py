"""Conversation memory keyed by the protocol's ``conversation_id``.

The protocol makes history server-side: clients send only the new message
plus a ``conversation_id``. This module provides the storage for that —
pass a store to :class:`AgentApp` and turns are recorded automatically;
handlers read history with ``app.memory.get(request.conversation_id)``,
already in the ``{"role", "content"}`` shape LangChain/LangGraph/LlamaIndex
accept.

Backends:
- :class:`InMemoryAgentMemory` — zero-config, for development. Conversations
  are lost on restart and not shared between replicas; note that Hopsworks
  agent deployments can scale to zero, so this is NOT for production.
- :class:`ManagedMemoryService` — any SQLAlchemy URL (e.g. the project's MySQL).
  Survives restarts and works across replicas.

If your framework already persists conversation state (a LangGraph
checkpointer, a LlamaIndex chat store), key it by ``conversation_id`` and do
NOT pass a memory store — one source of truth for history is enough.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING


log = logging.getLogger(__name__)

Turn = dict[str, str]  # {"role": "user"|"assistant", "content": str}

#: Rewrites the running summary to absorb the turns being folded away.
#: ``(previous_summary_or_None, turns_to_fold) -> summary``. Sync or async —
#: taking the previous summary is what makes the fold incremental, so a long
#: conversation never re-summarizes itself from the beginning.
Summarizer = Callable[[str | None, list[Turn]], str | Awaitable[str]]

#: Turns text into a vector. Query and stored content must come from the *same*
#: model — vectors from different models are not comparable even at equal
#: dimension, so a mismatch returns plausible nonsense rather than an error.
Embedder = Callable[[str], list[float]]

if TYPE_CHECKING:  # pragma: no cover
    from .vectorstore import VectorStore

# A turn is open from the moment the user message is recorded until the handler
# finishes. Rows of an open turn are invisible to reads: the reply does not
# exist yet, and a half-written turn must never reach the model (or, later, a
# summarizer). Turns that never close — a pod killed mid-request, an ASGI server
# that skipped generator cleanup — are swept to ``abandoned`` by age.
TURN_OPEN = "open"
TURN_CLOSED = "closed"
TURN_ABANDONED = "abandoned"

# Only ``message`` rows are conversation history. tool_call/event rows are
# debug telemetry: same table, excluded from get(), pruned on a TTL later.
ITEM_MESSAGE = "message"
ITEM_TOOL_CALL = "tool_call"
ITEM_EVENT = "event"

# Durable state is scoped. ``user`` is the end user's own memory, shared across
# their conversations; ``session`` is working state for one conversation; ``app``
# is agent-wide and read by everyone, which is why the model may not write it —
# see WRITABLE_SCOPES.
SCOPE_USER = "user"
SCOPE_APP = "app"
SCOPE_SESSION = "session"
SCOPES = (SCOPE_USER, SCOPE_APP, SCOPE_SESSION)

#: Scopes an agent's own LLM may write. ``app`` is excluded on purpose: it is
#: injected into every user's turns, so a model-writable ``app`` scope would let
#: one user's prompt injection persist into everyone else's context. Operators
#: write it out of band.
WRITABLE_SCOPES = (SCOPE_USER, SCOPE_SESSION)

WRITTEN_BY_AGENT = "agent"
WRITTEN_BY_OPERATOR = "operator"

# Bumped whenever the table shape changes. ``create_all`` is create-if-not-
# exists, so it silently does nothing to a table an older version already made;
# the recorded version is what turns "SDK upgraded under a live deployment"
# into a defined outcome instead of a column-missing error at query time.
SCHEMA_VERSION = 1

_TABLE_SUFFIX_RE = re.compile(r"^[A-Za-z0-9_]{1,48}$")

#: Feature group version these tables belong to. Hopsworks names an online
#: feature group's table ``<name>_<version>``, and the agent writes into that
#: table directly — so the rows are in the feature store as they are written,
#: with no copy and no export step.
FEATURE_GROUP_VERSION = 1

#: Table names, each saying what it holds rather than what layer wrote it.
#:
#: ``messages``      every user message, agent reply and tool event
#: ``conversations`` one row per conversation: its summary and fold bookkeeping
#: ``facts``         durable scoped state — what the agent knows about someone
#: ``schema``        the schema version, so an upgraded SDK knows what it found
#: Feature group names. The table each one lives in carries the version.
MESSAGES_FG = "agent_memory_messages"
CONVERSATIONS_FG = "agent_memory_conversations"
FACTS_FG = "agent_memory_facts"
SCHEMA_FG = "agent_memory_schema"

ITEMS_TABLE = f"{MESSAGES_FG}_{FEATURE_GROUP_VERSION}"
CONVERSATIONS_TABLE = CONVERSATIONS_FG
FACTS_TABLE = FACTS_FG
SCHEMA_TABLE = SCHEMA_FG


def _feature_group_name(table_name: str) -> str:
    """The feature group whose online table this is.

    Hopsworks names an online feature group's table ``<name>_<version>``, and
    the agent writes into that table, so the mapping back is dropping the
    version suffix. A table that does not carry one is not a feature group's.
    """
    stem, _, version = table_name.rpartition("_")
    if stem and version.isdigit():
        return stem
    return table_name


def _table_suffix(table_name: str) -> str:
    """What to append to the companion table names, or "" for the default.

    ``agent_memory_messages_1`` -> ``"1"``   (agent_memory_facts_1)
    ``agent_memory_messages_42`` -> ``"42"`` (agent_memory_facts_42)
    ``something_else`` -> ``"something_else"``
    """
    prefix = f"{MESSAGES_FG}_"
    if table_name.startswith(prefix):
        return table_name[len(prefix) :]
    return table_name


def _companion(base: str, suffix: str) -> str:
    return f"{base}_{suffix}" if suffix else base


#: Fragments MySQL, SQLite and Postgres use for "this object is already there".
#: Matched on the message because SQLAlchemy does not normalise the DBAPI code
#: for these across drivers.
_EXISTS_MARKERS = ("already exists", "duplicate key", "duplicate entry")


def _already_exists(err: Exception) -> bool:
    """Did this DDL fail because someone else had already run it?

    Two agents in a project start together, both find no tables, both issue
    CREATE — one wins. The loser must treat that as success, or it marks itself
    permanently unusable over a table that is sitting there working.
    """
    text = str(err).lower()
    return any(marker in text for marker in _EXISTS_MARKERS)


def new_turn_id() -> str:
    return f"turn_{uuid.uuid4().hex}"


def _utcnow() -> datetime:
    # naive UTC: MySQL DATETIME carries no zone, and mixing aware/naive values
    # across drivers is a reliable source of comparison bugs
    return datetime.now(timezone.utc).replace(tzinfo=None)


def deployment_mysql_url() -> str:
    """Build the SQLAlchemy URL of the project MySQL from the env vars the.

    platform injects into Hopsworks agent deployments (MYSQL_USER, MYSQL_HOST,
    MYSQL_PORT, MYSQL_DB, and the password via MYSQL_PASSWORD or the
    MYSQL_PASSWORD_SECRET_NAME Hopsworks secret).
    """
    try:
        user = os.environ["MYSQL_USER"]
        host = os.environ["MYSQL_HOST"]
        db = os.environ["MYSQL_DB"]
    except KeyError as err:
        raise RuntimeError(
            f"MySQL env var {err.args[0]} is not set — not running in a "
            "Hopsworks agent deployment? Pass an explicit url= to "
            "ManagedMemoryService instead."
        ) from err
    port = os.environ.get("MYSQL_PORT", "3306")

    password = os.environ.get("MYSQL_PASSWORD")
    if password is None:
        secret_name = os.environ.get("MYSQL_PASSWORD_SECRET_NAME")
        if secret_name is None:
            raise RuntimeError(
                "Neither MYSQL_PASSWORD nor MYSQL_PASSWORD_SECRET_NAME is set."
            )
        password = _read_hopsworks_secret(secret_name)

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"


def _read_hopsworks_secret(secret_name: str) -> str:
    try:
        import hopsworks
    except ImportError as err:
        raise RuntimeError(
            "Reading the MySQL password secret requires the hopsworks "
            "package (available in Hopsworks deployment environments)."
        ) from err
    try:
        return hopsworks.get_secrets_api().get(secret_name)
    except Exception:  # noqa: BLE001 — not logged in yet
        hopsworks.login()
        return hopsworks.get_secrets_api().get(secret_name)


def _url_from_connector(connector) -> str:
    """A SQLAlchemy URL for the project database, from its JDBC connector.

    The online feature store *is* the database the memory tables live in, so
    its connector already carries the credentials — which matters because the
    MYSQL_* environment variables this module normally reads are injected into
    agent deployments and nowhere else. Without this, registering feature
    groups would only be possible from inside the very pod that has no reason
    to do it.
    """
    raw = getattr(connector, "connection_string", None) or ""
    match = re.match(r"jdbc:mysql://([^:/]+):(\d+)/([^?;]+)", raw)
    if not match:
        raise RuntimeError(
            f"Could not read a MySQL host, port and database out of the online "
            f"storage connector ({raw!r}). Pass url= explicitly."
        )
    host, port, database = match.groups()

    arguments = getattr(connector, "arguments", None) or {}
    if isinstance(arguments, list):
        # some versions hand back [{"name": ..., "value": ...}, ...]
        arguments = {
            entry.get("name"): entry.get("value")
            for entry in arguments
            if isinstance(entry, dict)
        }
    user = arguments.get("user") or arguments.get("username")
    password = arguments.get("password")
    if not user or password is None:
        raise RuntimeError(
            "The online storage connector carries no user/password, so the "
            "project database URL cannot be built. Pass url= explicitly."
        )
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


#: Where an export writes. ``None`` means "let hsfs do both", which is its own
#: default; "online" is ours, because the offline half is a Delta write that
#: costs far more than the row it carries and is rarely wanted per batch.
EXPORT_STORAGE_ONLINE = "online"
EXPORT_STORAGE_OFFLINE = "offline"
EXPORT_STORAGE_BOTH = "both"


def _storage_arg(storage: str | None) -> str | None:
    """The ``storage=`` value to hand ``fg.insert``, or None to leave it out.

    hsfs writes both stores when the argument is absent, so "both" is expressed
    by omitting it rather than by passing a value.
    """
    if storage in (EXPORT_STORAGE_BOTH, None):
        return None
    if storage in (EXPORT_STORAGE_ONLINE, EXPORT_STORAGE_OFFLINE):
        return storage
    raise ValueError(f"Unknown storage {storage!r}. Use 'online', 'offline' or 'both'.")


def _url_from_connector(connector) -> str:
    """A SQLAlchemy URL for the project database, from its JDBC connector.

    The online feature store *is* the database the memory tables live in, so
    its connector already carries the credentials — which matters because the
    MYSQL_* environment variables this module normally reads are injected into
    agent deployments and nowhere else. Without this, an export job could only
    run inside the very pod that has no reason to run it.
    """
    raw = getattr(connector, "connection_string", None) or ""
    match = re.match(r"jdbc:mysql://([^:/]+):(\d+)/([^?;]+)", raw)
    if not match:
        raise RuntimeError(
            f"Could not read a MySQL host, port and database out of the online "
            f"storage connector ({raw!r}). Pass url= explicitly."
        )
    host, port, database = match.groups()
    arguments = getattr(connector, "arguments", None) or {}
    if isinstance(arguments, list):
        arguments = {
            entry.get("name"): entry.get("value")
            for entry in arguments
            if isinstance(entry, dict)
        }
    user = arguments.get("user") or arguments.get("username")
    password = arguments.get("password")
    if not user or password is None:
        raise RuntimeError(
            "The online storage connector carries no user/password, so the "
            "project database URL cannot be built. Pass url= explicitly."
        )
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def export_feature_groups(
    feature_store=None,
    *,
    url: str | None = None,
    storage: str | None = EXPORT_STORAGE_ONLINE,
    since: int | None = None,
    version: int = 1,
    include=None,
    table_name: str | None = None,
):
    """Copy the project's memory tables into feature groups.

    The entry point for a job or notebook: it needs nothing but a Hopsworks
    login, deriving the database URL from the project's own online storage
    connector when one is not passed. See
    :meth:`ManagedMemoryService.export_feature_groups`.

        import hopsworks
        from hopsworks_agents.protocol.memory import export_feature_groups

        export_feature_groups()                      # online only
        export_feature_groups(storage="offline")     # backfill Delta
    """
    if feature_store is None:
        import hopsworks

        feature_store = hopsworks.login().get_feature_store()
    if url is None:
        try:
            url = deployment_mysql_url()
        except RuntimeError:
            url = _url_from_connector(feature_store.get_online_storage_connector())
    memory = ManagedMemoryService(
        url=url,
        deployment_id="export",
        long_term=True,
        summarize=lambda previous, turns: "",
        table_name=table_name,
        # every tier declared so its definition exists to read; which tables
        # are real is a question for the database, and reading is not a reason
        # to create one
        create_tables=False,
    )
    return memory.export_feature_groups(
        feature_store,
        storage=storage,
        since=since,
        version=version,
        include=include,
    )


class ChatMemory(ABC):
    """Conversation store keyed by conversation_id.

    Writes go through the **turn lifecycle**: ``begin_turn`` records the user
    message before the handler runs, ``record_item`` adds the reply and any tool
    events, and ``end_turn`` either closes the turn or marks it abandoned.

    Recording the question up front is what lets a handler read back the message
    it is answering, and (from Phase 2) lets anything the agent remembers point
    at the turn that caused it. The cost is that a turn can now fail *after* its
    question is stored — so an open turn is invisible to :meth:`get` until it
    closes, and every exit path must end the turn. A store that skips that ends
    up serving questions whose answers never arrived.
    """

    @abstractmethod
    def get(self, conversation_id: str) -> list[Turn]:
        """Messages of closed turns, in chronological order."""

    @abstractmethod
    def begin_turn(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> None:
        """Open a turn and record its first message. Invisible to :meth:`get`.

        until :meth:`end_turn` closes it.
        """

    @abstractmethod
    def record_item(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        memory_type: str = ITEM_MESSAGE,
        seq: int | None = None,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> None:
        """Add a row to an open turn (the reply, a tool call, an event)."""

    @abstractmethod
    def end_turn(
        self, conversation_id: str, turn_id: str, *, status: str = TURN_CLOSED
    ) -> None:
        """Close a turn (``closed``) or mark it failed (``abandoned``)."""

    @abstractmethod
    def clear(self, conversation_id: str) -> None:
        """Drop a conversation."""

    def list_conversations(
        self, *, subject: str | None = None, limit: int = 50
    ) -> list[dict]:
        """Conversations this store holds, most recently active first.

        The transcript lives on the server, so a client that loses its own
        record of which conversations exist — cleared storage, a different
        browser, a colleague looking at the same deployment — can still find
        them. Returns ``[]`` for a store that cannot enumerate.
        """
        return []

    def conversation_subject(self, conversation_id: str) -> str | None:
        """Who this conversation's durable memory is keyed by, per the server.

        The client cannot answer this for itself. It knows which subject it
        *asserted* on the request, but an agent that identifies the caller
        mid-turn — see :meth:`rebind_turn_subject` — replaces that with
        something the client was never told, and a client that has since
        reloaded has forgotten even the assertion. Asking the store is the only
        way to find out what memory is actually filed under.

        Returns ``None`` for a store with no per-row subject, and for an
        unknown conversation.
        """
        return None

    def rebind_turn_subject(
        self, conversation_id: str, turn_id: str, subject: str
    ) -> None:
        """Restamp this turn's rows with a subject learned while it ran.

        ``begin_turn`` writes the user message before the handler executes, so
        an agent that discovers who it is talking to *during* the turn — the
        chatbot asking for your details, which is how identification works when
        the transport only authenticates the chatbot itself — would otherwise
        leave the question under one subject and the answer under another. That
        splits the turn for anything that filters by subject, notably
        :meth:`search`.

        Not abstract: a store with no per-row subject has nothing to restamp,
        and silently doing nothing is the correct behaviour for it.
        """
        return

    def append(self, conversation_id: str, role: str, content: str) -> None:
        """Record one standalone message, e.g. to seed a conversation.

        A convenience over the lifecycle, not a second write path: it opens a
        turn, writes the message, and closes it immediately.
        """
        turn_id = new_turn_id()
        self.begin_turn(conversation_id, turn_id, role, content)
        self.end_turn(conversation_id, turn_id)

    def healthcheck(self) -> bool:
        """Whether the store is currently usable.

        Used by the readiness probe;

        default assumes always ready.
        """
        return True

    def maybe_reap(self) -> None:  # noqa: B027 -- a no-op default, not a hook every store must fill
        """Sweep turns that were never closed. No-op for stores whose open.

        turns cannot outlive the process.
        """

    def transcript(
        self, conversation_id: str, *, include_events: bool = False
    ) -> list[dict]:
        """The human-facing record of a conversation.

        Deliberately not the same thing as :meth:`get`. ``get`` returns what the
        model reads — bounded, and shrinking as turns are folded into the
        summary. This returns the whole conversation, including turns that have
        been folded away, because it is what a person reads in the panel and
        what an operator reads during an incident.
        """
        return [
            dict(turn, memory_type=ITEM_MESSAGE) for turn in self.get(conversation_id)
        ]

    def summarized_through(self, conversation_id: str) -> int:
        """Highest item id represented by the summary rather than by.

        :meth:`get`. ``0`` when nothing has been folded — lets a UI draw the
        line between "the agent still sees this" and "this is only in the
        summary now".
        """
        return 0

    # ── summarization (tier 2) ───────────────────────────────────────────

    def get_summary(self, conversation_id: str) -> str | None:
        """The rolling summary of folded-away turns, or None."""
        return None

    async def maybe_summarize(self, conversation_id: str) -> bool:
        """Fold old turns into the summary if enough have accumulated.

        Called after the response has been sent, so its cost lands on request
        duration every Nth turn rather than on time-to-answer. Returns whether
        a fold happened.
        """
        return False

    # ── scoped durable state (tier 3) ────────────────────────────────────

    def set_state(  # noqa: B027 -- a no-op default, not a hook every store must fill
        self,
        scope: str,
        owner: str,
        key: str,
        value: str,
        *,
        source_ref: str | None = None,
        written_by: str = WRITTEN_BY_AGENT,
    ) -> None:
        """Upsert one scoped durable value."""

    def get_state(self, scope: str, owner: str, key: str) -> str | None:
        return None

    def list_state(
        self,
        scope: str,
        owner: str,
        *,
        key: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        return []

    def delete_state(self, scope: str, owner: str, key: str | None = None) -> int:
        return 0

    def state_block(self, subject: str, conversation_id: str) -> str:
        """Scoped state rendered for ``ctx.system_context()``."""
        return ""

    # ── semantic search (tier 3) ─────────────────────────────────────────

    def search(
        self,
        query: str,
        *,
        subject: str | None = None,
        conversation_id: str | None = None,
        k: int = 5,
    ) -> list[dict]:
        """Older messages relevant to ``query``, most relevant first."""
        return []

    async def ingest_turn(self, conversation_id: str, turn_id: str) -> int:
        """Embed a completed turn into the vector store. Returns rows sent."""
        return 0

    def purge_vectors(
        self, *, conversation_id: str | None = None, subject: str | None = None
    ) -> int:
        return 0


class InMemoryAgentMemory(ChatMemory):
    """Process-local store for development: lost on restart (agent.

    deployments can scale to zero) and not shared between replicas.
    """

    def __init__(self, max_messages: int = 50):
        self._max = max_messages
        self._conversations: dict[str, list[Turn]] = {}
        # open turns buffer their messages here; they land in the conversation
        # on close and are dropped on abandon, so history never shows a
        # question whose answer never arrived
        self._open: dict[str, list[Turn]] = {}
        self._lock = threading.Lock()

    def get(self, conversation_id: str) -> list[Turn]:
        with self._lock:
            return list(self._conversations.get(conversation_id, []))

    def begin_turn(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> None:
        with self._lock:
            self._open[turn_id] = [{"role": role, "content": content}]

    def record_item(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        memory_type: str = ITEM_MESSAGE,
        seq: int | None = None,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> None:
        if memory_type != ITEM_MESSAGE:
            # the dev store keeps history only; tool/event rows are debug
            # telemetry and are dropped rather than half-modelled
            return
        with self._lock:
            buffered = self._open.get(turn_id)
            if buffered is not None:
                buffered.append({"role": role, "content": content})

    def end_turn(
        self, conversation_id: str, turn_id: str, *, status: str = TURN_CLOSED
    ) -> None:
        with self._lock:
            buffered = self._open.pop(turn_id, None)
            if not buffered or status != TURN_CLOSED:
                return
            turns = self._conversations.setdefault(conversation_id, [])
            turns.extend(buffered)
            if len(turns) > self._max:
                del turns[: len(turns) - self._max]

    def clear(self, conversation_id: str) -> None:
        with self._lock:
            self._conversations.pop(conversation_id, None)


class ManagedMemoryService(ChatMemory):
    """SQLAlchemy-backed store (MySQL, Postgres, SQLite, ...).

    ``pip install 'hopsworks-agent-protocol[memory-sql]'``

    Inside a Hopsworks agent deployment both arguments are optional:
    ``ManagedMemoryService()`` connects to the project MySQL using the
    platform-injected env vars and derives a per-deployment table name from
    ``DEPLOYMENT_ID``.

    A per-conversation cache avoids a read round-trip on every turn for the
    lifetime of the process.

    Rows live in ``agent_memory_messages`` and carry turn identity
    (``turn_id``, ``seq``, ``status``, ``message_id``) so the SDK can record the
    user message before the handler runs without ever exposing a half-written
    turn: only ``closed`` turns are returned by :meth:`get`. See
    :meth:`begin_turn`.

    Connection is **lazy and non-fatal**: construction never touches the
    database (missing SQLAlchemy is the only hard error), the engine + tables
    are created on first use — in a deployment that happens via the ``/ready``
    probe, off the request path — and if the database is unreachable the store
    degrades to statelessness — reads return empty, writes are dropped, both
    with a warning — so a down database never crashes the agent at startup or
    on a turn.
    """

    def __init__(
        self,
        url: str | None = None,
        table_name: str | None = None,
        max_messages: int = 50,
        turn_timeout_seconds: int = 300,
        summarize: Summarizer | None = None,
        summarize_after_messages: int = 20,
        keep_recent_messages: int = 10,
        tool_event_retention_days: int | None = 30,
        message_retention_days: int | None = None,
        long_term: bool = False,
        embedder: Embedder | None = None,
        vector_store: VectorStore | None = None,
        state_ttl_days: int | None = 90,
        max_state_value_chars: int = 4096,
        max_state_keys_written: int = 128,
        max_state_keys_injected: int = 32,
        state_inject_value_chars: int = 1024,
        recency_half_life_days: float | None = 30.0,
        search_oversample: int = 3,
        deployment_id: str | None = None,
        create_tables: bool = True,
    ):
        try:
            import sqlalchemy  # noqa: F401 — fail fast if the extra is missing
        except ImportError as err:
            raise ImportError(
                "ManagedMemoryService requires SQLAlchemy: "
                "pip install 'hopsworks-agent-protocol[memory-sql]'"
            ) from err

        self._max = max_messages
        self._turn_timeout = turn_timeout_seconds
        self._summarize = summarize
        self._summarize_every = summarize_after_messages
        self._keep_recent = keep_recent_messages
        self._tool_event_retention = tool_event_retention_days
        self._message_retention = message_retention_days
        self._long_term = long_term
        self._embedder = embedder
        self._vector_store = vector_store
        self._state_ttl = state_ttl_days
        self._max_value_chars = max_state_value_chars
        self._max_keys_written = max_state_keys_written
        self._max_keys_injected = max_state_keys_injected
        self._inject_value_chars = state_inject_value_chars
        # How fast similarity gives way to age in search ranking. A support
        # agent wants recent context; an agent whose one relevant fact is a year
        # old does not, so this is a knob and `None` turns it off entirely,
        # restoring pure similarity order.
        self._half_life = recency_half_life_days
        self._search_oversample = max(1, search_oversample)
        self._url = url  # resolved lazily (may read env/secrets)
        self._table_name = table_name
        self._deployment_id = deployment_id
        # False for a handle that only wants to look at the schema — feature
        # group registration, say. Inspecting a database should never be the
        # thing that creates a table in it, and a tier this agent does not run
        # would otherwise be conjured into existence by the act of listing it.
        self._create_tables = create_tables
        # resolved on first connect, then constant for the process
        self._deployment: str | None = None
        # conversation_id -> (summary_version, turns). The version is what makes
        # the cache safe once folding exists: another replica advancing the
        # cutoff bumps it, and this process notices on the next read instead of
        # serving history that has since been compacted away.
        self._cache: dict[str, tuple[int, list[Turn]]] = {}
        # rows written by turns that are still open, held back from the cache
        # until the turn closes (or dropped when it is abandoned)
        self._pending: dict[str, list[Turn]] = {}
        self._seq: dict[str, int] = {}
        self._lock = threading.Lock()
        # set on first successful connection; None while unconnected/failed
        self._engine = None
        self._table = None
        self._meta = None
        self._sessions = None
        self._state = None
        # a failed connect blocks retries until this monotonic deadline, with
        # exponential backoff — never permanently (see _ensure_engine)
        self._init_failed_until = 0.0
        self._init_backoff = 1.0
        self._last_reap = 0.0
        self._last_prune = 0.0

    def _mine(self, stmt, table):
        """Restrict a statement to this deployment's rows.

        Every read, update and delete in this module goes through here. It is a
        one-line helper on purpose: with one shared table, a query that forgets
        the deployment filter does not fail — it quietly returns, edits or
        deletes another agent's memory. Making the filter a named call means a
        missing one is visible when reading the code, and
        ``TestDeploymentIsolation`` fails when it is not.
        """
        return stmt.where(table.c.deployment_id == self._deployment)

    def _adopt_feature_group_tables(self, engine, tables) -> bool:
        """Make the feature store's tables usable as the agent's live store.

        Hopsworks creates an online feature group's table for us, which is why
        the agent can write straight into it — but it creates it for a feature
        store's access pattern, not ours. Two things are missing and both are
        load-bearing:

        * ``id`` has no ``AUTO_INCREMENT``. It is not decoration: it orders
          history and is the fold cursor. Without it every insert fails, so a
          store that cannot add it reports itself unusable rather than failing
          on the first turn.
        * There are no secondary indexes, and the primary key is ``USING HASH``,
          which cannot serve ``id > ?`` or ``ORDER BY id`` at all. Reading one
          conversation's history would scan every agent's rows in the project.

        Both are added here, idempotently, at readiness. Note the consequence
        of living in someone else's table: a feature group that is deleted and
        recreated comes back without them, and the symptom is slowness rather
        than an error, so this runs on every connect rather than once.

        Returns False when the table is unusable, which degrades the store to
        stateless instead of breaking turns.
        """
        from sqlalchemy import text

        if engine.dialect.name != "mysql":
            return True  # SQLite (tests, local dev) creates our own schema

        ok = True
        for table in tables:
            if table is None:
                continue
            try:
                with engine.connect() as conn:
                    existing = {
                        row[2]
                        for row in conn.execute(text(f"SHOW INDEX FROM `{table.name}`"))
                    }
                    auto = conn.execute(
                        text(
                            "SELECT extra FROM information_schema.columns "
                            "WHERE table_schema = DATABASE() AND table_name = :t "
                            "AND column_name = 'id'"
                        ),
                        {"t": table.name},
                    ).fetchone()
                with engine.begin() as conn:
                    for index in table.indexes:
                        if index.name in existing:
                            continue
                        columns = ", ".join(f"`{c.name}`" for c in index.columns)
                        unique = "UNIQUE " if index.unique else ""
                        conn.execute(
                            text(
                                f"ALTER TABLE `{table.name}` ADD {unique}INDEX "
                                f"`{index.name}` ({columns})"
                            )
                        )
                        log.info("Added index %s on %s", index.name, table.name)
                    if auto is not None and "auto_increment" not in (auto[0] or ""):
                        # MySQL requires the auto column to lead some index, and
                        # the feature group's primary key starts with
                        # deployment_id — so give `id` an index of its own first
                        seed = f"idx_{table.name}_id"
                        if seed not in existing:
                            conn.execute(
                                text(
                                    f"ALTER TABLE `{table.name}` ADD INDEX `{seed}` (`id`)"
                                )
                            )
                        conn.execute(
                            text(
                                f"ALTER TABLE `{table.name}` "
                                "MODIFY `id` BIGINT NOT NULL AUTO_INCREMENT"
                            )
                        )
                        log.info("Enabled AUTO_INCREMENT on %s.id", table.name)
            except Exception:  # noqa: BLE001
                log.exception(
                    "Could not prepare %s for writing; memory is unavailable "
                    "until it has an AUTO_INCREMENT id and its indexes",
                    table.name,
                )
                ok = False
        return ok

    def _resolve_deployment_id(self) -> str:
        """Which agent's rows these are.

        Every deployment in a project shares one set of tables, so this value is
        what keeps them apart — it is not a label, it is the first column of
        every key and every WHERE clause in this module.

        A deployment must therefore have a real one. The old ``'default'``
        fallback was survivable when it only meant "share a table name"; now it
        would mean two agents reading each other's conversations, so it is
        allowed only for a caller who passed their own ``url`` (tests, local
        development) and cannot be reached inside a deployment.
        """
        if self._deployment_id is not None:
            value = self._deployment_id
        else:
            value = os.environ.get("DEPLOYMENT_ID")
            if value is None:
                if self._url is None:
                    raise RuntimeError(
                        "DEPLOYMENT_ID is not set, so memory rows cannot be "
                        "attributed to this agent. Inside a Hopsworks agent "
                        "deployment this is injected for you; elsewhere pass an "
                        "explicit deployment_id= (or url=) to "
                        "ManagedMemoryService."
                    )
                log.warning(
                    "DEPLOYMENT_ID is not set; falling back to deployment_id "
                    "'default'. Pass deployment_id= to keep agents apart."
                )
                value = "default"
        if not _TABLE_SUFFIX_RE.match(str(value)):
            raise RuntimeError(
                f"Invalid deployment_id {value!r}: must match [A-Za-z0-9_]{{1,48}}."
            )
        return str(value)

    def _resolve_table_name(self) -> str:
        """The shared items table, validated.

        Shared across the project rather than one table per deployment. Four
        tables per agent meant a project with a hundred agents carried four
        hundred tables, and every cold start ran DDL to make its own — which is
        also where the replicas raced each other. One set of tables is created
        once and found thereafter.

        An explicit ``table_name`` still wins, for tests and for anyone who
        wants a deployment kept physically apart.
        """
        name = self._table_name or ITEMS_TABLE
        # one definition of how a name splits, shared with the companions
        suffix = _table_suffix(name)
        if suffix and not _TABLE_SUFFIX_RE.match(suffix):
            raise RuntimeError(
                f"Invalid memory table name {name!r}: the suffix must "
                "match [A-Za-z0-9_]{1,48}."
            )
        return name

    def _ensure_engine(self) -> bool:
        """Create the engine + tables. Returns True when the store is usable,.

        False when the database is unreachable (already logged).

        Called by ``healthcheck()``, which the ``/ready`` probe drives — so in a
        deployment the DDL runs *before* the pod reports ready and the request
        path only ever finds existing tables. The lazy path stays for direct use
        (tests, local SQLite) where there is no readiness probe.
        """
        if self._engine is not None:
            return True
        if time.monotonic() < self._init_failed_until:
            return False
        with self._lock:
            if self._engine is not None:
                return True
            if time.monotonic() < self._init_failed_until:
                return False
            try:
                from sqlalchemy import (
                    Column,
                    DateTime,
                    Index,
                    Integer,
                    MetaData,
                    SmallInteger,
                    String,
                    Table,
                    Text,
                    UniqueConstraint,
                    create_engine,
                )

                url = self._url or deployment_mysql_url()
                deployment = self._resolve_deployment_id()
                table_name = self._resolve_table_name()
                # "" for the shared default, so the companion tables keep
                # their plain names. Only an explicit table_name= produces a
                # suffix, and then every table carries it.
                suffix = _table_suffix(table_name)
                # index and constraint names still have to be unique and
                # non-empty, so they get a stem even when the tables do not
                stem = suffix or "shared"
                engine = create_engine(url, pool_pre_ping=True)
                metadata = MetaData()
                table = Table(
                    table_name,
                    metadata,
                    Column("id", Integer, primary_key=True, autoincrement=True),
                    # Which deployment's memory this row is. One table now holds
                    # every agent in the project, so this is not descriptive: it
                    # is the first column of every index and every WHERE clause,
                    # and omitting it anywhere is a cross-deployment data leak.
                    Column("deployment_id", String(64), nullable=False),
                    Column("conversation_id", String(255), nullable=False),
                    Column("turn_id", String(64), nullable=False),
                    # the protocol message id, so anything written during the
                    # turn can reference the message that caused it
                    Column("message_id", String(64), nullable=True),
                    Column("seq", SmallInteger, nullable=False, default=0),
                    Column("status", String(16), nullable=False),
                    Column("subject", String(255), nullable=True),
                    Column("memory_type", String(16), nullable=False),
                    Column("role", String(16), nullable=False),
                    Column("content", Text, nullable=False),
                    Column("created_at", DateTime, nullable=False),
                    Column("expires_at", DateTime, nullable=True),
                    # deployment_id leads all of these. A conversation id is
                    # unique in practice, but an index that does not start with
                    # the deployment turns every lookup into a scan of every
                    # other agent's rows in the same table.
                    Index(f"idx_{stem}_conv", "deployment_id", "conversation_id", "id"),
                    Index(
                        f"idx_{stem}_turn",
                        "deployment_id",
                        "conversation_id",
                        "turn_id",
                    ),
                    # drives the "oldest open turn" lookup on the fold path
                    Index(
                        f"idx_{stem}_open",
                        "deployment_id",
                        "conversation_id",
                        "status",
                        "id",
                    ),
                    Index(f"idx_{stem}_msg", "deployment_id", "message_id"),
                )
                meta = Table(
                    _companion(SCHEMA_TABLE, suffix),
                    metadata,
                    Column("table_name", String(64), primary_key=True),
                    Column("schema_version", Integer, nullable=False),
                    Column("updated_at", DateTime, nullable=False),
                )
                sessions = None
                if self._summarize is not None:
                    # tier 2 only: no summarizer, no bookkeeping to keep
                    sessions = Table(
                        _companion(CONVERSATIONS_TABLE, suffix),
                        metadata,
                        # composite PK: two deployments may legitimately use
                        # the same conversation id, and before consolidation
                        # they were kept apart by living in different tables
                        Column("deployment_id", String(64), primary_key=True),
                        Column("conversation_id", String(255), primary_key=True),
                        Column("subject", String(255), nullable=True),
                        Column("summary", Text, nullable=True),
                        # fold cutoff. Meaningful only together with the
                        # conversation: item ids are a table-wide sequence.
                        Column(
                            "last_summarized_item_id",
                            Integer,
                            nullable=False,
                            default=0,
                        ),
                        # messages folded so far — the fold threshold is
                        # measured in messages, not row ids
                        Column(
                            "last_summarized_count", Integer, nullable=False, default=0
                        ),
                        # optimistic lock: the summarizer runs outside the
                        # transaction, so this is what makes the fold safe
                        Column("summary_version", Integer, nullable=False, default=0),
                        Column("message_count", Integer, nullable=False, default=0),
                        Column("created_at", DateTime, nullable=False),
                        Column("last_active_at", DateTime, nullable=False),
                    )
                state = None
                if self._long_term:
                    # tier 3 only
                    state = Table(
                        _companion(FACTS_TABLE, suffix),
                        metadata,
                        Column("id", Integer, primary_key=True, autoincrement=True),
                        Column("deployment_id", String(64), nullable=False),
                        Column("scope", String(8), nullable=False),
                        # subject (user), '' (app), or conversation id (session)
                        Column("owner", String(255), nullable=False),
                        # not `key`: reserved word in MySQL 8
                        Column("state_key", String(255), nullable=False),
                        Column("value", Text, nullable=False),
                        # {conversation_id, turn_id, message_id} — resolves to
                        # the turn that produced this value
                        Column("source_ref", Text, nullable=True),
                        # agent | operator. The audit surface shows this: a user
                        # looking at their own memories needs to know which of
                        # them their own conversation put there.
                        Column("written_by", String(16), nullable=False),
                        Column("updated_at", DateTime, nullable=False),
                        # agent-written state decays; operator-written does not
                        Column("expires_at", DateTime, nullable=True),
                        # `app` scope is owner='' and agent-wide — with a
                        # shared table that would collapse every deployment's
                        # app state onto one row without deployment_id here
                        UniqueConstraint(
                            "deployment_id",
                            "scope",
                            "owner",
                            "state_key",
                            name=f"uq_{stem}_state",
                        ),
                        Index(
                            f"idx_{stem}_state_owner",
                            "deployment_id",
                            "scope",
                            "owner",
                        ),
                    )
                # checkfirst=True is create-if-not-exists, but two replicas
                # starting together can still both pass the check and both
                # issue CREATE. With one table per deployment that was a rare
                # cold-start race; with shared tables, finding them already
                # created by *another deployment* is the ordinary case, so a
                # duplicate-object error here means someone else succeeded and
                # is not a failure.
                if self._create_tables:
                    try:
                        metadata.create_all(engine, checkfirst=True)
                    except Exception as err:  # noqa: BLE001
                        if not _already_exists(err):
                            raise
                        log.debug("Memory tables already created concurrently")
                    if not self._check_schema_version(engine, meta, table_name):
                        return False
                # The messages and facts tables are the feature groups' own,
                # created by Hopsworks without the id sequence or the indexes
                # the agent reads through. Adopt them before serving.
                if not self._adopt_feature_group_tables(engine, (table, state)):
                    return False
                self._engine = engine
                self._deployment = deployment
                self._table = table
                self._meta = meta
                self._sessions = sessions
                self._state = state
                log.info(
                    "ManagedMemoryService ready (table=%s, schema_version=%d)",
                    table_name,
                    SCHEMA_VERSION,
                )
                return True
            except Exception:  # noqa: BLE001 — degrade to stateless, never crash
                # Retryable, not sticky. This used to latch permanently, so a
                # database blip during the first touch left the replica
                # answering /ready with 503 forever while /health stayed ok —
                # so Kubernetes never restarted it. A zombie replica is worse
                # than a slow one.
                self._init_failed_until = time.monotonic() + self._init_backoff
                self._init_backoff = min(self._init_backoff * 2, 60.0)
                log.exception(
                    "ManagedMemoryService could not connect to the database; "
                    "retrying in %.0fs, running without persistent memory until "
                    "then",
                    self._init_backoff,
                )
                return False

    def _check_schema_version(self, engine, meta, table_name: str) -> bool:
        """Record our schema version, or refuse to run against a newer one.

        ``create_all`` is create-if-not-exists: against a table an older SDK
        already created it does nothing at all, so a column added in a later
        version would be missing and only surface as a query error much later.
        Recording the version makes that case explicit. Migrating *forward* is a
        no-op today (v1 is the first shape); the branch that matters now is
        refusing to write through an older SDK to a newer table, where guessing
        would corrupt data.
        """
        with engine.begin() as conn:
            row = conn.execute(
                meta.select().where(meta.c.table_name == table_name)
            ).fetchone()
            if row is None:
                conn.execute(
                    meta.insert().values(
                        table_name=table_name,
                        schema_version=SCHEMA_VERSION,
                        updated_at=_utcnow(),
                    )
                )
                return True
            if row.schema_version > SCHEMA_VERSION:
                log.error(
                    "Memory table %s is at schema version %d but this SDK "
                    "understands %d. Refusing to use it — upgrade "
                    "hopsworks-agent-protocol. The agent will run without "
                    "persistent memory.",
                    table_name,
                    row.schema_version,
                    SCHEMA_VERSION,
                )
                return False
            if row.schema_version < SCHEMA_VERSION:
                # no forward migrations exist yet; when one does, apply the
                # ordered ALTERs for (row.schema_version, SCHEMA_VERSION] here
                conn.execute(
                    meta.update()
                    .where(meta.c.table_name == table_name)
                    .values(schema_version=SCHEMA_VERSION, updated_at=_utcnow())
                )
            return True

    def _fold_state(self, conversation_id: str) -> tuple[int, int, int | None]:
        """``(summary_version, last_summarized_item_id, message_count)``.

        ``message_count`` is ``None`` when there is no session row to read —
        without a summarizer nothing folds, so paying for that read every turn
        would buy nothing. It is not ``0``: callers have to tell "this
        conversation holds no messages" apart from "there is no row to ask",
        because the second means the cache cannot be validated at all.
        """
        if self._summarize is None or self._sessions is None:
            return (0, 0, None)
        s = self._sessions
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    self._mine(
                        s.select().where(s.c.conversation_id == conversation_id), s
                    )
                ).fetchone()
        except Exception:  # noqa: BLE001
            log.exception("Session read failed; treating conversation as unfolded")
            return (0, 0, None)
        if row is None:
            return (0, 0, 0)
        return (row.summary_version, row.last_summarized_item_id, row.message_count)

    def get(self, conversation_id: str) -> list[Turn]:
        if not self._ensure_engine():
            return []
        version, cutoff, count = self._fold_state(conversation_id)
        # Cache identity is (fold version, message count). The version alone
        # only moves on a fold, so between folds — and always without a
        # summarizer, where it is pinned at 0 — a replica that cached this
        # conversation never saw turns written by another replica, and the model
        # silently lost them under round-robin routing. The count comes from the
        # session row already read above, so this costs nothing extra.
        key = (version, count)
        if count is not None:
            with self._lock:
                cached = self._cache.get(conversation_id)
                if cached is not None and cached[0] == key:
                    return list(cached[1])
        t = self._table
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    self._mine(t.select(), t)
                    .where(t.c.conversation_id == conversation_id)
                    # closed turns only: an open turn has no reply yet, and an
                    # abandoned one is a question we failed to answer — neither
                    # belongs in what the model reads back
                    .where(t.c.status == TURN_CLOSED)
                    .where(t.c.memory_type == ITEM_MESSAGE)
                    # everything at or below the cutoff is represented by the
                    # summary. Without this the model would get the summary
                    # *and* the turns it was built from: more tokens than
                    # before, and the same content twice.
                    .where(t.c.id > cutoff)
                    .order_by(t.c.id.desc())
                    .limit(self._max)
                ).fetchall()
        except Exception:  # noqa: BLE001
            log.exception("Memory read failed; returning empty history")
            return []
        turns = [{"role": r.role, "content": r.content} for r in reversed(rows)]
        if count is not None:
            with self._lock:
                self._cache[conversation_id] = (key, list(turns))
        return turns

    def clear(self, conversation_id: str) -> None:
        """Drop a conversation: its messages **and** its session row.

        The session row has to go too. It holds the rolling summary — a
        distillation of the very messages being deleted — so deleting only the
        items leaves the content readable through ``get_summary`` and
        ``system_context()``. It also holds ``message_count`` and the fold
        cursor, so a reused conversation id would otherwise fold immediately and
        blend the deleted conversation into the new one's summary.

        Both statements share a transaction: a half-cleared conversation whose
        summary outlived its messages is the exact state this exists to prevent.
        """
        if self._ensure_engine():
            try:
                with self._engine.begin() as conn:
                    conn.execute(
                        self._mine(
                            self._table.delete().where(
                                self._table.c.conversation_id == conversation_id
                            ),
                            self._table,
                        )
                    )
                    if self._sessions is not None:
                        conn.execute(
                            self._mine(
                                self._sessions.delete().where(
                                    self._sessions.c.conversation_id == conversation_id
                                ),
                                self._sessions,
                            )
                        )
            except Exception:  # noqa: BLE001
                log.exception("Memory clear failed")
        with self._lock:
            self._cache.pop(conversation_id, None)

    # ── feature-store registration (optional) ────────────────────────────

    #: SQLAlchemy column type -> Hopsworks feature type, for the types this
    #: module actually declares. Deliberately not a guess-anything mapping: a
    #: type that turns up here unmapped is a schema change someone forgot to
    #: think about, and should fail rather than be silently called a string.
    #: Keyed on the SQLAlchemy type class name, which is what the column
    #: objects carry ("String", not "VARCHAR"); the SQL spellings are here too
    #: so a reflected table maps as readily as a declared one.
    _FEATURE_TYPES = {
        "INTEGER": "int",
        "SMALLINTEGER": "int",
        "SMALLINT": "int",
        "STRING": "string",
        "VARCHAR": "string",
        "TEXT": "string",
        "DATETIME": "timestamp",
        "TIMESTAMP": "timestamp",
    }

    def _features_for(self, table):
        from hsfs.feature import Feature

        features = []
        for column in table.columns:
            name = type(column.type).__name__.upper()
            kind = self._FEATURE_TYPES.get(name)
            if kind is None:
                raise RuntimeError(
                    f"No Hopsworks feature type for column {column.name!r} "
                    f"({name}) of {table.name}. Add it to _FEATURE_TYPES."
                )
            features.append(Feature(column.name, type=kind))
        return features

    # ── feature-store export (optional) ──────────────────────────────────

    #: What each exported table is keyed on, and which column carries its
    #: time. The keys are natural, not the autoincrement id alone: a feature
    #: group holds every deployment's rows, so the deployment has to be part of
    #: what makes a row unique — the same reason it leads every SQL index here.
    #: SQLAlchemy column type -> (offline type, online type). Declared rather
    #: than inferred, for two reasons found the hard way: a column that happens
    #: to be entirely NULL in a batch infers as dtype 'null' and is rejected,
    #: and an inferred string lands as varchar(100) online — which would
    #: silently truncate message content to 100 characters.

    def _features_for(self, table):
        from hsfs.feature import Feature

        features = []
        for column in table.columns:
            name = type(column.type).__name__.upper()
            if name == "STRING":
                length = getattr(column.type, "length", None) or 255
                offline, online = "string", f"varchar({length})"
            else:
                mapped = self._FEATURE_TYPES.get(name)
                if mapped is None:
                    raise RuntimeError(
                        f"No feature type for column {column.name!r} ({name}) "
                        f"of {table.name}. Add it to _FEATURE_TYPES."
                    )
                offline, online = mapped
            features.append(Feature(column.name, type=offline, online_type=online))
        return features

    _EXPORT_SHAPE = {
        "items": (("deployment_id", "id"), "created_at"),
        "sessions": (("deployment_id", "conversation_id"), "last_active_at"),
        "state": (("deployment_id", "id"), "updated_at"),
    }

    @staticmethod
    def _prepare_for_insert(frame):
        """Make a batch's null columns survive the trip to the online store.

        Two distinct traps, and the fix for one must not create the other.

        A *mixed* datetime column — some rows expiring, some never — carries
        NaT, and hsfs raises "NaTType does not support timetuple" instead of
        writing NULL. Object dtype holding real datetimes and Nones converts
        cleanly.

        A column that is *entirely* null in the batch has no type at all once
        it reaches Arrow, and the online writer rejects it — silently and
        asynchronously: the insert returns, and the rows never appear, with
        only an `online_ingestion_result` row of status FAILED to say so. That
        is why the datetime fix above is conditional. Applying it to an
        all-NaT column would strip the one piece of type information the column
        still had and turn a working write into a vanishing one.

        For strings there is no typed empty value to fall back on, so an
        all-null string column is written as empty strings. Within such a batch
        nothing is lost — every value was null — but a null and an empty string
        are not distinguishable in the feature group afterwards.
        """
        import pandas as pd

        for column in frame.columns:
            values = frame[column]
            if values.notna().any():
                if pd.api.types.is_datetime64_any_dtype(values) and values.isna().any():
                    frame[column] = values.astype(object).where(values.notna(), None)
                continue
            # entirely null: give Arrow something to type
            if pd.api.types.is_datetime64_any_dtype(values):
                continue  # datetime64 is already typed; leave NaT alone
            frame[column] = ""
        return frame

    def export_feature_groups(
        self,
        feature_store=None,
        *,
        storage: str | None = EXPORT_STORAGE_OFFLINE,
        since: int | None = None,
        batch_size: int = 5000,
        version: int = 1,
        include=None,
    ) -> dict:
        """Copy memory rows into feature groups, for analysis.

        The agent already writes to the feature groups' online tables as it
        runs — that is where its memory lives, read back as ordered ranges,
        closed with predicate updates and erased with row deletes, none of
        which a feature group API offers. So this does not move memory
        anywhere; it fills the *offline* store from those same rows, which is
        what makes them joinable, queryable and usable as training data.

        Hence ``storage`` defaults to offline: the online side is already
        current by construction, and the offline write is a Delta write worth
        doing on a schedule rather than per turn. Pass ``"both"`` only when
        rebuilding an online table that was dropped.

        Re-exporting a range is safe. Feature groups upsert on the primary key,
        and the keys here are stable, so a job that overlaps its previous run —
        or does not track a watermark at all — produces the same rows rather
        than duplicates.

        Args:
            feature_store: an existing handle; logs in if omitted.
            storage: "offline" (default), "online", or "both".
            since: only rows with a higher id. None exports everything.
            batch_size: rows per insert.
            version: feature group version.
            include: any of "items", "sessions", "state". Defaults to all.

        Returns:
            ``{name: rows_exported}``.
        """
        if not self._ensure_engine():
            raise RuntimeError(
                "Memory tables are not reachable, so there is nothing to "
                "export. Check the database connection first."
            )
        storage_arg = _storage_arg(storage)
        if feature_store is None:
            import hopsworks

            feature_store = hopsworks.login().get_feature_store()

        import pandas as pd
        from sqlalchemy import inspect as sqlalchemy_inspect
        from sqlalchemy import select

        inspector = sqlalchemy_inspect(self._engine)
        tables = {
            "items": self._table,
            "sessions": self._sessions,
            "state": self._state,
        }
        wanted = list(tables) if include is None else list(include)
        unknown = [name for name in wanted if name not in tables]
        if unknown:
            raise ValueError(
                f"Unknown table(s) {unknown}. Choose from {sorted(tables)}."
            )

        exported = {}
        for name in wanted:
            table = tables[name]
            if table is None or not inspector.has_table(table.name):
                # asked of the database, not of this handle's configuration
                log.info("Skipping %s: no such table", name)
                continue
            keys, event_time = self._EXPORT_SHAPE[name]
            stmt = select(table)
            if since is not None and "id" in table.c:
                stmt = stmt.where(table.c.id > since)
            if "id" in table.c:
                stmt = stmt.order_by(table.c.id)
            with self._engine.connect() as conn:
                rows = [dict(r._mapping) for r in conn.execute(stmt)]
            if not rows:
                exported[table.name] = 0
                continue

            fg = feature_store.get_feature_group(
                name=_feature_group_name(table.name), version=version
            )
            if fg is None:
                raise RuntimeError(
                    f"No feature group for {table.name}. These are provisioned "
                    "by Hopsworks when an agent deployment starts, not by this "
                    "SDK — start the deployment first."
                )
            for start in range(0, len(rows), batch_size):
                frame = pd.DataFrame(rows[start : start + batch_size])
                frame = self._prepare_for_insert(frame)
                insert_kwargs = {"write_options": {"mode": "append"}}
                if storage_arg is not None:
                    insert_kwargs["storage"] = storage_arg
                fg.insert(frame, **insert_kwargs)
            exported[table.name] = len(rows)
        return exported

    def healthcheck(self) -> bool:
        # ready once the engine + tables are established and reachable; this is
        # also where a deployment's DDL runs, off the request path
        return self._ensure_engine()

    # ── turn lifecycle ────────────────────────────────────────────────────

    def _expiry_for(self, memory_type: str, now: datetime) -> datetime | None:
        """When a row becomes prunable.

        ``message`` rows keep forever by default: they are the transcript — what
        a user reads in the panel and an operator reads during an incident — and
        they are the small half of the table. Deleting them silently on a timer
        would make history lossier than not compacting at all, so it is an
        explicit operator policy instead. tool_call/event rows are debug
        telemetry and the bulk of the volume, so they do expire.
        """
        days = (
            self._message_retention
            if memory_type == ITEM_MESSAGE
            else self._tool_event_retention
        )
        return None if days is None else now + timedelta(days=days)

    def _insert_item(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        status: str,
        seq: int,
        memory_type: str = ITEM_MESSAGE,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> bool:
        if not self._ensure_engine():
            return False
        now = _utcnow()
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    self._table.insert().values(
                        deployment_id=self._deployment,
                        conversation_id=conversation_id,
                        turn_id=turn_id,
                        message_id=message_id,
                        seq=seq,
                        status=status,
                        subject=subject,
                        memory_type=memory_type,
                        role=role,
                        content=content,
                        created_at=now,
                        expires_at=self._expiry_for(memory_type, now),
                    )
                )
            return True
        except Exception:  # noqa: BLE001 — memory must never break a turn
            log.exception("Memory write failed; item not persisted")
            return False

    def begin_turn(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> None:
        with self._lock:
            self._pending[turn_id] = []
            self._seq[turn_id] = 0
        ok = self._insert_item(
            conversation_id,
            turn_id,
            role,
            content,
            status=TURN_OPEN,
            seq=0,
            message_id=message_id,
            subject=subject,
        )
        if not ok:
            with self._lock:
                self._pending.pop(turn_id, None)
                self._seq.pop(turn_id, None)
            return
        with self._lock:
            self._seq[turn_id] = 1
            self._pending[turn_id].append({"role": role, "content": content})

    def record_item(
        self,
        conversation_id: str,
        turn_id: str,
        role: str,
        content: str,
        *,
        memory_type: str = ITEM_MESSAGE,
        seq: int | None = None,
        message_id: str | None = None,
        subject: str | None = None,
    ) -> None:
        with self._lock:
            if seq is None:
                seq = self._seq.get(turn_id, 0)
                self._seq[turn_id] = seq + 1
        ok = self._insert_item(
            conversation_id,
            turn_id,
            role,
            content,
            status=TURN_OPEN,
            seq=seq,
            memory_type=memory_type,
            message_id=message_id,
            subject=subject,
        )
        if ok and memory_type == ITEM_MESSAGE:
            with self._lock:
                if turn_id in self._pending:
                    self._pending[turn_id].append({"role": role, "content": content})

    def conversation_subject(self, conversation_id: str) -> str | None:
        """The subject most recently stamped on this conversation's messages.

        Most recent rather than first: an agent that identifies the caller
        mid-conversation rebinds, and it is the identity it arrived at that
        durable memory is filed under. Reads messages rather than session rows
        so it answers for a store with no summarizer configured.
        """
        if not self._ensure_engine():
            return None
        from sqlalchemy import select

        t = self._table
        stmt = (
            self._mine(select(t.c.subject), t)
            .where(t.c.conversation_id == conversation_id)
            .where(t.c.subject != "")
            .order_by(t.c.id.desc())
            .limit(1)
        )
        try:
            with self._engine.connect() as conn:
                row = conn.execute(stmt).first()
        except Exception:  # noqa: BLE001
            log.exception("Looking up conversation subject failed")
            return None
        return row[0] if row and row[0] else None

    def list_conversations(
        self, *, subject: str | None = None, limit: int = 50
    ) -> list[dict]:
        """Conversations this deployment holds, most recently active first.

        Derived from the messages rather than the conversation rows, because
        those only exist once a summarizer is configured — a Tier-1 store has
        conversations and no session bookkeeping at all. The summary, when
        there is one, is merged in afterwards.
        """
        if not self._ensure_engine():
            return []
        from sqlalchemy import func, select

        t = self._table
        stmt = (
            self._mine(
                select(
                    t.c.conversation_id,
                    func.count().label("message_count"),
                    func.max(t.c.created_at).label("last_active_at"),
                    func.max(t.c.subject).label("subject"),
                ),
                t,
            )
            .where(t.c.memory_type == ITEM_MESSAGE)
            .where(t.c.status == TURN_CLOSED)
        )
        if subject is not None:
            stmt = stmt.where(t.c.subject == subject)
        stmt = (
            stmt.group_by(t.c.conversation_id)
            .order_by(func.max(t.c.created_at).desc())
            .limit(limit)
        )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).fetchall()
        except Exception:  # noqa: BLE001
            log.exception("Listing conversations failed")
            return []

        summaries: dict[str, str] = {}
        if self._sessions is not None and rows:
            s = self._sessions
            try:
                with self._engine.connect() as conn:
                    for row in conn.execute(
                        self._mine(s.select(), s).where(
                            s.c.conversation_id.in_([r.conversation_id for r in rows])
                        )
                    ):
                        if row.summary:
                            summaries[row.conversation_id] = row.summary
            except Exception:  # noqa: BLE001 — a missing summary is not a failure
                log.exception("Reading conversation summaries failed")

        return [
            {
                "conversation_id": r.conversation_id,
                "subject": r.subject,
                "message_count": r.message_count,
                "last_active_at": (
                    r.last_active_at.isoformat() if r.last_active_at else None
                ),
                "has_summary": r.conversation_id in summaries,
            }
            for r in rows
        ]

    def rebind_turn_subject(
        self, conversation_id: str, turn_id: str, subject: str
    ) -> None:
        """Restamp every row already written for this turn (see the base class).

        Rows written *after* this carry the new subject because the caller
        passes ``ctx.subject``, which the context has already updated — so this
        only has to catch up the ones ``begin_turn`` wrote. Best-effort like
        every other write here: a failure leaves the turn split across two
        subjects, which is worth a warning but never worth failing the reply.
        """
        if not self._ensure_engine():
            return
        t = self._table
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    self._mine(t.update(), t)
                    .where(t.c.conversation_id == conversation_id)
                    .where(t.c.turn_id == turn_id)
                    .values(subject=subject)
                )
        except Exception:  # noqa: BLE001 — memory must never break a turn
            log.exception("Memory rebind_turn_subject failed for turn %s", turn_id)

    def end_turn(
        self, conversation_id: str, turn_id: str, *, status: str = TURN_CLOSED
    ) -> None:
        with self._lock:
            pending = self._pending.pop(turn_id, [])
            self._seq.pop(turn_id, None)
        if not self._ensure_engine():
            return
        t = self._table
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    self._mine(t.update(), t)
                    .where(t.c.conversation_id == conversation_id)
                    .where(t.c.turn_id == turn_id)
                    # NOT `status == open`: the reaper may have already flipped
                    # this turn to abandoned for outliving turn_timeout_seconds,
                    # which agents legitimately do on deep tool loops. A turn
                    # that actually finished should win over the reaper's guess,
                    # otherwise the completed answer stays abandoned and
                    # invisible to get(). Already-closed rows are excluded so a
                    # duplicate end_turn cannot reopen or re-count anything.
                    .where(t.c.status != TURN_CLOSED)
                    .values(status=status)
                )
        except Exception:  # noqa: BLE001
            log.exception("Memory end_turn failed; turn left open")
            return
        if result.rowcount == 0:
            # Nothing transitioned: the turn was already closed, so its messages
            # are already counted and cached. Bumping again would drift the fold
            # trigger and duplicate rows in the cache.
            return
        if status == TURN_CLOSED and pending:
            self._bump_message_count(conversation_id, len(pending))
        # Drop the entry either way. This used to extend the cache in place to
        # save a read, but the entry is now identified by (fold version, message
        # count) and the count has just moved — so an extended entry would carry
        # a key the next get() cannot match, and would have to be rebuilt
        # anyway. Dropping it costs one indexed read on the next turn and
        # removes the only path by which the in-process copy could drift from
        # the table.
        with self._lock:
            self._cache.pop(conversation_id, None)

    def _bump_message_count(self, conversation_id: str, added: int) -> None:
        """Track how many messages the conversation holds, for the fold trigger.

        Incremented in SQL rather than read-modify-write so overlapping turns
        cannot lose an increment and stall summarization.
        """
        if self._sessions is None or added <= 0:
            return
        s = self._sessions
        now = _utcnow()
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    self._mine(s.update(), s)
                    .where(s.c.conversation_id == conversation_id)
                    .values(
                        message_count=s.c.message_count + added,
                        last_active_at=now,
                    )
                )
                if result.rowcount:
                    return
            with self._engine.begin() as conn:
                conn.execute(
                    s.insert().values(
                        deployment_id=self._deployment,
                        conversation_id=conversation_id,
                        message_count=added,
                        last_summarized_item_id=0,
                        last_summarized_count=0,
                        summary_version=0,
                        created_at=now,
                        last_active_at=now,
                    )
                )
        except Exception:  # noqa: BLE001 — another turn inserted first, or the
            # DB is unhappy; retry the update once and give up quietly
            try:
                with self._engine.begin() as conn:
                    conn.execute(
                        self._mine(s.update(), s)
                        .where(s.c.conversation_id == conversation_id)
                        .values(
                            message_count=s.c.message_count + added,
                            last_active_at=now,
                        )
                    )
            except Exception:  # noqa: BLE001
                log.exception("Could not update session message count")

    def reap_open_turns(self, older_than_seconds: int | None = None) -> int:
        """Mark long-open turns as abandoned.

        A turn is closed in a ``finally``, but that is not a guarantee: a pod
        can be killed mid-request, and an ASGI server may not run an async
        generator's cleanup promptly (or at all) when a client disconnects.
        Without a sweep those rows stay ``open`` forever — invisible to reads,
        and (once summarization lands) permanently blocking the fold cutoff,
        which is the failure that actually bites.
        """
        if not self._ensure_engine():
            return 0
        timeout = (
            self._turn_timeout if older_than_seconds is None else older_than_seconds
        )
        cutoff = _utcnow() - timedelta(seconds=timeout)
        t = self._table
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    self._mine(t.update(), t)
                    .where(t.c.status == TURN_OPEN)
                    .where(t.c.created_at < cutoff)
                    .values(status=TURN_ABANDONED)
                )
            count = result.rowcount or 0
        except Exception:  # noqa: BLE001
            log.exception("Reaping open turns failed")
            return 0
        if count:
            log.warning("Marked %d stale open turn row(s) as abandoned", count)
        return count

    def maybe_reap(self) -> None:
        """Throttled ``reap_open_turns`` + prune for the post-response slot."""
        now = time.monotonic()
        with self._lock:
            due_reap = now - self._last_reap >= max(self._turn_timeout, 60)
            if due_reap:
                self._last_reap = now
            due_prune = now - self._last_prune >= 300
            if due_prune:
                self._last_prune = now
        if due_reap:
            self.reap_open_turns()
        if due_prune:
            self.prune_expired()

    def prune_expired(self, limit: int = 500) -> int:
        """Delete rows past ``expires_at``, a bounded batch at a time.

        Bounded because this runs inline after a response — amortized across
        turns rather than a table scan someone waits on. Selecting the ids
        first and deleting by id keeps it portable: MySQL will not accept a
        subquery against the table being deleted from.
        """
        if not self._ensure_engine():
            return 0
        t = self._table
        now = _utcnow()
        try:
            with self._engine.connect() as conn:
                ids = [
                    r.id
                    for r in conn.execute(
                        self._mine(t.select(), t)
                        .with_only_columns(t.c.id)
                        .where(t.c.expires_at.isnot(None))
                        .where(t.c.expires_at < now)
                        .limit(limit)
                    ).fetchall()
                ]
            if not ids:
                return 0
            with self._engine.begin() as conn:
                conn.execute(self._mine(t.delete().where(t.c.id.in_(ids)), t))
        except Exception:  # noqa: BLE001
            log.exception("Pruning expired memory rows failed")
            return 0
        log.info("Pruned %d expired memory row(s)", len(ids))
        return len(ids)

    def transcript(
        self, conversation_id: str, *, include_events: bool = False
    ) -> list[dict]:
        if not self._ensure_engine():
            return []
        t = self._table
        stmt = self._mine(t.select().where(t.c.conversation_id == conversation_id), t)
        if not include_events:
            # the default is a clean transcript: messages of turns that
            # completed. Folded rows stay — they are still what was said.
            stmt = stmt.where(t.c.memory_type == ITEM_MESSAGE).where(
                t.c.status == TURN_CLOSED
            )
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt.order_by(t.c.id)).fetchall()
        except Exception:  # noqa: BLE001
            log.exception("Transcript read failed")
            return []
        return [
            {
                "id": r.id,
                "turn_id": r.turn_id,
                "message_id": r.message_id,
                "role": r.role,
                "content": r.content,
                "memory_type": r.memory_type,
                "status": r.status,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]

    def summarized_through(self, conversation_id: str) -> int:
        return self._fold_state(conversation_id)[1] if self._ensure_engine() else 0

    # ── summarization (tier 2) ───────────────────────────────────────────

    def get_summary(self, conversation_id: str) -> str | None:
        if self._summarize is None or not self._ensure_engine():
            return None
        s = self._sessions
        if s is None:
            return None
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    self._mine(
                        s.select().where(s.c.conversation_id == conversation_id), s
                    )
                ).fetchone()
        except Exception:  # noqa: BLE001
            log.exception("Summary read failed")
            return None
        return row.summary if row is not None else None

    def _claim_fold(self, conversation_id: str):
        """Phase 1: decide what to fold and lock the decision in.

        Returns ``(version, old_summary, turns, max_id, count)`` or ``None``
        when there is nothing to do. Deliberately short: it commits before the
        summarizer is called.
        """
        from sqlalchemy import func

        s, t = self._sessions, self._table
        with self._engine.begin() as conn:
            row = conn.execute(
                self._mine(s.select(), s)
                .where(s.c.conversation_id == conversation_id)
                .with_for_update()
            ).fetchone()
            if row is None:
                return None
            if row.message_count - row.last_summarized_count < self._summarize_every:
                return None

            # Never fold across a turn that is still open. Its reply does not
            # exist yet, so folding would compact the question into the summary
            # and leave the answer dangling in the visible window.
            open_min = conn.execute(
                self._mine(t.select(), t)
                .with_only_columns(func.min(t.c.id))
                .where(t.c.conversation_id == conversation_id)
                .where(t.c.status == TURN_OPEN)
            ).scalar()

            stmt = (
                self._mine(t.select(), t)
                .where(t.c.conversation_id == conversation_id)
                .where(t.c.status == TURN_CLOSED)
                .where(t.c.memory_type == ITEM_MESSAGE)
                .where(t.c.id > row.last_summarized_item_id)
            )
            if open_min is not None:
                stmt = stmt.where(t.c.id < open_min)
            items = conn.execute(stmt.order_by(t.c.id)).fetchall()

        # Keep a verbatim tail. Folding everything unfolded would leave the
        # turn right after a fold with a summary and no recent messages at all,
        # which inverts the tiering: the buffer is supposed to own recency
        # ("what did I just say") and the summary the older context.
        if self._keep_recent:
            items = items[: -self._keep_recent]
        if not items:
            return None
        turns = [{"role": r.role, "content": r.content} for r in items]
        return (row.summary_version, row.summary, turns, items[-1].id, len(items))

    def _commit_fold(
        self,
        conversation_id: str,
        version: int,
        summary: str,
        max_id: int,
        count: int,
    ) -> bool:
        """Phase 3: publish the summary, if nobody else folded meanwhile."""
        s = self._sessions
        with self._engine.begin() as conn:
            result = conn.execute(
                self._mine(s.update(), s)
                .where(s.c.conversation_id == conversation_id)
                .where(s.c.summary_version == version)
                .values(
                    summary=summary,
                    last_summarized_item_id=max_id,
                    last_summarized_count=s.c.last_summarized_count + count,
                    summary_version=version + 1,
                    last_active_at=_utcnow(),
                )
            )
        return bool(result.rowcount)

    # ── scoped durable state (tier 3) ────────────────────────────────────

    def set_state(
        self,
        scope: str,
        owner: str,
        key: str,
        value: str,
        *,
        source_ref: str | None = None,
        written_by: str = WRITTEN_BY_AGENT,
    ) -> None:
        """Upsert one scoped value.

        Agent-written values are bounded and decay. That is not tidiness: this
        state is auto-injected into every future turn for its owner, so a value
        the model was talked into writing outlives the conversation that
        produced it. Caps bound how much a single conversation can plant, and
        the TTL means a bad one fades instead of persisting forever. Neither
        prevents a false memory — the audit surface (``list_state`` /
        ``delete_state``, exposed to the subject) is what does.
        """
        if scope not in SCOPES:
            raise ValueError(f"unknown scope {scope!r}; expected one of {SCOPES}")
        if not self._ensure_engine() or self._state is None:
            return
        value = value[: self._max_value_chars]
        now = _utcnow()
        expires_at = (
            now + timedelta(days=self._state_ttl)
            if written_by == WRITTEN_BY_AGENT and self._state_ttl is not None
            else None
        )
        st = self._state
        values = {
            "value": value,
            "source_ref": source_ref,
            "written_by": written_by,
            "updated_at": now,
            "expires_at": expires_at,
        }
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    self._mine(st.update(), st)
                    .where(st.c.scope == scope)
                    .where(st.c.owner == owner)
                    .where(st.c.state_key == key)
                    .values(**values)
                )
                if not result.rowcount:
                    conn.execute(
                        st.insert().values(
                            deployment_id=self._deployment,
                            scope=scope,
                            owner=owner,
                            state_key=key,
                            **values,
                        )
                    )
        except Exception:  # noqa: BLE001 — a lost race on the unique key, or a
            # sick database; retry as an update and give up quietly
            try:
                with self._engine.begin() as conn:
                    conn.execute(
                        self._mine(st.update(), st)
                        .where(st.c.scope == scope)
                        .where(st.c.owner == owner)
                        .where(st.c.state_key == key)
                        .values(**values)
                    )
            except Exception:  # noqa: BLE001
                log.exception("Could not write %s-scoped state %r", scope, key)
                return
        if written_by == WRITTEN_BY_AGENT:
            self._evict_excess_state(scope, owner)

    def _evict_excess_state(self, scope: str, owner: str) -> None:
        """Keep at most ``max_state_keys_written`` agent-written keys per owner,.

        dropping the least recently updated.
        """
        st = self._state
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    self._mine(st.select(), st)
                    .with_only_columns(st.c.id)
                    .where(st.c.scope == scope)
                    .where(st.c.owner == owner)
                    .where(st.c.written_by == WRITTEN_BY_AGENT)
                    .order_by(st.c.updated_at.desc())
                ).fetchall()
            excess = [r.id for r in rows[self._max_keys_written :]]
            if not excess:
                return
            with self._engine.begin() as conn:
                conn.execute(self._mine(st.delete().where(st.c.id.in_(excess)), st))
            log.info(
                "Evicted %d agent-written state key(s) for %s/%s over the cap",
                len(excess),
                scope,
                owner,
            )
        except Exception:  # noqa: BLE001
            log.exception("State eviction failed")

    def get_state(self, scope: str, owner: str, key: str) -> str | None:
        rows = self.list_state(scope, owner, key=key)
        return rows[0]["value"] if rows else None

    def list_state(
        self,
        scope: str,
        owner: str,
        *,
        key: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Unexpired values for an owner, most recently updated first.

        Expiry is applied on read, not only by the pruner: a decayed value must
        stop being injected on its TTL rather than whenever a prune pass next
        happens to run.
        """
        if not self._ensure_engine() or self._state is None:
            return []
        st = self._state
        now = _utcnow()
        stmt = (
            self._mine(st.select(), st)
            .where(st.c.scope == scope)
            .where(st.c.owner == owner)
            .where((st.c.expires_at.is_(None)) | (st.c.expires_at > now))
        )
        if key is not None:
            stmt = stmt.where(st.c.state_key == key)
        stmt = stmt.order_by(st.c.updated_at.desc())
        if limit is not None:
            stmt = stmt.limit(limit)
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt).fetchall()
        except Exception:  # noqa: BLE001
            log.exception("State read failed")
            return []
        return [
            {
                "key": r.state_key,
                "value": r.value,
                "scope": r.scope,
                "owner": r.owner,
                "written_by": r.written_by,
                "source_ref": r.source_ref,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                "expires_at": r.expires_at.isoformat() if r.expires_at else None,
            }
            for r in rows
        ]

    def delete_state(self, scope: str, owner: str, key: str | None = None) -> int:
        """Forget one value, or every value for an owner. Returns rows removed."""
        if not self._ensure_engine() or self._state is None:
            return 0
        st = self._state
        stmt = self._mine(
            st.delete().where(st.c.scope == scope).where(st.c.owner == owner), st
        )
        if key is not None:
            stmt = stmt.where(st.c.state_key == key)
        try:
            with self._engine.begin() as conn:
                return conn.execute(stmt).rowcount or 0
        except Exception:  # noqa: BLE001
            log.exception("State delete failed")
            return 0

    def state_block(self, subject: str, conversation_id: str) -> str:
        """The state part of ``ctx.system_context()``.

        Bounded on read as well as write: an owner past the cap contributes only
        their most recent keys, each truncated, with a marker saying so. Without
        it, an agent that remembers a few things per session grows the system
        prompt without limit.
        """
        if not self._ensure_engine() or self._state is None:
            return ""
        rows: list[dict] = []
        for scope, owner in (
            (SCOPE_APP, ""),
            (SCOPE_USER, subject),
            (SCOPE_SESSION, conversation_id),
        ):
            # one past the cap, so we can tell "exactly at the cap" from
            # "truncated" without a second count query
            rows.extend(
                self.list_state(scope, owner, limit=self._max_keys_injected + 1)
            )
        if not rows:
            return ""
        shown = rows[: self._max_keys_injected]
        lines = [
            f"- {r['key']}: {r['value'][: self._inject_value_chars]}" for r in shown
        ]
        if len(rows) > len(shown):
            # Say there is a budget and that this view is partial. A silent cap
            # leaves the model believing it can remember without limit and that
            # what it sees is everything; told there is pressure, it prunes.
            # Still uncounted beyond "more than": the exact total costs another
            # query and does not change what the model should do about it.
            lines.append(
                f"- (showing the {len(shown)} most recently updated of more "
                "than that — consolidate related keys, or `forget` ones that "
                "are stale, so the useful ones stay visible)"
            )
        return "\n".join(lines)

    # ── semantic search (tier 3) ─────────────────────────────────────────

    def search(
        self,
        query: str,
        *,
        subject: str | None = None,
        conversation_id: str | None = None,
        k: int = 5,
    ) -> list[dict]:
        """Find older messages relevant to ``query``.

        Vector search when an embedder and vector store are configured, keyword
        matching over SQL otherwise. The fallback is deliberate: ``search``
        should be registerable as a tool from day one, so an agent's prompt does
        not have to change when semantic search is switched on later.

        This is for *older* context. The current turn lives in the synchronous
        SQL tier and may not have reached the vector index yet — see
        ``vectorstore``.
        """
        if self._vector_store is not None and self._embedder is not None:
            try:
                vector = self._embedder(query)
            except Exception:  # noqa: BLE001
                log.exception("Embedding the query failed; falling back to keywords")
            else:
                # Over-fetch, then re-rank. Re-ranking the k rows the index
                # already chose would only reorder a set similarity picked, so
                # the old-but-relevant message still never appears — which is
                # the entire complaint recency weighting exists to answer. The
                # candidate pool has to be wider than the answer.
                wanted = k * self._search_oversample if self._half_life else k
                hits = self._vector_store.search(
                    vector,
                    k=wanted,
                    subject=subject,
                    conversation_id=conversation_id,
                )
                return self._rerank(hits, k)
        return self._keyword_search(
            query, subject=subject, conversation_id=conversation_id, k=k
        )

    def _rerank(self, hits: list[dict], k: int) -> list[dict]:
        """Trade similarity off against age, then cut to ``k``.

        ``weight = 0.5 ** (age / half_life)`` — a true half-life, so a memory
        exactly ``recency_half_life_days`` old counts half as much as an
        identical one from today. (The ``exp(-age / half_life)`` form often
        quoted for this is a *time constant*: it leaves 0.37 at the half-life,
        not 0.5, which makes the parameter mean something other than its name.)

        Only the vector path needs this. Keyword results come back ordered by
        id, which is already recency, and carry no similarity to trade against.
        """
        if not self._half_life or not hits:
            return hits[:k]
        now = _utcnow()
        scored = []
        for hit in hits:
            score = hit.get("score")
            if score is None:
                scored.append((0.0, hit))
                continue
            created = hit.get("created_at")
            weight = 1.0
            if created:
                try:
                    when = datetime.fromisoformat(str(created))
                    if when.tzinfo is not None:
                        when = when.replace(tzinfo=None)
                    age_days = max((now - when).total_seconds(), 0.0) / 86400.0
                    weight = 0.5 ** (age_days / self._half_life)
                except (TypeError, ValueError):
                    # an unparseable timestamp must not silently sort a row to
                    # the bottom; leave it on similarity alone
                    log.debug("Unparseable created_at %r; ranking on score", created)
            scored.append((score * weight, hit))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [hit for _, hit in scored[:k]]

    def _keyword_search(
        self,
        query: str,
        *,
        subject: str | None,
        conversation_id: str | None,
        k: int,
    ) -> list[dict]:
        if not self._ensure_engine():
            return []
        terms = [word for word in re.split(r"\W+", query) if len(word) > 2][:8]
        if not terms:
            return []
        t = self._table
        stmt = (
            self._mine(t.select(), t)
            .where(t.c.memory_type == ITEM_MESSAGE)
            .where(t.c.status == TURN_CLOSED)
        )
        for term in terms:
            stmt = stmt.where(t.c.content.ilike(f"%{term}%"))
        # same isolation the vector filter gives: a user only searches their own
        # past, never someone else's
        if subject is not None:
            stmt = stmt.where(t.c.subject == subject)
        if conversation_id is not None:
            stmt = stmt.where(t.c.conversation_id == conversation_id)
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(stmt.order_by(t.c.id.desc()).limit(k)).fetchall()
        except Exception:  # noqa: BLE001
            log.exception("Keyword search failed")
            return []
        return [
            {
                "item_id": r.id,
                "conversation_id": r.conversation_id,
                "subject": r.subject,
                "role": r.role,
                "content": r.content,
                "memory_type": r.memory_type,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "score": None,  # keyword match: no meaningful similarity
            }
            for r in rows
        ]

    async def ingest_turn(self, conversation_id: str, turn_id: str) -> int:
        """Embed a completed turn's messages into the vector store.

        Runs in the same awaited-after-streaming slot as summarization — never
        a background thread, because the pod can scale to zero and take an
        un-awaited task with it. Only ``message`` rows are ingested: every row
        costs an embedding plus an insert, and tool/event rows are debug
        telemetry nobody searches for.
        """
        if self._vector_store is None or self._embedder is None:
            return 0
        if not self._ensure_engine():
            return 0
        t = self._table
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    self._mine(t.select(), t)
                    .where(t.c.conversation_id == conversation_id)
                    .where(t.c.turn_id == turn_id)
                    .where(t.c.memory_type == ITEM_MESSAGE)
                    .where(t.c.status == TURN_CLOSED)
                    .order_by(t.c.seq)
                ).fetchall()
        except Exception:  # noqa: BLE001
            log.exception("Could not read turn %s for ingest", turn_id)
            return 0
        if not rows:
            return 0

        def _embed_rows():
            payload = []
            for r in rows:
                payload.append(
                    {
                        "item_id": r.id,
                        "conversation_id": r.conversation_id,
                        "subject": r.subject,
                        "memory_type": r.memory_type,
                        "role": r.role,
                        "content": r.content,
                        "created_at": (
                            r.created_at.isoformat() if r.created_at else None
                        ),
                        "embedding": self._embedder(r.content),
                    }
                )
            return payload

        try:
            # embedding is CPU-bound and the insert is I/O; neither belongs on
            # the event loop
            payload = await asyncio.to_thread(_embed_rows)
            await asyncio.to_thread(self._vector_store.ingest, payload)
        except Exception:  # noqa: BLE001 — search is best-effort; the SQL tier
            # remains the source of truth
            log.exception("Vector ingest failed for turn %s", turn_id)
            return 0
        return len(payload)

    def purge_vectors(
        self, *, conversation_id: str | None = None, subject: str | None = None
    ) -> int:
        """Remove a conversation's or subject's copies from the vector store.

        The vector store holds a *copy* of the content, so deleting from SQL
        alone would leave it searchable. Called from conversation delete.
        """
        if self._vector_store is None:
            return 0
        try:
            return self._vector_store.purge(
                conversation_id=conversation_id, subject=subject
            )
        except Exception:  # noqa: BLE001
            log.exception("Vector purge failed — deleted content may remain searchable")
            return 0

    async def maybe_summarize(self, conversation_id: str) -> bool:
        """Fold old turns into the rolling summary.

        Three phases, and the split is the point: **the summarizer is called
        with no transaction open.** RonDB aborts transactions that hold locks
        past short timeouts, so wrapping an LLM call in one would roll back
        under any real latency and take concurrent turns down with it. Claiming
        and committing are separately short, and ``summary_version`` is what
        makes that safe — two racing folds cannot lose rows or double-fold;
        the loser just wasted a summarizer call.
        """
        if self._summarize is None or not self._ensure_engine():
            return False
        if self._sessions is None:
            return False
        for _ in range(3):
            try:
                claim = await asyncio.to_thread(self._claim_fold, conversation_id)
            except Exception:  # noqa: BLE001
                log.exception("Could not claim a summarization batch")
                return False
            if claim is None:
                return False
            version, old_summary, turns, max_id, count = claim

            try:
                if inspect.iscoroutinefunction(self._summarize):
                    summary = await self._summarize(old_summary, turns)
                else:
                    # A sync summarizer is very likely a blocking LLM call, so
                    # it goes to a thread. Note the check is on the callable,
                    # not its result: deciding by calling it first would already
                    # have blocked the event loop for the whole request.
                    summary = await asyncio.to_thread(
                        self._summarize, old_summary, turns
                    )
                    if inspect.isawaitable(summary):
                        summary = await summary
            except Exception:  # noqa: BLE001 — a failed summary must not fail
                # the request; the same batch is retried on a later turn
                log.exception("Summarizer failed; leaving history unfolded")
                return False
            if not isinstance(summary, str) or not summary.strip():
                log.warning("Summarizer returned no text; leaving history unfolded")
                return False

            try:
                committed = await asyncio.to_thread(
                    self._commit_fold,
                    conversation_id,
                    version,
                    summary,
                    max_id,
                    count,
                )
            except Exception:  # noqa: BLE001
                log.exception("Could not commit the summary")
                return False
            if committed:
                with self._lock:
                    self._cache.pop(conversation_id, None)
                return True
            log.info("Concurrent fold won; retrying with fresh state")
        return False

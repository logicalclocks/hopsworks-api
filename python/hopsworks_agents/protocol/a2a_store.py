"""Where A2A tasks live when they must outlive a replica: the project's online database.

``AgentApp`` keeps tasks in the process by default on a laptop, and in the
project's online database (RonDB) on a deployment, through the ``a2a-sdk``'s own
SQLAlchemy stores: the versioned task store, the event log a subscription on
another replica follows, and the push-notification configs.
The tables are the platform's: the backend creates them when a deployment with
database access starts, so the stores here never issue DDL against RonDB.
A local database (SQLite, in tests and local runs) is the exception, where the
tables are created on first use.

Two things here are ours rather than the SDK's:

- the **owner** every row is filed under, which is what keeps one deployment's
  tasks, and in private mode one caller's, apart from another's;
- the **lease** that says which replica runs a task, so a task whose replica
  died is failed by someone instead of staying ``WORKING`` for ever, and which
  every write to the task locks first (``_fenced_store``);
- the event log is read by task version rather than by its sequence number
  (``_version_ordered_stream``): on RonDB with several SQL nodes, the order of
  an auto-increment is not the order in which its rows were committed.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .scope import (
    ANONYMOUS,
    PrincipalRequired,
    _header_principal,
    _principal,
)


if TYPE_CHECKING:
    from a2a.server.context import ServerCallContext


_logger = logging.getLogger(__name__)

#: The tables, in the project's online database. Created by the platform.
TASKS_TABLE = "agent_a2a_tasks"
TASK_VERSIONS_TABLE = "agent_a2a_task_versions"
TASK_EVENTS_TABLE = "agent_a2a_task_events"
PUSH_CONFIGS_TABLE = "agent_a2a_push_configs"
TASK_LEASES_TABLE = "agent_a2a_task_leases"
SPILL_CLEANUP_TABLE = "agent_a2a_spill_cleanup"
#: A task id that names a directory of spilled files, the platform's check too.
SPILL_TASK_ID = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]{0,63}$")

#: A local run's database, for tests and for trying the durable stores without
#: a cluster: any SQLAlchemy async URL. On a deployment the ``MYSQL_*``
#: variables are read instead.
DATABASE_URL_ENV = "HOPSWORKS_AGENT_A2A_DATABASE_URL"
#: The Fernet key push configs are encrypted with at rest: one per deployment,
#: shared by its replicas and the backend, set by the platform.
PUSH_KEY_ENV = "HOPSWORKS_AGENT_PUSH_KEY"
#: What separates the deployment from the caller in a private-mode owner.
OWNER_SEPARATOR = ":"
#: Where a call context made by this process names the owner outright.
OWNER_STATE_KEY = "hopsworks_owner"

#: How often the replica running a task renews its lease.
HEARTBEAT_SECONDS = 10.0
#: How many heartbeats may be missed before a lease is taken to be lost, by the
#: executor that holds it and by whoever reaps it.
MISSED_HEARTBEATS = 3


def _serving_id() -> str:
    """The deployment's immutable id, which its rows are filed under; ``local`` off the platform."""
    return os.environ.get("DEPLOYMENT_ID") or "local"


def _executor_name() -> str:
    """This replica, as a lease names it: the pod."""
    return os.environ.get("HOSTNAME") or socket.gethostname()


@dataclass(frozen=True)
class _Ownership:
    """Whose a task is: the deployment's, or in private mode one caller's of it."""

    private: bool
    on_platform: bool

    def _owner(self, context: ServerCallContext) -> str:
        """The owner every store files and filters by, from the call's headers."""
        serving = _serving_id()
        state = getattr(context, "state", None) or {}
        known = state.get(OWNER_STATE_KEY) if hasattr(state, "get") else None
        if known:
            # set by this process only (a sweep acting on a row it read): the
            # SDK fills the state from the request, never with this key
            return str(known)
        if not self.private:
            return serving
        headers = state.get("headers") if hasattr(state, "get") else None
        lowered = {str(k).lower(): v for k, v in (headers or {}).items()}
        principal = _header_principal(lowered) or _principal.get()
        if principal is None:
            if self.on_platform:
                # refused at the door already; a store reached without a
                # principal files under an owner nobody can ask for
                raise PrincipalRequired
            principal = ANONYMOUS
        return f"{serving}{OWNER_SEPARATOR}{principal}"


def _database_url() -> str | None:
    """The async SQLAlchemy URL of the database tasks are kept in, or None when there is none.

    An explicit URL wins, for tests and local runs.
    Otherwise the ``MYSQL_*`` variables a deployment with database access
    gets, the ones the memory service reads.
    """
    explicit = os.environ.get(DATABASE_URL_ENV)
    if explicit:
        return explicit
    env = os.environ
    if not (env.get("MYSQL_USER") and env.get("MYSQL_HOST") and env.get("MYSQL_DB")):
        return None
    from sqlalchemy.engine import URL  # noqa: PLC0415

    password = env.get("MYSQL_PASSWORD")
    if password is None:
        secret_name = env.get("MYSQL_PASSWORD_SECRET_NAME")
        if not secret_name:
            return None
        from .memory import _read_hopsworks_secret  # noqa: PLC0415

        try:
            password = _read_hopsworks_secret(secret_name)
        except Exception:  # noqa: BLE001 -- no password is no database, not a crash at import
            _logger.warning(
                "could not read the database password from the secret %s, so "
                "A2A tasks cannot be kept in the project's online database",
                secret_name,
                exc_info=True,
            )
            return None
    return URL.create(
        "mysql+aiomysql",
        username=env["MYSQL_USER"],
        password=password,
        host=env["MYSQL_HOST"],
        port=int(env.get("MYSQL_PORT", "3306")),
        database=env["MYSQL_DB"],
    ).render_as_string(hide_password=False)


def _platform_creates_tables(url: str) -> bool:
    """Whether the tables are the platform's to create: on RonDB, yes; on a local SQLite, no."""
    return not url.startswith("sqlite")


class _Leases:
    """Which replica runs which task, with a fence a stale replica cannot get past.

    One row per running turn.
    The replica that runs it renews ``heartbeat_at`` with the database's clock;
    whoever finds the heartbeat too old takes the lease by bumping ``fence`` in
    the same conditional update, so of two reapers one wins, and the replica
    that lost it finds no row with its fence at its next heartbeat and stops.
    After a terminal state the row stays, as the outbox of the terminal push
    notification, until that was delivered.
    """

    def __init__(self, engine: Any, create_table: bool):
        from sqlalchemy import (  # noqa: PLC0415
            BigInteger,
            Boolean,
            Column,
            DateTime,
            Index,
            Integer,
            MetaData,
            String,
            Table,
        )

        self._engine = engine
        self._create_table = create_table
        self._ready = False
        self._metadata = MetaData()
        self.table = Table(
            TASK_LEASES_TABLE,
            self._metadata,
            Column("task_id", String(36), primary_key=True),
            Column("owner", String(255), nullable=False),
            Column("deployment_id", String(64), nullable=False),
            Column("executor", String(255), nullable=False),
            Column("fence", BigInteger, nullable=False),
            Column("attempt", Integer, nullable=False, default=1),
            Column("started_at", DateTime, nullable=False),
            Column("heartbeat_at", DateTime, nullable=False),
            # set when the task reached a terminal state; the row then waits
            # for its terminal notification to be delivered
            Column("terminal_at", DateTime, nullable=True),
            Column("terminal_pushed", Boolean, nullable=False, default=False),
            # the turn runs an action a person approved: no cancel lands on it
            Column("protected", Boolean, nullable=False, default=False),
            # how many times a lost turn is run again, the author's word at acquire
            Column("retry_limit", Integer, nullable=False, default=0),
            # the platform handed a lost task back, for a replica to run again
            Column("retry_pending", Boolean, nullable=False, default=False),
            Index(
                f"idx_{TASK_LEASES_TABLE}_heartbeat", "deployment_id", "heartbeat_at"
            ),
        )

    async def _ensure(self) -> None:
        if self._ready:
            return
        if self._create_table:
            async with self._engine.begin() as conn:
                await conn.run_sync(self._metadata.create_all)
        self._ready = True

    async def _acquire(
        self, task_id: str, owner: str, retry_limit: int = 0
    ) -> tuple[int, int]:
        """Take the lease of a task this replica starts to run; the fence and the attempt it runs under.

        A row left by an earlier turn of the task (it paused, or its executor
        was lost) is taken over with a higher fence.
        ``retry_limit`` is how many times a lost turn may be run again, which
        whoever finds the turn lost reads from the row, the platform included.
        """
        from sqlalchemy import func, insert, select, update  # noqa: PLC0415
        from sqlalchemy.exc import IntegrityError  # noqa: PLC0415

        await self._ensure()
        table = self.table
        executor = _executor_name()
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    select(table.c.fence, table.c.attempt).where(
                        table.c.task_id == task_id
                    )
                )
            ).first()
            if row is None:
                try:
                    await conn.execute(
                        insert(table).values(
                            task_id=task_id,
                            owner=owner,
                            deployment_id=_serving_id(),
                            executor=executor,
                            fence=1,
                            attempt=1,
                            started_at=func.now(),
                            heartbeat_at=func.now(),
                            terminal_pushed=False,
                            protected=False,
                            retry_limit=retry_limit,
                            retry_pending=False,
                        )
                    )
                except IntegrityError:
                    row = None
                else:
                    return 1, 1
        # the row exists (or appeared under us): take it with the next fence
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    select(table.c.fence, table.c.attempt).where(
                        table.c.task_id == task_id
                    )
                )
            ).first()
            fence = int(row.fence) + 1 if row is not None else 1
            attempt = int(row.attempt) if row is not None else 1
            await conn.execute(
                update(table)
                .where(table.c.task_id == task_id)
                .values(
                    owner=owner,
                    executor=executor,
                    fence=fence,
                    started_at=func.now(),
                    heartbeat_at=func.now(),
                    terminal_at=None,
                    terminal_pushed=False,
                    protected=False,
                    retry_limit=retry_limit,
                    retry_pending=False,
                )
            )
            return fence, attempt

    async def _heartbeat(self, task_id: str, fence: int) -> bool:
        """Renew the lease; False when it is no longer this replica's."""
        from sqlalchemy import func, update  # noqa: PLC0415

        table = self.table
        async with self._engine.begin() as conn:
            result = await conn.execute(
                update(table)
                .where(
                    table.c.task_id == task_id,
                    table.c.executor == _executor_name(),
                    table.c.fence == fence,
                    table.c.terminal_at.is_(None),
                )
                .values(heartbeat_at=func.now())
            )
            return bool(result.rowcount)

    async def _release(self, task_id: str, fence: int) -> None:
        """The turn ended without a terminal state (it paused): nothing left to own."""
        from sqlalchemy import delete  # noqa: PLC0415

        table = self.table
        async with self._engine.begin() as conn:
            await conn.execute(
                delete(table).where(
                    table.c.task_id == task_id,
                    table.c.executor == _executor_name(),
                    table.c.fence == fence,
                )
            )

    async def _settle(self, task_id: str, fence: int, *, pushed: bool) -> None:
        """The task reached a terminal state.

        The row is deleted when nothing waits on a terminal notification, and
        kept as its outbox otherwise, for the backend to deliver from if this
        replica does not.
        """
        from sqlalchemy import delete, func, update  # noqa: PLC0415

        table = self.table
        mine = (
            table.c.task_id == task_id,
            table.c.executor == _executor_name(),
            table.c.fence == fence,
        )
        async with self._engine.begin() as conn:
            if pushed:
                await conn.execute(delete(table).where(*mine))
            else:
                await conn.execute(
                    update(table)
                    .where(*mine)
                    .values(terminal_at=func.now(), terminal_pushed=False)
                )

    async def _pushed(self, task_id: str) -> None:
        """The terminal notification was delivered: the outbox row has done its work."""
        from sqlalchemy import delete  # noqa: PLC0415

        table = self.table
        async with self._engine.begin() as conn:
            await conn.execute(
                delete(table).where(
                    table.c.task_id == task_id, table.c.terminal_at.is_not(None)
                )
            )

    def _stale(self) -> Any:
        """The test for a lease nobody renews, on the database's clock."""
        from sqlalchemy import func, text  # noqa: PLC0415

        window = int(HEARTBEAT_SECONDS * MISSED_HEARTBEATS)
        if self._engine.dialect.name == "sqlite":
            return self.table.c.heartbeat_at < func.datetime(
                "now", text(f"'-{window} seconds'")
            )
        return self.table.c.heartbeat_at < func.date_sub(
            func.now(), text(f"INTERVAL {window} SECOND")
        )

    async def _stale_rows(self, limit: int = 20) -> list[Any]:
        """The leases of this deployment that look lost; each must still be taken."""
        from sqlalchemy import false, select  # noqa: PLC0415

        await self._ensure()
        table = self.table
        async with self._engine.begin() as conn:
            return list(
                (
                    await conn.execute(
                        select(
                            table.c.task_id,
                            table.c.owner,
                            table.c.fence,
                            table.c.attempt,
                            table.c.executor,
                            table.c.retry_limit,
                        )
                        .where(
                            table.c.deployment_id == _serving_id(),
                            table.c.terminal_at.is_(None),
                            # a task the platform handed back is taken as such
                            table.c.retry_pending == false(),
                            self._stale(),
                        )
                        .limit(limit)
                    )
                ).all()
            )

    async def _take(self, row: Any) -> dict[str, Any] | None:
        """Take one lease that looked lost; None when it was renewed or taken since it was read.

        One conditional update decides it: the fence must be the one read and
        the heartbeat must still be stale now, so a heartbeat that landed
        since the scan, which leaves the fence as it was, keeps the lease.
        """
        from sqlalchemy import false, func, update  # noqa: PLC0415

        table = self.table
        async with self._engine.begin() as conn:
            result = await conn.execute(
                update(table)
                .where(
                    table.c.task_id == row.task_id,
                    table.c.fence == row.fence,
                    table.c.terminal_at.is_(None),
                    table.c.retry_pending == false(),
                    self._stale(),
                )
                .values(
                    executor=_executor_name(),
                    fence=int(row.fence) + 1,
                    attempt=int(row.attempt) + 1,
                    protected=False,
                    heartbeat_at=func.now(),
                )
            )
        if not result.rowcount:
            return None
        return {
            "task_id": row.task_id,
            "owner": row.owner,
            "fence": int(row.fence) + 1,
            "attempt": int(row.attempt) + 1,
            "retry_limit": int(row.retry_limit or 0),
            "lost_executor": row.executor,
            "handed_back": False,
        }

    async def _take_lost(self, limit: int = 20) -> list[dict[str, Any]]:
        """Take the leases of this deployment whose replica stopped renewing them; each comes back to the one caller that won it."""
        taken = []
        for row in await self._stale_rows(limit):
            lease = await self._take(row)
            if lease is not None:
                taken.append(lease)
        return taken

    async def _take_handed_back(self, limit: int = 20) -> list[dict[str, Any]]:
        """Take the tasks of this deployment the platform handed back to be run again; each comes back to the one caller that won it.

        The platform found the turn lost and its retries not used up, and
        left the row for a replica (``retry_pending``); it counted the attempt
        already, so the take leaves ``attempt`` as it is.
        """
        from sqlalchemy import func, select, true, update  # noqa: PLC0415

        await self._ensure()
        table = self.table
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(
                    select(
                        table.c.task_id,
                        table.c.owner,
                        table.c.fence,
                        table.c.attempt,
                        table.c.executor,
                        table.c.retry_limit,
                    )
                    .where(
                        table.c.deployment_id == _serving_id(),
                        table.c.retry_pending == true(),
                        table.c.terminal_at.is_(None),
                    )
                    .limit(limit)
                )
            ).all()
        taken = []
        for row in rows:
            async with self._engine.begin() as conn:
                result = await conn.execute(
                    update(table)
                    .where(
                        table.c.task_id == row.task_id,
                        table.c.fence == row.fence,
                        table.c.retry_pending == true(),
                    )
                    .values(
                        executor=_executor_name(),
                        fence=int(row.fence) + 1,
                        retry_pending=False,
                        # the run starts afresh: it protects itself again
                        # if it claims an approved action
                        protected=False,
                        heartbeat_at=func.now(),
                    )
                )
            if not result.rowcount:
                # another replica took it first
                continue
            taken.append(
                {
                    "task_id": row.task_id,
                    "owner": row.owner,
                    "fence": int(row.fence) + 1,
                    "attempt": int(row.attempt),
                    "retry_limit": int(row.retry_limit or 0),
                    "lost_executor": row.executor,
                    "handed_back": True,
                }
            )
        return taken


class _Fence:
    """A replica writes to a task only while it holds the task's lease.

    A reaper that took a lease bumps its fence, and fails the task under a
    version check; between the two, the replica that lost the lease could
    still advance the task and make that check fail.
    So every save by a replica that runs the task locks the lease row first,
    in the save's own transaction, and is refused unless the row still names
    this replica and the fence the task is held under (``_fenced_store``).
    What is kept here is only which fence that is, and which tasks this
    replica knows it lost, which it writes nothing more to at all.
    Writes to a task this replica does not run (a cancel that arrived here,
    a reaper's own write) are not its executor's, and are checked only
    against the lease's ``protected`` flag.
    """

    def __init__(self) -> None:
        #: The fence each task this replica runs is held under.
        self.held: dict[str, int] = {}
        #: Tasks whose lease this replica lost: nothing more is written to them from here.
        self.out: set[str] = set()

    def _lost(self, task_id: str) -> None:
        """The lease of a task is no longer this replica's."""
        self.held.pop(task_id, None)
        self.out.add(task_id)


class _Cleanups:
    """The spilled files a deleted task left behind, until they are removed.

    Whoever deletes a task's rows files a cleanup row in the same
    transaction, then removes the task's directory and deletes the row; a
    row left by a removal that failed is retried by the replica's sweep, and
    by the platform's when no replica is left.
    A row whose ``task_id`` is empty stands for a whole deployment's files,
    the platform's when it forgets a deployment; a replica leaves those alone.
    """

    #: How many times a removal is tried before the row is given up on.
    ATTEMPTS = 20

    def __init__(self, engine: Any, create_table: bool):
        from sqlalchemy import (  # noqa: PLC0415
            Column,
            DateTime,
            Integer,
            MetaData,
            String,
            Table,
        )

        self._engine = engine
        self._create_table = create_table
        self._ready = False
        self._metadata = MetaData()
        self.table = Table(
            SPILL_CLEANUP_TABLE,
            self._metadata,
            Column("deployment_id", String(64), primary_key=True),
            Column("task_id", String(64), primary_key=True),
            Column("requested_at", DateTime, nullable=False),
            Column("attempts", Integer, nullable=False, default=0),
        )

    async def _ensure(self) -> None:
        if self._ready:
            return
        if self._create_table:
            async with self._engine.begin() as conn:
                await conn.run_sync(self._metadata.create_all)
        self._ready = True

    def _file(self, task_id: str) -> Any:
        """The statement that files a cleanup for a task, once: a second request for the same task is the same request."""
        from sqlalchemy import func, insert  # noqa: PLC0415

        statement = insert(self.table).values(
            deployment_id=_serving_id(),
            task_id=task_id,
            requested_at=func.now(),
            attempts=0,
        )
        if self._engine.dialect.name == "sqlite":
            return statement.prefix_with("OR IGNORE")
        return statement.prefix_with("IGNORE")

    async def _done(self, task_id: str) -> None:
        """The task's files are gone: so is its row."""
        from sqlalchemy import delete  # noqa: PLC0415

        table = self.table
        async with self._engine.begin() as conn:
            await conn.execute(
                delete(table).where(
                    table.c.deployment_id == _serving_id(), table.c.task_id == task_id
                )
            )

    async def _failed(self, task_id: str) -> int:
        """A removal failed: counted on the row; how many have failed now."""
        from sqlalchemy import select, update  # noqa: PLC0415

        table = self.table
        mine = (table.c.deployment_id == _serving_id(), table.c.task_id == task_id)
        async with self._engine.begin() as conn:
            await conn.execute(
                update(table).where(*mine).values(attempts=table.c.attempts + 1)
            )
            attempts = (
                await conn.execute(select(table.c.attempts).where(*mine))
            ).scalar_one_or_none()
        return int(attempts or 0)

    async def _due(self, limit: int = 50) -> list[str]:
        """The tasks of this deployment whose files are still to be removed, oldest first."""
        from sqlalchemy import select  # noqa: PLC0415

        await self._ensure()
        table = self.table
        async with self._engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(table.c.task_id)
                    .where(
                        table.c.deployment_id == _serving_id(), table.c.task_id != ""
                    )
                    .order_by(table.c.requested_at)
                    .limit(limit)
                )
            ).all()
        return [str(row.task_id) for row in rows]


_MODELS: dict[str, Any] = {}


def _models() -> dict[str, Any]:
    """The SDK's table models under the platform's table names, declared once per process."""
    if not _MODELS:
        from a2a.server.models import (  # noqa: PLC0415
            create_push_notification_config_model,
            create_task_event_model,
            create_task_model,
            create_task_version_model,
        )
        from sqlalchemy.orm import DeclarativeBase  # noqa: PLC0415

        class _Base(DeclarativeBase):
            """Our own registry, so nothing here meets the SDK's default models."""

        _MODELS.update(
            tasks=create_task_model(TASKS_TABLE, _Base),
            events=create_task_event_model(TASK_EVENTS_TABLE, _Base),
            versions=create_task_version_model(TASK_VERSIONS_TABLE, _Base),
            push=create_push_notification_config_model(PUSH_CONFIGS_TABLE, _Base),
        )
    return _MODELS


_TYPES: dict[str, type] = {}


def _fenced_store() -> type:
    """The SDK's versioned task store, with every write made under the task's lease.

    Every transaction that writes a task locks, in this order, the lease row
    (``SELECT ... FOR UPDATE``), the version row, then the task row; the
    platform's own writes to a task take the same order.
    In that transaction:

    - a save by the replica that runs the task (it holds a fence) is refused
      unless the lease row still names this replica and that fence, so a
      lease taken between a check and the write cannot be written past;
      a lease that cannot be read refuses the save;
    - a ``CANCELED`` save, from this replica or any other, is refused while
      the lease row says the turn runs an action a person approved
      (``protected``).

    A refusal is the SDK's ``ConcurrentTaskModificationError``, which a remote
    cancel reports as ``TaskNotCancelableError``.
    The body of ``_save_once`` is the SDK's (a2a-sdk 1.3), with the lease
    locked first.
    """
    if "store" in _TYPES:
        return _TYPES["store"]
    import json  # noqa: PLC0415

    from a2a.server.cluster import (  # noqa: PLC0415
        ConcurrentTaskModificationError,
        VersionedDatabaseTaskStore,
    )
    from a2a.server.cluster.database_task_store import (  # noqa: PLC0415
        _as_int,
        _is_transient,
    )
    from a2a.server.cluster.version import TaskVersion  # noqa: PLC0415
    from a2a.types import TaskState  # noqa: PLC0415
    from a2a.utils.constants import TERMINAL_TASK_STATES  # noqa: PLC0415
    from a2a.utils.proto_utils import to_stream_response  # noqa: PLC0415
    from sqlalchemy import and_, delete, insert, select, update  # noqa: PLC0415
    from sqlalchemy.exc import DBAPIError  # noqa: PLC0415

    class _FencedTaskStore(VersionedDatabaseTaskStore):
        def __init__(self, engine: Any, leases: _Leases, fence: _Fence, **kwargs: Any):
            super().__init__(engine, **kwargs)
            self._leases = leases
            self._fence = fence
            #: Where a deleted task's spilled files are filed for removal; None when its files need no row.
            self.cleanups: _Cleanups | None = None

        async def save(
            self,
            task: Any,
            *,
            event: Any = None,
            prev: Any = None,
            prev_version: Any,
            context: Any,
            fence: int | None = None,
        ) -> Any:
            """Persist the task under its lease; ``fence`` names the lease a reaper's own write is made under."""
            del prev
            if fence is None and task.id in self._fence.out:
                # this replica knows it lost the task: not even asked
                raise ConcurrentTaskModificationError(task.id)
            await self._db._ensure_initialized()  # noqa: SLF001
            await self._ensure_cluster_tables()
            await self._leases._ensure()
            owner = self._db.owner_resolver(context)
            attempts = 0
            while True:
                attempts += 1
                try:
                    return await self._save_once(
                        task,
                        event=event,
                        prev_version=prev_version,
                        owner=owner,
                        fence=fence,
                    )
                except DBAPIError as e:
                    if not _is_transient(e) or attempts >= self._max_attempts:
                        raise
                    await asyncio.sleep(self._retry_delay_s * attempts)

        async def _lease(self, session: Any, task_id: str) -> Any:
            """Lock the task's lease row, the first lock of every task write; the row, or None when no turn holds the task."""
            table = self._leases.table
            if self._db.engine.dialect.name == "sqlite":
                # SQLite has no row locks: a write takes its database lock,
                # which stands for the row's here
                await session.execute(
                    update(table)
                    .where(table.c.task_id == task_id)
                    .values(fence=table.c.fence)
                )
            return (
                await session.execute(
                    select(table.c.executor, table.c.fence, table.c.protected)
                    .where(table.c.task_id == task_id)
                    .with_for_update()
                )
            ).first()

        async def _save_once(
            self,
            task: Any,
            *,
            event: Any,
            prev_version: Any,
            owner: str,
            fence: int | None = None,
        ) -> Any:
            model = self._db._to_orm(task, owner)  # noqa: SLF001
            async with self._db.async_session_maker.begin() as session:
                lease = await self._lease(session, task.id)
                held = fence if fence is not None else self._fence.held.get(task.id)
                if held is not None and (
                    lease is None
                    or lease.executor != _executor_name()
                    or int(lease.fence) != held
                ):
                    if fence is None:
                        self._fence._lost(task.id)
                    raise ConcurrentTaskModificationError(task.id)
                if (
                    task.status.state == TaskState.TASK_STATE_CANCELED
                    and lease is not None
                    and lease.protected
                ):
                    # the turn runs an action a person approved
                    raise ConcurrentTaskModificationError(task.id)

                # from here, the SDK's own save
                if task.status.state == TaskState.TASK_STATE_CANCELED:
                    stored = await self._lock_version(session, task.id, owner)
                    current = await self._current_task(session, task.id, owner)
                    if current is None or current.status.state in TERMINAL_TASK_STATES:
                        raise ConcurrentTaskModificationError(task.id)
                    new_version = (stored or 0) + 1
                    if stored is None:
                        await self._insert_version(session, task.id, owner, new_version)
                    else:
                        await session.execute(
                            update(self._version_model)
                            .where(
                                and_(
                                    self._version_model.task_id == task.id,
                                    self._version_model.owner == owner,
                                )
                            )
                            .values(version=new_version)
                        )
                elif prev_version.is_missing:
                    new_version = 1
                    await self._insert_version(session, task.id, owner, new_version)
                else:
                    new_version = _as_int(prev_version) + 1
                    result = await session.execute(
                        update(self._version_model)
                        .where(
                            and_(
                                self._version_model.task_id == task.id,
                                self._version_model.owner == owner,
                                self._version_model.version == _as_int(prev_version),
                            )
                        )
                        .values(version=new_version)
                    )
                    if result.rowcount == 0:
                        raise ConcurrentTaskModificationError(task.id)

                await session.merge(model)

                if event is not None:
                    await session.execute(
                        insert(self._event_model).values(
                            task_id=task.id,
                            owner=owner,
                            task_version=new_version,
                            event_data=to_stream_response(event).SerializeToString(),
                        )
                    )

            return TaskVersion(new_version)

        async def _protect(self, task_id: str, fence: int) -> bool:
            """Before a resumed turn runs the action a person approved: make the task one no cancel lands on.

            One transaction locks the lease row, which must still be this
            replica's under ``fence``, reads the task's state, and sets
            ``protected`` only when the task has not ended; False when it has
            (a cancel landed first) or the lease is gone, and the action must
            then not run.
            """
            await self._leases._ensure()
            table = self._leases.table
            model = self._db.task_model
            async with self._db.async_session_maker.begin() as session:
                lease = await self._lease(session, task_id)
                if (
                    lease is None
                    or lease.executor != _executor_name()
                    or int(lease.fence) != fence
                ):
                    return False
                status = (
                    await session.execute(
                        select(model.status).where(model.id == task_id)
                    )
                ).scalar_one_or_none()
                if isinstance(status, str):
                    status = json.loads(status)
                state = status.get("state") if isinstance(status, dict) else None
                if state is not None and TaskState.Value(state) in TERMINAL_TASK_STATES:
                    return False
                await session.execute(
                    update(table)
                    .where(table.c.task_id == task_id, table.c.fence == fence)
                    .values(protected=True)
                )
            return True

        async def delete(self, task_id: str, context: Any) -> bool:
            """Delete the caller's task with its version row; whether there was one.

            When the task's files were spilled to the project, a cleanup row is
            filed in the same transaction, so the files are removed even if
            this replica is not the one that gets to it.
            """
            await self._db._ensure_initialized()  # noqa: SLF001
            await self._ensure_cluster_tables()
            cleanups = self.cleanups
            if cleanups is not None:
                await cleanups._ensure()
            owner = self._db.owner_resolver(context)
            model = self._db.task_model
            await self._leases._ensure()
            async with self._db.async_session_maker.begin() as session:
                # the same lock order as a save: lease, version, task
                await self._lease(session, task_id)
                await self._lock_version(session, task_id, owner)
                result = await session.execute(
                    delete(model).where(model.id == task_id, model.owner == owner)
                )
                if not result.rowcount:
                    # not the caller's: nothing of it is touched
                    return False
                await session.execute(
                    delete(self._version_model).where(
                        self._version_model.task_id == task_id,
                        self._version_model.owner == owner,
                    )
                )
                if cleanups is not None and SPILL_TASK_ID.match(task_id):
                    # a task whose id names no directory had nothing spilled
                    await session.execute(cleanups._file(task_id))
            return True

    _TYPES["store"] = _FencedTaskStore
    return _FencedTaskStore


def _version_ordered_stream() -> type:
    """The SDK's database event stream, read by task version instead of by sequence number.

    The SDK follows the log by its auto-increment ``seq``; on RonDB with
    several SQL nodes, a lower ``seq`` can be committed after a higher one was
    read, and its event is then skipped for good.
    A version is assigned under the version row's compare-and-swap, so once
    version v is visible, every lower version's transaction has committed,
    whichever SQL node wrote it: polling ``task_version > <last yielded>``
    misses nothing.
    The rest is the SDK's: owner filter, retry on a transient error, poll
    interval, and the meaning of ``after``.
    """
    if "stream" in _TYPES:
        return _TYPES["stream"]
    from a2a.server.cluster import DatabaseTaskEventStream  # noqa: PLC0415
    from a2a.server.cluster.database_event_stream import (  # noqa: PLC0415
        _as_int,
        stream_response_to_event,
    )
    from a2a.server.cluster.event_stream import VersionedEvent  # noqa: PLC0415
    from a2a.server.cluster.version import TaskVersion  # noqa: PLC0415
    from a2a.server.context import ServerCallContext  # noqa: PLC0415
    from a2a.types.a2a_pb2 import StreamResponse  # noqa: PLC0415
    from sqlalchemy import and_, select  # noqa: PLC0415
    from sqlalchemy.exc import OperationalError  # noqa: PLC0415

    class _VersionOrderedStream(DatabaseTaskEventStream):
        async def subscribe(  # type: ignore[override]
            self,
            task_id: str,
            *,
            after: Any,
            context: Any = None,
        ) -> Any:
            await self._ensure_initialized()
            owner = (
                self.owner_resolver(context or ServerCallContext())
                if self.owner_resolver is not None
                else None
            )
            model = self._event_model
            cursor = 0 if after.is_missing else _as_int(after)
            while True:
                conditions = [model.task_id == task_id, model.task_version > cursor]
                if owner is not None:
                    conditions.append(model.owner == owner)
                try:
                    async with self._session_maker() as session:
                        rows = (
                            await session.execute(
                                select(model.task_version, model.event_data)
                                .where(and_(*conditions))
                                .order_by(model.task_version.asc(), model.seq.asc())
                            )
                        ).all()
                except OperationalError:
                    _logger.debug(
                        "transient database error polling the events of %s; retrying",
                        task_id,
                        exc_info=True,
                    )
                    await asyncio.sleep(self._poll_interval_s)
                    continue
                for task_version, event_data in rows:
                    cursor = int(task_version)
                    version = TaskVersion(cursor)
                    if not version.is_after(after):
                        continue
                    response = StreamResponse()
                    response.ParseFromString(event_data)
                    yield VersionedEvent(
                        event=stream_response_to_event(response), version=version
                    )
                if not rows:
                    await asyncio.sleep(self._poll_interval_s)

    _TYPES["stream"] = _VersionOrderedStream
    return _VersionOrderedStream


class _DurableStores:
    """The SDK's database stores over one engine, under our table names and owner."""

    def __init__(self, url: str, ownership: _Ownership):
        from a2a.server.tasks.database_push_notification_config_store import (  # noqa: PLC0415
            DatabasePushNotificationConfigStore,
        )
        from sqlalchemy.ext.asyncio import create_async_engine  # noqa: PLC0415

        self.url = url
        create = not _platform_creates_tables(url)
        options: dict[str, Any] = {}
        if url.startswith("sqlite"):
            # A local file: no pool, so no connection outlives its use. A
            # pooled SQLite connection keeps a thread that a process which
            # never ran the app's shutdown would wait on at exit.
            from sqlalchemy.pool import NullPool  # noqa: PLC0415

            options.update(poolclass=NullPool)
        else:
            # A replica holds few connections; RonDB's MySQL servers are shared.
            # Connections are recycled young rather than pinged before use: the
            # pool's ping does not work with the aiomysql build hopsworks
            # ships, and the stores retry the one statement a dropped
            # connection fails.
            options.update(pool_size=5, max_overflow=5, pool_recycle=300)
        self.engine = create_async_engine(url, **options)
        # The SDK's stores declare a model per table name on one shared
        # registry, and so cannot be built twice under our names (the event
        # table alone is declared by two of them). They are built on their
        # default models, which declares nothing, and handed ours.
        models = _models()
        self.leases = _Leases(self.engine, create_table=create)
        self.fence = _Fence()
        self.cleanups = _Cleanups(self.engine, create_table=create)
        self.task_store = _fenced_store()(
            self.engine,
            self.leases,
            self.fence,
            create_table=create,
            owner_resolver=ownership._owner,
        )
        self.task_store._db.task_model = models["tasks"]
        self.task_store._event_model = models["events"]
        self.task_store._version_model = models["versions"]
        self.event_stream = _version_ordered_stream()(
            self.engine, create_table=create, owner_resolver=ownership._owner
        )
        self.event_stream._event_model = models["events"]
        self.push_configs = DatabasePushNotificationConfigStore(
            self.engine,
            create_table=create,
            encryption_key=os.environ.get(PUSH_KEY_ENV) or None,
            owner_resolver=ownership._owner,
        )
        self.push_configs.config_model = models["push"]

    async def _tables_exist(self) -> bool:
        """Whether the tables are there to be used: created here for a local database, by the platform otherwise."""
        from sqlalchemy import select  # noqa: PLC0415

        try:
            await self.task_store.initialize()
            await self.leases._ensure()
            await self.cleanups._ensure()
            model = self.task_store.as_task_store.task_model
            async with self.engine.connect() as conn:
                await conn.execute(select(model.id).limit(1))
                await conn.execute(select(self.leases.table.c.task_id).limit(1))
        except Exception:  # noqa: BLE001 -- said by the caller
            _logger.debug("the A2A task tables could not be read", exc_info=True)
            return False
        return True

    async def _state(self, task_id: str) -> str | None:
        """A task's stored state, by name, read without its history and artifacts."""
        from sqlalchemy import select  # noqa: PLC0415

        model = self.task_store.as_task_store.task_model
        async with self.engine.connect() as conn:
            status = (
                await conn.execute(select(model.status).where(model.id == task_id))
            ).scalar_one_or_none()
        if isinstance(status, str):
            import json  # noqa: PLC0415

            status = json.loads(status)
        return status.get("state") if isinstance(status, dict) else None

    async def _close(self) -> None:
        await self.engine.dispose()

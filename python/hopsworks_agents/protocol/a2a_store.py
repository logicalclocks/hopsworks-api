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
  died is failed by someone instead of staying ``WORKING`` for ever.
"""

from __future__ import annotations

import logging
import os
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

        password = _read_hopsworks_secret(secret_name)
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

    async def _acquire(self, task_id: str, owner: str) -> tuple[int, int]:
        """Take the lease of a task this replica starts to run; the fence and the attempt it runs under.

        A row left by an earlier turn of the task (it paused, or its executor
        was lost) is taken over with a higher fence.
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

    async def _take_lost(self, limit: int = 20) -> list[dict[str, Any]]:
        """Take the leases of this deployment whose replica stopped renewing them.

        Each is taken with the fence-bumping update, so a row comes back only
        to the one caller that won it.
        The staleness test uses the database's clock on both sides.
        """
        from sqlalchemy import func, select, update  # noqa: PLC0415

        await self._ensure()
        table = self.table
        window = HEARTBEAT_SECONDS * MISSED_HEARTBEATS
        dialect = self._engine.dialect.name
        if dialect == "sqlite":
            from sqlalchemy import text  # noqa: PLC0415

            stale = table.c.heartbeat_at < func.datetime(
                "now", text(f"'-{int(window)} seconds'")
            )
        else:
            from sqlalchemy import text  # noqa: PLC0415

            stale = table.c.heartbeat_at < func.date_sub(
                func.now(), text(f"INTERVAL {int(window)} SECOND")
            )
        me = _executor_name()
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(
                    select(
                        table.c.task_id,
                        table.c.owner,
                        table.c.fence,
                        table.c.attempt,
                        table.c.executor,
                    )
                    .where(
                        table.c.deployment_id == _serving_id(),
                        table.c.terminal_at.is_(None),
                        stale,
                    )
                    .limit(limit)
                )
            ).all()
        taken: list[dict[str, Any]] = []
        for row in rows:
            async with self._engine.begin() as conn:
                result = await conn.execute(
                    update(table)
                    .where(
                        table.c.task_id == row.task_id,
                        table.c.fence == row.fence,
                        table.c.terminal_at.is_(None),
                    )
                    .values(
                        executor=me,
                        fence=int(row.fence) + 1,
                        attempt=int(row.attempt) + 1,
                        heartbeat_at=func.now(),
                    )
                )
            if result.rowcount:
                taken.append(
                    {
                        "task_id": row.task_id,
                        "owner": row.owner,
                        "fence": int(row.fence) + 1,
                        "attempt": int(row.attempt) + 1,
                        "lost_executor": row.executor,
                    }
                )
        return taken


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


class _DurableStores:
    """The SDK's database stores over one engine, under our table names and owner."""

    def __init__(self, url: str, ownership: _Ownership):
        from a2a.server.cluster import (  # noqa: PLC0415
            DatabaseTaskEventStream,
            VersionedDatabaseTaskStore,
        )
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
        self.task_store = VersionedDatabaseTaskStore(
            self.engine, create_table=create, owner_resolver=ownership._owner
        )
        self.task_store._db.task_model = models["tasks"]
        self.task_store._event_model = models["events"]
        self.task_store._version_model = models["versions"]
        self.event_stream = DatabaseTaskEventStream(
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
        self.leases = _Leases(self.engine, create_table=create)

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

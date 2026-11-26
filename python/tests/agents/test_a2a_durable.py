"""A2A tasks that outlive a replica: the database stores, private mode, and the lease.

Two ``AgentApp`` instances over one database stand for two replicas of a
deployment.
The database is SQLite here; the same stores run on the project's online
database on a deployment.
"""

import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import (
    CancelTaskRequest,
    GetTaskRequest,
    ListTasksRequest,
    Message,
    Part,
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    TaskState,
)
from a2a.utils.errors import TaskNotFoundError
from fastapi.testclient import TestClient
from google.protobuf.json_format import MessageToDict
from hopsworks_agents.protocol import AgentApp, InMemoryAgentMemory, a2a_store
from hopsworks_agents.protocol.a2a import A2AConfig


BASE = "http://agent.test"


def run(coro):
    return asyncio.run(coro)


#: A MySQL to run this module against instead of SQLite, as an async SQLAlchemy
#: URL (``mysql+aiomysql://user:password@host:port/database``). The tables are
#: dropped and created from the platform's definitions, which is what checks
#: those definitions against the stores.
MYSQL_URL_ENV = "A2A_TEST_MYSQL_URL"

# The platform's tables (hopsworks-ee A2ATables), on the engine a plain MySQL has.
# RonDB allows three JSON columns in a table's default layout, so the two that
# grow, artifacts and history, are LONGTEXT; the SDK writes JSON text either way.
MYSQL_TABLES = {
    "agent_a2a_tasks": """
        `id` VARCHAR(36) NOT NULL,
        `context_id` VARCHAR(255) NOT NULL,
        `kind` VARCHAR(16) NOT NULL DEFAULT 'task',
        `owner` VARCHAR(255) NULL,
        `last_updated` DATETIME(6) NULL,
        `status` JSON NOT NULL,
        `artifacts` LONGTEXT NULL,
        `history` LONGTEXT NULL,
        `protocol_version` VARCHAR(16) NULL,
        `metadata` JSON NULL,
        PRIMARY KEY (`id`),
        KEY `idx_agent_a2a_tasks_owner_last_updated` (`owner`, `last_updated`)""",
    "agent_a2a_task_versions": """
        `task_id` VARCHAR(36) NOT NULL,
        `owner` VARCHAR(255) NULL,
        `version` BIGINT NOT NULL,
        PRIMARY KEY (`task_id`)""",
    "agent_a2a_task_events": """
        `seq` BIGINT NOT NULL AUTO_INCREMENT,
        `task_id` VARCHAR(36) NOT NULL,
        `owner` VARCHAR(255) NULL,
        `task_version` BIGINT NOT NULL,
        `event_data` LONGBLOB NOT NULL,
        PRIMARY KEY (`seq`),
        UNIQUE KEY `uk_agent_a2a_task_events_version` (`task_id`, `task_version`)""",
    "agent_a2a_push_configs": """
        `task_id` VARCHAR(36) NOT NULL,
        `config_id` VARCHAR(255) NOT NULL,
        `config_data` BLOB NOT NULL,
        `owner` VARCHAR(255) NULL,
        `protocol_version` VARCHAR(16) NULL,
        PRIMARY KEY (`task_id`, `config_id`),
        KEY `idx_agent_a2a_push_configs_owner` (`owner`)""",
    "agent_a2a_task_leases": """
        `task_id` VARCHAR(36) NOT NULL,
        `owner` VARCHAR(255) NOT NULL,
        `deployment_id` VARCHAR(64) NOT NULL,
        `executor` VARCHAR(255) NOT NULL,
        `fence` BIGINT NOT NULL,
        `attempt` INT NOT NULL DEFAULT 1,
        `started_at` DATETIME NOT NULL,
        `heartbeat_at` DATETIME NOT NULL,
        `terminal_at` DATETIME NULL,
        `terminal_pushed` TINYINT(1) NOT NULL DEFAULT 0,
        `protected` TINYINT(1) NOT NULL DEFAULT 0,
        `retry_limit` INT NOT NULL DEFAULT 0,
        `retry_pending` TINYINT(1) NOT NULL DEFAULT 0,
        PRIMARY KEY (`task_id`),
        KEY `idx_agent_a2a_task_leases_heartbeat` (`deployment_id`, `heartbeat_at`)""",
    "agent_a2a_spill_cleanup": """
        `deployment_id` VARCHAR(64) NOT NULL,
        `task_id` VARCHAR(64) NOT NULL,
        `requested_at` DATETIME NOT NULL,
        `attempts` INT NOT NULL DEFAULT 0,
        `location` VARCHAR(16) NOT NULL DEFAULT 'project',
        PRIMARY KEY (`deployment_id`, `task_id`)""",
}


def _fresh_mysql(url):
    from sqlalchemy import create_engine, text

    engine = create_engine(url.replace("+aiomysql", "+pymysql"))
    with engine.begin() as conn:
        for table, columns in MYSQL_TABLES.items():
            conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
            conn.execute(text(f"CREATE TABLE `{table}` ({columns}) ENGINE = InnoDB"))
    engine.dispose()


@pytest.fixture
def database(tmp_path, monkeypatch):
    """One database for the test's replicas: SQLite, or the MySQL the environment names."""
    import os

    url = os.environ.get(MYSQL_URL_ENV)
    if url:
        _fresh_mysql(url)
    else:
        url = f"sqlite+aiosqlite:///{tmp_path / 'a2a.db'}"
    monkeypatch.setenv(a2a_store.DATABASE_URL_ENV, url)
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    return url


async def change_leases(app, **values):
    """Rewrite every lease row, as something outside this replica would."""
    from sqlalchemy import update

    leases = app._a2a.stores.leases
    async with app._a2a.stores.engine.begin() as conn:
        await conn.execute(update(leases.table).values(**values))


async def connect(app, *, headers=None, streaming=False):
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE, headers=headers or {}
    )
    card = await A2ACardResolver(http, BASE).get_agent_card()
    config = ClientConfig(httpx_client=http, streaming=streaming)
    return ClientFactory(config).create(card)


def user_message(text, *, context_id=None, task_id=None):
    message = Message(
        role=Role.ROLE_USER,
        message_id=f"msg-{abs(hash((text, context_id, task_id))) % 10**8}",
        parts=[Part(text=text)],
    )
    if context_id:
        message.context_id = context_id
    if task_id:
        message.task_id = task_id
    return message


async def send(client, message, *, return_immediately=False):
    request = SendMessageRequest(
        message=message,
        configuration=SendMessageConfiguration(return_immediately=return_immediately),
    )
    events = [event async for event in client.send_message(request)]
    assert len(events) == 1 and events[0].HasField("task"), events
    return events[0].task


def state_name(task):
    return TaskState.Name(task.status.state)


def text_of(message):
    return "".join(p.text for p in message.parts if p.WhichOneof("content") == "text")


def hopsworks_of(message):
    return (MessageToDict(message).get("metadata") or {}).get("hopsworks") or {}


def replica(handler=None, **kwargs):
    app = AgentApp(name="Echo agent", description="echoes", **kwargs)

    async def echo(request, ctx):
        return f"echo: {request.text}"

    app.chat(handler or echo)
    return app


async def lease_rows(app):
    from sqlalchemy import select

    leases = app._a2a.stores.leases
    await leases._ensure()
    async with app._a2a.stores.engine.connect() as conn:
        return (await conn.execute(select(leases.table))).all()


async def stored_as_working(client, task_id):
    """Wait until the task's WORKING state is in the store: the handler can start before it is saved."""

    async def working():
        current = await client.get_task(GetTaskRequest(id=task_id))
        return state_name(current) == "TASK_STATE_WORKING"

    await wait_for(working)


async def wait_for(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.02)
    raise AssertionError("the condition never held")


class TestWhereTasksAreKept:
    def test_a_local_run_keeps_tasks_in_the_process(self, monkeypatch):
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        app = replica()
        assert app._a2a.mode == "memory"
        assert app._a2a.stores is None

    def test_startup_says_when_the_platforms_tables_are_missing(
        self, tmp_path, monkeypatch, caplog
    ):
        from hopsworks_agents.protocol import a2a_store as store

        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        monkeypatch.setenv(
            store.DATABASE_URL_ENV, f"sqlite+aiosqlite:///{tmp_path / 'empty.db'}"
        )
        # as on a deployment, where the tables are the platform's to create
        monkeypatch.setattr(store, "_platform_creates_tables", lambda url: True)
        app = replica()

        async def scenario():
            with caplog.at_level("ERROR"):
                await app._a2a._start()
            await app._a2a._stop()

        run(scenario())
        assert "task tables are missing" in caplog.text

    def test_a_named_database_is_used(self, database):
        app = replica()
        assert app._a2a.mode == "database"
        manifest = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
        a2a = manifest["capabilities"]["custom"]["hopsworks"]["a2a"]
        assert a2a["task_store"] == "database"
        assert a2a["task_limit"] is None

    def test_a_deployment_without_a_database_serves_no_a2a(self, monkeypatch):
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        for name in ("MYSQL_USER", "MYSQL_HOST", "MYSQL_DB"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        app = replica()
        client = TestClient(app)
        assert client.get("/.well-known/agent-card.json").status_code == 404
        assert client.post("/a2a", json={}).status_code == 404
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["custom"]["hopsworks"]["a2a"] == {
            "enabled": False,
            "reason": "no_database",
        }
        # the agent itself is unharmed
        assert "agent_card" not in manifest["endpoints"]

    def test_memory_is_served_on_such_a_deployment_only_when_chosen(self, monkeypatch):
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        for name in ("MYSQL_USER", "MYSQL_HOST", "MYSQL_DB"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        app = replica(a2a=A2AConfig(task_store="memory"))
        assert app._a2a.mode == "memory"

    def test_asking_for_a_database_that_is_not_there_is_an_error(self, monkeypatch):
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        for name in ("MYSQL_USER", "MYSQL_HOST", "MYSQL_DB"):
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(ValueError, match="needs a database"):
            replica(a2a=A2AConfig(task_store="database"))

    def test_a_deployment_reads_the_database_it_was_given(self, monkeypatch):
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        monkeypatch.setenv("MYSQL_USER", "proj_meb1")
        monkeypatch.setenv("MYSQL_PASSWORD", "p@ss/word")
        monkeypatch.setenv("MYSQL_HOST", "onlinefs.mysql.service.consul")
        monkeypatch.setenv("MYSQL_DB", "proj")
        assert a2a_store._database_url() == (
            "mysql+aiomysql://proj_meb1:p%40ss%2Fword"
            "@onlinefs.mysql.service.consul:3306/proj"
        )

    def test_a_password_that_cannot_be_read_is_no_database(self, monkeypatch):
        from hopsworks_agents.protocol import memory

        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
        monkeypatch.setenv("MYSQL_USER", "proj_meb1")
        monkeypatch.setenv("MYSQL_HOST", "onlinefs.mysql.service.consul")
        monkeypatch.setenv("MYSQL_DB", "proj")
        monkeypatch.setenv("MYSQL_PASSWORD_SECRET_NAME", "proj_meb1")

        def unreadable(name):
            raise RuntimeError("the secrets API is down")

        monkeypatch.setattr(memory, "_read_hopsworks_secret", unreadable)
        assert a2a_store._database_url() is None
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        client = TestClient(replica())
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["custom"]["hopsworks"]["a2a"] == {
            "enabled": False,
            "reason": "no_database",
        }

    def test_retries_need_the_authors_word(self):
        with pytest.raises(ValueError, match="idempotent"):
            A2AConfig(retry_lost_tasks=1)
        assert A2AConfig(idempotent=True, retry_lost_tasks=1).retry_lost_tasks == 1


class TestAnyReplicaServesATask:
    def test_a_task_is_read_and_listed_from_another_replica(self, database):
        first, second = replica(), replica()

        async def scenario():
            a, b = await connect(first), await connect(second)
            task = await send(a, user_message("hi", context_id="ctx-1"))
            assert state_name(task) == "TASK_STATE_COMPLETED"
            fetched = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(fetched) == "TASK_STATE_COMPLETED"
            assert text_of(fetched.status.message) == "echo: hi"
            listed = await b.list_tasks(ListTasksRequest(context_id="ctx-1"))
            assert [t.id for t in listed.tasks] == [task.id]

        run(scenario())

    def test_a_paused_task_is_answered_on_another_replica(self, database, tmp_path):
        memory_url = f"sqlite:///{tmp_path / 'memory.db'}"

        def build():
            from hopsworks_agents.protocol import ManagedMemoryService

            app = AgentApp(
                memory=ManagedMemoryService(url=memory_url, deployment_id="d1")
            )

            @app.chat
            async def chat(request, ctx):
                answer = ctx.interrupt("Delete it?", key="confirm")
                return f"answered: {answer.payload}"

            return app

        first, second = build(), build()

        async def scenario():
            a, b = await connect(first), await connect(second)
            paused = await send(a, user_message("delete", context_id="ctx-2"))
            assert state_name(paused) == "TASK_STATE_INPUT_REQUIRED"

            async def released():
                return not await lease_rows(first)

            # a paused task is nobody's to run: its lease goes with the turn
            await wait_for(released)
            done = await send(
                b, user_message("yes", context_id="ctx-2", task_id=paused.id)
            )
            assert state_name(done) == "TASK_STATE_COMPLETED"
            assert text_of(done.status.message) == "answered: yes"

        run(scenario())

    def test_a_cancel_on_another_replica_stops_the_handler(self, database, monkeypatch):
        monkeypatch.setattr(a2a_store, "HEARTBEAT_SECONDS", 0.05)
        seen = {}

        async def slow(request, ctx):
            seen["started"] = True
            try:
                # a tool that reports nothing while it works
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                seen["stopped"] = True
                seen["flag"] = ctx.cancelled.is_set()
                raise
            return "never"

        first, second = replica(slow), replica()

        async def scenario():
            a, b = await connect(first), await connect(second)
            task = await send(a, user_message("work"), return_immediately=True)

            async def started():
                return seen.get("started")

            await wait_for(started)
            cancelled = await b.cancel_task(CancelTaskRequest(id=task.id))
            assert state_name(cancelled) == "TASK_STATE_CANCELED"

            async def stopped():
                return seen.get("stopped")

            await wait_for(stopped)
            assert seen["flag"] is True
            await asyncio.sleep(0.1)
            final = await a.get_task(GetTaskRequest(id=task.id))
            assert state_name(final) == "TASK_STATE_CANCELED"
            assert await lease_rows(first) == []

        run(scenario())


class TestTheLease:
    def test_a_running_task_is_leased_and_a_finished_one_is_not(self, database):
        gate = asyncio.Event

        holder = {}

        async def held(request, ctx):
            holder["running"] = True
            await holder["release"].wait()
            return "done"

        app = replica(held)

        async def scenario():
            holder["release"] = gate()
            client = await connect(app)
            task = await send(client, user_message("go"), return_immediately=True)

            async def leased():
                return await lease_rows(app)

            rows = await wait_for(leased)
            assert [(r.task_id, r.fence, r.deployment_id) for r in rows] == [
                (task.id, 1, "local")
            ]
            holder["release"].set()

            async def released():
                return not await lease_rows(app)

            await wait_for(released)
            final = await client.get_task(GetTaskRequest(id=task.id))
            assert state_name(final) == "TASK_STATE_COMPLETED"

        run(scenario())

    def test_a_task_whose_replica_was_lost_is_failed_as_unknown(
        self, database, monkeypatch
    ):
        holder = {}

        async def held(request, ctx):
            await holder["release"].wait()
            return "done"

        lost, survivor = replica(held), replica()

        async def scenario():
            holder["release"] = asyncio.Event()
            a, b = await connect(lost), await connect(survivor)
            task = await send(a, user_message("go"), return_immediately=True)

            async def leased():
                return await lease_rows(lost)

            await wait_for(leased)
            # the replica dies: it stops renewing, which a heartbeat that is
            # already too old says
            import datetime

            await change_leases(lost, heartbeat_at=datetime.datetime(2000, 1, 1))
            assert await survivor._a2a._reap() == 1
            failed = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(failed) == "TASK_STATE_FAILED"
            facts = hopsworks_of(failed.status.message)
            assert facts["status"] == "executor_lost"
            assert facts["outcome"] == "unknown"
            assert facts["retryable"] is True
            assert await lease_rows(survivor) == []
            # nothing left to reap
            assert await survivor._a2a._reap() == 0
            holder["release"].set()

        run(scenario())

    def test_an_idempotent_agent_runs_a_lost_task_again_instead_of_failing_it(
        self, database
    ):
        holder = {"runs": 0}

        async def stuck(request, ctx):
            await holder["never"].wait()
            return "never"

        async def works(request, ctx):
            holder["runs"] += 1
            return f"did: {request.text}"

        # replicas of one deployment: the retry policy is on the lease row,
        # written by the replica that started the task
        retries = A2AConfig(idempotent=True, retry_lost_tasks=1)
        lost = replica(stuck, a2a=retries)
        survivor = replica(works, a2a=retries)

        async def scenario():
            import datetime

            holder["never"] = asyncio.Event()
            a, b = await connect(lost), await connect(survivor)
            task = await send(a, user_message("report"), return_immediately=True)

            async def leased():
                return await lease_rows(lost)

            await wait_for(leased)
            await change_leases(lost, heartbeat_at=datetime.datetime(2000, 1, 1))
            assert await survivor._a2a._reap() == 1

            async def finished():
                current = await b.get_task(GetTaskRequest(id=task.id))
                # never failed on the way: working, then done
                assert state_name(current) != "TASK_STATE_FAILED"
                return (
                    current if state_name(current) == "TASK_STATE_COMPLETED" else None
                )

            done = await wait_for(finished)
            assert text_of(done.status.message) == "did: report"
            assert hopsworks_of(done.status.message)["attempt"] == 2
            assert holder["runs"] == 1

            async def released():
                return not await lease_rows(survivor)

            await wait_for(released)

        run(scenario())

    def test_a_task_lost_again_past_its_retries_is_failed(self, database):
        async def stuck(request, ctx):
            await asyncio.Event().wait()

        retries = A2AConfig(idempotent=True, retry_lost_tasks=1)
        lost = replica(stuck, a2a=retries)
        survivor = replica(stuck, a2a=retries)

        async def scenario():
            import datetime

            a, b = await connect(lost), await connect(survivor)
            task = await send(a, user_message("report"), return_immediately=True)

            async def leased():
                return await lease_rows(lost)

            await wait_for(leased)
            old = datetime.datetime(2000, 1, 1)
            await change_leases(lost, heartbeat_at=old)
            assert await survivor._a2a._reap() == 1

            async def running_again():
                rows = await lease_rows(survivor)
                # the retry runs under the fence the take holds
                return rows and rows[0].attempt == 2 and rows[0].fence == 2

            await wait_for(running_again)
            # the second run is lost too, and there is no retry left
            await change_leases(survivor, heartbeat_at=old, executor="gone-pod")
            assert await lost._a2a._reap() == 1
            failed = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(failed) == "TASK_STATE_FAILED"
            facts = hopsworks_of(failed.status.message)
            assert facts["status"] == "executor_lost"
            assert facts["attempt"] == 2

        run(scenario())

    def test_a_replica_that_lost_its_lease_stops_and_writes_nothing(
        self, database, monkeypatch
    ):
        monkeypatch.setattr(a2a_store, "HEARTBEAT_SECONDS", 0.05)
        seen = {}

        async def slow(request, ctx):
            seen["started"] = True
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                seen["stopped"] = True
                raise
            return "never"

        stale, taker = replica(slow), replica()

        async def scenario():
            a, b = await connect(stale), await connect(taker)
            task = await send(a, user_message("work"), return_immediately=True)

            async def started():
                return seen.get("started")

            await wait_for(started)
            await stored_as_working(b, task.id)
            # someone took the lease: the fence moved on
            await change_leases(stale, fence=2, executor="other-pod")

            async def stopped():
                return seen.get("stopped")

            await wait_for(stopped)
            await asyncio.sleep(0.1)
            # the stale replica reported nothing: the task is as the taker
            # left it, which here is still working
            current = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(current) == "TASK_STATE_WORKING"
            rows = await lease_rows(taker)
            assert [(r.executor, r.fence) for r in rows] == [("other-pod", 2)]

        run(scenario())


class TestPrivateMode:
    """``memory_routes=False``: what a caller opened is the caller's alone."""

    def _app(self):
        app = AgentApp(memory=InMemoryAgentMemory(), memory_routes=False)

        @app.chat
        async def chat(request, ctx):
            return f"{len(ctx.history)} earlier, id {ctx.conversation_id}"

        return app

    def test_a_task_of_another_caller_does_not_exist(self, database):
        app = self._app()

        async def scenario():
            alice = await connect(app, headers={"x-hopsworks-principal": "user:alice"})
            bob = await connect(app, headers={"x-hopsworks-principal": "user:bob"})
            task = await send(alice, user_message("hi", context_id="shared-ctx"))
            assert state_name(task) == "TASK_STATE_COMPLETED"
            mine = await alice.get_task(GetTaskRequest(id=task.id))
            assert mine.id == task.id
            with pytest.raises(TaskNotFoundError):
                await bob.get_task(GetTaskRequest(id=task.id))
            with pytest.raises(TaskNotFoundError):
                await bob.cancel_task(CancelTaskRequest(id=task.id))
            listed = await bob.list_tasks(ListTasksRequest(context_id="shared-ctx"))
            assert list(listed.tasks) == []
            listed = await alice.list_tasks(ListTasksRequest(context_id="shared-ctx"))
            assert [t.id for t in listed.tasks] == [task.id]

        run(scenario())

    def test_a_known_context_id_is_not_a_key_to_its_conversation(self, database):
        app = self._app()

        async def scenario():
            alice = await connect(app, headers={"x-hopsworks-principal": "user:alice"})
            bob = await connect(app, headers={"x-hopsworks-principal": "user:bob"})
            first = await send(alice, user_message("one", context_id="ctx"))
            assert text_of(first.status.message) == "0 earlier, id ctx"
            second = await send(alice, user_message("two", context_id="ctx"))
            assert text_of(second.status.message).startswith("2 earlier")
            # the same context id, another caller: an empty conversation
            other = await send(bob, user_message("hello", context_id="ctx"))
            assert text_of(other.status.message) == "0 earlier, id ctx"
            # and the first caller's is as it was
            third = await send(alice, user_message("three", context_id="ctx"))
            assert text_of(third.status.message).startswith("4 earlier")

        run(scenario())

    def test_the_same_holds_over_the_chat_route(self, database):
        app = self._app()
        client = TestClient(app)

        def chat(principal, text):
            response = client.post(
                "/v1/chat",
                json={
                    "conversation_id": "ctx",
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": text}],
                    },
                },
                headers={"x-hopsworks-principal": principal},
            )
            assert response.status_code == 200, response.text
            body = response.json()
            assert body["conversation_id"] == "ctx"
            return body["message"]["content"][0]["text"]

        assert chat("user:alice", "one").startswith("0 earlier")
        assert chat("user:alice", "two").startswith("2 earlier")
        assert chat("user:bob", "hello").startswith("0 earlier")
        # what the store holds is filed under each caller
        inner = app.memory._inner
        assert len(inner.get("user:alice/ctx")) == 4
        assert len(inner.get("user:bob/ctx")) == 2
        assert inner.get("ctx") == []

    def test_a_request_that_names_no_caller_is_refused_on_a_deployment(
        self, monkeypatch
    ):
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        app = AgentApp(
            memory=InMemoryAgentMemory(),
            memory_routes=False,
            a2a=A2AConfig(task_store="memory"),
        )

        @app.chat
        async def chat(request, ctx):
            return "ok"

        client = TestClient(app)
        body = {"message": {"role": "user", "content": [{"type": "text", "text": "x"}]}}
        refused = client.post("/v1/chat", json=body)
        assert refused.status_code == 401
        assert refused.json()["detail"]["code"] == "principal_required"
        assert client.post("/a2a", json={}).status_code == 401
        # what anyone may read stays open: the card, the manifest, the probes
        assert client.get("/.well-known/agent-card.json").status_code == 200
        assert client.get("/.well-known/hopsworks-agent.json").status_code == 200
        allowed = client.post(
            "/v1/chat", json=body, headers={"x-hopsworks-principal": "user:alice"}
        )
        assert allowed.status_code == 200

    def test_a_shared_agent_files_tasks_under_the_deployment(self, database):
        app = replica()

        async def scenario():
            alice = await connect(app, headers={"x-hopsworks-principal": "user:alice"})
            bob = await connect(app, headers={"x-hopsworks-principal": "user:bob"})
            task = await send(alice, user_message("hi"))
            fetched = await bob.get_task(GetTaskRequest(id=task.id))
            assert fetched.id == task.id

        run(scenario())


class TestReviewOfTheLease:
    """What a review of the lease found, each as the case that showed it."""

    def test_a_task_queued_behind_another_is_leased_before_it_is_acknowledged(
        self, database
    ):
        holder = {}

        async def held(request, ctx):
            await holder["release"].wait()
            return "done"

        app = replica(held, a2a=A2AConfig(serialize_context=True))

        async def scenario():
            holder["release"] = asyncio.Event()
            client = await connect(app)
            first = await send(
                client, user_message("one", context_id="ctx"), return_immediately=True
            )
            second = await send(
                client, user_message("two", context_id="ctx"), return_immediately=True
            )
            assert state_name(second) == "TASK_STATE_SUBMITTED"
            # the caller holds both ids, and a reaper can find both tasks
            rows = await lease_rows(app)
            assert {r.task_id for r in rows} == {first.id, second.id}
            holder["release"].set()

            async def both_done():
                return not await lease_rows(app)

            await wait_for(both_done)

        run(scenario())

    def test_a_queued_task_whose_replica_was_lost_is_failed(self, database):
        holder = {}

        async def held(request, ctx):
            await holder["never"].wait()

        lost = replica(held, a2a=A2AConfig(serialize_context=True))
        survivor = replica()

        async def scenario():
            import datetime

            holder["never"] = asyncio.Event()
            a, b = await connect(lost), await connect(survivor)
            await send(
                a, user_message("one", context_id="ctx"), return_immediately=True
            )
            queued = await send(
                a, user_message("two", context_id="ctx"), return_immediately=True
            )
            await change_leases(lost, heartbeat_at=datetime.datetime(2000, 1, 1))
            assert await survivor._a2a._reap() == 2
            failed = await b.get_task(GetTaskRequest(id=queued.id))
            assert state_name(failed) == "TASK_STATE_FAILED"
            assert hopsworks_of(failed.status.message)["status"] == "executor_lost"

        run(scenario())

    def test_a_lease_renewed_since_the_scan_is_not_taken(self, database):
        holder = {}

        async def held(request, ctx):
            await holder["release"].wait()
            return "done"

        app, reaper = replica(held), replica()

        async def scenario():
            import datetime

            holder["release"] = asyncio.Event()
            client = await connect(app)
            task = await send(client, user_message("go"), return_immediately=True)

            async def leased():
                return await lease_rows(app)

            await wait_for(leased)
            leases = reaper._a2a.stores.leases
            await change_leases(app, heartbeat_at=datetime.datetime(2000, 1, 1))
            stale = await leases._stale_rows()
            assert [row.task_id for row in stale] == [task.id]
            # the replica's heartbeat lands between the scan and the takeover
            assert await app._a2a.stores.leases._heartbeat(task.id, 1) is True
            assert await leases._take(stale[0]) is None
            rows = await lease_rows(app)
            assert [(r.fence, r.attempt) for r in rows] == [(1, 1)]
            holder["release"].set()

            async def done():
                return not await lease_rows(app)

            await wait_for(done)
            final = await client.get_task(GetTaskRequest(id=task.id))
            assert state_name(final) == "TASK_STATE_COMPLETED"

        run(scenario())

    def test_a_replica_whose_lease_was_taken_cannot_write_before_it_notices(
        self, database
    ):
        # the heartbeat is slow here on purpose: the replica has not noticed
        holder = {}

        async def answers_late(request, ctx):
            await holder["go"].wait()
            return "a late answer"

        stale, taker = replica(answers_late), replica()

        async def scenario():
            holder["go"] = asyncio.Event()
            a, b = await connect(stale), await connect(taker)
            task = await send(a, user_message("work"), return_immediately=True)

            async def leased():
                return await lease_rows(stale)

            await wait_for(leased)
            await stored_as_working(b, task.id)
            await change_leases(stale, fence=2, executor="other-pod")
            holder["go"].set()
            await asyncio.sleep(0.3)
            # the answer was refused at the store: the task is as the taker left it
            current = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(current) == "TASK_STATE_WORKING"
            assert task.id in stale._a2a.stores.fence.out

        run(scenario())

    def test_a_reaper_fails_a_task_that_moved_under_it(self, database, monkeypatch):
        holder = {}

        async def held(request, ctx):
            await holder["never"].wait()

        lost, survivor = replica(held), replica()

        async def scenario():
            import datetime

            holder["never"] = asyncio.Event()
            a, b = await connect(lost), await connect(survivor)
            task = await send(a, user_message("go"), return_immediately=True)

            async def leased():
                return await lease_rows(lost)

            await wait_for(leased)
            await stored_as_working(b, task.id)
            await change_leases(lost, heartbeat_at=datetime.datetime(2000, 1, 1))
            store = survivor._a2a.stores.task_store
            real_get, calls = store.get, {"n": 0}

            async def get_then_move(task_id, context):
                found = await real_get(task_id, context)
                calls["n"] += 1
                if calls["n"] == 1 and found is not None:
                    # the lost replica's last write lands after the reaper read
                    moved = await real_get(task_id, context)
                    await store.save(
                        moved.task,
                        event=None,
                        prev=None,
                        prev_version=moved.version,
                        context=context,
                    )
                return found

            monkeypatch.setattr(store, "get", get_then_move)
            assert await survivor._a2a._reap() == 1
            failed = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(failed) == "TASK_STATE_FAILED"
            assert await lease_rows(survivor) == []

        run(scenario())


def owner_context(owner="local"):
    from a2a.server.context import ServerCallContext

    return ServerCallContext(state={a2a_store.OWNER_STATE_KEY: owner})


def new_task(task_id, state="TASK_STATE_WORKING"):
    from a2a.types import Task, TaskStatus

    return Task(
        id=task_id,
        context_id="ctx",
        status=TaskStatus(state=TaskState.Value(state)),
    )


async def shut(*apps):
    """End the SDK's active tasks of each replica, as a server's shutdown does, before the loop closes under them."""
    for app in apps:
        await app._a2a.handler.aclose()


async def held_task(app, task_id):
    """A task this replica runs, saved once under its lease; its fence and stored version."""
    from a2a.server.cluster.version import TaskVersion

    stores = app._a2a.stores
    fence, _ = await stores.leases._acquire(task_id, "local")
    stores.fence.held[task_id] = fence
    version = await stores.task_store.save(
        new_task(task_id),
        event=None,
        prev=None,
        prev_version=TaskVersion.MISSING,
        context=owner_context(),
    )
    return fence, version


async def stored_version(app, task_id):
    stored = await app._a2a.stores.task_store.get(task_id, owner_context())
    return stored.version._value if stored is not None else None


class TestReviewRoundTwo:
    """The second review's findings (contract section 7), each as the case that showed it."""

    # F4: the lease is checked inside the save's own transaction

    def test_a_takeover_before_the_write_refuses_it_and_nothing_is_written(
        self, database
    ):
        from a2a.server.cluster import ConcurrentTaskModificationError

        app = replica()

        async def scenario():
            stores = app._a2a.stores
            fence, version = await held_task(app, "t-f4a")
            await change_leases(app, fence=fence + 1, executor="other-pod")
            with pytest.raises(ConcurrentTaskModificationError):
                await stores.task_store.save(
                    new_task("t-f4a", "TASK_STATE_COMPLETED"),
                    event=None,
                    prev=None,
                    prev_version=version,
                    context=owner_context(),
                )
            assert await stored_version(app, "t-f4a") == 1
            assert "t-f4a" in stores.fence.out

        run(scenario())

    def test_a_takeover_racing_a_write_waits_for_it_and_the_next_write_is_refused(
        self, database
    ):
        from a2a.server.cluster import ConcurrentTaskModificationError
        from sqlalchemy import update

        app = replica()

        async def scenario():
            stores = app._a2a.stores
            store = stores.task_store
            fence, version = await held_task(app, "t-f4b")
            seen = {}
            real_lease = store._lease

            async def takeover():
                table = stores.leases.table
                async with stores.engine.begin() as conn:
                    await conn.execute(
                        update(table)
                        .where(table.c.task_id == "t-f4b", table.c.fence == fence)
                        .values(fence=fence + 1, executor="other-pod")
                    )
                seen["taken"] = True

            async def lease_then_race(session, task_id):
                row = await real_lease(session, task_id)
                if "race" not in seen:
                    # the lease was checked; another replica takes it now,
                    # before this save has written anything
                    seen["race"] = asyncio.create_task(takeover())
                    await asyncio.sleep(0.5)
                    seen["taken_inside"] = seen.get("taken", False)
                return row

            store._lease = lease_then_race
            await store.save(
                new_task("t-f4b", "TASK_STATE_WORKING"),
                event=None,
                prev=None,
                prev_version=version,
                context=owner_context(),
            )
            # the takeover could not land while the save held the lease row
            assert seen["taken_inside"] is False
            await seen["race"]
            assert seen["taken"] is True
            assert await stored_version(app, "t-f4b") == 2
            # and the old executor's next write is refused
            with pytest.raises(ConcurrentTaskModificationError):
                await store.save(
                    new_task("t-f4b", "TASK_STATE_COMPLETED"),
                    event=None,
                    prev=None,
                    prev_version=2,
                    context=owner_context(),
                )
            assert await stored_version(app, "t-f4b") == 2

        run(scenario())

    def test_a_lease_that_cannot_be_read_refuses_the_write(self, database):
        app = replica()

        async def scenario():
            store = app._a2a.stores.task_store
            _, version = await held_task(app, "t-f4c")

            async def broken(session, task_id):
                raise RuntimeError("the lease table is not there")

            store._lease = broken
            with pytest.raises(RuntimeError):
                await store.save(
                    new_task("t-f4c", "TASK_STATE_COMPLETED"),
                    event=None,
                    prev=None,
                    prev_version=version,
                    context=owner_context(),
                )
            assert await stored_version(app, "t-f4c") == 1

        run(scenario())

    # F2: a protected turn is not cancelled, from here or from another replica

    def test_a_cancel_is_refused_once_the_task_is_protected(self, database):
        from a2a.server.cluster import ConcurrentTaskModificationError

        app = replica()

        async def scenario():
            stores = app._a2a.stores
            fence, version = await held_task(app, "t-f2a")
            assert await stores.task_store._protect("t-f2a", fence) is True
            rows = await lease_rows(app)
            assert [r.protected for r in rows] == [True]
            # a remote cancel: this replica does not run the task
            stores.fence.held.pop("t-f2a")
            with pytest.raises(ConcurrentTaskModificationError):
                await stores.task_store.save(
                    new_task("t-f2a", "TASK_STATE_CANCELED"),
                    event=None,
                    prev=None,
                    prev_version=version,
                    context=owner_context(),
                )
            assert await stored_version(app, "t-f2a") == 1

        run(scenario())

    def test_a_cancel_that_landed_first_keeps_the_task_from_being_protected(
        self, database
    ):
        app = replica()

        async def scenario():
            stores = app._a2a.stores
            fence, version = await held_task(app, "t-f2b")
            stores.fence.held.pop("t-f2b")
            await stores.task_store.save(
                new_task("t-f2b", "TASK_STATE_CANCELED"),
                event=None,
                prev=None,
                prev_version=version,
                context=owner_context(),
            )
            assert await stores.task_store._protect("t-f2b", fence) is False
            rows = await lease_rows(app)
            assert [r.protected for r in rows] == [False]

        run(scenario())

    def _approving(self, tmp_path, holder):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory_url = f"sqlite:///{tmp_path / 'memory.db'}"

        def build():
            app = AgentApp(
                memory=ManagedMemoryService(url=memory_url, deployment_id="d1")
            )

            @app.chat
            async def chat(request, ctx):
                answer = ctx.interrupt("Refund it?", key="confirm")
                holder["acting"] = True
                await holder["release"].wait()
                holder["done"] = True
                return f"refunded: {answer.payload}"

            return app

        return build(), build()

    def test_a_cancel_on_another_replica_does_not_land_on_an_approved_action(
        self, database, tmp_path
    ):
        from a2a.utils.errors import TaskNotCancelableError

        holder = {}
        first, second = self._approving(tmp_path, holder)

        async def scenario():
            holder["release"] = asyncio.Event()
            a, b = await connect(first), await connect(second)
            paused = await send(a, user_message("refund", context_id="ctx-f2"))
            assert state_name(paused) == "TASK_STATE_INPUT_REQUIRED"
            await send(
                a,
                user_message("yes", context_id="ctx-f2", task_id=paused.id),
                return_immediately=True,
            )

            async def protected():
                rows = await lease_rows(first)
                return holder.get("acting") and rows and rows[0].protected

            await wait_for(protected)
            with pytest.raises(TaskNotCancelableError):
                await b.cancel_task(CancelTaskRequest(id=paused.id))
            holder["release"].set()

            async def finished():
                current = await b.get_task(GetTaskRequest(id=paused.id))
                return (
                    current if state_name(current) == "TASK_STATE_COMPLETED" else None
                )

            done = await wait_for(finished)
            assert text_of(done.status.message) == "refunded: yes"
            await shut(first, second)

        run(scenario())

    def test_an_approved_action_does_not_run_when_a_cancel_landed_first(
        self, database, tmp_path
    ):
        holder = {}
        first, second = self._approving(tmp_path, holder)

        async def scenario():
            holder["release"] = asyncio.Event()
            a, b = await connect(first), await connect(second)
            paused = await send(a, user_message("refund", context_id="ctx-f2c"))
            store = first._a2a.stores.task_store
            real_protect = store._protect

            async def cancel_first(task_id, fence):
                # the cancel reaches the other replica between the resume and
                # the action
                cancelled = await b.cancel_task(CancelTaskRequest(id=task_id))
                holder["cancelled"] = state_name(cancelled)
                return await real_protect(task_id, fence)

            store._protect = cancel_first
            await send(
                a,
                user_message("yes", context_id="ctx-f2c", task_id=paused.id),
                return_immediately=True,
            )

            async def settled():
                return holder.get("cancelled") and not await lease_rows(first)

            await wait_for(settled)
            assert holder["cancelled"] == "TASK_STATE_CANCELED"
            assert "acting" not in holder
            final = await b.get_task(GetTaskRequest(id=paused.id))
            assert state_name(final) == "TASK_STATE_CANCELED"
            await shut(first, second)

        run(scenario())

    # F3: the event log is followed by task version

    def test_events_committed_out_of_sequence_order_are_all_replayed(self, database):
        from a2a.server.cluster.version import TaskVersion
        from a2a.types import TaskStatus, TaskStatusUpdateEvent
        from a2a.utils.proto_utils import to_stream_response
        from sqlalchemy import insert

        app = replica()

        def event(state):
            return to_stream_response(
                TaskStatusUpdateEvent(
                    task_id="t-f3",
                    context_id="ctx",
                    status=TaskStatus(state=TaskState.Value(state)),
                )
            ).SerializeToString()

        async def scenario():
            stores = app._a2a.stores
            await stores.task_store.initialize()
            stream = stores.event_stream
            stream._poll_interval_s = 0.02
            model = stream._event_model

            async def write(seq, version, state):
                async with stores.engine.begin() as conn:
                    await conn.execute(
                        insert(model.__table__).values(
                            seq=seq,
                            task_id="t-f3",
                            owner="local",
                            task_version=version,
                            event_data=event(state),
                        )
                    )

            seen = []

            async def follow(after):
                async for item in stream.subscribe(
                    "t-f3", after=after, context=owner_context()
                ):
                    seen.append(
                        (item.version._value, TaskState.Name(item.event.status.state))
                    )

            # version 1 was given a high sequence number by one SQL node ...
            await write(50, 1, "TASK_STATE_SUBMITTED")
            follower = asyncio.create_task(follow(TaskVersion.MISSING))

            async def got(count):
                return len(seen) >= count

            await wait_for(lambda: got(1))
            # ... and version 2 a lower one by another, committed after
            await write(10, 2, "TASK_STATE_WORKING")
            await write(60, 3, "TASK_STATE_COMPLETED")
            await wait_for(lambda: got(3))
            follower.cancel()
            assert seen == [
                (1, "TASK_STATE_SUBMITTED"),
                (2, "TASK_STATE_WORKING"),
                (3, "TASK_STATE_COMPLETED"),
            ]
            # a subscription after a version starts past it
            seen.clear()
            follower = asyncio.create_task(follow(TaskVersion(1)))
            await wait_for(lambda: got(2))
            await asyncio.sleep(0.1)
            follower.cancel()
            assert [version for version, _ in seen] == [2, 3]

        run(scenario())

    # F6: one recovery policy, on the lease row

    def test_the_retry_policy_is_read_from_the_lease_not_the_reaper(self, database):
        holder = {"runs": 0}

        async def stuck(request, ctx):
            await holder["never"].wait()

        async def works(request, ctx):
            holder["runs"] += 1
            return f"did: {request.text}"

        lost = replica(stuck, a2a=A2AConfig(idempotent=True, retry_lost_tasks=1))
        # this replica's own config says nothing of retries
        survivor = replica(works)

        async def scenario():
            import datetime

            holder["never"] = asyncio.Event()
            a, b = await connect(lost), await connect(survivor)
            task = await send(a, user_message("report"), return_immediately=True)
            rows = await wait_for(lambda: lease_rows(lost))
            assert rows[0].retry_limit == 1
            await change_leases(lost, heartbeat_at=datetime.datetime(2000, 1, 1))
            assert await survivor._a2a._reap() == 1

            async def finished():
                current = await b.get_task(GetTaskRequest(id=task.id))
                return (
                    current if state_name(current) == "TASK_STATE_COMPLETED" else None
                )

            done = await wait_for(finished)
            assert hopsworks_of(done.status.message)["attempt"] == 2
            assert holder["runs"] == 1
            # the lost replica's turn ends, its writes refused, rather than
            # being cancelled in the middle of one at teardown
            holder["never"].set()
            await asyncio.sleep(0.2)
            await shut(lost, survivor)

        run(scenario())

    def test_a_task_the_platform_handed_back_is_run_again(self, database):
        holder = {"runs": 0}

        async def stuck(request, ctx):
            await holder["never"].wait()

        async def works(request, ctx):
            holder["runs"] += 1
            return f"did: {request.text}"

        retries = A2AConfig(idempotent=True, retry_lost_tasks=1)
        lost, survivor = replica(stuck, a2a=retries), replica(works, a2a=retries)

        async def scenario():
            import datetime

            holder["never"] = asyncio.Event()
            a, b = await connect(lost), await connect(survivor)
            task = await send(a, user_message("report"), return_immediately=True)
            await wait_for(lambda: lease_rows(lost))
            await stored_as_working(b, task.id)
            # the platform took the lost lease and handed the task back
            await change_leases(
                lost,
                executor="",
                fence=2,
                attempt=2,
                retry_pending=True,
                heartbeat_at=datetime.datetime(2000, 1, 1),
            )
            # a handed-back row is not a lost one, however old its heartbeat
            assert await survivor._a2a._reap() == 0
            # taken at the sweep's start, before its first interval
            sweeper = asyncio.create_task(survivor._a2a._sweep())

            async def finished():
                current = await b.get_task(GetTaskRequest(id=task.id))
                return (
                    current if state_name(current) == "TASK_STATE_COMPLETED" else None
                )

            done = await wait_for(finished)
            sweeper.cancel()
            await asyncio.gather(sweeper, return_exceptions=True)
            assert hopsworks_of(done.status.message)["attempt"] == 2
            assert holder["runs"] == 1
            assert await survivor._a2a._take_handed_back() == 0
            holder["never"].set()
            await asyncio.sleep(0.2)
            await shut(lost, survivor)

        run(scenario())

    # R4: a retry keeps the policy the task started with

    @staticmethod
    async def _lose_twice(started_with, recovering_with):
        """A task started on a replica with one policy, lost, run again on a replica with another, and lost again; the task as the second loss leaves it."""
        import datetime

        starts = {"lost": 0, "survivor": 0}

        def stuck(name):
            async def handler(request, ctx):
                starts[name] += 1
                await asyncio.Event().wait()

            return handler

        lost = replica(stuck("lost"), a2a=started_with)
        survivor = replica(stuck("survivor"), a2a=recovering_with)
        a, b = await connect(lost), await connect(survivor)
        task = await send(a, user_message("report"), return_immediately=True)
        await wait_for(lambda: lease_rows(lost))
        await stored_as_working(b, task.id)
        old = datetime.datetime(2000, 1, 1)
        await change_leases(lost, heartbeat_at=old)
        assert await survivor._a2a._reap() == 1

        async def running_again():
            return starts["survivor"] == 1

        await wait_for(running_again)
        rows = await lease_rows(survivor)
        # run under the take's fence and attempt, the policy as written
        assert [(r.fence, r.attempt, r.retry_limit) for r in rows] == [
            (2, 2, started_with.retry_lost_tasks)
        ]
        # the retry is lost too; the replica configured otherwise finds it
        await change_leases(survivor, heartbeat_at=old, executor="gone-pod")
        assert await survivor._a2a._reap() == 1
        return survivor, b, task, starts

    def test_a_retry_does_not_lower_the_tasks_retries(self, database):
        async def scenario():
            survivor, b, task, starts = await self._lose_twice(
                A2AConfig(idempotent=True, retry_lost_tasks=2), A2AConfig()
            )

            async def running_a_third_time():
                return starts["survivor"] == 2

            await wait_for(running_a_third_time)
            current = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(current) == "TASK_STATE_WORKING"
            rows = await lease_rows(survivor)
            assert [(r.attempt, r.retry_limit) for r in rows] == [(3, 2)]

        run(scenario())

    def test_a_retry_does_not_raise_the_tasks_retries(self, database):
        async def scenario():
            _, b, task, starts = await self._lose_twice(
                A2AConfig(idempotent=True, retry_lost_tasks=1),
                A2AConfig(idempotent=True, retry_lost_tasks=3),
            )
            failed = await b.get_task(GetTaskRequest(id=task.id))
            assert state_name(failed) == "TASK_STATE_FAILED"
            assert hopsworks_of(failed.status.message)["attempt"] == 2
            assert starts["survivor"] == 1

        run(scenario())

    def test_a_handed_back_task_is_taken_by_one_replica(self, database):
        app, other = replica(), replica()

        async def scenario():
            stores = app._a2a.stores
            await stores.leases._acquire("t-f6", "local", 1)
            await change_leases(
                app, executor="", fence=2, attempt=2, retry_pending=True
            )
            first, second = await asyncio.gather(
                stores.leases._take_handed_back(),
                other._a2a.stores.leases._take_handed_back(),
            )
            taken = first + second
            assert [(t["task_id"], t["fence"], t["attempt"]) for t in taken] == [
                ("t-f6", 3, 2)
            ]
            rows = await lease_rows(app)
            assert [(r.retry_pending, r.fence, r.attempt) for r in rows] == [
                (False, 3, 2)
            ]

        run(scenario())


class TestDeletedTasksTakeTheirFiles:
    """F7: spilled files are removed with their task, now or by the sweep."""

    @pytest.fixture
    def spilling(self, database, tmp_path, monkeypatch):
        from hopsworks_agents.protocol import a2a_artifacts

        monkeypatch.setenv(a2a_artifacts.ARTIFACT_DIR_ENV, str(tmp_path / "files"))
        monkeypatch.setenv(a2a_artifacts.INLINE_BYTES_ENV, "1000")
        return tmp_path / "files"

    @staticmethod
    def _reporter(**kwargs):
        import base64

        from hopsworks_agents.protocol import AgentResponse, Artifact

        app = AgentApp(name="Reporter", **kwargs)

        @app.chat
        async def chat(request, ctx):
            return AgentResponse.text(
                "done",
                artifacts=[
                    Artifact(
                        name="big.bin",
                        media_type="application/octet-stream",
                        data=base64.b64encode(b"x" * 5000).decode(),
                    )
                ],
            )

        return app

    @staticmethod
    async def _cleanup_rows(app):
        from sqlalchemy import select

        cleanups = app._a2a.stores.cleanups
        await cleanups._ensure()
        async with app._a2a.stores.engine.connect() as conn:
            return (await conn.execute(select(cleanups.table))).all()

    def test_a_deleted_task_takes_its_files(self, spilling):
        app = self._reporter()

        async def scenario():
            client = await connect(app)
            task = await send(client, user_message("go"))
            assert (spilling / task.id).is_dir()
            await app._a2a.handler_store.delete(task.id, owner_context())
            assert not (spilling / task.id).exists()
            assert await self._cleanup_rows(app) == []
            with pytest.raises(TaskNotFoundError):
                await client.get_task(GetTaskRequest(id=task.id))

        run(scenario())

    def test_a_removal_that_fails_is_retried_by_the_sweep(self, spilling, monkeypatch):
        app = self._reporter()

        async def scenario():
            client = await connect(app)
            task = await send(client, user_message("go"))
            spill = app._a2a.spill
            real_remove = spill._remove

            def broken(task_id):
                raise OSError("the file system is away")

            monkeypatch.setattr(spill, "_remove", broken)
            await app._a2a.handler_store.delete(task.id, owner_context())
            rows = await self._cleanup_rows(app)
            # R3: an author's directory, which only a replica can clean
            assert [(r.task_id, r.attempts, r.location) for r in rows] == [
                (task.id, 1, "directory")
            ]
            assert (spilling / task.id).is_dir()
            # the sweep fails too, and counts it
            assert await app._a2a._clean_spills() == 0
            rows = await self._cleanup_rows(app)
            assert [r.attempts for r in rows] == [2]
            monkeypatch.setattr(spill, "_remove", real_remove)
            assert await app._a2a._clean_spills() == 1
            assert not (spilling / task.id).exists()
            assert await self._cleanup_rows(app) == []

        run(scenario())

    def test_a_removal_is_given_up_on_after_its_attempts(self, spilling, monkeypatch):
        app = self._reporter()

        async def scenario():
            client = await connect(app)
            task = await send(client, user_message("go"))
            cleanups = app._a2a.stores.cleanups
            monkeypatch.setattr(cleanups, "ATTEMPTS", 2)

            def broken(task_id):
                raise OSError("the file system is away")

            monkeypatch.setattr(app._a2a.spill, "_remove", broken)
            await app._a2a.handler_store.delete(task.id, owner_context())
            await app._a2a._clean_spills()
            assert await self._cleanup_rows(app) == []

        run(scenario())

    def test_a_replica_cleans_only_the_rows_of_where_its_files_are(
        self, spilling, monkeypatch
    ):
        app = self._reporter()

        async def scenario():
            await connect(app)
            cleanups = app._a2a.stores.cleanups
            assert cleanups.location == "directory"
            await cleanups._ensure()
            # a row a replica with the project's files filed
            cleanups.location = "project"
            async with app._a2a.stores.engine.begin() as conn:
                await conn.execute(cleanups._file("t-project"))
            cleanups.location = "directory"
            removed = []
            monkeypatch.setattr(app._a2a.spill, "_remove", removed.append)
            assert await app._a2a._clean_spills() == 0
            assert removed == []
            await cleanups._failed("t-project")
            await cleanups._done("t-project")
            rows = await self._cleanup_rows(app)
            assert [(r.task_id, r.attempts, r.location) for r in rows] == [
                ("t-project", 0, "project")
            ]
            # and the other way round
            async with app._a2a.stores.engine.begin() as conn:
                await conn.execute(cleanups._file("t-directory"))
            cleanups.location = "project"
            assert await cleanups._due() == ["t-project"]

        run(scenario())

    def test_another_callers_task_is_not_deleted_nor_its_files(self, spilling):
        app = self._reporter(memory=InMemoryAgentMemory(), memory_routes=False)

        async def scenario():
            alice = await connect(app, headers={"x-hopsworks-principal": "user:alice"})
            task = await send(alice, user_message("go"))
            await app._a2a.handler_store.delete(
                task.id, owner_context("local:user:bob")
            )
            assert (spilling / task.id).is_dir()
            assert await self._cleanup_rows(app) == []
            assert state_name(await alice.get_task(GetTaskRequest(id=task.id))) == (
                "TASK_STATE_COMPLETED"
            )

        run(scenario())

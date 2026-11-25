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
MYSQL_TABLES = {
    "agent_a2a_tasks": """
        `id` VARCHAR(36) NOT NULL,
        `context_id` VARCHAR(255) NOT NULL,
        `kind` VARCHAR(16) NOT NULL DEFAULT 'task',
        `owner` VARCHAR(255) NULL,
        `last_updated` DATETIME(6) NULL,
        `status` JSON NOT NULL,
        `artifacts` JSON NULL,
        `history` JSON NULL,
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
        KEY `idx_agent_a2a_task_events_task` (`task_id`)""",
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
        PRIMARY KEY (`task_id`),
        KEY `idx_agent_a2a_task_leases_heartbeat` (`deployment_id`, `heartbeat_at`)""",
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

        lost = replica(stuck)
        survivor = replica(works, a2a=A2AConfig(idempotent=True, retry_lost_tasks=1))

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

        lost = replica(stuck)
        survivor = replica(stuck, a2a=A2AConfig(idempotent=True, retry_lost_tasks=1))

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
                return rows and rows[0].attempt == 2 and rows[0].fence >= 3

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

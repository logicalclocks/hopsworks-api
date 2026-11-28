"""Push notifications: a task's events posted to the webhook its caller registered."""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import (
    Message,
    Part,
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    TaskPushNotificationConfig,
    TaskState,
)
from hopsworks_agents.protocol import AgentApp, a2a_push, a2a_store
from hopsworks_agents.protocol import a2a as a2a_module
from hopsworks_agents.protocol.a2a_push import (
    _Delivery,
    _pin_webhook,
    _PushSender,
    _valid_webhook,
)
from hopsworks_agents.protocol.models import AgentError


BASE = "http://agent.test"


def run(coro):
    return asyncio.run(coro)


class Receiver:
    """A webhook on loopback that records what it is sent and answers as told."""

    def __init__(self, status=200, hang=0.0, location=None):
        self.requests = []
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 -- http.server's name
                body = self.rfile.read(int(self.headers.get("content-length") or 0))
                receiver.requests.append(
                    {
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "body": json.loads(body or b"{}"),
                    }
                )
                if hang:
                    threading.Event().wait(hang)
                self.send_response(status)
                if location:
                    self.send_header("Location", location)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/hook"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def states(self):
        found = []
        for request in self.requests:
            body = request["body"]
            status = (body.get("statusUpdate") or body.get("task") or {}).get("status")
            if status:
                found.append(status["state"])
        return found


@pytest.fixture
def receiver():
    made = []

    def make(**kwargs):
        made.append(Receiver(**kwargs))
        return made[-1]

    yield make
    for one in made:
        one.close()


@pytest.fixture(autouse=True)
def local(monkeypatch):
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
    monkeypatch.delenv(a2a_push.ALLOWED_HOSTS_ENV, raising=False)
    monkeypatch.setattr(a2a_push, "BACKOFF_S", (0.01, 0.01, 0.01))


def echo_app(handler=None, **kwargs):
    app = AgentApp(name="Echo agent", **kwargs)

    async def echo(request, ctx):
        return f"echo: {request.text}"

    app.chat(handler or echo)
    return app


async def connect(app, *, streaming=False):
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE)
    card = await A2ACardResolver(http, BASE).get_agent_card()
    return ClientFactory(ClientConfig(httpx_client=http, streaming=streaming)).create(
        card
    )


def request_with_webhook(text, url, **config):
    configuration = SendMessageConfiguration(
        task_push_notification_config=TaskPushNotificationConfig(url=url, **config)
    )
    return SendMessageRequest(
        message=Message(
            role=Role.ROLE_USER, message_id=f"m-{text}", parts=[Part(text=text)]
        ),
        configuration=configuration,
    )


async def settle(app, task_id, timeout=5.0):
    """Wait until the task's notifications have been dealt with."""
    sender = app._a2a.push_sender
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        worker = sender._workers.get(task_id)
        if worker is None and task_id in sender.records:
            return sender.records[task_id]
        await asyncio.sleep(0.02)
    raise AssertionError("the notifications were never dealt with")


class TestDelivery:
    def test_the_card_and_manifest_say_push_is_served(self):
        from fastapi.testclient import TestClient

        client = TestClient(echo_app())
        card = client.get("/.well-known/agent-card.json").json()
        assert card["capabilities"]["pushNotifications"] is True
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        a2a = manifest["capabilities"]["custom"]["hopsworks"]["a2a"]
        assert a2a["push_notifications"] is True

    def test_a_tasks_events_reach_its_webhook_with_the_callers_token(self, receiver):
        hook = receiver()
        app = echo_app()

        async def scenario():
            client = await connect(app)
            request = request_with_webhook(
                "hi",
                hook.url,
                token="tok-1",
                authentication={"scheme": "ApiKey", "credentials": "cb-key"},
            )
            events = [event async for event in client.send_message(request)]
            task = events[0].task
            record = await settle(app, task.id)
            assert record.terminal_delivered is True
            assert record.dropped == 0
            return task

        task = run(scenario())
        assert hook.states()[-1] == "TASK_STATE_COMPLETED"
        last = hook.requests[-1]
        assert last["headers"]["x-a2a-notification-token"] == "tok-1"
        assert last["headers"]["authorization"] == "ApiKey cb-key"
        assert last["body"]["statusUpdate"]["taskId"] == task.id
        # in the order the task went through them
        states = hook.states()
        assert states.index("TASK_STATE_WORKING") < states.index("TASK_STATE_COMPLETED")

    def test_a_receiver_that_hangs_does_not_hold_the_tasks_own_answer(self, receiver):
        hook = receiver(hang=1.0)
        app = echo_app()

        async def scenario():
            client = await connect(app)
            started = asyncio.get_running_loop().time()
            events = [
                event
                async for event in client.send_message(
                    request_with_webhook("hi", hook.url)
                )
            ]
            took = asyncio.get_running_loop().time() - started
            assert TaskState.Name(events[0].task.status.state) == "TASK_STATE_COMPLETED"
            # every notification of the task would have taken a second each
            assert took < 0.8, took
            await app._a2a.push_sender._drain(0.05)

        run(scenario())

    def test_sending_is_only_queueing(self):
        calls = []

        class Store:
            async def get_info_for_dispatch(self, task_id):
                calls.append(task_id)
                await asyncio.sleep(10)
                return []

        async def scenario():
            sender = _PushSender(Store())
            from a2a.types import TaskStatus, TaskStatusUpdateEvent

            event = TaskStatusUpdateEvent(
                task_id="t1", status=TaskStatus(state=TaskState.TASK_STATE_WORKING)
            )
            await sender.send_notification("t1", event)
            # nothing was attempted before the call came back
            assert calls == []
            await sender._drain(0.01)

        run(scenario())

    def test_a_server_error_is_tried_again_and_then_given_up(self, receiver):
        hook = receiver(status=500)
        app = echo_app()

        async def scenario():
            client = await connect(app)
            events = [
                event
                async for event in client.send_message(
                    request_with_webhook("hi", hook.url)
                )
            ]
            record = await settle(app, events[0].task.id, timeout=10)
            assert record.terminal_delivered is False
            assert record.last_status == 500
            assert record.delivered == 0

        run(scenario())
        # four attempts for each event: the first and three more
        assert len(hook.requests) % 4 == 0
        assert len(hook.requests) >= 4

    def test_a_redirect_is_a_failed_delivery_not_a_second_destination(self, receiver):
        elsewhere = receiver()
        hook = receiver(status=302, location=elsewhere.url)
        app = echo_app()

        async def scenario():
            client = await connect(app)
            events = [
                event
                async for event in client.send_message(
                    request_with_webhook("hi", hook.url)
                )
            ]
            record = await settle(app, events[0].task.id)
            assert record.last_status == 302
            assert record.terminal_delivered is False
            return record

        record = run(scenario())
        assert elsewhere.requests == []
        # one attempt an event: a redirect is not tried again
        assert record.attempts == len(hook.requests)

    def test_a_refusal_is_not_tried_again(self, receiver):
        hook = receiver(status=403)
        app = echo_app()

        async def scenario():
            client = await connect(app)
            events = [
                event
                async for event in client.send_message(
                    request_with_webhook("hi", hook.url)
                )
            ]
            record = await settle(app, events[0].task.id)
            assert record.attempts == len(hook.requests)

        run(scenario())

    def test_a_full_queue_drops_the_oldest_event_that_is_not_the_last(self):
        from a2a.types import TaskStatus, TaskStatusUpdateEvent

        def event(state):
            return TaskStatusUpdateEvent(task_id="t", status=TaskStatus(state=state))

        async def scenario():
            queue = asyncio.Queue()
            done = event(TaskState.TASK_STATE_COMPLETED)
            first = event(TaskState.TASK_STATE_WORKING)
            queue.put_nowait(done)
            queue.put_nowait(first)
            queue.put_nowait(event(TaskState.TASK_STATE_WORKING))
            record = _Delivery()
            _PushSender._drop_oldest(queue, record)
            assert record.dropped == 1
            kept = [queue.get_nowait() for _ in range(queue.qsize())]
            assert kept[0] is done
            assert all(event is not first for event in kept)
            assert len(kept) == 2

        run(scenario())


class TestWhereAWebhookMayBe:
    def test_the_request_goes_to_the_address_that_was_checked(
        self, receiver, monkeypatch
    ):
        hook = receiver()
        port = hook.server.server_address[1]
        # a name whose answer is this receiver: the request must reach the
        # address the check saw, with the name kept for the Host header
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["127.0.0.1"])
        app = echo_app()

        async def scenario():
            client = await connect(app)
            events = [
                event
                async for event in client.send_message(
                    request_with_webhook("hi", f"http://hooks.example.test:{port}/hook")
                )
            ]
            record = await settle(app, events[0].task.id)
            assert record.terminal_delivered is True

        run(scenario())
        assert hook.requests[-1]["headers"]["host"] == f"hooks.example.test:{port}"

    def test_an_answer_that_turns_private_is_refused_at_delivery(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        answers = iter([["93.184.216.34"], ["10.0.0.7"]])
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: next(answers))
        # public when the config was stored
        assert run(_valid_webhook("https://hooks.example.test/x")) is True
        # private by the time of delivery: checked again, and refused
        with pytest.raises(AgentError, match="private address"):
            _pin_webhook("https://hooks.example.test/x")

    @pytest.mark.parametrize(
        ("url", "addresses"),
        [
            ("http://hooks.example.test/x", ["93.184.216.34"]),
            ("https://hooks.example.test/x", ["10.1.2.3"]),
            ("https://hooks.example.test/x", ["127.0.0.1"]),
            ("https://hooks.example.test/x", ["169.254.169.254"]),
            ("ftp://hooks.example.test/x", ["93.184.216.34"]),
        ],
    )
    def test_a_deployment_refuses_what_it_must_not_reach(
        self, monkeypatch, url, addresses
    ):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: addresses)
        assert run(_valid_webhook(url)) is False

    def test_the_platforms_internal_list_lets_a_cluster_receiver_through(
        self, monkeypatch
    ):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["10.1.2.3"])
        monkeypatch.setenv("HOPSWORKS_AGENT_PUSH_INTERNAL_ALLOW", "10.1.0.0/16")
        assert run(_valid_webhook("https://gateway.internal/x")) is True

    def test_an_administrators_host_list_narrows_it(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setattr(
            a2a_module, "_resolve", lambda host, port: ["93.184.216.34"]
        )
        monkeypatch.setenv(a2a_push.ALLOWED_HOSTS_ENV, "hooks.example.test")
        assert run(_valid_webhook("https://hooks.example.test/x")) is True
        assert run(_valid_webhook("https://other.example.test/x")) is False

    def test_a_refused_webhook_is_refused_when_it_is_registered(self, monkeypatch):
        from a2a.utils.errors import InvalidParamsError

        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["10.1.2.3"])
        from hopsworks_agents.protocol.a2a import A2AConfig

        app = echo_app(a2a=A2AConfig(task_store="memory"))

        async def scenario():
            client = await connect(app)
            with pytest.raises(InvalidParamsError):
                async for _ in client.send_message(
                    request_with_webhook("hi", "https://internal.example.test/x")
                ):
                    pass

        run(scenario())


class TestTheTerminalOutbox:
    """With shared tasks, the terminal notification is owed until it was delivered."""

    @pytest.fixture
    def database(self, tmp_path, monkeypatch):
        monkeypatch.setenv(
            a2a_store.DATABASE_URL_ENV, f"sqlite+aiosqlite:///{tmp_path / 'a2a.db'}"
        )

    async def _rows(self, app):
        from sqlalchemy import select

        leases = app._a2a.stores.leases
        async with app._a2a.stores.engine.connect() as conn:
            return (await conn.execute(select(leases.table))).all()

    def test_a_delivered_terminal_event_leaves_nothing_owed(self, database, receiver):
        hook = receiver()
        app = echo_app()

        async def scenario():
            client = await connect(app)
            events = [
                event
                async for event in client.send_message(
                    request_with_webhook("hi", hook.url)
                )
            ]
            await settle(app, events[0].task.id)
            for _ in range(100):
                if not await self._rows(app):
                    break
                await asyncio.sleep(0.02)
            assert await self._rows(app) == []

        run(scenario())
        assert hook.states()[-1] == "TASK_STATE_COMPLETED"

    def test_an_undelivered_one_stays_owed_for_the_platform(self, database, receiver):
        hook = receiver(status=500)
        app = echo_app()

        async def scenario():
            client = await connect(app)
            events = [
                event
                async for event in client.send_message(
                    request_with_webhook("hi", hook.url)
                )
            ]
            task = events[0].task
            await settle(app, task.id, timeout=10)
            rows = await self._rows(app)
            assert [(r.task_id, r.terminal_pushed) for r in rows] == [(task.id, False)]
            assert rows[0].terminal_at is not None

        run(scenario())

    def test_a_task_nobody_listens_to_owes_nothing(self, database):
        app = echo_app()

        async def scenario():
            client = await connect(app)
            request = SendMessageRequest(
                message=Message(
                    role=Role.ROLE_USER, message_id="m", parts=[Part(text="hi")]
                )
            )
            [event async for event in client.send_message(request)]
            for _ in range(100):
                if not await self._rows(app):
                    break
                await asyncio.sleep(0.02)
            assert await self._rows(app) == []

        run(scenario())


class TestPushNeedsTheDeploymentsKey:
    """On a deployment the configs hold a receiver's credentials, and are stored encrypted or not at all."""

    def _deployment(self, tmp_path, monkeypatch, key):
        monkeypatch.setenv(
            a2a_store.DATABASE_URL_ENV, f"sqlite+aiosqlite:///{tmp_path / 'a2a.db'}"
        )
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        if key:
            monkeypatch.setenv(a2a_store.PUSH_KEY_ENV, key)
        else:
            monkeypatch.delenv(a2a_store.PUSH_KEY_ENV, raising=False)
        return echo_app()

    def test_without_the_key_push_is_not_served(self, tmp_path, monkeypatch):
        from a2a.utils.errors import PushNotificationNotSupportedError
        from fastapi.testclient import TestClient

        app = self._deployment(tmp_path, monkeypatch, None)
        assert app._a2a.push is False
        client = TestClient(app)
        card = client.get("/.well-known/agent-card.json").json()
        assert card["capabilities"]["pushNotifications"] is False
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        a2a = manifest["capabilities"]["custom"]["hopsworks"]["a2a"]
        assert a2a["push_notifications"] is False

        async def scenario():
            sdk = await connect(app)
            # a task is still served; a webhook sent with it is stored nowhere
            events = [
                event
                async for event in sdk.send_message(
                    request_with_webhook("hi", "https://hooks.example.test/x")
                )
            ]
            task = events[0].task
            assert TaskState.Name(task.status.state) == "TASK_STATE_COMPLETED"
            from a2a.types import ListTaskPushNotificationConfigsRequest

            with pytest.raises(PushNotificationNotSupportedError):
                await sdk.list_task_push_notification_configs(
                    ListTaskPushNotificationConfigsRequest(task_id=task.id)
                )

        run(scenario())

    def test_with_the_key_the_stored_config_is_not_readable(
        self, tmp_path, monkeypatch
    ):
        from cryptography.fernet import Fernet

        monkeypatch.setattr(
            a2a_module, "_resolve", lambda host, port: ["93.184.216.34"]
        )
        app = self._deployment(tmp_path, monkeypatch, Fernet.generate_key().decode())
        assert app._a2a.push is True

        async def scenario():
            from sqlalchemy import select

            sdk = await connect(app)
            [
                event
                async for event in sdk.send_message(
                    request_with_webhook(
                        "hi",
                        "https://hooks.example.test/x",
                        authentication={"scheme": "ApiKey", "credentials": "cb-secret"},
                    )
                )
            ]
            model = app._a2a.stores.push_configs.config_model
            async with app._a2a.stores.engine.connect() as conn:
                rows = (await conn.execute(select(model.config_data))).all()
            assert rows and all(b"cb-secret" not in row[0] for row in rows)
            await app._a2a.push_sender._drain(0.05)

        run(scenario())

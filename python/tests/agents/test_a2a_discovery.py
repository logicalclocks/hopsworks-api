"""A2A phase C in the agent: gRPC, the agent's own hostname and the public card."""

import asyncio
import socket

import grpc
import grpc.aio
import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import (
    GetExtendedAgentCardRequest,
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageConfiguration,
    SendMessageRequest,
    a2a_pb2_grpc,
)
from a2a.utils.constants import TransportProtocol
from fastapi.testclient import TestClient
from hopsworks_agents.protocol import AgentApp, InMemoryAgentMemory, a2a_store
from hopsworks_agents.protocol.a2a import A2AConfig


BASE = "http://agent.test"
HOST = "advisor-42.agents.example.com"
PRINCIPAL = "x-hopsworks-principal"


def run(coro):
    return asyncio.run(coro)


def free_port():
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.bind(("::", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
def local(monkeypatch):
    """A local run with tasks in memory, and none of the platform's discovery settings."""
    for name in (
        "DEPLOYMENT_ID",
        a2a_store.DATABASE_URL_ENV,
        "HOPSWORKS_AGENT_PUBLIC_URL",
        "HOPSWORKS_AGENT_GRPC_URL",
        "HOPSWORKS_AGENT_GRPC_PORT",
        "HOPSWORKS_AGENT_PUBLIC_CARD",
        "HOPSWORKS_AGENT_HOSTNAME_OFF_REASON",
        "HOPSWORKS_AGENT_CARD_SIGN_URL",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def grpc_port(monkeypatch):
    port = free_port()
    monkeypatch.setenv("HOPSWORKS_AGENT_GRPC_URL", f"localhost:{port}")
    monkeypatch.setenv("HOPSWORKS_AGENT_GRPC_PORT", str(port))
    return port


def echo_app(**kwargs):
    app = AgentApp(name="Advisor", a2a=A2AConfig(task_store="memory"), **kwargs)

    @app.chat
    async def chat(request, ctx):
        return "echo: " + request.text

    return app


def message(text):
    return SendMessageRequest(
        message=Message(
            role=Role.ROLE_USER, message_id=f"m-{text}", parts=[Part(text=text)]
        ),
        configuration=SendMessageConfiguration(),
    )


def bindings(card_json, version="1.0"):
    return [
        (i["protocolBinding"], i["url"])
        for i in card_json["supportedInterfaces"]
        if i["protocolVersion"] == version
    ]


def a2a_view(app):
    manifest = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
    return manifest["capabilities"]["custom"]["hopsworks"]["a2a"]


class _Serving:
    """The app's A2A startup and shutdown, as the lifespan runs them."""

    def __init__(self, app):
        self.app = app

    async def __aenter__(self):
        await self.app._a2a._start()
        return self.app

    async def __aexit__(self, *exc):
        await self.app._a2a._stop()


class TestCardInterfaces:
    def test_without_a_grpc_url_the_card_names_http_only(self):
        card = TestClient(echo_app()).get("/.well-known/agent-card.json").json()
        assert [b for b, _ in bindings(card)] == ["JSONRPC", "HTTP+JSON"]
        assert "GRPC" not in [b for b, _ in bindings(card, "0.3")]

    def test_with_a_grpc_url_the_card_names_it_third(self, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_AGENT_GRPC_URL", f"{HOST}:443")
        card = TestClient(echo_app()).get("/.well-known/agent-card.json").json()
        assert bindings(card) == [
            ("JSONRPC", "http://testserver/a2a"),
            ("HTTP+JSON", "http://testserver/a2a/rest"),
            ("GRPC", f"{HOST}:443"),
        ]
        # a 0.3 client finds it too, after the HTTP bindings it prefers
        assert bindings(card, "0.3")[-1] == ("GRPC", f"{HOST}:443")
        assert card["preferredTransport"] == "JSONRPC"

    def test_without_grpcio_the_card_names_http_only(self, monkeypatch):
        import sys

        monkeypatch.setenv("HOPSWORKS_AGENT_GRPC_URL", f"{HOST}:443")
        # what importing the gRPC module does without grpcio: an ImportError
        monkeypatch.setitem(sys.modules, "hopsworks_agents.protocol.a2a_grpc", None)
        app = echo_app()
        card = TestClient(app).get("/.well-known/agent-card.json").json()
        assert "GRPC" not in [b for b, _ in bindings(card)]
        assert a2a_view(app)["grpc"] is False

    def test_a_port_that_cannot_be_opened_drops_the_interface(
        self, monkeypatch, grpc_port
    ):
        app = echo_app()
        blocker = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        blocker.bind(("::", grpc_port))
        blocker.listen()
        try:

            async def scenario():
                async with _Serving(app):
                    return a2a_view(app), TestClient(app).get(
                        "/.well-known/agent-card.json"
                    ).json()

            view, card = run(scenario())
        finally:
            blocker.close()
        assert view["grpc"] is False
        assert "GRPC" not in view["bindings"]
        assert "GRPC" not in [b for b, _ in bindings(card)]


class TestGrpcRoundTrip:
    def test_a_task_sent_over_grpc_is_read_over_grpc_and_http(self, grpc_port):
        app = echo_app()

        async def scenario():
            async with _Serving(app):
                http = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url=BASE
                )
                card = await A2ACardResolver(http, BASE).get_agent_card()
                # a client that speaks only gRPC picks it off the card
                client = ClientFactory(
                    ClientConfig(
                        grpc_channel_factory=grpc.aio.insecure_channel,
                        supported_protocol_bindings=[TransportProtocol.GRPC],
                        streaming=False,
                    )
                ).create(card)
                events = [e async for e in client.send_message(message("hi"))]
                task = events[-1].task
                assert task.artifacts[0].parts[0].text == "echo: hi"
                again = await client.get_task(GetTaskRequest(id=task.id))
                assert again.id == task.id
                # the same handler: the HTTP bindings see the task too
                over_http = ClientFactory(ClientConfig(httpx_client=http)).create(card)
                seen = await over_http.get_task(GetTaskRequest(id=task.id))
                assert seen.status.state == again.status.state
                await client.close()

        run(scenario())

    def test_the_03_service_is_served_too(self, grpc_port):
        from a2a.compat.v0_3 import a2a_v0_3_pb2, a2a_v0_3_pb2_grpc

        app = echo_app()

        async def scenario():
            async with (
                _Serving(app),
                grpc.aio.insecure_channel(f"localhost:{grpc_port}") as ch,
            ):
                stub = a2a_v0_3_pb2_grpc.A2AServiceStub(ch)
                answer = await stub.SendMessage(
                    a2a_v0_3_pb2.SendMessageRequest(
                        request=a2a_v0_3_pb2.Message(
                            message_id="m-1",
                            role=a2a_v0_3_pb2.ROLE_USER,
                            content=[a2a_v0_3_pb2.Part(text="old")],
                        )
                    )
                )
                assert answer.task.artifacts[0].parts[0].text == "echo: old"

        run(scenario())

    def test_the_manifest_says_grpc_is_served(self, grpc_port):
        app = echo_app()
        view = a2a_view(app)
        assert view["grpc"] is True
        assert view["bindings"] == ["JSONRPC", "HTTP+JSON", "GRPC"]

    def test_shutdown_closes_the_port(self, grpc_port):
        app = echo_app()

        async def scenario():
            async with _Serving(app):
                pass
            async with grpc.aio.insecure_channel(f"localhost:{grpc_port}") as ch:
                stub = a2a_pb2_grpc.A2AServiceStub(ch)
                with pytest.raises(grpc.aio.AioRpcError) as refused:
                    await stub.GetTask(GetTaskRequest(id="t"), timeout=2)
                return refused.value.code()

        assert run(scenario()) == grpc.StatusCode.UNAVAILABLE

    def test_the_drain_stops_taking_calls(self, grpc_port):
        app = echo_app()

        async def scenario():
            async with _Serving(app):
                app._a2a._stop_accepting(0)
                await asyncio.sleep(0.2)
                async with grpc.aio.insecure_channel(f"localhost:{grpc_port}") as ch:
                    stub = a2a_pb2_grpc.A2AServiceStub(ch)
                    with pytest.raises(grpc.aio.AioRpcError) as refused:
                        await stub.GetTask(GetTaskRequest(id="t"), timeout=2)
                    return refused.value.code()

        assert run(scenario()) == grpc.StatusCode.UNAVAILABLE


class TestPrivateOverGrpc:
    """``memory_routes=False``: over gRPC the caller is the gateway's metadata."""

    def _app(self):
        return echo_app(memory=InMemoryAgentMemory(), memory_routes=False)

    async def _call(self, port, method, request, principal=None):
        metadata = [("a2a-version", "1.0")]
        if principal is not None:
            metadata.append((PRINCIPAL, principal))
        async with grpc.aio.insecure_channel(f"localhost:{port}") as ch:
            stub = a2a_pb2_grpc.A2AServiceStub(ch)
            return await getattr(stub, method)(request, metadata=metadata, timeout=10)

    def test_each_caller_sees_only_its_own_tasks(self, grpc_port, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        app = self._app()

        async def scenario():
            async with _Serving(app):
                sent = await self._call(
                    grpc_port, "SendMessage", message("mine"), "user:alice"
                )
                task_id = sent.task.id
                read = await self._call(
                    grpc_port, "GetTask", GetTaskRequest(id=task_id), "user:alice"
                )
                assert read.id == task_id
                with pytest.raises(grpc.aio.AioRpcError) as other:
                    await self._call(
                        grpc_port, "GetTask", GetTaskRequest(id=task_id), "user:bob"
                    )
                assert other.value.code() == grpc.StatusCode.NOT_FOUND
                # and over HTTP the same caller is the same owner
                http = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url=BASE,
                    headers={PRINCIPAL: "user:alice"},
                )
                card = await A2ACardResolver(http, BASE).get_agent_card()
                client = ClientFactory(ClientConfig(httpx_client=http)).create(card)
                seen = await client.get_task(GetTaskRequest(id=task_id))
                assert seen.id == task_id

        run(scenario())

    def test_a_call_without_a_principal_is_refused_on_a_deployment(
        self, grpc_port, monkeypatch
    ):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        app = self._app()

        async def scenario():
            async with _Serving(app):
                for method, request in (
                    ("SendMessage", message("anon")),
                    ("GetTask", GetTaskRequest(id="t")),
                    ("GetExtendedAgentCard", GetExtendedAgentCardRequest()),
                ):
                    with pytest.raises(grpc.aio.AioRpcError) as refused:
                        await self._call(grpc_port, method, request)
                    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
                # a streaming call is refused the same way
                with pytest.raises(grpc.aio.AioRpcError) as refused:
                    async with grpc.aio.insecure_channel(
                        f"localhost:{grpc_port}"
                    ) as ch:
                        stub = a2a_pb2_grpc.A2AServiceStub(ch)
                        async for _ in stub.SendStreamingMessage(
                            message("anon"), timeout=10
                        ):
                            pass
                assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
                return app._a2a.executor.runs

        assert not run(scenario())

    def test_a_local_run_takes_the_caller_for_anonymous(self, grpc_port):
        app = self._app()

        async def scenario():
            async with _Serving(app):
                sent = await self._call(grpc_port, "SendMessage", message("local"))
                return sent.task.artifacts[0].parts[0].text

        assert run(scenario()) == "echo: local"


class TestHostnameAndPublicCard:
    def test_the_manifest_names_the_agents_hostname(self, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_URL", f"https://{HOST}")
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_CARD", "true")
        view = a2a_view(echo_app())
        assert view["hostname"] == HOST
        assert view["public_card"] is True
        assert "hostname_off" not in view

    def test_the_path_form_is_not_a_hostname(self, monkeypatch):
        monkeypatch.setenv(
            "HOPSWORKS_AGENT_PUBLIC_URL", "https://gateway.example/v1/proj/advisor"
        )
        monkeypatch.setenv("HOPSWORKS_AGENT_HOSTNAME_OFF_REASON", "gateway_api_off")
        view = a2a_view(echo_app())
        assert view["hostname"] is None
        assert view["hostname_off"] == "gateway_api_off"
        assert view["public_card"] is False

    def test_the_platforms_reason_wins_over_a_bare_url(self, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_URL", "https://gateway.example")
        monkeypatch.setenv("HOPSWORKS_AGENT_HOSTNAME_OFF_REASON", "no_domain")
        view = a2a_view(echo_app())
        assert view["hostname"] is None
        assert view["hostname_off"] == "no_domain"

    def test_the_extended_card_carries_the_discovery_facts(self, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_URL", f"https://{HOST}")
        monkeypatch.setenv("HOPSWORKS_AGENT_GRPC_URL", f"{HOST}:443")
        app = echo_app()
        card = app._a2a._extended_card(f"https://{HOST}")
        from google.protobuf.json_format import MessageToDict

        params = MessageToDict(card.capabilities.extensions[0].params)
        a2a = params["agent"]["hopsworks"]["a2a"]
        assert a2a["hostname"] == HOST
        assert a2a["grpc"] is True

    @pytest.mark.parametrize("public", ["true", None])
    def test_a_private_agent_serves_the_public_card_to_anyone(
        self, monkeypatch, public
    ):
        # The gateway decides who reaches the card: with the public card on,
        # its card route has no authentication. The pod serves the card to
        # whoever reaches it, the platform's directory included, which reads
        # it in-cluster without a principal.
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_URL", f"https://{HOST}")
        if public:
            monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_CARD", public)
        client = TestClient(echo_app(memory=InMemoryAgentMemory(), memory_routes=False))
        card = client.get("/.well-known/agent-card.json")
        assert card.status_code == 200
        # the public card: the Hopsworks extension says nothing of the agent
        params = card.json()["capabilities"]["extensions"][0]["params"]
        assert "agent" not in params

    def test_the_extended_card_is_never_anonymous_in_a_private_agent(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_URL", f"https://{HOST}")
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_CARD", "true")
        client = TestClient(echo_app(memory=InMemoryAgentMemory(), memory_routes=False))
        rest = client.get("/a2a/rest/extendedAgentCard", headers={"A2A-Version": "1.0"})
        assert rest.status_code == 401
        rpc = client.post(
            "/a2a",
            headers={"A2A-Version": "1.0"},
            json={"jsonrpc": "2.0", "id": 1, "method": "GetExtendedAgentCard"},
        )
        assert rpc.status_code == 401
        # with the gateway's principal it is served
        rpc = client.post(
            "/a2a",
            headers={"A2A-Version": "1.0", PRINCIPAL: "user:alice"},
            json={"jsonrpc": "2.0", "id": 1, "method": "GetExtendedAgentCard"},
        )
        assert (
            "agent" in rpc.json()["result"]["capabilities"]["extensions"][0]["params"]
        )


class TestAnEmptyPrincipalIsNoCaller:
    """On the Istio path the authenticator blanks the principal headers of an anonymous request.

    An empty ``x-hopsworks-principal`` (and ``x-hopsworks-auth-kind``) is how
    the public card request reaches the pod there, overwriting whatever the
    client sent; it names no caller, exactly like a missing header.
    """

    BLANK = {PRINCIPAL: "", "x-hopsworks-auth-kind": ""}

    @pytest.fixture
    def client(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_URL", f"https://{HOST}")
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_CARD", "true")
        return TestClient(echo_app(memory=InMemoryAgentMemory(), memory_routes=False))

    def test_the_public_card_is_served(self, client):
        card = client.get("/.well-known/agent-card.json", headers=self.BLANK)
        assert card.status_code == 200
        assert "agent" not in card.json()["capabilities"]["extensions"][0]["params"]

    @pytest.mark.parametrize(
        ("method", "path", "body"),
        [
            (
                "POST",
                "/a2a",
                {"jsonrpc": "2.0", "id": 1, "method": "GetExtendedAgentCard"},
            ),
            ("GET", "/a2a/rest/extendedAgentCard", None),
            ("GET", "/a2a/rest/tasks/t-1", None),
            ("POST", "/v1/chat", {"messages": [{"role": "user", "content": "hi"}]}),
        ],
    )
    def test_everything_else_is_refused(self, client, method, path, body):
        refused = client.request(
            method,
            path,
            headers={**self.BLANK, "A2A-Version": "1.0"},
            json=body,
        )
        assert refused.status_code == 401
        assert refused.json()["detail"]["code"] == "principal_required"

    def test_a_blank_principal_is_refused_over_grpc(self, monkeypatch, grpc_port):
        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        app = echo_app(memory=InMemoryAgentMemory(), memory_routes=False)

        async def scenario():
            async with (
                _Serving(app),
                grpc.aio.insecure_channel(f"localhost:{grpc_port}") as ch,
            ):
                stub = a2a_pb2_grpc.A2AServiceStub(ch)
                with pytest.raises(grpc.aio.AioRpcError) as refused:
                    await stub.SendMessage(
                        message("anon"),
                        metadata=[(PRINCIPAL, ""), ("x-hopsworks-auth-kind", "")],
                        timeout=10,
                    )
                return refused.value.code()

        assert run(scenario()) == grpc.StatusCode.UNAUTHENTICATED

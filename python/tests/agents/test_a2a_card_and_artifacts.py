"""Large parts kept beside the database, the extended card, and the platform's signature on the card."""

import asyncio
import base64
import json

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import (
    AgentCard,
    GetExtendedAgentCardRequest,
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
)
from a2a.utils.signing import (
    InvalidSignaturesError,
    create_signature_verifier,
)
from fastapi.testclient import TestClient
from google.protobuf.json_format import MessageToDict, ParseDict
from hopsworks_agents.protocol import (
    AgentApp,
    AgentResponse,
    Artifact,
    InMemoryAgentMemory,
    a2a_artifacts,
    a2a_store,
)
from hopsworks_agents.protocol.models import AgentError


BASE = "http://agent.test"


def run(coro):
    return asyncio.run(coro)


async def connect(app, *, headers=None):
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE, headers=headers or {}
    )
    card = await A2ACardResolver(http, BASE).get_agent_card()
    return ClientFactory(ClientConfig(httpx_client=http)).create(card), http


def message(text):
    return SendMessageRequest(
        message=Message(
            role=Role.ROLE_USER, message_id=f"m-{text}", parts=[Part(text=text)]
        )
    )


async def ask(client, text):
    events = [event async for event in client.send_message(message(text))]
    return events[0].task


def artifact_part(task, name):
    return next(a for a in task.artifacts if a.name == name).parts[0]


@pytest.fixture
def shared(tmp_path, monkeypatch):
    """Tasks in a database, large parts in a directory beside it."""
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    monkeypatch.setenv(
        a2a_store.DATABASE_URL_ENV, f"sqlite+aiosqlite:///{tmp_path / 'a2a.db'}"
    )
    monkeypatch.setenv(a2a_artifacts.ARTIFACT_DIR_ENV, str(tmp_path / "artifacts"))
    monkeypatch.setenv(a2a_artifacts.INLINE_BYTES_ENV, "1000")
    return tmp_path


def reporting_app(payloads, **kwargs):
    """An agent that answers with one file artifact per payload."""
    app = AgentApp(name="Reporter", **kwargs)

    @app.chat
    async def chat(request, ctx):
        return AgentResponse.text(
            "done",
            artifacts=[
                Artifact(
                    name=name,
                    media_type="application/octet-stream",
                    data=base64.b64encode(data).decode(),
                )
                for name, data in payloads.items()
            ],
        )

    return app


class TestLargeParts:
    def test_a_large_part_is_stored_as_a_url_and_served_back(self, shared):
        big, small = b"x" * 5000, b"y" * 10
        app = reporting_app({"big.bin": big, "small.bin": small})

        async def scenario():
            client, http = await connect(app)
            task = await ask(client, "go")
            stored = await client.get_task(GetTaskRequest(id=task.id))
            part = artifact_part(stored, "big.bin")
            assert part.WhichOneof("content") == "url"
            facts = MessageToDict(part.metadata)["hopsworks"]
            assert facts["spilled"] is True and facts["size"] == 5000
            # the small one stays where it was
            assert artifact_part(stored, "small.bin").raw == small
            path = part.url[part.url.index("/a2a/artifacts") :]
            response = await http.get(path)
            assert response.status_code == 200
            assert response.content == big
            assert response.headers["content-type"] == "application/octet-stream"
            return task.id, path

        task_id, path = run(scenario())
        digest = path.rsplit("/", 1)[-1]
        assert (shared / "artifacts" / task_id / digest).read_bytes() == big

    def test_a_digest_is_not_a_key_without_the_task_that_carries_it(self, shared):
        app = reporting_app({"big.bin": b"x" * 5000})

        async def scenario():
            client, http = await connect(app)
            first = await ask(client, "one")
            second_app_task = await ask(client, "two")
            stored = await client.get_task(GetTaskRequest(id=first.id))
            digest = artifact_part(stored, "big.bin").url.rsplit("/", 1)[-1]
            unknown = "0" * 64
            assert (
                await http.get(f"/a2a/artifacts/{first.id}/{unknown}")
            ).status_code == 404
            assert (
                await http.get(f"/a2a/artifacts/no-such-task/{digest}")
            ).status_code == 404
            assert (
                await http.get(f"/a2a/artifacts/{second_app_task.id}/../{digest}")
            ).status_code == 404

        run(scenario())

    def test_in_a_private_agent_only_the_tasks_caller_reads_it(self, shared):
        app = reporting_app(
            {"big.bin": b"x" * 5000},
            memory=InMemoryAgentMemory(),
            memory_routes=False,
        )

        async def scenario():
            alice, alice_http = await connect(
                app, headers={"x-hopsworks-principal": "user:alice"}
            )
            _bob, bob_http = await connect(
                app, headers={"x-hopsworks-principal": "user:bob"}
            )
            task = await ask(alice, "go")
            stored = await alice.get_task(GetTaskRequest(id=task.id))
            url = artifact_part(stored, "big.bin").url
            path = url[url.index("/a2a/artifacts") :]
            assert (await alice_http.get(path)).status_code == 200
            assert (await bob_http.get(path)).status_code == 404

        run(scenario())

    def test_many_small_parts_past_the_tasks_cap_go_largest_first(
        self, shared, monkeypatch
    ):
        monkeypatch.setenv(a2a_artifacts.INLINE_BYTES_ENV, "100000")
        monkeypatch.setenv(a2a_artifacts.TASK_MAX_BYTES_ENV, "6000")
        payloads = {
            "a.bin": b"a" * 3000,
            "b.bin": b"b" * 2500,
            "c.bin": b"c" * 1500,
        }
        app = reporting_app(payloads)

        async def scenario():
            client, _ = await connect(app)
            task = await ask(client, "go")
            stored = await client.get_task(GetTaskRequest(id=task.id))
            kinds = {
                a.name: a.parts[0].WhichOneof("content")
                for a in stored.artifacts
                if a.name.endswith(".bin")
            }
            assert kinds["a.bin"] == "url"
            assert kinds["c.bin"] == "raw"
            assert stored.ByteSize() <= 6000

        run(scenario())

    def test_without_shared_storage_parts_stay_inline(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        monkeypatch.delenv(a2a_artifacts.ARTIFACT_DIR_ENV, raising=False)
        monkeypatch.setenv(
            a2a_store.DATABASE_URL_ENV, f"sqlite+aiosqlite:///{tmp_path / 'a2a.db'}"
        )
        monkeypatch.setenv(a2a_artifacts.INLINE_BYTES_ENV, "1000")
        app = reporting_app({"big.bin": b"x" * 5000})
        assert app._a2a.spill is None

        async def scenario():
            client, _ = await connect(app)
            task = await ask(client, "go")
            stored = await client.get_task(GetTaskRequest(id=task.id))
            assert artifact_part(stored, "big.bin").raw == b"x" * 5000

        run(scenario())


class TestTheCapHoldsForEveryKindOfContent:
    def _talker(self, text):
        app = AgentApp(name="Talker")

        @app.chat
        async def chat(request, ctx):
            return text

        return app

    def test_a_long_text_answer_is_moved_out_like_a_file(self, shared, monkeypatch):
        monkeypatch.setenv(a2a_artifacts.TASK_MAX_BYTES_ENV, "10000")
        answer = "word " * 10000
        app = self._talker(answer)

        async def scenario():
            client, http = await connect(app)
            task = await ask(client, "go")
            stored = await client.get_task(GetTaskRequest(id=task.id))
            assert stored.ByteSize() <= 10000
            part = artifact_part(stored, "response")
            assert part.WhichOneof("content") == "url"
            assert part.media_type.startswith("text/plain")
            path = part.url[part.url.index("/a2a/artifacts") :]
            assert (await http.get(path)).text == answer

        run(scenario())

    def test_without_storage_what_does_not_fit_is_dropped_and_said(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        monkeypatch.delenv(a2a_artifacts.ARTIFACT_DIR_ENV, raising=False)
        monkeypatch.setenv(
            a2a_store.DATABASE_URL_ENV, f"sqlite+aiosqlite:///{tmp_path / 'a2a.db'}"
        )
        monkeypatch.setenv(a2a_artifacts.TASK_MAX_BYTES_ENV, "10000")
        app = self._talker("word " * 10000)

        async def scenario():
            client, _ = await connect(app)
            task = await ask(client, "go")
            stored = await client.get_task(GetTaskRequest(id=task.id))
            assert stored.ByteSize() <= 10000
            facts = MessageToDict(stored.metadata)["hopsworks"]
            assert facts["truncated"] is True
            assert facts["omitted_parts"] >= 1
            assert "omitted" in artifact_part(stored, "response").text

        run(scenario())

    def test_a_storage_that_fails_does_not_let_the_task_through_whole(
        self, shared, monkeypatch
    ):
        monkeypatch.setenv(a2a_artifacts.TASK_MAX_BYTES_ENV, "10000")
        app = reporting_app({"big.bin": b"x" * 50000})

        def broken(task_id, digest, data):
            raise OSError("the file system is gone")

        monkeypatch.setattr(app._a2a.spill, "_write", broken)

        async def scenario():
            client, _ = await connect(app)
            task = await ask(client, "go")
            stored = await client.get_task(GetTaskRequest(id=task.id))
            assert stored.ByteSize() <= 10000
            assert MessageToDict(stored.metadata)["hopsworks"]["truncated"] is True

        run(scenario())

    def test_an_old_history_goes_before_any_content(self, shared, monkeypatch):
        from a2a.types import Message, Part, Role, Task, TaskState, TaskStatus

        monkeypatch.setenv(a2a_artifacts.TASK_MAX_BYTES_ENV, "4000")
        store = a2a_artifacts._SpillingStore(None, None, lambda: None)
        task = Task(
            id="t1",
            context_id="c",
            status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
        )
        for index in range(20):
            task.history.add().CopyFrom(
                Message(
                    message_id=f"m{index}",
                    role=Role.ROLE_USER,
                    parts=[Part(text=f"{index}:" + "x" * 500)],
                )
            )
        run(store._fit(task))
        assert task.ByteSize() <= 4000
        # the newest messages are the ones kept
        assert task.history[-1].message_id == "m19"
        facts = MessageToDict(task.metadata)["hopsworks"]
        assert facts["dropped_history_messages"] >= 13
        assert facts["omitted_parts"] == 0


class TestExtendedCard:
    def test_an_authenticated_caller_gets_what_the_manifest_knows(self, monkeypatch):
        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        app = AgentApp(name="Analyst", memory=InMemoryAgentMemory())

        @app.chat
        async def chat(request, ctx):
            return "ok"

        async def scenario():
            client, http = await connect(app)
            public = await A2ACardResolver(http, BASE).get_agent_card()
            assert public.capabilities.extended_agent_card is True
            extended = await client.get_extended_agent_card(
                GetExtendedAgentCardRequest()
            )
            params = MessageToDict(extended.capabilities.extensions[0].params)
            agent = params["agent"]
            assert agent["endpoints"]["conversations"] == "/v1/conversations"
            assert agent["hopsworks"]["memory"]["conversation_history"] is True
            assert agent["hopsworks"]["a2a"]["task_store"] == "memory"
            # the public card says none of it
            assert "agent" not in MessageToDict(
                public.capabilities.extensions[0].params
            )
            assert extended.supported_interfaces[0].url == f"{BASE}/a2a"

        run(scenario())


class _Platform:
    """The platform's signing endpoint, as the deployment's own client sees it."""

    configured = True
    deployment_id = "42"
    project_id = "119"

    def __init__(self, refuse=False):
        from cryptography.hazmat.primitives.asymmetric import ec

        self.key = ec.generate_private_key(ec.SECP256R1())
        self.refuse = refuse
        self.requests = []

    def _post_as_deployment(self, url, body):
        from jwt import api_jws

        self.requests.append((url, body))
        if self.refuse:
            raise AgentError("not found", "platform_refused", 404)
        token = api_jws.encode(
            payload=body["payload"].encode(),
            key=self.key,
            algorithm="ES256",
            headers={"typ": "JOSE", "kid": "k1", "jku": "https://hw/jwks.json"},
        )
        protected, _, signature = token.split(".")
        return {"signature": {"protected": protected, "signature": signature}}


class TestSignedCard:
    PUBLIC = "https://gateway.example/v1/proj/advisor"
    SIGN_URL = "https://hopsworks.internal/hopsworks-api/api/project/119/serving/42/agents/card/sign"

    @pytest.fixture(autouse=True)
    def deployment(self, monkeypatch):
        monkeypatch.delenv(a2a_store.DATABASE_URL_ENV, raising=False)
        monkeypatch.setenv("HOPSWORKS_AGENT_PUBLIC_URL", self.PUBLIC)
        monkeypatch.setenv("HOPSWORKS_AGENT_CARD_SIGN_URL", self.SIGN_URL)

    def _app(self, platform):
        from hopsworks_agents.protocol.a2a import A2AConfig

        app = AgentApp(
            name="Advisor", platform=platform, a2a=A2AConfig(task_store="memory")
        )

        @app.chat
        async def chat(request, ctx):
            return "ok"

        return app

    def test_the_served_card_verifies_against_the_platforms_key(self):
        platform = _Platform()
        app = self._app(platform)
        run(app._a2a._sign())
        url, body = platform.requests[0]
        assert url == self.SIGN_URL
        # what was sent is the card's canonical form, with the URLs to check
        sent = json.loads(body["payload"])
        assert sent["supportedInterfaces"][0]["url"] == f"{self.PUBLIC}/a2a"
        client = TestClient(app)
        served = client.get("/.well-known/agent-card.json").json()
        assert len(served["signatures"]) == 1
        card = ParseDict(served, AgentCard(), ignore_unknown_fields=True)
        verify = create_signature_verifier(
            lambda kid, jku: platform.key.public_key(), ["ES256"]
        )
        verify(card)
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["custom"]["hopsworks"]["a2a"]["signed"] is True

    def test_a_card_that_was_changed_does_not_verify(self):
        platform = _Platform()
        app = self._app(platform)
        run(app._a2a._sign())
        served = TestClient(app).get("/.well-known/agent-card.json").json()
        served["supportedInterfaces"][0]["url"] = "https://evil.example/a2a"
        card = ParseDict(served, AgentCard(), ignore_unknown_fields=True)
        verify = create_signature_verifier(
            lambda kid, jku: platform.key.public_key(), ["ES256"]
        )
        with pytest.raises(InvalidSignaturesError):
            verify(card)

    def test_a_platform_that_cannot_sign_leaves_the_card_unsigned(self):
        platform = _Platform(refuse=True)
        app = self._app(platform)
        run(app._a2a._sign())
        client = TestClient(app)
        served = client.get("/.well-known/agent-card.json").json()
        assert not served.get("signatures")
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["custom"]["hopsworks"]["a2a"]["signed"] is False

    def test_an_older_platform_with_no_signing_url_is_not_asked(self, monkeypatch):
        monkeypatch.delenv("HOPSWORKS_AGENT_CARD_SIGN_URL", raising=False)
        platform = _Platform()
        app = self._app(platform)
        run(app._a2a._sign())
        assert platform.requests == []

    def test_the_request_carries_the_deployments_own_key(self, monkeypatch):
        import requests
        from hopsworks_agents.protocol.platform import PlatformClient

        seen = {}

        class Answer:
            status_code = 200

            def json(self):
                return {"signature": {"protected": "p", "signature": "s"}}

        def post(url, json, headers, verify, timeout):
            seen.update(url=url, headers=headers, body=json)
            return Answer()

        monkeypatch.setattr(requests, "post", post)
        monkeypatch.setenv("SERVING_API_KEY", "dep-key.secret")
        client = PlatformClient()
        monkeypatch.setattr(
            client, "_connect", lambda: type("C", (), {"_verify": "/ca.pem"})()
        )
        answer = client._post_as_deployment(self.SIGN_URL, {"payload": "{}"})
        assert answer["signature"]["signature"] == "s"
        assert seen["headers"] == {"Authorization": "ApiKey dep-key.secret"}
        assert seen["url"] == self.SIGN_URL

    def test_a_local_run_asks_nobody(self, monkeypatch):
        monkeypatch.delenv("HOPSWORKS_AGENT_PUBLIC_URL", raising=False)
        platform = _Platform()
        app = self._app(platform)
        run(app._a2a._sign())
        assert platform.requests == []

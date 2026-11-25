"""The A2A face of ``AgentApp``, driven with the official client over the app in process."""

import asyncio
import base64
import json
import sys

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
from a2a.utils.errors import InvalidParamsError, TaskNotCancelableError
from fastapi.testclient import TestClient
from google.protobuf.json_format import MessageToDict, ParseDict
from hopsworks_agents.protocol import (
    AgentApp,
    AgentError,
    AgentResponse,
    AgentSkill,
    Artifact,
    TextContent,
)
from hopsworks_agents.protocol.pending import _task_key


BASE = "http://agent.test"


def run(coro):
    return asyncio.run(coro)


async def connect(app, *, streaming=False, polling=False, bindings=None, headers=None):
    """The SDK client against the app, over httpx's ASGI transport."""
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=BASE, headers=headers or {}
    )
    card = await A2ACardResolver(http, BASE).get_agent_card()
    config = ClientConfig(httpx_client=http, streaming=streaming, polling=polling)
    if bindings:
        config.supported_protocol_bindings = bindings
    return ClientFactory(config).create(card), card


def user_message(
    text=None, *, context_id=None, task_id=None, parts=None, hopsworks=None
):
    message = Message(
        role=Role.ROLE_USER,
        message_id=f"msg-{abs(hash((text, context_id, task_id))) % 10**8}",
        parts=parts if parts is not None else [Part(text=text)],
    )
    if context_id:
        message.context_id = context_id
    if task_id:
        message.task_id = task_id
    if hopsworks:
        message.metadata.update({"hopsworks": hopsworks})
    return message


def data_part(value):
    return ParseDict({"data": value}, Part())


async def send(client, message, *, return_immediately=False, accepted=None):
    """One SendMessage; the task it answered with, or the events when streaming."""
    configuration = SendMessageConfiguration(return_immediately=return_immediately)
    if accepted:
        configuration.accepted_output_modes.extend(accepted)
    request = SendMessageRequest(message=message, configuration=configuration)
    return [event async for event in client.send_message(request)]


def task_of(events):
    assert len(events) == 1 and events[0].HasField("task"), events
    return events[0].task


def state_name(task):
    return TaskState.Name(task.status.state)


def text_of(message):
    return "".join(p.text for p in message.parts if p.WhichOneof("content") == "text")


def artifact(task, name):
    found = [a for a in task.artifacts if a.name == name]
    assert found, [a.name for a in task.artifacts]
    return found[0]


def hopsworks_of(message):
    return (MessageToDict(message).get("metadata") or {}).get("hopsworks") or {}


def echo_app(**kwargs):
    app = AgentApp(name="Echo agent", description="echoes", **kwargs)

    @app.chat
    async def chat(request, ctx):
        if request.text == "boom":
            raise AgentError("It broke", code="boom", status_code=400)
        return AgentResponse.text(
            f"echo: {request.text}",
            citations=[{"title": "doc"}],
            usage={"input_tokens": 3, "output_tokens": 4},
        )

    return app


class TestAgentCard:
    def test_card_is_built_from_the_app(self):
        app = AgentApp(
            name="Churn analyst",
            description="Answers churn questions",
            version="2.1.0",
            suggested_prompts=["Which segments churned?"],
            input_modalities=["text", "image"],
        )
        card = TestClient(app).get("/.well-known/agent-card.json").json()
        assert card["name"] == "Churn analyst"
        assert card["version"] == "2.1.0"
        # interface URLs come from the request when nothing fixed the base
        current = [
            i for i in card["supportedInterfaces"] if i["protocolVersion"] == "1.0"
        ]
        assert [i["url"] for i in current] == [
            "http://testserver/a2a",
            "http://testserver/a2a/rest",
        ]
        assert [i["protocolBinding"] for i in current] == ["JSONRPC", "HTTP+JSON"]
        # the same URLs as 0.3 interfaces, and the 0.3 fields a 0.3 client reads
        legacy = [
            i for i in card["supportedInterfaces"] if i["protocolVersion"] == "0.3"
        ]
        assert [i["url"] for i in legacy] == [i["url"] for i in current]
        assert card["url"] == "http://testserver/a2a"
        assert card["preferredTransport"] == "JSONRPC"
        assert card["protocolVersion"] == "0.3"
        assert card["capabilities"]["streaming"] is True
        assert card["capabilities"]["extensions"][0]["uri"].startswith(
            "https://hopsworks.ai/a2a/extensions/hopsworks/"
        )
        assert card["capabilities"]["extensions"][0]["required"] is False
        # one skill derived from the agent when none are declared
        (skill,) = card["skills"]
        assert skill["id"] == "chat"
        assert skill["name"] == "Churn analyst"
        assert skill["description"] == "Answers churn questions"
        assert skill["tags"] == ["chat"]
        assert skill["examples"] == ["Which segments churned?"]
        # exact media types: the SDK validates parts by set membership
        assert card["defaultInputModes"] == [
            "text/plain",
            "text/markdown",
            "image/png",
            "image/jpeg",
            "image/webp",
            "image/gif",
            "application/json",
        ]
        assert set(card["securitySchemes"]) == {
            "hopsworks-api-key",
            "hopsworks-session",
        }

    def test_declared_skills_and_media_types_win(self):
        app = AgentApp(
            skills=[
                AgentSkill(
                    id="report",
                    name="Report",
                    description="A churn report",
                    tags=["churn"],
                    output_modes=["text/csv"],
                )
            ],
            input_media_types=["text/plain", "application/pdf"],
            output_media_types=["text/markdown"],
        )
        card = TestClient(app).get("/.well-known/agent-card.json").json()
        assert [s["id"] for s in card["skills"]] == ["report"]
        assert card["skills"][0]["outputModes"] == ["text/csv"]
        assert card["defaultInputModes"] == [
            "text/plain",
            "application/pdf",
            "application/json",
        ]
        assert card["defaultOutputModes"] == ["text/markdown"]

    def test_the_platform_fixes_the_public_url(self, monkeypatch):
        monkeypatch.setenv(
            "HOPSWORKS_AGENT_PUBLIC_URL", "https://lb.example.com/v1/ns/churn"
        )
        card = TestClient(AgentApp()).get("/.well-known/agent-card.json").json()
        assert card["supportedInterfaces"][0]["url"] == (
            "https://lb.example.com/v1/ns/churn/a2a"
        )

    def test_forwarded_headers_shape_the_url(self):
        card = (
            TestClient(AgentApp(public_url=None))
            .get(
                "/.well-known/agent-card.json",
                headers={
                    "x-forwarded-proto": "https",
                    "x-forwarded-host": "lb.example.com",
                    "x-forwarded-prefix": "/v1/ns/churn/",
                },
            )
            .json()
        )
        assert card["supportedInterfaces"][0]["url"] == (
            "https://lb.example.com/v1/ns/churn/a2a"
        )

    def test_manifest_lists_a2a(self):
        manifest = (
            TestClient(AgentApp()).get("/.well-known/hopsworks-agent.json").json()
        )
        assert manifest["endpoints"]["agent_card"] == "/.well-known/agent-card.json"
        assert manifest["endpoints"]["a2a"] == "/a2a"
        assert manifest["endpoints"]["a2a_rest"] == "/a2a/rest"
        assert manifest["capabilities"]["custom"]["hopsworks"]["a2a"] == {
            "enabled": True,
            "bindings": ["JSONRPC", "HTTP+JSON"],
            "push_notifications": False,
            "signed": False,
            "task_store": "memory",
            "task_limit": 1000,
            "serialize_context": False,
        }

    def test_a2a_can_be_switched_off(self):
        client = TestClient(AgentApp(a2a=False))
        assert client.get("/.well-known/agent-card.json").status_code == 404
        assert client.post("/a2a", json={}).status_code == 404
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["custom"]["hopsworks"]["a2a"] == {
            "enabled": False
        }
        assert "a2a" not in manifest["endpoints"]


class TestSendMessage:
    def test_blocking_send_answers_with_a_completed_task(self):
        async def scenario():
            client, _ = await connect(echo_app())
            task = task_of(
                await send(client, user_message("hello", context_id="ctx-1"))
            )
            assert state_name(task) == "TASK_STATE_COMPLETED"
            assert task.context_id == "ctx-1"
            # the reply is always the artifact named response
            reply = artifact(task, "response")
            assert reply.artifact_id == "response"
            assert text_of(reply) == "echo: hello"
            # the final status message is the answer too, so GetTask has it
            # without reading artifacts; the history holds the question
            assert [m.role for m in task.history] == [Role.ROLE_USER]
            assert text_of(task.status.message) == "echo: hello"
            facts = hopsworks_of(task.status.message)
            assert facts["citations"] == [{"title": "doc"}]
            assert facts["usage"] == {"input_tokens": 3, "output_tokens": 4}
            assert facts["response_id"].startswith("response_")

        run(scenario())

    def test_the_rest_binding_serves_the_same_handler(self):
        async def scenario():
            client, _ = await connect(echo_app(), bindings=["HTTP+JSON"])
            task = task_of(await send(client, user_message("rest")))
            assert text_of(artifact(task, "response")) == "echo: rest"

        run(scenario())

    def test_the_subject_and_context_ride_in_the_extension(self, monkeypatch):
        monkeypatch.setenv("DEPLOYMENT_ID", "7")
        seen = {}
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            seen["subject"] = ctx.subject
            seen["source"] = ctx.subject_source
            seen["context"] = request.context
            seen["metadata"] = request.metadata
            seen["protocol"] = ctx.protocol
            seen["task_id"] = ctx.task_id
            return "ok"

        async def scenario():
            client, _ = await connect(app)
            message = user_message(
                "hi",
                hopsworks={
                    "subject": "alice",
                    "context": {"tier": "gold"},
                    "metadata": {"source": "crm"},
                },
            )
            task = task_of(await send(client, message))
            assert seen == {
                "subject": "alice",
                "source": "client",
                "context": {"tier": "gold"},
                "metadata": {"source": "crm"},
                "protocol": "a2a",
                "task_id": task.id,
            }
            assert hopsworks_of(task.status.message)["subject"] == "alice"

        run(scenario())

    def test_data_parts_reach_the_handler_as_context(self):
        seen = {}
        app = AgentApp()

        @app.chat
        async def chat(request):
            seen.update(request.context)
            return request.text

        async def scenario():
            client, _ = await connect(app)
            message = user_message(
                parts=[Part(text="q"), data_part({"order_id": 42})], context_id="c"
            )
            task_of(await send(client, message))
            assert seen == {"order_id": 42}

        run(scenario())

    def test_a_handler_error_fails_the_task_with_its_code(self):
        async def scenario():
            client, _ = await connect(echo_app())
            task = task_of(await send(client, user_message("boom")))
            assert state_name(task) == "TASK_STATE_FAILED"
            assert text_of(task.status.message) == "It broke"
            facts = hopsworks_of(task.status.message)
            assert facts["status"] == "boom"
            assert facts["http_status"] == 400
            assert facts["retryable"] is False

        run(scenario())

    def test_a_handler_crash_fails_the_task_and_says_why(self):
        app = AgentApp()

        @app.chat
        async def chat(request):
            raise ValueError("temperature is not supported")

        async def scenario():
            client, _ = await connect(app)
            task = task_of(await send(client, user_message("x")))
            assert state_name(task) == "TASK_STATE_FAILED"
            assert "temperature" in text_of(task.status.message)
            assert hopsworks_of(task.status.message)["status"] == "agent_error"

        run(scenario())

    def test_no_handler_rejects_the_task(self):
        async def scenario():
            client, _ = await connect(AgentApp())
            task = task_of(await send(client, user_message("x")))
            assert state_name(task) == "TASK_STATE_REJECTED"
            assert hopsworks_of(task.status.message)["status"] == "not_implemented"

        run(scenario())

    def test_an_undeclared_media_type_is_refused_before_the_handler(self):
        async def scenario():
            client, _ = await connect(echo_app())
            part = Part(raw=b"\x89PNG", media_type="image/png", filename="x.png")
            with pytest.raises(Exception) as err:
                await send(client, user_message(parts=[Part(text="q"), part]))
            assert "image/png" in str(err.value) or "ContentType" in str(err.value)

        run(scenario())

    def test_an_empty_answer_still_creates_the_reply_artifact(self):
        app = AgentApp()

        @app.chat
        async def chat(request):
            return ""

        async def scenario():
            client, _ = await connect(app)
            task = task_of(await send(client, user_message("x")))
            assert state_name(task) == "TASK_STATE_COMPLETED"
            reply = artifact(task, "response")
            assert text_of(reply) == ""

        run(scenario())

    def test_return_immediately_hands_back_a_running_task(self):
        gate = asyncio.Event()
        app = AgentApp()

        @app.chat
        async def chat(request):
            await gate.wait()
            return "done"

        async def scenario():
            client, _ = await connect(app, polling=True)
            task = task_of(
                await send(client, user_message("x"), return_immediately=True)
            )
            assert state_name(task) in ("TASK_STATE_SUBMITTED", "TASK_STATE_WORKING")
            gate.set()
            for _ in range(50):
                task = await client.get_task(GetTaskRequest(id=task.id))
                if state_name(task) == "TASK_STATE_COMPLETED":
                    break
                await asyncio.sleep(0.02)
            assert state_name(task) == "TASK_STATE_COMPLETED"
            assert text_of(artifact(task, "response")) == "done"

        run(scenario())


class TestStreaming:
    def stream_app(self):
        app = AgentApp()

        @app.stream
        async def stream(request, ctx):
            yield "hello "
            await ctx.emit_event("search", status="done", message="looked it up")
            yield "world"

        return app

    def test_deltas_become_appending_chunks_of_the_reply_artifact(self):
        async def scenario():
            client, _ = await connect(self.stream_app(), streaming=True)
            events = await send(client, user_message("q"))
            chunks = [
                e.artifact_update for e in events if e.HasField("artifact_update")
            ]
            assert [a.artifact.artifact_id for a in chunks] == ["response"] * 3
            # the first chunk creates the artifact, the rest append, the last closes it
            assert [a.append for a in chunks] == [False, True, True]
            assert [a.last_chunk for a in chunks] == [False, False, True]
            assert "".join(text_of(a.artifact) for a in chunks) == "hello world"
            states = [
                TaskState.Name(e.status_update.status.state)
                for e in events
                if e.HasField("status_update")
            ]
            assert states[0] == "TASK_STATE_WORKING"
            assert states[-1] == "TASK_STATE_COMPLETED"

        run(scenario())

    def test_a_step_is_a_working_status_with_the_detail_in_the_extension(self):
        async def scenario():
            client, _ = await connect(self.stream_app(), streaming=True)
            events = await send(client, user_message("q"))
            steps = [
                e.status_update
                for e in events
                if e.HasField("status_update")
                and "step" in hopsworks_of(e.status_update.status.message)
            ]
            assert len(steps) == 1
            assert TaskState.Name(steps[0].status.state) == "TASK_STATE_WORKING"
            assert text_of(steps[0].status.message) == "looked it up (done)"
            detail = hopsworks_of(steps[0].status.message)["step"]
            assert detail["name"] == "search"
            assert detail["status"] == "done"

        run(scenario())

    def test_a_chat_handler_streams_its_answer_as_one_chunk(self):
        async def scenario():
            client, _ = await connect(echo_app(), streaming=True)
            events = await send(client, user_message("one"))
            chunks = [
                e.artifact_update for e in events if e.HasField("artifact_update")
            ]
            assert len(chunks) == 1
            assert chunks[0].append is False
            assert chunks[0].last_chunk is True
            assert text_of(chunks[0].artifact) == "echo: one"

        run(scenario())

    def test_the_blocking_call_sees_the_whole_streamed_answer(self):
        async def scenario():
            client, _ = await connect(self.stream_app())
            task = task_of(await send(client, user_message("q")))
            assert text_of(artifact(task, "response")) == "hello world"
            assert text_of(task.status.message) == "hello world"
            # the step it reported on the way is kept in the history
            assert "looked it up" in text_of(task.history[-1])

        run(scenario())


class TestArtifacts:
    def test_returned_artifacts_and_media_parts_become_task_artifacts(self):
        from hopsworks_agents.protocol import ImageContent

        app = AgentApp(output_modalities=["text", "image"])
        csv = base64.b64encode(b"a,b\n1,2\n").decode()

        @app.chat
        async def chat(request):
            return AgentResponse.parts(
                TextContent(text="here"),
                ImageContent(media_type="image/png", data="aGVsbG8="),
                artifacts=[
                    Artifact(
                        name="churn.csv",
                        media_type="text/csv",
                        data=csv,
                        description="monthly churn",
                    )
                ],
            )

        async def scenario():
            client, _ = await connect(app)
            task = task_of(await send(client, user_message("report")))
            names = [a.name for a in task.artifacts]
            assert names == ["response", "image-2", "churn.csv"]
            report = artifact(task, "churn.csv")
            assert report.parts[0].raw == b"a,b\n1,2\n"
            assert report.parts[0].media_type == "text/csv"
            assert report.parts[0].filename == "churn.csv"
            assert MessageToDict(report)["metadata"] == {"description": "monthly churn"}
            assert artifact(task, "image-2").parts[0].media_type == "image/png"

        run(scenario())

    def test_emitted_chunks_follow_the_create_then_append_rule(self):
        app = AgentApp()

        @app.stream
        async def stream(request, ctx):
            # the author says append on the first chunk too; the executor
            # creates the artifact first regardless, since the SDK refuses otherwise
            await ctx.emit_artifact("log", TextContent(text="one "), append=True)
            yield "reply"
            await ctx.emit_artifact(
                "log", TextContent(text="two"), append=True, last=True
            )

        async def scenario():
            client, _ = await connect(app, streaming=True)
            events = await send(client, user_message("q"))
            logs = [
                e.artifact_update
                for e in events
                if e.HasField("artifact_update")
                and e.artifact_update.artifact.name == "log"
            ]
            assert [a.append for a in logs] == [False, True]
            assert [a.last_chunk for a in logs] == [False, True]
            task = await client.get_task(GetTaskRequest(id=events[0].task.id))
            assert text_of(artifact(task, "log")) == "one two"

        run(scenario())

    def test_an_artifact_left_open_is_closed_with_the_task(self):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_artifact("trace", TextContent(text="started"))
            return "ok"

        async def scenario():
            client, _ = await connect(app, streaming=True)
            events = await send(client, user_message("q"))
            traces = [
                e.artifact_update
                for e in events
                if e.HasField("artifact_update")
                and e.artifact_update.artifact.artifact_id == "trace"
            ]
            assert [a.last_chunk for a in traces] == [False, True]

        run(scenario())

    def test_refused_output_modes_drop_the_artifact(self):
        app = AgentApp(output_modalities=["text", "image"])

        @app.chat
        async def chat(request):
            return AgentResponse.text(
                "t",
                artifacts=[Artifact(name="x.csv", media_type="text/csv", data="YQ==")],
            )

        async def scenario():
            client, _ = await connect(app)
            task = task_of(
                await send(client, user_message("q"), accepted=["text/plain"])
            )
            assert [a.name for a in task.artifacts] == ["response"]

        run(scenario())

    def test_chunks_emitted_on_chat_assemble_into_the_json_reply(self):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_artifact("log", TextContent(text="one "))
            await ctx.emit_artifact(
                "log", TextContent(text="two"), append=True, last=True
            )
            return "ok"

        body = (
            TestClient(app)
            .post(
                "/v1/chat",
                json={
                    "message": {
                        "role": "user",
                        "content": [{"type": "text", "text": "q"}],
                    }
                },
            )
            .json()
        )
        assert body["artifacts"] == [
            {
                "name": "log",
                "id": "log",
                "description": None,
                "parts": [{"type": "text", "text": "one two"}],
            }
        ]

    def test_agui_carries_artifacts_as_custom_events(self):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_artifact("log", TextContent(text="one"))
            return AgentResponse.text(
                "ok",
                artifacts=[Artifact(name="x.csv", media_type="text/csv", data="YQ==")],
            )

        body = TestClient(app).post(
            "/v1/run",
            json={
                "threadId": "t",
                "runId": "r",
                "messages": [{"id": "m", "role": "user", "content": "q"}],
                "tools": [],
                "context": [],
                "forwardedProps": {},
            },
        )
        events = [
            json.loads(line[len("data: ") :])
            for line in body.text.split("\n")
            if line.startswith("data: ")
        ]
        custom = [e for e in events if e["type"] == "CUSTOM"]
        assert [e["name"] for e in custom] == [
            "hopsworks.artifact",
            "hopsworks.artifact",
        ]
        assert custom[0]["value"]["name"] == "log"
        assert custom[1]["value"]["name"] == "x.csv"


class TestUrlParts:
    def test_a_url_part_reaches_the_handler_as_a_url(self):
        seen = {}
        app = AgentApp(input_media_types=["text/plain", "application/pdf"])

        @app.chat
        async def chat(request):
            seen["urls"] = [(u.url, u.media_type, u.name) for u in request.urls]
            return "ok"

        async def scenario():
            client, _ = await connect(app)
            part = Part(
                url="https://x.test/a.pdf",
                media_type="application/pdf",
                filename="a.pdf",
            )
            task_of(await send(client, user_message(parts=[Part(text="read"), part])))
            assert seen["urls"] == [
                ("https://x.test/a.pdf", "application/pdf", "a.pdf")
            ]

        run(scenario())

    def test_an_allowed_host_is_fetched_into_a_file(self, monkeypatch):
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.setattr(
            a2a_module, "_fetch", lambda url, stop=None: ("text/csv", b"a,b")
        )
        seen = {}
        app = AgentApp(
            fetch_urls=["files.test"], input_media_types=["text/plain", "text/csv"]
        )

        @app.chat
        async def chat(request):
            seen["files"] = [(f.name, f.media_type, f.data) for f in request.files]
            seen["urls"] = request.urls
            return "ok"

        async def scenario():
            client, _ = await connect(app)
            part = Part(url="https://files.test/data/x.csv", media_type="text/csv")
            task_of(await send(client, user_message(parts=[Part(text="q"), part])))
            assert seen["files"] == [
                ("x.csv", "text/csv", base64.b64encode(b"a,b").decode())
            ]
            assert seen["urls"] == []

        run(scenario())


class TestHumanInTheLoop:
    def refund_app(self, reason="confirmation", **interrupt):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            answer = ctx.interrupt(
                "Refund 40 EUR?", key="refund-42", reason=reason, **interrupt
            )
            if answer.cancelled or not answer.payload:
                return "no refund"
            return f"refunded: {answer.payload}"

        return app

    def test_a_question_pauses_the_task_and_an_answer_completes_it(self):
        async def scenario():
            client, _ = await connect(self.refund_app())
            task = task_of(await send(client, user_message("refund", context_id="c1")))
            assert state_name(task) == "TASK_STATE_INPUT_REQUIRED"
            asked = task.status.message
            assert text_of(asked) == "Refund 40 EUR?"
            data = MessageToDict(
                [p for p in asked.parts if p.WhichOneof("content") == "data"][0]
            )
            assert data["data"]["interrupts"][0]["id"] == "refund-42"
            assert hopsworks_of(asked)["interrupts"][0]["reason"] == "confirmation"
            # the answer names the question, on the same task
            answer = user_message(
                parts=[data_part({"refund-42": {"approved": True}})],
                context_id="c1",
                task_id=task.id,
            )
            done = task_of(await send(client, answer))
            assert done.id == task.id
            assert state_name(done) == "TASK_STATE_COMPLETED"
            assert text_of(artifact(done, "response")) == "refunded: {'approved': True}"

        run(scenario())

    def test_plain_text_answers_the_one_open_question(self):
        async def scenario():
            client, _ = await connect(self.refund_app())
            task = task_of(await send(client, user_message("refund", context_id="c1")))
            done = task_of(
                await send(
                    client, user_message("yes please", context_id="c1", task_id=task.id)
                )
            )
            assert state_name(done) == "TASK_STATE_COMPLETED"
            assert text_of(artifact(done, "response")) == "refunded: yes please"

        run(scenario())

    def test_a_cancelled_answer_is_given_to_the_handler(self):
        async def scenario():
            client, _ = await connect(self.refund_app())
            task = task_of(await send(client, user_message("refund", context_id="c1")))
            done = task_of(
                await send(
                    client,
                    user_message(
                        parts=[data_part({"refund-42": {"$cancelled": True}})],
                        context_id="c1",
                        task_id=task.id,
                    ),
                )
            )
            assert text_of(artifact(done, "response")) == "no refund"

        run(scenario())

    def test_an_answer_to_a_question_not_asked_is_refused(self):
        """Spec 4.5: unknown ids are ``InvalidParamsError``, and the task stays paused."""

        async def scenario():
            client, _ = await connect(self.refund_app())
            task = task_of(await send(client, user_message("refund", context_id="c1")))
            wrong = [
                # only an id that was never asked
                {"other": {"approved": True}},
                # the right id next to one that was never asked
                {"refund-42": {"approved": True}, "never-asked": "boom"},
                # nothing answered at all
                {},
            ]
            for answers in wrong:
                with pytest.raises(InvalidParamsError):
                    await send(
                        client,
                        user_message(
                            parts=[data_part(answers)],
                            context_id="c1",
                            task_id=task.id,
                        ),
                    )
                still = await client.get_task(GetTaskRequest(id=task.id))
                assert state_name(still) == "TASK_STATE_INPUT_REQUIRED"
                assert text_of(still.status.message) == "Refund 40 EUR?"
            # and the right answer still works
            done = task_of(
                await send(
                    client,
                    user_message(
                        parts=[data_part({"refund-42": {"approved": True}})],
                        context_id="c1",
                        task_id=task.id,
                    ),
                )
            )
            assert state_name(done) == "TASK_STATE_COMPLETED"

        run(scenario())

    def test_auth_required_when_the_question_is_about_credentials(self):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            answer = ctx.interrupt(
                "Connect your CRM",
                key="crm-auth",
                reason="auth_required",
                metadata={"auth": {"kind": "secret", "secret_name": "crm_token"}},
            )
            return f"using {answer.payload['credential_ref']}"

        async def scenario():
            client, _ = await connect(app)
            task = task_of(await send(client, user_message("go", context_id="c1")))
            assert state_name(task) == "TASK_STATE_AUTH_REQUIRED"
            assert hopsworks_of(task.status.message)["interrupts"][0]["metadata"] == {
                "auth": {"kind": "secret", "secret_name": "crm_token"}
            }
            done = task_of(
                await send(
                    client,
                    user_message(
                        parts=[
                            data_part({"crm-auth": {"credential_ref": "crm_token"}})
                        ],
                        context_id="c1",
                        task_id=task.id,
                    ),
                )
            )
            assert state_name(done) == "TASK_STATE_COMPLETED"
            assert text_of(artifact(done, "response")) == "using crm_token"

        run(scenario())

    def test_the_streaming_call_ends_with_the_question(self):
        async def scenario():
            client, _ = await connect(self.refund_app(), streaming=True)
            events = await send(client, user_message("refund", context_id="c1"))
            last = events[-1].status_update
            assert TaskState.Name(last.status.state) == "TASK_STATE_INPUT_REQUIRED"

        run(scenario())


class TestTasks:
    def test_get_and_list_tasks(self):
        async def scenario():
            client, _ = await connect(echo_app())
            first = task_of(await send(client, user_message("a", context_id="ctx")))
            second = task_of(await send(client, user_message("b", context_id="ctx")))
            got = await client.get_task(GetTaskRequest(id=first.id))
            assert text_of(artifact(got, "response")) == "echo: a"
            listed = await client.list_tasks(ListTasksRequest(context_id="ctx"))
            assert {t.id for t in listed.tasks} == {first.id, second.id}

        run(scenario())

    def test_cancel_stops_a_running_task(self):
        started = asyncio.Event()
        seen = {}
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            started.set()
            try:
                await asyncio.sleep(10)
            finally:
                seen["cancelled"] = ctx.cancelled.is_set()
            return "late"

        async def scenario():
            client, _ = await connect(app, polling=True)
            task = task_of(
                await send(client, user_message("x"), return_immediately=True)
            )
            await asyncio.wait_for(started.wait(), 2)
            cancelled = await client.cancel_task(CancelTaskRequest(id=task.id))
            assert state_name(cancelled) == "TASK_STATE_CANCELED"
            assert seen["cancelled"] is True
            assert state_name(await client.get_task(GetTaskRequest(id=task.id))) == (
                "TASK_STATE_CANCELED"
            )

        run(scenario())

    def test_reference_tasks_are_given_to_the_handler(self):
        seen = {}
        app = AgentApp()

        @app.chat
        async def chat(request):
            seen["referenced"] = request.context.get("referenced_tasks")
            return f"answer to {request.text}"

        async def scenario():
            client, _ = await connect(app)
            first = task_of(await send(client, user_message("one", context_id="c")))
            message = user_message("continue", context_id="c")
            message.reference_task_ids.append(first.id)
            task_of(await send(client, message))
            assert seen["referenced"] == [
                {
                    "task_id": first.id,
                    "context_id": "c",
                    "state": "TASK_STATE_COMPLETED",
                    "text": "answer to one",
                    "artifacts": ["response"],
                }
            ]

        run(scenario())

    def test_concurrent_tasks_on_one_context_each_keep_their_own_history(self):
        app = AgentApp()

        @app.chat
        async def chat(request):
            await asyncio.sleep(0.05)
            return f"re: {request.text}"

        async def scenario():
            client, _ = await connect(app)
            results = await asyncio.gather(
                *(
                    send(client, user_message(f"q{i}", context_id="shared"))
                    for i in range(5)
                )
            )
            tasks = [task_of(r) for r in results]
            assert len({t.id for t in tasks}) == 5
            for i, task in enumerate(tasks):
                assert state_name(task) == "TASK_STATE_COMPLETED"
                assert [text_of(m) for m in task.history] == [f"q{i}"]
                assert text_of(task.status.message) == f"re: q{i}"

        run(scenario())

    def test_tasks_are_recorded_in_memory_as_turns_of_the_context(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t"
        )
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request, ctx):
            return f"{len(ctx.history)} before"

        async def scenario():
            client, _ = await connect(app)
            task_of(await send(client, user_message("first", context_id="conv")))
            second = task_of(
                await send(client, user_message("second", context_id="conv"))
            )
            assert text_of(artifact(second, "response")) == "2 before"
            # the turn is closed in the store before the terminal event goes out

        run(scenario())
        assert [t["content"] for t in memory.get("conv")] == [
            "first",
            "0 before",
            "second",
            "2 before",
        ]


class TestReviewRoundOne:
    """What the first review of the adapter found, and must stay fixed."""

    def _memory(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t"
        )

    def test_the_drain_deadline_fails_a_running_task_as_shutdown(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setenv("HOPSWORKS_AGENT_TASK_DRAIN_SECONDS", "0.1")
        memory = self._memory(tmp_path)
        started = asyncio.Event()
        seen = {}
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request, ctx):
            started.set()
            try:
                await asyncio.sleep(10)
            finally:
                seen["cancelled"] = ctx.cancelled.is_set()
            return "late"

        async def scenario():
            client, _ = await connect(app, polling=True)
            task = task_of(
                await send(
                    client, user_message("x", context_id="c1"), return_immediately=True
                )
            )
            await asyncio.wait_for(started.wait(), 2)
            await app._drain()
            got = task
            for _ in range(100):
                got = await client.get_task(GetTaskRequest(id=task.id))
                if state_name(got) not in (
                    "TASK_STATE_WORKING",
                    "TASK_STATE_SUBMITTED",
                ):
                    break
                await asyncio.sleep(0.02)
            # not a success with an empty answer: a failure the caller can act on
            assert state_name(got) == "TASK_STATE_FAILED"
            facts = hopsworks_of(got.status.message)
            assert facts["status"] == "executor_shutdown"
            assert facts["retryable"] is True
            assert seen["cancelled"] is True
            assert app._draining is True

        run(scenario())
        memory.healthcheck()
        with memory._engine.connect() as conn:
            rows = conn.execute(memory._table.select()).fetchall()
        assert [r.status for r in rows] == ["abandoned"]

    def test_finished_tasks_are_dropped_past_the_limit(self):
        from hopsworks_agents.protocol.a2a import A2AConfig

        app = echo_app(a2a=A2AConfig(max_tasks=2))

        async def scenario():
            client, _ = await connect(app)
            tasks = [
                task_of(await send(client, user_message(f"q{i}"))) for i in range(3)
            ]
            with pytest.raises(Exception, match="(?i)not found|TaskNotFound"):
                await client.get_task(GetTaskRequest(id=tasks[0].id))
            kept = await client.get_task(GetTaskRequest(id=tasks[2].id))
            assert state_name(kept) == "TASK_STATE_COMPLETED"

        run(scenario())

    def test_a_chunk_with_append_false_replaces_the_artifact(self):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_artifact("log", TextContent(text="draft"))
            await ctx.emit_artifact(
                "log", TextContent(text="final"), append=False, last=True
            )
            return "ok"

        async def scenario():
            client, _ = await connect(app, streaming=True)
            events = await send(client, user_message("q"))
            logs = [
                e.artifact_update
                for e in events
                if e.HasField("artifact_update")
                and e.artifact_update.artifact.artifact_id == "log"
            ]
            assert [a.append for a in logs] == [False, False]
            task = await client.get_task(GetTaskRequest(id=events[0].task.id))
            assert text_of(artifact(task, "log")) == "final"

        run(scenario())

    def test_a_final_text_that_rewrites_the_draft_replaces_the_reply(self):
        app = AgentApp()

        @app.stream
        async def stream(request):
            yield "draft answer"
            yield AgentResponse.text("corrected answer")

        async def scenario():
            client, _ = await connect(app)
            task = task_of(await send(client, user_message("q")))
            assert text_of(artifact(task, "response")) == "corrected answer"
            assert text_of(task.status.message) == "corrected answer"

        run(scenario())

    def test_a_stream_handlers_final_artifacts_are_kept(self):
        app = AgentApp()

        @app.stream
        async def stream(request):
            yield "text"
            yield AgentResponse.text(
                "",
                artifacts=[
                    Artifact(name="report.csv", media_type="text/csv", data="YQ==")
                ],
            )

        async def scenario():
            client, _ = await connect(app)
            task = task_of(await send(client, user_message("q")))
            assert [a.name for a in task.artifacts] == ["response", "report.csv"]
            assert text_of(artifact(task, "response")) == "text"

        run(scenario())
        # and on AG-UI, where the same merge runs
        body = TestClient(app).post(
            "/v1/run",
            json={
                "threadId": "t",
                "runId": "r",
                "messages": [{"id": "m", "role": "user", "content": "q"}],
                "tools": [],
                "context": [],
                "forwardedProps": {},
            },
        )
        custom = [
            json.loads(line[len("data: ") :])
            for line in body.text.split("\n")
            if line.startswith("data: ") and '"CUSTOM"' in line
        ]
        assert [e["value"]["name"] for e in custom] == ["report.csv"]

    def test_history_is_a_snapshot_and_turns_stay_contiguous(self, tmp_path):
        memory = self._memory(tmp_path)
        gate = asyncio.Event()
        seen = {}
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request, ctx):
            if request.text == "slow":
                first = ctx.history
                await gate.wait()
                seen["reads"] = (first, ctx.history)
                return "slow done"
            return "fast done"

        async def scenario():
            client, _ = await connect(app)
            slow = asyncio.create_task(
                send(client, user_message("slow", context_id="c"))
            )
            await asyncio.sleep(0.1)
            task_of(await send(client, user_message("fast", context_id="c")))
            gate.set()
            task_of(await slow)
            # the reply goes out before the turn closes in the store
            for _ in range(100):
                if len(await asyncio.to_thread(memory.get, "c")) == 4:
                    break
                await asyncio.sleep(0.02)

        run(scenario())
        # what the slow turn read did not change under it when the fast one ended
        assert seen["reads"] == ([], [])
        # each turn is read as one unit, where it started
        assert [t["content"] for t in memory.get("c")] == [
            "slow",
            "slow done",
            "fast",
            "fast done",
        ]
        assert [m["content"] for m in memory.transcript("c")] == [
            "slow",
            "slow done",
            "fast",
            "fast done",
        ]

    def test_the_process_store_also_keeps_turns_where_they_started(self):
        from hopsworks_agents.protocol import InMemoryAgentMemory

        memory = InMemoryAgentMemory()
        memory.begin_turn("c", "t1", "user", "slow")
        memory.begin_turn("c", "t2", "user", "fast")
        memory.record_item("c", "t2", "assistant", "fast done")
        memory.end_turn("c", "t2")
        memory.record_item("c", "t1", "assistant", "slow done")
        memory.end_turn("c", "t1")
        assert [t["content"] for t in memory.get("c")] == [
            "slow",
            "slow done",
            "fast",
            "fast done",
        ]

    def test_serialize_context_runs_the_tasks_of_a_context_one_at_a_time(self):
        from hopsworks_agents.protocol.a2a import A2AConfig

        order = []
        app = AgentApp(a2a=A2AConfig(serialize_context=True))

        @app.chat
        async def chat(request):
            order.append(("start", request.text))
            await asyncio.sleep(0.03)
            order.append(("end", request.text))
            return "ok"

        async def scenario():
            client, _ = await connect(app)
            results = await asyncio.gather(
                *(
                    send(client, user_message(f"q{i}", context_id="one"))
                    for i in range(3)
                )
            )
            for events in results:
                assert state_name(task_of(events)) == "TASK_STATE_COMPLETED"

        run(scenario())
        assert len(order) == 6
        for i in range(0, 6, 2):
            assert order[i][0] == "start"
            assert order[i + 1] == ("end", order[i][1])

    def test_emitted_artifacts_honour_the_accepted_output_modes(self):
        from hopsworks_agents.protocol import ImageContent

        app = AgentApp(output_modalities=["text", "image"])

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_artifact(
                "chart", ImageContent(media_type="image/png", data="aGk="), last=True
            )
            return AgentResponse.text(
                "t",
                artifacts=[Artifact(name="x.csv", media_type="text/csv", data="YQ==")],
            )

        async def scenario():
            client, _ = await connect(app)
            task = task_of(
                await send(client, user_message("q"), accepted=["text/plain"])
            )
            assert [a.name for a in task.artifacts] == ["response"]
            assert hopsworks_of(task.status.message)["dropped_artifacts"] == [
                "chart",
                "x.csv",
            ]

        run(scenario())


class TestReviewRoundTwo:
    """What the second review found: queued and paused tasks must stay bounded."""

    def test_the_drain_fails_a_task_queued_behind_a_context_lock(self, monkeypatch):
        from hopsworks_agents.protocol.a2a import A2AConfig

        monkeypatch.setenv("HOPSWORKS_AGENT_TASK_DRAIN_SECONDS", "0.1")
        started = asyncio.Event()
        ran = []
        app = AgentApp(a2a=A2AConfig(serialize_context=True))

        @app.chat
        async def chat(request, ctx):
            ran.append(request.text)
            started.set()
            await asyncio.sleep(10)
            return "late"

        async def scenario():
            client, _ = await connect(app, polling=True)
            first = task_of(
                await send(
                    client,
                    user_message("first", context_id="one"),
                    return_immediately=True,
                )
            )
            await asyncio.wait_for(started.wait(), 2)
            second = task_of(
                await send(
                    client,
                    user_message("second", context_id="one"),
                    return_immediately=True,
                )
            )
            await asyncio.sleep(0.05)
            await app._drain()
            states = {}
            for task in (first, second):
                got = task
                for _ in range(100):
                    got = await client.get_task(GetTaskRequest(id=task.id))
                    if state_name(got) not in (
                        "TASK_STATE_WORKING",
                        "TASK_STATE_SUBMITTED",
                    ):
                        break
                    await asyncio.sleep(0.02)
                states[task.id] = (state_name(got), hopsworks_of(got.status.message))
            # the running one was cut short, the queued one never started
            for state, facts in states.values():
                assert state == "TASK_STATE_FAILED"
                assert facts["status"] == "executor_shutdown"
                assert facts["retryable"] is True
            assert ran == ["first"]
            assert not app._a2a._running()

        run(scenario())

    def test_paused_tasks_count_toward_the_store_budget(self):
        from hopsworks_agents.protocol.a2a import A2AConfig

        app = AgentApp(a2a=A2AConfig(max_tasks=2))

        @app.chat
        async def chat(request, ctx):
            answer = ctx.interrupt("Go on?", key="go", reason="confirmation")
            return f"answered: {answer.payload}"

        async def scenario():
            client, _ = await connect(app)
            tasks = [
                task_of(await send(client, user_message("q", context_id=f"c{i}")))
                for i in range(3)
            ]
            assert all(state_name(t) == "TASK_STATE_INPUT_REQUIRED" for t in tasks)
            store = app._pending_store()
            # the oldest paused task is gone, and so is its open question
            with pytest.raises(Exception, match="(?i)not found|TaskNotFound"):
                await client.get_task(GetTaskRequest(id=tasks[0].id))
            assert store._get(_task_key("c0", tasks[0].id)) is None
            assert store._get(_task_key("c2", tasks[2].id)) is not None
            # the newest paused task still answers
            done = task_of(
                await send(
                    client, user_message("yes", context_id="c2", task_id=tasks[2].id)
                )
            )
            assert state_name(done) == "TASK_STATE_COMPLETED"
            assert text_of(artifact(done, "response")) == "answered: yes"

        run(scenario())

    def test_a_task_accepted_after_the_drain_began_does_not_start(self):
        ran = []
        app = echo_app()
        original = app._chat_handler

        @app.chat
        async def chat(request, ctx):
            ran.append(request.text)
            return await original(request, ctx) if original else "ok"

        async def scenario():
            client, _ = await connect(app)
            app._draining = True
            task = task_of(await send(client, user_message("q")))
            assert state_name(task) == "TASK_STATE_FAILED"
            assert hopsworks_of(task.status.message)["status"] == "executor_shutdown"
            assert ran == []

        run(scenario())


class TestUrlPinning:
    """An allowed URL part is fetched at the address that was checked, never re-resolved."""

    def test_a_private_address_is_refused(self, monkeypatch):
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["10.0.0.5"])
        monkeypatch.delenv(a2a_module.INTERNAL_ALLOW_ENV, raising=False)
        with pytest.raises(AgentError) as err:
            a2a_module._pin("https://files.test/x.csv")
        assert err.value.code == "unsupported_content"

    def test_a_rebinding_answer_is_refused(self, monkeypatch):
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.setattr(
            a2a_module,
            "_resolve",
            lambda host, port: ["93.184.216.34", "169.254.169.254"],
        )
        monkeypatch.delenv(a2a_module.INTERNAL_ALLOW_ENV, raising=False)
        with pytest.raises(AgentError):
            a2a_module._pin("https://files.test/x.csv")

    def test_a_public_address_is_pinned_with_the_name_kept(self, monkeypatch):
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.setattr(
            a2a_module, "_resolve", lambda host, port: ["93.184.216.34"]
        )
        pinned = a2a_module._pin("https://files.test/data/x.csv?v=1")
        assert pinned.url == "https://93.184.216.34:443/data/x.csv?v=1"
        assert pinned.host == "files.test"
        assert pinned.host_header == "files.test"

    def test_an_allowed_internal_network_is_fetched(self, monkeypatch):
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["10.0.0.5"])
        monkeypatch.setenv(a2a_module.INTERNAL_ALLOW_ENV, "10.0.0.0/8, other.test")
        pinned = a2a_module._pin("https://files.test:8443/x.csv")
        assert pinned.url == "https://10.0.0.5:8443/x.csv"
        assert pinned.host_header == "files.test:8443"
        # a listed hostname is allowed on a private address
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["10.1.2.3"])
        assert a2a_module._pin("https://other.test/x").url == "https://10.1.2.3:443/x"

    def test_the_platform_refuses_what_no_list_may_admit(self, monkeypatch):
        """Spec 4.7: loopback, link-local and metadata are refused listed or not; HTTPS only."""
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        monkeypatch.setenv(
            a2a_module.INTERNAL_ALLOW_ENV, "127.0.0.0/8, 169.254.0.0/16, other.test"
        )
        refused = {
            "https://files.test/x": "127.0.0.1",
            "https://other.test/x": "127.0.0.1",
            "https://files.test/x?meta": "169.254.169.254",
            "https://files.test/x?v6": "fd00:ec2::254",
        }
        for url, address in refused.items():
            monkeypatch.setattr(a2a_module, "_resolve", lambda h, p, a=address: [a])
            with pytest.raises(AgentError, match="never reaches"):
                a2a_module._pin(url)
        # an unlisted private address, and http, are refused as well
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["10.0.0.5"])
        with pytest.raises(AgentError, match="did not list"):
            a2a_module._pin("https://files.test/x")
        with pytest.raises(AgentError, match="not an https URL"):
            a2a_module._pin("http://files.test/x")

    def test_a_local_run_may_fetch_from_localhost_over_http(self, monkeypatch):
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        monkeypatch.delenv(a2a_module.INTERNAL_ALLOW_ENV, raising=False)
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["127.0.0.1"])
        assert (
            a2a_module._pin("http://localhost:8000/x").url == "http://127.0.0.1:8000/x"
        )
        # but not from the metadata service, or an unlisted private address
        monkeypatch.setattr(
            a2a_module, "_resolve", lambda host, port: ["169.254.169.254"]
        )
        with pytest.raises(AgentError, match="never reaches"):
            a2a_module._pin("http://meta.test/x")
        monkeypatch.setattr(a2a_module, "_resolve", lambda host, port: ["10.0.0.5"])
        with pytest.raises(AgentError, match="did not list"):
            a2a_module._pin("http://files.test/x")


class TestReviewRoundThree:
    """What the third review found: records, cancels, refusals, the card for 0.3 clients, the fetch policy."""

    def refund_app(self, **interrupt):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            answer = ctx.interrupt("Refund 40 EUR?", key="refund-42", **interrupt)
            return f"refunded: {answer.payload}"

        return app

    async def pause(self, client, context_id="c1"):
        task = task_of(
            await send(client, user_message("refund", context_id=context_id))
        )
        assert state_name(task) == "TASK_STATE_INPUT_REQUIRED"
        return task

    @staticmethod
    def answer(context_id, task_id, payload="yes"):
        return user_message(
            parts=[data_part({"refund-42": payload})],
            context_id=context_id,
            task_id=task_id,
        )

    def test_erasing_a_conversation_takes_its_tasks_questions_with_it(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_e"
        )
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request, ctx):
            answer = ctx.interrupt("Refund 40 EUR?", key="refund-42")
            return f"refunded: {answer.payload}"

        async def scenario():
            client, _ = await connect(app)
            # a context whose id would match more than itself in a naive LIKE
            erased = await self.pause(client, "c%")
            kept = await self.pause(client, "cx")
            store = app._pending_store()
            assert store._get(_task_key("c%", erased.id)) is not None
            http = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url=BASE
            )
            assert (await http.delete("/v1/conversations/c%25")).status_code == 204
            assert store._get(_task_key("c%", erased.id)) is None
            assert store._get(_task_key("cx", kept.id)) is not None
            # a late answer to the erased task fails it rather than resuming it
            failed = task_of(await send(client, self.answer("c%", erased.id)))
            assert state_name(failed) == "TASK_STATE_FAILED"
            assert hopsworks_of(failed.status.message)["status"] == "nothing_to_resume"
            done = task_of(await send(client, self.answer("cx", kept.id)))
            assert state_name(done) == "TASK_STATE_COMPLETED"

        run(scenario())

    def test_the_process_store_erases_a_contexts_task_records_too(self):
        from hopsworks_agents.protocol.pending import _ProcessPending

        store = _ProcessPending()
        store._put("conv", "own", _far())
        store._put(_task_key("conv", "t1"), "task", _far())
        store._put(_task_key("conversation", "t2"), "other context", _far())
        store._clear_context("conv")
        assert store._get("conv") is None
        assert store._get(_task_key("conv", "t1")) is None
        assert store._get(_task_key("conversation", "t2")) is not None

    def test_cancelling_a_paused_task_clears_its_question(self):
        app = self.refund_app()

        async def scenario():
            client, _ = await connect(app)
            task = await self.pause(client)
            store = app._pending_store()
            assert store._get(_task_key("c1", task.id)) is not None
            cancelled = await client.cancel_task(CancelTaskRequest(id=task.id))
            assert state_name(cancelled) == "TASK_STATE_CANCELED"
            assert store._get(_task_key("c1", task.id)) is None

        run(scenario())

    def test_a_cancel_of_a_resumed_turn_is_refused(self):
        app = AgentApp()
        finished = asyncio.Event()

        @app.chat
        async def chat(request, ctx):
            answer = ctx.interrupt("Refund 40 EUR?", key="refund-42")
            # the approved action: not stopped by a cancel
            await asyncio.sleep(0.3)
            finished.set()
            return f"refunded: {answer.payload}"

        async def scenario():
            client, _ = await connect(app)
            task = await self.pause(client)
            resumed = task_of(
                await send(client, self.answer("c1", task.id), return_immediately=True)
            )
            assert state_name(resumed) in ("TASK_STATE_SUBMITTED", "TASK_STATE_WORKING")
            with pytest.raises(TaskNotCancelableError):
                await client.cancel_task(CancelTaskRequest(id=task.id))
            await asyncio.wait_for(finished.wait(), 2)
            got = task
            for _ in range(100):
                got = await client.get_task(GetTaskRequest(id=task.id))
                if state_name(got) == "TASK_STATE_COMPLETED":
                    break
                await asyncio.sleep(0.02)
            assert state_name(got) == "TASK_STATE_COMPLETED"
            assert text_of(artifact(got, "response")) == "refunded: yes"

        run(scenario())

    def test_an_expired_question_fails_the_task(self):
        app = self.refund_app(expires_in=0.05)

        async def scenario():
            client, _ = await connect(app)
            task = await self.pause(client)
            await asyncio.sleep(0.1)
            failed = task_of(await send(client, self.answer("c1", task.id)))
            assert state_name(failed) == "TASK_STATE_FAILED"
            assert hopsworks_of(failed.status.message)["status"] == "interrupt_expired"

        run(scenario())

    def test_a_paused_task_whose_record_is_gone_fails(self):
        app = self.refund_app()

        async def scenario():
            client, _ = await connect(app)
            task = await self.pause(client)
            # the pod restarted, or the record was dropped
            app._pending_store()._clear(_task_key("c1", task.id))
            failed = task_of(await send(client, self.answer("c1", task.id)))
            assert state_name(failed) == "TASK_STATE_FAILED"
            assert hopsworks_of(failed.status.message)["status"] == "nothing_to_resume"
            # and that is final: a second answer is a message to a terminal task
            with pytest.raises(Exception, match="(?i)terminal"):
                await send(client, self.answer("c1", task.id))

        run(scenario())

    def test_a_message_from_an_agent_role_is_refused(self):
        async def scenario():
            client, _ = await connect(echo_app())
            message = user_message("hi")
            message.role = Role.ROLE_AGENT
            with pytest.raises(InvalidParamsError):
                await send(client, message)

        run(scenario())

    def test_a_context_id_with_a_control_character_is_refused(self):
        app = echo_app()

        async def scenario():
            client, _ = await connect(app)
            with pytest.raises(InvalidParamsError):
                await send(client, user_message("hi", context_id="a\x1fb"))

        run(scenario())
        http = TestClient(app)
        chat = http.post(
            "/v1/chat",
            json={
                "conversation_id": "a\x1fb",
                "message": {
                    "role": "user",
                    "content": [{"type": "text", "text": "hi"}],
                },
            },
        )
        assert chat.status_code == 422
        agui = http.post(
            "/v1/run",
            json={
                "threadId": "a\x1fb",
                "runId": "r1",
                "messages": [{"id": "m1", "role": "user", "content": "hi"}],
                "tools": [],
                "context": [],
                "state": {},
                "forwardedProps": {},
            },
        )
        assert agui.status_code == 422

    def test_a_0_3_client_finds_and_calls_the_agent(self):
        """Section 7: a request without the version header speaks the 0.3 protocol."""
        http = TestClient(echo_app())
        card = http.get("/.well-known/agent-card.json").json()
        assert card["url"].endswith("/a2a")
        assert card["protocolVersion"] == "0.3"
        reply = http.post(
            card["url"],
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "message/send",
                "params": {
                    "message": {
                        "kind": "message",
                        "messageId": "m-03",
                        "role": "user",
                        "parts": [{"kind": "text", "text": "hello"}],
                    }
                },
            },
        ).json()
        task = reply["result"]
        assert task["kind"] == "task"
        assert task["status"]["state"] == "completed"
        assert task["artifacts"][0]["parts"][0]["text"] == "echo: hello"

    def test_json_rpc_and_rest_errors_are_the_named_ones(self):
        http = TestClient(echo_app(), headers={"A2A-Version": "1.0"})
        missing = http.post(
            "/a2a",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "GetTask",
                "params": {"id": "nope"},
            },
        ).json()
        assert missing["error"]["code"] == -32001
        assert http.get("/a2a/rest/tasks/nope").status_code == 404
        unknown = http.post(
            "/a2a",
            json={"jsonrpc": "2.0", "id": 2, "method": "NoSuchMethod", "params": {}},
        ).json()
        assert unknown["error"]["code"] == -32601

    def test_the_closing_chunk_keeps_the_artifacts_name(self):
        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_artifact(
                "Quarterly report", TextContent(text="q1"), artifact_id="rep-1"
            )
            return "ok"

        async def scenario():
            client, _ = await connect(app, streaming=True)
            events = await send(client, user_message("q"))
            chunks = [
                e.artifact_update
                for e in events
                if e.HasField("artifact_update")
                and e.artifact_update.artifact.artifact_id == "rep-1"
            ]
            assert [c.artifact.name for c in chunks] == ["Quarterly report"] * 2
            assert chunks[-1].last_chunk is True

        run(scenario())

    def test_tool_calls_are_working_statuses_under_the_extension(self):
        app = AgentApp(tool_events=True)

        @app.chat
        async def chat(request, ctx):
            await ctx._emit_tool_call(
                "search", "done", "t1", args={"q": "churn"}, result={"hits": 2}
            )
            return "ok"

        async def scenario():
            client, _ = await connect(app, streaming=True)
            events = await send(client, user_message("q"))
            calls = [
                hopsworks_of(e.status_update.status.message)["tool_call"]
                for e in events
                if e.HasField("status_update")
                and "tool_call" in hopsworks_of(e.status_update.status.message)
            ]
            assert len(calls) == 1
            assert calls[0]["name"] == "search"
            # arguments and results travel as bounded text, as on AG-UI
            assert json.loads(calls[0]["args"]) == {"q": "churn"}
            assert json.loads(calls[0]["result"]) == {"hits": 2}

        run(scenario())

    def test_list_tasks_pages_and_filters(self):
        async def scenario():
            client, _ = await connect(echo_app())
            ids = {
                task_of(await send(client, user_message(f"q{i}", context_id="p"))).id
                for i in range(3)
            }
            task_of(await send(client, user_message("elsewhere", context_id="o")))
            first = await client.list_tasks(
                ListTasksRequest(context_id="p", page_size=2)
            )
            assert len(first.tasks) == 2 and first.next_page_token
            second = await client.list_tasks(
                ListTasksRequest(
                    context_id="p", page_size=2, page_token=first.next_page_token
                )
            )
            assert len(second.tasks) == 1 and not second.next_page_token
            assert {t.id for t in [*first.tasks, *second.tasks]} == ids
            completed = await client.list_tasks(
                ListTasksRequest(context_id="p", status=TaskState.TASK_STATE_COMPLETED)
            )
            assert len(completed.tasks) == 3
            failed = await client.list_tasks(
                ListTasksRequest(context_id="p", status=TaskState.TASK_STATE_FAILED)
            )
            assert failed.tasks == []

        run(scenario())

    def test_the_callers_trace_is_continued(self):
        from hopsworks_agents.protocol.tracing import install_baggage_propagation
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        install_baggage_propagation(provider)
        app = echo_app(tracing=False)
        app.tracer_provider = provider
        trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"

        async def scenario():
            client, _ = await connect(
                app, headers={"traceparent": f"00-{trace_id}-00f067aa0ba902b7-01"}
            )
            task = task_of(await send(client, user_message("q")))
            assert hopsworks_of(task.status.message)["trace_id"] == trace_id
            # on the task itself too, from the final status event
            assert MessageToDict(task.metadata)["hopsworks"]["trace_id"] == trace_id

        run(scenario())
        span = exporter.get_finished_spans()[-1]
        assert format(span.get_span_context().trace_id, "032x") == trace_id

    def test_without_the_sdk_an_agui_agent_keeps_working(self):
        """In a fresh interpreter with no a2a-sdk: the manifest says so, chat still answers."""
        import subprocess
        import sys

        script = """
import json, sys
sys.modules["a2a"] = None
from fastapi.testclient import TestClient
from hopsworks_agents.protocol import AgentApp
app = AgentApp()
@app.chat
async def chat(request):
    return "still here"
http = TestClient(app)
out = {
    "a2a": http.get("/.well-known/hopsworks-agent.json").json()["capabilities"]["custom"]["hopsworks"]["a2a"],
    "card": http.get("/.well-known/agent-card.json").status_code,
    "chat": http.post("/v1/chat", json={"message": {"role": "user", "content": [{"type": "text", "text": "hi"}]}}).json()["message"]["content"][0]["text"],
}
try:
    AgentApp(a2a=True)
    out["explicit"] = "built"
except ModuleNotFoundError:
    out["explicit"] = "refused"
print(json.dumps(out))
"""
        done = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=120
        )
        assert done.returncode == 0, done.stderr
        out = json.loads(done.stdout.strip().splitlines()[-1])
        assert out["a2a"] == {"enabled": False, "reason": "sdk_missing"}
        assert out["card"] == 404
        assert out["chat"] == "still here"
        assert out["explicit"] == "refused"

    def test_task_store_memory_is_the_only_store(self):
        from hopsworks_agents.protocol.a2a import A2AConfig

        app = AgentApp(a2a=A2AConfig(task_store="memory"))
        manifest = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["custom"]["hopsworks"]["a2a"]["task_store"] == (
            "memory"
        )
        with pytest.raises(ValueError, match="task_store"):
            A2AConfig(task_store="rondb")

    def test_a_fetch_refuses_redirects_oversized_bodies_and_slow_hosts(
        self, monkeypatch
    ):
        import hopsworks_agents.protocol.a2a as a2a_module

        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        monkeypatch.setattr(a2a_module, "MAX_FETCHED_BYTES", 16)
        with _drip_server() as base:
            assert a2a_module._fetch(f"{base}/small") == ("text/plain", b"ok")
            with pytest.raises(AgentError, match="answered 302"):
                a2a_module._fetch(f"{base}/redirect")
            with pytest.raises(AgentError, match="larger"):
                a2a_module._fetch(f"{base}/big")
            with pytest.raises(AgentError, match="too long"):
                a2a_module._fetch(f"{base}/slow", deadline=0.3)
            import threading

            stop = threading.Event()
            stop.set()
            with pytest.raises(AgentError, match="cancelled"):
                a2a_module._fetch(f"{base}/slow", stop=stop)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="SIGTERM terminates the process on Windows; no handler and no drain run",
    )
    def test_the_stop_signal_starts_the_drain_on_a_real_server(self, tmp_path):
        """A SIGTERM to a real uvicorn: the budget runs from the signal, not from the lifespan's end."""
        import signal
        import threading
        import time

        with _real_server(tmp_path, drain_seconds="1") as (proc, base):
            outcome = {}

            def blocking_send():
                async def call():
                    async with httpx.AsyncClient(base_url=base, timeout=30) as http:
                        card = await A2ACardResolver(http, base).get_agent_card()
                        client = ClientFactory(
                            ClientConfig(httpx_client=http, streaming=False)
                        ).create(card)
                        outcome["task"] = task_of(await send(client, user_message("q")))

                try:
                    asyncio.run(call())
                except Exception as err:  # noqa: BLE001 -- asserted below
                    outcome["error"] = err

            caller = threading.Thread(target=blocking_send)
            caller.start()
            time.sleep(1.0)
            signalled = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            caller.join(timeout=20)
            answered = time.monotonic() - signalled
            assert "error" not in outcome, outcome.get("error")
            task = outcome["task"]
            assert state_name(task) == "TASK_STATE_FAILED"
            assert hopsworks_of(task.status.message)["status"] == "executor_shutdown"
            # the one-second budget, not the handler's thirty seconds
            assert answered < 10
            # uvicorn re-raises the signal it captured once it has stopped
            assert proc.wait(timeout=20) in (0, -signal.SIGTERM)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="SIGTERM terminates the process on Windows; no handler and no drain run",
    )
    def test_a_stream_stays_open_through_the_drain_on_a_real_server(self, tmp_path):
        """sse-starlette would end the stream at the signal; the caller must see the terminal event."""
        import signal
        import threading
        import time

        with _real_server(tmp_path, drain_seconds="1") as (proc, base):
            outcome = {}

            def streaming_send():
                async def call():
                    async with httpx.AsyncClient(base_url=base, timeout=30) as http:
                        card = await A2ACardResolver(http, base).get_agent_card()
                        client = ClientFactory(
                            ClientConfig(httpx_client=http, streaming=True)
                        ).create(card)
                        outcome["events"] = await send(client, user_message("q"))

                try:
                    asyncio.run(call())
                except Exception as err:  # noqa: BLE001 -- asserted below
                    outcome["error"] = err

            caller = threading.Thread(target=streaming_send)
            caller.start()
            time.sleep(1.0)
            signalled = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            caller.join(timeout=20)
            ended = time.monotonic() - signalled
            assert "error" not in outcome, outcome.get("error")
            events = outcome["events"]
            last = events[-1]
            assert last.HasField("status_update"), events
            assert (
                TaskState.Name(last.status_update.status.state) == "TASK_STATE_FAILED"
            )
            assert (
                hopsworks_of(last.status_update.status.message)["status"]
                == "executor_shutdown"
            )
            # the stream outlived the signal for the budget, and no longer
            assert 0.5 < ended < 10
            assert proc.wait(timeout=20) in (0, -signal.SIGTERM)


class _real_server:  # noqa: N801 -- a context manager
    """A real uvicorn in a subprocess, serving an agent whose handler sleeps 30 s."""

    def __init__(self, tmp_path, drain_seconds):
        self._tmp_path = tmp_path
        self._drain_seconds = drain_seconds

    def __enter__(self):
        import os
        import socket
        import subprocess
        import sys
        import time

        script = self._tmp_path / "agent.py"
        script.write_text(
            "import asyncio, sys\n"
            "import uvicorn\n"
            "from hopsworks_agents.protocol import AgentApp\n"
            "app = AgentApp()\n"
            "@app.chat\n"
            "async def chat(request, ctx):\n"
            "    await asyncio.sleep(30)\n"
            "    return 'late'\n"
            "uvicorn.run(app, host='127.0.0.1', port=int(sys.argv[1]), "
            "log_level='warning')\n"
        )
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        env = dict(os.environ, HOPSWORKS_AGENT_TASK_DRAIN_SECONDS=self._drain_seconds)
        self._proc = subprocess.Popen([sys.executable, str(script), str(port)], env=env)
        base = f"http://127.0.0.1:{port}"
        for _ in range(200):
            try:
                if httpx.get(f"{base}/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            self._proc.kill()
            pytest.fail("the server did not come up")
        return self._proc, base

    def __exit__(self, *exc):
        if self._proc.poll() is None:
            self._proc.kill()


def _far():
    from datetime import timedelta

    from hopsworks_agents.protocol.pending import _utcnow

    return _utcnow() + timedelta(days=1)


class _drip_server:  # noqa: N801 -- a context manager
    """A local HTTP server for the fetch tests: a redirect, a big body, a slow drip."""

    def __enter__(self):
        import threading
        import time
        from http.server import BaseHTTPRequestHandler, HTTPServer

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):  # noqa: N802 -- the stdlib's name
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/small")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                if self.path == "/slow":
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    for _ in range(20):
                        self.wfile.write(b"1\r\nx\r\n")
                        self.wfile.flush()
                        time.sleep(0.1)
                    self.wfile.write(b"0\r\n\r\n")
                    return
                body = b"ok" if self.path == "/small" else b"x" * 64
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()

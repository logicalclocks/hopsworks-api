"""The A2A face of ``AgentApp``, driven with the official client over the app in process."""

import asyncio
import base64
import json

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
        assert [i["url"] for i in card["supportedInterfaces"]] == [
            "http://testserver/a2a",
            "http://testserver/a2a/rest",
        ]
        assert [i["protocolBinding"] for i in card["supportedInterfaces"]] == [
            "JSONRPC",
            "HTTP+JSON",
        ]
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

        monkeypatch.setattr(a2a_module, "_fetch", lambda url: ("text/csv", b"a,b"))
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

    def test_an_answer_to_the_wrong_question_keeps_the_task_paused(self):
        async def scenario():
            client, _ = await connect(self.refund_app())
            task = task_of(await send(client, user_message("refund", context_id="c1")))
            again = task_of(
                await send(
                    client,
                    user_message(
                        parts=[data_part({"other": {"approved": True}})],
                        context_id="c1",
                        task_id=task.id,
                    ),
                )
            )
            assert state_name(again) == "TASK_STATE_INPUT_REQUIRED"
            assert "answers none" in text_of(again.status.message)
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
            # the reply goes out before the turn is closed in the store
            for _ in range(100):
                if len(await asyncio.to_thread(memory.get, "conv")) == 4:
                    break
                await asyncio.sleep(0.02)

        run(scenario())
        assert [t["content"] for t in memory.get("conv")] == [
            "first",
            "0 before",
            "second",
            "2 before",
        ]

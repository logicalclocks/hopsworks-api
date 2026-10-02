import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from hopsworks_agents.protocol import AgentApp, AgentError, AgentResponse


def make_request(text, conversation_id=None):
    return {
        "conversation_id": conversation_id,
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }


def parse_sse(body: str):
    events = []
    for frame in body.strip().split("\n\n"):
        event, data = None, None
        for line in frame.split("\n"):
            if line.startswith("event: "):
                event = line[len("event: ") :]
            elif line.startswith("data: "):
                data = json.loads(line[len("data: ") :])
        events.append((event, data))
    return events


def build_basic_app():
    app = AgentApp(
        name="Test agent",
        description="unit test",
        welcome_message="Hi!",
        suggested_prompts=["What is attention?"],
    )

    @app.chat
    async def chat(request):
        if request.text == "boom":
            raise AgentError("It broke", code="boom", status_code=400)
        return AgentResponse.text(
            text=f"echo: {request.text}",
            conversation_id=request.conversation_id,
            citations=[{"title": "doc", "score": 0.9}],
        )

    return app


class TestManifestAndHealth:
    def test_manifest(self):
        client = TestClient(build_basic_app())
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["protocol"] == "hopsworks-agent"
        assert manifest["protocol_version"] == "1.4"
        assert manifest["agent"]["name"] == "Test agent"
        assert manifest["endpoints"] == {"chat": "/v1/chat", "feedback": "/v1/feedback"}
        assert manifest["capabilities"]["streaming"] is False
        assert manifest["ui"]["welcome_message"] == "Hi!"
        assert manifest["ui"]["suggested_prompts"] == ["What is attention?"]

    def test_health(self):
        client = TestClient(build_basic_app())
        assert client.get("/health").json() == {"status": "ok"}


class TestChat:
    def test_chat_assigns_conversation_id(self):
        client = TestClient(build_basic_app())
        response = client.post("/v1/chat", json=make_request("hello")).json()
        assert response["message"]["content"][0]["text"] == "echo: hello"
        assert response["conversation_id"].startswith("conv_")
        assert response["status"] == "completed"
        assert response["citations"] == [{"title": "doc", "score": 0.9}]

    def test_chat_keeps_conversation_id(self):
        client = TestClient(build_basic_app())
        response = client.post(
            "/v1/chat", json=make_request("hello", "conv_keep")
        ).json()
        assert response["conversation_id"] == "conv_keep"

    def test_string_return_is_wrapped(self):
        app = AgentApp()

        @app.chat
        def chat(request):  # sync + plain string
            return request.text.upper()

        client = TestClient(app)
        response = client.post("/v1/chat", json=make_request("hi")).json()
        assert response["message"]["content"][0]["text"] == "HI"
        assert response["message"]["role"] == "assistant"

    def test_agent_error_envelope(self):
        client = TestClient(build_basic_app())
        result = client.post("/v1/chat", json=make_request("boom"))
        assert result.status_code == 400
        assert result.json()["detail"] == {
            "code": "boom",
            "message": "It broke",
            "retryable": False,
        }

    def test_no_handler_is_501(self):
        client = TestClient(AgentApp())
        assert client.post("/v1/chat", json=make_request("x")).status_code == 501


class TestStreaming:
    def build_streaming_app(self):
        app = AgentApp(name="Streamer")

        @app.stream
        async def stream(request):
            for chunk in ["Atten", "tion"]:
                yield chunk
            yield AgentResponse.text(
                text="",
                conversation_id=request.conversation_id,
                usage={"output_tokens": 2},
            )

        return app

    def test_manifest_advertises_streaming(self):
        client = TestClient(self.build_streaming_app())
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["streaming"] is True
        assert manifest["endpoints"]["stream"] == "/v1/chat/stream"

    def test_stream_events(self):
        client = TestClient(self.build_streaming_app())
        result = client.post("/v1/chat/stream", json=make_request("q"))
        assert result.headers["content-type"].startswith("text/event-stream")
        events = parse_sse(result.text)
        assert events[0] == ("message.delta", {"delta": {"text": "Atten"}})
        assert events[1] == ("message.delta", {"delta": {"text": "tion"}})
        event, completed = events[-1]
        assert event == "message.completed"
        assert completed["message"]["content"][0]["text"] == "Attention"
        assert completed["usage"] == {"output_tokens": 2}
        assert completed["conversation_id"].startswith("conv_")

    def test_chat_collects_stream_when_no_chat_handler(self):
        client = TestClient(self.build_streaming_app())
        response = client.post("/v1/chat", json=make_request("q")).json()
        assert response["message"]["content"][0]["text"] == "Attention"

    def test_stream_falls_back_to_chat_handler(self):
        client = TestClient(build_basic_app())
        events = parse_sse(client.post("/v1/chat/stream", json=make_request("hi")).text)
        assert events[-1][0] == "message.completed"
        assert events[-1][1]["message"]["content"][0]["text"] == "echo: hi"

    def test_stream_error_event(self):
        app = AgentApp()

        @app.stream
        async def stream(request):
            yield "partial"
            raise AgentError("upstream died", code="upstream", retryable=True)

        client = TestClient(app)
        events = parse_sse(client.post("/v1/chat/stream", json=make_request("x")).text)
        assert events[-1] == (
            "error",
            {"code": "upstream", "message": "upstream died", "retryable": True},
        )


class TestRequestHelpers:
    def test_to_framework_messages(self):
        app = build_basic_app()

        captured = {}

        @app.chat
        async def chat(request):
            captured["messages"] = request.to_framework_messages()
            return "ok"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("hello world"))
        assert captured["messages"] == [{"role": "user", "content": "hello world"}]


class TestMultimodal:
    def build_vision_app(self):
        from hopsworks_agents.protocol import ImageContent, TextContent

        app = AgentApp(
            name="Vision agent",
            input_modalities=["text", "image"],
            output_modalities=["text", "image"],
        )

        @app.chat
        async def chat(request):
            n_images = len(request.images)
            return AgentResponse.parts(
                TextContent(text=f"got {n_images} image(s): {request.text}"),
                ImageContent(media_type="image/png", data="aGVsbG8="),
                conversation_id=request.conversation_id,
            )

        return app

    def test_manifest_modalities(self):
        client = TestClient(self.build_vision_app())
        caps = client.get("/.well-known/hopsworks-agent.json").json()["capabilities"]
        assert caps["input_modalities"] == ["text", "image"]
        assert caps["output_modalities"] == ["text", "image"]
        assert caps["attachments"] is True

    def test_text_only_manifest_defaults(self):
        client = TestClient(build_basic_app())
        caps = client.get("/.well-known/hopsworks-agent.json").json()["capabilities"]
        assert caps["input_modalities"] == ["text"]
        assert caps["attachments"] is False

    def test_image_request_and_response(self):
        client = TestClient(self.build_vision_app())
        response = client.post(
            "/v1/chat",
            json={
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "what is this?"},
                        {"type": "image", "media_type": "image/png", "data": "aW1n"},
                    ],
                }
            },
        ).json()
        parts = response["message"]["content"]
        assert parts[0] == {"type": "text", "text": "got 1 image(s): what is this?"}
        assert parts[1] == {
            "type": "image",
            "media_type": "image/png",
            "data": "aGVsbG8=",
        }

    def test_unknown_part_type_is_422(self):
        client = TestClient(self.build_vision_app())
        result = client.post(
            "/v1/chat",
            json={
                "message": {
                    "role": "user",
                    "content": [{"type": "hologram", "data": "x"}],
                }
            },
        )
        assert result.status_code == 422

    def test_stream_final_with_image_keeps_streamed_text(self):
        from hopsworks_agents.protocol import ImageContent

        app = AgentApp(output_modalities=["text", "image"])

        @app.stream
        async def stream(request):
            yield "Here is the chart"
            yield AgentResponse.parts(
                ImageContent(media_type="image/png", data="cGxvdA=="),
                conversation_id=request.conversation_id,
            )

        client = TestClient(app)
        events = parse_sse(
            client.post("/v1/chat/stream", json=make_request("chart")).text
        )
        event, completed = events[-1]
        assert event == "message.completed"
        parts = completed["message"]["content"]
        assert parts[0] == {"type": "text", "text": "Here is the chart"}
        assert parts[1]["type"] == "image"


class TestFrameworkAndTracing:
    def test_framework_defaults_to_custom(self, monkeypatch):
        monkeypatch.delenv("AGENT_FRAMEWORK", raising=False)
        app = AgentApp()
        assert app.framework == "custom"
        assert app.tracer_provider is None

    def test_framework_from_env(self, monkeypatch):
        monkeypatch.setenv("AGENT_FRAMEWORK", "langgraph")
        app = AgentApp()
        assert app.framework == "langgraph"
        manifest = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
        assert manifest["agent"]["framework"] == "langgraph"

    def test_explicit_framework_wins_over_env(self, monkeypatch):
        monkeypatch.setenv("AGENT_FRAMEWORK", "langgraph")
        app = AgentApp(framework="llamaindex")
        assert app.framework == "llamaindex"

    @pytest.mark.parametrize(
        ("framework", "expected"),
        [
            ("openai_agents", "openai_agents"),
            ("openai-agents", "openai_agents"),
            ("claude_agents", "claude_agents"),
            ("claude-agent-sdk", "claude_agents"),
        ],
    )
    def test_new_frameworks_and_aliases(self, monkeypatch, framework, expected):
        monkeypatch.delenv("AGENT_FRAMEWORK", raising=False)
        app = AgentApp(framework=framework)
        assert app.framework == expected

    def test_unknown_framework_falls_back(self, monkeypatch):
        monkeypatch.delenv("AGENT_FRAMEWORK", raising=False)
        app = AgentApp(framework="prolog")
        assert app.framework == "custom"

    def test_tracing_off_without_endpoint_env(self, monkeypatch):
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
        assert AgentApp(tracing=True).tracer_provider is None

    def test_tracing_disabled_explicitly(self, monkeypatch):
        monkeypatch.setenv(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces"
        )
        assert AgentApp(tracing=False).tracer_provider is None

    def test_tracing_graceful_without_otel_sdk(self, monkeypatch):
        # the test venv has no opentelemetry installed: the endpoint being set
        # must not crash the app, just leave it untraced
        monkeypatch.setenv(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces"
        )
        app = AgentApp()
        assert app.tracer_provider is None
        assert TestClient(app).get("/health").json() == {"status": "ok"}


class TestMemory:
    def build_memory_app(self, memory):
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            history = app.memory.get(request.conversation_id)
            return AgentResponse.text(
                text=f"history={len(history)}: {request.text}",
                conversation_id=request.conversation_id,
            )

        return app

    def test_in_memory_auto_records_and_serves_history(self):
        from hopsworks_agents.protocol import InMemoryAgentMemory

        memory = InMemoryAgentMemory()
        client = TestClient(self.build_memory_app(memory))
        first = client.post("/v1/chat", json=make_request("hello")).json()
        cid = first["conversation_id"]
        assert first["message"]["content"][0]["text"] == "history=0: hello"
        assert memory.get(cid) == [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "history=0: hello"},
        ]
        second = client.post("/v1/chat", json=make_request("again", cid)).json()
        assert second["message"]["content"][0]["text"] == "history=2: again"

    def test_in_memory_trims_to_max(self):
        from hopsworks_agents.protocol import InMemoryAgentMemory

        memory = InMemoryAgentMemory(max_messages=2)
        for i in range(4):
            memory.append("c1", "user", f"m{i}")
        assert memory.get("c1") == [
            {"role": "user", "content": "m2"},
            {"role": "user", "content": "m3"},
        ]

    def test_sql_memory_roundtrip(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        url = f"sqlite:///{tmp_path}/memory.db"
        memory = ManagedMemoryService(url)
        client = TestClient(self.build_memory_app(memory))
        cid = client.post("/v1/chat", json=make_request("hi")).json()["conversation_id"]
        # a fresh store over the same db sees the persisted turns (no cache)
        fresh = ManagedMemoryService(url)
        assert fresh.get(cid) == [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "history=0: hi"},
        ]
        fresh.clear(cid)
        assert fresh.get(cid) == []

    def test_memory_failure_does_not_break_chat(self):
        from hopsworks_agents.protocol import InMemoryAgentMemory

        class BrokenMemory(InMemoryAgentMemory):
            def begin_turn(self, *a, **k):
                raise RuntimeError("db down")

        client = TestClient(self.build_memory_app(BrokenMemory()))
        result = client.post("/v1/chat", json=make_request("hello"))
        assert result.status_code == 200

    def test_close_failure_does_not_break_chat(self):
        from hopsworks_agents.protocol import InMemoryAgentMemory

        class BrokenMemory(InMemoryAgentMemory):
            def end_turn(self, *a, **k):
                raise RuntimeError("db down")

        client = TestClient(self.build_memory_app(BrokenMemory()))
        result = client.post("/v1/chat", json=make_request("hello"))
        assert result.status_code == 200


class TestDeploymentMysqlUrl:
    def test_builds_url_from_envs(self, monkeypatch):
        from hopsworks_agents.protocol.memory import deployment_mysql_url

        monkeypatch.setenv("MYSQL_USER", "u")
        monkeypatch.setenv("MYSQL_HOST", "mysql.svc")
        monkeypatch.setenv("MYSQL_DB", "agents")
        monkeypatch.setenv("MYSQL_PASSWORD", "pw")
        monkeypatch.delenv("MYSQL_PORT", raising=False)
        assert deployment_mysql_url() == "mysql+pymysql://u:pw@mysql.svc:3306/agents"

    def test_missing_env_has_helpful_error(self, monkeypatch):
        from hopsworks_agents.protocol.memory import deployment_mysql_url

        monkeypatch.delenv("MYSQL_USER", raising=False)
        try:
            deployment_mysql_url()
            raise AssertionError("expected RuntimeError")
        except RuntimeError as err:
            assert "MYSQL_USER" in str(err)
            assert "url=" in str(err)


class TestSqlMemoryResilience:
    def test_unreachable_db_does_not_crash_construction_or_turns(self):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(url="mysql+pymysql://x:y@127.0.0.1:1/nope")
        assert memory.get("c1") == []
        memory.append("c1", "user", "hi")
        assert memory.get("c1") == []
        memory.clear("c1")

    def test_lazy_url_resolution_defers_env_errors(self, monkeypatch):
        from hopsworks_agents.protocol import ManagedMemoryService

        monkeypatch.delenv("MYSQL_USER", raising=False)
        memory = ManagedMemoryService()
        assert memory.get("c1") == []


class TestContextObject:
    def test_two_param_handler_receives_context(self):
        from hopsworks_agents.protocol import AgentApp, InMemoryAgentMemory

        app = AgentApp(memory=InMemoryAgentMemory())
        seen = {}

        @app.chat
        async def chat(request, ctx):
            seen["cid"] = ctx.conversation_id
            seen["history_len"] = len(ctx.history)
            seen["framework"] = ctx.framework
            seen["response_id"] = ctx.response_id
            return f"turn {len(ctx.history)}"

        client = TestClient(app)
        first = client.post("/v1/chat", json=make_request("hi")).json()
        assert seen["cid"] == first["conversation_id"]
        assert seen["history_len"] == 0
        assert seen["framework"] == "custom"
        # ctx.response_id matches the id on the response the client received
        assert first["id"] == seen["response_id"]
        # second turn sees prior history via ctx.history
        client.post("/v1/chat", json=make_request("again", first["conversation_id"]))
        assert seen["history_len"] == 2

    def test_one_param_handler_still_works(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp()

        @app.chat
        async def chat(request):
            return "ok"

        assert (
            TestClient(app)
            .post("/v1/chat", json=make_request("x"))
            .json()["message"]["content"][0]["text"]
            == "ok"
        )


class TestReadiness:
    def test_ready_ok(self):
        app = build_basic_app()
        result = TestClient(app).get("/ready")
        assert result.status_code == 200
        assert result.json()["status"] == "ready"

    def test_not_ready_without_handler(self):
        from hopsworks_agents.protocol import AgentApp

        result = TestClient(AgentApp()).get("/ready")
        assert result.status_code == 503
        assert result.json()["checks"]["handler"] is False

    def test_not_ready_when_memory_unreachable(self):
        from hopsworks_agents.protocol import AgentApp, ManagedMemoryService

        app = AgentApp(
            memory=ManagedMemoryService(url="mysql+pymysql://x:y@127.0.0.1:1/no")
        )

        @app.chat
        async def chat(request):
            return "ok"

        result = TestClient(app).get("/ready")
        assert result.status_code == 503
        assert result.json()["checks"]["memory"] is False


class TestConversationEndpoints:
    def test_history_and_clear(self):
        from hopsworks_agents.protocol import AgentApp, InMemoryAgentMemory

        app = AgentApp(memory=InMemoryAgentMemory())

        @app.chat
        async def chat(request):
            return "reply"

        client = TestClient(app)
        cid = client.post("/v1/chat", json=make_request("hello")).json()[
            "conversation_id"
        ]
        body = client.get(f"/v1/conversations/{cid}/messages").json()
        assert [(m["role"], m["content"]) for m in body["messages"]] == [
            ("user", "hello"),
            ("assistant", "reply"),
        ]
        assert body["summary"] is None
        assert body["summarized_through"] == 0
        assert client.delete(f"/v1/conversations/{cid}").status_code == 204
        assert client.get(f"/v1/conversations/{cid}/messages").json()["messages"] == []

    def test_endpoints_absent_without_memory(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp()

        @app.chat
        async def chat(request):
            return "ok"

        client = TestClient(app)
        assert client.get("/v1/conversations/x/messages").status_code == 404
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["conversation_management"] is False
        assert "conversations" not in manifest["endpoints"]

    def test_manifest_advertises_conversations_with_memory(self):
        from hopsworks_agents.protocol import AgentApp, InMemoryAgentMemory

        app = AgentApp(memory=InMemoryAgentMemory())

        @app.chat
        async def chat(request):
            return "ok"

        manifest = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["conversation_management"] is True
        assert manifest["endpoints"]["conversations"] == "/v1/conversations"


class TestToolEvents:
    def test_tool_events_capability_flag(self):
        from hopsworks_agents.protocol import AgentApp

        off = TestClient(AgentApp()).get("/.well-known/hopsworks-agent.json").json()
        assert off["capabilities"]["tool_events"] is False
        on = (
            TestClient(AgentApp(tool_events=True))
            .get("/.well-known/hopsworks-agent.json")
            .json()
        )
        assert on["capabilities"]["tool_events"] is True

    def test_emit_event_interleaves_in_stream(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp(tool_events=True)

        @app.stream
        async def stream(request, ctx):
            await ctx.emit_event("retrieve", status="running", message="searching")
            yield "answer "
            await ctx.emit_event("retrieve", status="done")
            yield "text"

        events = parse_sse(
            TestClient(app).post("/v1/chat/stream", json=make_request("q")).text
        )
        kinds = [e[0] for e in events]
        assert "tool_event" in kinds
        assert kinds[0] == "tool_event"
        assert events[0][1] == {
            "name": "retrieve",
            "status": "running",
            "message": "searching",
        }
        assert kinds[-1] == "message.completed"
        assert events[-1][1]["message"]["content"][0]["text"] == "answer text"

    def test_emit_event_buffered_in_non_streaming(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp()

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_event("tool", status="done")
            return "ok"

        response = TestClient(app).post("/v1/chat", json=make_request("x")).json()
        assert response["metadata"]["tool_events"] == [
            {"name": "tool", "status": "done"}
        ]


class TestAutoToolEvents:
    def test_span_processor_emits_from_tool_span(self):
        # simulate the OTel span processor path directly: a HandlerContext with
        # a queue receives running/done events keyed by span id
        import asyncio

        from hopsworks_agents.protocol.autoevents import current_context
        from hopsworks_agents.protocol.context import HandlerContext
        from hopsworks_agents.protocol.models import (
            ChatMessage,
            ChatRequest,
            TextContent,
        )

        async def run():
            req = ChatRequest(
                conversation_id="c1",
                message=ChatMessage(role="user", content=[TextContent(text="hi")]),
            )
            ctx = HandlerContext(req, None, "langgraph", "response_1")
            q: asyncio.Queue = asyncio.Queue()
            ctx._event_queue = q
            ctx._loop = asyncio.get_running_loop()
            token = current_context.set(ctx)
            try:
                # _emit_sync is what the span processor calls
                ctx._emit_sync("search_papers", "running", None, None, "span-abc")
                ctx._emit_sync("search_papers", "done", None, None, "span-abc")
            finally:
                current_context.reset(token)
            await asyncio.sleep(0)  # let call_soon_threadsafe run
            events = []
            while not q.empty():
                events.append(q.get_nowait())
            return events

        events = asyncio.run(run())
        assert events[0] == (
            "tool_event",
            {"name": "search_papers", "status": "running", "id": "span-abc"},
        )
        assert events[1] == (
            "tool_event",
            {"name": "search_papers", "status": "done", "id": "span-abc"},
        )

    def test_emit_event_id_collapses(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp(tool_events=True)

        @app.stream
        async def stream(request, ctx):
            await ctx.emit_event("t", status="running", event_id="e1")
            yield "x"
            await ctx.emit_event("t", status="done", event_id="e1")

        events = parse_sse(
            TestClient(app).post("/v1/chat/stream", json=make_request("q")).text
        )
        tool_events = [e[1] for e in events if e[0] == "tool_event"]
        assert tool_events[0] == {"name": "t", "status": "running", "id": "e1"}
        assert tool_events[1] == {"name": "t", "status": "done", "id": "e1"}


class TestStreamLangchainHelper:
    def _fake_events(self):
        # a langgraph astream_events(v2) sequence with a tool call
        async def gen():
            yield {
                "event": "on_chat_model_stream",
                "data": {"chunk": type("C", (), {"content": "Let me search "})()},
            }
            yield {
                "event": "on_tool_start",
                "name": "search_papers",
                "run_id": "r1",
                "data": {"input": {"query": "CoT"}},
            }
            yield {
                "event": "on_tool_end",
                "name": "search_papers",
                "run_id": "r1",
                "data": {},
            }
            yield {
                "event": "on_chat_model_stream",
                "data": {"chunk": type("C", (), {"content": "done."})()},
            }

        return gen()

    def test_helper_yields_text_and_emits_tool_events(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp(tool_events=True)
        helper = self

        @app.stream
        async def stream(request, ctx):
            async for delta in ctx.stream_langchain(helper._fake_events()):
                yield delta

        events = parse_sse(
            TestClient(app).post("/v1/chat/stream", json=make_request("q")).text
        )
        deltas = [e[1]["delta"]["text"] for e in events if e[0] == "message.delta"]
        tools = [e[1] for e in events if e[0] == "tool_event"]
        assert "".join(deltas) == "Let me search done."
        assert tools == [
            {
                "name": "search_papers",
                "status": "running",
                "id": "r1",
                "message": "{'query': 'CoT'}",
            },
            {"name": "search_papers", "status": "done", "id": "r1"},
        ]
        assert events[-1][1]["message"]["content"][0]["text"] == "Let me search done."


class TestGraph:
    class _Node:
        def __init__(self, nid, name=None):
            self.id = nid
            self.name = name or nid

    class _Edge:
        def __init__(self, source, target, data=None, conditional=False):
            self.source = source
            self.target = target
            self.data = data
            self.conditional = conditional

    class _Drawable:
        def __init__(self, nodes, edges):
            self.nodes = nodes
            self.edges = edges

    class _Compiled:
        def __init__(self, drawable):
            self._d = drawable

        def get_graph(self):
            return self._d

    def _langgraph_like(self):
        # mirrors agent.get_graph(): __start__ -> agent -> action/__end__
        nodes = {
            "__start__": self._Node("__start__"),
            "agent": self._Node("agent"),
            "action": self._Node("action"),
            "__end__": self._Node("__end__"),
        }
        edges = [
            self._Edge("__start__", "agent"),
            self._Edge("agent", "action", data="continue", conditional=True),
            self._Edge("agent", "__end__", data="end", conditional=True),
            self._Edge("action", "agent"),
        ]
        return self._Compiled(self._Drawable(nodes, edges))

    def test_manifest_and_endpoint(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp(graph=self._langgraph_like())

        @app.chat
        async def chat(request):
            return "ok"

        client = TestClient(app)
        manifest = client.get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["graph"] is True
        assert manifest["endpoints"]["graph"] == "/v1/graph"

        graph = client.get("/v1/graph").json()
        assert {n["id"] for n in graph["nodes"]} == {
            "__start__",
            "agent",
            "action",
            "__end__",
        }
        cont = next(e for e in graph["edges"] if e.get("label") == "continue")
        assert cont["source"] == "agent" and cont["target"] == "action"
        assert cont["conditional"] is True

    def test_no_graph_capability_off(self):
        app = build_basic_app()
        m = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
        assert m["capabilities"]["graph"] is False
        assert "graph" not in m["endpoints"]
        assert TestClient(app).get("/v1/graph").status_code == 404

    def test_accepts_plain_dict(self):
        from hopsworks_agents.protocol import AgentApp
        from hopsworks_agents.protocol.graph import to_graph_spec

        spec = {"nodes": [{"id": "a", "label": "a"}], "edges": []}
        assert to_graph_spec(spec) == spec
        app = AgentApp(graph=spec)

        @app.chat
        async def chat(request):
            return "ok"

        assert TestClient(app).get("/v1/graph").json() == spec

    def test_unreadable_graph_is_ignored(self):
        from hopsworks_agents.protocol.graph import to_graph_spec

        assert to_graph_spec(object()) is None
        assert to_graph_spec(None) is None


class TestStreamLlamaindexHelper:
    def _handler(self):
        # duck-typed llama-index workflow events
        AgentStream = type("AgentStream", (), {})
        ToolCall = type("ToolCall", (), {})
        ToolCallResult = type("ToolCallResult", (), {})

        def ev(cls, **attrs):
            e = cls()
            for k, v in attrs.items():
                setattr(e, k, v)
            return e

        events = [
            ev(AgentStream, delta="Let me search "),
            ev(
                ToolCall,
                tool_name="search_papers",
                tool_id="t1",
                tool_kwargs={"query": "CoT"},
            ),
            ev(ToolCallResult, tool_name="search_papers", tool_id="t1"),
            ev(AgentStream, delta="done."),
        ]

        class Handler:
            async def stream_events(self):
                for e in events:
                    yield e

        return Handler()

    def test_yields_text_and_emits_tool_events(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp(tool_events=True)
        helper = self

        @app.stream
        async def stream(request, ctx):
            async for delta in ctx.stream_llamaindex(helper._handler()):
                yield delta

        events = parse_sse(
            TestClient(app).post("/v1/chat/stream", json=make_request("q")).text
        )
        deltas = [e[1]["delta"]["text"] for e in events if e[0] == "message.delta"]
        tools = [e[1] for e in events if e[0] == "tool_event"]
        assert "".join(deltas) == "Let me search done."
        assert tools == [
            {
                "name": "search_papers",
                "status": "running",
                "id": "t1",
                "message": "{'query': 'CoT'}",
            },
            {"name": "search_papers", "status": "done", "id": "t1"},
        ]


class TestLlamaIndexWorkflowGraph:
    def _workflow(self):
        # duck-typed LlamaIndex Workflow: _get_steps() -> {name: fn with __step_config}
        Start = type("StartEvent", (), {})
        Stop = type("StopEvent", (), {})
        Search = type("SearchEvent", (), {})
        Answer = type("AnswerEvent", (), {})

        def step(fn, accepted, returns):
            cfg = type("Cfg", (), {})()
            cfg.accepted_events = accepted
            cfg.return_types = returns
            setattr(fn, "__step_config", cfg)
            return fn

        def plan():
            pass

        def search():
            pass

        def answer():
            pass

        step(plan, [Start], [Search, Stop])  # branches: search or stop
        step(search, [Search], [Answer])
        step(answer, [Answer], [Stop])

        class WF:
            def _get_steps(self):
                return {"plan": plan, "search": search, "answer": answer}

        return WF()

    def test_workflow_graph_from_steps(self):
        from hopsworks_agents.protocol.graph import to_graph_spec

        spec = to_graph_spec(self._workflow())
        assert spec is not None
        ids = {n["id"] for n in spec["nodes"]}
        assert ids == {"plan", "search", "answer", "__start__", "__end__"}
        edges = {(e["source"], e["target"]) for e in spec["edges"]}
        assert ("__start__", "plan") in edges
        assert ("plan", "search") in edges  # via SearchEvent
        assert ("search", "answer") in edges  # via AnswerEvent
        assert ("answer", "__end__") in edges  # via StopEvent
        assert ("plan", "__end__") in edges  # plan can also stop
        # plan branches (SearchEvent | StopEvent) -> conditional edges
        plan_search = next(
            e
            for e in spec["edges"]
            if e["source"] == "plan" and e["target"] == "search"
        )
        assert plan_search["conditional"] is True
        assert plan_search["label"] == "SearchEvent"

    def test_served_via_agentapp(self):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp(graph=self._workflow())

        @app.chat
        async def chat(request):
            return "ok"

        client = TestClient(app)
        assert (
            client.get("/.well-known/hopsworks-agent.json").json()["capabilities"][
                "graph"
            ]
            is True
        )
        assert "plan" in {n["id"] for n in client.get("/v1/graph").json()["nodes"]}


class TestTurnLifecycle:
    """The user message is recorded before the handler runs, so every exit path.

    has to close the turn. These cover the paths that previously just skipped
    recording entirely — where skipping now means leaving an orphan.
    """

    def _rows(self, memory, conversation_id=None):
        from sqlalchemy import select

        memory.healthcheck()
        t = memory._table
        stmt = select(t).order_by(t.c.id)
        if conversation_id:
            stmt = stmt.where(t.c.conversation_id == conversation_id)
        with memory._engine.connect() as conn:
            return conn.execute(stmt).fetchall()

    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t", **kw
        )

    def test_open_turn_is_invisible_until_closed(self, tmp_path):
        memory = self._store(tmp_path)
        seen = {}

        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request, ctx):
            # the handler can read the message it is answering...
            seen["history"] = memory.get(request.conversation_id)
            return "answer"

        client = TestClient(app)
        cid = client.post("/v1/chat", json=make_request("question")).json()[
            "conversation_id"
        ]
        # ...but it is not yet part of history, because the turn is still open
        assert seen["history"] == []
        assert memory.get(cid) == [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "answer"},
        ]
        assert {r.status for r in self._rows(memory, cid)} == {"closed"}

    def test_handler_error_abandons_turn_and_leaves_no_orphan(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            raise AgentError("nope", code="boom", status_code=400)

        client = TestClient(app)
        result = client.post("/v1/chat", json=make_request("question"))
        assert result.status_code == 400
        cid = "c-err"
        client.post("/v1/chat", json=make_request("q2", conversation_id=cid))
        # the failed turn's question is retained for debugging but never
        # reappears as history
        rows = self._rows(memory)
        assert [r.status for r in rows] == ["abandoned", "abandoned"]
        assert memory.get(cid) == []

    def test_stream_error_abandons_turn(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.stream
        async def stream(request):
            yield "partial"
            raise RuntimeError("mid-stream failure")

        client = TestClient(app)
        with client.stream(
            "POST", "/v1/chat/stream", json=make_request("question", "c1")
        ) as r:
            body = "".join(r.iter_text())
        assert "event: error" in body
        assert [r.status for r in self._rows(memory, "c1")] == ["abandoned"]
        assert memory.get("c1") == []

    def test_successful_stream_closes_turn(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.stream
        async def stream(request):
            yield "hello "
            yield "world"

        client = TestClient(app)
        with client.stream(
            "POST", "/v1/chat/stream", json=make_request("question", "c1")
        ) as r:
            "".join(r.iter_text())
        assert memory.get("c1") == [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "hello world"},
        ]

    def test_turn_rows_carry_identity(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            return "answer"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("question", "c1"))
        rows = self._rows(memory, "c1")
        assert [r.seq for r in rows] == [0, 1]
        assert [r.role for r in rows] == ["user", "assistant"]
        # one turn, one message id, linking the reply to the question
        assert len({r.turn_id for r in rows}) == 1
        assert len({r.message_id for r in rows}) == 1
        assert rows[0].message_id is not None

    def test_tool_events_are_recorded_but_not_history(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory, tool_events=True)

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_event("search", status="done")
            return "answer"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("question", "c1"))
        rows = self._rows(memory, "c1")
        assert [r.memory_type for r in rows] == ["message", "event", "message"]
        # excluded from what the model reads back
        assert [t["role"] for t in memory.get("c1")] == ["user", "assistant"]
        assert json.loads(rows[1].content)["name"] == "search"

    def test_reaper_abandons_stale_open_turns(self, tmp_path):
        memory = self._store(tmp_path, turn_timeout_seconds=0)
        memory.begin_turn("c1", "turn_stale", "user", "question")
        assert memory.get("c1") == []
        assert memory.reap_open_turns() == 1
        assert [r.status for r in self._rows(memory, "c1")] == ["abandoned"]
        # already-reaped rows are not re-counted
        assert memory.reap_open_turns() == 0

    def test_reaper_spares_turns_inside_the_timeout(self, tmp_path):
        memory = self._store(tmp_path, turn_timeout_seconds=3600)
        memory.begin_turn("c1", "turn_live", "user", "question")
        assert memory.reap_open_turns() == 0
        assert [r.status for r in self._rows(memory, "c1")] == ["open"]

    def test_abandoned_turn_does_not_poison_the_cache(self, tmp_path):
        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "one")
        memory.record_item("c1", "t1", "assistant", "first")
        memory.end_turn("c1", "t1")
        assert len(memory.get("c1")) == 2
        memory.begin_turn("c1", "t2", "user", "two")
        memory.end_turn("c1", "t2", status="abandoned")
        assert memory.get("c1") == [
            {"role": "user", "content": "one"},
            {"role": "assistant", "content": "first"},
        ]


class TestMemorySchemaGuards:
    def test_rejects_table_name_with_invalid_suffix(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="items; DROP TABLE x"
        )
        # degrades to stateless rather than creating a junk table
        assert memory.healthcheck() is False

    def test_requires_deployment_id_when_url_is_derived(self, monkeypatch):
        from hopsworks_agents.protocol import ManagedMemoryService

        monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
        memory = ManagedMemoryService()
        # tables are shared now, so an agent with no identity would read and
        # write every other agent's rows rather than merely share a table name
        with pytest.raises(RuntimeError, match="DEPLOYMENT_ID"):
            memory._resolve_deployment_id()

    def test_deployment_id_scopes_rows_not_the_table_name(self, monkeypatch, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        monkeypatch.setenv("DEPLOYMENT_ID", "42")
        memory = ManagedMemoryService(url=f"sqlite:///{tmp_path}/m.db")
        assert memory._resolve_deployment_id() == "42"
        assert memory._resolve_table_name() == "agent_memory_messages_1"

    def test_rejects_a_deployment_id_that_is_not_an_identifier(
        self, monkeypatch, tmp_path
    ):
        from hopsworks_agents.protocol import ManagedMemoryService

        monkeypatch.setenv("DEPLOYMENT_ID", "42; DROP TABLE x")
        memory = ManagedMemoryService(url=f"sqlite:///{tmp_path}/m.db")
        with pytest.raises(RuntimeError, match="Invalid deployment_id"):
            memory._resolve_deployment_id()

    def test_records_schema_version(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService
        from hopsworks_agents.protocol.memory import SCHEMA_VERSION
        from sqlalchemy import select

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t"
        )
        assert memory.healthcheck() is True
        with memory._engine.connect() as conn:
            row = conn.execute(select(memory._meta)).fetchone()
        assert row.schema_version == SCHEMA_VERSION

    def test_refuses_table_written_by_a_newer_sdk(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService
        from hopsworks_agents.protocol.memory import SCHEMA_VERSION

        url = f"sqlite:///{tmp_path}/m.db"
        first = ManagedMemoryService(url=url, table_name="agent_memory_messages_t")
        assert first.healthcheck() is True
        with first._engine.begin() as conn:
            conn.execute(first._meta.update().values(schema_version=SCHEMA_VERSION + 1))
        # a second process on an older SDK must not write through it
        second = ManagedMemoryService(url=url, table_name="agent_memory_messages_t")
        assert second.healthcheck() is False
        second.begin_turn("c1", "t1", "user", "hi")
        assert second.get("c1") == []


class TestStreamAbort:
    """A client that vanishes mid-stream is the case that previously recorded.

    nothing at all; now the question is already stored, so the generator's
    cleanup has to close the turn.
    """

    def test_closing_the_stream_early_abandons_the_turn(self, tmp_path):
        import asyncio

        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t"
        )
        app = AgentApp(memory=memory)

        @app.stream
        async def stream(request):
            yield "first"
            await asyncio.sleep(10)  # client goes away while we are here
            yield "never"

        async def scenario():
            from hopsworks_agents.protocol.models import ChatRequest

            request = ChatRequest.model_validate(make_request("question", "c1"))
            ctx = app._prepare(request)
            await app._open_turn(ctx)

            class _Raw:
                headers: dict = {}

                async def is_disconnected(self):
                    return False

            agen = app._stream_events(ctx, _Raw())
            await agen.__anext__()  # pull the first delta
            await agen.aclose()  # client disconnects

        asyncio.run(scenario())

        memory.healthcheck()
        with memory._engine.connect() as conn:
            rows = conn.execute(memory._table.select()).fetchall()
        assert [r.status for r in rows] == ["abandoned"]
        assert memory.get("c1") == []

    def test_the_turn_closes_inside_aclose_not_at_loop_shutdown(self, tmp_path):
        """Assert while the loop is still running.

        The test above reads the store after ``asyncio.run`` returns, and
        ``asyncio.run`` calls ``shutdown_asyncgens()`` on its way out — which
        closes any async generator whose cleanup was still pending and makes
        the turn look finalized even when the disconnect path did not do it.
        Under uvicorn the loop is never shut down per request, so cleanup
        deferred to generator finalization means the turn stays open with its
        question already recorded. This asserts the finalization happened by
        the time ``aclose()`` returned.
        """
        import asyncio

        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t"
        )
        app = AgentApp(memory=memory)

        @app.stream
        async def stream(request):
            yield "first"
            await asyncio.sleep(10)
            yield "never"

        async def scenario():
            from hopsworks_agents.protocol.models import ChatRequest

            request = ChatRequest.model_validate(make_request("question", "c1"))
            ctx = app._prepare(request)
            await app._open_turn(ctx)

            class _Raw:
                headers: dict = {}

                async def is_disconnected(self):
                    return False

            agen = app._stream_events(ctx, _Raw())
            await agen.__anext__()
            await agen.aclose()
            with memory._engine.connect() as conn:
                return [
                    r.status for r in conn.execute(memory._table.select()).fetchall()
                ]

        assert asyncio.run(scenario()) == ["abandoned"]


def _fake_summarizer(calls):
    """Records what it was asked to fold and returns a deterministic summary."""

    def summarize(previous, turns):
        calls.append((previous, list(turns)))
        joined = "; ".join(t["content"] for t in turns)
        return f"[{previous}] {joined}" if previous else joined

    return summarize


class TestSummarization:
    def _store(self, tmp_path, calls=None, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        kw.setdefault("summarize", _fake_summarizer(calls if calls is not None else []))
        kw.setdefault("summarize_after_messages", 4)
        kw.setdefault("keep_recent_messages", 2)
        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t", **kw
        )

    def _turn(self, memory, cid, q, a):
        from hopsworks_agents.protocol.memory import new_turn_id

        tid = new_turn_id()
        memory.begin_turn(cid, tid, "user", q)
        memory.record_item(cid, tid, "assistant", a)
        memory.end_turn(cid, tid)

    def test_no_fold_before_the_threshold(self, tmp_path):
        calls = []
        memory = self._store(tmp_path, calls)
        self._turn(memory, "c1", "q1", "a1")
        assert asyncio.run(memory.maybe_summarize("c1")) is False
        assert calls == []
        assert memory.get_summary("c1") is None

    def test_fold_summarizes_and_shrinks_history(self, tmp_path):
        calls = []
        memory = self._store(tmp_path, calls)
        for i in range(3):
            self._turn(memory, "c1", f"q{i}", f"a{i}")
        assert len(memory.get("c1")) == 6

        assert asyncio.run(memory.maybe_summarize("c1")) is True
        # folded everything except the verbatim tail
        assert memory.get_summary("c1") == "q0; a0; q1; a1"
        assert memory.get("c1") == [
            {"role": "user", "content": "q2"},
            {"role": "assistant", "content": "a2"},
        ]
        # and the folded turns are not handed to the model twice
        assert calls[0][0] is None
        assert [t["content"] for t in calls[0][1]] == ["q0", "a0", "q1", "a1"]

    def test_second_fold_is_incremental(self, tmp_path):
        calls = []
        memory = self._store(tmp_path, calls)
        for i in range(3):
            self._turn(memory, "c1", f"q{i}", f"a{i}")
        asyncio.run(memory.maybe_summarize("c1"))
        for i in range(3, 6):
            self._turn(memory, "c1", f"q{i}", f"a{i}")
        assert asyncio.run(memory.maybe_summarize("c1")) is True

        # the previous summary is passed back in, so the summary is rewritten
        # rather than rebuilt from the whole conversation
        assert calls[1][0] == "q0; a0; q1; a1"
        assert [t["content"] for t in calls[1][1]] == [
            "q2",
            "a2",
            "q3",
            "a3",
            "q4",
            "a4",
        ]
        assert memory.get("c1") == [
            {"role": "user", "content": "q5"},
            {"role": "assistant", "content": "a5"},
        ]

    def test_fold_never_crosses_an_open_turn(self, tmp_path):
        """The bug pre-insertion creates: fold a question whose answer is still.

        being generated and the reply is left dangling with no question.
        """
        calls = []
        memory = self._store(tmp_path, calls, keep_recent_messages=0)
        for i in range(2):
            self._turn(memory, "c1", f"q{i}", f"a{i}")
        # a concurrent turn is mid-flight
        memory.begin_turn("c1", "turn_inflight", "user", "in-flight question")

        assert asyncio.run(memory.maybe_summarize("c1")) is True
        folded = [t["content"] for t in calls[0][1]]
        assert folded == ["q0", "a0", "q1", "a1"]
        assert "in-flight question" not in folded

        # the in-flight turn completes and stays visible with its own question
        memory.record_item("c1", "turn_inflight", "assistant", "late answer")
        memory.end_turn("c1", "turn_inflight")
        assert memory.get("c1") == [
            {"role": "user", "content": "in-flight question"},
            {"role": "assistant", "content": "late answer"},
        ]

    def test_summarizer_failure_leaves_history_intact(self, tmp_path):
        def boom(previous, turns):
            raise RuntimeError("model unavailable")

        memory = self._store(tmp_path, summarize=boom)
        for i in range(3):
            self._turn(memory, "c1", f"q{i}", f"a{i}")
        assert asyncio.run(memory.maybe_summarize("c1")) is False
        # nothing folded, nothing lost — the batch is retried on a later turn
        assert memory.get_summary("c1") is None
        assert len(memory.get("c1")) == 6

    def test_empty_summary_is_not_committed(self, tmp_path):
        memory = self._store(tmp_path, summarize=lambda previous, turns: "   ")
        for i in range(3):
            self._turn(memory, "c1", f"q{i}", f"a{i}")
        assert asyncio.run(memory.maybe_summarize("c1")) is False
        assert len(memory.get("c1")) == 6

    def test_async_summarizer(self, tmp_path):
        seen = {}

        async def summarize(previous, turns):
            seen["n"] = len(turns)
            return "async summary"

        memory = self._store(tmp_path, summarize=summarize)
        for i in range(3):
            self._turn(memory, "c1", f"q{i}", f"a{i}")
        assert asyncio.run(memory.maybe_summarize("c1")) is True
        assert seen["n"] == 4
        assert memory.get_summary("c1") == "async summary"

    def test_concurrent_fold_loses_without_double_folding(self, tmp_path):
        """Two replicas folding at once: the optimistic lock lets exactly one.

        win, and the loser wastes a call rather than losing rows.
        """
        calls = []
        memory = self._store(tmp_path, calls)
        for i in range(3):
            self._turn(memory, "c1", f"q{i}", f"a{i}")

        claim = memory._claim_fold("c1")
        assert claim is not None
        version, _, turns, max_id, count = claim
        # another replica commits first
        assert memory._commit_fold("c1", version, "winner", max_id, count) is True
        # our stale-version commit is rejected
        assert memory._commit_fold("c1", version, "loser", max_id, count) is False
        assert memory.get_summary("c1") == "winner"

    def test_fold_invalidates_cache_across_replicas(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        url = f"sqlite:///{tmp_path}/m.db"
        kw = {
            "table_name": "agent_memory_messages_t",
            "summarize": _fake_summarizer([]),
            "summarize_after_messages": 4,
            "keep_recent_messages": 2,
        }
        a = ManagedMemoryService(url=url, **kw)
        b = ManagedMemoryService(url=url, **kw)

        for i in range(3):
            self._turn(a, "c1", f"q{i}", f"a{i}")
        assert len(b.get("c1")) == 6  # b caches the pre-fold history

        asyncio.run(a.maybe_summarize("c1"))
        # b must notice the fold rather than serve compacted-away turns
        assert b.get("c1") == [
            {"role": "user", "content": "q2"},
            {"role": "assistant", "content": "a2"},
        ]

    def test_message_count_counts_only_messages(self, tmp_path):
        memory = self._store(tmp_path)
        from hopsworks_agents.protocol.memory import new_turn_id

        tid = new_turn_id()
        memory.begin_turn("c1", tid, "user", "q")
        memory.record_item("c1", tid, "tool", '{"name": "x"}', memory_type="event")
        memory.record_item("c1", tid, "assistant", "a")
        memory.end_turn("c1", tid)
        with memory._engine.connect() as conn:
            row = conn.execute(memory._sessions.select()).fetchone()
        assert row.message_count == 2


class TestSummaryOnContext:
    def test_summary_and_system_context(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_t",
            summarize=_fake_summarizer([]),
            summarize_after_messages=2,
            keep_recent_messages=0,
        )
        seen = {}
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request, ctx):
            seen["summary"] = ctx.summary
            seen["context"] = ctx.system_context()
            seen["history"] = ctx.history
            return "answer"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("first", "c1"))
        assert seen["summary"] is None
        assert seen["context"] == ""  # safe to concatenate before any fold

        client.post("/v1/chat", json=make_request("second", "c1"))
        assert seen["summary"] == "first; answer"
        assert "first; answer" in seen["context"]
        assert seen["context"].startswith("\n\nContext from earlier:")
        # the folded turns are gone from the verbatim window
        assert seen["history"] == []

    def test_no_summarizer_means_no_summary(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t"
        )
        memory.healthcheck()
        assert memory._sessions is None  # tier-2 table is not created
        assert memory.get_summary("c1") is None
        assert asyncio.run(memory.maybe_summarize("c1")) is False


class TestRetention:
    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t", **kw
        )

    def test_messages_keep_forever_events_expire(self, tmp_path):
        memory = self._store(tmp_path)
        from hopsworks_agents.protocol.memory import new_turn_id

        tid = new_turn_id()
        memory.begin_turn("c1", tid, "user", "q")
        memory.record_item("c1", tid, "tool", "{}", memory_type="event")
        memory.end_turn("c1", tid)
        with memory._engine.connect() as conn:
            rows = conn.execute(
                memory._table.select().order_by(memory._table.c.id)
            ).fetchall()
        by_type = {r.memory_type: r for r in rows}
        # the transcript is not deleted on a timer; telemetry is
        assert by_type["message"].expires_at is None
        assert by_type["event"].expires_at is not None

    def test_message_retention_is_opt_in(self, tmp_path):
        memory = self._store(tmp_path, message_retention_days=7)
        memory.append("c1", "user", "q")
        with memory._engine.connect() as conn:
            row = conn.execute(memory._table.select()).fetchone()
        assert row.expires_at is not None

    def test_prune_deletes_only_expired_rows(self, tmp_path):
        memory = self._store(tmp_path, tool_event_retention_days=-1)
        from hopsworks_agents.protocol.memory import new_turn_id

        tid = new_turn_id()
        memory.begin_turn("c1", tid, "user", "q")
        memory.record_item("c1", tid, "tool", "{}", memory_type="event")
        memory.record_item("c1", tid, "assistant", "a")
        memory.end_turn("c1", tid)

        assert memory.prune_expired() == 1
        with memory._engine.connect() as conn:
            rows = conn.execute(memory._table.select()).fetchall()
        assert [r.memory_type for r in rows] == ["message", "message"]
        assert memory.prune_expired() == 0


class TestTranscriptEndpoint:
    def test_transcript_keeps_folded_turns_and_reports_the_cutoff(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_t",
            summarize=_fake_summarizer([]),
            summarize_after_messages=2,
            keep_recent_messages=0,
        )
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            return "reply"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("one", "c1"))
        client.post("/v1/chat", json=make_request("two", "c1"))

        body = client.get("/v1/conversations/c1/messages").json()
        # the model's window has been compacted away...
        assert memory.get("c1") == []
        # ...but the human-facing record is complete, with the summary beside it
        assert [m["content"] for m in body["messages"]] == [
            "one",
            "reply",
            "two",
            "reply",
        ]
        assert body["summary"] is not None
        assert body["summarized_through"] > 0

    def test_events_excluded_by_default(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t"
        )
        app = AgentApp(memory=memory, tool_events=True)

        @app.chat
        async def chat(request, ctx):
            await ctx.emit_event("search", status="done")
            return "reply"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("q", "c1"))

        default = client.get("/v1/conversations/c1/messages").json()["messages"]
        assert {m["memory_type"] for m in default} == {"message"}
        with_events = client.get("/v1/conversations/c1/messages?include=events").json()[
            "messages"
        ]
        assert "event" in {m["memory_type"] for m in with_events}


class TestDurableState:
    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        kw.setdefault("long_term", True)
        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t", **kw
        )

    def test_upsert_does_not_accumulate(self, tmp_path):
        memory = self._store(tmp_path)
        memory.set_state("user", "alice", "lang", "python")
        memory.set_state("user", "alice", "lang", "rust")
        assert memory.get_state("user", "alice", "lang") == "rust"
        assert len(memory.list_state("user", "alice")) == 1

    def test_scopes_are_isolated(self, tmp_path):
        memory = self._store(tmp_path)
        memory.set_state("user", "alice", "k", "user value")
        memory.set_state("session", "c1", "k", "session value")
        memory.set_state("app", "", "k", "app value")
        assert memory.get_state("user", "alice", "k") == "user value"
        assert memory.get_state("session", "c1", "k") == "session value"
        assert memory.get_state("app", "", "k") == "app value"
        assert memory.get_state("user", "bob", "k") is None

    def test_value_is_capped(self, tmp_path):
        memory = self._store(tmp_path, max_state_value_chars=10)
        memory.set_state("user", "alice", "k", "x" * 500)
        assert memory.get_state("user", "alice", "k") == "x" * 10

    def test_agent_writes_are_evicted_past_the_cap(self, tmp_path):
        memory = self._store(tmp_path, max_state_keys_written=3)
        for i in range(6):
            memory.set_state("user", "alice", f"k{i}", str(i))
        rows = memory.list_state("user", "alice")
        assert len(rows) == 3
        # the most recently written survive
        assert {r["key"] for r in rows} == {"k3", "k4", "k5"}

    def test_operator_writes_do_not_expire(self, tmp_path):
        memory = self._store(tmp_path, state_ttl_days=30)
        memory.set_state("app", "", "policy", "be terse", written_by="operator")
        memory.set_state("user", "alice", "lang", "python")
        app_row = memory.list_state("app", "")[0]
        user_row = memory.list_state("user", "alice")[0]
        assert app_row["expires_at"] is None
        assert app_row["written_by"] == "operator"
        assert user_row["expires_at"] is not None

    def test_expired_state_is_not_returned(self, tmp_path):
        memory = self._store(tmp_path, state_ttl_days=-1)
        memory.set_state("user", "alice", "stale", "old fact")
        # expiry applies on read, not only when a prune pass happens to run
        assert memory.list_state("user", "alice") == []
        assert memory.get_state("user", "alice", "stale") is None

    def test_delete_one_and_all(self, tmp_path):
        memory = self._store(tmp_path)
        memory.set_state("user", "alice", "a", "1")
        memory.set_state("user", "alice", "b", "2")
        assert memory.delete_state("user", "alice", "a") == 1
        assert {r["key"] for r in memory.list_state("user", "alice")} == {"b"}
        assert memory.delete_state("user", "alice") == 1
        assert memory.list_state("user", "alice") == []

    def test_state_block_is_bounded(self, tmp_path):
        memory = self._store(
            tmp_path, max_state_keys_injected=2, state_inject_value_chars=5
        )
        for i in range(5):
            memory.set_state("user", "alice", f"k{i}", "v" * 50)
        block = memory.state_block("alice", "c1")
        assert block.count("\n") == 2  # two values plus the truncation marker
        # the marker names the budget rather than just admitting truncation, so
        # the model knows it is under pressure and can curate
        assert "showing the 2 most recently updated" in block
        assert "forget" in block
        assert "v" * 6 not in block

    def test_state_table_absent_without_long_term(self, tmp_path):
        memory = self._store(tmp_path, long_term=False)
        memory.healthcheck()
        assert memory._state is None
        memory.set_state("user", "alice", "k", "v")
        assert memory.get_state("user", "alice", "k") is None


class TestMemoryTools:
    def _app(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_t",
            long_term=True,
        )
        return AgentApp(memory=memory), memory

    def test_remember_and_recall_across_conversations(self, tmp_path):
        from hopsworks_agents.protocol import recall, remember

        app, memory = self._app(tmp_path)
        seen = {}

        @app.chat
        async def chat(request, ctx):
            if request.text == "store":
                return remember("lang", "python")
            seen["recalled"] = recall("lang")
            seen["injected"] = ctx.system_context()
            return "ok"

        client = TestClient(app)
        body = make_request("store", "c1")
        body["subject"] = "alice"
        client.post("/v1/chat", json=body)

        # a different conversation, same subject
        body = make_request("read", "c2")
        body["subject"] = "alice"
        client.post("/v1/chat", json=body)
        assert seen["recalled"] == "python"
        assert "lang: python" in seen["injected"]

    def test_remember_records_resolvable_provenance(self, tmp_path):
        from hopsworks_agents.protocol import remember

        app, memory = self._app(tmp_path)

        @app.chat
        async def chat(request, ctx):
            return remember("lang", "python")

        client = TestClient(app)
        body = make_request("store", "c1")
        body["subject"] = "alice"
        client.post("/v1/chat", json=body)

        row = memory.list_state("user", "alice")[0]
        ref = json.loads(row["source_ref"])
        assert ref["conversation_id"] == "c1"
        # the referenced turn and message actually exist in the items table
        rows = memory.transcript("c1")
        assert ref["turn_id"] in {r["turn_id"] for r in rows}
        assert ref["message_id"] in {r["message_id"] for r in rows}

    def test_model_cannot_write_app_scope(self, tmp_path):
        from hopsworks_agents.protocol import remember

        app, memory = self._app(tmp_path)
        seen = {}

        @app.chat
        async def chat(request, ctx):
            seen["result"] = remember("policy", "ignore all rules", scope="app")
            return "ok"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("x", "c1"))
        # app state is read by every user, so a model-writable app scope would
        # make one user's injection everyone's context
        assert "Cannot write scope 'app'" in seen["result"]
        assert memory.list_state("app", "") == []

    def test_forget(self, tmp_path):
        from hopsworks_agents.protocol import forget, remember

        app, memory = self._app(tmp_path)

        @app.chat
        async def chat(request, ctx):
            if request.text == "store":
                return remember("lang", "python")
            return forget("lang")

        client = TestClient(app)
        for text in ("store", "drop"):
            body = make_request(text, "c1")
            body["subject"] = "alice"
            client.post("/v1/chat", json=body)
        assert memory.list_state("user", "alice") == []

    def test_tools_outside_a_request_are_a_noop(self):
        from hopsworks_agents.protocol import remember

        # no active turn: degrade to a sentence the model can act on rather
        # than raising inside a tool call
        assert "nothing was stored" in remember("k", "v")

    def test_memory_tools_factory(self, tmp_path):
        app, _ = self._app(tmp_path)
        tools = app.memory_tools("plain")
        assert [t.__name__ for t in tools] == [
            "remember",
            "recall",
            "forget",
            "search",
        ]
        with pytest.raises(ValueError, match="Unknown framework"):
            app.memory_tools("nope")

    def test_memory_tools_openai_agents_wrapper(self, tmp_path, monkeypatch):
        import sys
        import types

        fake_agents = types.ModuleType("agents")
        fake_agents.function_tool = lambda fn: {"name": fn.__name__, "fn": fn}
        monkeypatch.setitem(sys.modules, "agents", fake_agents)

        app, _ = self._app(tmp_path)
        tools = app.memory_tools("openai-agents", include=("recall", "search"))
        assert [tool["name"] for tool in tools] == ["recall", "search"]

    def test_memory_tools_claude_agents_wrapper(self, tmp_path, monkeypatch):
        import sys
        import types

        fake_sdk = types.ModuleType("claude_agent_sdk")

        def sdk_tool(name, description, schema):
            def decorator(fn):
                return {
                    "name": name,
                    "description": description,
                    "schema": schema,
                    "fn": fn,
                }

            return decorator

        fake_sdk.tool = sdk_tool
        monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)

        app, _ = self._app(tmp_path)
        tools = app.memory_tools("claude-agent-sdk", include=("remember",))
        assert tools[0]["name"] == "remember"
        assert tools[0]["schema"] == {"key": str, "value": str, "scope": str}
        result = asyncio.run(tools[0]["fn"]({"key": "genre", "value": "jazz"}))
        assert result == {
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Memory is unavailable in this call, so nothing was "
                        "stored. Continue without it and do not retry."
                    ),
                }
            ]
        }


class TestSubjectIdentity:
    def _app(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_t",
            long_term=True,
        )
        return AgentApp(memory=memory), memory

    def test_subject_falls_back_to_conversation(self, tmp_path):
        app, memory = self._app(tmp_path)
        seen = {}

        @app.chat
        async def chat(request, ctx):
            seen["subject"] = ctx.subject
            seen["has_subject"] = ctx.has_subject
            return "ok"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("x", "c1"))
        # no subject asserted: memories degrade to per-conversation durability
        # rather than pooling into a shared bucket
        assert seen["subject"] == "c1"
        assert seen["has_subject"] is False

    def test_subject_from_request(self, tmp_path):
        app, _ = self._app(tmp_path)
        seen = {}

        @app.chat
        async def chat(request, ctx):
            seen["subject"] = ctx.subject
            seen["has_subject"] = ctx.has_subject
            return "ok"

        client = TestClient(app)
        body = make_request("x", "c1")
        body["subject"] = "alice"
        client.post("/v1/chat", json=body)
        assert seen["subject"] == "alice"
        assert seen["has_subject"] is True


class TestStateAuditEndpoints:
    def _app(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_t",
            long_term=True,
        )
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            return "ok"

        return app, memory

    def test_subject_can_see_and_delete_their_memories(self, tmp_path):
        app, memory = self._app(tmp_path)
        memory.set_state("user", "alice", "lang", "python", source_ref='{"x": 1}')
        memory.set_state("user", "alice", "tz", "CET")
        client = TestClient(app)

        body = client.get("/v1/subjects/alice/state").json()
        assert {row["key"] for row in body["state"]} == {"lang", "tz"}
        # provenance and authorship are visible: a user needs to know which of
        # these their own conversation put there
        assert body["state"][0]["written_by"] == "agent"
        assert any(row["source_ref"] for row in body["state"])

        assert client.delete("/v1/subjects/alice/state?key=lang").json()["removed"] == 1
        assert client.delete("/v1/subjects/alice/state").json()["removed"] == 1
        assert client.get("/v1/subjects/alice/state").json()["state"] == []

    def test_clearing_a_conversation_spares_user_state(self, tmp_path):
        app, memory = self._app(tmp_path)
        memory.set_state("user", "alice", "lang", "python")
        memory.set_state("session", "c1", "draft", "in progress")
        client = TestClient(app)

        client.delete("/v1/conversations/c1")
        # "new session" must not erase durable knowledge about the person
        assert memory.get_state("user", "alice", "lang") == "python"
        assert memory.get_state("session", "c1", "draft") is None


def _toy_embedder(text: str):
    """Deterministic bag-of-chars vector: enough for exact cosine ranking in.

    tests without pulling in a real embedding model.
    """
    vec = [0.0] * 26
    for ch in text.lower():
        if "a" <= ch <= "z":
            vec[ord(ch) - 97] += 1.0
    return vec


class TestVectorStoreContract:
    """InMemoryVectorStore is the reference implementation; these pin the.

    semantics the Hopsworks backend has to match.
    """

    def _store(self):
        from hopsworks_agents.protocol import InMemoryVectorStore

        store = InMemoryVectorStore()
        store.ingest(
            [
                {
                    "item_id": 1,
                    "conversation_id": "c1",
                    "subject": "alice",
                    "memory_type": "message",
                    "role": "user",
                    "content": "kayaking",
                    "created_at": "2026-01-01",
                    "embedding": _toy_embedder("kayaking"),
                },
                {
                    "item_id": 2,
                    "conversation_id": "c2",
                    "subject": "alice",
                    "memory_type": "message",
                    "role": "user",
                    "content": "gardening",
                    "created_at": "2026-01-02",
                    "embedding": _toy_embedder("gardening"),
                },
                {
                    "item_id": 3,
                    "conversation_id": "c3",
                    "subject": "bob",
                    "memory_type": "message",
                    "role": "user",
                    "content": "kayaking",
                    "created_at": "2026-01-03",
                    "embedding": _toy_embedder("kayaking"),
                },
            ]
        )
        return store

    def test_ranks_by_similarity(self):
        store = self._store()
        hits = store.search(_toy_embedder("kayaking"), k=2, subject="alice")
        assert hits[0]["content"] == "kayaking"
        assert hits[0]["score"] > hits[1]["score"]

    def test_subject_filter_is_isolation_not_optimization(self):
        store = self._store()
        hits = store.search(_toy_embedder("kayaking"), k=5, subject="alice")
        # bob's identical memory must not surface for alice
        assert {h["subject"] for h in hits} == {"alice"}
        assert 3 not in {h["item_id"] for h in hits}

    def test_vectors_are_not_returned(self):
        store = self._store()
        hits = store.search(_toy_embedder("kayaking"), k=1, subject="alice")
        assert "embedding" not in hits[0]

    def test_purge_by_conversation_and_subject(self):
        store = self._store()
        assert store.purge(conversation_id="c1") == 1
        assert store.purge(subject="alice") == 1
        assert len(store.search(_toy_embedder("kayaking"), k=5)) == 1

    def test_ingest_is_idempotent_on_item_id(self):
        store = self._store()
        store.ingest(
            [
                {
                    "item_id": 1,
                    "conversation_id": "c1",
                    "subject": "alice",
                    "memory_type": "message",
                    "role": "user",
                    "content": "kayaking",
                    "created_at": "2026-01-01",
                    "embedding": _toy_embedder("kayaking"),
                },
            ]
        )
        assert len(store.search(_toy_embedder("kayaking"), k=10, subject="alice")) == 2


class TestSemanticSearch:
    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import InMemoryVectorStore, ManagedMemoryService

        kw.setdefault("long_term", True)
        kw.setdefault("embedder", _toy_embedder)
        kw.setdefault("vector_store", InMemoryVectorStore())
        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", table_name="agent_memory_messages_t", **kw
        )

    def _turn(self, memory, cid, q, a, subject=None):
        from hopsworks_agents.protocol.memory import new_turn_id

        tid = new_turn_id()
        memory.begin_turn(cid, tid, "user", q, subject=subject)
        memory.record_item(cid, tid, "assistant", a, subject=subject)
        memory.end_turn(cid, tid)
        asyncio.run(memory.ingest_turn(cid, tid))
        return tid

    def test_ingest_then_search(self, tmp_path):
        memory = self._store(tmp_path)
        self._turn(memory, "c1", "I love kayaking", "noted", subject="alice")
        self._turn(memory, "c2", "I hate gardening", "noted", subject="alice")

        hits = memory.search("kayaking", subject="alice", k=1)
        assert hits[0]["content"] == "I love kayaking"
        assert hits[0]["score"] is not None

    def test_search_is_isolated_by_subject(self, tmp_path):
        memory = self._store(tmp_path)
        self._turn(memory, "c1", "I love kayaking", "noted", subject="alice")
        assert memory.search("kayaking", subject="bob") == []

    def test_only_messages_are_ingested(self, tmp_path):
        from hopsworks_agents.protocol.memory import new_turn_id

        memory = self._store(tmp_path)
        tid = new_turn_id()
        memory.begin_turn("c1", tid, "user", "kayaking", subject="alice")
        memory.record_item(
            "c1",
            tid,
            "tool",
            '{"name": "kayaking"}',
            memory_type="event",
            subject="alice",
        )
        memory.end_turn("c1", tid)
        # tool/event rows cost an embedding each and nobody searches for them
        assert asyncio.run(memory.ingest_turn("c1", tid)) == 1

    def test_abandoned_turns_are_not_ingested(self, tmp_path):
        from hopsworks_agents.protocol.memory import new_turn_id

        memory = self._store(tmp_path)
        tid = new_turn_id()
        memory.begin_turn("c1", tid, "user", "kayaking", subject="alice")
        memory.end_turn("c1", tid, status="abandoned")
        assert asyncio.run(memory.ingest_turn("c1", tid)) == 0
        assert memory.search("kayaking", subject="alice") == []

    def test_keyword_fallback_without_embedder(self, tmp_path):
        memory = self._store(tmp_path, embedder=None, vector_store=None)
        self._turn(memory, "c1", "I love kayaking", "noted", subject="alice")
        self._turn(memory, "c2", "I hate gardening", "noted", subject="alice")

        # search is registerable as a tool from day one; turning on the vector
        # store later must not require a prompt change
        hits = memory.search("kayaking", subject="alice")
        assert [h["content"] for h in hits] == ["I love kayaking"]
        assert hits[0]["score"] is None

    def test_keyword_fallback_is_also_isolated(self, tmp_path):
        memory = self._store(tmp_path, embedder=None, vector_store=None)
        self._turn(memory, "c1", "I love kayaking", "noted", subject="alice")
        assert memory.search("kayaking", subject="bob") == []

    def test_embedder_failure_falls_back_to_keywords(self, tmp_path):
        def broken(text):
            raise RuntimeError("model gone")

        memory = self._store(tmp_path)
        self._turn(memory, "c1", "I love kayaking", "noted", subject="alice")
        memory._embedder = broken
        hits = memory.search("kayaking", subject="alice")
        assert [h["content"] for h in hits] == ["I love kayaking"]

    def test_deleting_a_conversation_purges_vectors(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            return "noted"

        client = TestClient(app)
        body = make_request("I love kayaking", "c1")
        body["subject"] = "alice"
        client.post("/v1/chat", json=body)
        assert memory.search("kayaking", subject="alice")

        client.delete("/v1/conversations/c1")
        # the vector store holds a copy; deleting only from SQL would leave
        # "deleted" content searchable
        assert memory.search("kayaking", subject="alice") == []

    def test_forgetting_a_subject_purges_their_vectors(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            return "noted"

        client = TestClient(app)
        for cid in ("c1", "c2"):
            body = make_request("I love kayaking", cid)
            body["subject"] = "alice"
            client.post("/v1/chat", json=body)

        body = client.delete("/v1/subjects/alice/state").json()
        assert body["vectors_removed"] == 4
        assert memory.search("kayaking", subject="alice") == []

    def test_ingest_happens_through_the_turn(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            return "noted"

        client = TestClient(app)
        body = make_request("I love kayaking", "c1")
        body["subject"] = "alice"
        client.post("/v1/chat", json=body)
        # embedding runs in the awaited-after-response slot, not a background
        # task a scale-to-zero pod could kill
        assert memory.search("kayaking", subject="alice")

    def test_search_tool_reports_when_nothing_matches(self, tmp_path):
        from hopsworks_agents.protocol import search

        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)
        seen = {}

        @app.chat
        async def chat(request, ctx):
            seen["result"] = search("kayaking")
            return "ok"

        client = TestClient(app)
        client.post("/v1/chat", json=make_request("hello", "c1"))
        assert "Nothing found" in seen["result"]

    def test_search_tool_dates_its_hits(self, tmp_path):
        from hopsworks_agents.protocol import search

        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)
        seen = {}

        @app.chat
        async def chat(request, ctx):
            if request.text == "look":
                seen["result"] = search("kayaking")
                return "ok"
            return "noted"

        client = TestClient(app)
        for text in ("I love kayaking", "look"):
            body = make_request(text, "c1")
            body["subject"] = "alice"
            client.post("/v1/chat", json=body)
        # the model needs the age of a memory to weigh it against the present
        assert "I love kayaking" in seen["result"]
        assert seen["result"].startswith("[20")

    def test_manifest_advertises_search(self, tmp_path):
        memory = self._store(tmp_path)
        app = AgentApp(memory=memory)

        @app.chat
        async def chat(request):
            return "ok"

        client = TestClient(app)
        caps = client.get("/.well-known/hopsworks-agent.json").json()["capabilities"]
        assert caps["memory"] == {
            "conversation_history": True,
            "summary": False,
            "state": True,
            "search": True,
        }


class TestReviewRegressions:
    """Bugs found reviewing the implementation against the design doc.

    Each of these failed silently: a stale summary served as if it were live, a
    completed turn left invisible, a replica serving a truncated history, a
    deletion reported as done. None raised, so only a test pins them.
    """

    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_r",
            **kw,
        )

    def test_clear_drops_the_summary_not_just_the_messages(self, tmp_path):
        memory = self._store(tmp_path, summarize=lambda *a, **k: "a summary")
        memory.append("c1", "user", "something private")
        with memory._engine.begin() as conn:
            s = memory._sessions
            conn.execute(
                s.update()
                .where(s.c.conversation_id == "c1")
                .values(summary="a distillation of something private")
            )
        assert memory.get_summary("c1")

        memory.clear("c1")

        # the summary is derived from the deleted messages, so it goes with them
        assert memory.get_summary("c1") is None
        assert memory.get("c1") == []

    def test_reused_conversation_id_does_not_inherit_the_fold_cursor(self, tmp_path):
        memory = self._store(tmp_path, summarize=lambda *a, **k: "s")
        for i in range(6):
            memory.append("c1", "user", f"m{i}")
        memory.clear("c1")
        with memory._engine.connect() as conn:
            s = memory._sessions
            row = conn.execute(s.select().where(s.c.conversation_id == "c1")).fetchone()
        # no row at all: a stale message_count would trigger an immediate fold
        # and blend the deleted conversation into the new one's summary
        assert row is None

    def test_completed_turn_beats_the_reaper(self, tmp_path):
        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "a slow question")
        memory.record_item("c1", "t1", "assistant", "the answer")
        # the reaper flips a long-running turn to abandoned while it is still
        # working — deep tool loops legitimately outlive turn_timeout_seconds
        memory.end_turn("c1", "t1", status="abandoned")
        # ...and then it finishes
        memory.end_turn("c1", "t1")

        assert memory.get("c1") == [
            {"role": "user", "content": "a slow question"},
            {"role": "assistant", "content": "the answer"},
        ]

    def test_double_close_does_not_double_count(self, tmp_path):
        memory = self._store(tmp_path, summarize=lambda *a, **k: "s")
        memory.begin_turn("c1", "t1", "user", "q")
        memory.record_item("c1", "t1", "assistant", "a")
        memory.end_turn("c1", "t1")
        memory.end_turn("c1", "t1")
        with memory._engine.connect() as conn:
            s = memory._sessions
            row = conn.execute(s.select().where(s.c.conversation_id == "c1")).fetchone()
        assert row.message_count == 2

    def test_replica_sees_turns_written_by_another_replica(self, tmp_path):
        url = f"sqlite:///{tmp_path}/m.db"
        kw = {"table_name": "agent_memory_messages_r", "summarize": lambda *a, **k: "s"}
        from hopsworks_agents.protocol import ManagedMemoryService

        one = ManagedMemoryService(url=url, **kw)
        two = ManagedMemoryService(url=url, **kw)

        one.append("c1", "user", "first")
        assert one.get("c1") == [{"role": "user", "content": "first"}]  # caches

        two.append("c1", "user", "second")  # the other replica takes the turn

        # no fold happened, so the summary version is unchanged: only the
        # message count tells replica one its cached copy is short
        assert one.get("c1") == [
            {"role": "user", "content": "first"},
            {"role": "user", "content": "second"},
        ]

    def test_failed_purge_raises_instead_of_reporting_zero(self):
        from hopsworks_agents.protocol.vectorstore import (
            HopsworksVectorStore,
            VectorPurgeError,
        )

        class Unreadable:
            def read(self, online=False):
                raise RuntimeError("Connection to HopsFS failed")

            def delete_from_online_store(self, keys):
                raise AssertionError("should not be reached")

        store = HopsworksVectorStore("d1", 3)
        store._fg = Unreadable()

        # "0 rows removed" would be indistinguishable from "nothing matched",
        # and this is the path that tells a user their data is gone
        with pytest.raises(VectorPurgeError):
            store.purge(conversation_id="c1")

    def test_purge_reads_the_online_store(self):
        from hopsworks_agents.protocol.vectorstore import HopsworksVectorStore

        seen = {}

        class Recording:
            def read(self, online=False):
                seen["online"] = online
                # duck-typed stand-in for a DataFrame: purge only ever calls
                # iterrows(), and pandas is not a test dependency
                rows = [{"item_id": "i1", "conversation_id": "c1"}]
                return type("Frame", (), {"iterrows": lambda self: enumerate(rows)})()

            def delete_from_online_store(self, keys):
                seen.setdefault("deleted", []).append(keys)

        store = HopsworksVectorStore("d1", 3)
        store._fg = Recording()
        assert store.purge(conversation_id="c1") == 1
        # ingest writes online-only, so the offline table this defaults to is
        # always empty — and unreachable from a serving pod
        assert seen["online"] is True


class TestRetrievalAndReconciliation:
    """Ranking and key-sprawl features taken from the SDK comparison."""

    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        return ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_rr",
            long_term=True,
            **kw,
        )

    def test_recency_outranks_a_slightly_better_old_match(self, tmp_path):
        from datetime import timedelta

        from hopsworks_agents.protocol.memory import _utcnow

        memory = self._store(tmp_path, recency_half_life_days=30.0)
        now = _utcnow()
        old = {
            "item_id": 1,
            "content": "the better match, from a year ago",
            "score": 0.90,
            "created_at": (now - timedelta(days=365)).isoformat(),
        }
        recent = {
            "item_id": 2,
            "content": "the slightly worse match, from today",
            "score": 0.80,
            "created_at": now.isoformat(),
        }
        ranked = memory._rerank([old, recent], k=2)
        assert [r["item_id"] for r in ranked] == [2, 1]

    def test_half_life_means_half(self, tmp_path):
        from datetime import timedelta

        from hopsworks_agents.protocol.memory import _utcnow

        memory = self._store(tmp_path, recency_half_life_days=10.0)
        now = _utcnow()
        # identical similarity; one is exactly one half-life old. Ranking is by
        # score * 0.5 ** (age / half_life), so the older one must score half.
        fresh = {"item_id": 1, "score": 1.0, "created_at": now.isoformat()}
        aged = {
            "item_id": 2,
            "score": 1.0,
            "created_at": (now - timedelta(days=10)).isoformat(),
        }
        ranked = memory._rerank([aged, fresh], k=2)
        assert [r["item_id"] for r in ranked] == [1, 2]
        # and a twice-as-similar old memory still wins, which is the point of
        # weighting rather than sorting by date
        aged_strong = dict(aged, score=2.1)
        assert memory._rerank([aged_strong, fresh], k=1)[0]["item_id"] == 2

    def test_disabling_recency_restores_similarity_order(self, tmp_path):
        from datetime import timedelta

        from hopsworks_agents.protocol.memory import _utcnow

        memory = self._store(tmp_path, recency_half_life_days=None)
        now = _utcnow()
        old = {
            "item_id": 1,
            "score": 0.9,
            "created_at": (now - timedelta(days=900)).isoformat(),
        }
        recent = {"item_id": 2, "score": 0.8, "created_at": now.isoformat()}
        assert [r["item_id"] for r in memory._rerank([old, recent], k=2)] == [1, 2]

    def test_unparseable_timestamp_ranks_on_score_alone(self, tmp_path):
        memory = self._store(tmp_path, recency_half_life_days=30.0)
        rows = [
            {"item_id": 1, "score": 0.9, "created_at": "not a date"},
            {"item_id": 2, "score": 0.5, "created_at": None},
        ]
        # neither is dropped or sorted to the bottom for lacking a usable date
        assert [r["item_id"] for r in memory._rerank(rows, k=2)] == [1, 2]

    def test_search_over_fetches_so_reranking_can_change_the_answer(self, tmp_path):
        from hopsworks_agents.protocol.vectorstore import InMemoryVectorStore

        seen = {}

        class Recording(InMemoryVectorStore):
            def search(self, vector, *, k=5, subject=None, conversation_id=None):
                seen["k"] = k
                return []

        memory = self._store(
            tmp_path,
            vector_store=Recording(),
            embedder=lambda text: [1.0, 0.0],
            recency_half_life_days=30.0,
            search_oversample=3,
        )
        memory.search("anything", subject="s1", k=5)
        # re-ranking the 5 the index already picked would only reorder them
        assert seen["k"] == 15

    def test_remember_names_existing_near_duplicate_keys(self, tmp_path):
        from hopsworks_agents.protocol import tools

        memory = self._store(tmp_path)
        memory.set_state("user", "alice", "customer_first_name", "Aaron")

        class Ctx:
            conversation_id = "c1"
            turn_id = "t1"
            message_id = "m1"
            subject = "alice"

        ctx = Ctx()
        ctx.memory = memory
        original = tools._resolve
        tools._resolve = lambda: (memory, ctx)
        try:
            reply = tools.remember("Customer Name", "Aaron Mitchell")
        finally:
            tools._resolve = original
        # normalized on the way in...
        assert "'customer_name'" in reply
        assert memory.get_state("user", "alice", "customer_name") == "Aaron Mitchell"
        # ...and the model is told what it now sits beside, which is how the
        # customer_name / customer_first_name split got noticed in the first place
        assert "customer_first_name" in reply

    def test_recall_finds_what_remember_normalized(self, tmp_path):
        from hopsworks_agents.protocol import tools

        memory = self._store(tmp_path)

        class Ctx:
            conversation_id = "c1"
            turn_id = "t1"
            message_id = "m1"
            subject = "alice"

        ctx = Ctx()
        ctx.memory = memory
        original = tools._resolve
        tools._resolve = lambda: (memory, ctx)
        try:
            tools.remember("Preferred Language", "Finnish")
            assert tools.recall("preferred language") == "Finnish"
            assert tools.recall("PREFERRED_LANGUAGE") == "Finnish"
            assert "Forgot" in tools.forget("Preferred Language")
        finally:
            tools._resolve = original


class TestDeploymentIsolation:
    """Every agent in a project now shares one set of tables.

    Before consolidation, isolation was physical — a query could not reach
    another deployment's rows because they were in another table. Now it is a
    WHERE clause on every statement, so a single omission silently returns,
    edits or deletes someone else's memory. These tests are what stands in for
    the table boundary that used to do it.
    """

    def _pair(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        url = f"sqlite:///{tmp_path}/shared.db"
        common = dict(url=url, long_term=True, **kw)
        return (
            ManagedMemoryService(deployment_id="agent_a", **common),
            ManagedMemoryService(deployment_id="agent_b", **common),
        )

    def test_same_conversation_id_in_two_deployments_stays_apart(self, tmp_path):
        a, b = self._pair(tmp_path)
        # conversation ids are chosen by clients; two agents can collide
        a.append("shared-id", "user", "agent A's message")
        b.append("shared-id", "user", "agent B's message")

        assert a.get("shared-id") == [{"role": "user", "content": "agent A's message"}]
        assert b.get("shared-id") == [{"role": "user", "content": "agent B's message"}]

    def test_state_is_per_deployment_for_the_same_subject(self, tmp_path):
        a, b = self._pair(tmp_path)
        a.set_state("user", "alice", "tone", "formal")
        b.set_state("user", "alice", "tone", "playful")

        # the same person talking to two agents keeps two memories, and the
        # unique constraint must not collapse them onto one row
        assert a.get_state("user", "alice", "tone") == "formal"
        assert b.get_state("user", "alice", "tone") == "playful"
        assert len(a.list_state("user", "alice")) == 1

    def test_app_scope_does_not_leak_between_deployments(self, tmp_path):
        a, b = self._pair(tmp_path)
        # `app` scope is owner='' — without deployment_id in the key, every
        # agent's app state would collapse onto a single row
        a.set_state("app", "", "policy", "A refunds up to 50")
        b.set_state("app", "", "policy", "B refunds nothing")
        assert a.get_state("app", "", "policy") == "A refunds up to 50"
        assert b.get_state("app", "", "policy") == "B refunds nothing"

    def test_clear_does_not_delete_another_deployments_conversation(self, tmp_path):
        a, b = self._pair(tmp_path)
        a.append("shared-id", "user", "keep me")
        b.append("shared-id", "user", "delete me")

        b.clear("shared-id")

        assert a.get("shared-id") == [{"role": "user", "content": "keep me"}]
        assert b.get("shared-id") == []

    def test_delete_state_does_not_reach_another_deployment(self, tmp_path):
        a, b = self._pair(tmp_path)
        a.set_state("user", "alice", "tone", "formal")
        b.set_state("user", "alice", "tone", "playful")

        assert b.delete_state("user", "alice") == 1
        assert a.get_state("user", "alice", "tone") == "formal"

    def test_reaper_does_not_abandon_another_deployments_open_turn(self, tmp_path):
        a, b = self._pair(tmp_path)
        a.begin_turn("c1", "t1", "user", "A is still working")
        b.begin_turn("c2", "t2", "user", "B is still working")

        # B sweeps everything older than zero seconds — but only its own
        assert b.reap_open_turns(older_than_seconds=0) == 1

        a.record_item("c1", "t1", "assistant", "A finished")
        a.end_turn("c1", "t1")
        assert a.get("c1") == [
            {"role": "user", "content": "A is still working"},
            {"role": "assistant", "content": "A finished"},
        ]

    def test_keyword_search_does_not_cross_deployments(self, tmp_path):
        a, b = self._pair(tmp_path)
        a.append("c1", "user", "the password is hunter2")
        b.append("c2", "user", "the password is swordfish")

        hits = b.search("password", subject=None, k=10)
        assert [h["content"] for h in hits] == ["the password is swordfish"]

    def test_summary_is_per_deployment(self, tmp_path):
        a, b = self._pair(tmp_path, summarize=lambda prev, turns: "a summary")
        a.append("shared-id", "user", "A's history")
        b.append("shared-id", "user", "B's history")
        # sessions rows are keyed (deployment_id, conversation_id); a single-
        # column PK would have made the second insert collide with the first
        b.clear("shared-id")
        assert a.get("shared-id") == [{"role": "user", "content": "A's history"}]


class TestSharedTableNames:
    def test_tables_are_the_feature_groups_own(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            deployment_id="d1",
            long_term=True,
            summarize=lambda prev, turns: "s",
        )
        assert memory.healthcheck() is True
        # Hopsworks names an online feature group's table <name>_<version>, and
        # the agent writes into that table, so there is one copy of the data
        assert memory._table.name == "agent_memory_messages_1"
        assert memory._sessions.name == "agent_memory_conversations_1"
        assert memory._state.name == "agent_memory_facts_1"
        assert memory._meta.name == "agent_memory_schema_1"

    def test_feature_group_name_drops_the_version_suffix(self):
        from hopsworks_agents.protocol.memory import _feature_group_name

        assert _feature_group_name("agent_memory_messages_1") == "agent_memory_messages"
        assert _feature_group_name("agent_memory_facts_2") == "agent_memory_facts"
        # a table with no version suffix is not a feature group's
        assert _feature_group_name("something_else") == "something_else"

    def test_explicit_table_name_suffixes_every_companion(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            table_name="agent_memory_messages_42",
            deployment_id="d1",
            long_term=True,
            summarize=lambda prev, turns: "s",
        )
        assert memory.healthcheck() is True
        assert memory._meta.name == "agent_memory_schema_42"
        assert memory._sessions.name == "agent_memory_conversations_42"
        assert memory._state.name == "agent_memory_facts_42"


class TestFeatureGroupExport:
    """Filling the offline half of the feature groups.

    The agent writes into the feature groups' own online tables, so an export
    is not a copy between stores — it is materialising what is already online
    into Delta, where it can be queried over time.
    """

    @pytest.fixture(autouse=True)
    def fake_hsfs(self, monkeypatch):
        import sys
        import types

        class Feature:
            def __init__(self, name, type=None, online_type=None):  # noqa: A002
                self.name = name
                self.type = type
                self.online_type = online_type

        mod = types.ModuleType("hsfs")
        feature_mod = types.ModuleType("hsfs.feature")
        feature_mod.Feature = Feature
        mod.feature = feature_mod
        monkeypatch.setitem(sys.modules, "hsfs", mod)
        monkeypatch.setitem(sys.modules, "hsfs.feature", feature_mod)
        return Feature

    class FakeFG:
        def __init__(self, name):
            self.name = name
            self.inserts = []

        def insert(self, frame, **kwargs):
            self.inserts.append((frame, kwargs))

    class FakeStore:
        def __init__(self, missing=()):
            self.missing = set(missing)
            self.groups = {}

        def get_feature_group(self, name, version):
            if name in self.missing:
                return None
            return self.groups.setdefault(
                (name, version), TestFeatureGroupExport.FakeFG(name)
            )

    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db",
            deployment_id="d1",
            long_term=True,
            summarize=lambda prev, turns: "s",
            **kw,
        )
        assert memory.healthcheck() is True
        return memory

    def test_exports_to_offline_by_default(self, tmp_path):
        memory = self._store(tmp_path)
        memory.append("c1", "user", "hello")
        fs = self.FakeStore()

        counts = memory.export_feature_groups(fs, include=("items",))

        assert counts["agent_memory_messages_1"] == 1
        _, kwargs = fs.groups[("agent_memory_messages", 1)].inserts[0]
        # online is already live — the agent wrote it. Offline is what is missing.
        assert kwargs["storage"] == "offline"
        assert kwargs["write_options"] == {"mode": "append"}

    def test_looks_up_the_feature_group_by_its_own_name(self, tmp_path):
        memory = self._store(tmp_path)
        memory.append("c1", "user", "hello")
        fs = self.FakeStore()
        memory.export_feature_groups(fs, include=("items",))
        # the table is agent_memory_messages_1; the feature group is version 1
        # of agent_memory_messages
        assert ("agent_memory_messages", 1) in fs.groups

    def test_missing_feature_group_is_refused(self, tmp_path):
        memory = self._store(tmp_path)
        memory.append("c1", "user", "hello")
        fs = self.FakeStore(missing={"agent_memory_messages"})
        with pytest.raises(RuntimeError, match="provisioned by Hopsworks"):
            memory.export_feature_groups(fs, include=("items",))

    def test_both_omits_the_storage_argument(self, tmp_path):
        memory = self._store(tmp_path)
        memory.append("c1", "user", "hello")
        fs = self.FakeStore()
        memory.export_feature_groups(fs, storage="both", include=("items",))
        _, kwargs = fs.groups[("agent_memory_messages", 1)].inserts[0]
        assert "storage" not in kwargs

    def test_unknown_storage_is_refused(self, tmp_path):
        memory = self._store(tmp_path)
        with pytest.raises(ValueError, match="Unknown storage"):
            memory.export_feature_groups(self.FakeStore(), storage="nearline")

    def test_since_exports_only_newer_rows(self, tmp_path):
        memory = self._store(tmp_path)
        memory.append("c1", "user", "first")
        memory.append("c1", "user", "second")
        fs = self.FakeStore()
        assert (
            memory.export_feature_groups(fs, include=("items",))[
                "agent_memory_messages_1"
            ]
            == 2
        )
        fs2 = self.FakeStore()
        assert (
            memory.export_feature_groups(fs2, include=("items",), since=1)[
                "agent_memory_messages_1"
            ]
            == 1
        )

    def test_batches_large_exports(self, tmp_path):
        memory = self._store(tmp_path)
        for i in range(5):
            memory.append("c1", "user", f"m{i}")
        fs = self.FakeStore()
        memory.export_feature_groups(fs, include=("items",), batch_size=2)
        inserts = fs.groups[("agent_memory_messages", 1)].inserts
        assert [len(frame) for frame, _ in inserts] == [2, 2, 1]

    def test_nothing_to_export_touches_no_feature_group(self, tmp_path):
        memory = self._store(tmp_path)
        fs = self.FakeStore()
        counts = memory.export_feature_groups(fs, include=("items",))
        assert counts["agent_memory_messages_1"] == 0
        assert fs.groups == {}

    def test_all_null_column_is_typed_before_insert(self, tmp_path):
        import pandas as pd

        memory = self._store(tmp_path)
        frame = pd.DataFrame([{"a": None, "b": "x", "t": pd.NaT}])
        prepared = memory._prepare_for_insert(frame)
        # an all-null string column would reach Arrow untyped and be dropped by
        # the online writer, silently
        assert prepared["a"].tolist() == [""]
        # an all-NaT datetime keeps its dtype: that is the type information
        assert pd.api.types.is_datetime64_any_dtype(prepared["t"])


class TestConversationListing:
    """The server knows which conversations exist; a client need not remember."""

    def _store(self, tmp_path, **kw):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", deployment_id="d1", **kw
        )
        assert memory.healthcheck() is True
        return memory

    def test_lists_most_recently_active_first(self, tmp_path):
        memory = self._store(tmp_path)
        memory.append("older", "user", "first")
        memory.append("newer", "user", "second")
        listed = memory.list_conversations()
        assert [c["conversation_id"] for c in listed] == ["newer", "older"]
        assert listed[0]["message_count"] == 1

    def test_scoped_to_the_deployment(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        url = f"sqlite:///{tmp_path}/m.db"
        a = ManagedMemoryService(url=url, deployment_id="a")
        b = ManagedMemoryService(url=url, deployment_id="b")
        a.append("mine", "user", "hello")
        b.append("theirs", "user", "hello")
        assert [c["conversation_id"] for c in a.list_conversations()] == ["mine"]

    def test_filters_by_subject(self, tmp_path):
        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "hi", subject="alice")
        memory.end_turn("c1", "t1")
        memory.begin_turn("c2", "t2", "user", "hi", subject="bob")
        memory.end_turn("c2", "t2")
        assert [
            c["conversation_id"] for c in memory.list_conversations(subject="bob")
        ] == ["c2"]

    def test_open_turns_are_not_listed_as_conversations(self, tmp_path):
        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "still going")
        # an unanswered question is not yet a conversation to show
        assert memory.list_conversations() == []

    def test_endpoint_returns_the_list(self, tmp_path):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", deployment_id="d1"
        )
        memory.append("c1", "user", "hello")
        client = TestClient(self.build_app(memory))
        body = client.get("/v1/conversations").json()
        assert [c["conversation_id"] for c in body["conversations"]] == ["c1"]

    def build_app(self, memory):
        from hopsworks_agents.protocol import AgentApp

        app = AgentApp(name="t", memory=memory)

        @app.chat
        async def handler(request, ctx):  # pragma: no cover - not exercised
            return "ok"

        return app


class TestConversationSubject:
    """Who a conversation is filed under is a server fact, not a client one."""

    def _store(self, tmp_path, deployment_id="d1"):
        from hopsworks_agents.protocol import ManagedMemoryService

        memory = ManagedMemoryService(
            url=f"sqlite:///{tmp_path}/m.db", deployment_id=deployment_id
        )
        assert memory.healthcheck() is True
        return memory

    def test_reports_the_subject_the_turn_was_stamped_with(self, tmp_path):
        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "hi", subject="alice")
        memory.end_turn("c1", "t1")
        assert memory.conversation_subject("c1") == "alice"

    def test_reports_the_rebound_subject_not_the_asserted_one(self, tmp_path):
        # The case the client cannot answer for itself: it asserted one subject
        # and the agent worked out another while the turn ran.
        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "I'm Patrick", subject="meb10000")
        memory.rebind_turn_subject("c1", "t1", "customer:14")
        memory.end_turn("c1", "t1")
        assert memory.conversation_subject("c1") == "customer:14"

    def test_later_turns_win(self, tmp_path):
        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "hi", subject="alice")
        memory.end_turn("c1", "t1")
        memory.begin_turn("c1", "t2", "user", "actually", subject="bob")
        memory.end_turn("c1", "t2")
        assert memory.conversation_subject("c1") == "bob"

    def test_scoped_to_the_deployment(self, tmp_path):
        # Another deployment's conversation must not leak an identity, even to
        # a caller who guessed the conversation id.
        url_dir = tmp_path
        a = self._store(url_dir, deployment_id="a")
        b = self._store(url_dir, deployment_id="b")
        a.begin_turn("shared-id", "t1", "user", "hi", subject="alice")
        a.end_turn("shared-id", "t1")
        assert b.conversation_subject("shared-id") is None

    def test_none_when_no_subject_was_ever_stamped(self, tmp_path):
        memory = self._store(tmp_path)
        memory.append("c1", "user", "hello")
        assert memory.conversation_subject("c1") is None

    def test_none_for_an_unknown_conversation(self, tmp_path):
        memory = self._store(tmp_path)
        assert memory.conversation_subject("nope") is None

    def test_endpoint_exposes_it_on_the_transcript(self, tmp_path):
        from hopsworks_agents.protocol import AgentApp

        memory = self._store(tmp_path)
        memory.begin_turn("c1", "t1", "user", "hi", subject="meb10000")
        memory.rebind_turn_subject("c1", "t1", "customer:14")
        memory.end_turn("c1", "t1")

        app = AgentApp(name="t", memory=memory)

        @app.chat
        async def handler(request, ctx):  # pragma: no cover - not exercised
            return "ok"

        body = TestClient(app).get("/v1/conversations/c1/messages").json()
        assert body["subject"] == "customer:14"

    def test_endpoint_reports_null_rather_than_guessing(self, tmp_path):
        from hopsworks_agents.protocol import AgentApp, InMemoryAgentMemory

        # A store with no per-row subject must say so, so the client falls back
        # to its own value instead of being handed a fabricated identity.
        app = AgentApp(name="t", memory=InMemoryAgentMemory())

        @app.chat
        async def handler(request, ctx):  # pragma: no cover - not exercised
            return "ok"

        body = TestClient(app).get("/v1/conversations/c1/messages").json()
        assert body["subject"] is None


class TestTurnSpan:
    """The turn span is what makes a trace findable by the caller that asked.

    for it. Without it the agent starts a fresh trace per request and the eval
    runner's trial rows point at trace ids that were never created.
    """

    def _traced_app(self, **kwargs):
        pytest.importorskip("opentelemetry.sdk")
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

        app = AgentApp(name="Traced agent", tracing=False, **kwargs)
        app.tracer_provider = provider

        @app.chat
        async def chat(request):
            return AgentResponse.text(
                text=f"echo: {request.text}",
                usage={"input_tokens": 11, "output_tokens": 7},
            )

        return app, exporter

    @staticmethod
    def _attrs(exporter):
        spans = exporter.get_finished_spans()
        assert spans, "no span was exported"
        return dict(spans[-1].attributes)

    def test_continues_the_callers_trace(self):
        app, exporter = self._traced_app()
        trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
        client = TestClient(app)
        body = client.post(
            "/v1/chat",
            json=make_request("hi"),
            headers={"traceparent": f"00-{trace_id}-00f067aa0ba902b7-01"},
        ).json()

        span = exporter.get_finished_spans()[-1]
        assert format(span.get_span_context().trace_id, "032x") == trace_id
        # and the caller can find it from the response alone
        assert body["metadata"]["trace_id"] == trace_id

    def test_copies_eval_baggage_onto_the_span(self):
        app, exporter = self._traced_app()
        TestClient(app).post(
            "/v1/chat",
            json=make_request("hi"),
            headers={
                "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                "baggage": "hopsworks.eval.run_id=run_7,hopsworks.eval.trial_index=2",
            },
        )
        attrs = self._attrs(exporter)
        assert attrs["hopsworks.eval.run_id"] == "run_7"
        assert attrs["hopsworks.eval.trial_index"] == "2"

    def test_drops_baggage_outside_the_allowlist(self):
        # baggage is caller-controlled, so an arbitrary key must not become an
        # attribute in the project's trace tables
        app, exporter = self._traced_app()
        TestClient(app).post(
            "/v1/chat",
            json=make_request("hi"),
            headers={"baggage": "attacker=1,hopsworks.eval.run_id=run_7"},
        )
        attrs = self._attrs(exporter)
        assert "attacker" not in attrs
        assert attrs["hopsworks.eval.run_id"] == "run_7"

    def test_carries_authoritative_input_and_output(self):
        app, exporter = self._traced_app()
        TestClient(app).post("/v1/chat", json=make_request("what is attention?"))
        attrs = self._attrs(exporter)
        assert attrs["input.value"] == "what is attention?"
        assert attrs["output.value"] == "echo: what is attention?"
        assert attrs["openinference.span.kind"] == "AGENT"
        assert attrs["gen_ai.usage.input_tokens"] == 11
        assert attrs["gen_ai.usage.output_tokens"] == 7

    def test_failed_turn_is_an_error_span(self):
        app, exporter = self._traced_app()

        @app.chat
        async def chat(request):
            raise AgentError("It broke", code="boom", status_code=400)

        TestClient(app).post("/v1/chat", json=make_request("hi"))
        span = exporter.get_finished_spans()[-1]
        assert span.status.status_code.name == "ERROR"

    def test_streaming_span_spans_the_whole_generator(self):
        app, exporter = self._traced_app()

        @app.stream
        async def stream(request):
            yield "one "
            yield "two"

        with TestClient(app).stream(
            "POST", "/v1/chat/stream", json=make_request("hi")
        ) as response:
            frames = parse_sse("".join(response.iter_text()))

        assert [e for e, _ in frames] == [
            "message.delta",
            "message.delta",
            "message.completed",
        ]
        attrs = self._attrs(exporter)
        assert attrs["output.value"] == "one two"

    def test_untraced_app_still_serves(self):
        # tracing off is the common case; the span must degrade to nothing
        app = build_basic_app()
        body = TestClient(app).post("/v1/chat", json=make_request("hi")).json()
        assert body["message"]["content"][0]["text"] == "echo: hi"
        assert "trace_id" not in body["metadata"]


class TestEvalCapabilities:
    """The manifest is how a runner learns what a deployment can do before it.

    fires a suite at it, rather than discovering it from broken results.
    """

    def test_trace_correlation_reflects_tracing(self):
        app = AgentApp(name="t", tracing=False)
        manifest = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["trace_correlation"] is False

        app.tracer_provider = object()
        manifest = TestClient(app).get("/.well-known/hopsworks-agent.json").json()
        assert manifest["capabilities"]["trace_correlation"] is True

    def test_eval_mode_comes_from_the_platform(self, monkeypatch):
        from hopsworks_agents.protocol import conventions

        monkeypatch.setenv(conventions.EVAL_MODE_ENV, "true")
        manifest = (
            TestClient(AgentApp(name="t"))
            .get("/.well-known/hopsworks-agent.json")
            .json()
        )
        assert manifest["capabilities"]["eval_mode"] is True

    def test_eval_mode_defaults_off(self, monkeypatch):
        from hopsworks_agents.protocol import conventions

        monkeypatch.delenv(conventions.EVAL_MODE_ENV, raising=False)
        manifest = (
            TestClient(AgentApp(name="t"))
            .get("/.well-known/hopsworks-agent.json")
            .json()
        )
        assert manifest["capabilities"]["eval_mode"] is False


class TestTheEvalModeVariableIsSettable:
    def test_it_avoids_every_prefix_the_platform_reserves(self):
        """The point of the name.

        HOPSWORKS_EVAL_MODE was rejected by the deployment form — the platform
        reserves HOPS_, HOPSWORKS_, HOPSFS_ and AGENT_ — so the flag could not be
        set by the only people who need to set it, and a sandboxed suite could
        never run.
        """
        from hopsworks_agents.protocol import conventions

        reserved = ("HOPS_", "HOPSWORKS_", "HOPSFS_", "AGENT_")
        assert not conventions.EVAL_MODE_ENV.startswith(reserved)

    def test_it_joins_the_evaluation_family(self):
        # EVAL_JUDGE_API_KEY, EVAL_JUDGE_MODEL, EVAL_MODE
        from hopsworks_agents.protocol import conventions

        assert conventions.EVAL_MODE_ENV.startswith("EVAL_")


class FakePlatform:
    """Hopsworks as the deployment's feedback relay sees it, recording what it was sent."""

    def __init__(self, latest=None):
        self.posted: list[tuple[str, dict]] = []
        self.latest = latest or {}

    def post_feedback(self, trace_id, body):
        self.posted.append((trace_id, body))
        return {"feedbackId": "fb-1", "reviewer": "user:" + body.get("reviewer", "")}

    def latest_trace_id(self, conversation_id):
        return self.latest.get(conversation_id)


class UnavailablePlatform:
    def post_feedback(self, trace_id, body):
        raise AgentError(
            "nowhere to record feedback", code="platform_unavailable", status_code=503
        )

    def latest_trace_id(self, conversation_id):
        raise AgentError(
            "nowhere to look", code="platform_unavailable", status_code=503
        )


def build_feedback_app(platform):
    app = AgentApp(name="Rated agent", platform=platform)

    @app.chat
    async def chat(request):
        response = AgentResponse.text(
            text="ok", conversation_id=request.conversation_id
        )
        response.metadata["trace_id"] = "trace-for-" + request.conversation_id
        return response

    return app


class TestFeedback:
    def test_the_manifest_advertises_feedback(self):
        manifest = (
            TestClient(build_basic_app())
            .get("/.well-known/hopsworks-agent.json")
            .json()
        )
        assert manifest["endpoints"]["feedback"] == "/v1/feedback"
        assert manifest["capabilities"]["feedback"] is True

    def test_a_verdict_on_a_trace_is_relayed_under_the_end_user(self):
        platform = FakePlatform()
        client = TestClient(build_feedback_app(platform))
        response = client.post(
            "/v1/feedback",
            json={
                "trace_id": "abc",
                "verdict": "negative",
                "issue_category": "wrong_tool",
                "corrected_answer": "look the customer up by key",
                "subject": "alice",
            },
        )
        assert response.status_code == 200
        assert response.json() == {
            "feedback_id": "fb-1",
            "trace_id": "abc",
            "verdict": "negative",
            "reviewer": "user:alice",
        }
        [(trace_id, body)] = platform.posted
        assert trace_id == "abc"
        assert body == {
            "verdict": "negative",
            "issueCategory": "wrong_tool",
            "correctedAnswer": "look the customer up by key",
            "reviewer": "alice",
        }

    def test_without_a_subject_the_verdict_is_anonymous(self):
        platform = FakePlatform()
        TestClient(build_feedback_app(platform)).post(
            "/v1/feedback",
            json={"trace_id": "abc", "verdict": "positive", "subject": "  "},
        )
        assert platform.posted[0][1]["reviewer"] == "anonymous"

    def test_a_conversation_resolves_to_the_turn_the_app_just_answered(self):
        platform = FakePlatform()
        client = TestClient(build_feedback_app(platform))
        reply = client.post("/v1/chat", json=make_request("hi", "conv-9")).json()
        assert reply["metadata"]["trace_id"] == "trace-for-conv-9"
        client.post(
            "/v1/feedback", json={"conversation_id": "conv-9", "verdict": "positive"}
        )
        assert platform.posted[0][0] == "trace-for-conv-9"

    def test_a_conversation_this_replica_never_saw_is_looked_up(self):
        platform = FakePlatform(latest={"conv-old": "trace-old"})
        client = TestClient(build_feedback_app(platform))
        client.post(
            "/v1/feedback", json={"conversation_id": "conv-old", "verdict": "negative"}
        )
        assert platform.posted[0][0] == "trace-old"
        missing = client.post(
            "/v1/feedback", json={"conversation_id": "conv-none", "verdict": "negative"}
        )
        assert missing.status_code == 404
        assert missing.json()["detail"]["code"] == "trace_not_found"

    def test_the_turn_must_be_named(self):
        response = TestClient(build_feedback_app(FakePlatform())).post(
            "/v1/feedback", json={"verdict": "negative"}
        )
        assert response.status_code == 400
        assert response.json()["detail"]["code"] == "invalid_request"

    def test_an_unknown_verdict_is_refused_before_anything_is_sent(self):
        platform = FakePlatform()
        response = TestClient(build_feedback_app(platform)).post(
            "/v1/feedback", json={"trace_id": "abc", "verdict": "meh"}
        )
        assert response.status_code == 422 and platform.posted == []

    def test_outside_hopsworks_the_route_says_so(self):
        response = TestClient(build_feedback_app(UnavailablePlatform())).post(
            "/v1/feedback", json={"trace_id": "abc", "verdict": "negative"}
        )
        assert response.status_code == 503
        assert response.json()["detail"]["code"] == "platform_unavailable"

    def test_remembered_traces_are_bounded(self):
        from hopsworks_agents.protocol import app as app_module

        app = build_feedback_app(FakePlatform())
        client = TestClient(app)
        for i in range(app_module.LAST_TRACE_LIMIT + 5):
            client.post("/v1/chat", json=make_request("hi", f"c{i}"))
        assert len(app._last_trace) == app_module.LAST_TRACE_LIMIT
        assert (
            "c0" not in app._last_trace
            and f"c{app_module.LAST_TRACE_LIMIT + 4}" in app._last_trace
        )

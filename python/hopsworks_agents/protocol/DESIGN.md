# Hopsworks Agent SDK — Design

Server-side Python library (`hopsworks-agent-protocol`, `github.com/gibchikafa/hopsworks-agent-protocol`) that makes any Python agent chat-ready in the Hopsworks UI. This documents the SDK's *design* — the wire contract it implements is specified separately in `hopsworks-agent-protocol.md`; the chat panel that consumes it in `agent-deployment-chat-design.md`.

## Purpose

Before the SDK, every Hopsworks agent hand-rolled the same ~150 lines: a FastAPI app, request parsing, session handling, a MySQL chat store, OTel tracer setup, and CORS — all subtly different, each re-solving problems the platform already knows the answer to (the DB URL, whether tracing is on, the deployment id). The SDK moves all of that behind one object so agent code is only domain logic:

```python
agent_app = AgentApp(name="My agent", framework="langgraph", memory=ManagedMemoryService())

@agent_app.chat
async def chat(request):
    ...   # the only code the author writes
```

Everything else — HTTP surface, session identity, tracing, memory, CORS, the manifest — is the SDK reading the environment the platform sets up.

## Module layout

```
models.py    protocol Pydantic models + AgentResponse/AgentError helpers
app.py       AgentApp (FastAPI subclass): routes, handler decorators, streaming
memory.py    ChatMemory abstraction + InMemory/Sql backends + MySQL URL resolver
tools.py     model-callable remember/recall/forget/search + framework wrapping
summarizers.py  anthropic_summarizer, openai_summarizer (any OpenAI-compatible endpoint), hopsworks_summarizer (an LLM deployment, by name) + the default local embedder
vectorstore.py  embedding feature group behind a VectorStore seam
tracing.py   env-gated OTel setup + per-framework OpenInference instrumentation
```

~860 lines total. No runtime dependency beyond FastAPI + Pydantic; tracing, SQL, and framework instrumentation are optional extras.

## Key decisions

### 1. `AgentApp` subclasses `FastAPI`

Not a wrapper that owns a hidden app, not a `serve(fn)` function. Consequences:

- Runs anywhere a FastAPI app runs — `uvicorn module:agent_app`, the existing KServe/Knative deployment path — with no special launcher.
- Authors add custom routes, middleware, and dependencies the normal FastAPI way; the SDK's routes coexist.
- The four protocol routes (`/.well-known/hopsworks-agent.json`, `/v1/chat`, `/v1/chat/stream`, `/health`) and CORS are registered in `__init__`; CORS defaults on because the in-browser chat panel calls the agent cross-origin through the ingress and authors invariably forget it.

### 2. Framework and protocol are separate axes

This is the load-bearing distinction (a `langgraph` *wire protocol* was built and then removed once it became clear it conflated the two):

- **Framework** (`langgraph` / `llamaindex` / `openai_agents` / `claude_agents` / `custom`) — what the agent is *built with*. Lives in the pod. Consumers: tracing instrumentation, trace parsing. Resolved from the explicit `framework=` arg, else the platform-injected `AGENT_FRAMEWORK` env var, else `custom`. Advertised in the manifest as `agent.framework`.
- **Protocol** — what the agent *speaks over HTTP*. The SDK always emits the **hopsworks-agent** protocol regardless of framework.

The SDK is precisely the thing that decouples them: any framework in, one wire protocol out. That is why there is no `langgraph`/`llamaindex` *protocol* — the whole point is that the framework is invisible to the client. (`openai` survives as a UI-side protocol because it is a genuine wire contract of non-SDK servers like vLLM, not a framework.)

### 3. Two handler styles, one collapses into the other

`@app.chat` (returns a `ChatResponse`/str) and `@app.stream` (async generator of text deltas, optional final `ChatResponse` for citations/usage/media). Registering `@stream` flips the manifest's `streaming` capability automatically. Each endpoint degrades gracefully to the other handler: `/v1/chat` collects a stream into one response; `/v1/chat/stream` emits a single `message.completed` when only `@chat` exists. Authors implement whichever fits; both endpoints always work.

### 3a. Optional handler context

Handlers may declare a second parameter (`def chat(request, ctx)`); the SDK inspects the signature and injects a `HandlerContext` only when present, so the one-arg form keeps working. The context bundles per-turn conveniences (`conversation_id`, `history`/`summary`/`state()`/`system_context()` from memory, `memory`, `logger`, `deployment_id`, `framework`, `subject`, correlation ids) and `emit_event`. This adds ergonomics without a new wire protocol — the context is a server-side object, not part of the contract.

`ctx.emit_event(name, status, message, data, event_id)` surfaces intermediate progress (retrieval, tool calls, code runs). While streaming it is interleaved as a `tool_event` SSE frame via a queue the streaming route drains alongside the handler's own yields (the handler runs as a task so `await ctx.emit_event(...)` and `yield delta` compose); otherwise it buffers into the response metadata. `event_id` ties a `running` event to its `done`/`failed` so the client renders one updating chip. Gated by `AgentApp(tool_events=True)`.

**Automatic emission (zero agent code).** When `tool_events` is on *and* tracing is active, the SDK installs a span processor (`autoevents.py`) on the tracer provider that watches for OpenInference `TOOL`/`RETRIEVER` spans and emits `running`/`done`/`failed` tool events keyed by span id — so tool calls appear in the panel automatically, the same way they appear in framework tracing, because it's the same instrumentation. The processor finds the active turn through a `contextvars.ContextVar[HandlerContext]` set around handler execution, and `_emit_sync` chooses a direct `put_nowait` (on the loop thread) vs. `loop.call_soon_threadsafe` (off-thread) so a tool running in a worker thread still enqueues safely. Correlation across threads depends on the framework copying context into its executors — **LangChain/LangGraph do** (`langchain_core.runnables.config.run_in_executor` copies the context); frameworks that don't propagate contextvars won't auto-emit, and fall back to manual events or the trace view. Framework-specific spans currently come from OpenInference instrumentors for LangChain/LangGraph, LlamaIndex, OpenAI Agents, and Claude Agent SDK. Best-effort throughout: any failure logs and never disturbs the span or the chat.

### 3b. Operational surface

`/health` stays a bare liveness probe. `/ready` reports operational readiness — a handler is registered and the configured memory backend is reachable (`ChatMemory.healthcheck()`) — returning `503` with per-check detail otherwise, so "running but not chat-ready" is distinguishable. When memory is configured, `GET /v1/conversations/{id}/messages` and `DELETE /v1/conversations/{id}` let clients read the human-facing transcript (deliberately *not* the agent's window — folded turns stay, with `summary`/`summarized_through` marking the divergence) and drop server-side memory (the review noted that "new session" otherwise left SQL memory behind). Both are registered only when memory exists and advertised via `capabilities.conversation_management`.

### 3c. Correlation, not evaluation

The SDK owns one **turn span** per request: a `SERVER` span opened from the caller's extracted W3C context and held open for the whole turn (for streaming, around the generator — the route returns before the first token, so a span opened there would close too early). It carries agent identity, `conversation_id` / `response_id` / `message_id` / `deployment_id` / framework, and — via `input.value` / `output.value` — the **authoritative** input and final answer, taken from the request and response objects rather than reconstructed downstream from whichever child span looked most like an answer.

Three things follow, and none of them work without it:

- **Continuation.** Before, the SDK built a `TracerProvider` but installed no server instrumentation, so an incoming `traceparent` was ignored and every request began a fresh trace. A caller that generated a trace id — an eval runner — could never find the trace the agent produced.
- **Correlation actually firing.** `_annotate_span` stamped `trace.get_current_span()`, which by finalize time is the invalid default span: the framework's spans have already ended. It was a silent no-op. It now prefers the turn span.
- **Baggage reaching storage.** A `BaggageSpanProcessor` copies `hopsworks.eval.*` entries onto every span, since baggage rides the request context but never becomes span attributes on its own — and the OTLP sidecar only ever sees spans. Only allowlisted prefixes are copied: baggage is caller-controlled, and copying it wholesale would let any caller write arbitrary attributes into the project's trace tables.

The manifest reports `capabilities.trace_correlation` and `capabilities.eval_mode` so a runner can check a deployment *before* firing a suite at it instead of inferring from broken results — absent means an SDK too old to correlate at all.

`capabilities.eval_per_request` is the finer-grained alternative to `eval_mode`: the agent declares (`AgentApp(eval_per_request=True)`) that its tools consult `evaluation.in_evaluation()` before a production side effect, and the SDK turns that on for a turn whose `hopsworks.eval.run_id` baggage names a run Hopsworks confirms exists and targets this deployment. The verification is the point — the baggage is caller-controlled, and an unverified header could make the agent tell a customer their order is recorded while recording nothing. A trial that cannot be verified is refused (403 `eval_unverified`) rather than run as production traffic. `EVAL_MODE` still wins: under it every turn is an evaluation and nothing is verified. The runner accepts a sandboxed suite against either declaration; it can verify neither, so both are the author's word.

Attribute names live in `conventions.py`, which is deliberately dependency-free so the sidecar and the eval runner can import it. Three components agreeing on string literals by copy-paste is how they stop agreeing.

This makes it *possible* for a separate evaluation system to join chat turns, feedback, and traces — but feedback/ratings/scores are deliberately **not** in the SDK (see non-goals). The SDK's job ends at emitting correlatable ids.

One consequence for existing deployments: the turn span is now the root, where previously the framework's outermost span was. Trace latency in `otel_trace_metrics` therefore starts including SDK overhead and post-answer memory finalization — which is what the client actually waits for, but it will shift the numbers when a deployment upgrades.

### 4. Memory is opt-in, keyed by `conversation_id`, and tiered

Three tiers behind one store: the conversation buffer, a rolling summary that
compacts old turns instead of dropping them, and durable per-subject state the
agent writes through model-callable tools. Full rationale — the turn lifecycle,
the three-phase fold and why the summarizer cannot run inside the transaction,
retention split by row type, and why the `app` scope is not model-writable —
lives in `~/Work/hopsworks-agent-memory-design.md`.


The protocol makes history server-side (clients send only the new message + a `conversation_id`), so the SDK can own storage. `ChatMemory` has two backends: `InMemoryAgentMemory` (dev — documented as unfit for production since deployments scale to zero and replicas don't share it) and `ManagedMemoryService` (any SQLAlchemy URL; zero-config inside a deployment, resolving the project MySQL from injected `MYSQL_*` env vars + the password secret, table name from `DEPLOYMENT_ID`).

Design choices:

- **Auto-recording**: the SDK appends both turns after each *successful* exchange (`status == completed`), so handlers only read. History returns as `{"role", "content"}` dicts — the shape LangChain/LangGraph/LlamaIndex all accept, so it drops straight into a framework call.
- **Opt-in, not default**: many frameworks already persist state (LangGraph checkpointers, LlamaIndex chat stores). Defaulting memory on would create two competing sources of truth. The rule, documented at the call site: if your framework persists state, key it by `conversation_id` and skip `memory=`.
- **Never fatal, including at startup**: `ManagedMemoryService` construction never touches the database — the engine and table are created lazily on first use. If the database is unreachable at startup or on any turn, the store degrades to statelessness (reads return empty, writes are dropped, both logged) rather than crashing the pod or 500-ing the chat. Only a missing `[memory-sql]` dependency is a hard error, and only at construction.
- **Text-only history**: memory records the *text* of each turn (see the multimodal note below). Non-text content parts (images, files) are not persisted — a design choice, not an oversight: agents rarely re-feed prior images, and inline base64 in a history table is a poor fit. Agents that need multimodal history should carry it in their own store or framework state.

### 5. Tracing is env-gated, zero-config

The platform runs an OTLP sidecar and injects `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` **only when tracing is enabled on the deployment**. The SDK keys off exactly that: endpoint present → build a `TracerProvider` (with `SimpleSpanProcessor`, which the sidecar's span propagation requires) and activate the framework-matching OpenInference instrumentor; endpoint absent → untraced, silently. `tracing=True/False` overrides the auto-detection. This mirror-images a real bug in the pre-SDK agents, which hardcoded `localhost:4318` as a fallback and spammed connection-refused retries on every request whenever tracing was disabled.

### 6. Everything degrades to a warning, never a crash

Optional dependencies (OTel SDK, instrumentation packages, SQLAlchemy) are imported lazily inside the code paths that need them. Missing package → logged warning + reduced functionality (untraced, or a clear "install the extra" error for memory), never an import-time failure. An agent that declares `framework="langgraph"` but forgot the `[langgraph]` extra still serves chat, just without framework spans.

## Versioning & packaging

- Package version tracks capability additions; protocol version (`protocol_version` in the manifest, currently `1.1`) tracks the wire contract and evolves additively — clients accept any `1.x` and ignore unknown fields.
- Optional extras keep the base install thin: `[tracing]`, `[langgraph]`, `[llamaindex]`, `[openai-agents]`, `[claude-agents]`, `[memory-sql]`, `[memory-search]`. Base install is FastAPI + Pydantic only.
- Distributed as a git dependency today (`hopsworks-agent-protocol @ git+...`); the intended path is an internal PyPI publish or vendoring into the agent base image so `from hopsworks_agents.protocol import AgentApp` works out of the box in every deployment.

## Non-goals (deliberate)

- **No per-framework wire protocols.** The framework is an implementation detail the client must not see (§2).
- **No feedback/ratings in the SDK.** Thumbs, scores, labels, eval runs, and their storage belong to the Agent Evaluation system, not the serving SDK. The SDK's contribution is reliable correlation ids (§3c); a chat client submits feedback to the evaluation API keyed by `conversation_id` / `response_id` / `trace_id`. Adding a `/v1/feedback` here would couple serving to evaluation and duplicate storage.
- **No live/bidirectional voice.** Audio *attachments* fit as content parts; real-time spoken conversation is a different transport (WebSocket/WebRTC) and would be a separate capability, not bolted onto request/response.
- **No large-artifact storage yet.** Binary content is inline base64 with a size cap; a `file_id` upload/download endpoint pair (§roadmap) is reserved for when agents need to exchange artifacts larger than chat scale.
- **The SDK does not own the client.** It emits a protocol; the chat panel (and any other consumer) is independent.

## Roadmap

- **Publish** to internal PyPI or bake into the agent base image so `from hopsworks_agents.protocol import AgentApp` works without a git dependency.
- **File upload/download** (`POST /v1/files`, `GET /v1/files/{id}`) — the upgrade path from inline base64 for large generated artifacts (reports, PDFs, audio). Needs a storage abstraction (HopsFS/dataset path or object store) and a manifest capability. Deferred as the largest of the reviewed suggestions.
- **Panel-side tool-event rendering** — the SDK already emits `tool_event` frames; the chat panel needs to render retrieval/tool/code progress rows.
- **Agent graph** (`graph=` + `GET /v1/graph`, `capabilities.graph`) — normalises a LangGraph/LlamaIndex/dict graph to `{nodes, edges}` for the panel's Graph tab (static topology). Live node/path highlighting is a phase 2 on the tool-event stream.
- **Framework recipe cookbook** — LangGraph checkpointer + `conversation_id`, LlamaIndex chat engine, plain-Anthropic streaming — so authors copy the right integration for their framework.
- **Richer trace correlation** — a typed `trace_id`/`span_id` field on `ChatResponse` rather than only `metadata`, once the evaluation join is designed.

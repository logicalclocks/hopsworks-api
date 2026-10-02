# hopsworks_agents.protocol

Server helpers for the **Hopsworks Agent Protocol**: make any Python agent chat-ready in the Hopsworks UI with a few lines. `AgentApp` is a FastAPI subclass that automatically exposes:

| Route | Purpose |
|---|---|
| `GET /.well-known/hopsworks-agent.json` | Protocol manifest — the Hopsworks chat panel auto-detects the agent from it |
| `POST /v1/chat` | Non-streaming chat |
| `POST /v1/chat/stream` | SSE streaming (when a stream handler is registered) |
| `GET /health` | Liveness probe |

CORS is enabled by default (required for the in-browser chat panel). Part of the `hopsworks` package; serving an agent needs the `agents` extra (`pip install 'hopsworks[agents]'`).

## Quick start

```python
from hopsworks_agents.protocol import AgentApp, AgentResponse

agent_app = AgentApp(
    name="My agent",
    description="A custom LangGraph agent",
    welcome_message="How can I help?",
    suggested_prompts=["What is attention?"],
)


@agent_app.chat
async def chat(request):
    result = await my_agent.ainvoke(
        {"messages": request.to_framework_messages()},
        config={"configurable": {"thread_id": request.conversation_id}},
    )
    return AgentResponse.text(
        text=result["messages"][-1].content,
        conversation_id=request.conversation_id,
    )
```

Run it like any FastAPI app (`uvicorn my_agent:agent_app`). Deploy it as a Hopsworks agent deployment and the UI chat panel works with zero configuration.

Notes:

- `request.conversation_id` is **always set** (generated on the first turn) — safe to pass straight to a LangGraph checkpointer / LlamaIndex chat store. History is server-side: the request carries only the new message.
- `request.text` gives the plain message text; `request.to_framework_messages()` gives `[{"role", "content"}]` accepted by LangChain/LangGraph/LlamaIndex.
- Returning a plain `str` from the handler is shorthand for `AgentResponse.text(...)`.
- Raise `AgentError("msg", code="...", status_code=400, retryable=False)` for structured errors.

## Streaming

Register an async generator; the manifest advertises `streaming: true` automatically, `/v1/chat/stream` emits `message.delta` events, and `/v1/chat` still works by collecting the stream:

```python
@agent_app.stream
async def stream(request):
    async for event in my_agent.astream_events(...):
        if delta := extract_text(event):
            yield delta
    # optional: attach citations/usage to the final message
    yield AgentResponse.text(text="", citations=[...], usage={"output_tokens": 42})
```

If only a `@chat` handler exists, `/v1/chat/stream` degrades gracefully to a single `message.completed` event.

## Multimodal (v1.1)

Declare what your agent accepts/returns and use content parts:

```python
from hopsworks_agents.protocol import AgentApp, AgentResponse, ImageContent, TextContent

agent_app = AgentApp(
    name="Vision agent",
    input_modalities=["text", "image"],
    output_modalities=["text", "image"],
)

@agent_app.chat
async def chat(request):
    for image in request.images:          # base64 in image.data, image.media_type
        ...
    return AgentResponse.parts(
        TextContent(text="Here's the chart:"),
        ImageContent(media_type="image/png", data=chart_base64),
        conversation_id=request.conversation_id,
    )
```

`request.images` / `request.files` / `request.audio_clips` expose non-text parts; the manifest advertises the modalities so the Hopsworks chat panel enables the matching attachment pickers and renders returned images/files/audio inline. Binary parts don't stream — they arrive in the final `message.completed` response.

## Tracing (automatic)

When tracing is enabled on the Hopsworks deployment, the platform injects
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and runs an OTLP collector sidecar. The
library detects this and wires OTel automatically — no tracing code in the
agent. Declare the framework so the right OpenInference instrumentor is used
(or let the platform-injected `AGENT_FRAMEWORK` env var decide):

```python
agent_app = AgentApp(name="My agent", framework="langgraph")
```

- `framework="langgraph"` → LangChain/LangGraph instrumentation (`pip install openinference-instrumentation-langchain`)
- `framework="llamaindex"` → LlamaIndex instrumentation (`openinference-instrumentation-llama-index`)
- `framework="openai_agents"` → OpenAI Agents SDK instrumentation (`openinference-instrumentation-openai-agents`)
- `framework="claude_agents"` → Claude Agent SDK instrumentation (`openinference-instrumentation-claude-agent-sdk`)
- `framework="custom"` → provider only; instrument manually via `agent_app.tracer_provider`
- `tracing=False` opts out; `tracing=True` warns if the deployment has no tracing endpoint
- Missing instrumentation packages never crash the agent — it runs untraced with a warning

## Memory

The protocol keeps history server-side (`conversation_id`), so the SDK can own
storage. One store, three tiers — each opt-in, all served by the same object:

```python
from hopsworks_agents.protocol import (
    AgentApp, ManagedMemoryService, anthropic_summarizer,
)

agent_app = AgentApp(
    name="My agent",
    memory=ManagedMemoryService(          # zero-config in a deployment
        summarize=anthropic_summarizer(),  # tier 2
        long_term=True,                    # tier 3
    ),
)
```

| Tier | Holds | Turn on with |
|---|---|---|
| 1. Conversation buffer | this conversation's turns | `memory=` alone |
| 2. Rolling summary | older turns, compacted instead of dropped | `summarize=` |
| 3. Durable memory | facts about the user, across conversations | `long_term=True` |

### Reading it in a handler

```python
@agent_app.chat
async def chat(request, ctx):
    system = MY_PROMPT + ctx.system_context()   # summary + what you know
    messages = ctx.history + [{"role": "user", "content": request.text}]
    ...
```

> **`ctx.history` stops meaning "the conversation" once tier 2 is on.** It is
> the turns *since the last fold*; everything older is in `ctx.summary`. Passing
> `ctx.history` alone silently drops the compacted part.
> `ctx.system_context()` assembles the summary and this user's stored facts into
> a block for your system prompt, and returns `""` when there is nothing yet, so
> it is safe to concatenate unconditionally. The SDK builds it every turn but
> never places it — where context belongs is a property of your prompt.

Summarizing runs *after* the response has streamed and is awaited before the
route returns, so it costs request duration every Nth turn and never
time-to-answer. `summarize` is any callable
`(previous_summary, turns) -> str`, sync or async. Two are shipped:
`anthropic_summarizer()` for Claude and `openai_summarizer()` for OpenAI and
for anything that speaks its chat-completions API through `base_url` — a vLLM
or Ollama instance in the cluster, a LiteLLM proxy, Azure:

```python
summarize=openai_summarizer()                                      # OPENAI_API_KEY
summarize=openai_summarizer("llama-3.1-8b", base_url="http://vllm:8000/v1", api_key="none")
```

Both resolve the key on first use — explicit argument, then the environment
(`OPENAI_API_KEY` / `ANTHROPIC_API_KEY`), then the Hopsworks secret named by
`api_key_secret` — so a deployment reuses the secret its agent already holds.

An LLM deployed in Hopsworks needs no key at all. `hopsworks_summarizer` finds
the deployment by name, goes through the inference gateway with the serving
key the pod already has, and uses the model the deployment serves:

```python
summarize=hopsworks_summarizer("my-llm")                    # an LLM deployment in the project
summarize=hopsworks_summarizer("my-llm", "llama-3.1-8b")    # when it serves several
```

Anything else is just a function with that shape.

### Agent-callable memory tools

Tier 3 adds `remember` / `recall` / `forget` / `search`, which the agent's own
LLM calls when it decides to — so there is no extraction model in the SDK
guessing what is worth keeping. Register them in your agent's tool list; the SDK
cannot reach into an arbitrary framework's tools, and appending to yours behind
your back would be worse than asking:

```python
from hopsworks_agents.protocol import memory_tools

agent = create_react_agent(llm, [*my_tools, *memory_tools("langgraph")])
# memory_tools("llamaindex") -> [FunctionTool]
# memory_tools("openai_agents") -> [@function_tool wrappers]
# memory_tools("claude_agents") -> [Claude Agent SDK tools]
# memory_tools("plain") -> bare functions
```

They take no store or user argument — those resolve from the request context, so
the signature the model sees carries no plumbing.

`search` looks over the user's own past conversations. It works as soon as tier 3
is on, using keyword matching; add an `embedder` and a `vector_store` to upgrade
it to semantic search over a Hopsworks embedding feature group, with no prompt
change:

```python
from hopsworks_agents.protocol import sentence_transformer_embedder, vector_store_for

embedder = sentence_transformer_embedder()          # hopsworks[agents-memory]
ManagedMemoryService(long_term=True, embedder=embedder,
                      vector_store=vector_store_for(embedder))
```

The embedder loads its model from the project's model registry, so a pod never
downloads it from the internet. Register it once, where there is internet (a
notebook, or the feature pipeline that embeds the data):

```python
from hopsworks_agents.protocol import register_sentence_transformer, load_sentence_transformer

register_sentence_transformer("all-MiniLM-L6-v2")    # idempotent; force=True for a new version
embed = load_sentence_transformer("all-MiniLM-L6-v2")  # from the registry, the hub only as fallback
```

`load_sentence_transformer` is also what to call for a model the agent's own
tools use. An unregistered model still comes from the hub, with a warning;
`fallback=False` makes it an error for pods that must stay offline.

### Telling the traces who a conversation was with

An agent that asks for a customer key learns who it is talking to *during* the
conversation — after the first turn's spans have been exported and can no
longer be changed. `identify` records the answer against the conversation
instead, so a developer holding that key can find every conversation it had:

```python
from hopsworks_agents.protocol import identity_tools

agent = create_react_agent(llm, [*my_tools, *identity_tools("langgraph")])
```

Registered separately from `memory_tools`, and not for tidiness. What it writes
is what the Traces view shows as the person on the other end, and the caller is
a model reading text a user typed, so it **labels only** — it changes nothing
about what the agent may remember or recall. An agent that must act on an
identity should verify the claim itself and call `ctx.rebind_subject()`, which
does both.

When the client already knows who the user is and asserts `subject` on the
request, nothing needs registering: the SDK stamps OpenInference `user.id` on
the turn span and the traces pick it up from there.

### Backends and behaviour

- `InMemoryAgentMemory()` — zero-config for development. Lost on restart and
  per-replica; agent deployments can scale to zero, so not for production.
  Tiers 2 and 3 are no-ops on it.
- `ManagedMemoryService()` — inside a Hopsworks agent deployment this is
  zero-config: the project MySQL URL is built from the platform-injected
  `MYSQL_*` env vars (password via the `MYSQL_PASSWORD_SECRET_NAME` secret)
  and table names are derived from `DEPLOYMENT_ID`. Outside a deployment pass
  any SQLAlchemy URL (`hopsworks[agents-memory]`). Survives restarts, shared across
  replicas.
- **A turn is recorded as it happens, not after it succeeds.** The user message
  is written when the turn opens — that is what lets a handler read back the
  message it is answering, and lets anything the agent remembers point at the
  turn that caused it. An open turn is invisible to `ctx.history` until it
  closes, and a turn that fails (handler error, client disconnect) is marked
  *abandoned* rather than left as a question whose answer never arrived.
- Memory failures never break the chat — they log and the reply still goes out.
- **If your framework persists state itself** (LangGraph checkpointer,
  LlamaIndex chat store), key it by `conversation_id` and skip `memory=` —
  keep one source of truth for history.
- **`subject` is client-asserted.** Tier 3 keys durable memory by
  `ChatRequest.subject`; the ingress authenticates a project-wide serving key,
  not a person, so the agent cannot verify it. Without one it falls back to the
  conversation id (memory degrades to per-conversation durability). Fine between
  project members; not a security boundary.

## Handler context (optional)

Declare a second parameter and the SDK passes a `HandlerContext` with per-turn
conveniences — `def chat(request)` and `def chat(request, ctx)` both work:

```python
@agent_app.chat
async def chat(request, ctx):
    history = ctx.history                       # turns since the last fold
    ctx.logger.info("turn for %s", ctx.conversation_id)
    await ctx.emit_event("retrieve", status="running", message="searching")
    ...
```

`ctx` exposes `conversation_id`, `request`, `memory`, `logger`, `deployment_id`,
`framework`, `response_id`/`message_id`/`turn_id`, and `emit_event`, plus the
memory accessors: `history`, `summary`, `state(scope=...)`, `system_context()`,
and `subject` (see [Memory](#memory)).

## Progress (tool) events

With `AgentApp(tool_events=True)`, tool calls surface as `tool_event` SSE frames
(interleaved with the reply while streaming; buffered into response `metadata`
otherwise), which the chat panel renders as progress chips.

- **Automatic** — when tracing is active, the SDK taps the framework
  instrumentation it already runs (LangChain/LangGraph, LlamaIndex, OpenAI
  Agents, Claude Agent SDK) and emits a `running`/`done` event per tool span,
  keyed by span id. Zero code in the agent. (Requires the framework to
  propagate context into worker threads; LangChain does. Frameworks that don't
  will not auto-emit — use manual events or the trace view.)
- **Manual** — `await ctx.emit_event(name, status, message, data, event_id)`
  for custom progress; pass the same `event_id` for a call's start and end so
  the client shows one updating chip.
- **LangChain/LangGraph helper** — pipe `astream_events(version="v2")` through
  `ctx.stream_langchain(...)`: it yields the assistant text deltas and turns
  `on_tool_start`/`on_tool_end`/`on_tool_error` into tool-event chips
  automatically, with no `emit_event` calls and no dependency on tracing:

  ```python
  @agent_app.stream
  async def stream(request, ctx):
      async for delta in ctx.stream_langchain(agent.astream_events(inputs, version="v2")):
          yield delta
  ```
- **LlamaIndex helper** — `ctx.stream_llamaindex(handler)` does the same for a
  LlamaIndex workflow agent's run handler (`AgentStream` deltas + `ToolCall` /
  `ToolCallResult` chips):

  ```python
  @agent_app.stream
  async def stream(request, ctx):
      async for delta in ctx.stream_llamaindex(agent.run(msg)):
          yield delta
  ```

## End-user feedback

A chat UI built on the agent's endpoint has the gateway, the serving credential and the
`trace_id` on every reply, and nothing else -- no Hopsworks address, and its users need no
Hopsworks account. So the agent takes the verdict and relays it itself:

```http
POST /v1/feedback
{"trace_id": "<metadata.trace_id of the reply>", "verdict": "negative",
 "issue_category": "wrong_tool", "corrected_answer": "...", "note": "...",
 "subject": "alice"}
```

`verdict` is `positive`, `negative` or `false_alarm`. A client that kept no trace id sends
`conversation_id` instead and the latest turn of that conversation is rated. `subject` is who
is speaking, asserted by the client like the chat request's; the row lands in Hopsworks under
the reviewer `user:<subject>` (`user:anonymous` without one), so it never collides with a
reviewer's account, shows as **user** in the Feedback tab, and reaches the failure analysis as
a signal to triage rather than a verdict that closes the trace. The manifest advertises
`endpoints.feedback` and `capabilities.feedback`.

The relay runs over the deployment's own credential (`REST_ENDPOINT`, `DEPLOYMENT_ID` and the
key the platform mounts), the same way an agent reaches the feature store; outside a
deployment the route answers `503 platform_unavailable`.

## Operational endpoints

- `GET /health` — liveness (process up).
- `GET /ready` — readiness: a handler is registered and the memory backend (if
  configured) is reachable; `503` otherwise, with per-check detail.
- With memory configured:
  - `GET /v1/conversations/{id}/messages` — the **human-facing transcript**,
    which is deliberately not what the model sees: once turns are folded they
    leave `ctx.history` but stay here, with `summary` and `summarized_through`
    marking where the two diverge. `?include=events` adds tool/event rows and
    abandoned turns.
  - `DELETE /v1/conversations/{id}` — what a client's "new session" calls to
    also drop server-side memory. Clears the conversation and its
    session-scoped state; leaves durable per-user memory alone, because
    starting a new chat must not erase what the agent knows about the person.
- With `long_term=True`, `GET`/`DELETE /v1/subjects/{subject}/state` let a user
  see every durable value held about them — with its provenance and whether the
  agent or an operator wrote it — and delete any of them. Caps and TTLs bound a
  false memory; being able to see and delete it is what fixes it.

## Agent structure graph

Expose the agent's structure and the chat panel shows a **Graph** tab. For a
compiled LangGraph, pass it straight in:

```python
graph = build_graph()            # a compiled LangGraph
agent_app = AgentApp(name="My agent", graph=graph)  # or graph=graph.get_graph()
```

The SDK serves `{nodes, edges}` at `GET /v1/graph` and advertises the `graph`
capability. A **LlamaIndex `Workflow`** works too — its graph is derived from
the `@step` methods' consumed/produced event types (event nodes collapsed into
labeled edges, `StartEvent`/`StopEvent` → `__start__`/`__end__`), so a custom
workflow renders as cleanly as a LangGraph. A plain `{"nodes", "edges"}` dict
works as well; an unreadable object is ignored (the tab just won't appear).

## Extra routes

`AgentApp` is a `FastAPI` — add anything else the usual way:

```python
@agent_app.get("/my/custom/route")
def custom():
    ...
```

## Client: talking to agents, tracing and evaluation from Python

Everything the Hopsworks UI does for agents is reachable from the project's
agent-serving API. It rides the connected `hopsworks` client, so nothing else
needs configuring inside a job or a notebook, and an API key from outside.

```python
import hopsworks
from hopsworks_agents.eval.sdk import check

project = hopsworks.login()
agents = project.get_agent_serving()

# the agents: by name or id, or all of them
agent = agents.get_agent("support")
print(agent.url, agent.is_running())

# talk to one; the reply carries the conversation to continue and the trace it recorded
reply = agent.chat("Where is order 42?")
print(reply.text, reply.trace_id)
agent.chat("And order 43?", conversation_id=reply.conversation_id)
agent.give_feedback(reply.trace_id, "positive")

# or streamed: text as it is produced, the agent's steps, then the completed reply
stream = agent.chat_stream("Cancel order 43")
for delta in stream:
    print(delta, end="", flush=True)
print(stream.reply.trace_id, [t.name for t in stream.tool_events])
for frame in agent.chat_stream("Refund it").events():   # every frame: delta / tool / completed
    ...

# suites, tasks, the evaluator library
suite = agents.suites.create(
    "Refunds",
    checks=[check("llm_judge", "quality", provider="anthropic",
                  criteria=["Answers the question", "Uses the customer key"]),
            check("no_tool_error")],
    tags=["regression"],
)
suite.add_task("Refund order 42", expectations={"quality": "Confirms the refund and its amount."})
suite.import_tasks([{"question": "Where is my order?"}, {"question": "Cancel it"}])
suite = suite.publish()          # frozen; runs can say what they executed
suite.update(description="Refund flows")   # name, tags, description at any time

# runs against the agent
run = agent.run(suite, n_trials=3).wait()
for trial in run.trials():
    print(trial.task_id, trial.status, trial.latency_ms)
for result in run.results():
    print(result.evaluator_name, result.passed, result.reason)
print(agent.gates().passed)

# production: traces, sessions, feedback
for trace in agent.traces(search="customer key", search_field="messages"):
    print(trace.trace_id, trace.session_id, trace.latency_ms, trace.failed)
for turn in agent.conversation("conv_123"):
    print(turn["user"], "->", turn["assistant"])
agent.give_feedback("305b97bb...", "negative", issue_category="wrong_tool",
                    corrected_answer="Look the customer up by the key they gave.")
page = agent.feedback(verdict="negative")
print(page.count, [f.reviewer for f in page.feedback])

# failure analysis: the job, the proposals, the clusters
job = agents.jobs.ensure_review_job(agent.id, provider="anthropic", model="claude-sonnet-5",
                                    sources=["feedback", "errors", "judge"], read_source_code=True)
run = job.analyse().wait()                       # or job.analyse(since=..., until=...) / trace_id=...
for triage in agent.triage(page.feedback):
    print(triage.category, triage.failure_summary, triage.suspected_code_bug, triage.findings)
    agent.decide_triage(triage, "accepted")
for cluster in agent.clusters():
    print(cluster.label, cluster.size)
task = agent.promote_cluster(agent.clusters()[0])   # PENDING_REDACTION
task.confirm_redaction().add_to_regressions()
agents.jobs.run_regressions(agent.id)

# monitoring
agent.sample(evaluator=agents.evaluators.find("Hallucination"))
agent.trace_metrics(since=..., until=...); agent.llm_metrics(); agent.tool_metrics()

# lifecycle, the same as model serving's
agent = agents.deploy_agent("my_agent.py", name="support")
agent.start(); agent.restart(); agent.stop()
agent.deployment       # the hsml Deployment, for resources, scaling and logs
```

Every model keeps `raw`, the API's dict, so a field the model does not name is
still there. A refusal raises `AgentServingError` with the API's own message and
the status. Messages reach the agent through the cluster's inference gateway
(the same route the chat panel uses); outside Hopsworks the API key is the
credential, and `AgentServing(..., gateway_url=...)` names the gateway when the
library cannot discover it.

## Development

```bash
cd python && uv sync --extra dev
pytest tests/agents
```

Extras of the `hopsworks` package: `agents` (FastAPI, pydantic, OpenTelemetry: serve an
agent), `agents-eval` (the judges' provider SDKs, for the evaluation and analysis jobs),
`agents-memory` (SQLAlchemy and sentence-transformers, for durable memory). The client
half -- talking to a deployed agent, the evaluation and tracing API -- needs none of them.
The framework instrumentations (`openinference-instrumentation-*`) are installed alongside
the framework they instrument.

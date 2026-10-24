---
name: hops-agent-deployment
description: Use when writing and deploying an interactive agent (LangGraph, LlamaIndex, OpenAI Agents, Claude Agent SDK or plain Python) as a served Hopsworks agent deployment, or when talking to one from Python. Auto-invoke for "deploy my agent", "make my agent chat-ready in Hopsworks", AgentApp, the hopsworks agent protocol, agent tracing or memory, `hops agent`, or `project.get_agent_serving()`. Input an entry script; output a served, chat-ready agent deployment.
---

# Hopsworks Agent Deployments

An agent deployment is a **server-only deployment with no model attached**: you ship an entry script (or a package) and Hopsworks runs it as a service. The agent *is* the **inference pipeline** of the AI system: it usually skips the training pipeline and calls a foundation LLM, and if it needs RAG it reads context from the feature store (write the RAG features in a separate **feature pipeline** — see hops-features). Agents can be created from HopsFS or from a Git repository, like apps. For a scheduled, non-interactive coding agent use **hops-agent-task**; for a model-backed predictor use **hops-online-inference**; to evaluate and improve a deployed agent use **hops-agent-evals**.

Start with a deterministic **LLM workflow** (a fixed sequence of steps) and only graduate to an autonomous agent when the task is open-ended enough to require runtime planning over tools. Workflows are cheaper, lower-latency, and easier to make reliable.

## Contract
- **Input:** an entry script (a `.py` file, or a directory containing a `pyproject.toml`) that builds an `AgentApp`, from HopsFS or from a Git repository.
- **Output:** a served agent deployment: chat-ready in the Hopsworks UI, traced, reachable from Python with `agent.chat()`.
- **Pre-condition:** auth + serving reachable; the agent name and environment are valid (`[A-Za-z0-9_-]+`); the environment is cloned from `python-agent-pipeline` (it carries `hopsworks_agents.protocol` and the `agents` extra).

## Smoke-test (cheap pre/post-flight)

```bash
hops agent list                  # what agents exist (confirms auth + serving reachable)
hops agent info my_agent         # status, URL, git source and commit
hops agent logs my_agent         # startup and request errors land here
```

## Ask the user (only when state is ambiguous)
- **Framework.** `langgraph`, `llamaindex`, `openai_agents`, `claude_agents` or `custom`: picks the tracing instrumentation.
- **Memory.** Working memory only, plus a short-term rolling summary, plus long-term memory across conversations (the summary needs a summariser key; long-term memory needs the *client* to say who the user is — see **Who long-term memory belongs to**).
- **Source.** A local script uploaded to HopsFS, or a Git repository (branch, and whether to auto-redeploy on push).
- **Evaluation.** Whether the agent will be evaluated against a sandboxed suite while serving customers (`eval_per_request=True` and tools that check `in_evaluation()`), or in a deployment of its own (`EVAL_MODE=true`).

## Write the entry script

Build the agent with the framework of your choice and wrap it in `AgentApp` from `hopsworks_agents.protocol`. `AgentApp` is a FastAPI app that speaks the **Hopsworks Agent Protocol**: the chat panel detects it from its manifest, `/v1/chat` and `/v1/chat/stream` are served, health and readiness probes exist, CORS is on, and when tracing is enabled on the deployment the library wires OpenTelemetry itself. The script runs as a program in the pod, so serve the app on port 8080 in its main block.

```python
# my_agent.py
from hopsworks_agents.protocol import AgentApp, AgentError

agent_app = AgentApp(
    name="Support agent",
    description="Answers catalogue questions and handles refunds.",
    framework="langgraph",                 # picks the tracing instrumentation
    welcome_message="How can I help?",
    suggested_prompts=["What albums do you have by Prince?"],
    tool_events=True,                      # tool calls show as progress chips in the chat
)

graph = build_graph()                      # your LangGraph / LlamaIndex / custom agent, built once


@agent_app.stream                          # one handler serves /v1/chat and /v1/chat/stream
async def stream(request, ctx):
    if not request.text:
        raise AgentError("The message cannot be empty.", status_code=400)
    events = graph.astream_events(
        {"messages": request.to_framework_messages()},
        config={"configurable": {"thread_id": request.conversation_id}},
        version="v2",
    )
    async for delta in ctx.stream_langchain(events):   # text deltas out, tool calls as chips
        yield delta


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(agent_app, host="0.0.0.0", port=8080)
```

What matters in the handler:
- `request.conversation_id` is always set; history is server-side, the request carries only the new message. `request.text` is the plain text; `request.to_framework_messages()` gives `[{"role", "content"}]`.
- A `@chat` handler returning a `str` (or `AgentResponse.text(...)`) is enough when you do not stream. Only `@stream` gives token deltas in the panel.
- **Tracing is automatic** when enabled on the deployment. Every LLM call, tool call and retrieval becomes a span; these traces are what evaluation, feedback and failure analysis read. An agent without tracing cannot be debugged or improved, so enable it.
- **Memory without code:** a deployment can carry the policy instead — the form's Agent memory card, or `deploy_agent(memory=DeploymentMemoryConfig(summarize={"provider": "hopsworks", "deployment": "my-llm"}, long_term=True))`, injected as `AGENT_MEMORY_CONFIG`. `AgentApp` applies it only when the agent passed no `memory=`, so code wins; `capabilities.memory.source` says which it was. The handler still has to read memory, so `ctx.history` and `ctx.system_context()` stay in the prompt either way.
- **Memory:** `memory=ManagedMemoryService(summarize=anthropic_summarizer(), long_term=True)` gives all three tiers with no storage to set up — working memory (the turns in context), short-term memory (older turns folded into a rolling summary) and long-term memory, both semantic (what the agent decides to remember, through `remember`/`recall`/`forget`) and episodic (`search` over past conversations); read it with `ctx.system_context()` and `ctx.history`, and read or write your own facts with `ctx.recall(key)` / `ctx.remember(key, value, scope="user"|"session")` / `ctx.forget(key)`, which key each scope the way `ctx.state(scope)` reads it (`app` scope is agent-wide and not a turn's to write). Match the summarizer to the key the agent already has: `openai_summarizer()` for OpenAI, `openai_summarizer(model, base_url=...)` for a vLLM/Ollama/LiteLLM-proxy endpoint, `anthropic_summarizer()` for Claude, `hopsworks_summarizer("<deployment name>")` for an LLM deployed in the same project (no key to configure); any `(previous_summary, turns) -> str` callable also works.
- **Embedding models never download in a pod.** Register once where there is internet with `register_sentence_transformer("all-MiniLM-L6-v2")` (a notebook or the feature pipeline that embeds the data) and load with `load_sentence_transformer("all-MiniLM-L6-v2")` in the agent; `sentence_transformer_embedder()` does the same for memory search. An unregistered model falls back to the hub with a warning; `fallback=False` forbids it.
- **Evaluation-safe writes:** an agent that writes (refunds, tickets) checks `hopsworks_agents.protocol.evaluation.in_evaluation()` in its tools and skips the real write, so a sandboxed suite can run against the production deployment (`eval_per_request=True` on the app). A deployment that exists only to be evaluated sets `EVAL_MODE=true` instead.

## Who long-term memory belongs to

`user`-scope memory is keyed by the request's **subject**, and the subject is whatever the client asserted — `agent.chat(..., subject="marcus@example.com")`, the chat panel's signed-in user, or your own application calling `ctx.rebind_subject(key)` after it has verified the person. That is what makes a second conversation start knowing them.

**Asking the user who they are does not do this.** The `identify` tool (from `identity_tools`) calls `ctx.record_subject(..., source="model")`, which *labels* the conversation for traces and failure analysis and nothing more. It deliberately does not rebind: rebinding decides whose durable memory a conversation may read, and the caller there is a model acting on text a user typed, so an agent could be talked into reading someone else's facts. With `identify` alone, every `remember(..., scope="user")` is owned by the conversation id, and the next conversation — same email, same person — knows nothing.

So, when you design an agent with long-term memory:

- a real deployment has a login: the app asserts `subject` per request and nobody is asked anything;
- a demo without one either passes `subject=` from the client, or calls `ctx.rebind_subject` in app code after the email is given — and should say in its own README why that is acceptable only for a demo;
- `ctx.has_subject` is true exactly when the turn has a real identity, and `ctx.subject_source` says how it was known: `"client"` (asserted on the request), `"app"` (bound this turn with `rebind_subject`) or `"conversation"` (bound on an earlier turn of this conversation and read back by the SDK — what every turn after the binding reports). The per-conversation fallback is the one case with `has_subject` false. Check `has_subject`, not a list of sources: an agent that only trusted `"client"`/`"app"` never told the model it was identified on turn three, and the model called `identify` again.
- a trace's `subject_source` (the traces table) can also read `"model"`: a label from `identify` on a conversation nobody bound. A model's label never replaces a binding, and `identify` on a bound conversation answers that it is already identified.
- the traces table's subject is the **conversation's**, as it stands now, on every row of it — the first turn of a conversation shows the address given on the second. Which turn identified the person is not readable there; the `identify` call in that turn's `trace.tool_calls` is.
- an **evaluation trial** with a subject reads that person's memory and never writes it: `remember`/`forget` under `user` scope land under the trial's own conversation, and its messages stay out of the person's history. That is what lets a regression replay a customer's turn as them (see **hops-agent-evals**) without the suite changing what is stored about them.

**Reading back what an agent remembers.** There is no SDK accessor yet. The facts are a feature group in the project's online store, so read them there:

```python
fs = project.get_feature_store()
facts = fs.get_feature_group("agent_memory_facts", 1).read(online=True)
facts[facts.deployment_id.astype(str) == str(agent.id)][["owner", "scope", "state_key", "value"]]
```

`deployment_id` is **text** in the memory groups (`agent_memory_facts`, `_messages`, `_conversations`) and a number in the others: the memory store runs outside Hopsworks too, where a deployment id is any name, so compare it as a string — `== agent.id` silently matches nothing.

Online, not offline: the offline Delta table has no commits until a materialisation runs, so `hops sql` / Trino answers `DELTA_LAKE_INVALID_SCHEMA ... has no commits`. `owner` is the subject for `user` scope — an email where the client asserted one, a conversation id where it did not, which is the quickest way to see whether the paragraph above is biting. The other columns are `id, source_ref, written_by, updated_at, expires_at` (`fg.columns`; `fg.features` is deprecated). An evaluation trial's `user`-scope facts have the **trial's** conversation id as `owner` (`<run>/<task>/…`), never the person's — that is how to confirm a replay did not write into a real user's memory.

Streaming, multimodal content, memory tiers, progress events and the structure graph are in the protocol README: `python/hopsworks_agents/protocol/README.md`.

## Deploy — CLI (preferred for local sources)

```bash
hops agent create my_agent.py --name my_agent \
  --requirements requirements.txt --environment my_agent [--memory 3072]
hops agent start my_agent                       # waits for RUNNING
hops agent info my_agent                        # status + URL
hops agent logs my_agent                        # follow startup / errors
hops agent stop my_agent
hops agent delete my_agent --yes
```

`create` re-run uploads the latest code and rewrites the predictor; a running agent is left untouched (`start`, or `restart` from Python, rolls it onto the new code). `hops agent query` posts a predictor-style body and is for the legacy `Predict` class contract; a protocol agent is spoken to with `agent.chat()` below or the UI chat panel.

A model loaded inside the agent (torch, an embedder) needs more than the default 1 GB of memory: pass `--memory` to `hops agent create`, or the pod is OOM-killed while it loads.

## Price the turns, or cost reads zero

An agent deployment reports a cost per turn, per trace and per evaluation run, computed from the tokens its spans record against the prices **on the deployment**. Those prices default to nothing, so an agent deployed without them reports `0.0` everywhere — and `0.0` is what a free model reports too. Nothing in the UI distinguishes "this cost nothing" from "nobody said what it costs"; the giveaway is `costed_trial_rate` of `0.0` on a run.

Set them when you deploy, from the model the agent actually uses:

```python
agents.deploy_agent(
    "my_agent.py", name="my_agent",
    tracing={
        "enabled": True,
        "input_token_price_per_million_tokens": 3.0,
        "output_token_price_per_million_tokens": 15.0,
    },
)
```

**Read the model out of the agent before you pick the numbers.** It is not always where you expect:

- the model is usually a constant or an environment variable near the top of the entry script — `MODEL = os.environ.get("AGENT_MODEL", "claude-sonnet-4-5")`;
- a framework default counts: an `Agent(...)` built with no `model=` runs the SDK's own default, which is a different price from the one in the README;
- an agent with several models — a cheap router and an expensive writer, or a judge of its own — bills at whichever one runs, so price the one that does the volume and say so in a comment;
- a provider-prefixed name (`anthropic/claude-sonnet-4-5`) is LiteLLM routing to that provider, and the price is that provider's.

**Then check against a real trace, because the source can be wrong.** The spans say which model actually answered, and a framework default beats anything the README claims:

```python
from collections import Counter
trace = agent.trace(agent.traces(limit=1)[0].trace_id)
print(Counter(s["attributes"].get("llm.model_name") for s in trace.spans
               if s["attributes"].get("openinference.span.kind") == "LLM"))
```

One chat after deploying settles it in a second, and it is the only way to catch an agent priced for the model its docstring names while the SDK runs its own default.

Prices change, so write them where the model is chosen and say when they were taken. A deployment whose model changed and whose prices did not is worse than an unpriced one: it reports a number that is confidently wrong.

**Where the cost shows.** A trace: `trace.total_input_tokens` / `total_output_tokens` and `trace.total_input_cost` / `total_output_cost` (there is no `total_cost` or `estimated_cost` on a trace). The totals are the sum over the trace's **model calls** — `llm.token_count.prompt` / `completion` on each LLM span, the turn's own figure only when no model call was traced — and cached prompt tokens are counted and priced at the full input price, so the cost is an upper bound for a provider that discounts cache reads. A run: `estimated_cost` and `costed_trial_rate` on its metrics, suite runs and monitor samples alike. A window: `agent.llm_metrics()` → `estimated_cost_usd` per model.

The two keys are per deployment, so two agents on different models each cost correctly, and re-deploying with new prices is what changes them — they are read per turn, not baked into old traces. Re-reading an old window after a price change does not re-cost it.

**A trace is not readable the instant the reply is.** The spans are exported after the answer and land over a second or two, so a read straight after `chat()` either raises `AgentServingError: Trace not found` or — worse — returns the trace **without its model calls**: no LLM span, zero tokens, zero cost, a turn that reads as free. Ask it to wait for the turn to close:

```python
trace = agent.trace(reply.trace_id, wait=10)   # until the turn span has landed, or 10 s
```

It polls until the turn span carries its response id, which the agent SDK stamps last. An agent not built on the Hopsworks agent SDK never stamps it, so `wait` costs it the full time.

**When every chat fails.** A running agent whose handler raises answers `/v1/chat` with a 500 whose body says why (`detail.code` `agent_error`, the exception in `detail.message`), and the traceback is in `hops agent logs`; an agent built on an older image returns a bare `500 Internal Server Error`, and then the log is the only place the reason is. A frequent cause after changing the model is a setting the new model refuses: gpt-6 and the o-series fix their own sampling and reject `temperature`, so leave it out for them.

**When a start takes too long, or says nothing.** A first start on a node that has not pulled the agent image can take ten minutes or more, and `start(await_running=...)` giving up does **not** stop the deployment — it usually comes up shortly after. When `hops agent info` shows an empty status and `hops agent logs` prints only `[failed to read pod logs]`, the container never started, so there are no logs to read: look at the pod instead.

```bash
kubectl get pods -n <project> | grep <name>-predictor    # ImagePullBackOff, Pending, OOMKilled
kubectl describe pod -n <project> <pod>                  # Events says which
```

See **hops-kubectl-debug**. `Pending` with no events is usually resources; `ImagePullBackOff` on a first deploy is the image still being pulled or missing from the cluster's registry.

**Confirm before deleting.** `hops agent delete` tears down the served agent irreversibly; confirm the exact name with the user, and never tear down an agent you created as a side effect (temp or test ones included) unless they asked. From Python it is `agent.delete(force=True)`, or `agent.stop()` and then `agent.delete()` — a plain `delete()` on a running agent raises `ModelServingException: Deployment not stopped`.

## Deploy — SDK

```python
import hopsworks

project = hopsworks.login()
agents = project.get_agent_serving()

agent = agents.deploy_agent(
    entry="my_agent.py",                 # .py file or a dir with pyproject.toml
    name="my_agent",
    requirements="requirements.txt",
    environment="my_agent",
    tracing={"enabled": True},           # spans for evals, feedback and failure analysis
)
agent.start(await_running=600)
# After editing the code: deploy_agent again, then agent.restart()
```

`deploy_agent` here is model serving's call (`project.get_model_serving().deploy_agent(...)` takes the same arguments) returning an `Agent`. `agent.deployment` is the `hsml` deployment for resources, scaling and describe.

An agent scales like a model deployment: pass `scaling_configuration=PredictorScalingConfig(...)` to `deploy_agent` (instances, the metric and its target; `min_instances=0` rests at zero when idle and wakes on the first request, the SDK's default minimum being 1). The choices and their rules are in [hops-autoscaling](../../scaling/hops-autoscaling/SKILL.md).

### Git-backed agents

When the source lives in Git, give the repository instead of a local path. The repository is cloned on each start, so a restart or redeploy picks up new commits. Providers: `GitHub`, `GitLab`, `BitBucket`; a private repository needs the user's git provider configured in Hopsworks.

```python
agent = agents.deploy_agent(
    entry="agent.py",                    # repo-relative
    name="my_agent",
    git_url="https://github.com/acme/my-agent-repo.git",
    git_provider="GitHub",
    git_branch="main",
    git_auto_redeploy=True,              # roll to branch HEAD on every new commit
    environment="my_agent",
)
```

`hops agent info` shows the git source, the resolved branch and the commit the deployment is running.

## Talk to the agent — SDK

```python
agent = agents.get_agent("my_agent")           # by name or deployment id; None if missing
agents.get_agents()                            # every agent in the project, for scripts

reply = agent.chat("hello")                    # ChatReply: .text, .conversation_id, .trace_id
agent.chat("and then?", conversation_id=reply.conversation_id)

stream = agent.chat_stream("tell me more")     # the protocol's SSE stream
for delta in stream:
    print(delta, end="")
stream.reply.trace_id; stream.tool_events      # after the stream is read

agent.give_feedback(reply.trace_id, "positive")
agent.url                                      # https://<gateway>/v1/<namespace>/<name>, for other clients
agent.manifest()                               # what the agent advertises
```

**A turn that takes minutes needs a longer client timeout.** `chat()` and `chat_stream()` wait 120 s by default (`timeout=`). An agent that runs analysts with web search can take longer, and a request that times out is not cancelled: the turn finishes in the pod and is traced, but the client never sees the reply. Pass `timeout=600` for such an agent, or use `chat_stream`, whose first delta arrives early.

**A turn the application answers itself has no model call.** When the app replies without running the model (an agent that asks who you are before doing anything else, say), the trace is one agent span: no LLM span, zero tokens, no cost, and `trace(wait=)` returns as soon as the turn closes.

**What `manifest()` returns** is the protocol document, not a flat summary: the agent's own fields are under `agent` (`name`, `description`, `version`, `framework`), the routes under `endpoints`, and the rest under `capabilities` and `ui`. `manifest()["agent"]["framework"]`, not `manifest()["framework"]` — the latter is `None` for every agent.

**Tool events pair by id.** Each tool call yields more than one event, `status` going `running` then `done`, matched on the id. The closing event is named for the event, not for the tool — an agent that emits `ctx.emit_event("tool", status="done")` ends with `name == "tool"` — so follow the id rather than the name.

They arrive in two shapes, which is worth knowing before writing the loop. `stream.tool_events` are `ToolEvent` **objects** — `.id`, `.name`, `.status`, `.message`, `.data`, `.raw`; the field is `id`, not `event_id`, and `json.dumps(event)` raises. On a non-streamed `agent.chat()` the same events ride on `reply.metadata["tool_events"]` as plain dicts with the same keys:

```python
for event in stream.tool_events:
    print(event.id, event.name, event.status)                  # objects
for event in reply.metadata.get("tool_events", []):
    print(event["id"], event["name"], event["status"])         # dicts, same fields
```

**Don't emit your own when `tool_events=True`.** The flag bridges the framework's tool spans to events by itself, which is the point of it: no code in the agent. An agent that also calls `ctx.emit_event` for the same tool used to produce two chips per call under two id spaces — the framework's call id and the span id. The automatic emitter now stays off any tool the handler reports itself, so either style works and mixing them is harmless; a tool you never claim is still reported for you.

**Reading a conversation back.** `agent.traces()` lists conversations, one row per session; `agent.session(session_id)` gives that conversation's turns and `agent.conversation(session_id)` pairs them as `{trace_id, user, assistant}` — `role`/`content` is the shape of the messages inside a turn, not of these.

Messages go through the cluster's inference gateway, the route the chat panel uses; the logged-in user's credential is accepted there. From outside Hopsworks, log in with an API key.

**A custom chat UI** talks to `agent.url` directly with the serving credential: `POST /v1/chat`
with `{"message": {"role": "user", "content": [{"type": "text", "text": "..."}]}}`, and its users
rate a reply with `POST /v1/feedback` `{"trace_id": reply.metadata.trace_id, "verdict":
"negative", "subject": "<who>"}`. The agent relays the verdict to Hopsworks itself, filed under
`user:<subject>`; the end user needs no Hopsworks account and the UI never learns the Hopsworks
address. See "End-user feedback" in `python/hopsworks_agents/protocol/README.md`.

## Next Steps

- Evaluate, monitor and improve the deployed agent: **hops-agent-evals** (suites, online sampling, feedback, failure analysis).
- Scheduled/batch coding agent instead of a served one: **hops-agent-task**.
- Model-backed online predictor: **hops-online-inference**.
- Instances, scale metric, scale to zero when idle: [hops-autoscaling](../../scaling/hops-autoscaling/SKILL.md).
- Agent serving dependencies: [hops-environments](../../platform/hops-environments/SKILL.md) — clone `python-agent-pipeline` and install requirements.
- Give the agent feature-store access for RAG: **hops-fv** (online feature vectors). Pass entity IDs (e.g. `user_id`) in the query so the agent can look up application state from the feature store.
- Agent memory or app state in the project database (`MYSQL_*` variables, password secret, privileges, RonDB table rules): **hops-app-db**.

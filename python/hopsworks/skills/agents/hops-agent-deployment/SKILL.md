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
  It goes in two places: `AgentApp(framework=...)` in the code and the deployment's config (see **Framework and chat contract** below).
- **Memory.** Working memory only, plus a short-term rolling summary, plus long-term memory across conversations (the summary needs a summariser key; long-term memory needs the *client* to say who the user is — see references/memory.md, **Who long-term memory belongs to**).
- **Source.** A local script uploaded to HopsFS, or a Git repository (branch, and whether to auto-redeploy on push).
- **Evaluation.** Whether the agent will be evaluated against a sandboxed suite while serving customers (`eval_per_request=True` and tools that check `in_evaluation()`), or in a deployment of its own (`EVAL_MODE=true`).

## Write the entry script

Build the agent with the framework of your choice and wrap it in `AgentApp` from `hopsworks_agents.protocol`. `AgentApp` is a FastAPI app that speaks **AG-UI**, the open agent–user interaction protocol, plus Hopsworks' extensions: the chat panel detects it from its manifest, `/v1/run` (AG-UI runs) and `/v1/chat` (one JSON reply) are served, health and readiness probes exist, CORS is on, and when tracing is enabled on the deployment the library wires OpenTelemetry itself. The script runs as a program in the pod, so serve the app on port 8080 in its main block.

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


@agent_app.stream                          # one handler serves /v1/run and /v1/chat
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
- **Memory without code:** a deployment can carry the policy instead — the form's Agent memory card, or `deploy_agent(memory=DeploymentMemoryConfig(summarize={"provider": "hopsworks", "deployment": "my-llm"}, long_term=True))`, injected as `AGENT_MEMORY_CONFIG`. `AgentApp` applies it only when the agent passed no `memory=`, so code wins; `capabilities.custom.hopsworks.memory.source` says which it was. The handler still has to read memory, so `ctx.history` and `ctx.system_context()` stay in the prompt either way.
- **Memory:** `memory=ManagedMemoryService(summarize=anthropic_summarizer(), long_term=True)` gives all three tiers with no storage to set up — working memory (the turns in context), short-term memory (older turns folded into a rolling summary) and long-term memory, both semantic (what the agent decides to remember, through `remember`/`recall`/`forget`) and episodic (`search` over past conversations); read it with `ctx.system_context()` and `ctx.history`, and read or write your own facts with `ctx.recall(key)` / `ctx.remember(key, value, scope="user"|"session")` / `ctx.forget(key)`, which key each scope the way `ctx.state(scope)` reads it (`app` scope is agent-wide and not a turn's to write). Match the summarizer to the key the agent already has: `openai_summarizer()` for OpenAI, `openai_summarizer(model, base_url=...)` for a vLLM/Ollama/LiteLLM-proxy endpoint, `anthropic_summarizer()` for Claude (which summarizes through Claude Code by itself on a deployment that holds the connected Claude Code login and no key; `claude_agents_summarizer()` names that path), `hopsworks_summarizer("<deployment name>")` for an LLM deployed in the same project (no key to configure); any `(previous_summary, turns) -> str` callable also works.
- **Embedding models never download in a pod.** Register once where there is internet with `register_sentence_transformer("all-MiniLM-L6-v2")` (a notebook or the feature pipeline that embeds the data) and load with `load_sentence_transformer("all-MiniLM-L6-v2")` in the agent; `sentence_transformer_embedder()` does the same for memory search. An unregistered model falls back to the hub with a warning; `fallback=False` forbids it.
- **Evaluation-safe writes:** an agent that writes (refunds, tickets) checks `hopsworks_agents.protocol.evaluation.in_evaluation()` in its tools and skips the real write, so a sandboxed suite can run against the production deployment (`eval_per_request=True` on the app). A deployment that exists only to be evaluated sets `EVAL_MODE=true` instead.

## Ask a person before acting

For an action that needs a person's approval (a refund, a deletion, sending an email), call `answer = ctx.interrupt("Refund 40 EUR?", key="refund-42", response_schema={...})` before the action. The run pauses with an AG-UI interrupt; the chat panel shows the question and the person's answer resumes the turn, which runs again from the start and gets `answer` (`answer.cancelled`, `answer.payload`). Everything before the call runs twice, so it must not act. In evaluation, the task context's `interrupt_answers` (question id to payload) answers on the trial's behalf; unanswered questions are cancelled. See the protocol README's human-in-the-loop section.

Long-term memory and whose it is (the request's subject, `rebind_subject`, why `identify` does not bind, `has_subject`), reading back what an agent remembers and searching its memory: `hops skills info hops-agent-deployment references/memory.md`. Streaming, multimodal content, progress events and the structure graph: the protocol README, `python/hopsworks_agents/protocol/README.md`.

## Deploy — CLI (preferred for local sources)

```bash
hops agent create my_agent.py --name my_agent \
  --requirements requirements.txt --environment my_agent \
  --framework langgraph --chat-contract hopsworks [--memory 3072]
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

Prices change, so write them where the model is chosen and say when they were taken. A deployment whose model changed and whose prices did not is worse than an unpriced one: it reports a number that is confidently wrong.

Checking the price against the model a real trace shows, and where the cost shows on a trace, a run and a window: `hops skills info hops-agent-deployment references/pricing.md`.

**A trace is not readable the instant the reply is.** The spans are exported after the answer and land over a second or two, so a read straight after `chat()` either raises `AgentServingError: Trace not found` or — worse — returns the trace **without its model calls**: no LLM span, zero tokens, zero cost, a turn that reads as free. Ask it to wait for the turn to close:

```python
trace = agent.trace(reply.trace_id, wait=10)   # until the turn span has landed, or 10 s
```

It polls until the turn span carries its response id, which the agent SDK stamps last. An agent not built on the Hopsworks agent SDK never stamps it, so `wait` costs it the full time.

Every chat answers 500, or a start takes long or prints no logs: `hops skills info hops-agent-deployment references/troubleshooting.md`.

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
    framework="langgraph",               # same value as AgentApp(framework=...)
    chat_config="hopsworks",             # AgentApp speaks the Hopsworks agent protocol
    tracing={"enabled": True},           # spans for evals, feedback and failure analysis
)
agent.start(await_running=600)
# After editing the code: deploy_agent again, then agent.restart()
```

### Framework and chat contract

Every agent deployment carries both in its config, the same fields as the **Agent framework** and **Chat contract** of the UI's deployment form.
Always set them:

- **`framework`** is injected into the pod as `AGENT_FRAMEWORK`.
  It also picks the model provider of the agent's failure analysis (OpenAI for `openai_agents`, Anthropic for `claude_agents`), and keeps the Claude login token away from agents that are not built on the Claude SDK.
  A deployment without one shows as **Custom** in the UI.
- **`chat_config`** tells the chat panel and chat clients how to talk to the agent.
  Use `"hopsworks"` for an `AgentApp` agent, `"openai"` for an OpenAI-compatible server, or a dict such as `{"protocol": "simple", "path": "/chat", "message_field_in": "body", "message_field_name": "message"}` for a custom endpoint.
  Without one, the panel detects the contract from the running agent.

From a local script or package, `deploy_agent` reads both from the code when you omit them.
The framework comes from a string literal in `AgentApp(framework="...")`; a name the platform does not know is recorded as `custom`, and calls that name different frameworks record none.
A new agent that builds an `AgentApp` gets the `"hopsworks"` contract.
**A git-backed agent is not read, so pass both explicitly.**
On a redeploy that omits them, the framework is the code's literal when it has one and the stored value otherwise, and the chat contract is kept.
So a framework changed in the UI is replaced on the next bare redeploy when the code names a different one: change it in the code, or pass `framework=`.
Check with `hops agent info my_agent` (the **Framework** and **Chat contract** rows) or `agent.deployment.predictor.agent_framework` / `.chat_config`.

`deploy_agent` here is model serving's call (`project.get_model_serving().deploy_agent(...)` takes the same arguments) returning an `Agent`. `agent.deployment` is the `hsml` deployment for resources, scaling and describe.

An agent scales like a model deployment: pass `scaling_configuration=PredictorScalingConfig(...)` to `deploy_agent` (instances, the metric and its target; `min_instances=0` rests at zero when idle and wakes on the first request, the SDK's default minimum being 1). The choices and their rules are in [hops-autoscaling](../../scaling/hops-autoscaling/SKILL.md).

Source in a Git repository (cloned on each start, auto-redeploy on push): `hops skills info hops-agent-deployment references/git.md`.

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

**A turn the application answers itself has no model call.** When the app replies without running the model (an agent that asks who you are before doing anything else, say), the trace is one agent span: no LLM span, zero tokens, no cost, and `trace(wait=)` returns as soon as the turn closes. `wait` also covers the trace reaching the tracing store, which can take a few seconds after the reply on a busy cluster.

Messages go through the cluster's inference gateway, the route the chat panel uses; the logged-in user's credential is accepted there. From outside Hopsworks, log in with an API key.

**A ready-made chat app for people outside Hopsworks**: `app = agent.create_chat_app()` (or `deploy_agent(..., chat_app=True)`, or "Create chat app" on the deployment page) creates and starts a Hopsworks app named `<agent>_chat` that speaks AG-UI to the agent: streaming, tool calls, attachments, the agent's questions, and thumbs up or down. Members open it with their Hopsworks login and chat as their Hopsworks user (the same subject, and memory, as the chat panel); turning on the app's public link shares it, and public visitors are anonymous with their own memory. The app sets the subject either way, and no serving key reaches the browser. Its thumbs up and down reach the Feedback tab marked "via chat app" (`Feedback.given_via == "chat-app"`). App variables change it: `CHAT_TITLE`, `CHAT_WELCOME`, `CHAT_SHOW_TOOL_CALLS`, `CHAT_ACCENT_COLOR`, `CHAT_LOGO_URL`, and `CHAT_THEME_DIR`, a project folder such as `Resources/chat-theme` with a `theme.css` and a `logo.svg`/`.png`. `agent.chat_app()` returns it, and it is deleted with the agent. To change more than the variables allow (its page, layout, components, routes of its own), `app = agent.customize_chat_app()` runs it from the project folder `Resources/chat-apps/<app>`: the app copies its template there (a `server.mjs` on `@hopsworks/agent-chat`, a React page in `web/`, a `theme.css`) and runs it; read that folder's `AGENTS.md` before changing anything, keep the chat's identity and safety to the package as it says, and `app.redeploy()` then `app.get_logs()` to try a change (a folder that fails to build falls back to the default page, logged).

`manifest()`'s shape, tool events by id and their two shapes, reading a conversation back, and a custom chat UI over `agent.url`: `hops skills info hops-agent-deployment references/chat-client.md`.

## Next Steps

- Evaluate, monitor and improve the deployed agent: **hops-agent-evals** (suites, online sampling, feedback, failure analysis).
- Scheduled/batch coding agent instead of a served one: **hops-agent-task**.
- Model-backed online predictor: **hops-online-inference**.
- Instances, scale metric, scale to zero when idle: [hops-autoscaling](../../scaling/hops-autoscaling/SKILL.md).
- Agent serving dependencies: [hops-environments](../../platform/hops-environments/SKILL.md) — clone `python-agent-pipeline` and install requirements.
- Give the agent feature-store access for RAG: **hops-fv** (online feature vectors). Pass entity IDs (e.g. `user_id`) in the query so the agent can look up application state from the feature store.
- Agent memory or app state in the project database (`MYSQL_*` variables, password secret, privileges, RonDB table rules): **hops-app-db**.

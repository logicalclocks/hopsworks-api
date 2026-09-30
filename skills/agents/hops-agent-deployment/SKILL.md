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
hops agent logs my_agent         # startup errors land here
```

## Ask the user (only when state is ambiguous)
- **Framework.** `langgraph`, `llamaindex`, `openai_agents`, `claude_agents` or `custom`: picks the tracing instrumentation.
- **Memory.** Conversation buffer only, a rolling summary, or durable per-user memory (needs a summariser key and, for durable memory, the agent to know who the user is).
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
- **Memory:** `memory=ManagedMemoryService(summarize=anthropic_summarizer(), long_term=True)` gives a conversation buffer, a rolling summary and durable per-user memory with no storage to set up; read it with `ctx.system_context()` and `ctx.history`. Match the summarizer to the key the agent already has: `openai_summarizer()` for OpenAI, `openai_summarizer(model, base_url=...)` for a vLLM/Ollama/LiteLLM-proxy endpoint, `anthropic_summarizer()` for Claude; any `(previous_summary, turns) -> str` callable also works.
- **Evaluation-safe writes:** an agent that writes (refunds, tickets) checks `hopsworks_agents.protocol.evaluation.in_evaluation()` in its tools and skips the real write, so a sandboxed suite can run against the production deployment (`eval_per_request=True` on the app). A deployment that exists only to be evaluated sets `EVAL_MODE=true` instead.

Streaming, multimodal content, memory tiers, progress events and the structure graph are in the protocol README: `python/hopsworks_agents.protocol/README.md`.

## Deploy — CLI (preferred for local sources)

```bash
hops agent create my_agent.py --name my_agent \
  --requirements requirements.txt --environment my_agent
hops agent start my_agent                       # waits for RUNNING
hops agent info my_agent                        # status + URL
hops agent logs my_agent                        # follow startup / errors
hops agent stop my_agent
hops agent delete my_agent --yes
```

`create` re-run uploads the latest code and rewrites the predictor; a running agent is left untouched (`start`, or `restart` from Python, rolls it onto the new code). `hops agent query` posts a predictor-style body and is for the legacy `Predict` class contract; a protocol agent is spoken to with `agent.chat()` below or the UI chat panel.

**Confirm before deleting.** `hops agent delete` tears down the served agent irreversibly; confirm the exact name with the user, and never tear down an agent you created as a side effect (temp or test ones included) unless they asked.

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
- Agent serving dependencies: [hops-environments](../../platform/hops-environments/SKILL.md) — clone `python-agent-pipeline` and install requirements.
- Give the agent feature-store access for RAG: **hops-fv** (online feature vectors). Pass entity IDs (e.g. `user_id`) in the query so the agent can look up application state from the feature store.

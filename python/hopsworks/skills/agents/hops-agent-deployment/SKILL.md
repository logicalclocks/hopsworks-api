---
name: hops-agent-deployment
description: Use when writing and deploying an interactive agent (e.g. a LlamaIndex program) as a served Hopsworks deployment.
---

# Hopsworks Agent Deployments

An agent deployment is a **server-only KServe deployment with no model attached** — you ship an entry script (or a package) that handles requests. Use it for interactive agents and LLM workflows (LlamaIndex, custom LLM orchestration). The agent *is* the **inference pipeline** of the AI system: it usually skips the training pipeline and calls a foundation LLM, and if it needs RAG it reads context from the feature store (write the RAG features in a separate **feature pipeline** — see hops-features). Agents can be created from HopsFS or from GitHub/Git repositories, just like apps. For a scheduled, non-interactive coding agent, use **hops-agent-task** instead; for a model-backed predictor, use **hops-online-inference**.

Start with a deterministic **LLM workflow** (a fixed sequence of steps) and only graduate to an autonomous agent when the task is open-ended enough to require runtime planning over tools. Workflows are cheaper, lower-latency, and easier to make reliable.

## Contract
- **Input:** an entry script (a `.py` file, or a directory containing a `pyproject.toml`), either from HopsFS or from a Git repository.
- **Output:** a served agent deployment (server-only KServe deployment, queryable endpoint).
- **Pre-condition:** auth + serving reachable; the agent name and environment are valid (`[A-Za-z0-9_-]+`).

## Smoke-test (cheap pre/post-flight)

```bash
hops agent list          # what agents already exist (confirms auth + serving reachable)
```

## Write the entry script

The deployment runs the script with `python`, so **the script is the server**: it
serves HTTP on port 8080 and keeps running. A script that only defines a class
exits, and the deployment restarts it forever. Serve three routes:

- `POST /predict`: where the Hopsworks inference endpoint forwards a request;
- `POST /v1/models/<name>:predict`: KServe's path, which `deployment.predict` and
  `hops agent query` use from inside the cluster, with the colon percent-encoded
  (`%3A`), so match `/v1/models/{target}` rather than a literal `:predict`;
- `GET /`: the readiness probe.

The SDK sends `{"instances": [...]}`; take the first instance, and accept a bare
body too. Build the LLM clients, indexes and models before the server starts, so
the deployment turns ready only once it can answer.

**Log every step's inputs and outputs** (the user query, each RAG/tool call with its response, each LLM prompt and reply, and the final response). These traces are what you use later for error analysis, evals, and monitoring of the deployed agent. An agent built without trace logging cannot be debugged or improved.

```python
# my_agent.py
from fastapi import FastAPI, HTTPException
import uvicorn


class Agent:
    def __init__(self):
        ...  # the LLM client, the index: once

    def predict(self, body: dict) -> dict:
        request = body.get("instances", [body])[0]
        return {"answer": ...}


def build_app(agent: Agent) -> FastAPI:
    app = FastAPI()

    @app.get("/")
    def ready() -> dict:
        return {"status": "ok"}

    @app.post("/predict")
    def predict(payload: dict) -> dict:
        return agent.predict(payload)

    @app.post("/v1/models/{target}")
    def kserve_predict(target: str, payload: dict) -> dict:
        if not target.endswith(":predict"):
            raise HTTPException(status_code=404)
        return agent.predict(payload)

    return app


if __name__ == "__main__":
    uvicorn.run(build_app(Agent()), host="0.0.0.0", port=8080)
```

A model loaded in the agent (torch, an embedder) needs more than the default 1 GB
of memory: pass `--memory` to `hops agent create`, or it is OOM-killed while it
loads. The help desk example (`hops-reqs/references/rag_agent/agent.py`) is a
complete LangGraph agent built this way.

### Traces: what fills the deployment's Metrics and Traces panels

Hopsworks runs a trace collector beside the agent and sets
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` in the pod; it counts OpenInference spans,
LLM calls by `openinference.span.kind = LLM` and tools by `TOOL`. An agent that
exports no spans leaves both panels empty however many requests it answers. Install
`opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http` (pinned to the
`opentelemetry-api` the base ships) and the OpenInference instrumentor for the
framework (`openinference-instrumentation-langchain` covers LangChain and LangGraph,
`openinference-instrumentation-openai` the OpenAI client), export before the server
starts, and mark each step that is not an instrumented call, a feature lookup or a
vector search, as a TOOL span:

```python
provider = TracerProvider(resource=Resource.create({"service.name": "<name>"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(
    endpoint=os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"])))
trace.set_tracer_provider(provider)
LangChainInstrumentor().instrument(tracer_provider=provider)

with trace.get_tracer("<name>").start_as_current_span("retrieve", attributes={
        "openinference.span.kind": "TOOL", "tool.name": "retrieve",
        "input.value": json.dumps(query)}) as span:
    hits = fg.find_neighbors(vector, k=k)
    span.set_attribute("output.value", json.dumps(hits, default=str))
```

`setup_tracing` and `tool_span` in the help desk example do this, and run untraced
when the endpoint or the libraries are absent.

## Deploy — CLI (preferred)

Use the CLI for local HopsFS sources. Git-backed agents are supported too, but
they are created through the SDK example below with `git_url`, `git_provider`, and `git_branch`.
Add `git_auto_redeploy=True` to roll the agent to the branch HEAD whenever a new commit is pushed.

`hops agent info` shows the git source, the branch (the resolved default when none was configured), and the commit the deployment is running.
The same values are on the predictor: `git_current_commit` and `git_resolved_branch`, both read-only.

```bash
hops agent create my_agent.py --name my_agent \
  --requirements requirements.txt --environment my_agent [--memory 3072]
hops agent start my_agent                       # waits for RUNNING
hops agent query my_agent --data '{"prompt": "hello"}'
hops agent logs my_agent                        # follow startup / errors
hops agent info my_agent                        # status + URL
hops agent stop my_agent
hops agent delete my_agent --yes
```

**Confirm before deleting.** `hops agent delete` tears down the served agent irreversibly; confirm the exact name with the user, and never tear down an agent you created as a side effect (temp or test ones included) unless they asked.

`create` re-run uploads the latest code and rewrites the predictor; a running
agent is left untouched (use `start`, or `restart` via the SDK, to roll onto
new code). For Git-backed agents, use the SDK example below so the repository
is cloned on each start.

## Deploy — SDK

```python
import hopsworks

project = hopsworks.login()
ms = project.get_model_serving()

deployment = ms.deploy_agent(
    entry="my_agent.py",                 # .py file or a dir with pyproject.toml
    name="my_agent",
    requirements="requirements.txt",
    environment="my_agent",
    upload_dir="Resources/agents",       # default
)
deployment.start(await_running=600)
print(deployment.predict(inputs={"prompt": "hello"}))
# After editing the code: re-create, then deployment.restart()
```

### Git-backed Agents

When the agent source lives in Git, provide the repository fields instead of a HopsFS path. Git-backed agents are cloned again on each start, so a restart or redeploy picks up new commits.

- Supported Git providers: `GitHub`, `GitLab`, and `BitBucket`.
- Use the repository root or a repo-relative path for the entry script.

```python
deployment = ms.deploy_agent(
    entry="agent.py",
    name="my_agent",
    git_url="https://github.com/gibchikafa/my-agent-repo.git",
    git_provider="GitHub",
    git_branch="main",
    git_auto_redeploy=True,  # roll to branch HEAD on every new commit
    environment="my_agent",
)
```

## Next Steps

- Scheduled/batch coding agent instead of a served one: **hops-agent-task**.
- Model-backed online predictor: **hops-online-inference**.
- Agent serving dependencies: [hops-environments](../../platform/hops-environments/SKILL.md) — clone an agent env and install requirements.
- Give the agent feature-store access for RAG: **hops-fv** (online feature vectors). Pass entity IDs (e.g. `user_id`) in the query so the agent can look up application state from the feature store.
- Agent memory or app state in the project database (`MYSQL_*` variables, password secret, privileges, RonDB table rules): **hops-app-db**.

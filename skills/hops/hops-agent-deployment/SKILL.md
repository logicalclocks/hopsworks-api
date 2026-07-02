---
name: hops-agent-deployment
description: Use when writing and deploying an interactive agent (e.g. a LlamaIndex program) as a served Hopsworks deployment.
---

# Hopsworks Agent Deployments

An agent deployment is a **server-only KServe deployment with no model attached** — you ship an entry script (or a package) that handles requests. Use it for interactive agents and LLM workflows (LlamaIndex, custom LLM orchestration). The agent *is* the **inference pipeline** of the AI system: it usually skips the training pipeline and calls a foundation LLM, and if it needs RAG it reads context from the feature store (write the RAG features in a separate **feature pipeline** — see hops-features). Agents can be created from HopsFS or from GitHub/Git repositories, just like apps. For a scheduled, non-interactive coding agent, use **hops-agent-job** instead; for a model-backed predictor, use **hops-online-inference**.

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

The script exposes a class with `predict` (and optional `init`) — same predictor contract as a model deployment, but no model is loaded. Keep heavy setup (LLM clients, indexes) in `__init__` so it runs once.

**Log every step's inputs and outputs** (the user query, each RAG/tool call with its response, each LLM prompt and reply, and the final response). These traces are what you use later for error analysis, evals, and monitoring of the deployed agent. An agent built without trace logging cannot be debugged or improved.

```python
# my_agent.py
class Predict:
    def __init__(self):
        # build the LlamaIndex query engine / LLM client once
        ...

    def predict(self, inputs):
        prompt = inputs.get("prompt", "")
        return {"answer": self._engine.query(prompt).response}
```

## Deploy — CLI (preferred)

Use the CLI for local HopsFS sources. Git-backed agents are supported too, but
they are created through the SDK example below with `git_url`,
`git_provider`, and `git_branch`.

```bash
hops agent create my_agent.py --name my_agent \
  --requirements requirements.txt --environment my_agent
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
    environment="my_agent",
)
```

## Next Steps

- Scheduled/batch coding agent instead of a served one: **hops-agent-job**.
- Model-backed online predictor: **hops-online-inference**.
- Agent serving dependencies: [hops-environments](../hops-environments/SKILL.md) — clone an agent env and install requirements.
- Give the agent feature-store access for RAG: **hops-fv** (online feature vectors). Pass entity IDs (e.g. `user_id`) in the query so the agent can look up application state from the feature store.

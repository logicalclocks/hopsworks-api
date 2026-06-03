---
name: hops-agent-deployment
description: Use when writing and deploying an interactive agent (e.g. a LlamaIndex program) as a served Hopsworks deployment.
---

# Hopsworks Agent Deployments

An agent deployment is a **server-only KServe deployment with no model attached** — you ship an entry script (or a package) that handles requests. Use it for interactive agents (LlamaIndex, custom LLM orchestration). For a scheduled, non-interactive coding agent, use **hops-agent-job** instead; for a model-backed predictor, use **hops-online-inference**.

## Contract
- **Input:** an entry script (a `.py` file, or a directory containing a `pyproject.toml`).
- **Output:** a served agent deployment (server-only KServe deployment, queryable endpoint).
- **Pre-condition:** auth + serving reachable; the agent name and environment are valid (`[A-Za-z0-9_-]+`).

## Smoke-test (cheap pre/post-flight)

```bash
hops agent list          # what agents already exist (confirms auth + serving reachable)
```

## Write the entry script

The script exposes a class with `predict` (and optional `init`) — same predictor contract as a model deployment, but no model is loaded. Keep heavy setup (LLM clients, indexes) in `__init__` so it runs once.

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

`create` re-run uploads the latest code and rewrites the predictor; a running agent is left untouched (use `start`, or `restart` via the SDK, to roll onto new code).

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

## Next Steps

- Scheduled/batch coding agent instead of a served one: **hops-agent-job**.
- Model-backed online predictor: **hops-online-inference**.
- Give the agent feature-store access: **hops-fv** (online feature vectors).

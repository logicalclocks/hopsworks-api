---
name: hops-environments
description: Use when a Hopsworks job, app, or deployment needs Python libraries that are not in a base environment. Clone a base environment and install requirements or a wheel into the clone. Auto-invoke when the user hits a missing-package error in a job/app/deployment, asks to install custom dependencies, add a pip requirement, install a wheel, or pick which Python environment a workload should run in.
---

# Hopsworks Python Environments

Every job, app, and deployment runs in a named Python environment. This is
Hopsworks' **automatic containerization**: you pick a base, add libraries, and
the backend builds and registers the container for you — no Dockerfile, no
registry to manage. The base environments are read-only managed images; to add
libraries you **clone a base, then install into the clone**, and point the
workload at the clone.

## Contract
- **Input:** a base environment name + a `requirements.txt` (or a `.whl`).
- **Output:** a cloned environment with the dependencies installed.
- **Pre-condition:** the requirements file / wheel is uploaded to the project
  (a Hopsworks path like `Users/<username>/app-requirements.txt`), or local for
  the CLI to upload.

## Smoke-test (cheap pre/post-flight)
```bash
hops env list                                   # base + cloned environments
hops env clone my_env --from python-feature-pipeline   # blocks minutes; provisions the clone
hops env install my_env -f requirements.txt     # blocks minutes; resolves + commits libs
```

## Base environments (pick by workload)
One per workload; all Python 3.12. Each base matches an FTI pipeline stage
(feature / training / inference) or a serving workload. Clone the one matching
the workload, then add libs.
| Workload | Base environment |
|---|---|
| Feature pipeline (Python) | `python-feature-pipeline` |
| Feature pipeline (Spark) | `spark-feature-pipeline` |
| Training | `pandas-training-pipeline`, `torch-training-pipeline`, `tensorflow-training-pipeline`, `ray-training-pipeline` |
| Inference / deployment | `pandas-inference-pipeline`, `torch-inference-pipeline`, `tensorflow-inference-pipeline`, `minimal-inference-pipeline` |
| Streamlit / Custom app | `python-app-pipeline` |
| Agent job / deployment | `agent-job`, `python-agent-pipeline` |

`hops env list` shows the live set for the project — it can differ from this table.

## Clone-then-install (the core workflow)
Both steps **block for several minutes** (the backend builds the image from the base
and runs the install). Warn the user before starting.
```python
import hopsworks
project = hopsworks.login()
env_api = project.get_environment_api()

# 1. Clone a base into a new named environment
cloned = env_api.create_environment(
    "my_app_env",
    description="app env + custom libs",
    base_environment_name="python-app-pipeline",  # default: python-feature-pipeline
    await_creation=True,                           # return only when provisioned
)

# 2. Install dependencies into the CLONE (Hopsworks path to an uploaded file)
cloned.install_requirements("Users/<username>/app-requirements.txt", await_installation=True)
# or a wheel:
cloned.install_wheel("Users/<username>/my_pkg-0.1.0-py3-none-any.whl")
```
Then attach `my_app_env` when creating the job / app / deployment.

## Manage
```python
env_api.get_environments()                 # list
env_api.get_environment("my_app_env")      # None if it does not exist
env = env_api.get_environment("my_app_env")
env.uninstall("some-package")              # remove a library
env.delete()                               # remove the environment
```

## Ask the user (only when state is ambiguous)
- Which base matches the workload (training vs inference vs app vs agent)?
- Is there a `requirements.txt` already? If not, prompt for one before cloning —
  a clone with nothing to install is wasted minutes.
- **Before deleting** — `env.delete()` is irreversible; confirm with the user, and
  never delete an environment a job, app, or deployment still references.

## Caveats
- **Clone and install each block for minutes.** Use `await_creation=False` /
  `await_installation=False` to fire-and-forget, but then you must poll before the
  workload can use the env.
- **Install into the clone, never the base** — bases are managed/read-only.
- The requirements path is a **project (Hopsworks) path**, not a local one, for the
  SDK. The CLI `-f` flag uploads a local file for you.
- **MLOps:** prefer code (SDK/CLI) over the UI so env setup is reproducible across
  dev/staging/prod, and name the clone after the pipeline version it serves (e.g.
  `spark-feature-pipeline-v1`) so a pinned env travels with the code version.

## Toolset
- **CLI:** `hops env list`, `hops env clone <new> --from <base> [--description]`, `hops env install <env> -f <requirements.txt>`.
- **SDK:** `project.get_environment_api()` → `create_environment(base_environment_name=, await_creation=)`, `env.install_requirements()`, `env.install_wheel()`, `env.uninstall()`, `env.delete()`.
- **Source:** `python/hopsworks_common/core/environment_api.py`, `python/hopsworks_common/environment.py`, `python/hopsworks/cli/commands/env.py`.

## Next steps
- [hops-job](../hops-job/SKILL.md) — run a job in the cloned environment.
- [hops-app](../hops-app/SKILL.md) — Streamlit app on a cloned `python-app-pipeline`.
- [hops-online-inference](../hops-online-inference/SKILL.md) — deployment env for predictor dependencies.
- [hops-agent-deployment](../hops-agent-deployment/SKILL.md) — agent serving env.

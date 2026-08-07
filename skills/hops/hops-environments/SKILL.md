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
- **Pre-condition:** the requirements file / wheel **already exists inside the
  project filesystem** at a project-relative path such as
  `Users/<username>/app-requirements.txt`. Neither the SDK nor the CLI uploads
  it for you — see *Requirements-file paths* below. Writing the file into the
  project FUSE mount (`/hopsfs/...`) is how you put it there.

## Requirements-file paths (read this before running `install`)

**The path is always a project path, for both the SDK and the CLI.** The CLI's
`-f/--file` flag does **not** upload a local file; `hops env install` passes the
string straight to `env.install_requirements()`, and the backend resolves it
against the project root `/Projects/<project>/`. Getting this wrong is the most
common failure:

```
Error: Install failed: ... HTTP code: 404 ...
{"errorCode":110008,"usrMsg":"path: /Projects/<project>/requirements.txt",
 "errorMsg":"File not found."}
```

That means the backend looked at the project root and found nothing — the file
existed only on the local disk.

There is a second trap layered on top: the CLI declares `-f` as
`click.Path(exists=True)`, so the path must **also** resolve locally from the
current working directory. A bare project path fails Click's check before any
request is sent:

```
Error: Invalid value for '-f' / '--file': File 'Users/me/requirements.txt' does not exist.
```

**Both constraints are satisfied at once by writing the file into the project
FUSE mount and invoking the CLI from the project root**, so that one relative
path is simultaneously a valid local path and the correct project path:

```bash
# /hopsfs is the FUSE mount of the project root (/Projects/<project>).
# Confirm with: ls /hopsfs   ->   Users/ Models/ Resources/ Jupyter/ Logs/ ...
printf 'ibis-framework\n' > /hopsfs/Users/<username>/requirements.txt

cd /hopsfs                                    # cwd == project root
hops env install my_env -f Users/<username>/requirements.txt
```

Rule of thumb: **`cd` to the project root mount first, then pass a
project-relative path.** Absolute local paths (`/tmp/...`, or even
`/hopsfs/Users/...`) pass Click's check and then 404 on the backend, because the
backend prepends the project root to whatever string it receives.

With the SDK there is no local-existence check, so only the project path matters:

```python
env.install_requirements("Users/<username>/requirements.txt", await_installation=True)
```

If you cannot write to a FUSE mount (no `/hopsfs` in this context), upload the
file explicitly first and then pass the path the upload returns:

```python
ds = project.get_dataset_api()
ds.upload("requirements.txt", "Users/<username>", overwrite=True)   # -> Users/<username>/requirements.txt
env.install_requirements("Users/<username>/requirements.txt", await_installation=True)
```

## Smoke-test (cheap pre/post-flight)
```bash
hops env list                                          # base + cloned environments
hops env clone my_env --from pandas-training-pipeline  # provisions the clone
cd /hopsfs && hops env install my_env -f Users/<username>/requirements.txt
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
Both steps block until the backend finishes. Duration varies with the base image
and how heavy the dependency set is: a small pure-Python install can complete in
under a minute, while a large one (torch, CUDA wheels, conflicting pins) can run
for many minutes. Tell the user it may take several minutes before you start,
and prefer running it in the background rather than blocking the session.

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

# 2. Install into the CLONE — path is a PROJECT path (see above)
cloned.install_requirements("Users/<username>/app-requirements.txt", await_installation=True)
# or a wheel — same project-path rule:
cloned.install_wheel("Users/<username>/my_pkg-0.1.0-py3-none-any.whl")
```
Then attach `my_app_env` when creating the job / app / deployment.

## Verifying an install
`await_installation=True` returning without raising **is** the success signal —
the backend only reports completion once the libraries are committed to the
image. The SDK's `Environment` object exposes no library-listing method
(`get_libraries()` does not exist), so do not try to enumerate packages that way.
To actually confirm a package imports, run a one-line job/app in the cloned
environment, or check the environment's library list in the Hopsworks UI under
*Project Settings → Python Environments*.

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
  a clone with nothing to install is wasted time.
- **Ambiguous PyPI names.** Confirm the distribution name before installing when
  the import name and the PyPI name differ — e.g. Ibis is `ibis-framework` (plain
  `ibis` is an unrelated legacy package), and backends are extras
  (`ibis-framework[duckdb]`). Same class of trap: `sklearn` → `scikit-learn`,
  `cv2` → `opencv-python`, `PIL` → `pillow`.
- **Before deleting** — `env.delete()` is irreversible; confirm with the user, and
  never delete an environment a job, app, or deployment still references.

## Caveats
- **The requirements path is a project path for BOTH the SDK and the CLI.** The
  CLI does not upload; it only adds a local-existence check on top. `cd /hopsfs`
  first and pass a project-relative path. This is the single most common failure
  mode — see *Requirements-file paths*.
- **Clone and install block.** Use `await_creation=False` /
  `await_installation=False` to fire-and-forget, but then you must poll before the
  workload can use the env.
- **Install into the clone, never the base** — bases are managed/read-only.
- **MLOps:** prefer code (SDK/CLI) over the UI so env setup is reproducible across
  dev/staging/prod, and name the clone after the pipeline version it serves (e.g.
  `spark-feature-pipeline-v1`) so a pinned env travels with the code version.

## Toolset
- **CLI:** `hops env list`, `hops env clone <new> --from <base> [--description]`, `hops env install <env> -f <project-relative-requirements.txt>` (run from `/hopsfs`).
- **SDK:** `project.get_environment_api()` → `create_environment(base_environment_name=, await_creation=)`, `env.install_requirements()`, `env.install_wheel()`, `env.uninstall()`, `env.delete()`.
- **Source:** `python/hopsworks_common/core/environment_api.py`, `python/hopsworks_common/environment.py`, `python/hopsworks/cli/commands/env.py` (see `env_install` — it forwards the path unchanged, confirming the no-upload behaviour).

## Next steps
- [hops-job](../hops-job/SKILL.md) — run a job in the cloned environment.
- [hops-app](../hops-app/SKILL.md) — Streamlit app on a cloned `python-app-pipeline`.
- [hops-online-inference](../hops-online-inference/SKILL.md) — deployment env for predictor dependencies.
- [hops-agent-deployment](../hops-agent-deployment/SKILL.md) — agent serving env.

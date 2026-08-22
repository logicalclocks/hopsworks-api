---
name: hops-environments
description: Use when a Hopsworks job, app, or deployment needs Python libraries or npm packages that are not in a base environment. Clone a base environment and install requirements, a wheel, or an npm package into the clone. Auto-invoke when the user hits a missing-package error in a job/app/deployment, asks to install custom dependencies, add a pip requirement, install a wheel, install an npm package or CLI tool, or pick which Python environment a workload should run in.
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
- **Pre-condition (SDK only):** the requirements file / wheel **already exists
  inside the project filesystem** at a project-relative path such as
  `Users/<username>/app-requirements.txt`. The SDK does not upload it for you —
  see *Requirements-file paths* below. The CLI does: `hops env install -f` takes
  a local file and uploads it before installing.

## Requirements-file paths (read this before running `install`)

**The CLI uploads; the SDK does not.** `hops env install <env> -f <file>`
accepts either a local file, which it uploads to
`Resources/environments/<env>/` first (`--upload-dir` overrides the
destination), or a path that already exists in the project:

```bash
printf 'ibis-framework\n' > requirements.txt
hops env install my_env -f requirements.txt          # local file: uploaded, then installed
hops env install my_env -f Users/me/requirements.txt # no such local file: passed through as a project path
```

The passthrough rule is `os.path.isfile`: a string that names a local file is
uploaded, anything else is handed to the backend as a project path. So run the
CLI from a directory where the string means what you intend — a local
`Users/me/requirements.txt` relative to the cwd would be uploaded rather than
treated as the project path of the same name.

**The SDK takes project paths only.** `env.install_requirements()` forwards the
string to the backend, which resolves it against the project root
`/Projects/<project>/`. A local-only path fails with:

```
Error: Install failed: ... HTTP code: 404 ...
{"errorCode":110008,"usrMsg":"path: /Projects/<project>/requirements.txt",
 "errorMsg":"File not found."}
```

That means the backend looked at the project root and found nothing — the file
existed only on the local disk. Writing the file into the project FUSE mount
(`/hopsfs/...`, where mounted) is one way to put it there:

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
hops env install my_env -f requirements.txt            # uploads the local file, then installs
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

## npm packages (requires Hopsworks 5.1+)

Environments also carry npm packages, installed globally in the image so they
are on PATH for jobs, Jupyter and the terminal — the way to get a CLI tool or a
JS dependency into a workload. Same rules as Python libraries: install into a
**clone**, never a base, and the backend queues a build that applies it.

The SDK has no npm method yet; the install goes through the same library
endpoint the SDK uses internally, with `packageSource: "NPM"`:

```python
import json
import time
import hopsworks
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError

project = hopsworks.login()
_client = client._get_instance()

def _npm_path(env_name, package):
    # A scoped name (@scope/name) is two path segments; splitting keeps both
    # forms on the route the backend expects.
    return ["project", _client._project_id, "python", "environments",
            env_name, "libraries", *package.split("/")]

def npm_install(env_name, package, version="latest", flags=None):
    spec = {"packageSource": "NPM", "channelUrl": "npm",
            "version": version, "flags": flags or []}
    return _client._send_request(
        "POST", _npm_path(env_name, package),
        headers={"content-type": "application/json"}, data=json.dumps(spec))

def npm_uninstall(env_name, package):
    # packageSource on every name-addressed call: the same name can exist as a
    # Python package too, and the backend refuses the ambiguity otherwise.
    _client._send_request("DELETE", _npm_path(env_name, package),
                          query_params={"packageSource": "NPM"})

def npm_await(env_name, package, timeout_s=900):
    # Polls the npm row's own commands; an environment-level await returns
    # before a library build starts. The row can 404 for a moment right after
    # queueing, so not-found is retried until the deadline; anything else
    # raises immediately rather than being retried into silence.
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            row = _client._send_request(
                "GET", _npm_path(env_name, package),
                query_params={"packageSource": "NPM", "expand": "commands"})
        except RestAPIError as e:
            if e.response.status_code != 404:
                raise
            time.sleep(5)
            continue
        cmds = (row.get("commands") or {}).get("items") or []
        failed = [c for c in cmds if c.get("status") == "FAILED"]
        if failed:
            raise RuntimeError(failed[0].get("errorMessage")
                               or "npm install FAILED")
        if not cmds:
            return row   # commands swept: the install is applied
        time.sleep(5)
    raise TimeoutError(f"{package} still building after {timeout_s}s")

npm_install("my_app_env", "left-pad", version="1.3.0",
            flags=["--ignore-scripts"])
row = npm_await("my_app_env", "left-pad")
print(row["library"], row["version"])   # version is the resolved one, e.g. 1.3.0

libs = _client._send_request(
    "GET", ["project", _client._project_id, "python", "environments",
            "my_app_env", "libraries"], query_params={"limit": 500})
[(i["library"], i["version"]) for i in libs["items"]
 if i.get("packageSource") == "NPM"]
```

Rules the backend enforces:
- **Versions are exact (`1.3.0`) or a dist-tag (`latest`).** Ranges (`^1.0.0`)
  are refused; they are not reproducible.
- **Flags come from a closed allowlist:** `--ignore-scripts`,
  `--legacy-peer-deps`, `--no-audit`, `--no-fund`, `--no-optional`,
  `--strict-peer-deps`. Anything else is refused with the list in the message.
- **A package the image already carries is refused** with "already installed";
  uninstall it first to change the version. The base image's own npm packages
  (e.g. `corepack`) cannot be uninstalled.
- The registry is the cluster's `npm_registry_url` variable (empty means
  registry.npmjs.org), applied at build time and baked into the image so the
  runtime resolves from the same registry.

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
- **The SDK takes project paths only; the CLI uploads local files.** With the
  SDK, put the file in the project first (FUSE mount or `dataset.upload`) and
  pass the project path. With the CLI, a local `-f` is uploaded to
  `Resources/environments/<env>/` and anything that is not a local file is
  passed through as a project path — see *Requirements-file paths*.
- **Clone and install block.** Use `await_creation=False` /
  `await_installation=False` to fire-and-forget, but then you must poll before the
  workload can use the env.
- **Install into the clone, never the base** — bases are managed/read-only.
- **MLOps:** prefer code (SDK/CLI) over the UI so env setup is reproducible across
  dev/staging/prod, and name the clone after the pipeline version it serves (e.g.
  `spark-feature-pipeline-v1`) so a pinned env travels with the code version.

## Toolset
- **CLI:** `hops env list`, `hops env clone <new> --from <base> [--description]`, `hops env install <env> -f <local-file-or-project-path> [--upload-dir] [--no-overwrite]`.
- **SDK:** `project.get_environment_api()` → `create_environment(base_environment_name=, await_creation=)`, `env.install_requirements()`, `env.install_wheel()`, `env.uninstall()`, `env.delete()`.
- **npm:** no SDK method yet — POST the library endpoint with `packageSource: "NPM"` as in *npm packages* above.
- **Source:** `python/hopsworks_common/core/environment_api.py`, `python/hopsworks_common/environment.py`, `python/hopsworks/cli/commands/env.py` (see `env_install` — a local `-f` is uploaded, any other string is passed through as a project path).

## Next steps
- [hops-job](../hops-job/SKILL.md) — run a job in the cloned environment.
- [hops-app](../hops-app/SKILL.md) — Streamlit app on a cloned `python-app-pipeline`.
- [hops-online-inference](../hops-online-inference/SKILL.md) — deployment env for predictor dependencies.
- [hops-agent-deployment](../hops-agent-deployment/SKILL.md) — agent serving env.

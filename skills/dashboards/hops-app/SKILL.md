---
name: hops-app
description: Use when writing Streamlit or custom Python apps for Hopsworks,
  deploying from HopsFS or Git, migrating legacy apps off `APP_BASE_URL_PATH`,
  or managing app routing/readiness, monitoring, and public sharing.
  Auto-invoke when user wants to create a dashboard, deploy a Python app to
  Hopsworks, configure app routing/readiness, or access the feature store from
  an app. Input an app source + env + memory; output a running app and its URL.
---

# Hopsworks Apps — Python SDK Best Practices

Hopsworks runs **Streamlit** and **custom Python** web apps as managed apps backed
by a Hopsworks job, sourced from HopsFS or a Git repository. A Streamlit app is a
**UI / consumer of the FTI pipelines**, not a pipeline: it reads features and
predictions written by the feature, training, and inference pipelines and presents
them, or embeds a downloaded model to predict locally. A custom app is a
general-purpose web service (FastAPI, Flask, Gradio, any WSGI/ASGI app) that binds
to `0.0.0.0` and the injected `$APP_PORT`.

## Contract

- **Input:** a Streamlit or custom app + environment + memory.
- **Output:** a running app and its URL.
- **Pre-condition:** the app source is available in HopsFS or Git (project-relative path for the SDK, HopsFS-absolute for the CLI).

## Smoke-test (cheap pre/post-flight)

```bash
hops app list                 # apps + state; verify RUNNING/serving after
hops app info <name>          # detail (id, type, source, monitoring state and routes)
hops app url <name>           # the app URL
hops app start <name> / stop <name>
hops app logs <name>          # tail logs (a running app is directed to the live logs in the UI)
hops app delete <name> --yes  # non-interactive
```

`hops app create <name> --path /Projects/<project>/Users/<user>/app.py --start`
creates, starts, and waits for serving in one call. It takes a **HopsFS-absolute**
path, while the SDK `create_app(app_path=...)` takes a **project-relative** one
(`Users/<username>/app.py`); each surface rejects the other's form. Routing and
readiness are `--app-base-path` and `--readiness-probe-path`. `logs` / `redeploy`
may be absent on older deployed `hops` binaries.

## Ask the user (only when state is ambiguous)

- Does the app need **custom libraries** not in `python-app-pipeline`? If so, clone the env and install `app-requirements.txt` (see **Custom libraries**).
- Does the app come from **HopsFS or Git**? If Git, ask for `git_url`, `git_provider`, and (if needed) `git_branch` plus the entrypoint.
- Should a git-backed app **auto-redeploy** on new commits (`git_auto_redeploy=True`)? It only applies to git-backed apps.
- What **memory / cores** should the app get? Defaults are `memory=2048` MB, `cores=1.0`.
- Does the app still depend on `APP_BASE_URL_PATH`? Then it is a **migration** task (see **Routing and readiness**); confirm whether the code can switch to root routing.
- Does the app need **monitoring** narrowed to specific routes? Monitoring is on by default and routes are optional.
- Does the app need **public access**? Streamlit sharing is feature-flagged; only ask for it when the platform has it enabled.
- **Before deleting** — `app.delete()` / `hops app delete --yes` tears down the app irreversibly; confirm the exact name with the user, and never tear down an app you created as a side effect (temp or test ones included) unless they asked.

## Routing and readiness

The browser URL is always the proxy mount point
`/hopsworks-api/pythonapp/{projectName}/{appName}/`. Hopsworks owns that prefix
and forwards requests to the app container, so **write new apps as if they run
at `/`** and do not read `APP_BASE_URL_PATH`.

- The proxy forwards the browser mount, rewrites some HTML/CSS links, and preserves redirects. It does **not** rewrite arbitrary JavaScript string literals or client-side `fetch("/api/...")` calls; build API and asset URLs from the app mount, or derive the base path from `X-Forwarded-Prefix` / framework support (Next.js: a mount-aware `basePath` / `assetPrefix`).
- Readiness is separate from routing: Streamlit probes `/_stcore/health`, custom apps probe `/`, and `readinessProbePath` overrides the probe path.

**Legacy apps** (created before the `App base path` migration, or still reading
`APP_BASE_URL_PATH`) can stay on `Compatibility prefix` only as a bridge. To
migrate: make route decorators use `/` (or the app's own subpath) instead of
`APP_BASE_URL_PATH` and build browser URLs from the mount; in the app settings
dialog set `Proxy routing mode` to `Root routing` and `App base path` to the
public mount you want (`/` or `/myapp`); keep the readiness path separate (custom
apps `/` or `/health`, Streamlit `/_stcore/health` or `/<base>/_stcore/health`);
then delete any code that still builds absolute links from `APP_BASE_URL_PATH`.

---

## Writing the app

### Streamlit

Stay Streamlit-native and root-based. Prefer seaborn over plotly for charts;
plotly is not installed by default.

```python
# Users/<username>/app.py
import hopsworks
import streamlit as st

st.title("Feature Store Dashboard")

@st.cache_resource
def get_feature_store():
    project = hopsworks.login()          # auto-authenticates inside the cluster
    return project.get_feature_store()

fs = get_feature_store()

@st.cache_data(ttl=300)
def load_data():
    return fs.get_feature_group("transactions", version=1).read(dataframe_type="pandas")

df = load_data()
st.subheader("Transaction Data")
st.dataframe(df.head(100))
st.subheader("Amount Distribution")
st.bar_chart(df["amount"].value_counts().head(20))
```

### Custom app

Bind to `0.0.0.0` and the injected `APP_PORT`:

```python
# Users/<username>/app.py
import os
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def index():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ["APP_PORT"]))
```

### Reading Hopsworks data from the app

The SDK calls are the same as anywhere (hops-fg / hops-fv / hops-online-inference);
three things are app-specific:

- **A just-created feature group is not queryable via Trino/`hops sql` immediately.** The offline table syncs into the Trino catalog with a short lag, so a `SELECT ... FROM <fresh_fg>` right after `insert` can return `TABLE_NOT_FOUND`. Online feature-vector reads are available before the Trino table is, so make the app not-found-safe (warn on an empty online vector) instead of trusting a range from a fresh query.
- **Embedded model** (predict locally instead of calling a deployment): `model_dir = mr.get_model("fraud_model", version=1).download()`; cache the loaded model and its feature view in `@st.cache_resource` so the download happens once, and read through the feature view so the same MDTs/ODTs the model saw in training are applied (no training/serving skew).
- **Calling a deployment:** check `deployment.is_running()` before `predict`, and surface a message rather than blocking (see cold start below).

### Streamlit caching and cold start

Heavy work at the top of the script (`login()`, `init_serving()`, a call to an
online deployment) runs on every cold load and blocks the first paint: the app
shows RUNNING but the page hangs. Keep the top of the script cheap:

- `@st.cache_resource` for the connection, feature store handle, initialised feature view (`fv.init_serving(...)` inside the cached function) and deployment handle, so they initialise once, not per rerun.
- `@st.cache_data(ttl=...)` for data reads.
- Never call a deployment at import time. Trigger it from a button / form submit behind `st.spinner(...)`, and guard on `is_running()`:

```python
@st.cache_resource
def get_deployment():
    return project.get_model_serving().get_deployment("fraud_predictor")

if st.button("Score"):
    dep = get_deployment()
    if dep.is_running():
        with st.spinner("Scoring…"):
            st.write(dep.predict(inputs=[{"id": user_id}]))
    else:
        st.warning("Deployment is starting — try again shortly.")
```

### Hopsworks look & feel

Apps render as a bare default Streamlit page unless you theme them. Drop in the
Hopsworks palette so a shipped app reads as part of the platform — brand accents
only, don't restyle every widget.

`.streamlit/config.toml` — **must sit in the same directory as the app script**,
not in `~/.streamlit/`. Streamlit reads config from the script's own directory
(and CWD), so an app under `customer_spend_fti/app.py` needs
`customer_spend_fti/.streamlit/config.toml`. Copy it per app directory.

```toml
[server]
fileWatcherType = "none"   # REQUIRED on HopsFS/FUSE: the watcher stats the
headless = true            # script over FUSE on the event loop and blocks the
runOnSave = false          # readiness probe, making the managed app flap
                           # serving<->running. Without this the app never holds.

[browser]
gatherUsageStats = false

[theme]
primaryColor = "#1EB182"           # Hopsworks teal-green
backgroundColor = "#0E1117"
secondaryBackgroundColor = "#1A1F2B"
textColor = "#FAFAFA"
font = "sans serif"
```

A brand header + accents, injected once at the top of the app:
```python
import streamlit as st

st.set_page_config(page_title="My Hopsworks App", layout="wide")
st.markdown(
    """
    <style>
      .hw-band {background:linear-gradient(90deg,#0E1117,#1A1F2B);
                border-left:6px solid #1EB182; padding:0.75rem 1rem;
                border-radius:6px; margin-bottom:1rem;}
      .hw-band h1 {color:#FAFAFA; margin:0; font-size:1.4rem;}
      div[data-testid="stMetricValue"] {color:#1EB182;}
      .stButton>button {background:#1EB182; color:#0E1117; border:none; font-weight:600;}
    </style>
    <div class="hw-band"><h1>⬡ My Hopsworks App</h1></div>
    """,
    unsafe_allow_html=True,
)
```

A full dashboard (statistics, monitoring history, data sample) is in
[references/monitoring_dashboard.md](references/monitoring_dashboard.md).

---

## Custom libraries

If the app needs libraries not in `python-app-pipeline`, clone that base env and
install the app's `app-requirements.txt` into the **clone** (never the base). It
takes a few minutes; warn the user, and prompt for a requirements file if none
exists. Full workflow, including the project-path rule for the requirements
file: [hops-environments](../../platform/hops-environments/SKILL.md).

```python
env_api = project.get_environment_api()
# get_environment returns None for a missing environment, it does NOT raise:
# guard with `is None`, a try/except never fires.
cloned_env = env_api.get_environment("my_cloned_env")
if cloned_env is None:
    cloned_env = env_api.create_environment(
        "my_cloned_env", base_environment_name="python-app-pipeline",
    )
cloned_env.install_requirements("Users/<username>/app-requirements.txt", await_installation=True)
```

Then pass `environment="my_cloned_env"` to `create_app(...)`, not the base.

If the app loads a pickled model (e.g. scikit-learn via `joblib`), the base
`python-app-pipeline` may ship a different scikit-learn than the one that pickled
it, and the load fails. The base env exposes no library list (no `get_libraries`),
so pin the model's exact training versions (e.g. `scikit-learn==1.8.0` plus
matching `numpy`/`scipy`/`joblib`) in the requirements file.

---

## Creating and running the app

```python
import hopsworks

project = hopsworks.login()
apps = project.get_app_api()

app = apps.create_app(
    name="my_dashboard",
    app_path="Users/<username>/app.py",   # project-relative (CLI --path is HopsFS-absolute)
    environment="python-app-pipeline",    # or the clone from Custom libraries
    memory=2048,                          # MB
    cores=1.0,
)

app.run(await_serving=True)   # blocks until ready; await_serving=False returns immediately
if app.serving:
    print(f"App URL: {app.app_url}")
```

### Git-backed apps

Leave `app_path` unset and provide the repository fields. Git-backed apps are
cloned again on each start, so a restart or redeploy picks up new commits;
`git_auto_redeploy=True` rolls to the branch HEAD on every new commit. Supported
providers: `GitHub`, `GitLab`, `BitBucket`.

- Streamlit: `app_kind="STREAMLIT"` and `entrypoint_script` relative to the repository root.
- Custom: `app_kind="CUSTOM"`, an `entrypoint_command`, and `app_port`.

```python
streamlit_app = apps.create_app(
    name="streamlitfromgithub",
    app_kind="STREAMLIT",
    git_url="https://github.com/<org>/<repo>.git",
    git_provider="GitHub",
    git_branch="main",
    git_auto_redeploy=True,
    entrypoint_script="streamlitapp.py",
    environment="python-app-pipeline",
)

fastapi_app = apps.create_app(
    name="fastapifromgithub",
    app_kind="CUSTOM",
    git_url="https://github.com/<org>/<repo>.git",
    git_provider="GitHub",
    git_branch="main",
    entrypoint_command=(
        'bash -lc "python -m uv pip install --no-cache fastapi uvicorn && '
        'exec python -m uvicorn fastapiapp:app --host 0.0.0.0 --port \\"$APP_PORT\\""'
    ),
    app_port=8080,
    environment="python-app-pipeline",
)
fastapi_app.run()
print(fastapi_app.app_url)
```

### Lifecycle and states

```python
app.run(await_serving=True)
print(app.get_url())
app.redeploy()                # re-clone / re-read the source and restart
app.stop()
app.delete()

apps.get_apps()               # list; each has .name, .state, .serving
app = apps.get_app("my_dashboard")
```

| State | Meaning |
|---|---|
| `INITIALIZING` | starting up |
| `RUNNING` | container running; check `serving` for readiness |
| `STOPPED` / `FINISHED` | ended normally (stopped by the user / exited) |
| `KILLED` / `FAILED` | ended abnormally (the SDK treats both, plus `INITIALIZATION_FAILED` etc., as failures) |

An app is accessible only when `state == "RUNNING"` **and** `serving == True`.

### Monitoring

Monitoring is enabled by default. Leave routes empty for the default
ignored-path behaviour, or add routes to narrow the signal:

```python
monitoringConfig = {
    "enabled": True,
    "routes": [
        {"path": "/api", "matchType": "prefix"},     # matchType: prefix | exact
        {"path": "/predict", "matchType": "exact"},
    ],
}
```

Streamlit ignores `/_stcore/health`, `/_stcore/host-config`, `/_stcore/stream`
and `/static` by default. Custom apps should leave framework noise (`/static`,
`/docs`, `/openapi.json`, `/redoc`, `/favicon.ico`) out of the monitored routes
unless you want it counted.

## Next Steps

- Read features in the app: **hops-fg** / **hops-fv**. Query via SQL: **hops-trino-sql**.
- Dashboards instead of an app: **hops-superset**.

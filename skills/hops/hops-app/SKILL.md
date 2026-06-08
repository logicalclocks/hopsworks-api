---
name: hops-app
description: Use when writing Streamlit or custom Python apps for Hopsworks or managing app
  deployments. Auto-invoke when user wants to create a dashboard, deploy a Python
  app from HopsFS or Git, configure app monitoring, or access the feature store
  from an app. Input an app.py or git repo + environment + memory; output a
  running app and its URL.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Apps — Python SDK Best Practices

## Overview

Hopsworks supports deploying **Streamlit** applications and **custom Python** applications as managed apps. Apps are backed by a Hopsworks job and can be sourced from HopsFS or a Git repository. Streamlit apps run the Streamlit server; custom apps run a web server that listens on `$APP_PORT` (for example Flask, FastAPI, or Gradio).

A Streamlit app is a **UI / consumer of the FTI pipelines**, not a pipeline itself. A custom app is a general-purpose service or API frontend. Streamlit apps read features and predictions written by the feature, training, and inference pipelines (via the feature store, model registry, and online deployments) and present them. They can also act as a thin online-inference front by downloading a model from the registry and predicting locally (an **embedded model**), avoiding a network call to a model deployment.

## Contract

- **Input:** a Streamlit or custom `app.py` or a Git-backed app + environment + memory (plus optional monitoring config).
- **Output:** a running app and its URL.
- **Pre-condition:** app code is available either in HopsFS or in a Git repository.

## Smoke-test (cheap pre/post-flight)

```bash
hops app list                 # confirm state before; verify it is RUNNING/serving after
```

Full CLI surface is in **Manage Apps from the CLI** below.

## Ask the user (only when state is ambiguous)

- Does the app need **custom libraries** not in `python-app-pipeline`? If so, clone the env and install `app-requirements.txt` (see **Your App uses Custom libraries**).
- Does the app come from **HopsFS or Git**? If Git, ask for `git_url`, `git_provider`, and (if needed) `git_branch` plus the entrypoint.
- What **memory / cores** should the app get? Defaults are `memory=2048` MB, `cores=1.0`.
- Does the app need **monitoring** narrowed to specific routes? Monitoring is enabled by default and routes are optional; `hops app info` shows the monitoring state and route list when present.
- Does the app need **public access**? Streamlit sharing is feature-flagged; only ask for it when the platform has it enabled.
- **Before deleting** — `app.delete()` / `hops app delete --yes` tears down the app irreversibly; confirm the exact name with the user, and never tear down an app you created as a side effect (temp or test ones included) unless they asked.

---

## Creating and Running an App

When you create charts in Streamlit apps, prefer to use seaborn over plotly (which isn't installed by default).

### 1. Write the App


```python
# Users/<username>/app.py
import hopsworks
import streamlit as st
import pandas as pd

st.title("Feature Store Dashboard")

# Connect to Hopsworks (auto-authenticates inside the cluster)
project = hopsworks.login()
fs = project.get_feature_store()

# Read data from a feature group
fg = fs.get_feature_group("transactions", version=1)

@st.cache_data(ttl=300)
def load_data():
    return fg.read(dataframe_type="pandas")

df = load_data()

st.subheader("Transaction Data")
st.dataframe(df.head(100))

st.subheader("Amount Distribution")
st.bar_chart(df["amount"].value_counts().head(20))
```

### Custom Apps

For custom Python apps, bind to `0.0.0.0` and the injected `APP_PORT`. Use `APP_BASE_URL_PATH` if the app needs to build links that include the proxy prefix.

```python
# Users/<username>/app.py
import os
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def index():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ["APP_PORT"]))
```

### Git-backed Apps

When the app source lives in Git, leave `app_path` unset and provide the repository fields instead. Git-backed apps are cloned again on each start, so a restart or redeploy picks up new commits.

- Streamlit apps: use `app_kind="STREAMLIT"` and set `entrypoint_script` relative to the repository root.
- Custom apps: use `app_kind="CUSTOM"`, an `entrypoint_command`, and `app_port`.
- Supported Git providers: `GitHub`, `GitLab`, and `BitBucket`.

```python
streamlit_app = apps.create_app(
    name="streamlitfromgithub",
    app_kind="STREAMLIT",
    git_url="https://github.com/gibchikafa/appshopsworkstests.git",
    git_provider="GitHub",
    git_branch="main",
    entrypoint_script="streamlitapp.py",
    environment="python-app-pipeline",
)
```

```python
fastapi_app = apps.create_app(
    name="fastapifromgithub",
    app_kind="CUSTOM",
    git_url="https://github.com/gibchikafa/appshopsworkstests.git",
    git_provider="GitHub",
    git_branch="main",
    entrypoint_command='bash -lc "exec python -m uvicorn fastapiapp:app --host 0.0.0.0 --port \"$APP_PORT\""',
    app_port=8080,
    environment="python-app-pipeline",
)
```

```python
flask_app = apps.create_app(
    name="flaskfromgithub",
    app_kind="CUSTOM",
    git_url="https://github.com/gibchikafa/appshopsworkstests.git",
    git_provider="GitHub",
    git_branch="main",
    entrypoint_command='bash -lc "exec python -m flask --app flaskapp run --host 0.0.0.0 --port \"$APP_PORT\""',
    app_port=8080,
    environment="python-app-pipeline",
)
```

```python
gradio_app = apps.create_app(
    name="gradiofromgithub",
    app_kind="CUSTOM",
    git_url="https://github.com/gibchikafa/appshopsworkstests.git",
    git_provider="GitHub",
    git_branch="main",
    entrypoint_command='bash -lc "python -m uv pip install --system --no-cache gradio && exec python gradioapp.py"',
    app_port=7860,
    environment="python-app-pipeline",
)
```

## Your App uses Custom libraries

If the app needs libraries not in `python-app-pipeline`, clone that base env and
install the app's `app-requirements.txt` into the clone (it takes a few minutes;
warn the user, and prompt for a requirements file if none exists). Full workflow:
[hops-environments](../hops-environments/SKILL.md).

```python
env_api = project.get_environment_api()
cloned_env = env_api.create_environment(
    "my_cloned_env", base_environment_name="python-app-pipeline",
)
cloned_env.install_requirements("Users/<username>/app-requirements.txt")
```

Then pass `environment="my_cloned_env"` to `create_app(...)` below, not the base.

If the app loads a pickled model (e.g. scikit-learn via `joblib`), the base `python-app-pipeline` may ship a different scikit-learn than the one that pickled it, and the load fails. The base env has no library-introspection API (no `get_libraries`), so you cannot read its versions. Clone the base and pin the model's exact training versions (e.g. `scikit-learn==1.8.0` plus matching `numpy`/`scipy`/`joblib`) in the requirements file.

### 2. Create and Run the App

```python
import hopsworks

project = hopsworks.login()
apps = project.get_app_api()

# Create the app
app = apps.create_app(
    name="my_dashboard",
    app_path="Users/<username>/app.py",
    environment="python-app-pipeline",  # default Python environment
    memory=2048,                         # MB
    cores=1.0,
)

# Start the app (waits for Streamlit to be ready)
app.run(await_serving=True)

# Access the app
if app.serving:
    print(f"App URL: {app.app_url}")
```

> **The app path differs between the SDK and the `hops` CLI.** The SDK
> `create_app(app_path=...)` takes a **project-relative** path
> (`Users/<username>/app.py`); the CLI `hops app create --path` takes the
> **HopsFS absolute** path (`/Projects/<project>/Users/<username>/app.py`). Each
> surface rejects the other's form, so don't copy one into the other.

## Monitoring

Monitoring is enabled by default. Leave routes empty to use the default ignored-path behavior, or add routes to narrow the signal.

Monitoring config shape:

```python
monitoringConfig = {
    "enabled": True,
    "routes": [
        {"path": "/api", "matchType": "prefix"},
        {"path": "/predict", "matchType": "exact"},
    ],
}
```

- `enabled`: turns app monitoring on or off.
- `routes`: optional list of monitored paths. If omitted or empty, Hopsworks uses the default ignored-path behavior.
- `path`: absolute request path to include.
- `matchType`: `prefix` or `exact`.
- Streamlit ignores internal/static paths like `/_stcore/health`, `/_stcore/host-config`, `/_stcore/stream`, and `/static` by default.
- Custom apps should leave framework noise like `/static`, `/docs`, `/openapi.json`, `/redoc`, and `/favicon.ico` out of the monitored routes unless you want them counted.

---

## App Lifecycle

```python
app.run(await_serving=True)   # blocks until the app is ready
app.run(await_serving=False)   # returns immediately

print(app.get_url())

app.stop()

app.delete()

```

### App States

| State | Description |
|---|---|
| `INITIALIZING` | App is starting up |
| `RUNNING` | App container is running (check `serving` for readiness) |
| `KILLED` | App was stopped by user |
| `STOPPED` | App was stopped |
| `FAILED` | App crashed or failed to start |
| `FINISHED` | App execution completed |

An app is accessible only when `state == "RUNNING"` **and** `serving == True`.

---

## Managing Apps

```python
apps = project.get_app_api()

# List all apps
all_apps = apps.get_apps()
for a in all_apps:
    print(f"{a.name}: state={a.state}, serving={a.serving}")

# Get a specific app by name
app = apps.get_app("my_dashboard")

# Check if running
if app and app.serving:
    print(f"URL: {app.app_url}")
```

---

## Python Environments

Apps run in a Python environment that provides pre-installed packages. The default environment is `python-app-pipeline`.

### Installing Custom Dependencies

```python
env_api = project.get_environment_api()

# get_environment returns None for a missing environment, it does NOT raise.
# Guard with `is None`; a try/except never fires and you hit
# `AttributeError: 'NoneType' has no attribute 'install_requirements'`.
env = env_api.get_environment("python-app-pipeline")
if env is None:
    env = env_api.create_environment("python-app-pipeline")

# Install from requirements.txt
env.install_requirements("Resources/requirements.txt", await_installation=True)

# Install a wheel file
env.install_wheel("Resources/my_package-1.0-py3-none-any.whl", await_installation=True)
```

---

## Accessing Hopsworks Data in Streamlit Apps

### Feature Store Data

```python
import hopsworks
import streamlit as st

project = hopsworks.login()
fs = project.get_feature_store()

# Read feature group
fg = fs.get_feature_group("users", version=1)
df = fg.read(dataframe_type="pandas")

# Read from feature view
fv = fs.get_feature_view("user_features_fv", version=1)
batch_df = fv.get_batch_data(dataframe_type="pandas")

# Get online feature vectors
fv.init_serving(training_dataset_version=1)
vector = fv.get_feature_vector(entry={"user_id": 123}, return_type="pandas")
```

> **A just-created feature group is not queryable via Trino/`hops sql`
> immediately.** The offline table syncs into the Trino catalog with a short lag,
> so a `SELECT ... FROM <fresh_fg>` right after `insert` can return
> `TABLE_NOT_FOUND`. Online feature-vector reads are available before the offline
> Trino table is, so don't hardcode a UI bound from a fresh Trino query — make the
> app not-found-safe (warn on an empty online vector) instead of trusting a range.

### Model Registry

```python
mr = project.get_model_registry()
model = mr.get_model("fraud_model", version=1)

# Download model files
model_dir = model.download()
```

For an embedded model (predict locally instead of calling a deployment), cache the
downloaded model and its feature view in `@st.cache_resource` so the download and
load happen once, not per rerun. Apply the same MDTs/ODTs the model saw in
training — the feature view does this for you when you read through it — so there
is no training/serving skew.

### Model Serving

```python
ms = project.get_model_serving()
deployment = ms.get_deployment("fraud_predictor")

if deployment.is_running():
    result = deployment.predict(inputs=[[1.0, 2.0, 3.0]])
    st.write("Prediction:", result)
```

---

## Streamlit Best Practices for Hopsworks

### Cache Hopsworks Connections

```python
@st.cache_resource
def get_feature_store():
    project = hopsworks.login()
    return project.get_feature_store()

fs = get_feature_store()
```

### Cache Data Reads

```python
@st.cache_data(ttl=600)  # cache for 10 minutes
def load_feature_group(name, version):
    fg = fs.get_feature_group(name, version=version)
    return fg.read(dataframe_type="pandas")

df = load_feature_group("transactions", 1)
```

### Cache Feature View Initialization

```python
@st.cache_resource
def init_feature_view():
    fv = fs.get_feature_view("my_fv", version=1)
    fv.init_serving(training_dataset_version=1)
    return fv

fv = init_feature_view()
```

### Cold Start: do not block the first render

A common footgun: doing heavy work at the top of the script — `connect()`, `init_serving()`, or calling an online deployment — runs on every cold load and blocks the first paint, so the app shows RUNNING but the page appears to hang until everything is "fully loaded". Keep the top of the script cheap:

- Wrap the connection and FV/serving handle in `@st.cache_resource` (above) so they initialize once, not per rerun.
- Do not call an online deployment at import time. Trigger it from a button / form submit, and show `st.spinner(...)` so the wait is visible instead of looking frozen.
- Guard against a not-yet-ready deployment: check `deployment.is_running()` before `predict`, and surface a message rather than blocking.

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

---

## Hopsworks look & feel

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

## Complete Example: Feature Monitoring Dashboard

```python
# Resources/monitoring_dashboard.py
import hopsworks
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Feature Monitoring", layout="wide")
st.title("Feature Monitoring Dashboard")

@st.cache_resource
def get_feature_store():
    project = hopsworks.login()
    return project.get_feature_store()

fs = get_feature_store()

# Sidebar: select feature group
fg_name = st.sidebar.text_input("Feature Group", "transactions")
fg_version = st.sidebar.number_input("Version", min_value=1, value=1)

fg = fs.get_feature_group(fg_name, version=fg_version)

# Show feature group statistics
st.subheader("Latest Statistics")
stats = fg.get_statistics()
if stats:
    for feature_stat in stats.feature_descriptive_statistics:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(f"{feature_stat.feature_name} - Mean", f"{feature_stat.mean:.2f}" if feature_stat.mean else "N/A")
        col2.metric("Std Dev", f"{feature_stat.stddev:.2f}" if feature_stat.stddev else "N/A")
        col3.metric("Completeness", f"{feature_stat.completeness:.1%}" if feature_stat.completeness else "N/A")
        col4.metric("Distinct", str(feature_stat.approx_num_distinct_values or "N/A"))

# Show monitoring history
st.subheader("Monitoring History")
configs = fg.get_feature_monitoring_configs()
if configs:
    for config in configs if isinstance(configs, list) else [configs]:
        st.write(f"**{config.name}** - {config.feature_name}")
        history = config.get_history(with_statistics=True)
        if history:
            rows = []
            for r in history:
                rows.append({
                    "Time": r.monitoring_time,
                    "Shift Detected": r.shift_detected,
                    "Difference": r.difference,
                })
            st.dataframe(pd.DataFrame(rows))

# Show recent data
st.subheader("Recent Data Sample")

@st.cache_data(ttl=300)
def load_sample(name, version):
    fg = fs.get_feature_group(name, version=version)
    return fg.read(dataframe_type="pandas").head(100)

sample_df = load_sample(fg_name, fg_version)
st.dataframe(sample_df)
```

Deploy it:

```python
apps = project.get_app_api()
app = apps.create_app(
    name="monitoring_dashboard",
    app_path="Resources/monitoring_dashboard.py",
    memory=4096,
)
app.run()
```

---

## Quick Reference

| Task | Code |
|---|---|
| Get app API | `project.get_app_api()` |
| Create app | `apps.create_app(name=..., app_path=...)` |
| Start app | `app.run(await_serving=True)` |
| Stop app | `app.stop()` |
| Redeploy app | `app.redeploy()` |
| Delete app | `app.delete()` |
| Get app URL | `app.app_url` |
| List all apps | `apps.get_apps()` |
| Get app by name | `apps.get_app("name")` |
| Check if serving | `app.serving` |
| Install deps | `env.install_requirements("Resources/requirements.txt")` |

---

## Manage Apps from the CLI

```bash
hops app list                 # apps + state
hops app info <name>          # detail (id, type, source)
hops app url <name>           # the app URL
hops app start <name> / stop <name>
hops app logs <name>          # tail logs (newer CLI)
hops app delete <name> --yes  # non-interactive
```

`hops app create <name> --path /Projects/<project>/Users/<user>/app.py --start`
takes a HopsFS-absolute path (the SDK `create_app(app_path=...)` takes a
project-relative one). `--start` creates, starts, and waits for serving in one
call (no separate `hops app start`). `logs` / `redeploy` may be absent on older
deployed `hops` binaries.

## Next Steps

- Read features in the app: **hops-fg** / **hops-fv**. Query via SQL: **hops-trino-sql**.
- Dashboards instead of an app: **hops-superset**.

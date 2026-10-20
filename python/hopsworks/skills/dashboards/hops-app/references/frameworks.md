# Writing a Streamlit, Flask, Gradio, Node.js or custom app

Part of **hops-app**: read when writing an app by hand, not from the `/hops app` skeleton.

## Streamlit

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

## Flask, Gradio and Node.js (5.2+)

Flask: an `app` object in the file; routes at `/` and `/health`; the app is served
at its root, and `APP_PUBLIC_PATH` / `X-Forwarded-Prefix` carry the public path for
an app that builds absolute links (`ProxyFix(app.wsgi_app, x_prefix=1)`).

```python
# Users/<username>/flaskapp.py, app_kind="FLASK"
from flask import Flask, jsonify

app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify(status="ok")

@app.get("/")
def home():
    return jsonify(status="ready")
```

Gradio: call `demo.launch()` with no `server_port` or `root_path`; Hopsworks sets
`GRADIO_SERVER_NAME`, `GRADIO_SERVER_PORT` and `GRADIO_ROOT_PATH`, which `launch()`
reads, and a hardcoded port breaks the readiness probe.

Node.js: listen on `process.env.PORT`; `import` resolves the environment's npm
packages (install them first, e.g. `env.install_npm(["express@latest"])`).

```javascript
// Users/<username>/server.mjs, app_kind="NODEJS"
import express from "express";
const app = express();
app.get("/health", (req, res) => res.json({ status: "ok" }));
app.get("/", (req, res) => res.json({ status: "ready" }));
app.listen(process.env.PORT, process.env.HOST ?? "0.0.0.0");
```

## Custom app

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

## Streamlit caching and cold start

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
        st.warning("Deployment is starting, try again shortly.")
```

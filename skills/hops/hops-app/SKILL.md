---
name: hopsworks-apps
description: Use when writing Streamlit apps for Hopsworks or managing app
  deployments. Auto-invoke when user wants to create a Streamlit dashboard, deploy a
  Python app to Hopsworks, or access the feature store from a Streamlit application.
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---

# Hopsworks Streamlit Apps — Python SDK Best Practices

Reference: `/tmp/hopsworks-api/python/hopsworks_common/`

## Overview

Hopsworks supports deploying **Streamlit** applications as managed apps. Apps are Python scripts backed by a Hopsworks job that runs the Streamlit server. Only Streamlit apps are currently supported.

---

## Creating and Running an App

### 1. Write a Streamlit Script


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

## Your App uses Custom libraries

If an app uses libraries that are not installed in `python-app-pipeline`, you need to clone the `python-app-pipline` Python environment, install the requirements (e.g., app-requirements.txt) in the cloned environment, and then create the app using the cloned environment. If no app-requirements.txt file exists, prompt the user to create one. You should also warn the user that installing the app-requirements.txt file in the user's cloned Python environment takes a couple of minutes.

env_api = project.get_environment_api()
# Clone from an existing environment
cloned_env = env_api.create_environment(
    "my_cloned_env",                                                                                                                                                                        
    description="Cloned from feature pipeline env",
    base_environment_name="python-app-pipeline",                                                                                                                                        
)                                                                
cloned_env.install_requirements("Users/<username>/app-requirements.txt")
     

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

---

## App Lifecycle

```python
app.run(await_serving=True)   # blocks until Streamlit is ready
app.run(await_serving=False)   # returns immediately

print(app.get_url())

app.stop()

app.delete()

```

### App States

| State | Description |
|---|---|
| `INITIALIZING` | App is starting up |
| `RUNNING` | App container is running (check `serving` for Streamlit readiness) |
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

# Get or create environment
env = env_api.get_environment("python-app-pipeline")

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

### Model Registry

```python
mr = project.get_model_registry()
model = mr.get_model("fraud_model", version=1)

# Download model files
model_dir = model.download()
```

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

---

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
| Delete app | `app.delete()` |
| Get app URL | `app.app_url` |
| List all apps | `apps.get_apps()` |
| Get app by name | `apps.get_app("name")` |
| Check if serving | `app.serving` |
| Install deps | `env.install_requirements("Resources/requirements.txt")` |

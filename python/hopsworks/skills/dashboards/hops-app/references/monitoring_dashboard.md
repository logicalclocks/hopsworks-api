# Complete example: feature monitoring dashboard

A Streamlit app that shows a feature group's latest statistics and a data sample, deployed with the SDK.
It applies the caching rules from the parent skill: the feature store handle in `st.cache_resource`, reads in `st.cache_data`.

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

fg_name = st.sidebar.text_input("Feature Group", "transactions")
fg_version = st.sidebar.number_input("Version", min_value=1, value=1)

fg = fs.get_feature_group(fg_name, version=fg_version)

st.subheader("Latest Statistics")
stats = fg.get_statistics()
if stats:
    for feature_stat in stats.feature_descriptive_statistics:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(f"{feature_stat.feature_name} - Mean", f"{feature_stat.mean:.2f}" if feature_stat.mean else "N/A")
        col2.metric("Std Dev", f"{feature_stat.stddev:.2f}" if feature_stat.stddev else "N/A")
        col3.metric("Completeness", f"{feature_stat.completeness:.1%}" if feature_stat.completeness else "N/A")
        col4.metric("Distinct", str(feature_stat.approx_num_distinct_values or "N/A"))

# Drift history needs the feature-monitoring service enabled cluster-wide
# (see hops-monitoring); guard it so the dashboard still renders without it.
st.subheader("Monitoring History")
configs = fg.get_feature_monitoring_configs()
for config in (configs if isinstance(configs, list) else [configs]) if configs else []:
    st.write(f"**{config.name}** - {config.feature_name}")
    history = config.get_history(with_statistics=True)
    if history:
        st.dataframe(pd.DataFrame(
            {"Time": r.monitoring_time, "Shift Detected": r.shift_detected, "Difference": r.difference}
            for r in history
        ))

st.subheader("Recent Data Sample")

@st.cache_data(ttl=300)
def load_sample(name, version):
    return fs.get_feature_group(name, version=version).read(dataframe_type="pandas").head(100)

st.dataframe(load_sample(fg_name, fg_version))
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

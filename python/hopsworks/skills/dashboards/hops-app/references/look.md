# The Hopsworks look in an app

Part of **hops-app**: read when styling an app.

In Hopsable, the `look` tool renders the app in headless Chromium at 600 and 900 px wide and
reports the layout as text: call it, and never script a browser to check the layout.

Apps render as a bare default Streamlit page unless you theme them. Theme them
with the Hopsworks tokens and follow the layout rules in
[hops-design](../../hops-design/SKILL.md): light, flat, the green accent only on the
primary action and the selection.

`.streamlit/config.toml` must sit in the same directory as the app script,
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
base = "light"
primaryColor = "#1EB182"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F7F7F7"
textColor = "#252525"
font = "sans serif"
```

A plain title, and tabular numbers in metrics, injected once at the top of the app.
No banner, gradient or emoji:
```python
import streamlit as st

st.set_page_config(page_title="Customer spend", layout="wide")
st.markdown(
    """
    <style>
      div[data-testid="stMetricValue"] {font-variant-numeric: tabular-nums;}
      div[data-testid="stMetricLabel"] {color: #737373;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Customer spend")
```

A full dashboard (statistics, monitoring history, data sample) is in
[references/monitoring_dashboard.md](monitoring_dashboard.md).

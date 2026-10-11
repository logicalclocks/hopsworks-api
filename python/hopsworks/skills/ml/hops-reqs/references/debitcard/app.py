"""Card fraud console: live per-card risk from the streaming windows and the dbt baselines.

    streamlit run app.py   (deployed as a Hopsworks app; DEPLOYMENT names the model deployment)

The table scores every card with an open window, read from the online store in
one snapshot, with the registered model; the lookup box asks the deployment
about one card and shows its round trip.
"""

import os
import time
from pathlib import Path

import joblib
import model as fraud_model
import pandas as pd
import streamlit as st

import hopsworks

DEPLOYMENT = os.environ.get("DEPLOYMENT", "debitcardexample")

st.set_page_config(page_title="Card fraud console", layout="wide")


@st.cache_resource(show_spinner="Connecting to Hopsworks...")
def resources():
    project = hopsworks.login()
    fs = project.get_feature_store()
    windows = fs.get_feature_group("card_window_aggregates", version=1)
    baselines = fs.get_feature_group("card_spending_baselines", version=1)
    models = project.get_model_registry().get_models(fraud_model.MODEL_NAME)
    artifact = None
    if models:
        latest = max(models, key=lambda m: int(m.version))
        artifact = joblib.load(Path(latest.download()) / fraud_model.MODEL_FILE)
    return project, windows, baselines, artifact


project, windows, baselines, artifact = resources()
st.title("Card fraud console")
if windows is None or baselines is None or artifact is None:
    st.error(
        "Run the batch features, the streaming jobs and training first: the windows, baselines or model are missing."
    )
    st.stop()

auto = st.sidebar.checkbox("Auto-refresh", value=True)
every = st.sidebar.slider("Refresh every (s)", 2, 60, 5)
threshold = st.sidebar.slider(
    "Risk threshold", 0.0, 1.0, round(fraud_model.default_threshold(artifact), 2), 0.01
)
st.sidebar.caption(
    f"Model trained on {artifact['training_rows']:,} cards at {artifact['trained_at'][:16]}"
)


@st.fragment(run_every=f"{every}s" if auto else None)
def live():
    started = time.perf_counter()
    snapshot = windows.read(online=True)
    if snapshot.empty:
        st.warning(
            "No card has an open window: start the transaction stream and the streaming features job."
        )
        return
    snapshot = snapshot.merge(baselines.read(online=True), on="card_number", how="left")
    read_ms = (time.perf_counter() - started) * 1000
    started = time.perf_counter()
    risk, _, _ = fraud_model.score(artifact, snapshot)
    score_ms = (time.perf_counter() - started) * 1000
    snapshot["risk"] = risk
    snapshot = snapshot.sort_values("risk", ascending=False)
    flagged = snapshot[snapshot["risk"] >= threshold]

    tiles = st.columns(4)
    tiles[0].metric("Cards with an open window", f"{len(snapshot):,}")
    tiles[1].metric("Over the threshold", f"{len(flagged):,}")
    tiles[2].metric("Online read", f"{read_ms:.0f} ms")
    tiles[3].metric("Scoring", f"{score_ms:.1f} ms")
    st.subheader("Riskiest cards")
    shown = (flagged if not flagged.empty else snapshot).head(50)
    st.dataframe(
        shown[
            [
                "card_number",
                "window_end",
                "risk",
                "txn_count",
                "amount_sum_eur",
                "avg_daily_amount_eur_30d",
                "decline_ratio",
                "max_merchant_distance_km",
                "max_issuer_fraud_score",
            ]
        ],
        hide_index=True,
        column_config={
            "risk": st.column_config.ProgressColumn("risk", min_value=0.0, max_value=1.0)
        },
    )


live()

st.subheader("Ask the deployment about a card")
card = st.text_input("Card number")
if card:
    deployment = project.get_model_serving().get_deployment(DEPLOYMENT)
    if deployment is None or not deployment.is_running():
        st.error(f"The deployment {DEPLOYMENT} is not running.")
    else:
        started = time.perf_counter()
        reply = deployment.predict(inputs=[[card.strip()]])
        st.metric("Round trip", f"{(time.perf_counter() - started) * 1000:.0f} ms")
        st.dataframe(pd.DataFrame(reply["predictions"]), hide_index=True)

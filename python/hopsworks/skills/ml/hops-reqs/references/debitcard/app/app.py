"""Card fraud console.

A live view of every debit card with an open streaming window, scored by the
registered fraud model against the card's dbt spending baseline. Tiles count
the cards in a window, those over the risk threshold and the authorisations in
the windows, with the online read and scoring times; a histogram shows how the
risk is spread and a table the riskiest cards. Selecting a card shows its
window beside its baseline and asks the deployment about it, with the round
trip. Status chips say whether the deployment and the two streaming jobs run.
The page refreshes every few seconds and keeps its content while it does.

A custom Hopsworks app: one process serving a JSON API under /api and a static
JavaScript UI, bound to 0.0.0.0:$APP_PORT, with /health for the readiness probe.
The UI calls the API with relative URLs, so the Hopsworks proxy mount works
without the app knowing it. DEPLOYMENT and JOBS (comma-separated) name the
deployment and the streaming jobs.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from functools import lru_cache
from pathlib import Path

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

STATIC = Path(__file__).resolve().parent / "static"
DEPLOYMENT = os.environ.get("DEPLOYMENT", "debitcardexample")
JOBS = os.environ.get(
    "JOBS", "debitcard-example-transactions,debitcard-example-streaming-features"
).split(",")
# The windows are one online row per card, a few thousand rows: one snapshot
# serves every request for this long.
SNAPSHOT_SECONDS = 3.0
TOP = 40
BINS = 20

app = FastAPI(title="Card fraud console", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@lru_cache(maxsize=1)
def _project():
    # Deferred so the app starts and answers /health before the SDK connects.
    import hopsworks

    return hopsworks.login()


@lru_cache(maxsize=2)
def _group(name: str):
    group = _project().get_feature_store().get_feature_group(name, version=1)
    if group is None:
        raise HTTPException(503, f"{name} does not exist yet: run the feature pipelines first")
    return group


@lru_cache(maxsize=1)
def _model():
    """The latest registered model and its model.py, which computes its features."""
    models = _project().get_model_registry().get_models("card_fraud_model")
    if not models:
        raise HTTPException(503, "card_fraud_model is not registered yet: run the training job")
    latest = max(models, key=lambda m: int(m.version))
    directory = latest.download()
    sys.path.insert(0, directory)
    import model as fraud_model

    artifact = joblib.load(Path(directory) / fraud_model.MODEL_FILE)
    return fraud_model, artifact, int(latest.version)


_snapshot: dict = {"at": 0.0}
_lock = threading.Lock()


def _scored():
    """Every card with an open window, joined with its baseline and scored; cached briefly."""
    with _lock:
        if time.monotonic() - _snapshot["at"] < SNAPSHOT_SECONDS:
            return _snapshot
        fraud_model, artifact, version = _model()
        started = time.perf_counter()
        windows = _group("card_window_aggregates").read(online=True, dataframe_type="pandas")
        baselines = _group("card_spending_baselines").read(online=True, dataframe_type="pandas")
        read_ms = (time.perf_counter() - started) * 1000
        frame = windows.merge(baselines, on="card_number", how="left")
        started = time.perf_counter()
        if len(frame):
            risk, _, _ = fraud_model.score(artifact, frame)
            frame["risk"] = risk
        score_ms = (time.perf_counter() - started) * 1000
        _snapshot.update(
            at=time.monotonic(),
            frame=frame,
            read_ms=read_ms,
            score_ms=score_ms,
            threshold=fraud_model.default_threshold(artifact),
            model={
                "version": version,
                "trained_at": artifact["trained_at"],
                "training_rows": artifact["training_rows"],
            },
        )
        return _snapshot


def _number(value):
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return None
    return round(float(value), 3)


CARD_FIELDS = [
    "card_number",
    "risk",
    "txn_count",
    "declined_count",
    "amount_sum_eur",
    "amount_max_eur",
    "avg_daily_amount_eur_30d",
    "avg_daily_txn_count_7d",
    "max_merchant_distance_km",
    "max_merchant_distance_km_30d",
    "decline_ratio",
    "decline_ratio_30d",
    "distinct_merchants",
    "distinct_regions",
    "max_issuer_fraud_score",
    "max_network_risk_score",
    "high_risk_count",
]


def _card(row) -> dict:
    card = {k: _number(row.get(k)) for k in CARD_FIELDS if k != "card_number"}
    card["card_number"] = str(row["card_number"])
    card["window_end"] = str(row.get("window_end"))
    return card


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the feature store is read by /api/snapshot."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/snapshot")
def snapshot(threshold: float | None = None) -> dict:
    """The tiles, the risk histogram and the riskiest cards, at `threshold` or the model's own."""
    snap = _scored()
    frame = snap["frame"]
    cut = snap["threshold"] if threshold is None else threshold
    if frame.empty:
        return {
            "cards": 0,
            "flagged": 0,
            "authorisations": 0,
            "histogram": [0] * BINS,
            "top": [],
            "threshold": cut,
            "model_threshold": snap["threshold"],
            "read_ms": snap["read_ms"],
            "score_ms": snap["score_ms"],
            "model": snap["model"],
        }
    risk = frame["risk"].to_numpy()
    histogram, _ = np.histogram(risk, bins=BINS, range=(0.0, 1.0))
    top = frame.sort_values("risk", ascending=False).head(TOP)
    return {
        "cards": len(frame),
        "flagged": int((risk >= cut).sum()),
        "authorisations": int(frame["txn_count"].sum()),
        "with_baseline": float(frame["avg_daily_amount_eur_30d"].notna().mean()),
        "histogram": histogram.tolist(),
        "top": [_card(row) for _, row in top.iterrows()],
        "threshold": cut,
        "model_threshold": snap["threshold"],
        "read_ms": round(snap["read_ms"], 1),
        "score_ms": round(snap["score_ms"], 1),
        "window_end": str(frame["window_end"].max()),
        "model": snap["model"],
    }


class Ask(BaseModel):
    """A card to ask the deployment about."""

    card_number: str = Field(pattern=r"^\d{12,19}$")


@lru_cache(maxsize=1)
def _deployment():
    deployment = _project().get_model_serving().get_deployment(DEPLOYMENT)
    if deployment is None:
        raise HTTPException(503, f"the deployment {DEPLOYMENT} does not exist")
    return deployment


@app.post("/api/score")
def score(ask: Ask) -> dict:
    """The deployment's verdict on one card, its round trip, and the card's window and baseline."""
    started = time.perf_counter()
    try:
        reply = _deployment().predict(inputs=[[ask.card_number]])
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - shown to the user as the deployment's error
        raise HTTPException(502, f"the deployment failed: {exc}") from exc
    round_trip = (time.perf_counter() - started) * 1000
    frame = _scored()["frame"]
    rows = frame[frame["card_number"] == ask.card_number] if len(frame) else frame
    return {
        "prediction": reply["predictions"][0],
        "round_trip_ms": round(round_trip, 1),
        "card": _card(rows.iloc[0]) if len(rows) else None,
    }


@app.get("/api/status")
def status() -> dict:
    """Whether the deployment and the streaming jobs run."""
    out = {"deployment": "missing", "jobs": {}}
    try:
        deployment = _project().get_model_serving().get_deployment(DEPLOYMENT)
        if deployment is not None:
            out["deployment"] = "running" if deployment.is_running(or_idle=False) else "stopped"
    except Exception:  # noqa: BLE001 - a status chip, not an error page
        out["deployment"] = "unknown"
    jobs = _project().get_job_api()
    for name in JOBS:
        try:
            job = jobs.get_job(name)
            out["jobs"][name] = (job.get_state() or "unknown").lower() if job else "missing"
        except Exception:  # noqa: BLE001
            out["jobs"][name] = "unknown"
    return out


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

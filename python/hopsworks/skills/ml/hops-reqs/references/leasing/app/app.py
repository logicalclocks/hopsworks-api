# ruff: noqa: INP001
"""Leasing decisions console.

Pick a customer, a gardening machine and a lease length and get two decisions:
the XGBoost classifier trained on the leasing_decisions feature view, and NVIDIA
Kumo Tabular (medium), the pretrained in-context classifier, reading the same
transformed features with a sample of the training rows as its context. A batch
button scores 100 or 1,000 random orders. Every decision is logged through the
feature view to its logging feature group. Simulate leasing outcomes turns the
logged decisions into orders with outcomes (paid, paid late, damaged, defaulted,
declined); Retrain runs the training job on them, and the app switches to the new
model when it is registered.

A custom Hopsworks app: one process serving a JSON API under /api and a static
JavaScript UI, bound to 0.0.0.0:$APP_PORT, with /health for the readiness probe.
It imports scoring.py, kumo.py, synth.py and names.py from the directory above.
SYSTEM is the system's slug; TRAIN_JOB the training job, `<SYSTEM>-train` by default.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from functools import lru_cache
from pathlib import Path

import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import names  # noqa: E402 - from the directory above, added to the path first
import synth  # noqa: E402


STATIC = HERE / "static"
SYSTEM = os.environ.get("SYSTEM", "leasing-example")
TRAIN_JOB = os.environ.get("TRAIN_JOB", f"{SYSTEM}-train")
SHOWN = 50

app = FastAPI(title="Leasing decisions", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")

_state = {"error": None, "started": time.time()}
_loaded = threading.Event()


@lru_cache(maxsize=1)
def _project():
    import hopsworks

    return hopsworks.login()


@lru_cache(maxsize=1)
def _scorer():
    from scoring import Scorer

    return Scorer(_project(), SYSTEM)


def _load():
    """Load the models in the background: Kumo Tabular takes a while, and /health answers meanwhile."""
    try:
        _scorer().load()
        _state["error"] = None
        _loaded.set()
    except Exception as exc:  # noqa: BLE001 - shown on the page until a reload works
        _state["error"] = str(exc)


threading.Thread(target=_load, daemon=True).start()


def _ready():
    if not _loaded.is_set():
        raise HTTPException(503, _state["error"] or "loading the models")
    return _scorer()


def _clean(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not np.isfinite(value) else round(float(value), 4)
    return value


@lru_cache(maxsize=1)
def _catalogue():
    """Customers (with their newest credit report) and machines, for the pickers; read once."""
    scorer = _ready()
    tables = scorer.catalogue()
    credit = tables[names.CREDIT].sort_values("customer_id")
    equipment = tables[names.EQUIPMENT].sort_values(["category", "monthly_price"])
    return {
        "customers": [
            {
                "id": int(r.customer_id),
                "name": r.customer_name,
                "type": r.customer_type,
                "credit_score": int(r.credit_score),
            }
            for r in credit.itertuples()
        ],
        "equipment": [
            {
                "id": int(r.equipment_id),
                "name": r.name,
                "category": r.category,
                "monthly_price": float(r.monthly_price),
            }
            for r in equipment.itertuples()
        ],
        "lease_months": list(synth.LEASE_MONTHS),
    }


def _named(frame):
    """Rows for the page, with the customer's and the machine's names."""
    catalogue = _catalogue()
    customers = {c["id"]: c["name"] for c in catalogue["customers"]}
    machines = {m["id"]: m for m in catalogue["equipment"]}
    rows = []
    for row in frame.to_dict("records"):
        machine = machines.get(int(row["equipment_id"]), {})
        rows.append(
            {k: _clean(v) for k, v in row.items()}
            | {
                "customer": customers.get(int(row["customer_id"]), ""),
                "machine": machine.get("name", ""),
                "category": machine.get("category", ""),
            }
        )
    return rows


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the models may still be loading."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/status")
def status() -> dict:
    """The models in use, switching to a newer leasing_xgboost once training registers one, and the training job."""
    out = {
        "loaded": _loaded.is_set(),
        "error": _state["error"],
        "train_job": {"name": TRAIN_JOB, "state": "missing"},
    }
    try:
        job = _project().get_job_api().get_job(TRAIN_JOB)
        if job is not None:
            executions = job.get_executions() or []
            latest = max(executions, key=lambda e: e.id, default=None)
            out["train_job"] = {
                "name": TRAIN_JOB,
                "state": (latest.state if latest else "never run").lower(),
                "final_status": (latest.final_status or "").lower() if latest else "",
                "submitted": str(latest.submission_time) if latest else None,
            }
    except Exception as exc:  # noqa: BLE001 - a status line, not an error page
        out["train_job"]["state"] = f"unknown: {exc}"
    if _loaded.is_set():
        scorer = _scorer()
        try:
            scorer.load()  # a no-op unless a newer version is registered
        except Exception as exc:  # noqa: BLE001
            out["error"] = str(exc)
        out["xgboost"] = {
            "name": names.MODEL,
            "version": int(scorer.model.version),
            "training_dataset_version": scorer.model.training_dataset_version,
            "metrics": {k: _clean(float(v)) for k, v in scorer.metrics.items()},
        }
        out["kumo"] = {
            "name": names.KUMO,
            "size": scorer.kumo.size,
            "context_rows": len(scorer.context),
        }
    return out


@app.get("/api/catalogue")
def catalogue() -> dict:
    """The customers, machines and lease lengths to choose from."""
    return _catalogue()


class Order(BaseModel):
    """One new order to decide."""

    customer_id: int = Field(ge=1)
    equipment_id: int = Field(ge=1)
    lease_months: int = Field(ge=1, le=36)


@app.post("/api/decide")
def decide(order: Order) -> dict:
    """Both models' decisions on one order, with the features they read; logged."""
    import pandas as pd

    scorer = _ready()
    frame = pd.DataFrame([order.model_dump()])
    started = time.perf_counter()
    out = scorer.score(frame)
    total = (time.perf_counter() - started) * 1000
    raw = out.attrs["features"].iloc[0].to_dict()
    return {
        "decision": _named(out)[0],
        "features": {k: _clean(v) for k, v in raw.items()},
        "timings_ms": out.attrs["timings_ms"] | {"total": round(total, 1)},
    }


class Batch(BaseModel):
    """How many random orders to score."""

    n: int = Field(ge=1, le=1000)


@app.post("/api/batch")
def batch(request: Batch) -> dict:
    """Score `n` random orders with both models and log them."""
    scorer = _ready()
    started = time.perf_counter()
    out = scorer.score(scorer.random_orders(request.n))
    total = (time.perf_counter() - started) * 1000
    return {
        "orders": len(out),
        "xgboost_leased": float(out["leased"].mean()),
        "kumo_leased": float(out["kumo_leased"].mean()),
        "agreement": float((out["leased"] == out["kumo_leased"]).mean()),
        "timings_ms": out.attrs["timings_ms"] | {"total": round(total, 1)},
        "rows": _named(out.head(SHOWN)),
    }


@app.post("/api/simulate")
def simulate() -> dict:
    """Write an order and its outcome for every logged decision not yet simulated."""
    scorer = _ready()
    started = time.perf_counter()
    try:
        out = scorer.simulate()
    except Exception as exc:  # noqa: BLE001 - shown to the user
        raise HTTPException(502, f"the simulation failed: {exc}") from exc
    out["seconds"] = round(time.perf_counter() - started, 1)
    return out


@app.post("/api/retrain")
def retrain() -> dict:
    """Run the training job, the system's own training pipeline, on every order so far."""
    job = _project().get_job_api().get_job(TRAIN_JOB)
    if job is None:
        raise HTTPException(404, f"the training job {TRAIN_JOB} does not exist")
    execution = job.run(await_termination=False)
    return {"job": TRAIN_JOB, "execution": execution.id}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

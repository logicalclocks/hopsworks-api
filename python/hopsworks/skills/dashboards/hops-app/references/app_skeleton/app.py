# ruff: noqa: INP001
"""Customer lookup.

Shows a customer's latest features and churn score. A person types a customer
id and sees the feature values and the score, and the ten highest-risk
customers are listed on load. Reads the telco_churn_predictions feature group
through Trino, which picks each customer's latest prediction and the top ten.

The description above is the app's record: `/hops app` writes it from the
user's words and updates it on every edit, so the next edit starts from what
the app is now.

A custom Hopsworks app: one process serving a JSON API under /api and a static
JavaScript UI, bound to 0.0.0.0:$APP_PORT, with /health for the readiness probe.
The UI calls the API with relative URLs, so the Hopsworks proxy mount
(/hopsworks-api/pythonapp/<project>/<app>/) works without the app knowing it.
"""

from __future__ import annotations

import os
import threading
import time
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

STATIC = Path(__file__).resolve().parent / "static"
# The feature group's Trino catalog follows its table format: delta, hudi or iceberg.
PREDICTIONS = {"name": "telco_churn_predictions", "version": 1, "catalog": "delta"}
MAX_LIMIT = 100
CACHE_SECONDS = 30

# Trino picks each customer's latest prediction and the top k, so a request reads k rows,
# not the prediction history.
TOP = """
SELECT customer_id, score FROM (
  SELECT customer_id, score,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY predicted_at DESC) AS newest
  FROM {table}
) WHERE newest = 1
ORDER BY score DESC
LIMIT ?
"""
CUSTOMER = """
SELECT score, predicted_at FROM {table}
WHERE customer_id = ?
ORDER BY predicted_at DESC
LIMIT 1
"""

app = FastAPI(title="Customer lookup", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")
_cache: dict[tuple, tuple[float, list]] = {}
_cache_lock = threading.Lock()


@lru_cache(maxsize=1)
def _connection():
    # Deferred so the app starts and answers /health before the SDK connects.
    import hopsworks

    project = hopsworks.login()
    return project.get_trino_api().connect(
        catalog=PREDICTIONS["catalog"], schema=f"{project.name.lower()}_featurestore"
    )


def _table() -> str:
    return f'"{PREDICTIONS["name"]}_{PREDICTIONS["version"]}"'


def _query(sql: str, params: tuple) -> list[tuple]:
    """Rows of `sql`, its values bound as parameters, from a cache of CACHE_SECONDS."""
    key = (sql, params)
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and hit[0] > now:
            return hit[1]
    cursor = _connection().cursor()
    cursor.execute(sql.format(table=_table()), params)
    rows = cursor.fetchall()
    with _cache_lock:
        _cache[key] = (now + CACHE_SECONDS, rows)
        for stale in [k for k, (until, _) in _cache.items() if until <= now]:
            del _cache[stale]
    return rows


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the data connection is checked by the API routes."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/top")
def top(limit: int = Query(10, ge=1, le=MAX_LIMIT)) -> list[dict]:
    """The customers with the highest score, each by its latest prediction."""
    return [
        {"customer_id": customer_id, "score": float(score)}
        for customer_id, score in _query(TOP, (limit,))
    ]


@app.get("/api/customers/{customer_id}")
def customer(customer_id: int) -> dict:
    """One customer's latest score."""
    rows = _query(CUSTOMER, (customer_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"no predictions for customer {customer_id}")
    score, predicted_at = rows[0]
    return {"customer_id": customer_id, "score": float(score), "predicted_at": str(predicted_at)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

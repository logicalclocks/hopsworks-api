# ruff: noqa: INP001
"""Customer lookup.

Shows a customer's latest features and churn score. A person types a customer
id and sees the feature values and the score, and the ten highest-risk
customers are listed on load. Reads the telco_churn_predictions feature group.

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
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


STATIC = Path(__file__).resolve().parent / "static"
PREDICTIONS = {"name": "telco_churn_predictions", "version": 1}

app = FastAPI(title="Customer lookup", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@lru_cache(maxsize=1)
def _feature_store():
    # Deferred so the app starts and answers /health before the SDK connects.
    import hopsworks

    return hopsworks.login().get_feature_store()


def _predictions():
    fs = _feature_store()
    return fs.get_feature_group(PREDICTIONS["name"], version=PREDICTIONS["version"])


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the data connection is checked by the API routes."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/top")
def top(limit: int = 10) -> list[dict]:
    """The customers with the highest score in the latest predictions."""
    fg = _predictions()
    rows = fg.select_all().show(1000)
    latest = rows.sort_values("predicted_at").groupby("customer_id").tail(1)
    return latest.nlargest(limit, "score")[["customer_id", "score"]].to_dict(
        orient="records"
    )


@app.get("/api/customers/{customer_id}")
def customer(customer_id: int) -> dict:
    """One customer's latest score."""
    fg = _predictions()
    rows = fg.filter(fg.customer_id == customer_id).read(dataframe_type="pandas")
    if rows.empty:
        raise HTTPException(
            status_code=404, detail=f"no predictions for customer {customer_id}"
        )
    latest = rows.sort_values("predicted_at").iloc[-1]
    return {
        "customer_id": customer_id,
        "score": float(latest["score"]),
        "predicted_at": str(latest["predicted_at"]),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

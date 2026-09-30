# ruff: noqa: INP001
"""Storefront.

A shop window over the recommender deployment. A person picks a customer and
sees the products the deployment ranks for them, as cards with the H&M picture.
Click and Buy are recorded as the shopper's interactions (score 1 and 2), and
Buy also as a purchase, so the next recommendation leaves it out; the cards
shown and neither clicked nor bought are recorded as ignores (score 0) when the
shopper asks for new recommendations. The history panel reads the customer's
interactions back online, and the session panel counts what was shown and done.

A custom Hopsworks app: one process serving a JSON API under /api and a static
JavaScript UI, bound to 0.0.0.0:$APP_PORT, with /health for the readiness probe.
The UI calls the API with relative URLs, so the Hopsworks proxy mount
(/hopsworks-api/pythonapp/<project>/<app>/) works without the app knowing it.
"""

from __future__ import annotations

import math
import os
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

STATIC = Path(__file__).resolve().parent / "static"
DEPLOYMENT = "recsexample"
SCORES = {"ignore": 0, "click": 1, "buy": 2}

app = FastAPI(title="Storefront", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@lru_cache(maxsize=1)
def _project():
    # Deferred so the app starts and answers /health before the SDK connects.
    import hopsworks

    return hopsworks.login()


def _group(name: str):
    return _project().get_feature_store().get_feature_group(name, version=1)


class Ask(BaseModel):
    """A request for recommendations."""

    customer_id: str = Field(min_length=1, max_length=64)
    k: int = Field(default=12, ge=1, le=50)


class Action(BaseModel):
    """What the shopper did with one or more articles."""

    customer_id: str = Field(min_length=1, max_length=64)
    kind: Literal["click", "buy", "ignore"]
    article_ids: list[str] = Field(min_length=1, max_length=50)
    prev_article_id: str = "START"


def interaction_rows(action: Action, now: datetime) -> list[dict]:
    """The interactions rows for an action; each row's prev_article_id chains to the last."""
    rows, prev = [], action.prev_article_id
    for i, article in enumerate(action.article_ids):
        # One microsecond apart: several ignores at once must not share a key.
        t = now + timedelta(microseconds=i)
        rows.append(
            {
                "t_dat": t.replace(tzinfo=None),
                "customer_id": action.customer_id,
                "article_id": article,
                "interaction_score": SCORES[action.kind],
                "prev_article_id": prev,
            }
        )
        prev = article
    return rows


def purchase_row(action: Action, now: datetime) -> dict:
    angle = now.month * (2 * math.pi / 12)
    return {
        "t_dat": now.replace(tzinfo=None),
        "customer_id": action.customer_id,
        "article_id": action.article_ids[0],
        # The catalogue has no price; the models read none.
        "price": 0.0,
        "sales_channel_id": 2,
        "month_sin": math.sin(angle),
        "month_cos": math.cos(angle),
    }


def _insert(name: str, rows: list[dict]) -> None:
    import pandas as pd

    # Online now, offline at the group's next materialization: a job per click
    # would take minutes and a Spark application each.
    _group(name).insert(pd.DataFrame(rows), write_options={"start_offline_materialization": False})


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the deployment is checked by /api/recommend."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/customers")
@lru_cache(maxsize=1)
def customers() -> list[dict]:
    """The 200 most active customers to pick from, with their age and purchases; read once."""
    purchases = _group("transactions").select(["customer_id"]).read(dataframe_type="pandas")
    counts = purchases["customer_id"].value_counts().head(200)
    people = _group("customers").select(["customer_id", "age"]).read(dataframe_type="pandas")
    ages = dict(zip(people["customer_id"], people["age"], strict=False))
    return [{"customer_id": c, "purchases": int(n), "age": ages.get(c)} for c, n in counts.items()]


@app.post("/api/recommend")
def recommend(ask: Ask) -> dict:
    """The deployment's ranked products for one customer."""
    deployment = _project().get_model_serving().get_deployment(DEPLOYMENT)
    if deployment is None:
        raise HTTPException(status_code=503, detail=f"the deployment {DEPLOYMENT} is not deployed")
    try:
        reply = deployment.predict(data={"instances": [ask.model_dump()]})
    except Exception as exc:  # noqa: BLE001 - shown to the user as the deployment's error
        raise HTTPException(status_code=502, detail=f"the deployment failed: {exc}") from exc
    predictions = reply.get("predictions", reply) if isinstance(reply, dict) else reply
    return predictions[0] if isinstance(predictions, list) else predictions


@app.post("/api/interactions")
def interact(action: Action) -> dict:
    """Record a click, a purchase, or the ignored cards of the last recommendation."""
    now = datetime.now(UTC)
    rows = interaction_rows(action, now)
    _insert("interactions", rows)
    if action.kind == "buy":
        _insert("transactions", [purchase_row(action, now)])
    return {"recorded": len(rows)}


@app.get("/api/history/{customer_id}")
def history(customer_id: str) -> list[dict]:
    """The customer's 30 most recent interactions, newest first, with the article's name."""
    fg = _group("interactions")
    events = fg.filter(fg.customer_id == customer_id).read(online=True, dataframe_type="pandas")
    if events.empty:
        return []
    events = events.sort_values("t_dat", ascending=False).head(30)
    articles = _group("articles")
    names = (
        articles.select(["article_id", "prod_name", "image_url"])
        .filter(articles.article_id.isin(events["article_id"].unique().tolist()))
        .read(online=True, dataframe_type="pandas")
    )
    events = events.merge(names, on="article_id", how="left")
    return [
        {
            "t_dat": str(row["t_dat"]),
            "article_id": row["article_id"],
            "interaction_score": int(row["interaction_score"]),
            "prod_name": row.get("prod_name") if isinstance(row.get("prod_name"), str) else None,
            "image_url": row.get("image_url") if isinstance(row.get("image_url"), str) else None,
        }
        for _, row in events.iterrows()
    ]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

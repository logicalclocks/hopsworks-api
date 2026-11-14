# ruff: noqa: INP001
"""Ask the shop.

A chat over the ecommerce semantic layer. A person asks about the shop's sales in plain language, such
as "net revenue by region per month this year" or "which customer segment had the highest average
order value last quarter?", and the app has an LLM translate the question into a semantic layer query,
checks every name in it against the semantic project's catalog, runs it, and shows the answer as a
chart and a table, with the query, one sentence on what it shows, and its SQL. A follow-up ("only
Europe", "by quarter instead") refines the previous query. The query of an answer can be edited in
place (metrics, group-bys, filters, time range) and run again without the LLM, and the catalog panel
lists every metric and dimension with its description.

The LLM only writes the query: it sees the catalog and the question, never a data row, and the filters
it writes are bound as literals, never run as SQL (translator.py). Queries run on Trino as the member
who started the app, so they see what that member can read.

The LLM is any OpenAI-compatible chat endpoint, from the account environment variables of the member
who started the app: LLM_URL (its base URL, such as https://api.openai.com/v1), LLM_MODEL and
LLM_API_KEY. Without them the app says so and still runs queries edited by hand.
The semantic project is named in app.json, beside this file.

A custom Hopsworks app: one process serving a JSON API under /api and a static JavaScript UI, bound
to 0.0.0.0:$APP_PORT, with /health for the readiness probe. The UI calls the API with relative URLs,
so the Hopsworks proxy mount (/hopsworks-api/pythonapp/<project>/<app>/) works without the app
knowing it.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import translator

HERE = Path(__file__).resolve().parent
STATIC = HERE / "static"
SETTINGS = json.loads((HERE / "app.json").read_text(encoding="utf-8"))
# A new build can add metrics: the catalog is read again after this long.
CATALOG_SECONDS = 600
LLM_TIMEOUT_S = 90

app = FastAPI(title="Ask the shop", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")
_catalog: dict[str, Any] = {}
_catalog_lock = threading.Lock()


class Ask(BaseModel):
    question: str = Field(min_length=1, max_length=2000)
    history: list[dict[str, Any]] = Field(default_factory=list, max_length=20)


class Run(BaseModel):
    query: dict[str, Any]


@lru_cache(maxsize=1)
def _semantic_project():
    # Deferred so the app starts and answers /health before the SDK connects.
    import hopsworks

    project = hopsworks.login()
    found = project.get_semantic_layer_api().get_project(SETTINGS["semantic_project"])
    if found is None:
        raise HTTPException(
            503, f"No semantic project {SETTINGS['semantic_project']} in {project.name}"
        )
    return found


def catalog() -> translator.Catalog:
    with _catalog_lock:
        if _catalog.get("until", 0) < time.monotonic():
            _catalog["value"] = translator.catalog_from(
                _semantic_project(), SETTINGS.get("examples", [])
            )
            _catalog["until"] = time.monotonic() + CATALOG_SECONDS
        return _catalog["value"]


def _unit(metric: dict[str, Any]) -> str:
    """How the UI formats a metric: money, a share shown as a percentage, or a count."""
    text = metric["description"]
    if "USD" in text:
        return "usd"
    if "from 0 to 1" in text or "as a share" in text:
        return "share"
    return "number"


def _llm_settings() -> tuple[str, str, str] | None:
    url, model, key = (
        os.environ.get(k, "").strip() for k in ("LLM_URL", "LLM_MODEL", "LLM_API_KEY")
    )
    return (url, model, key) if url and model and key else None


def _chat(messages: list[dict[str, str]]) -> str:
    """One reply of the OpenAI-compatible endpoint, at temperature 0."""
    url, model, key = _llm_settings()
    url = url.rstrip("/")
    if not url.endswith("/chat/completions"):
        url += "/chat/completions"
    reply = requests.post(
        url,
        headers={"Authorization": f"Bearer {key}"},
        json={"model": model, "messages": messages, "temperature": 0},
        timeout=LLM_TIMEOUT_S,
    )
    if reply.status_code >= 400:
        raise HTTPException(
            502, f"The LLM at {url} answered {reply.status_code}: {reply.text[:300]}"
        )
    return reply.json()["choices"][0]["message"]["content"] or ""


def _plain(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def _run(query: dict[str, Any]) -> dict[str, Any]:
    """Rows and SQL of a checked query."""
    project = _semantic_project()
    args = translator.sdk_arguments(query)
    rows = project.query(**args, dataframe_type="python")
    columns = [*query["group_by"], *query["metrics"]]
    return {
        "columns": columns,
        "rows": [[_plain(row.get(c)) for c in columns] for row in rows],
        "sql": project.sql(**args),
    }


def _answer(
    query: dict[str, Any], result: dict[str, Any], attempts: int
) -> dict[str, Any]:
    metrics = catalog().metrics
    return {
        "kind": "answer",
        "query": query,
        "attempts": attempts,
        "units": {m: _unit(metrics[m]) for m in query["metrics"]},
        "labels": {m: metrics[m]["label"] for m in query["metrics"]},
        **result,
    }


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the semantic layer is checked by the API routes."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/catalog")
def get_catalog() -> dict:
    """The metrics and dimensions a question can use, the example questions, and whether an LLM is set."""
    found = catalog()
    settings = _llm_settings()
    return {
        "semantic_project": SETTINGS["semantic_project"],
        "llm": {
            "configured": settings is not None,
            "model": settings[1] if settings else None,
        },
        "examples": found.examples,
        "metrics": [
            {"name": n, "unit": _unit(m), **m} for n, m in sorted(found.metrics.items())
        ],
        "dimensions": [{"name": n, **d} for n, d in sorted(found.dimensions.items())],
    }


@app.post("/api/ask")
def ask(body: Ask) -> dict:
    """A question in plain language, translated into a semantic layer query and run."""
    if _llm_settings() is None:
        raise HTTPException(
            503,
            "No LLM is configured: set LLM_URL, LLM_MODEL and LLM_API_KEY in your account settings "
            "(Environment variables) and restart the app. Queries edited by hand still run.",
        )
    found = catalog()
    outcome = translator.translate(
        _chat,
        found,
        body.question,
        body.history,
        _run,
        dt.datetime.now(dt.timezone.utc).date(),
    )
    if "clarify" in outcome:
        return {
            "kind": "clarify",
            "message": outcome["clarify"],
            "attempts": outcome["attempts"],
        }
    if "error" in outcome:
        return {
            "kind": "error",
            "message": outcome["error"],
            "attempts": outcome["attempts"],
        }
    return _answer(outcome["query"], outcome["result"], outcome["attempts"])


@app.post("/api/run")
def run(body: Run) -> dict:
    """A query edited in the UI, checked and run without the LLM."""
    query, problems = translator.check(body.query, catalog())
    if problems:
        raise HTTPException(400, "; ".join(problems))
    try:
        return _answer(query, _run(query), 0)
    except HTTPException:
        raise
    except Exception as err:  # noqa: BLE001 - the gateway's reason is the answer
        raise HTTPException(400, str(err)) from err


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

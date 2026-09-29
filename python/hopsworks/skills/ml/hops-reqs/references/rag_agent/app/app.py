# ruff: noqa: INP001
"""Help desk.

A support chat over the help desk agent. A person picks a user id from the ones
that have events, writes a question and presses Send; the app sends the user id
and the question to the agent deployment and shows its answer, the document
passages it cited (name, page, paragraph, a link to the document) and the
user's recent events it read.

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
from pydantic import BaseModel, Field


STATIC = Path(__file__).resolve().parent / "static"
EVENTS = {"name": "user_events", "version": 1}
AGENT = "helpdeskagent"

app = FastAPI(title="Help desk", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@lru_cache(maxsize=1)
def _project():
    # Deferred so the app starts and answers /health before the SDK connects.
    import hopsworks

    return hopsworks.login()


class Question(BaseModel):
    """What the UI sends."""

    user_id: int
    query: str = Field(min_length=1, max_length=2000)
    k: int = Field(default=25, ge=1, le=100)


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the agent is checked by /api/ask."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/users")
def users() -> list[int]:
    """The user ids that have events, to pick from."""
    fs = _project().get_feature_store()
    fg = fs.get_feature_group(EVENTS["name"], version=EVENTS["version"])
    ids = fg.select(["user_id"]).read(dataframe_type="pandas")["user_id"]
    return sorted(int(i) for i in ids.unique())


@app.post("/api/ask")
def ask(question: Question) -> dict:
    """The agent's answer to one question from one user."""
    deployment = _project().get_model_serving().get_deployment(AGENT)
    if deployment is None:
        raise HTTPException(
            status_code=503, detail=f"the agent {AGENT} is not deployed"
        )
    try:
        # The SDK sends KServe's v1 shape; the agent takes the first instance.
        reply = deployment.predict(data={"instances": [question.model_dump()]})
    except Exception as exc:  # noqa: BLE001 - shown to the user as the agent's error
        raise HTTPException(status_code=502, detail=f"the agent failed: {exc}") from exc
    # A KServe v1 reply wraps the result in `predictions`.
    return reply.get("predictions", reply) if isinstance(reply, dict) else reply


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

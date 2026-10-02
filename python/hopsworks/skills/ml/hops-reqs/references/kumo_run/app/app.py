# ruff: noqa: INP001
"""Hops Run with NVIDIA Kumo Tabular as its pilot.

Hops Run is a low-poly racer: the hops flies a generated track through rows of walls,
low blocks and bars, and a player changes lane, jumps and ducks to get through. A player
can fly it, or watch Kumo Tabular fly it (?pilot=kumo): the page sends every game state to
/api/decide, which puts it to the Kumo Tabular deployment with a context of game
situations labelled by the rules, and steers by the move the model finds most probable.
Kumo Tabular is a pretrained in-context learner, so nothing is trained: the context in each
request is its training set. Players and the model share one leaderboard.

A custom Hopsworks app: one process serving the game, its JSON API under /api, and /health
for the readiness probe, bound to 0.0.0.0:$APP_PORT. The page calls the API with relative
URLs, so the Hopsworks proxy mount (/hopsworks-api/pythonapp/<project>/<app>/) works
without the app knowing it.

Settings (env): DEPLOYMENT (the Kumo Tabular deployment, default runexample), BOARD_FILE
(where the leaderboard is kept; default Resources/hops-run/board.json in the project's
HopsFS mount, in memory when there is none), BOARD_SIZE (rows shown, default 10).
"""

from __future__ import annotations

import hashlib
import html
import json
import math
import os
import re
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import game_rules
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

STATIC = Path(__file__).resolve().parent / "static"
# The game's version, shown in the HUD and kept with every run: hops-run v1.8.0, whose
# track and physics game.js carries.
VERSION = "1.8.0"
DEPLOYMENT = os.environ.get("DEPLOYMENT", "runexample")
BOARD_SIZE = int(os.environ.get("BOARD_SIZE", "10"))
DEFAULT_BOARD = Path("/hopsfs/Resources/hops-run/board.json")
PILOTS = ["kumo"]
NAME = re.compile(r"^[\w .-]{1,20}$")
RUN_KEY = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
# The furthest the hops can fly in a run's time: the speed curve, plus a boost gate at most
# every gate_gap metres, with a margin. Mirrored from game.js (SPEED, BOOST, PAD_GAP).
PHYSICS = {
    "start": 45,
    "max": 160,
    "gain": 1.6,
    "kick": 45,
    "decay": 18,
    "gate_gap": 140,
    "margin": 1.05,
}
CLOCK_SLACK_MS = 3000
STARTS_KEPT_S = 24 * 3600

app = FastAPI(title="Hops Run", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def max_distance(duration_ms: float) -> float:
    p, t = PHYSICS, duration_ms / 1000
    ramp = (p["max"] - p["start"]) / p["gain"]
    if t <= ramp:
        base = p["start"] * t + p["gain"] * t * t / 2
    else:
        base = p["start"] * ramp + p["gain"] * ramp * ramp / 2 + p["max"] * (t - ramp)
    per_gate = p["kick"] ** 2 / (2 * p["decay"])
    return (base + per_gate) / (1 - per_gate / p["gate_gap"]) * p["margin"]


# region The leaderboard


class Board:
    """Every run, best first, kept in a JSON file when there is somewhere to keep it."""

    def __init__(self, path: Path | None):
        self.path = path
        self.lock = threading.Lock()
        self.runs: list[dict] = []
        if path and path.exists():
            self.runs = json.loads(path.read_text())

    def _save(self) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Written beside and renamed over, so a crash mid-write never leaves half a board.
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.runs))
        tmp.replace(self.path)

    def find(self, run_key: str) -> dict | None:
        return next((r for r in self.runs if r.get("run_key") == run_key), None)

    def add(self, run: dict) -> dict:
        """Records `run` unless its key is already on the board; returns the recorded run."""
        with self.lock:
            known = self.find(run["run_key"])
            if known:
                return known
            self.runs.append(run)
            self.runs.sort(key=lambda r: (-r["distance_m"], r["created_at"]))
            self._save()
            return run

    def rank(self, run: dict) -> int:
        return self.runs.index(run) + 1

    def top(self) -> list[dict]:
        """The best runs, then each model pilot missing from them with its best run and its place."""
        rows = [{**r, "rank": i + 1} for i, r in enumerate(self.runs[:BOARD_SIZE])]
        for pilot in PILOTS:
            if any(r["pilot"] == pilot for r in rows):
                continue
            best = next((r for r in self.runs if r["pilot"] == pilot), None)
            if best:
                rows.append({**best, "rank": self.rank(best), "below": True})
        return rows


def _board_path() -> Path | None:
    if os.environ.get("BOARD_FILE"):
        return Path(os.environ["BOARD_FILE"])
    return DEFAULT_BOARD if DEFAULT_BOARD.parents[1].is_dir() else None


board = Board(_board_path())
started_runs: dict[str, float] = {}

ROBOT = (
    '<svg class="bot" viewBox="0 0 16 16" aria-label="model"><path d="M8 1.5V4M3 4h10v8.5H3z'
    'M6 7.25h.5M9.5 7.25h.5M6 10h4M1.5 7v3M14.5 7v3"/></svg>'
)
MAKER = (
    '<a href="https://huggingface.co/nvidia/Kumo-Tabular" target="_blank" rel="noopener">NVIDIA</a>'
)


def _row(r: dict) -> str:
    esc = html.escape
    who = esc(r["name"])
    if r["pilot"] != "player":
        model = f" · {esc(r['model'])}" if r.get("model") else ""
        who = f'{ROBOT}{who} <i class="pilot">{esc(r["pilot"])}{model} · by {MAKER}</i>'
    below = ' class="below"' if r.get("below") else ""
    return (
        f'<li data-rank="{r["rank"]}"{below}><span class="rank">{r["rank"]:02d}</span>'
        f'<span class="who">{who}</span><span class="dist">{r["distance_m"]} m '
        f'<i class="ver">v{esc(r.get("game_version", "?"))}</i></span></li>'
    )


def board_html(rows: list[dict]) -> str:
    if not rows:
        return '<li class="empty">No runs yet. Be the first.</li>'
    out = []
    for i, r in enumerate(rows):
        if r.get("below") and not (i and rows[i - 1].get("below")):
            out.append('<li class="gap" aria-hidden="true">···</li>')
        out.append(_row(r))
    return "".join(out)


# endregion

# region The pilot

_lock = threading.Lock()
_deployment: dict = {}
CONTEXT, HELD_OUT = game_rules.split()
_round_trips: list[float] = []


def _kumo():
    """The Kumo Tabular deployment, looked up once."""
    with _lock:
        if not _deployment:
            import hopsworks

            deployment = (
                hopsworks.login(engine="python").get_model_serving().get_deployment(DEPLOYMENT)
            )
            if deployment is None:
                raise HTTPException(status_code=503, detail=f"no deployment named {DEPLOYMENT}")
            _deployment.update(
                deployment=deployment,
                model=f"{deployment.model_name} v{deployment.model_version}",
            )
        return _deployment


def classify(rows: list[dict]) -> list[dict]:
    """Kumo's class probabilities for `rows`, with the labelled situations as its context."""
    kumo = _kumo()
    reply = kumo["deployment"].predict(
        inputs=[{"context": CONTEXT, "query": rows, "target": game_rules.TARGET}]
    )
    predictions = reply["predictions"]
    # The predictor answers {"predictions": [...]}, which the server may wrap once more.
    if isinstance(predictions, dict):
        predictions = predictions["predictions"]
    answer = predictions[0]
    return [
        {
            "probabilities": dict(zip(answer["classes"], row, strict=True)),
            "seconds": answer["seconds"],
        }
        for row in answer["probabilities"]
    ]


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)]


# endregion


def _client(request: Request) -> str:
    address = request.headers.get("x-forwarded-for", request.client.host if request.client else "")
    return hashlib.sha256(address.split(",")[0].strip().encode()).hexdigest()[:16]


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the deployment is looked up with the first decision."""
    return {
        "status": "ok",
        "version": VERSION,
        "runs": len(board.runs),
        "decisions": len(_round_trips),
        "p50_ms": percentile(_round_trips, 0.5),
        "p99_ms": percentile(_round_trips, 0.99),
    }


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    page = (STATIC / "page.html").read_text()
    return page.replace("{{VERSION}}", VERSION).replace("{{BOARD}}", board_html(board.top()))


@app.post("/api/decide")
async def decide(request: Request) -> dict:
    """The move probabilities for a game state, from Kumo Tabular."""
    state = await request.json()
    try:
        row, allowed = game_rules.query(state)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    started = time.perf_counter()
    [answer] = await _in_thread(classify, [row])
    round_trip = (time.perf_counter() - started) * 1000
    _round_trips.append(round_trip)
    del _round_trips[:-500]
    # Moves the hops cannot make here (a lane change off the track, a jump in the air) are
    # masked out, and the rest renormalised.
    scores = [answer["probabilities"].get(m, 0.0) for m in allowed]
    total = sum(scores) or 1.0
    return {
        "pilot": "kumo",
        "moves": allowed,
        "probabilities": [s / total for s in scores],
        "forwardMs": round_trip,
        "model": f"{_kumo()['model']} · {answer['seconds'] * 1000:.0f} ms in the model",
    }


async def _in_thread(fn, *args):
    import anyio

    return await anyio.to_thread.run_sync(fn, *args)


@app.post("/api/seat")
async def seat(request: Request) -> dict:
    """Every page plays at once: one app serves a project, not the internet."""
    body = await request.json()
    return {
        "id": body.get("id") or str(uuid.uuid4()),
        "state": "play",
        "heartbeatMs": 10_000,
    }


@app.post("/api/seat/leave")
def seat_leave() -> Response:
    return Response(status_code=204)


@app.get("/api/board")
def get_board() -> dict:
    rows = board.top()
    return {"runs": rows, "html": board_html(rows)}


@app.post("/api/runs/start")
def start_run() -> dict:
    """A key for the run taking off, timed from now: the run posted with it may not last longer."""
    now = time.time()
    for key, at in list(started_runs.items()):
        if now - at > STARTS_KEPT_S:
            del started_runs[key]
    run_key = str(uuid.uuid4())
    started_runs[run_key] = now
    return {"runKey": run_key}


@app.post("/api/runs")
async def post_run(request: Request) -> JSONResponse:
    body = await request.json()
    name = str(body.get("name", "")).strip()
    try:
        distance, duration = int(body.get("distance")), int(body.get("durationMs"))
    except (TypeError, ValueError):
        distance = duration = -1
    run_key = str(body.get("runKey") or "")
    pilot = str(body.get("pilot") or "player")
    error = None
    if not NAME.match(name):
        error = "Name: 1 to 20 letters, digits, spaces, dots, dashes or underscores."
    elif distance < 0 or duration <= 0:
        error = "Distance and duration must be positive numbers."
    elif distance > max_distance(duration):
        error = "That run is further than the hops can fly in its time."
    elif not RUN_KEY.match(run_key):
        error = "This page is out of date. Reload it to put runs on the board."
    elif pilot not in ["player", *PILOTS]:
        error = f"Pilot: player or {', '.join(PILOTS)}."
    elif pilot == "player" and not board.find(run_key):
        # A player's run is timed by the server; a model pilot draws its own key.
        at = started_runs.get(run_key)
        if at is None:
            error = "Unknown run. Fly a run to put it on the board."
        elif duration > (time.time() - at) * 1000 + CLOCK_SLACK_MS:
            error = "That run lasted longer than the time since it took off."
    if error:
        return JSONResponse({"error": error}, status_code=400)

    run = board.add(
        {
            "name": name,
            "pilot": pilot,
            "model": str(body.get("model") or "")[:64]
            or (_deployment.get("model") if pilot != "player" else None),
            "distance_m": distance,
            "duration_ms": duration,
            "game_version": VERSION,
            "run_key": run_key,
            "client": _client(request),
            "created_at": datetime.now(UTC).isoformat(),
        }
    )
    rows = board.top()
    if pilot != "player":
        own = [r for r in board.runs if r["pilot"] == pilot]
        return JSONResponse(
            {
                "number": len(own),
                "best": own[0]["distance_m"],
                "runs": rows,
                "html": board_html(rows),
            }
        )
    return JSONResponse({"rank": board.rank(run), "runs": rows, "html": board_html(rows)})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

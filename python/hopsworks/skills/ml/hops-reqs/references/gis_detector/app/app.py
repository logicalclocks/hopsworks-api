# ruff: noqa: INP001
"""Military infrastructure finder.

A map of Sweden on satellite imagery, with aircraft, helicopters, ships,
harbours, storage tanks, bridges and large vehicles outlined by an aerial object
detection model. A person picks an example place or pans and zooms; after each
move the page draws the map as it is on screen into an image and posts it to
/api/detect, which runs the model on it and returns each object's class and its
rotated box's corners in the image's pixels, and the page draws them on the map.

The model is embedded: `infrastructure_detector` (YOLO26-S OBB, trained on DOTA,
ONNX) is downloaded from the Model Registry once, when the first image arrives,
and run in this process with onnxruntime. There is no deployment.

A custom Hopsworks app: one process serving a JSON API under /api and a static
JavaScript UI, bound to 0.0.0.0:$APP_PORT, with /health for the readiness probe.
The UI calls the API with relative URLs, so the Hopsworks proxy mount
(/hopsworks-api/pythonapp/<project>/<app>/) works without the app knowing it.
"""

from __future__ import annotations

import base64
import io
import json
import math
import os
import threading
import time
from pathlib import Path

import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

STATIC = Path(__file__).resolve().parent / "static"
MODEL = "infrastructure_detector"
# Windows of the model's input size, overlapping so an object on a seam is whole
# in one of them.
OVERLAP = 128
MAX_PIXELS = 4096 * 4096
# Example places, public in Swedish Armed Forces and aviation listings, centred
# where the detector finds the most objects in a screen at zoom 17.
LOCATIONS = [
    {"name": "Karlskrona naval base", "lat": 56.1680, "lon": 15.5902, "zoom": 17},
    {"name": "Stockholm Arlanda airport", "lat": 59.6540, "lon": 17.9342, "zoom": 17},
    {"name": "Göteborg oil harbour (Skarvik)", "lat": 57.6975, "lon": 11.8659, "zoom": 17},
    {"name": "Malmen air base, Linköping", "lat": 58.4102, "lon": 15.5251, "zoom": 17},
    {"name": "Muskö naval base", "lat": 58.9962, "lon": 18.1470, "zoom": 17},
]

app = FastAPI(title="Military infrastructure finder", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


# region Detection


def windows(width: int, height: int, size: int, overlap: int = OVERLAP) -> list[tuple[int, int]]:
    """Top-left corners of `size`-pixel windows covering a `width` by `height` image."""

    def starts(length: int) -> list[int]:
        # The fewest windows that overlap by at least `overlap`, spread evenly.
        if length <= size:
            return [0]
        last = length - size
        count = 1 + math.ceil(last / (size - overlap))
        return [round(i * last / (count - 1)) for i in range(count)]

    return [(x, y) for y in starts(height) for x in starts(width)]


def to_input(window: np.ndarray, size: int) -> np.ndarray:
    """An HxWx3 uint8 window as the model's 1x3xSIZExSIZE float input, padded with grey."""
    padded = np.full((size, size, 3), 114, dtype=np.uint8)
    padded[: window.shape[0], : window.shape[1]] = window
    return padded.transpose(2, 0, 1)[None].astype(np.float32) / 255.0


def decode(output: np.ndarray, threshold: float, classes: int) -> np.ndarray:
    """YOLO OBB rows to [cx, cy, w, h, angle, score, class] above `threshold`.

    A row of output0 is cx, cy, w, h, one score per class, and the angle in
    radians; the row's class is its best-scoring one.
    """
    rows = output[0].T
    scores = rows[:, 4 : 4 + classes]
    best = scores.argmax(axis=1)
    score = scores[np.arange(len(rows)), best]
    keep = score >= threshold
    rows, best, score = rows[keep], best[keep], score[keep]
    return np.column_stack([rows[:, :4], rows[:, 4 + classes], score, best])


def corners(boxes: np.ndarray) -> np.ndarray:
    """The four corners, Nx4x2, of rotated boxes [cx, cy, w, h, angle, ...]."""
    cx, cy, w, h, angle = (boxes[:, i] for i in range(5))
    cos, sin = np.cos(angle), np.sin(angle)
    along = np.stack([w / 2 * cos, w / 2 * sin], axis=1)
    across = np.stack([-h / 2 * sin, h / 2 * cos], axis=1)
    centre = np.stack([cx, cy], axis=1)
    return np.stack(
        [
            centre + along + across,
            centre - along + across,
            centre - along - across,
            centre + along - across,
        ],
        axis=1,
    )


def nms(boxes: np.ndarray, iou: float) -> np.ndarray:
    """Per class, the boxes no better box overlaps by more than `iou`, best first.

    Overlap is measured on each rotated box's axis-aligned bounds, which is close
    enough for objects that are seldom packed at an angle to each other.
    """
    if not len(boxes):
        return boxes
    points = corners(boxes)
    bounds = np.column_stack([points.min(axis=1), points.max(axis=1)])
    order = boxes[:, 5].argsort()[::-1]
    boxes, bounds = boxes[order], bounds[order]
    area = (bounds[:, 2] - bounds[:, 0]) * (bounds[:, 3] - bounds[:, 1])
    suppressed = np.zeros(len(boxes), dtype=bool)
    keep = []
    for i in range(len(boxes)):
        if suppressed[i]:
            continue
        keep.append(i)
        rest = slice(i + 1, None)
        x0 = np.maximum(bounds[i, 0], bounds[rest, 0])
        y0 = np.maximum(bounds[i, 1], bounds[rest, 1])
        x1 = np.minimum(bounds[i, 2], bounds[rest, 2])
        y1 = np.minimum(bounds[i, 3], bounds[rest, 3])
        inter = np.clip(x1 - x0, 0, None) * np.clip(y1 - y0, 0, None)
        overlap = inter / (area[i] + area[rest] - inter + 1e-9)
        same_class = boxes[rest, 6] == boxes[i, 6]
        suppressed[rest] |= same_class & (overlap > iou)
    return boxes[keep]


def detect(
    image: np.ndarray, run, size: int, threshold: float, classes: int, iou: float = 0.5
) -> np.ndarray:
    """Rotated boxes [cx, cy, w, h, angle, score, class] in `image`'s pixels.

    `run` maps the model's input to its output0.
    """
    height, width = image.shape[:2]
    found = []
    for x, y in windows(width, height, size):
        window = image[y : y + size, x : x + size]
        boxes = decode(run(to_input(window, size)), threshold, classes)
        boxes[:, 0] += x
        boxes[:, 1] += y
        found.append(boxes)
    boxes = np.concatenate(found) if found else np.zeros((0, 7))
    return nms(boxes, iou)


# endregion

# region The embedded model

_lock = threading.Lock()
_model: dict = {}


def _detector() -> dict:
    """The registry's latest `infrastructure_detector`, loaded into onnxruntime once."""
    with _lock:
        if not _model:
            import hopsworks
            import onnxruntime as ort

            registry = hopsworks.login().get_model_registry()
            models = registry.get_models(MODEL)
            if not models:
                raise HTTPException(status_code=503, detail=f"{MODEL} is not registered")
            model = max(models, key=lambda m: m.version)
            directory = Path(model.download())
            spec = json.loads((directory / "detector.json").read_text())
            options = ort.SessionOptions()
            # The app pod's CPU limit, not the node's cores: more threads only contend.
            options.intra_op_num_threads = 2
            session = ort.InferenceSession(
                str(directory / "detector.onnx"), options, providers=["CPUExecutionProvider"]
            )
            name = session.get_inputs()[0].name
            _model.update(
                run=lambda tensor: session.run(None, {name: tensor})[0],
                spec=spec,
                version=model.version,
            )
        return _model


# endregion


class Screen(BaseModel):
    """The map as shown, drawn into an image."""

    image: str = Field(min_length=32, description="A JPEG or PNG data URL or base64 string")
    threshold: float = Field(default=0.35, ge=0.05, le=0.95)


def read_image(data: str) -> np.ndarray:
    """A data URL or base64 image as an HxWx3 uint8 array."""
    from PIL import Image

    raw = base64.b64decode(data.split(",", 1)[-1], validate=False)
    with Image.open(io.BytesIO(raw)) as picture:
        if picture.width * picture.height > MAX_PIXELS:
            raise HTTPException(status_code=413, detail="the image is too large")
        return np.asarray(picture.convert("RGB"))


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the model is loaded with the first image."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/locations")
def locations() -> list[dict]:
    """The example places to jump to."""
    return LOCATIONS


@app.post("/api/detect")
def detect_objects(screen: Screen) -> dict:
    """The objects in the image, with their class and rotated box, and each step's time."""
    started = time.perf_counter()
    try:
        image = read_image(screen.image)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - not an image
        raise HTTPException(status_code=422, detail=f"not an image: {exc}") from exc
    decoded = time.perf_counter()
    model = _detector()
    spec = model["spec"]
    loaded = time.perf_counter()
    names = spec["classes"]
    shown = set(spec.get("of_interest", names))
    boxes = detect(image, model["run"], spec["image_size"], screen.threshold, len(names))
    boxes = boxes[[names[int(c)] in shown for c in boxes[:, 6]]] if len(boxes) else boxes
    done = time.perf_counter()
    objects = [
        {
            "label": names[int(box[6])],
            "score": round(float(box[5]), 3),
            "corners": [[round(float(x), 1), round(float(y), 1)] for x, y in points],
        }
        for box, points in zip(boxes, corners(boxes), strict=True)
    ]
    return {
        "width": int(image.shape[1]),
        "height": int(image.shape[0]),
        "objects": objects,
        "model": {"name": MODEL, "version": model["version"], "source": spec["source"]},
        "timings_ms": {
            "decode": round((decoded - started) * 1000, 1),
            "load": round((loaded - decoded) * 1000, 1),
            "detect": round((done - loaded) * 1000, 1),
        },
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

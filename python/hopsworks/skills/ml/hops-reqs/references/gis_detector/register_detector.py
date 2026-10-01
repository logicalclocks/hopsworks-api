# ruff: noqa: INP001
"""Register a pretrained YOLO26 aerial object detector in the Model Registry, as ONNX.

Run as the job `<slug>-register-detector` in `<slug>-jobs-env`:

    hops job deploy <slug>-register-detector src/<slug_pkg>/register_detector.py \
        --env <slug>-jobs-env --run --wait

Nothing is trained. It downloads `openvision/yolo26-s-obb` (Ultralytics YOLO26-S
with oriented bounding boxes, trained on the DOTA aerial imagery dataset, mAP@0.5
80.9 at 1024 pixels) at a pinned revision from Hugging Face, exports it to ONNX,
and registers `infrastructure_detector` with the ONNX file and `detector.json`,
which names the input size, every class the model knows, and the ones the app
shows: aircraft, helicopters, ships, harbours, storage tanks, bridges and large
vehicles. The app loads the ONNX file with onnxruntime, so it needs neither
PyTorch nor Ultralytics. A second run finds the revision registered and leaves
it. The weights are AGPL-3.0, as Ultralytics' models are.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

REPO = "openvision/yolo26-s-obb"
REVISION = "567278bad4cc58dde7efe31eb58de8c7732c198b"
WEIGHTS = "model.pt"
IMAGE_SIZE = 1024
# DOTA v1's classes, in the model's order.
CLASSES = [
    "plane",
    "ship",
    "storage tank",
    "baseball diamond",
    "tennis court",
    "basketball court",
    "ground track field",
    "harbor",
    "bridge",
    "large vehicle",
    "small vehicle",
    "helicopter",
    "roundabout",
    "soccer ball field",
    "swimming pool",
]
# What the app shows: infrastructure of military interest, not sports grounds.
OF_INTEREST = ["plane", "helicopter", "ship", "harbor", "storage tank", "bridge", "large vehicle"]


def detector_spec(revision: str, image_size: int = IMAGE_SIZE) -> dict:
    """What the app needs to run the exported model: input size, classes and source."""
    return {
        "source": f"hf:{REPO}@{revision}",
        "image_size": image_size,
        "classes": CLASSES,
        "of_interest": OF_INTEREST,
        "format": "onnx",
        "outputs": "YOLO OBB: output0 rows are cx, cy, w, h, one score per class, angle in radians",
    }


def already_registered(registry, name: str, revision: str) -> bool:
    """Whether a version of `name` was registered from `revision`; its description says so."""
    try:
        models = registry.get_models(name) or []
    except Exception:  # noqa: BLE001 - no such model yet
        return False
    return any(revision in (model.description or "") for model in models)


def export_onnx(directory: Path, revision: str) -> Path:
    """Download the weights at `revision` and export them to `directory/detector.onnx`."""
    from huggingface_hub import hf_hub_download
    from ultralytics import YOLO

    weights = hf_hub_download(REPO, WEIGHTS, revision=revision, local_dir=directory)
    # simplify=False: simplifying needs onnxslim, which Ultralytics would pip-install
    # into the job's environment at run time.
    exported = YOLO(weights).export(format="onnx", imgsz=IMAGE_SIZE, opset=17, simplify=False)
    target = directory / "detector.onnx"
    Path(exported).rename(target)
    Path(weights).unlink()
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Register the infrastructure detector.")
    parser.add_argument("--name", default="infrastructure_detector")
    parser.add_argument("--revision", default=REVISION)
    args = parser.parse_args(argv)

    import hopsworks

    registry = hopsworks.login().get_model_registry()
    if already_registered(registry, args.name, args.revision):
        print(f"{args.name}: {REPO}@{args.revision} is already registered")
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        export_onnx(directory, args.revision)
        (directory / "detector.json").write_text(json.dumps(detector_spec(args.revision)))
        model = registry.python.create_model(
            name=args.name,
            description=f"YOLO26-S aerial object detector (DOTA), {REPO}@{args.revision}, ONNX",
        )
        model.save(str(directory))
    print(f"{args.name}: registered {REPO}@{args.revision}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

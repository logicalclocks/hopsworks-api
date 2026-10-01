# Building the GIS military infrastructure finder

The real-time example (`gis-example`) is an app with a map of Sweden on
satellite imagery that outlines what is of military interest in view: aircraft,
helicopters, ships, harbours, storage tanks, bridges and large vehicles. It uses
a pretrained aerial object detector, so it has no feature or training pipeline:
the model is a YOLO26 variant downloaded from Hugging Face, and the only data is
the image the page sends. The model is embedded in the app, which loads it from
the Model Registry, so there is no deployment either. It is built from the
reference implementation in [gis_detector/](gis_detector/):

| Phase | Builds | From |
| --- | --- | --- |
| `data` | nothing: `skipped`, the imagery is the map's | this page |
| `features` | nothing: `skipped` | this page |
| `train` | `infrastructure_detector` in the Model Registry, downloaded and exported to ONNX; nothing is trained | `gis_detector/register_detector.py` |
| `infer` | the detector embedded in the app; its latency measured against the SLA | `gis_detector/app/app.py` |
| `app` | the JavaScript map UI | `gis_detector/app/` |

The names are fixed: the app reads `infrastructure_detector`. Copy each file into
the system (`src/<slug_pkg>/register_detector.py`, and `app/` for the app) and
change nothing else: the code is tested as it is.

## data and features: skipped

Record `data: {status: skipped}` and `features: {status: skipped}` with a
`decisions` line: the model is pretrained and reads only the image a request
sends. Nothing is ingested, so no feature group is created.

## The environments

```bash
hops env clone <slug>-jobs-env --from python-feature-pipeline    # the registration job
hops env install <slug>-jobs-env -f requirements.txt
hops env clone <slug>-app-env --from python-agent-pipeline       # the app and its model
hops env install <slug>-app-env -f app/app-requirements.txt
```

`requirements.txt` is `gis_detector/requirements.txt`: Ultralytics with the CPU
builds of torch and torchvision pinned together, and huggingface_hub, for the
download and the export only; a torchvision from PyPI against the CPU torch fails
to load. The app's environment adds only onnxruntime: `python-agent-pipeline`
ships FastAPI, NumPy and Pillow. Clone one environment at a time.

## train: register the pretrained detector

```bash
hops job deploy <slug>-register-detector src/<slug_pkg>/register_detector.py --env <slug>-jobs-env --run --wait
```

It downloads `openvision/yolo26-s-obb` (Ultralytics YOLO26-S with oriented
bounding boxes, trained on the DOTA aerial imagery dataset, mAP@0.5 80.9 at 1024
pixels) at a pinned revision, exports it to ONNX at 1024 by 1024 pixels, and
registers `infrastructure_detector` with `detector.onnx` and `detector.json`: the
input size, DOTA's fifteen classes in the model's order, and the seven the app
shows (`of_interest`); sports grounds, pools, roundabouts and small vehicles are
left out. A second run finds the revision registered and leaves it. Record
`training: {required: false, pretrained: {repo, revision, name:
infrastructure_detector, version}, status: met}`. The weights are AGPL-3.0, as
Ultralytics' models are: say so in the report.

## infer: the embedded model

The app downloads the latest `infrastructure_detector` from the registry when the
first image arrives and runs it in its own process with onnxruntime, two threads.
`POST /api/detect` takes `{image, threshold}`, the screen as a JPEG data URL, and
returns `{width, height, objects: [{label, score, corners}], model, timings_ms}`,
each object's rotated box as its four corners in the image's pixels. A screen
larger than the model's input is read in overlapping 1024-pixel windows at full
resolution, since an aircraft is a few pixels when the screen is shrunk, and the
windows' boxes are merged per class with non-maximum suppression. `measured` gets
the p99 of the `detect` time over 50 screens of the example places against
`requirements.sla.realtime`.

## app: the map

Copy `gis_detector/app/` to `<slug>/app/` and deploy it as **hops-app** says, in
`<slug>-app-env`, with 2048 MB (the app uses about 400 MB) and two cores.
Leaflet is vendored under `static/vendor/` (BSD-2-Clause); the imagery comes from
Esri World Imagery, which serves its tiles with CORS, so the page can draw the
tiles on screen into a canvas and send that image. The viewer's browser needs
access to `server.arcgisonline.com`, and the app's pod to the registry only. A
place picker jumps to air and naval bases and ports in Sweden, centred where the
detector finds objects; after each pan or zoom the page waits for the tiles,
reads the screen and outlines what the model returns, coloured by kind, with a
legend of the counts in view. Below zoom 15 it asks to zoom in instead. `met`
when a detection at an example place returns objects.

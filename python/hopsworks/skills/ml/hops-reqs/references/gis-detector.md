# Building the GIS military infrastructure finder

The real-time example (`gis-example`) is an app with a map of Sweden on
satellite imagery that outlines what is of military interest in view: aircraft,
helicopters, ships, harbours, storage tanks, bridges and large vehicles. It uses
a pretrained aerial object detector, so it has no feature or training pipeline:
the model is a YOLO26 variant downloaded from Hugging Face, and the only data is
the image the page sends. The model is embedded in the app, which loads it from
the Model Registry, so there is no deployment either.

There is no reference code: every file is written from the requirements and this
page, which says what each phase builds, the contracts between them, the facts
the code depends on, and how the build is checked. Write the tests below with the
code, and make them pass before a phase is `met`.

| Phase | Builds |
| --- | --- |
| `data` | nothing: `skipped`, the imagery is the map's |
| `features` | nothing: `skipped` |
| `train` | `infrastructure_detector` in the Model Registry, downloaded and exported to ONNX; nothing is trained (`src/<slug_pkg>/register_detector.py`) |
| `infer` | the detector embedded in the app; its latency measured against the SLA (`app/app.py`) |
| `app` | the JavaScript map UI (`app/static/`) |

The names are fixed: the app reads `infrastructure_detector`, and the app is
`gis-example-app`.

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

Clone one environment at a time. `requirements.txt`, for the download and the
export only, pins these together; each pin is there for a failure seen without it:

| Line | Why |
| --- | --- |
| `--extra-index-url https://download.pytorch.org/whl/cpu` | the CPU builds; nothing here uses a GPU |
| `torch==2.14.1+cpu`, `torchvision==0.29.1+cpu` | pinned as a pair: a PyPI torchvision against the CPU torch fails to load (`operator torchvision::nms does not exist`) |
| `ultralytics==8.4.170`, `onnx==1.23.1` | loads the weights and exports them |
| `huggingface_hub==2.0.0` | the download; Ultralytics does not pull it in and `python-feature-pipeline` lacks it |

`app/app-requirements.txt` is `onnxruntime==1.30.0` only: `python-agent-pipeline`
ships FastAPI, uvicorn, NumPy and Pillow. The app needs neither PyTorch nor
Ultralytics.

## train: register the pretrained detector

```bash
hops job deploy <slug>-register-detector src/<slug_pkg>/register_detector.py --env <slug>-jobs-env --run --wait
```

The job:

- downloads `model.pt` of `openvision/yolo26-s-obb` (Ultralytics YOLO26-S with
  oriented bounding boxes, trained on DOTA v1, mAP@0.5 80.9 at 1024 pixels) at
  revision `567278bad4cc58dde7efe31eb58de8c7732c198b`;
- exports it to ONNX at 1024 by 1024 pixels, opset 17, with `simplify=False`:
  simplifying needs onnxslim, which Ultralytics would otherwise pip-install into
  the job's environment at run time;
- writes `detector.json` beside `detector.onnx`: `source`
  (`hf:<repo>@<revision>`), `image_size` (1024), `classes`, `of_interest`,
  `format` (`onnx`);
- registers both files as `infrastructure_detector` (a Python model), with the
  repo and revision in the description, and does nothing when a version with
  that revision in its description already exists.

`classes` are DOTA v1's fifteen, in the model's order: plane, ship, storage tank,
baseball diamond, tennis court, basketball court, ground track field, harbor,
bridge, large vehicle, small vehicle, helicopter, roundabout, soccer ball field,
swimming pool. `of_interest`, the ones the app shows, are plane, helicopter,
ship, harbor, storage tank, bridge and large vehicle. Record `training:
{required: false, pretrained: {repo, revision, name: infrastructure_detector,
version}, status: met}`. The weights are AGPL-3.0, as Ultralytics' models are:
say so in the report.

## infer: the embedded model

The app loads the latest `infrastructure_detector` from the registry when the
first image arrives (not at start, so `/health` answers at once) and runs it in
its own process with onnxruntime, two intra-op threads: the app pod's CPU limit,
not the node's cores. `POST /api/detect` takes `{image, threshold}` (the screen
as a JPEG data URL; threshold default 0.35, 0.05 to 0.95) and returns `{width,
height, objects: [{label, score, corners}], model: {name, version, source},
timings_ms: {decode, load, detect}}`, each object's rotated box as its four
corners in the image's pixels. An image over 4096 by 4096 pixels gets a 413, one
that is not an image a 422.

The facts the code depends on:

- **Input.** One `1x3x1024x1024` float32 tensor, RGB, scaled to 0..1; a window
  smaller than 1024 pixels is padded with grey (114) at the bottom and right.
- **Output.** `output0` is `[1, 4 + 15 + 1, N]`: per candidate, cx, cy, w, h in
  input pixels, one score per class, and the box's angle in radians. A
  candidate's class is its best-scoring one; keep those at or above the
  threshold.
- **Windows.** A screen larger than the input is read in 1024-pixel windows at
  full resolution, since an aircraft is a few pixels when the screen is shrunk:
  the fewest windows that overlap by at least 128 pixels, spread evenly along
  each axis (the last window ends at the edge, so windows are not left
  under-filled). Each window's boxes are shifted by its offset.
- **Merging.** Non-maximum suppression per class over all windows' boxes, IoU
  0.5, measured on each rotated box's axis-aligned bounds, best score first.
- **Corners.** For a box at (cx, cy) of w by h at angle a: with u = (w/2 cos a,
  w/2 sin a) and v = (-h/2 sin a, h/2 cos a), the corners are c+u+v, c-u+v,
  c-u-v, c+u-v.
- **Comparing with Ultralytics.** Ultralytics treats a NumPy image as BGR: flip
  the channels when checking the ONNX path against `YOLO(...).predict`.

`measured` gets the p99 of the `detect` time over 50 screens of the example
places against `requirements.sla.realtime`.

## app: the map

Deploy `app/` as **hops-app** says, in `<slug>-app-env`, with 2048 MB (the app
uses about 400 MB) and two cores. The page:

- uses Leaflet 1.9.4, served by the app (no CDN): fetch
  `https://registry.npmjs.org/leaflet/-/leaflet-1.9.4.tgz`, check it against its
  published integrity
  `sha512-nxS1ynzJOmOlHp+iL3FyWqK89GtNL8U8rvlMOsQdTTssxZwCXh8N2NB3GDQOL+YR3XnWyZAxwQixURb+FA74PA==`,
  and keep `dist/leaflet.js`, `dist/leaflet.css` and its LICENSE (BSD-2-Clause)
  under `static/vendor/`;
- shows Esri World Imagery,
  `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}`,
  max zoom 19, with `crossOrigin: "anonymous"` and Esri's attribution. Esri
  serves the tiles with CORS, so the page can draw the loaded tiles, where they
  are on screen, into a canvas and send it; the viewer's browser needs access to
  `server.arcgisonline.com`, the app's pod only to the registry;
- offers the places to jump to from `/api/locations`, each at zoom 17, centred
  where the detector finds objects: Karlskrona naval base (56.1680, 15.5902),
  Stockholm Arlanda airport (59.6540, 17.9342), Göteborg oil harbour, Skarvik
  (57.6975, 11.8659), Malmen air base, Linköping (58.4102, 15.5251), and Muskö
  naval base (58.9962, 18.1470);
- after each pan or zoom waits 400 ms and for the tiles to load, reads the
  screen, and outlines what the model returns as polygons coloured by kind, with
  a tooltip of the kind and confidence and a legend of the counts in view; an
  answer that a later move made stale is dropped;
- below zoom 15 asks to zoom in instead, since aircraft and vehicles are then a
  few pixels and the model finds none;
- shows the model's time, the round trip, the image size and the detector's
  version for the last detection, and a confidence slider.

`met` when a detection at an example place returns objects: at Arlanda, planes;
at Karlskrona, ships and harbours.

## Tests the build writes

- **Windows.** One window for an image no larger than the input; windows that
  cover a wide image, overlap by at least 128 pixels, and end at its edges.
- **Decoding.** A synthetic `output0` decodes to the box, score and class it
  holds; candidates below the threshold are dropped.
- **Merging.** Two overlapping boxes of one class keep the better one; the same
  boxes of two classes are both kept.
- **Corners.** An unrotated box's corners are its axis-aligned rectangle.
- **The API.** With a stubbed model, `/api/detect` on a small JPEG returns the
  stub's objects of interest only (a tennis court is dropped), with corners in
  pixels and the three timings; a non-image is a 422; `/health` answers without
  the model.
- **Registration.** A version whose description carries the revision is found;
  another revision is not.

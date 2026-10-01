# Building Hops Run with Kumo Tabular

The real-time example (`run-example`) is Hops Run, a low-poly racer in the
Hopsworks paper style, with NVIDIA Kumo Tabular as a pilot that flies it. The
hops flies a generated track through rows of walls, low blocks and bars; a
player changes lane, jumps and ducks, or watches the model do it. Kumo Tabular
is a pretrained in-context tabular classifier: every request carries a small
labelled table, the context, with the rows to classify, so nothing is trained
and there is no feature or training pipeline. The model is downloaded from
Hugging Face into the Model Registry and served as a deployment, which the app
asks for every move. It is built from the reference implementation in
[kumo_run/](kumo_run/):

| Phase | Builds | From |
| --- | --- | --- |
| `data` | nothing: `skipped`, the game states are the page's | this page |
| `features` | nothing: `skipped` | this page |
| `train` | `kumo_tabular` in the Model Registry, downloaded from Hugging Face; nothing is trained | `kumo_run/register_kumo.py` |
| `infer` | the `runexample` deployment; its accuracy and latency measured against the SLA | `kumo_run/predictor.py`, `kumo_run/app/measure.py` |
| `app` | the game and its pilot | `kumo_run/app/` |

The names are fixed: the app reads the `runexample` deployment of
`kumo_tabular`. Copy each file into the system (`src/<slug_pkg>/register_kumo.py`
and `src/<slug_pkg>/predictor.py`, and `app/` for the app) and change nothing
else: the code is tested as it is.

## data and features: skipped

Record `data: {status: skipped}` and `features: {status: skipped}` with a
`decisions` line: the model is pretrained and reads only the game state a
request sends, with a context built from the game's rules. Nothing is ingested,
so no feature group is created.

## The environments

```bash
hops env clone <slug>-jobs-env --from python-feature-pipeline          # the registration job
hops env install <slug>-jobs-env -f requirements.txt
hops env clone <slug>-inference-env --from torch-inference-pipeline   # the deployment
hops env install <slug>-inference-env -f inference-requirements.txt
```

`requirements.txt` is `kumo_run/requirements.txt`: huggingface_hub, for the
download. `inference-requirements.txt` is `kumo_run/inference-requirements.txt`:
NVIDIA's structured-data-models, which provides `KumoTabular` and is not on
PyPI, installed from its repository at a pinned commit; torch comes with
`torch-inference-pipeline`. The app runs in `python-agent-pipeline` as it is,
which ships FastAPI. Clone one environment at a time.

## train: register the pretrained classifier

```bash
hops job deploy <slug>-register-kumo src/<slug_pkg>/register_kumo.py --env <slug>-jobs-env --run --wait
```

It downloads `medium/classifier.pt` of `nvidia/Kumo-Tabular` (246 MB) with the
repository's README and LICENSE at a pinned revision, and registers
`kumo_tabular`. A second run finds the revision registered and leaves it.
Record `training: {required: false, pretrained: {repo, revision, name:
kumo_tabular, version}, status: met}`. The weights are under the OpenMDW 1.1
license: say so in the report.

## infer: the deployment

```bash
hops deployment create kumo_tabular --name runexample --script src/<slug_pkg>/predictor.py \
  --env <slug>-inference-env --no-default-predictor
hops deployment start runexample
```

The predictor loads the checkpoint from the model's files and answers
`{"instances": [{context, query, target}]}` with each query row's class
probabilities, its most probable class and the model's own time. It runs on the
deployment's default resources, one core. The app builds the context from the
game's rules: every situation (the hops' lane and what each lane holds in the
next row) labelled with the move the rules call for, 30% of them held out so
the model must decide rows it has never seen.

From the app directory, `python measure.py` reports the share of held-out
situations the model gets right and the round trip of single-state decisions;
`measured` gets the p99 against `requirements.sla.realtime`. On one core the
medium model takes about half a second per decision (a p99 near one second for
the round trip) and gets 30 to 32 of the 33 held-out situations right; its
answers vary a little from run to run. The context is one row per situation
without the distance to the row: the rules ignore distance, and as a column it
cost both time and accuracy.

`hops deployment create` on an existing name keeps its script: to deploy a
changed `predictor.py`, `hops deployment stop runexample`, then `hops deployment
delete runexample --yes`, and create it again.

## app: the game

```bash
cd app && python fetch_three.py && cd ..
```

puts three.js 0.186.1 under `app/static/vendor/three`, checked against npm's
integrity hash: the app serves everything the page loads. Then deploy `app/` as
**hops-app** says, in `python-agent-pipeline`, with 1024 MB and half a core:

```bash
hops app create <slug>-app --path /Projects/<project>/Users/<user>/<slug>/app/app.py \
  --app-kind custom --entrypoint-command "python app.py" --app-port 8080 \
  --readiness-probe-path /health --environment python-agent-pipeline --cores 0.5 --memory 1024 --start
```

The page is the game; *Let Kumo Tabular fly* (`?pilot=kumo`) hands it to the
model. Each move is a request to the app's `/api/decide`, which sends the state
to `runexample` with the context and returns the move probabilities, which the
page shows beside the run; moves the hops cannot make (off the track, a jump in
the air) are masked out. Players and the model share the leaderboard, kept in
`Resources/hops-run/board.json`; a player's run is timed by the server from its
takeoff and refused when it is further than the hops can fly in its time.
`/health` reports the decisions' p50 and p99. `met` when the model's runs reach
the board. The game is hops-run v1.8.0 by Lex Avstreikh, under the MIT license
(`static/LICENSE-hops-run.txt`); its fonts are Geist, under the OFL.

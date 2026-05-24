# Onboarding Flow

Wizard for a brand-new Hopsworks user.
Goal: from "I just landed in a project with some shared FGs" to "I have a deployed Streamlit app reading from a model I trained".

Core assumption: **the raw data is not clean and will never be**.
The flow always inserts a cleaning step between raw shared FGs and the feature view, materialized as a derived feature group computed by a scheduled job.
This is the FTI shape: feature pipeline writes a clean derived FG, training pipeline reads from a FV on top of that FG, inference pipeline reads the same FV online.

## Loop

Inner loop in this session is: pick a step, try it on the live project, fix CLI/SDK gaps in source as we hit them.
PRs from `feat/onboarding-flow-fixes` carry the gap fixes; this file carries the wizard plan.

## Handoff state (for the next agent landing here)

Three cross-repo PRs carry the work so far:

- `logicalclocks/hopsworks-api#945` (this branch, `feat/onboarding-flow-fixes`) — every CLI / SDK fix from Steps 0–5.
- `logicalclocks/hopsworks-ee#2996` — the Java side of the HQS schema fix. Merged and deployed on lexterm2.
- `logicalclocks/flyingduck#196` — the DuckDB schema-registration side of the same fix. Deployed in the running `arrowflight-deployment` pod.

The dev cluster is `lexterm2`. Kubeconfig lives at `/hopsfs/Resources/kubeconfig-lexterm2` (project-scoped, survives pod restarts). FlyingDuck pod logs are reachable via `kubectl -n hopsworks logs arrowflight-deployment-...` and were the only way we caught the catalog-error root cause.

Project on the cluster: `onboarding_project_test` (id 1198). It has the live derived `btc_daily_features` v1 plus the broken `btc_daily_fv` v1 from Step 5 (see below).

Sample scripts that drove the live runs are checked in under `.claude/docs/samples/`:

- `onboarding_pipeline.py` — minimal heartbeat used to validate `hops job deploy --run --wait`.
- `btc_daily_pipeline.py` — Step 3 feature pipeline that joins four shared raw FGs and writes `btc_daily_features` v1.
- `btc_transformations.py` — Step 5 user-defined retrieval-time transformations (`log1p_views`, `standardize_pressure`).

Step-by-step status is below; the `verified:` line on each step is the source of truth.

## Step 0. Land

`hops setup` or `hops login` cached.
`hops context` prints project, feature groups, feature views, jobs, models, deployments.
Wizard reads this first so it knows what the user already has.

verified: yes (5.0.0.dev1)

## Step 1. Discover shared raw data

`hops fg list` shows every FG visible (own + shared).
`hops fg info <name> --version <v>` + `hops fg features <name> --version <v>` to inspect each candidate.
`hops fg preview <name> --version <v> --n 5` to eyeball values (best-effort: shared FGs may have metadata only).

Wizard prompt: ask the user what they want to predict.
Map that to a label column and one or more candidate raw feature FGs.

verified: yes (`info` + `features` work across shared FSs; `preview` fails on unmaterialized FGs in this test env)

## Step 2. Pick raw features with the user

For each candidate FG, read `hops fg features <name>`.
Wizard surfaces:

- which columns look like keys (pk / event_time)
- which columns are numeric vs categorical
- which columns the wizard cannot judge without seeing rows (preview check)

User picks the subset that goes into the derived FG.
Wizard records the picks for the feature pipeline step.

verified: partial (commands work; the "preview to judge nulls" leg is blocked when raw FGs are unmaterialized)

## Step 3. Write the feature pipeline (clean + derive)

This is where the assumption "data is dirty" gets paid down.
The wizard writes a Python file `feature_pipeline.py`:

- reads each raw FG with `fs.get_feature_group(name, version=v).read()`
- joins what needs joining
- imputes (fillna, median, mode, sentinel; user-chosen per column)
- drops rows that cannot be salvaged (null PK, null label, obvious garbage)
- engineers derived features (ratios, lags, rolling windows, time-of-day, etc.)
- writes the result to a new derived FG in the **current project** with explicit primary key, event time, types

```python
clean_fg = fs.get_or_create_feature_group(
    name="ml_features_v1",
    version=1,
    primary_key=["entity_id"],
    event_time="ts",
    online_enabled=True,
    description="Cleaned and engineered features for <use case>.",
)
clean_fg.insert(df_clean)
```

Then the wizard registers a Hopsworks job that re-runs this on a schedule so the derived FG stays fresh as raw FGs grow.
A single CLI move handles upload, create-or-update, schedule, and optional first run:

```
hops job deploy feature_pipeline.py \
  --name feature-pipeline \
  --env python-feature-pipeline \
  --cron "0 0 0/1 ? * *" \
  --run --wait
```

The CLI delegates to `JobApi.deploy(local_path, name, ...)` on the SDK side (new in this branch), then chains `job.schedule(cron_expression=...)` and `job.run(await_termination=True)` on the returned object.
Re-deploying with the same `--name` overwrites the script in HopsFS and updates the job definition in place (PUT semantics on the backend).

verified: yes — end-to-end on lexterm2. `hops job deploy /tmp/btc_daily_pipeline.py --name btc-daily-pipeline --env python-feature-pipeline --run --wait` uploaded the pipeline, registered the job, ran it to completion (execution #244 finished in ~35s), the pipeline read 4 shared raw FGs from `hopsworks_default` (blocks=30, txs=7, wiki=7, hn=90 rows), joined on `day`, imputed nulls, derived 2 cross-source features, and wrote 90 rows into a new derived FG `btc_daily_features` v1 (id 1169) in the active project. Requires the HQS schema fix from `logicalclocks/hopsworks-ee#2996` + `logicalclocks/flyingduck#196` to be deployed on the cluster.

## Step 4. Sanity-check the derived FG

`hops fg info btc_daily_features --version 1` confirms schema and metadata.
`hops fg preview btc_daily_features --version 1 --n 10` shows actual cleaned rows.
`hops fg stats btc_daily_features --version 1` for null counts and ranges (TBD: confirm command).

verified: partial — `hops fg info` and `hops fg preview` work on the live derived FG (18 columns, 90 rows surfaced cleanly). The naive `fillna(0)` impute is a known smell: hn covers 90 days while blocks/txs cover only 30, so many rows have synthetic zeros in the block-side columns. A second iteration of the pipeline should either filter to the intersection date range or use a median/forward-fill impute keyed on date coverage. `hops fg stats` not exercised yet.

## Step 5. Define user transformations and a feature view

Retrieval-time transformations live in Python and get attached to the FV.
These run on **every read**, both online (single-row inference) and offline (training-data generation), so they cover label encoding, scaling, lookup-table joins, and anything that should not be baked into the offline FG.

Wizard writes `transformations.py`. The functions receive a pandas Series and must return a Series of the same length; scalar control flow on the input coerces it to bool and raises `truth value of a Series is ambiguous`. See `samples/btc_transformations.py` for a working pair (`log1p_views`, `standardize_pressure`).

Register every UDF in the file in one call, then create the FV pointing at the derived FG:

```
hops transformation create --file transformations.py
hops fv create btc_daily_fv \
  --feature-group btc_daily_features:1 \
  --transform log1p_views:wiki_views \
  --transform standardize_pressure:social_pressure \
  --labels tx_per_block
```

`hops transformation create` iterates every `@udf`-decorated function in the file (one `fs.create_transformation_function()` call each) and defaults to `--version 1` because the backend returns `HTTP 500 / NPE on null version` when the SDK passes `version=None`.
The default in `fs.get_transformation_function(name=...)` is also v1, so if a transform was re-registered (v1 broken, v2 fixed) the FV still binds the broken v1. Pin it explicitly with `--transform fn[v]:col`, e.g. `--transform log1p_views[2]:wiki_views`.

verified: partial — UDFs register, FV creates, schema and label flag correct. The first iteration of `btc_transformations.py` used scalar control flow and failed the training-data Spark job with `truth value of a Series is ambiguous`; the vectorised v2 in `samples/btc_transformations.py` is the working version. On lexterm2 the live `btc_daily_fv` v1 still binds the broken v1 transforms (cannot delete the transforms while the FV references them, cannot recreate the FV with `--transform fn[v]:col` until the CLI fix from this branch is deployed); recreate the FV after re-deploying the CLI to close Step 5.

## Step 6. Materialize training data

`hops td compute btc_daily_fv 1 --split "train:0.8,test:0.2"` runs a Spark job that computes the transformation statistics, applies the user-defined transformations to every row, and writes both splits to HopsFS.
`hops td list btc_daily_fv` to see the new training dataset version.

verified: not yet — pre-blocked on Step 5 ending with a working FV bound to working transforms.

## Step 7. Train

Wizard writes `train.py`:

- gets FV, gets training data version
- fits a model (default: XGBoost for tabular, sized to the problem)
- evaluates, writes PNGs to `images/`, a `metrics.json`
- `mr.python.create_model(name, metrics=...)`, `model.save("model/")`

Run as a job so it lands in the model registry from a known environment:

```
hops job create train --script train.py --env pandas-training-pipeline
hops job run train --await-termination
hops model list
```

verified: not yet

## Step 8. Deploy

`hops deployment create fraud_predictor --model fraud_model --version 1` (TBD: confirm flags + transformer support).
`hops deployment start fraud_predictor`, then `hops deployment status fraud_predictor` until ready.

verified: not yet

## Step 9. Streamlit app

Wizard writes `app.py`:

- imports `hopsworks`, logs in, gets the deployment handle
- builds a form for the FV's primary keys
- on submit: `deployment.predict(inputs=...)` (or the matching `serving_keys` shape)
- renders the prediction
- uses the Hopsworks design system tokens (TBD: locate the canonical theme file)

```
hops app create fraud-app --path .
hops app start fraud-app
```

verified: not yet

## Step 10. Test the deployed system

Open the app URL from `hops app info fraud-app`.
Wizard runs a known-good input through the form, checks the response is sane.
Done.

verified: not yet

## Known gaps to fix in CLI / SDK (rolling list)

- [x] `hops fg list` did not show shared feature stores → fixed
- [x] `hops fv list` had the same shape → fixed
- [x] `hops fg info / features / preview` were project-scoped → fixed
- [x] `hops fv info / read / delete` were project-scoped → fixed
- [x] `hops fg derive` and `hops fv create` could not source shared FGs → fixed
- [x] `_get_fg`/`_get_fv` returned silently on a None SDK response → now raise a clear "not found in any visible feature store" with a hint to `hops fg list` / `hops fv list`
- [x] `hops job deploy <file.py> --name ...` (compose of upload + create-or-update + optional schedule + optional run); backed by new `JobApi.deploy()` SDK method
- [x] `hops <command> --json` mixed SDK chatter with the JSON payload → isolated, stdout now carries only the payload
- [x] `arrow_flight_client._disable_feature_query_service_client()` crashed instead of disabling → fixed
- [x] FlyingDuck "Catalog Error: Table does not exist" on every HUDI read → root caused, fixed end-to-end via `logicalclocks/hopsworks-ee#2996` (HQS FROM clause emits a two-part identifier) and `logicalclocks/flyingduck#196` (registration uses a real DuckDB schema)
- [x] `hops fg stats` dumped raw JSON instead of a table → renders as a table with FEATURE / TYPE / COUNT / MIN / MAX / MEAN / STDDEV / COMPLETENESS columns
- [x] `hops transformation create` rejected files with more than one `@udf` function → registers every `@udf`-decorated function in source order
- [x] `hops transformation create` hit `HTTP 500: Cannot invoke java.lang.Integer.intValue() because version is null` on a fresh transform → CLI defaults to `--version 1`. Backend side still needs a fix (handle null version, or have the SDK auto-assign). Not blocking onboarding.
- [x] `hops fv create --transform fn:col` always picked v1 of the transform (SDK default), so a re-registered fix at v2 stayed invisible to the FV → CLI now accepts `--transform fn[v]:col` to pin a version.
- [ ] `fs.create_transformation_function(transformation_function=..., version=None)` should default to v1 in the SDK instead of letting the backend NPE. Separate fix; the CLI workaround above is in place.
- [ ] `hops td list` requires a FV arg; consider a naked listing
- [ ] `hops search ls` requires a term; document or relax
- [ ] No verified `hops fv read` UX when an FV has transforms (it errors with "Training data version is required" instead of telling the user to run `hops td compute` first; consider auto-detecting and surfacing a clearer message).
- [ ] No verified `hops deployment create` UX for a model+transformer pair
- [ ] No verified `hops app create / start / info` UX
- [ ] No canonical Hopsworks design system file to point Streamlit apps at (find it)
- [ ] Naive `fillna(0)` in the BTC pipeline imputed zeros into the block-side columns when source date ranges differ. Wizard should propose median/forward-fill or a date-range intersection when sources have unequal coverage.
- [ ] `hops fv list` shows `LABELS` column as `-` even when labels are set on the FV (cosmetic; `hops fv info` reports the labels correctly).

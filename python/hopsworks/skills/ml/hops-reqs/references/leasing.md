# Building the leasing decisions example

The `leasing-example` of the `ml-realtime` factory decides whether a gardening
machine (a mower, a chainsaw, a wood chipper) is leased to a customer. Its
feature pipeline writes the source tables as they are, with no feature
engineering: every feature is a custom model-dependent transformation attached
to the feature view, applied at training and, in the app, at inference. Two
models decide side by side from the same transformed features: an XGBoost
classifier the system trains, and NVIDIA Kumo Tabular (medium), a pretrained
in-context classifier that reads a sample of the training rows as its context.
The app logs every decision through the feature view, simulates how the leases
end, and retrains. Its code is in [leasing/](leasing/):

| Phase | Builds | From |
| --- | --- | --- |
| `data` | `leasing_equipment` (100 machines), `customer_credit_history` (20,000 reports of 5,000 customers), `leasing_orders` (10,000 orders, labelled `leased`) and `leasing_outcomes` (how each ended), online and offline | [generate_data.py](leasing/generate_data.py), [synth.py](leasing/synth.py) |
| `features` | nothing: `skipped`, the features are the feature view's transformations | [transformations.py](leasing/transformations.py) |
| `train` | the feature view `leasing_decisions`, `leasing_xgboost` with its charts and Kumo context, and `kumo_tabular_medium` from Hugging Face | [train.py](leasing/train.py), [register_kumo.py](leasing/register_kumo.py) |
| `infer` | nothing deployed: both models run in the app; its decision latency is measured | [scoring.py](leasing/scoring.py), [kumo.py](leasing/kumo.py) |
| `app` | the leasing console | [app/](leasing/app/) |

## The code

Copy the directory into the system and change nothing else: it is tested as it is.

```bash
cp -r <this directory>/leasing <slug>/leasing
```

The scripts import each other by module name from their directory, and the app
imports `scoring.py`, `kumo.py`, `synth.py` and `names.py` from the directory
above it, so every job and the app run in place from HopsFS: pass `hops job
deploy` the script's HopsFS path,
`hdfs:///Projects/<project>/<the system directory below /hopsfs/>/leasing/<script>`,
never the local file. The feature group, feature view and model names are fixed
in `names.py` and read by the app, so a project holds one leasing example. Every
job takes `--system <slug>`, which starts each description it writes: deleting
the system with its assets deletes only the assets whose description names it.

## The environments

```bash
hops env clone <slug>-jobs-env --from pandas-training-pipeline          # data, training, Kumo registration
hops env install <slug>-jobs-env -f leasing/requirements.txt
( export HOPSWORKS_CERT_FOLDER=$(mktemp -d) \
  && hops env clone <slug>-app-env --from torch-inference-pipeline \
  && hops env install <slug>-app-env -f leasing/app-requirements.txt ) > <log dir>/envs.log 2>&1 &
```

`pandas-training-pipeline` ships XGBoost 3.2.0, scikit-learn and matplotlib;
`requirements.txt` adds huggingface_hub for the Kumo download. The app's
environment, needed only at `app`, is built in the background while the jobs
run, with its own `HOPSWORKS_CERT_FOLDER`: `torch-inference-pipeline` ships
torch, FastAPI and uvicorn, and `app-requirements.txt` adds XGBoost at the
version training used, scikit-learn for its classifier wrapper, and NVIDIA's
structured-data-models (`KumoTabular`), which is not on PyPI, at a pinned commit.

## data: the history

```bash
P=hdfs:///Projects/<project>/<system dir>/leasing
hops job deploy <slug>-data $P/generate_data.py --env <slug>-jobs-env --args "--system <slug>" --run --wait
```

Every customer has a hidden risk that no table shows: it lowers their credit
score and raises their debt and delinquencies, makes an underwriter decline them
more often, and makes their leases end badly more often. Orders are decided a
month at a time, each from the customer's risk, income and how their previous
order ended. The generator joins nothing by time: the point-in-time joins are
the feature view's, at training. About
68% of the orders are leased; a leased order ends paid, paid late, damaged or
defaulted, a declined one `declined`. The history ends now, so a second run
leaves the feature groups alone; delete all four to write a new one.

Every group has an event time, since the feature view's joins are point-in-time
only when all of them do: the machines carry `listed_ts`. The credit reports
and the outcomes are keyed by `customer_id` with their event time, since a join
must cover the joined group's whole primary key: offline keeps every row (the
upsert key includes the event time), online keeps each customer's newest report
and outcome, which is what the app reads. Record `data: {status: met}` with
the four groups in `data.<source>.writes`, and `features: {status: skipped}`
with a `decisions` line: the features are the feature view's transformations.

## train: the feature view, XGBoost and Kumo Tabular

```bash
hops job deploy <slug>-register-kumo $P/register_kumo.py --env <slug>-jobs-env --args "--system <slug>" --run --wait
hops job deploy <slug>-train $P/train.py --env <slug>-jobs-env --args "--system <slug>" --run --wait
```

`register_kumo.py` downloads `medium/classifier.pt` of `nvidia/Kumo-Tabular`
(246 MB) with its README and LICENSE at a pinned revision and registers
`kumo_tabular_medium`; a second run finds the revision registered. The weights
are under the OpenMDW 1.1 license: say so in the report.

`train.py` creates `leasing_decisions` on its first run: each order with its
machine, the customer's credit report and their last settled lease (`prev_*`)
as they were when it was placed, labelled `leased`, with `customer_id` and
`equipment_id` as inference helper columns, logging enabled with three extra
columns, and these transformations:

| Transformation | Reads | Gives |
| --- | --- | --- |
| `lease_cost` | `monthly_price`, `lease_months` | what the whole lease costs |
| `affordability` | `monthly_price`, `monthly_income`, `total_debt` | `payment_to_income`, `debt_to_income` |
| `prior_lease` | the `prev_*` columns | whether the last order was a lease, paid, and paid with no problem; its days late and damage cost |
| `credit_score_z` | `credit_score`, its training mean and standard deviation | the score in standard deviations |
| `price_to_typical` | `monthly_price`, its training mean | the price against the typical leased machine |
| `label_encoder` | `category`, `power_source`, `customer_type` | codes learned from training |

It then splits the orders in memory (80/20), trains XGBoost, and registers
`leasing_xgboost` with ROC AUC, PR AUC, accuracy and F1, the charts in `images/`
(ROC and precision-recall curves, confusion matrix, calibration, score
distribution, feature importance), and `kumo_context.parquet`, 512 transformed
training rows, half leased where the data allows, with their label: Kumo's
context. On the default history ROC AUC is about 0.86, and it rises a little as retraining adds the simulated orders. Record
`training.models` for both, with `metric: roc_auc` and `target: 0.85` for
XGBoost and `pretrained` for Kumo. The app runs this job again to retrain, so
keep its name `<slug>-train`.

## infer: in the app

Nothing is deployed: `scoring.py` loads the newest `leasing_xgboost`, its feature
view (`init_serving` with the model's training dataset version, so the
transformations use that dataset's statistics) and Kumo Tabular. For a new
order (customer, machine, lease length) it asks the feature view for the
features: `FeatureView.get_feature_vectors` looks the machine and the customer
up in the online store by their keys (the customer's newest report and outcome,
what the feature view's joins give an order placed now) with the lease length
passed in and the order's own key missing, since the order is new; then
`FeatureView.transform` applies the transformations, and both models decide.
The app joins nothing itself. Every decision is logged with
`FeatureView.log`: the untransformed and transformed features, XGBoost's
decision as the prediction, `customer_id` and `equipment_id` as helper columns,
both probabilities and Kumo's decision as extra columns, and the order time as
the event time. `measured` gets the app's `/api/decide` latency against
`requirements.sla.realtime`. On the app's two cores one order takes 2.5 to 5.5 s, all
but about 30 ms of it Kumo Tabular medium reading its 512-row context; XGBoost takes
a few milliseconds. A batch of 1,000 takes about 12 s. A smaller context
(`train.py --context-rows`) makes Kumo faster and less accurate.

## app: the leasing console

Deploy `leasing/app/` as **hops-app** says, from its HopsFS path, with 4096 MB and
two cores for Kumo Tabular medium:

```bash
hops app create <slug>-app --path /Projects/<project>/<system dir>/leasing/app/app.py \
  --app-kind custom --entrypoint-command "python app.py" --app-port 8080 \
  --readiness-probe-path /health --environment <slug>-app-env --cores 2 --memory 4096 --start
```

The app reads the system's slug from `SYSTEM` and the training job from
`TRAIN_JOB` (`<slug>-train`); its defaults are this example's names. The page
has three parts:

- **Decide one lease**: pick a customer, a machine and a lease length; both
  models' decisions and probabilities, the facts they read (credit, price, last
  lease), and the time of each step.
- **Batch of random orders**: score 100 or 1,000 random orders with both models,
  all logged; tiles for the share each model leases and how often they agree.
- **Learn from what happened**: *Simulate leasing outcomes* reads the logged
  decisions newer than the newest order, lets the history's underwriter decide
  each and plays its lease out, and writes the orders to `leasing_orders` and
  their outcomes to `leasing_outcomes`, so the customer's next order sees them;
  it reports how often the underwriter agreed with each model. *Retrain
  XGBoost* runs `<slug>-train`, and the app switches to the new version when it
  is registered.

A Terminal cannot read a running app's logs; check its API through its service:

```bash
curl -s http://$(kubectl get svc pythonapp-<project>--<slug>-app-svc -o jsonpath='{.spec.clusterIP}'):8080/api/status
```

`met` when a decision, a batch, a simulation and a retrain have run from the page.

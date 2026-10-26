# Building the credit card fraud detector

The real-time example (`fraud-example`) predicts, per request, whether a credit
card transaction is fraud. It is the credit card fraud system of the book
*Building Machine Learning Systems with a Feature Store* (O'Reilly), whose code
is public at https://github.com/featurestorebook/mlfs-book, built phase by phase
like any real-time system:

| Phase | Builds | From |
| --- | --- | --- |
| `data` | `merchant_details`, `bank_details`, `account_details`, `card_details`, `credit_card_transactions` and the labels in `cc_fraud`, then a live stream of transactions | the book's `1_data_generator.py`, `1b-transaction-generator-job.py` |
| `features` | `cc_trans_fg` (per-transaction features) and `cc_trans_aggs_fg` v2 (per-card sliding windows), backfilled and then streamed | the book's `3-batch-feature-pipeline.py`, `2b-backfill-aggs-pipeline.py`, `2-spark-streaming-feature-pipeline.py` |
| `train` | the feature view `cc_fraud_fv` and `cc_fraud_xgboost_model` | [ccfraud/train_fraud.py](ccfraud/train_fraud.py) |
| `infer` | one deployment, with feature logging | [ccfraud/predictor.py](ccfraud/predictor.py) |
| `monitoring` | hourly PSI drift of the transaction amount | the book's `5-feature-monitoring.py` |
| `app` | the JavaScript fraud console | the book's `app/` |

## The code

Take the book's code at the commit this example is tested with, in a scratch
directory outside the system:

```bash
git clone -q https://github.com/featurestorebook/mlfs-book /tmp/mlfs-book
git -C /tmp/mlfs-book checkout -q cfd40f192486d4b2baf00e837af3f5567c847511
cp -r /tmp/mlfs-book/ccfraud/ccfraud <slug>/ccfraud      # the package: generator, pipelines, features/
cp -r /tmp/mlfs-book/ccfraud/app <slug>/app
cp -r /tmp/mlfs-book/ccfraud/tests <slug>/tests/book
cp <this directory>/ccfraud/{train_fraud.py,predictor.py} <slug>/ccfraud/
cp <this directory>/ccfraud/run_unit.py <slug>/tests/
cp <this directory>/ccfraud/benchmark_requests.py <slug>/src/<slug_pkg>/
cp <this directory>/ccfraud/{requirements.txt,inference-requirements.txt} <slug>/
rm <slug>/ccfraud/{jobs.py,run_notebook.py,streamlit_app.py}
rm <slug>/src/<slug_pkg>/{charts,evaluate,feature_pipeline,inference_pipeline,parity,predictor,training_pipeline}.py
rm <slug>/tests/unit/{test_features,test_inference,test_training}.py <slug>/tests/integration/test_parity.py
```

The template's pipelines, harness and their tests are removed: this example runs
the book's. The book's code is not the system's to restyle: add `"ccfraud"`,
`"app"` and `"tests/book"` to `extend-exclude` in `pyproject.toml`, so
`ruff` checks only the system's own files. Its tests import Faker and geoip2,
which only the jobs environment holds, and mock the hopsworks modules for the
whole process, so they run there, as one job of their own, while the streaming
jobs start:

```bash
hops job deploy <slug>-tests hdfs:///Projects/<project>/<system dir>/tests/run_unit.py \
  --env <slug>-jobs-env --run --wait
```

`benchmark_requests.py` gives `benchmarks/benchmark_inference.py` this
deployment's request shape, so the benchmark runs unedited.

The scripts import the `ccfraud` package from the directory above them
(`<slug>/`), so a job runs a script in place from HopsFS rather than uploaded on
its own: pass `hops job deploy` the script's HopsFS path,
`hdfs:///Projects/<project>/<the system directory below /hopsfs/>/ccfraud/<script>`,
never the local file, which `hops job deploy` would upload alone to
`Resources/jobs/<name>`. In `app/app.py` set `DEPLOYMENT` to the deployment's
name and `JOBS` to `["<slug>-streaming-aggs", "<slug>-transactions"]`. The page
names them too, and finds each job's row by an id made of its name, so rename
them in `app/static/index.html` as well:

```bash
sed -i -e 's/ccfraud-streaming-aggs/<slug>-streaming-aggs/g' \
  -e 's/ccfraud-transactions/<slug>-transactions/g' \
  -e 's|<strong>ccfraud</strong>|<strong><deployment></strong>|' \
  -e 's|<span class="name">ccfraud</span>|<span class="name"><deployment></span>|' \
  <slug>/app/static/index.html
```

Change nothing else: the code is tested as it is. The feature group, feature view and
model names are fixed, which the deployment and the app read, so a project holds
one fraud example.

## The environments

```bash
hops env clone <slug>-jobs-env --from python-feature-pipeline              # generator, features, training
hops env install <slug>-jobs-env -f requirements.txt
( export HOPSWORKS_CERT_FOLDER=$(mktemp -d) \
  && hops env clone <slug>-inference-env --from minimal-inference-pipeline \
  && hops env install <slug>-inference-env -f inference-requirements.txt ) > <log dir>/env-inference.log 2>&1 &
```

The data generator needs the jobs environment, so it is built first; the
inference environment, needed only at `infer`, is built in the background while
the data and feature jobs run, with its own `HOPSWORKS_CERT_FOLDER` (every `hops`
call writes the client's certificates there).

The streaming job is the only Spark job and runs in `spark-feature-pipeline`;
the app runs in `python-agent-pipeline`. scikit-learn and xgboost are pinned to
the same versions in both files, since the deployment unpickles the pipeline
training saved.

## data and features: the jobs, in order

Give `<slug>-datamart`, `<slug>-features` and `<slug>-backfill-aggs` 8192 MB, as
**hops-job** says. `<end>` is today's date (UTC) and `<start>` thirty days before:
the history ends at the last midnight, where the live stream takes over.

```bash
P=hdfs:///Projects/<project>/<system dir>/ccfraud
hops job deploy <slug>-datamart $P/1_data_generator.py --env <slug>-jobs-env \
  --args "--mode backfill --start-date <start> --end-date <end>" --run --wait
hops job deploy <slug>-features $P/3-batch-feature-pipeline.py --env <slug>-jobs-env \
  --args "--current-date <end> --wait" --run
hops job deploy <slug>-backfill-aggs $P/2b-backfill-aggs-pipeline.py --env <slug>-jobs-env \
  --args "--wait" --run --wait
hops job deploy <slug>-streaming-aggs $P/2-spark-streaming-feature-pipeline.py --type pyspark \
  --env spark-feature-pipeline --args "--mode stream" --run
hops job deploy <slug>-transactions $P/1b-transaction-generator-job.py --env <slug>-jobs-env \
  --args "--transactions-per-min 100" --run
```

`<slug>-features` and `<slug>-backfill-aggs` both read only the generator's
groups, so they run at the same time: submit the features job without waiting
and wait on the backfill, the longer of the two, then check the features job
succeeded. Submit the streaming job and the transaction generator one after the
other without waiting for either to run. Training reads only the backfilled
history, so `train` starts as soon as both backfills succeeded; whether the
stream is live and fresh (`hops fg preview cc_trans_aggs_fg --online`, windows at
most ~3 minutes old) is checked at verify, by when it has run for many minutes.

The backfill writes 50 merchants, 50 banks, 1,000 accounts, 2,000 cards and
500,000 transactions with 0.5% fraud: chain attacks (a burst of small then
larger card-not-present charges from a foreign IP) and impossible travel (a
card-present charge far from the previous one minutes earlier). The features
job writes `cc_trans_fg`, whose on-demand transformation is the distance from
the card's previous transaction. The aggregates backfill writes, for each
transaction, the card's window a point-in-time join selects, and each card's
latest window online. Then two jobs run until stopped: the streaming job reads
the transactions' Kafka topic and writes `cc_trans_aggs_fg` every minute (1-hour
windows sliding by one minute, online at most ~3 minutes old), and the
transaction generator writes 100 transactions a minute with fraud at the same
rate. The streaming job keeps its state in 4 shuffle partitions, fixed when its
checkpoint is first written. Run the backfill before the streaming job, which needs the feature group
it creates. Every feature group has statistics off, so no insert starts a Spark
statistics job.

The book's code names each group in its own words, so once both backfills
succeeded, put the slug at the start of each description: deleting the system
with its assets deletes only the groups whose description names it.

```bash
python - <<'PY'
import hopsworks
fs = hopsworks.login().get_feature_store()
for name, version in [("merchant_details", 1), ("bank_details", 1), ("account_details", 1),
                      ("card_details", 1), ("credit_card_transactions", 1), ("cc_fraud", 1),
                      ("cc_trans_fg", 1), ("cc_trans_aggs_fg", 2)]:
    fg = fs.get_feature_group(name, version)
    if not (fg.description or "").startswith("<slug>: "):
        fg.update_description(f"<slug>: {fg.description or ''}")
PY
```

Record the six groups the generator writes as a list of `{feature_group,
version}` in `data.<source>.writes`, `cc_trans_fg` v1 and `cc_trans_aggs_fg` v2
as the `writes` of their pipelines in `features.pipelines`, and both running
jobs in `features.jobs`.

## train

```bash
hops job deploy <slug>-train $P/train_fraud.py --env <slug>-jobs-env \
  --args "--system <slug> --test-days 7" --run --wait
```

It creates `cc_fraud_fv` with feature logging on, both descriptions starting with the slug, holds out the last seven days,
and registers `cc_fraud_xgboost_model` with `pr_auc`, `precision`, `recall`,
`f1_score` and `accuracy`, `predictor.py` among its files.
`requirements.targets` holds the PR-AUC target.

## infer: the deployment

```bash
hops deployment create cc_fraud_xgboost_model --name <alnum slug> --script <slug>/ccfraud/predictor.py \
  --env <slug>-inference-env --no-default-predictor
python -c "import hopsworks; d = hopsworks.login().get_model_serving().get_deployment('<alnum slug>'); \
  d.predictor.scaling_configuration.max_instances = 1; d.save()"
hops deployment start <alnum slug>
hops deployment predict <alnum slug> --data '{"inputs": [["<a cc_num>", 42.5, "<a merchant_id>", "81.2.69.160", false, 1]]}'
```

One replica serves the SLA, so the replicas are capped at 1 before the
deployment starts: the autoscaler otherwise starts a second one under the
benchmark, and a request that reaches it while it starts times out
(**hops-autoscaling**). Raise the cap, within
`requirements.budget.inference.max_replicas`, only when a measurement needs it.

The request is `[cc_num, amount, merchant_id, ip_address, card_present, t_id]`
and the reply `{"predictions": [true]}` for fraud. The card's aggregates and
details and the merchant's come from the online store; the amount, IP address
and card presence are request parameters. Each scored vector is written to the
feature view's logging tables. `measured` gets the p99 of 50 requests over
random cards and merchants against `requirements.sla.realtime`: one replica serves
10 requests a second at a p99 of about 50 ms. To see the benchmark's predictions
in the logging tables, start the log group's materialization job,
`cc_fraud_fv_1_log_1_offline_fg_materialization`, without waiting, and count the
rows at verify.

## monitoring

```bash
hops job deploy <slug>-monitoring $P/5-feature-monitoring.py --env <slug>-jobs-env --run --wait
```

It attaches `amount_psi_hourly` to `credit_card_transactions`: every hour the
amount's distribution over the last day is compared with the week before by
PSI, and 0.2 or more is a detected shift. Put the slug at the start of its
description too: a feature group with a monitoring configuration cannot be
deleted, and deleting the system deletes the configuration only when its
description names the system.

```bash
python - <<'PY'
import hopsworks
fg = hopsworks.login().get_feature_store().get_feature_group("credit_card_transactions", 1)
config = fg.get_feature_monitoring_configs(name="amount_psi_hourly")
if not (config.description or "").startswith("<slug>: "):
    config.description = f"<slug>: {config.description or ''}"
    config.update()
PY
```

Record it, as its job `credit_card_transactions_1_run_fm_amount_psi_hourly`,
with the logging tables in `inference.monitoring`.

## app: the fraud console

Deploy `<slug>/app/` as **hops-app** says, in `python-agent-pipeline`. The page
shows the project, the entity counts, the deployment's state (with a Start
button) and whether the two streaming jobs run. A form generates a batch of
transactions for real cards and merchants, injects chain attacks at the chosen
fraud rate, and scores each with the deployment. With "Write transactions to
feature group" checked (it starts unchecked) it also writes them to
`credit_card_transactions`, and so through Kafka into the streaming job: metric tiles
for the predicted and the injected fraud caught and the prediction latency (p50,
p95 and max of the predict round trips, sent several at a time), the transactions
with predicted fraud highlighted and each one's latency, and each card's live
window features.

A Terminal cannot `kubectl exec` into the app or port-forward to it; check its
API through its service instead, with a run that writes nothing:

```bash
APP=http://$(kubectl get svc pythonapp-<project>--<slug>-app-svc -o jsonpath='{.spec.clusterIP}'):8080
curl -s $APP/api/status
RUN=$(curl -s -X POST $APP/api/runs -H 'Content-Type: application/json' \
  -d '{"n": 50, "fraud_rate_pct": 2, "seed": 42, "write": false}' | jq -r .id)
sleep 20; curl -s $APP/api/runs/$RUN | jq '{status, error, result: (.result | keys?)}'
```

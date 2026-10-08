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
git -C /tmp/mlfs-book checkout -q 23509a956f1b35b8eef0d9598152520d6bb26f66
cp -r /tmp/mlfs-book/ccfraud/ccfraud <slug>/ccfraud      # the package: generator, pipelines, features/
cp -r /tmp/mlfs-book/ccfraud/app <slug>/app
cp <this directory>/ccfraud/{train_fraud.py,predictor.py} <slug>/ccfraud/
cp <this directory>/ccfraud/{requirements.txt,inference-requirements.txt} <slug>/
rm <slug>/ccfraud/{jobs.py,run_notebook.py,streamlit_app.py}
```

The scripts import the `ccfraud` package from the directory above them
(`<slug>/`), so a job runs a script in place from HopsFS rather than uploaded on
its own: pass `hops job deploy` the script's HopsFS path,
`hdfs:///Projects/<project>/<the system directory below /hopsfs/>/ccfraud/<script>`,
never the local file, which `hops job deploy` would upload alone to
`Resources/jobs/<name>`. In `app/app.py` set `DEPLOYMENT` to the deployment's
name and `JOBS` to `["<slug>-streaming-aggs", "<slug>-transactions"]`. Change
nothing else: the code is tested as it is. The feature group, feature view and
model names are fixed, which the deployment and the app read, so a project holds
one fraud example.

## The environments

```bash
hops env clone <slug>-jobs-env --from python-feature-pipeline              # generator, features, training
hops env install <slug>-jobs-env -f requirements.txt
hops env clone <slug>-inference-env --from minimal-inference-pipeline      # the deployment
hops env install <slug>-inference-env -f inference-requirements.txt
```

The streaming job is the only Spark job and runs in `spark-feature-pipeline`;
the app runs in `python-agent-pipeline`. scikit-learn and xgboost are pinned to
the same versions in both files, since the deployment unpickles the pipeline
training saved. Clone one environment at a time.

## data and features: the jobs, in order

Give `<slug>-datamart`, `<slug>-features` and `<slug>-backfill-aggs` 8192 MB, as
**hops-job** says. `<end>` is today's date (UTC) and `<start>` thirty days before:
the history ends at the last midnight, where the live stream takes over.

```bash
P=hdfs:///Projects/<project>/<system dir>/ccfraud
hops job deploy <slug>-datamart $P/1_data_generator.py --env <slug>-jobs-env \
  --args "--mode backfill --start-date <start> --end-date <end>" --run --wait
hops job deploy <slug>-features $P/3-batch-feature-pipeline.py --env <slug>-jobs-env \
  --args "--current-date <end> --wait" --run --wait
hops job deploy <slug>-backfill-aggs $P/2b-backfill-aggs-pipeline.py --env <slug>-jobs-env \
  --args "--wait" --run --wait
hops job deploy <slug>-streaming-aggs $P/2-spark-streaming-feature-pipeline.py --type pyspark \
  --env spark-feature-pipeline --args "--mode stream" --run
hops job deploy <slug>-transactions $P/1b-transaction-generator-job.py --env <slug>-jobs-env \
  --args "--transactions-per-min 100" --run
```

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
statistics job. Record each group in `data.<source>.writes` and both running
jobs in `features.jobs`.

## train

```bash
hops job deploy <slug>-train $P/train_fraud.py --env <slug>-jobs-env --args "--test-days 7" --run --wait
```

It creates `cc_fraud_fv` with feature logging on, holds out the last seven days,
and registers `cc_fraud_xgboost_model` with `pr_auc`, `precision`, `recall`,
`f1_score` and `accuracy`, `predictor.py` among its files.
`requirements.targets` holds the PR-AUC target.

## infer: the deployment

```bash
hops deployment create cc_fraud_xgboost_model --name <alnum slug> --script <slug>/ccfraud/predictor.py \
  --env <slug>-inference-env --no-default-predictor
hops deployment start <alnum slug>
hops deployment predict <alnum slug> --data '{"inputs": [["<a cc_num>", 42.5, "<a merchant_id>", "81.2.69.160", false, 1]]}'
```

The request is `[cc_num, amount, merchant_id, ip_address, card_present, t_id]`
and the reply `{"predictions": [true]}` for fraud. The card's aggregates and
details and the merchant's come from the online store; the amount, IP address
and card presence are request parameters. Each scored vector is written to the
feature view's logging tables. `measured` gets the p99 of 50 requests over
random cards and merchants against `requirements.sla.realtime`.

## monitoring

```bash
hops job deploy <slug>-monitoring $P/5-feature-monitoring.py --env <slug>-jobs-env --run --wait
```

It attaches `amount_psi_hourly` to `credit_card_transactions`: every hour the
amount's distribution over the last day is compared with the week before by
PSI, and 0.2 or more is a detected shift. Record it with the logging tables in
`inference.monitoring`.

## app: the fraud console

Deploy `<slug>/app/` as **hops-app** says, in `python-agent-pipeline`. The page
shows the project, the entity counts, the deployment's state (with a Start
button) and whether the two streaming jobs run. A form generates a batch of
transactions for real cards and merchants, injects chain attacks at the chosen
fraud rate, and scores each with the deployment. With "Write transactions to
feature group" checked (it starts unchecked) it also writes them to
`credit_card_transactions`, and so through Kafka into the streaming job: metric tiles
for the predicted and the injected fraud caught, the transactions with predicted
fraud highlighted, and each card's live window features.

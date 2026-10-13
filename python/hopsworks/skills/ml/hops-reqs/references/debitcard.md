# Building the debit card fraud detector

The `debitcard-example` of the `ml-streaming` factory scores, per request, how
unusual a debit card's last hour is. Its batch features are dbt models run in
Hopsworks Trino and its streaming features a Spark Structured Streaming job,
the two halves every system of the factory has. Its code is in
[debitcard/](debitcard/), built phase by phase like any real-time system:

| Phase | Builds | From |
| --- | --- | --- |
| `data` | a year of synthetic authorisations, 1,000,000 by default, in `card_authorisations`, `card_authorisation_signals` (the fraud labels), `card_weekly_spend` and `account_events` | [generate_data.py](debitcard/generate_data.py), [synth.py](debitcard/synth.py) |
| `features` | batch: `card_daily_features` and `card_spending_baselines` by dbt in Trino, daily; streaming: `card_authorisations_live` written every second, aggregated into `card_window_aggregates` by Spark Structured Streaming | [batch_features.py](debitcard/batch_features.py), [dbt/](debitcard/dbt/), [transaction_stream.py](debitcard/transaction_stream.py), [streaming_features.py](debitcard/streaming_features.py) |
| `train` | the feature view `card_fraud_fv` and `card_fraud_model` | [train.py](debitcard/train.py), [model.py](debitcard/model.py) |
| `infer` | one deployment that reads a card's window and baseline online | [predictor.py](debitcard/predictor.py) |
| `app` | the JavaScript fraud console | [app/](debitcard/app/) |

The model is an Isolation Forest over a card's live window (velocity, amounts,
declines, distance, risk scores over the last hour) and its 30-active-day
spending baseline, with the window's amount, count and distance divided by the
baseline's. It is unsupervised: the labels in `card_authorisation_signals` and
`fraud_next_active_day` in `card_daily_features` are for evaluation and
analysis, never inputs.

## The code

Copy the code into the system and change nothing else: it is tested as it is.

```bash
cp -r <this directory>/debitcard <slug>/debitcard
```

The scripts import each other by module name from their directory, so a job
runs its script in place from HopsFS rather than uploaded on its own: pass
`hops job deploy` the script's HopsFS path,
`hdfs:///Projects/<project>/<the system directory below /hopsfs/>/debitcard/<script>`,
never the local file, which `hops job deploy` would upload alone to
`Resources/jobs/<name>`. The app reads the deployment's name from `DEPLOYMENT` and
the streaming jobs' from `JOBS`; its defaults are this example's names.
The feature group, feature view and model names are fixed and read by the
deployment and the app, so a project holds one debit card example.

## The environments

```bash
hops env clone <slug>-jobs-env --from python-feature-pipeline          # generator, stream, training
hops env install <slug>-jobs-env -f debitcard/requirements.txt
hops env clone <slug>-inference-env --from minimal-inference-pipeline  # the deployment
hops env install <slug>-inference-env -f debitcard/inference-requirements.txt
hops env clone <slug>-app-env --from python-agent-pipeline             # the console
hops env install <slug>-app-env -f debitcard/app-requirements.txt
```

The dbt job runs in the stock `dbt-pipeline` environment, which ships dbt-core,
dbt-trino and `dbt_run_graph`; the streaming job in `spark-feature-pipeline`.
scikit-learn is pinned to the same version in the three files, since the
deployment and the app unpickle the model training saved. Clone one environment
at a time.

## data: the history

Give `<slug>-data` 8192 MB, as **hops-job** says: the generator holds the
year's keys and the fraud plan in memory.

```bash
P=hdfs:///Projects/<project>/<system dir>/debitcard
hops job deploy <slug>-data $P/generate_data.py --env <slug>-jobs-env --run --wait
```

It writes 1,000,000 authorisations on 10,000 cards over the year to
2026-03-31, about 0.3% of them fraud on 5% of the cards, each from one of eight schemes: velocity
spikes, card testing, impossible geography, risky merchants, unusual amounts,
night-time use, declines then an approval, and rings of cards sharing a wallet
or token requestor. A third of the fraud is stealthy (labelled, normal-looking)
and 1.2% of the legitimate authorisations look like fraud. Pass `--rows` for
another size; the transaction stream must then be given the same
`--history-rows`. Amounts are in euros, distances in kilometres, regions EEA
(home), UK, NA, AP and LA. Every feature group has statistics off. Record each
in `data.<source>.writes`.

## features: batch with dbt, streaming with Spark

Load **hops-dbt** and **hops-spark**.

```bash
hops job deploy <slug>-batch-features $P/batch_features.py --env dbt-pipeline --run --wait
hops job schedule <slug>-batch-features "0 0 3 * * ?"     # daily, after the day closes
hops job deploy <slug>-transactions $P/transaction_stream.py --env <slug>-jobs-env --run
hops job deploy <slug>-streaming-features $P/streaming_features.py --type pyspark \
  --env spark-feature-pipeline --args "--window-minutes 60" --run
```

The batch job runs `dbt compile` and `dbt build` (the models' data tests), then
streams each model from Trino into its feature group: `card_daily_features`
offline, one row per card and active day with trailing 7 and 30 active days and
the next active day's fraud label, and `card_spending_baselines` online and
offline, each card's latest 30-active-day baseline, which the deployment reads.
Its dbt artifacts and the execution graph the Jobs UI shows land in
`Resources/dbt_runs/<job>/<execution>`. Then two jobs run until stopped: the
transaction stream writes 5 authorisations a second on the history's cards,
with fraud bursts, to the online stream feature group
`card_authorisations_live`, and the streaming job reads its Kafka topic and
upserts each card's current 60-minute tumbling window into
`card_window_aggregates` online. Start the stream before the streaming job,
which waits up to three minutes for the feature group the stream creates. The
streaming job keeps its state in 4 shuffle partitions, fixed when its
checkpoint is first written. Record the pipelines and the two running jobs in
`features`.

## train

Wait until the streaming job has windows for at least 100 cards (about 5
minutes at 5 a second; `hops fg preview card_window_aggregates --online`), then:

```bash
hops job deploy <slug>-train $P/train.py --env <slug>-jobs-env --run --wait
```

It creates `card_fraud_fv`, the windows left-joined with the baselines on
`card_number`, trains the Isolation Forest on the online snapshot of every card
with an open window and registers `card_fraud_model` with `training_cards`,
`flagged_fraction` and `with_baseline`; `model.py` is among its files. Nearly
every card should have a baseline: fewer means the stream's cards are not the
history's (another `--rows` or `--seed`). Retrain as the windows change; there
is no label to target, so `requirements.targets` records the flagged fraction.

## infer: the deployment

```bash
hops deployment create card_fraud_model --name <alnum slug> --script <slug>/debitcard/predictor.py \
  --env <slug>-inference-env --no-default-predictor
hops deployment start <alnum slug>
hops deployment predict <alnum slug> --data '{"inputs": [["<a card_number>"]]}'
```

The request is one card number per row and the reply, per card, its `risk`
(0-1), `fraud` (risk at or over the Isolation Forest's own boundary),
`anomaly_score`, and whether it had a window and a baseline. `measured` gets the
p99 of 50 requests over random cards against `requirements.sla.realtime`.

## app: the fraud console

`debitcard/app/` is a custom app: FastAPI serving a JSON API under `/api` and a
JavaScript page from `static/`, with no build step and no CDN. Deploy it as
**hops-app** says, from its HopsFS path:

```bash
hops app create <slug>-app --path /Projects/<project>/<system dir>/debitcard/app/app.py \
  --app-kind custom --entrypoint-command "python app.py" --app-port 8080 \
  --readiness-probe-path /health --environment <slug>-app-env --start
```

The app loads the latest `card_fraud_model` with its `model.py` and, every few
seconds, scores every card with an open window from one online snapshot of the
windows and the baselines. The page shows status chips for the deployment and
the two streaming jobs; tiles for the cards in a window, those over the
threshold, the authorisations in the windows and the read and scoring times; a
histogram of the cards' risk with the threshold, which a slider moves (it starts
at the model's own boundary); and the riskiest cards. Selecting a card, or
typing its number, asks the deployment about it and shows its risk as a gauge,
the round trip, and its window against its baseline: the hour's spend and
authorisations against a usual day, the furthest merchant and the decline ratio
against its last 30 active days.

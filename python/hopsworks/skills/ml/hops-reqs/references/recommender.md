# Building the personalized recommender

The real-time example (`recs-example`) recommends H&M products to a shopper per
request. It follows the Decoding AI / Hopsworks course
(https://github.com/decodingai-magazine/personalized-recommender-course) on the
public H&M data, with PyTorch in place of TensorFlow Recommenders and a
JavaScript storefront in place of the course's Streamlit UI. It is built phase
by phase like any real-time system, from the reference implementation in
[recommender/](recommender/):

| Phase | Builds | From |
| --- | --- | --- |
| `data` | `customers`, `articles`, `transactions` and `interactions`, online | `recommender/hm_features.py` |
| `features` | the `retrieval`, `customers` and `articles` feature views | `recommender/train_retrieval.py` |
| `train` | `query_model` (two-tower query tower), `candidate_embeddings` and `ranking_model` (CatBoost) | `recommender/train_retrieval.py`, `recommender/train_ranker.py` |
| `infer` | one deployment: retrieve, filter, rank | `recommender/predictor.py` |
| `app` | the JavaScript storefront | `recommender/app/` |

The names are fixed: the four feature groups, the three feature views and the
two models are the ones the deployment and the app read. Copy each file into
the system (`src/<slug_pkg>/`, and `app/` for the storefront), set `DEPLOYMENT`
in `app/app.py` to the deployment's name, and change nothing else: the code is
tested as it is.

## data: the H&M files

Nothing is generated or uploaded: the files are public at
`https://repo.hops.works/dev/jdowling/h-and-m/` (`customers.csv`,
`articles.csv`, `transactions_train.csv`, and `images/`). `hm_features.py`
samples `--customers` customers (2,000 by default), streams the 3.5 GB
transactions file keeping only their purchases, keeps the articles they bought,
and generates clicks and ignores around the purchases, as the course does. Each
data source records `status: present` and `data.<source>.writes` its group
once the job has run.

## The environments

```bash
hops env clone <slug>-jobs-env --from torch-training-pipeline          # the feature and training jobs
hops env install <slug>-jobs-env -f requirements.txt
hops env clone <slug>-inference-env --from torch-inference-pipeline    # the deployment
hops env install <slug>-inference-env -f requirements.txt
```

`requirements.txt` is `recommender/requirements.txt`, copied into the system.
The app runs in `python-agent-pipeline`, which ships FastAPI.

## The jobs, in order

```bash
hops job deploy <slug>-features src/<slug_pkg>/hm_features.py --env <slug>-jobs-env \
  --args "--customers 2000" --run --wait
hops job deploy <slug>-train-retrieval src/<slug_pkg>/train_retrieval.py --env <slug>-jobs-env --run --wait
hops job deploy <slug>-train-ranker src/<slug_pkg>/train_ranker.py --env <slug>-jobs-env --run --wait
```

`train_retrieval.py` creates the feature views, trains the two towers with an
in-batch softmax loss, registers the query tower as `query_model` (TorchScript
plus the customer vocabulary) with `recall_at_100` on the test split, and writes
the item tower's embedding of every trained article to `candidate_embeddings`,
whose vector index retrieval searches. `train_ranker.py` trains CatBoost on the
purchases and ten random negatives per purchase and registers `ranking_model`
with precision, recall, F1 and ROC-AUC. `requirements.targets` holds the ranker's
ROC-AUC target.

## infer: the deployment

```bash
hops deployment create ranking_model --name <alnum slug> --script src/<slug_pkg>/predictor.py \
  --env <slug>-inference-env --no-default-predictor
hops deployment start <alnum slug> --wait
hops deployment predict <alnum slug> --data '{"instances": [{"customer_id": "<an id from customers>", "k": 12}]}'
```

The request is `{customer_id, k}`; the reply is `{customer_id, items:
[{article_id, prod_name, product_type_name, colour_group_name,
index_group_name, garment_group_name, image_url, score}], retrieved,
already_bought, timings_ms}`, with each stage's time (query, retrieve, filter,
rank). `measured` gets the p99 of 50 requests over random customers against
`requirements.sla.realtime`.

## app: the storefront

Copy `recommender/app/` to `<slug>/app/` and deploy it as **hops-app** says, in
`python-agent-pipeline`. A customer picker (the 200 most active), product cards
ranked by the deployment, with Click and Buy, and New recommendations, which
records the cards shown and not touched as ignores. Every action is written to
`interactions` online (Buy also to `transactions`, so the next request leaves
the purchase out), and reaches the offline store at the groups' next
materialization. The history panel reads the customer's interactions back.

# Building the personalized recommender

The real-time example (`recs-example`) recommends H&M products to a shopper per
request. It follows the Decoding AI / Hopsworks course
(https://github.com/decodingai-magazine/personalized-recommender-course) on the
public H&M data, with the models in PyTorch and CatBoost and a JavaScript
storefront in place of the course's Streamlit UI. It is built phase
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
samples `--customers` customers (5,000 by default), streams the 3.5 GB
transactions file keeping only their purchases, and keeps the articles they
bought. A sample this size is too sparse for the two-tower model to beat
recommending the most popular articles, so it adds `--synthetic` purchases per
customer (30 by default), each a copy of one of the customer's own purchases
with the article swapped for a popular one of the same index and garment group,
marked `synthetic` in `transactions`. Then it generates clicks and ignores around
all purchases, as the course does. `transactions` and `interactions` get a
secondary index on `customer_id` (`online_config={"secondary_indexes":
[["customer_id"]]}`): the deployment and the app read a customer's rows online,
and the online key leads with the event time, so without it each read scans the
table. Each
data source records `status: present` and `data.<source>.writes` its group
once the job has run.

## The environments

```bash
hops env clone <slug>-jobs-env --from python-feature-pipeline          # the feature and training jobs
hops env install <slug>-jobs-env -f requirements.txt
hops env clone <slug>-inference-env --from minimal-inference-pipeline  # the deployment
hops env install <slug>-inference-env -f requirements.txt
```

`requirements.txt` is `recommender/requirements.txt`, copied into the system:
the CPU build of torch and CatBoost, on bases that already ship pandas, polars
and the Hopsworks client. Nothing trains or serves on a GPU, so the torch
bases, whose CUDA wheels are gigabytes, are not used. Clone one environment at
a time: two `hops` commands logging in at once from the same client race on its
certificate directory.
The app runs in `python-agent-pipeline`, which ships FastAPI.

## The jobs, in order

```bash
hops job deploy <slug>-features src/<slug_pkg>/hm_features.py --env <slug>-jobs-env \
  --args "--customers 5000 --synthetic 30" --run --wait
hops job deploy <slug>-train-retrieval src/<slug_pkg>/train_retrieval.py --env <slug>-jobs-env --run --wait
hops job deploy <slug>-train-ranker src/<slug_pkg>/train_ranker.py --env <slug>-jobs-env --run --wait
```

`train_retrieval.py` creates the feature views, trains the two towers with an
in-batch softmax loss, registers the query tower as `query_model` (TorchScript
plus the customer vocabulary, and the item tower with its vocabularies and the
catalogue's mean embedding, for the session) with `recall_at_100` on the real
purchases of the test split, and writes
the item tower's embedding of every trained article to `candidate_embeddings`,
whose vector index retrieval searches. `train_ranker.py` trains CatBoost on each
customer's latest fifth of purchases against ten negatives per purchase drawn by
popularity, with the customer's taste as features: the share of their earlier
purchases with the article's colour, index group, garment group, product type
and section. Without the taste features the model sees only the article and the
customer's age, learns that black sells, and ranks black first for everyone; with
uniform negatives it learns popularity the same way. It registers
`ranking_model` with precision, recall, F1 and ROC-AUC, and `features.json`
naming the features, the categorical ones and the taste attributes. `requirements.targets` holds the ranker's
ROC-AUC target.

## infer: the deployment

```bash
hops deployment create ranking_model --name <alnum slug> --script src/<slug_pkg>/predictor.py \
  --env <slug>-inference-env --no-default-predictor
hops deployment start <alnum slug>
hops deployment predict <alnum slug> --data '{"instances": [{"customer_id": "<an id from customers>", "k": 12}]}'
```

The request is `{customer_id, k, recent}`, `recent` the page's clicks and
purchases, newest first; the reply is `{customer_id, items: [{article_id,
prod_name, product_type_name, colour_group_name, index_group_name,
garment_group_name, image_url, score, session_similarity, reason}], retrieved,
already_bought, session_items, timings_ms}`, with each stage's time (query,
retrieve, filter, rank).

The deployment follows the shopper's session: their clicks and purchases of the
last day, read from `interactions` and joined with `recent`, since a click
written a moment ago may not be in the online store yet. The item tower embeds
them, minus the catalogue's mean embedding, which every item shares and which
otherwise hides what tells a shoe from a sweater, and the result is blended into
the query, so clicking a shoe retrieves shoes. The 50 articles most like the
session are added to the candidates, found exactly over the catalogue, which the
deployment embeds once at start and keeps in memory with the attributes the
ranker needs: the vector index's approximate inner-product search returns poor
neighbours for a centered vector. Articles added to `articles` later are served
after a restart. Articles the session already
showed and the shopper acted on are left out. Of the slots, half of those not
left to exploring go to the candidates most like the session (`reason:
session`), the rest to the highest purchase probability (`taste`), and a fifth
to candidates drawn at random from the remainder (`explore`). One shoe click
gives five to seven shoes in twelve. `hops deployment create` on an existing name keeps its script: to deploy a
changed `predictor.py`, `hops deployment delete <name> --yes` and create it again. `measured` gets the p99 of 50 requests over random customers against
`requirements.sla.realtime`: about 40 ms in the deployment on the example's
data, p99 44 ms, with or without a session. The deployment's matrix products are
NumPy einsum, not `@`: OpenBLAS starts a thread per node core under the pod's CPU
limit and the throttling stalls the request for up to 100 ms.

## app: the storefront

Copy `recommender/app/` to `<slug>/app/` and deploy it as **hops-app** says, in
`python-agent-pipeline`. A customer picker (the 200 most active), product cards
ranked by the deployment, with Click and Buy, which record the action and ask
for new recommendations with the page's session, and New recommendations, which
records the cards shown and not touched as ignores. Cards chosen for the session
carry "Like your clicks" and exploration picks "Discover". Every action is written to
`interactions` in the online store (Buy also to `transactions`, so the next
request leaves the purchase out), which the deployment and the history panel
read. Last lookup is the deployment's own time for the latest request, the sum
of its stages, and the round trip under it the app's call to the deployment.
An app pod cannot write the offline Delta tables (it has no HopsFS
certificates for the client's direct write), so the shoppers' actions are not
training data until a job copies them offline.

# ruff: noqa: INP001
"""The recommender deployment: retrieve, filter and rank, per request.

Deployed with the ranking model; the query tower is downloaded from the
registry when the deployment starts:

    hops deployment create ranking_model --name <alnum slug> \
        --script src/<slug_pkg>/predictor.py --env <slug>-inference-env --no-default-predictor

A request is `{"customer_id": "...", "k": 12, "recent": [article ids]}`, `recent`
the session's clicks and purchases, newest first, which the caller may send. It runs the course's four stages,
steered by the shopper's session, their clicks and purchases of the last day:

1. the customer's features (age) from the `customers` feature view, and the
   month's cycle from the request time, through the query tower; the session's
   articles, through the item tower, are blended into that query, so clicking a
   shoe retrieves shoes;
2. the 100 nearest articles to the query in `candidate_embeddings`, and the 50
   most like the session alone, found exactly over the catalogue;
3. the articles the customer bought, and those the session already showed them
   and they clicked, bought or passed over, dropped, read online from
   `transactions` and `interactions`;
4. the rest scored by the ranking model's purchase probability, with each
   article's features and the customer's taste; the article features and
   embeddings are the catalogue's, read from `articles` once at start;
   the slots go to the articles most like the session, then the most probable,
   and a fifth to articles drawn at random from the rest (see `select`).

The reply is `{"customer_id", "items": [{article_id, score, session_similarity,
reason, prod_name, ..., image_url}], "retrieved", "already_bought",
"session_items", "timings_ms"}`, `reason` being session, taste or explore.
"""

from __future__ import annotations

import glob
import json
import math
import os
import re
import time
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

RETRIEVED = 100
SESSION_RETRIEVED = 50
SESSION = timedelta(days=1)
SESSION_ITEMS = 10
# The share of the query taken by the session: at 0.6 a shoe click makes about a
# quarter of the 100 candidates shoes and keeps a quarter of the customer's own.
SESSION_WEIGHT = 0.6
EXPLORE = 0.2
# The purchase lookup is SQL built from the id, so only an id of this shape is looked up.
CUSTOMER_ID = re.compile(r"^[0-9A-Za-z_-]{1,64}$")
ARTICLE_ID = re.compile(r"^[0-9]{1,16}$")
SHOWN = [
    "article_id",
    "prod_name",
    "product_type_name",
    "colour_group_name",
    "index_group_name",
    "garment_group_name",
    "image_url",
]


def load_model_file(name: str) -> str:
    """A file saved with the model; it mounts under MODEL_FILES_PATH when serving."""
    for root in (
        os.environ.get("MODEL_FILES_PATH"),
        os.environ.get("ARTIFACT_FILES_PATH"),
        "/mnt/models",
        "/mnt/artifacts",
    ):
        if root:
            hits = glob.glob(f"{root}/**/{name}", recursive=True)
            if hits:
                return hits[0]
    raise FileNotFoundError(f"{name} not found under the model/artifact mounts")


def month_cycle(when: datetime) -> tuple[float, float]:
    angle = when.month * (2 * math.pi / 12)
    return math.sin(angle), math.cos(angle)


def rank(candidates: list[str], history, articles, age: float, model, spec: dict, k: int):
    """The top `k` of the candidates not yet bought, by the model's purchase probability.

    `history` is a pandas frame of the customer's purchases, article_id plus the
    `spec["taste"]` attributes, and `articles` one of the candidates' features keyed
    by article_id; a candidate `articles` lacks is skipped. `spec` is the model's
    features.json. The taste shares are computed as train_ranker.py's `taste` does.
    """
    bought = set(history["article_id"].astype(str))
    fresh = [a for a in dict.fromkeys(candidates) if a not in bought]
    rows = articles[articles["article_id"].isin(fresh)].copy()
    if rows.empty:
        return []
    rows["age"] = age
    categorical = spec["categorical"]
    rows[categorical] = rows[categorical].fillna("").astype(str)
    for column in spec.get("taste", []):
        shares = history[column].value_counts(normalize=True) if len(history) else {}
        rows[f"{column}_share"] = rows[column].map(shares).fillna(0.0).astype(float)
    rows["score"] = model.predict_proba(rows[spec["features"]], thread_count=1)[:, 1]
    top = rows.sort_values("score", ascending=False).head(k)
    shown = [c for c in SHOWN if c in top.columns] + ["score"]
    return top[shown].astype(object).where(top[shown].notna(), None).to_dict("records")


def session_vector(embeddings: np.ndarray) -> np.ndarray | None:
    """The recency-weighted mean of the session's article embeddings, newest first.

    None for an empty session. Each older article counts 0.7 times the next.
    """
    if len(embeddings) == 0:
        return None
    weights = 0.7 ** np.arange(len(embeddings))
    return (embeddings * weights[:, None]).sum(axis=0) / weights.sum()


def blend(query: np.ndarray, session: np.ndarray | None, weight: float) -> np.ndarray:
    """`query` moved `weight` of the way towards the session's direction, at its own length.

    Dot-product retrieval scores grow with the query's length, so the blend keeps
    it and only turns the direction.
    """
    if session is None or not np.linalg.norm(session):
        return query
    norm = np.linalg.norm(query)
    turned = (1 - weight) * query / norm + weight * session / np.linalg.norm(session)
    return turned / np.linalg.norm(turned) * norm


def select(items: list[dict], embeddings: dict, session, k: int, rng) -> list[dict]:
    """The `k` items to show, each with the `reason` it was chosen.

    With a session, half the slots not left to exploring go to the articles most
    like it (`session`, by cosine similarity), so a click on a shoe shows shoes
    whatever the customer usually buys; the rest go to the highest purchase
    probability (`taste`). A fifth of the slots go to articles drawn at random
    from the remainder (`explore`), so the list never settles. `score` stays the
    purchase probability; `session_similarity` is 0 without a session.
    """
    for item in items:
        vector = embeddings.get(item["article_id"])
        similarity = 0.0
        if session is not None and vector is not None:
            norms = np.linalg.norm(vector) * np.linalg.norm(session)
            similarity = float(vector @ session / norms) if norms else 0.0
        item["session_similarity"] = similarity
    explore = min(round(k * EXPLORE), max(len(items) - k, 0))
    picked: list[dict] = []
    if session is not None:
        alike = sorted(items, key=lambda item: item["session_similarity"], reverse=True)
        picked = [dict(item, reason="session") for item in alike[: (k - explore) // 2]]
    chosen = {item["article_id"] for item in picked}
    by_taste = sorted(
        (item for item in items if item["article_id"] not in chosen),
        key=lambda item: item["score"],
        reverse=True,
    )
    taste = [dict(item, reason="taste") for item in by_taste[: k - explore - len(picked)]]
    rest = by_taste[len(taste) :]
    drawn = sorted(rng.choice(len(rest), explore, replace=False)) if explore else []
    return picked + taste + [dict(rest[i], reason="explore") for i in drawn]


class Predict:
    """Retrieval, filtering and ranking for one customer per request."""

    def __init__(self) -> None:
        import hopsworks
        import torch
        from catboost import CatBoostClassifier

        # One request is one query vector and a hundred rows: threads only contend.
        torch.set_num_threads(1)
        project = hopsworks.login()
        self.fs = project.get_feature_store()
        self.ranker = CatBoostClassifier()
        self.ranker.load_model(load_model_file("ranking_model.cbm"))
        with open(load_model_file("features.json")) as f:
            self.spec = json.load(f)

        # The latest query tower: each retrieval run registers one and rewrites every
        # candidate embedding with its item tower, so only the newest matches them.
        registry = project.get_model_registry()
        query_model = max(registry.get_models("query_model"), key=lambda m: m.version)
        query_dir = query_model.download()
        self.query_tower = torch.jit.load(os.path.join(query_dir, "query_tower.pt"))
        self.query_tower.eval()
        with open(os.path.join(query_dir, "customer_vocab.json")) as f:
            self.customer_vocab = json.load(f)
        self.item_tower = torch.jit.load(os.path.join(query_dir, "item_tower.pt"))
        self.item_tower.eval()
        with open(os.path.join(query_dir, "item_vocab.json")) as f:
            self.item_vocab = json.load(f)
        # Every item embedding shares one large direction; with the catalogue's mean
        # subtracted, a shoe's vector points at shoes instead of at everything.
        with open(os.path.join(query_dir, "item_mean.json")) as f:
            self.item_mean = np.asarray(json.load(f))
        self.torch = torch
        self.rng = np.random.default_rng()

        self.customers = self.fs.get_feature_view("customers", version=1)
        self.customers.init_serving(1)
        self.candidates = self.fs.get_feature_group("candidate_embeddings", version=1)
        transactions = self.fs.get_feature_group("transactions", version=1)
        articles = self.fs.get_feature_group("articles", version=1)
        # The purchases with the attributes the taste features are shares of, in one
        # query: transactions is indexed on customer_id and articles keyed by article_id.
        attributes = ", ".join(f"a.`{c}`" for c in self.spec.get("taste", []))
        self.bought_sql = (
            f"SELECT t.article_id{', ' + attributes if attributes else ''} "
            f"FROM `{transactions.name}_{transactions.version}` t "
            f"JOIN `{articles.name}_{articles.version}` a ON a.article_id = t.article_id "
            "WHERE t.customer_id = '{}'"
        )
        # The catalogue in memory: the attributes the ranker and the cards need, and
        # each article's centered item-tower embedding. Embedding per request costs
        # 45 ms (TorchScript re-specialises for every new batch size) and a feature
        # lookup of the candidates 35 ms; articles added later appear after a restart.
        columns = dict.fromkeys(["article_id", *SHOWN, *self.spec["categorical"], *self.item_vocab])
        catalogue = self.fs.sql(
            f"SELECT {', '.join(f'`{c}`' for c in columns)} "
            f"FROM `{articles.name}_{articles.version}`",
            online=True,
            dataframe_type="pandas",
        )
        catalogue["article_id"] = catalogue["article_id"].astype(str)
        self.catalogue = catalogue.set_index("article_id", drop=False)
        known = catalogue[catalogue["article_id"].isin(list(self.item_vocab["article_id"]))]
        centered = self.embed_items(known) - self.item_mean
        self.vectors = dict(zip(known["article_id"], centered, strict=True))
        self.catalogue_unit = centered / np.linalg.norm(centered, axis=1, keepdims=True)
        self.catalogue_ids = known["article_id"].tolist()
        interactions = self.fs.get_feature_group("interactions", version=1)
        # interactions is indexed on customer_id too.
        self.session_sql = (
            "SELECT article_id, interaction_score "
            f"FROM `{interactions.name}_{interactions.version}` "
            "WHERE customer_id = '{}' AND t_dat >= '{}' ORDER BY t_dat DESC LIMIT 50"
        )

    def embed_query(self, customer_id: str, age: float, when: datetime) -> list[float]:
        torch = self.torch
        sin, cos = month_cycle(when)
        with torch.no_grad():
            vector = self.query_tower(
                torch.tensor([self.customer_vocab.get(customer_id, 0)]),
                torch.tensor([age], dtype=torch.float32),
                torch.tensor([sin], dtype=torch.float32),
                torch.tensor([cos], dtype=torch.float32),
            )
        return vector[0].tolist()

    def most_like(self, session: np.ndarray, k: int = SESSION_RETRIEVED) -> list[str]:
        """The `k` catalogue articles with the highest centered cosine to `session`.

        Exact, over the catalogue embedded at start: the vector index's approximate
        inner-product search returns poor neighbours for a centered vector, which
        points away from where every article lies.
        """
        # einsum, not @: a matrix product goes to OpenBLAS, which starts a thread per
        # node core under the pod's CPU limit, and the throttling stalls every stage
        # after it for up to 100 ms. einsum's own loop takes 1 ms here.
        scores = np.einsum("ij,j->i", self.catalogue_unit, session / np.linalg.norm(session))
        top = np.argpartition(-scores, min(k, len(scores) - 1))[:k]
        return [self.catalogue_ids[i] for i in top[np.argsort(-scores[top])]]

    def embed_items(self, rows) -> np.ndarray:
        """Item-tower embeddings of articles, rows of article_id, garment and index group."""
        torch = self.torch

        def ids(column: str):
            vocab = self.item_vocab[column]
            return torch.tensor([vocab.get(str(v), 0) for v in rows[column]])

        with torch.no_grad():
            return self.item_tower(
                ids("article_id"), ids("garment_group_name"), ids("index_group_name")
            ).numpy()

    def recommend(self, customer_id: str, k: int = 12, recent: list[str] | None = None) -> dict:
        """Recommendations for one customer; `recent` is the session's clicks, newest first.

        The caller's `recent` comes first: a click written a moment ago may not be in
        the online store yet, and the next request is exactly when it matters.
        """
        timings = {}
        start = time.perf_counter()

        def lap(stage: str) -> None:
            nonlocal start
            now = time.perf_counter()
            timings[stage] = round((now - start) * 1000, 1)
            start = now

        if not CUSTOMER_ID.match(customer_id):
            return {"customer_id": customer_id, "items": [], "error": "invalid customer id"}
        customer = self.customers.get_feature_vector(
            {"customer_id": customer_id}, return_type="pandas"
        )
        if customer.empty or customer["age"].isna().all():
            return {"customer_id": customer_id, "items": [], "error": "unknown customer"}
        age = float(customer["age"].iloc[0])
        now = datetime.now(UTC)
        since = (now - SESSION).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        stored = self.fs.sql(
            self.session_sql.format(customer_id, since), online=True, dataframe_type="pandas"
        )
        clicked = [a for a in dict.fromkeys(recent or []) if ARTICLE_ID.match(a)][:SESSION_ITEMS]
        sent = pd.DataFrame({"article_id": clicked, "interaction_score": 1})
        stored = pd.concat([sent, stored], ignore_index=True)
        stored["article_id"] = stored["article_id"].astype(str)
        stored = stored.drop_duplicates("article_id")
        seen = set(stored["article_id"])
        engaged = [
            self.vectors[a]
            for a in stored.loc[stored["interaction_score"] >= 1, "article_id"]
            if a in self.vectors
        ][:SESSION_ITEMS]
        session = session_vector(np.array(engaged)) if engaged else None
        query = blend(np.array(self.embed_query(customer_id, age, now)), session, SESSION_WEIGHT)
        lap("query")

        neighbours = self.candidates.find_neighbors(query.tolist(), k=RETRIEVED)
        candidates = [str(values[0]) for _, values in neighbours]
        if session is not None:
            # The articles most like the session, whatever the customer's own query
            # retrieves, so a customer far from shoes who clicks one still sees shoes.
            candidates = list(dict.fromkeys(candidates + self.most_like(session)))
        lap("retrieve")

        # Raw SQL on the pooled online connection: a feature group filter read asks
        # the backend to build the query first, which is most of 300 ms.
        bought = self.fs.sql(
            self.bought_sql.format(customer_id), online=True, dataframe_type="pandas"
        )
        bought_ids = set(bought["article_id"].astype(str))
        lap("filter")

        unseen = [a for a in candidates if a not in seen]
        articles = self.catalogue[self.catalogue.index.isin(unseen)]
        scored = rank(unseen, bought, articles, age, self.ranker, self.spec, len(unseen))
        items = select(scored, self.vectors, session, k, self.rng)
        lap("rank")
        return {
            "customer_id": customer_id,
            "items": items,
            "retrieved": len(candidates),
            "already_bought": len(bought_ids & set(candidates)),
            "session_items": len(engaged),
            "timings_ms": timings,
        }

    def predict(self, inputs):
        """KServe hands over the request's instances; each is one customer."""
        if isinstance(inputs, dict):
            inputs = inputs.get("instances", [inputs])
        replies = []
        for request in inputs:
            k = max(1, min(int(request.get("k", 12)), 50))
            recent = [str(a) for a in request.get("recent") or []]
            replies.append(self.recommend(str(request["customer_id"]), k, recent))
        return {"predictions": replies}

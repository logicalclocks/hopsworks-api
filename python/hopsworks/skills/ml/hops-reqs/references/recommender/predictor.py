# ruff: noqa: INP001
"""The recommender deployment: retrieve, filter and rank, per request.

Deployed with the ranking model; the query tower is downloaded from the
registry when the deployment starts:

    hops deployment create ranking_model --name <alnum slug> \
        --script src/<slug_pkg>/predictor.py --env <slug>-inference-env --no-default-predictor

A request is `{"customer_id": "...", "k": 12, "recent": [article ids]}`, `recent`
being the page's clicks and purchases, newest first. It runs the course's four
stages, steered by the shopper's session, their clicks and purchases of the last
day, through the Feature Store's online APIs:

1. the customer's features (age) from the `customers` feature view, and the
   month's cycle from the request time, through the query tower; the session's
   vectors, from the `session_embeddings` feature view, are blended into that
   query, so clicking a shoe retrieves shoes;
2. the 100 nearest articles to the query in `candidate_embeddings`' `embeddings`
   index, and the 50 nearest to the session in its `session_embedding` index;
3. the articles the customer bought, and those the session already showed them
   and they clicked, bought or passed over, dropped, read online from the
   `transactions` and `interactions` feature groups;
4. the rest scored by the ranking model's purchase probability, with each
   article's features from the `articles` feature view and the customer's taste;
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

        # One request is one query vector: threads only contend.
        torch.set_num_threads(1)
        project = hopsworks.login()
        fs = project.get_feature_store()
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
        self.torch = torch
        self.rng = np.random.default_rng()

        self.customers = fs.get_feature_view("customers", version=1)
        self.articles = fs.get_feature_view("articles", version=1)
        self.session_embeddings = fs.get_feature_view("session_embeddings", version=1)
        for view in (self.customers, self.articles, self.session_embeddings):
            view.init_serving(1)
        self.candidates = fs.get_feature_group("candidate_embeddings", version=1)
        self.session_column = [f.name for f in self.candidates.features].index("session_embedding")
        # Both are indexed on customer_id online, so these reads are lookups.
        self.transactions = fs.get_feature_group("transactions", version=1)
        self.interactions = fs.get_feature_group("interactions", version=1)

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

    def article_features(self, article_ids: list[str]):
        """The `articles` feature view's rows for these articles; unknown ones are dropped."""
        if not article_ids:
            return pd.DataFrame(columns=["article_id"])
        rows = self.articles.get_feature_vectors(
            [{"article_id": a} for a in article_ids], return_type="pandas", allow_missing=True
        )
        return rows.dropna(subset=["article_id"])

    def session(self, customer_id: str, recent: list[str], since: datetime):
        """The session's article ids, newest first, the ones engaged with, and its vector."""
        interactions = self.interactions
        stored = interactions.filter(
            (interactions.customer_id == customer_id) & (interactions.t_dat >= since)
        ).read(online=True, dataframe_type="pandas")
        stored = stored.sort_values("t_dat", ascending=False)
        clicked = [a for a in dict.fromkeys(recent) if ARTICLE_ID.match(a)][:SESSION_ITEMS]
        rows = pd.concat(
            [pd.DataFrame({"article_id": clicked, "interaction_score": 1}), stored],
            ignore_index=True,
        )
        rows["article_id"] = rows["article_id"].astype(str)
        rows = rows.drop_duplicates("article_id")
        engaged = rows.loc[rows["interaction_score"] >= 1, "article_id"].head(SESSION_ITEMS)
        if engaged.empty:
            return set(rows["article_id"]), 0, None
        vectors = self.session_embeddings.get_feature_vectors(
            [{"article_id": a} for a in engaged], return_type="pandas", allow_missing=True
        ).dropna(subset=["session_embedding"])
        order = {a: i for i, a in enumerate(engaged)}
        vectors = vectors.sort_values("article_id", key=lambda ids: ids.map(order))
        stack = np.array([np.asarray(v, dtype=float) for v in vectors["session_embedding"]])
        return set(rows["article_id"]), len(engaged), session_vector(stack)

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
        seen, engaged, session = self.session(
            customer_id, recent or [], (now - SESSION).replace(tzinfo=None)
        )
        query = blend(np.array(self.embed_query(customer_id, age, now)), session, SESSION_WEIGHT)
        lap("query")

        neighbours = self.candidates.find_neighbors(query.tolist(), col="embeddings", k=RETRIEVED)
        if session is not None:
            # The articles most like the session, whatever the customer's own query
            # retrieves, so a customer far from shoes who clicks one still sees shoes.
            neighbours += self.candidates.find_neighbors(
                session.tolist(), col="session_embedding", k=SESSION_RETRIEVED
            )
        vectors = {
            str(values[0]): np.asarray(values[self.session_column], dtype=float)
            for _, values in neighbours
        }
        candidates = list(vectors)
        lap("retrieve")

        transactions = self.transactions
        bought = transactions.filter(transactions.customer_id == customer_id).read(
            online=True, dataframe_type="pandas"
        )
        bought_ids = set(bought["article_id"].astype(str))
        lap("filter")

        # One row per purchase, as train_ranker.py counts them for the taste shares.
        history = (
            bought[["article_id"]]
            .astype(str)
            .merge(self.article_features(sorted(bought_ids)), on="article_id")
        )
        unseen = [a for a in candidates if a not in seen]
        articles = self.article_features(unseen)
        scored = rank(unseen, history, articles, age, self.ranker, self.spec, len(unseen))
        items = select(scored, vectors, session, k, self.rng)
        lap("rank")
        return {
            "customer_id": customer_id,
            "items": items,
            "retrieved": len(candidates),
            "already_bought": len(bought_ids & set(candidates)),
            "session_items": engaged,
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

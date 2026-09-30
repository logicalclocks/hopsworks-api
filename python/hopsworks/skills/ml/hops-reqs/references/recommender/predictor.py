# ruff: noqa: INP001
"""The recommender deployment: retrieve, filter and rank, per request.

Deployed with the ranking model; the query tower is downloaded from the
registry when the deployment starts:

    hops deployment create ranking_model --name <alnum slug> \
        --script src/<slug_pkg>/predictor.py --env <slug>-inference-env --no-default-predictor

A request is `{"customer_id": "...", "k": 12}`. It runs the course's four stages:

1. the customer's features (age) from the `customers` feature view, and the
   month's cycle from the request time, through the query tower;
2. the 100 nearest articles to that embedding in `candidate_embeddings`;
3. the articles the customer already bought dropped, read online from
   `transactions`, so a purchase made in the app disappears on the next request;
4. the rest ranked by the ranking model's purchase probability, with each
   article's features from the `articles` feature view.

The reply is `{"customer_id", "items": [{article_id, score, prod_name, ...,
image_url}], "retrieved", "timings_ms"}`.
"""

from __future__ import annotations

import glob
import json
import math
import os
import re
import time
from datetime import UTC, datetime

RETRIEVED = 100
# The purchase lookup is SQL built from the id, so only an id of this shape is looked up.
CUSTOMER_ID = re.compile(r"^[0-9A-Za-z_-]{1,64}$")
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


def rank(candidates: list[str], bought: set[str], articles, age: float, model, features, k: int):
    """The top `k` of the candidates not yet bought, by the model's purchase probability.

    `articles` is a pandas frame of the candidates' features keyed by article_id;
    a candidate the frame lacks is skipped.
    """
    fresh = [a for a in dict.fromkeys(candidates) if a not in bought]
    rows = articles[articles["article_id"].isin(fresh)].copy()
    if rows.empty:
        return []
    rows["age"] = age
    categorical = [f for f in features if f != "age"]
    rows[categorical] = rows[categorical].fillna("").astype(str)
    rows["score"] = model.predict_proba(rows[features], thread_count=1)[:, 1]
    top = rows.sort_values("score", ascending=False).head(k)
    shown = [c for c in SHOWN if c in top.columns] + ["score"]
    return top[shown].astype(object).where(top[shown].notna(), None).to_dict("records")


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
            self.features = json.load(f)["features"]

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

        self.customers = self.fs.get_feature_view("customers", version=1)
        self.customers.init_serving(1)
        self.articles = self.fs.get_feature_view("articles", version=1)
        self.articles.init_serving(1)
        self.candidates = self.fs.get_feature_group("candidate_embeddings", version=1)
        transactions = self.fs.get_feature_group("transactions", version=1)
        self.bought_sql = (
            f"SELECT article_id FROM `{transactions.name}_{transactions.version}` "
            "WHERE customer_id = '{}'"
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

    def recommend(self, customer_id: str, k: int = 12) -> dict:
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
        query = self.embed_query(customer_id, age, datetime.now(UTC))
        lap("query")

        neighbours = self.candidates.find_neighbors(query, k=RETRIEVED)
        candidates = [str(values[0]) for _, values in neighbours]
        lap("retrieve")

        # Raw SQL on the pooled online connection: a feature group filter read asks
        # the backend to build the query first, which is most of 300 ms.
        bought = self.fs.sql(
            self.bought_sql.format(customer_id), online=True, dataframe_type="pandas"
        )
        bought_ids = set(bought["article_id"].astype(str)) if len(bought) else set()
        lap("filter")

        articles = self.articles.get_feature_vectors(
            [{"article_id": a} for a in candidates], return_type="pandas", allow_missing=True
        )
        items = rank(candidates, bought_ids, articles, age, self.ranker, self.features, k)
        lap("rank")
        return {
            "customer_id": customer_id,
            "items": items,
            "retrieved": len(candidates),
            "already_bought": len(bought_ids & set(candidates)),
            "timings_ms": timings,
        }

    def predict(self, inputs):
        """KServe hands over the request's instances; each is one customer."""
        if isinstance(inputs, dict):
            inputs = inputs.get("instances", [inputs])
        replies = []
        for request in inputs:
            k = max(1, min(int(request.get("k", 12)), 50))
            replies.append(self.recommend(str(request["customer_id"]), k))
        return {"predictions": replies}
